//! Bounded Interactive Brokers TWS API operations.
//!
//! The module owns IB Gateway discovery, connection reuse, provider contract
//! resolution, market-data normalization, and fail-closed order outcomes. Its
//! callers do not select arbitrary provider methods or addresses.

use crate::contract::{
    AccountOpenOrder, AccountPosition, AccountStateRequest, AccountStateResponse,
    AccountValueSummary, AdaptivePriority, AlgoStrategy, CancelOrderRequest, Candle, CandleRequest,
    GatewayDetection, InstrumentKind, OptionChainRequest, OptionChainResponse, OptionContract,
    OptionContractRight, OptionQuoteRequest, OptionQuoteResponse, OrderAlgo, OrderProtection,
    OrderRequest, OrderResponse, OrderType, PROTOCOL_VERSION, PriceIncrementRule, ProbeResponse,
    ProtectionLegKind, ProtectionLegOutcome, ProtectionLegRequest, ProtectionOutcome,
    ProtectionStatus, QuoteDataType, ReconcileOrderRequest, ReconcileOrderResponse,
    ReconcileOrderState, ServerLink, StockQuote, StockQuoteRequest,
};
use crate::order_events::{OrderEventHub, OrderEventSubscription};
use chrono::{NaiveDate, Utc};
use futures::{Stream, StreamExt, TryStreamExt, stream};
use ibapi::accounts::types::AccountGroup;
use ibapi::accounts::{AccountSummary, AccountSummaryResult, AccountSummaryTags, PositionUpdate};
use ibapi::contracts::tick_types::TickType;
use ibapi::contracts::{
    Contract, ContractDetails, OptionChain, OptionRight, SecurityType, TagValue,
};
use ibapi::market_data::historical::{
    BarSize as HistoricalBarSize, BarTimestamp, Duration as HistoricalDuration,
    HistoricalBarUpdate, WhatToShow as HistoricalWhatToShow,
};
use ibapi::market_data::realtime::TickTypes;
use ibapi::market_data::{MarketDataType, TradingHours};
use ibapi::orders::{
    Action, CancelOrder, ExecutionData, ExecutionFilter, Executions, OcaType, Order, OrderData,
    OrderStatusKind, Orders, PlaceOrder, TimeInForce,
};
use ibapi::prelude::{Client, NoticeCategory, SubscriptionItem};
use ibapi::subscriptions::{
    FilterDataStream, NoticeStream, Subscription, SubscriptionItemStreamExt,
};
use std::collections::HashMap;
use std::env;
use std::fmt::{Debug, Display, Formatter};
use std::future::Future;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;
use time::OffsetDateTime;
use tokio::sync::Mutex;
use uuid::Uuid;

mod linked_exits;
mod order_messages;
mod server_link;

use linked_exits::{
    CancelPlan, ExitBook, LinkedSession, NewExitPlan, own_working_order, plan_cancel,
    plan_new_exit, working_orders,
};
use order_messages::{
    ORDER_FOLLOW_UP_TIMEOUT, OrderMessage, OutcomeDeadlines, ProviderNote, ProviderOrders,
    SubmittedOrder, classify_order_message, completed_reason, describe_notes, follow_up,
    is_terminal, read_provider_orders, reports_working_status, single_order_outcome,
    snapshot_status_event, with_provider_notes,
};
use server_link::ServerLinkWatch;

/// Standard paper-trading socket exposed by IB Gateway.
pub const DEFAULT_PAPER_GATEWAY_ADDRESS: &str = "127.0.0.1:4002";
/// Standard live-trading socket exposed by IB Gateway.
pub const DEFAULT_LIVE_GATEWAY_ADDRESS: &str = "127.0.0.1:4001";
/// Default TWS client id used by the browser bridge process.
pub const DEFAULT_BRIDGE_CLIENT_ID: i32 = 17_654;
/// Default TWS client id used by the Electron native sidecar process.
pub const DEFAULT_DESKTOP_CLIENT_ID: i32 = 17_655;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(4);
const MANAGED_ACCOUNTS_TIMEOUT: Duration = Duration::from_secs(2);
const READ_TIMEOUT: Duration = Duration::from_secs(12);
const QUOTE_TIMEOUT: Duration = Duration::from_secs(6);
/// Longest single-option quote read, operation lock and retry included. The
/// desktop sidecar's and the web app's deadlines are longer, so neither gives
/// up while the read still holds the lock.
const OPTION_QUOTE_BUDGET: Duration = Duration::from_secs(15);
const ORDER_OPERATION_LOCK_TIMEOUT: Duration = Duration::from_secs(1);
/// Longest a live stock quote may go without any provider tick before its
/// subscription is treated as silently lost and resubscribed.
const LIVE_QUOTE_IDLE_TIMEOUT: Duration = Duration::from_secs(60);
/// Unrouted TWS notices after which existing market-data lines deliver
/// nothing: connectivity lost (1100), restored with data lost (1101), and
/// socket port reset (1300).
const QUOTE_CONNECTIVITY_LOSS_CODES: [i32; 3] = [1100, 1101, 1300];
/// IBKR notice code for a request no security definition matches.
const NO_SECURITY_DEFINITION_CODE: i32 = 200;
const ORDER_PREPARATION_TIMEOUT: Duration = Duration::from_secs(14);
const ORDER_SUBMISSION_TIMEOUT: Duration = Duration::from_secs(2);
const ORDER_ACKNOWLEDGEMENT_TIMEOUT: Duration = Duration::from_secs(6);
/// Event-stream and follow-up deadlines once an order was handed to TWS.
const ORDER_OUTCOME_DEADLINES: OutcomeDeadlines = OutcomeDeadlines {
    acknowledgement: ORDER_ACKNOWLEDGEMENT_TIMEOUT,
    follow_up: ORDER_FOLLOW_UP_TIMEOUT,
};
/// Bounds one order-reference reconciliation across its three provider reads.
const ORDER_RECONCILIATION_TIMEOUT: Duration = Duration::from_secs(30);
/// Longest OCA group name reported in account state (renderer contract bound).
const MAX_OCA_GROUP_LENGTH: usize = 512;
/// Prefix of every caller-generated IBKR order reference.
const CUSTOMER_ORDER_ID_PREFIX: &str = "sunk-";
#[cfg(test)]
const ORDER_CALLER_TIMEOUT_FLOOR: Duration = Duration::from_secs(30);
const MAX_STRIKES_PER_SIDE: usize = 20;
const MAX_HISTORY_POINTS: usize = 1_000;
/// Keeps contract-detail requests below the provider's conservative pacing limit.
const OPTION_CONTRACT_RESOLUTION_CONCURRENCY: usize = 8;
/// Bounds a maximum 80-contract chain to two quote waves before caller timeout.
const OPTION_QUOTE_CONCURRENCY: usize = MAX_STRIKES_PER_SIDE * 2;
#[cfg(test)]
const OPTION_CHAIN_CALLER_TIMEOUT_FLOOR: Duration = Duration::from_secs(45);
const DETECTION_CLIENT_ID_OFFSET: i32 = 100;
/// Empty `futFopExchange` value required when discovering stock option chains.
const STOCK_OPTION_FUT_FOP_EXCHANGE: &str = "";
/// IBKR code for an option quote with only partially entitled market data.
const PARTIAL_OPTION_MARKET_DATA_CODE: i32 = 10_091;
const HISTORICAL_DATA_ERROR_CODE: i32 = 162;
const HISTORICAL_NO_DATA_MESSAGE: &str = "HMDS query returned no data";

/// Validated same-machine IB Gateway configuration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GatewayConfig {
    addresses: Vec<SocketAddr>,
    client_id: i32,
}

impl GatewayConfig {
    /// Validate ordered loopback IB Gateway socket candidates and a TWS client id.
    ///
    /// # Errors
    ///
    /// Returns an error for an empty candidate list, a non-IPv4-loopback
    /// address, a non-positive client id, or a client id that cannot reserve a
    /// distinct transient discovery id.
    pub fn new(addresses: Vec<SocketAddr>, client_id: i32) -> Result<Self, GatewayError> {
        if addresses.is_empty() {
            return Err(GatewayError::Configuration(
                "at least one IB Gateway address is required".to_string(),
            ));
        }
        if client_id <= 0 {
            return Err(GatewayError::Configuration(
                "IB Gateway client id must be positive and must not use client id 0".to_string(),
            ));
        }
        if client_id > i32::MAX - DETECTION_CLIENT_ID_OFFSET {
            return Err(GatewayError::Configuration(
                "IB Gateway client id is too large to reserve a discovery client id".to_string(),
            ));
        }
        if addresses
            .iter()
            .any(|address| !matches!(address.ip(), IpAddr::V4(ip) if ip.is_loopback()))
        {
            return Err(GatewayError::Configuration(
                "IB Gateway addresses must use an explicit IPv4 loopback address".to_string(),
            ));
        }
        Ok(Self {
            addresses,
            client_id,
        })
    }

    /// Build the standard paper-first IB Gateway configuration.
    ///
    /// # Errors
    ///
    /// Returns an error only if an internal default is invalid.
    pub fn standard(client_id: i32) -> Result<Self, GatewayError> {
        Self::new(default_gateway_addresses(), client_id)
    }

    /// Build configuration from optional process-local advanced overrides.
    ///
    /// `SUNK_IBKR_GATEWAY_ADDRESS` replaces the standard paper/live candidates,
    /// and `SUNK_IBKR_CLIENT_ID` replaces the supplied process default.
    ///
    /// # Errors
    ///
    /// Returns an error when an override is malformed or unsafe.
    pub fn from_env(default_client_id: i32) -> Result<Self, GatewayError> {
        let client_id = match env::var("SUNK_IBKR_CLIENT_ID") {
            Ok(value) => value.parse::<i32>().map_err(|error| {
                GatewayError::Configuration(format!(
                    "SUNK_IBKR_CLIENT_ID must be a positive integer: {error}"
                ))
            })?,
            Err(env::VarError::NotPresent) => default_client_id,
            Err(error) => {
                return Err(GatewayError::Configuration(format!(
                    "could not read SUNK_IBKR_CLIENT_ID: {error}"
                )));
            }
        };
        let addresses = match env::var("SUNK_IBKR_GATEWAY_ADDRESS") {
            Ok(value) => vec![parse_gateway_address(&value)?],
            Err(env::VarError::NotPresent) => default_gateway_addresses(),
            Err(error) => {
                return Err(GatewayError::Configuration(format!(
                    "could not read SUNK_IBKR_GATEWAY_ADDRESS: {error}"
                )));
            }
        };
        Self::new(addresses, client_id)
    }

    /// Return the ordered IB Gateway socket candidates.
    pub fn addresses(&self) -> &[SocketAddr] {
        &self.addresses
    }

    /// Return the TWS client id for this process.
    pub fn client_id(&self) -> i32 {
        self.client_id
    }

    fn detection_client_id(&self) -> i32 {
        self.client_id + DETECTION_CLIENT_ID_OFFSET
    }

    /// Return a stable key suitable for a process-local client registry.
    pub fn registry_key(&self) -> String {
        format!(
            "{}@{}",
            self.client_id,
            self.addresses
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(",")
        )
    }
}

fn default_gateway_addresses() -> Vec<SocketAddr> {
    vec![
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 4002),
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 4001),
    ]
}

/// Parse one explicit IPv4 loopback socket.
///
/// # Errors
///
/// Returns an error for malformed, hostname, IPv6, or non-loopback input.
pub fn parse_gateway_address(value: &str) -> Result<SocketAddr, GatewayError> {
    let address = value.trim().parse::<SocketAddr>().map_err(|error| {
        GatewayError::Configuration(format!("invalid IB Gateway address: {error}"))
    })?;
    if !matches!(address.ip(), IpAddr::V4(ip) if ip.is_loopback()) {
        return Err(GatewayError::Configuration(
            "IB Gateway address must use an explicit IPv4 loopback address".to_string(),
        ));
    }
    Ok(address)
}

/// Stateful direct client for one same-machine IB Gateway process.
///
/// One instance owns connection selection, exclusive mutation/reset operations,
/// and ambiguous order state for the lifetime of its browser bridge or Electron
/// sidecar.
pub struct Gateway {
    config: GatewayConfig,
    active: Mutex<Option<Arc<Client>>>,
    /// Serializes order and cancellation work. Shared so that a linked exit,
    /// which places and cancels several orders, can keep holding it in its
    /// own task when the caller stops waiting.
    operation: Arc<Mutex<()>>,
    /// Serializes IBKR's connection-wide market-data mode with the single
    /// request that depends on it: each quote line, stock snapshot and option
    /// quote holds it only to switch the mode and send its own `reqMktData`
    /// (see [`lock_market_data_mode`]), so a quote stream and an option chain
    /// cannot subscribe under each other's mode and neither waits for the
    /// other's whole operation. Orders never take it. Shared with open quote
    /// streams so an idle resubscribe takes it too.
    market_data_mode: Arc<Mutex<()>>,
    pending_orders: Arc<Mutex<HashMap<String, String>>>,
    /// Serializes IBKR snapshot reads (positions, open and completed orders).
    ///
    /// `ibapi` routes the replies to these requests through shared broadcast
    /// channels, so two reads in flight at once see each other's rows and end
    /// markers, and one can end early without some orders. Account-state
    /// polling, reconciliation and order follow-ups each take it for one read.
    snapshots: Arc<Mutex<()>>,
    /// Every IBKR market-rule lookup, one at a time (see [`MarketRuleCache`]).
    market_rules: Arc<MarketRuleCache>,
    /// IB Gateway's link to IBKR's servers, watched on the current connection.
    server_link: Mutex<Option<ServerLinkWatch>>,
    /// Order-update fan-out for the current TWS client, started by the first
    /// order-event consumer and replaced when the connection changes.
    order_events: Mutex<Option<OrderEventHub>>,
}

/// Selected IBKR account value normalized for USD balance consumers.
#[derive(Clone, Debug, PartialEq)]
pub struct AccountBalance {
    /// Selected managed account identifier.
    pub account_ref: String,
    /// Net liquidation value in USD, converted from the base currency when needed.
    pub balance_usd: f64,
    /// Net liquidation, total cash, available funds, and gross position value.
    pub summary: AccountValueSummary,
}

/// Long-lived provider subscription for one selected IBKR account summary.
pub struct AccountBalanceSubscription {
    account_ref: String,
    subscription: Subscription<AccountSummaryResult>,
    summaries: FilterDataStream<Subscription<AccountSummaryResult>>,
    state: AccountSummaryState,
    initial_snapshot_seen: bool,
}

/// Account-summary tags shown for the selected account, all in its base currency.
const ACCOUNT_SUMMARY_VALUE_TAGS: [&str; 4] = [
    AccountSummaryTags::NET_LIQUIDATION,
    AccountSummaryTags::TOTAL_CASH_VALUE,
    AccountSummaryTags::AVAILABLE_FUNDS,
    AccountSummaryTags::GROSS_POSITION_VALUE,
];
/// Ledger rows for the account's USD balance, which carry the USD exchange rate.
///
/// The rate converts base-currency summary values into dollars without a
/// third-party FX source; see [`usd_per_base_unit`] for how it is read.
const USD_LEDGER_TAG: &str = "$LEDGER:USD";
/// Ledger tag carrying the exchange rate between the ledger and base currencies.
const EXCHANGE_RATE_TAG: &str = "ExchangeRate";
/// Prefix TWS/IB Gateway 10.47 and later put on per-currency ledger tags when
/// "Prepend '$LEDGER-' prefix to per-currency account values" is on (the
/// default for new installs), turning `ExchangeRate` into
/// `$LEDGER-ExchangeRate`. Account-level tags keep their names.
const LEDGER_TAG_PREFIX: &str = "$LEDGER-";

/// Names a per-currency ledger tag the same whichever way the gateway's
/// `$LEDGER-` prefix setting is set.
///
/// Only ledger rows go through this: an account-level tag such as
/// `NetLiquidation` must match exactly, so a prefixed per-currency row can
/// never stand in for it.
fn ledger_tag_name(tag: &str) -> &str {
    tag.strip_prefix(LEDGER_TAG_PREFIX).unwrap_or(tag)
}
/// Every tag requested by one account-summary subscription: the tracked value
/// tags followed by the USD ledger.
///
/// Built from [`ACCOUNT_SUMMARY_VALUE_TAGS`] so a newly tracked value is always
/// requested.
const ACCOUNT_SUMMARY_REQUEST_TAGS: [&str; ACCOUNT_SUMMARY_VALUE_TAGS.len() + 1] = {
    let mut tags = [USD_LEDGER_TAG; ACCOUNT_SUMMARY_VALUE_TAGS.len() + 1];
    let mut index = 0;
    while index < ACCOUNT_SUMMARY_VALUE_TAGS.len() {
        tags[index] = ACCOUNT_SUMMARY_VALUE_TAGS[index];
        index += 1;
    }
    tags
};

/// Converts the USD ledger's `ExchangeRate` into US dollars per base-currency unit.
///
/// IBKR documents a ledger's `ExchangeRate` as the rate of that ledger's
/// currency to the account's base currency, and its own ledger examples price
/// in base units (a USD-base account shows EUR at 1.155135 and AUD at
/// 0.650378). The USD ledger's rate is therefore base-currency units per one
/// US dollar, and dollars per base unit is its reciprocal. Not yet observed on
/// a live non-USD-base account; the owner's paper account is USD-base, where
/// every rate is 1.
///
/// Returns `None` for a zero, negative or non-finite rate.
fn usd_per_base_unit(usd_ledger_rate: f64) -> Option<f64> {
    let rate = 1.0 / usd_ledger_rate;
    (usd_ledger_rate > 0.0 && rate.is_finite()).then_some(rate)
}

/// One account-summary amount in the currency IBKR labelled it with.
#[derive(Clone, Debug, PartialEq)]
struct NativeAmount {
    amount: f64,
    currency: String,
}

/// Latest provider values for one account, accumulated across summary rows.
///
/// IBKR sends one row per tag, so a coherent summary is assembled here and
/// converted to USD only when a snapshot is taken.
#[derive(Clone, Debug, Default, PartialEq)]
struct AccountSummaryState {
    values: HashMap<&'static str, NativeAmount>,
    /// Raw positive `ExchangeRate` of the USD ledger, read by [`usd_per_base_unit`].
    usd_ledger_rate: Option<f64>,
}

impl AccountSummaryState {
    /// Records one provider row for `account_ref`.
    ///
    /// Returns whether a tracked value or the USD exchange rate changed. Rows
    /// for other accounts and untracked tags are ignored; a blank value clears
    /// the tag because IBKR no longer reports it. A malformed secondary value
    /// (total cash, available funds, gross position or the USD rate) is logged
    /// and treated as not reported, so it cannot hide a valid net liquidation.
    ///
    /// # Errors
    ///
    /// Returns a contract error only for a malformed or non-finite net
    /// liquidation, the one value the balance cannot do without.
    fn apply(&mut self, summary: &AccountSummary, account_ref: &str) -> Result<bool, GatewayError> {
        if summary.account != account_ref {
            return Ok(false);
        }
        if ledger_tag_name(&summary.tag) == EXCHANGE_RATE_TAG {
            if !summary.currency.trim().eq_ignore_ascii_case("USD") {
                return Ok(false);
            }
            let rate = parse_secondary_summary_amount(&summary.value, EXCHANGE_RATE_TAG)
                .filter(|rate| *rate > 0.0);
            let changed = self.usd_ledger_rate != rate;
            self.usd_ledger_rate = rate;
            return Ok(changed);
        }
        let Some(tag) = ACCOUNT_SUMMARY_VALUE_TAGS
            .into_iter()
            .find(|tag| *tag == summary.tag)
        else {
            return Ok(false);
        };
        let amount = if tag == AccountSummaryTags::NET_LIQUIDATION {
            parse_summary_amount(&summary.value, tag)?
        } else {
            parse_secondary_summary_amount(&summary.value, tag)
        };
        let next = amount.map(|amount| NativeAmount {
            amount,
            currency: summary.currency.trim().to_string(),
        });
        let changed = self.values.get(tag) != next.as_ref();
        match next {
            Some(value) => {
                self.values.insert(tag, value);
            }
            None => {
                self.values.remove(tag);
            }
        }
        Ok(changed)
    }

    /// Whether IBKR has reported net liquidation for the account.
    fn has_net_liquidation(&self) -> bool {
        self.values
            .contains_key(AccountSummaryTags::NET_LIQUIDATION)
    }

    /// Currency of the non-ledger summary rows, which IBKR reports in base currency.
    fn base_currency(&self) -> Option<&str> {
        ACCOUNT_SUMMARY_VALUE_TAGS
            .into_iter()
            .find_map(|tag| self.values.get(tag))
            .map(|value| value.currency.as_str())
    }

    /// US dollars per one unit of `currency`, when IBKR provided enough data.
    fn usd_per_unit(&self, currency: &str) -> Option<f64> {
        let (major, minor_units) = currency_major_unit(currency)?;
        let major_rate = if major == "USD" {
            1.0
        } else {
            let base = self.base_currency().and_then(currency_major_unit)?;
            if base.0 != major {
                return None;
            }
            usd_per_base_unit(self.usd_ledger_rate?)?
        };
        let rate = major_rate / minor_units;
        rate.is_finite().then_some(rate)
    }

    fn usd_value(&self, tag: &str) -> Option<f64> {
        let value = self.values.get(tag)?;
        let usd = value.amount * self.usd_per_unit(&value.currency)?;
        usd.is_finite().then_some(usd)
    }

    /// USD-normalized view of the accumulated values.
    fn summary(&self) -> AccountValueSummary {
        let net_liquidation = self.values.get(AccountSummaryTags::NET_LIQUIDATION);
        AccountValueSummary {
            net_liquidation_usd: self.usd_value(AccountSummaryTags::NET_LIQUIDATION),
            total_cash_usd: self.usd_value(AccountSummaryTags::TOTAL_CASH_VALUE),
            available_funds_usd: self.usd_value(AccountSummaryTags::AVAILABLE_FUNDS),
            gross_position_value_usd: self.usd_value(AccountSummaryTags::GROSS_POSITION_VALUE),
            native_currency: net_liquidation.map(|value| value.currency.clone()),
            native_net_liquidation: net_liquidation.map(|value| value.amount),
            fx_rate_to_usd: net_liquidation.and_then(|value| self.usd_per_unit(&value.currency)),
        }
    }

    /// Builds the balance update for `account_ref`.
    ///
    /// # Errors
    ///
    /// Returns an availability error when net liquidation is missing or IBKR
    /// did not provide the exchange rate needed to express it in USD. The
    /// value is never relabelled as dollars.
    fn balance(&self, account_ref: &str) -> Result<AccountBalance, GatewayError> {
        let summary = self.summary();
        let Some(balance_usd) = summary.net_liquidation_usd else {
            return Err(GatewayError::Unavailable(match &summary.native_currency {
                Some(currency) => format!(
                    "IBKR reported net liquidation in {currency} without a USD exchange rate"
                ),
                None => "IBKR did not report net liquidation for the account".to_string(),
            }));
        };
        Ok(AccountBalance {
            account_ref: account_ref.to_string(),
            balance_usd,
            summary,
        })
    }
}

/// Parses one account-summary amount; a blank value means "not reported".
fn parse_summary_amount(value: &str, tag: &str) -> Result<Option<f64>, GatewayError> {
    let value = value.trim();
    if value.is_empty() {
        return Ok(None);
    }
    let amount = value.parse::<f64>().map_err(|_| {
        GatewayError::Contract(format!("IBKR returned an invalid {tag} account value"))
    })?;
    if !amount.is_finite() {
        return Err(GatewayError::Contract(format!(
            "IBKR returned a non-finite {tag} account value"
        )));
    }
    Ok(Some(amount))
}

/// Parses an account-summary amount the balance can do without.
///
/// A malformed or non-finite value is logged and read as "not reported", so
/// the update still carries net liquidation with this field set to `null`.
fn parse_secondary_summary_amount(value: &str, tag: &str) -> Option<f64> {
    parse_summary_amount(value, tag).unwrap_or_else(|error| {
        tracing::warn!(tag, %error, "ignoring a malformed IBKR account summary value");
        None
    })
}

/// Resolves a currency label to its ISO major currency and minor units per major unit.
///
/// Pence-quoted sterling (`GBp`, also written `GBX`) is 1/100 of `GBP`. The
/// lowercase `p` is significant, so plain `GBP` is never read as pence. Returns
/// `None` for labels that are not three-letter currency codes.
fn currency_major_unit(currency: &str) -> Option<(String, f64)> {
    let currency = currency.trim();
    if currency == "GBp" || currency.eq_ignore_ascii_case("GBX") {
        return Some(("GBP".to_string(), 100.0));
    }
    (currency.len() == 3
        && currency
            .chars()
            .all(|character| character.is_ascii_alphabetic()))
    .then(|| (currency.to_ascii_uppercase(), 1.0))
}

/// Long-lived provider subscription for one IBKR stock candle series.
pub struct CandleSubscription {
    subscription: Subscription<HistoricalBarUpdate>,
    updates: FilterDataStream<Subscription<HistoricalBarUpdate>>,
    timeframe_ms: i64,
    initial_snapshot_seen: bool,
}

/// Long-lived provider market-data subscription for one IBKR stock quote.
///
/// Each [`StockQuoteSubscription::next`] call yields the full normalized quote
/// after any change to its prices, last-trade time, or market-data mode.
pub struct StockQuoteSubscription {
    line: IbkrStockQuoteLine,
    notices: NoticeStream,
    state: StockQuoteState,
    /// The contract's tick rules once a lookup succeeds; dropping the
    /// subscription stops the lookup.
    price_rules: QuoteRules,
}

/// The contract's tick rules as a background lookup reports them.
struct QuoteRules {
    receiver: tokio::sync::watch::Receiver<Option<Vec<PriceIncrementRule>>>,
    /// Whether the lookup task can still report.
    open: bool,
}

impl QuoteRules {
    fn new(receiver: tokio::sync::watch::Receiver<Option<Vec<PriceIncrementRule>>>) -> Self {
        Self {
            receiver,
            open: true,
        }
    }

    /// Rules that never arrive, for tests of the tick wait alone.
    #[cfg(test)]
    fn closed() -> Self {
        let (_sender, receiver) = tokio::sync::watch::channel(None);
        Self {
            receiver,
            open: false,
        }
    }
}

impl StockQuoteSubscription {
    /// Cancels the provider market-data request and waits for the request to be sent.
    pub async fn cancel(&self) {
        self.line.subscription.cancel().await;
    }

    /// Waits for the next changed quote.
    ///
    /// IBKR controls the update cadence and only sends changed fields, so a
    /// quiet market yields nothing while its quote ages. A live quote that
    /// receives no provider item at all for [`LIVE_QUOTE_IDLE_TIMEOUT`] is
    /// resubscribed in place on the same connection and contract; that is
    /// not an outage, so nothing is returned for it unless the resubscribe
    /// itself fails.
    ///
    /// # Errors
    ///
    /// Returns a provider error when IBKR ends the subscription with an
    /// error, for example a missing contract definition. Returns an
    /// availability error when TWS reports lost market-data connectivity
    /// (1100, 1101, 1300). Returns the resubscribe's error when an idle line
    /// cannot be replaced. The caller then reports the outage and
    /// resubscribes from scratch.
    ///
    /// When the contract's tick rules arrive, the current quote is returned
    /// again with them, so a quiet market does not wait for its next tick.
    pub async fn next(&mut self) -> Option<Result<StockQuote, GatewayError>> {
        next_stock_quote_resubscribing_idle(
            &mut self.line,
            self.notices.stream(),
            &mut self.state,
            &mut self.price_rules,
            current_time_ms,
            LIVE_QUOTE_IDLE_TIMEOUT,
        )
        .await
    }
}

/// One stock quote market-data line that can be replaced in place.
///
/// The seam lets the idle-resubscribe policy be tested without a Gateway.
trait StockQuoteLine {
    /// Provider items of the current line.
    type Ticks: Stream<Item = Result<SubscriptionItem<TickTypes>, ibapi::Error>> + Unpin;

    /// Current line's provider items.
    fn ticks(&mut self) -> &mut Self::Ticks;

    /// Cancels the current line and opens a new one for the same contract.
    fn resubscribe(&mut self) -> impl Future<Output = Result<(), GatewayError>> + Send;
}

/// Live IBKR market-data line for one resolved stock contract.
struct IbkrStockQuoteLine {
    client: Arc<Client>,
    contract: Contract,
    market_data_mode: Arc<Mutex<()>>,
    subscription: Subscription<TickTypes>,
}

impl StockQuoteLine for IbkrStockQuoteLine {
    type Ticks = Subscription<TickTypes>;

    fn ticks(&mut self) -> &mut Self::Ticks {
        &mut self.subscription
    }

    /// Reuses the connection and resolved contract, so an idle line costs
    /// one cancel and one market-data request rather than a reconnect and a
    /// contract lookup.
    ///
    /// The mode lock is taken before the old line is cancelled, so the stock
    /// is without a line only for the cancel, the mode switch and one
    /// request, never while another request holds the mode.
    async fn resubscribe(&mut self) -> Result<(), GatewayError> {
        let held = lock_market_data_mode(&self.market_data_mode).await?;
        self.subscription.cancel().await;
        self.subscription = subscribe_stock_quote_line(&self.client, &self.contract, &held).await?;
        Ok(())
    }
}

/// Proof that the caller holds the gateway's market-data mode lock.
type MarketDataModeGuard<'a> = tokio::sync::MutexGuard<'a, ()>;

/// Waits for the connection-wide market-data mode lock.
///
/// Every holder keeps it only for one mode switch and one market-data
/// request, together bounded by `QUOTE_TIMEOUT`, so a wait longer than that
/// means the provider socket has stalled.
///
/// # Errors
///
/// Returns an availability error when the lock is not free within
/// `QUOTE_TIMEOUT`.
async fn lock_market_data_mode(
    market_data_mode: &Mutex<()>,
) -> Result<MarketDataModeGuard<'_>, GatewayError> {
    tokio::time::timeout(QUOTE_TIMEOUT, market_data_mode.lock())
        .await
        .map_err(|_| GatewayError::Unavailable("IBKR market data mode timed out".to_string()))
}

/// Switches IBKR's connection-wide market-data mode, then sends one request.
///
/// TWS applies the mode that is current when a `reqMktData` arrives, so the
/// caller's lock only has to cover the switch and the send. The caller
/// releases it as soon as this returns and reads the ticks afterwards. Both
/// steps share one `QUOTE_TIMEOUT`, which bounds how long any holder keeps
/// the lock.
///
/// # Errors
///
/// Returns an availability error when the two steps do not finish within
/// `QUOTE_TIMEOUT`, or the provider error either step failed with.
async fn send_market_data_request<T>(
    client: &Client,
    _held: &MarketDataModeGuard<'_>,
    mode: MarketDataType,
    label: &str,
    request: impl Future<Output = Result<T, ibapi::Error>>,
) -> Result<T, GatewayError> {
    provider_timeout(QUOTE_TIMEOUT, label, async {
        client
            .switch_market_data_type(mode)
            .await
            .map_err(|error| provider_error("market data mode", error))?;
        request.await.map_err(|error| provider_error(label, error))
    })
    .await
}

/// Opens one market-data line for a stock contract under the held mode lock.
///
/// Delayed-frozen asks for real-time data first and falls back to delayed or
/// frozen values when the account lacks a live subscription or the market is
/// closed.
async fn subscribe_stock_quote_line(
    client: &Client,
    contract: &Contract,
    held: &MarketDataModeGuard<'_>,
) -> Result<Subscription<TickTypes>, GatewayError> {
    send_market_data_request(
        client,
        held,
        MarketDataType::DelayedFrozen,
        "stock quote",
        client.market_data(contract).subscribe(),
    )
    .await
}

impl CandleSubscription {
    /// Cancels the provider historical-data stream and waits for the request to be sent.
    pub async fn cancel(&self) {
        self.subscription.cancel().await;
    }

    /// Waits for the latest initial candle or the next provider-paced update.
    ///
    /// # Errors
    ///
    /// Returns a contract error for malformed bars or an availability error
    /// when the provider stream fails.
    pub async fn next(&mut self) -> Option<Result<Candle, GatewayError>> {
        loop {
            let update = match self.updates.next().await? {
                Ok(update) => update,
                Err(error) => return Some(Err(provider_error("historical data stream", error))),
            };
            match update {
                HistoricalBarUpdate::Historical(history) => {
                    self.initial_snapshot_seen = true;
                    let Some(mut candle) =
                        history.bars.iter().filter_map(normalize_bar).next_back()
                    else {
                        return Some(Err(GatewayError::Contract(
                            "IBKR returned no usable streaming candles".to_string(),
                        )));
                    };
                    candle.is_snapshot = Some(true);
                    candle.confirmed = candle.timestamp.saturating_add(self.timeframe_ms)
                        <= Utc::now().timestamp_millis();
                    return Some(Ok(candle));
                }
                HistoricalBarUpdate::Update(bar) => {
                    let Some(mut candle) = normalize_bar(&bar) else {
                        return Some(Err(GatewayError::Contract(
                            "IBKR returned an invalid streaming candle".to_string(),
                        )));
                    };
                    candle.is_snapshot = Some(false);
                    candle.confirmed = false;
                    return Some(Ok(candle));
                }
                HistoricalBarUpdate::End { .. } => {
                    if !self.initial_snapshot_seen {
                        continue;
                    }
                }
            }
        }
    }
}

impl AccountBalanceSubscription {
    /// Cancels the provider account-summary request and waits for the request to be sent.
    pub async fn cancel(&self) {
        self.subscription.cancel().await;
    }

    /// Waits for the initial account summary, then for each changed value.
    ///
    /// The first call collects rows until IBKR's initial snapshot-end marker,
    /// so the four summary values and the USD exchange rate arrive as one
    /// coherent update. Later calls return whenever a tracked value or the
    /// exchange rate changes; IBKR controls that cadence.
    ///
    /// # Errors
    ///
    /// Returns a contract error for a malformed net liquidation, an
    /// availability error when net liquidation cannot be expressed in USD or
    /// the initial snapshot times out, or a classified provider error when the
    /// stream fails.
    pub async fn next(&mut self) -> Option<Result<AccountBalance, GatewayError>> {
        if self.initial_snapshot_seen {
            return match apply_until_account_change(
                &mut self.summaries,
                &mut self.state,
                &self.account_ref,
            )
            .await
            {
                Ok(true) => Some(self.state.balance(&self.account_ref)),
                Ok(false) => None,
                Err(error) => Some(Err(error)),
            };
        }
        let initial = tokio::time::timeout(
            READ_TIMEOUT,
            apply_until_snapshot_end(&mut self.summaries, &mut self.state, &self.account_ref),
        )
        .await;
        match initial {
            Ok(Ok(true)) => {}
            Ok(Ok(false)) => return None,
            Ok(Err(error)) => return Some(Err(error)),
            // A snapshot whose end marker is late still has usable rows.
            Err(_) if self.state.has_net_liquidation() => {}
            Err(_) => {
                return Some(Err(GatewayError::Unavailable(
                    "IBKR initial account summary timed out".to_string(),
                )));
            }
        }
        self.initial_snapshot_seen = true;
        Some(self.state.balance(&self.account_ref))
    }
}

/// Provider state resolved before an order crosses the submission boundary.
struct PreparedOrder {
    client: Arc<Client>,
    account: String,
    details: ContractDetails,
    order_id: i32,
    /// Tick-aligned bracket children, empty for a plain order.
    protection: Vec<AlignedProtectionLeg>,
    /// Where a reduce-only exit goes beside the position's other exits.
    exit: Option<NewExitPlan>,
}

/// One bracket child after its price was moved onto the contract's increment.
#[derive(Clone, Debug, PartialEq)]
struct AlignedProtectionLeg {
    kind: ProtectionLegKind,
    price: f64,
    customer_order_id: String,
}

impl Debug for Gateway {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Gateway")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl Gateway {
    /// Construct one stateful TWS API client facade.
    ///
    /// # Errors
    ///
    /// Returns an error when configuration is empty or unsafe.
    pub fn new(config: GatewayConfig) -> Result<Self, GatewayError> {
        GatewayConfig::new(config.addresses.clone(), config.client_id)?;
        Ok(Self {
            config,
            active: Mutex::new(None),
            operation: Arc::new(Mutex::new(())),
            market_data_mode: Arc::new(Mutex::new(())),
            pending_orders: Arc::new(Mutex::new(HashMap::new())),
            snapshots: Arc::new(Mutex::new(())),
            market_rules: Arc::new(MarketRuleCache::default()),
            server_link: Mutex::new(None),
            order_events: Mutex::new(None),
        })
    }

    /// Detect a running IB Gateway without selecting an account or retaining a client.
    ///
    /// # Errors
    ///
    /// Returns an availability error when no configured Gateway accepts a TWS
    /// API connection or exposes at least one managed account.
    pub async fn detect(&self) -> Result<GatewayDetection, GatewayError> {
        let _operation = self.operation.lock().await;
        let (client, accounts) = self
            .connect_new_for_account(None, self.config.detection_client_id())
            .await?;
        client.disconnect().await;
        if accounts.is_empty() {
            return Err(GatewayError::Unavailable(
                "IB Gateway returned no managed accounts".to_string(),
            ));
        }
        Ok(GatewayDetection::Connected {
            message: "IB Gateway TWS API is connected".to_string(),
            accounts,
        })
    }

    /// Inspect managed accounts and select the requested accessible account.
    ///
    /// # Errors
    ///
    /// Returns an error for transport failures or ambiguous/missing account
    /// selection.
    pub async fn probe(&self, account_ref: Option<String>) -> Result<ProbeResponse, GatewayError> {
        let _operation = self.operation.lock().await;
        let (_, accounts) = self.connect_for_account(account_ref.as_deref()).await?;
        let account = select_account(&accounts, account_ref.as_deref())?;
        Ok(ProbeResponse {
            version: PROTOCOL_VERSION,
            provider: "ibkr",
            ok: true,
            status: "connected".to_string(),
            message: "IB Gateway TWS API is connected".to_string(),
            account_ref: Some(account),
        })
    }

    /// Fetch an authoritative position and working-order snapshot for one account.
    ///
    /// The read is retried once only when connection setup fails before a
    /// provider subscription is established. The returned rows retain IBKR
    /// contract and order identifiers so later local actions do not guess from
    /// display symbols.
    ///
    /// # Errors
    ///
    /// Returns an error for unavailable sessions, ambiguous account selection,
    /// rejected subscriptions, timeouts, or malformed provider values.
    pub async fn account_state(
        &self,
        request: AccountStateRequest,
    ) -> Result<AccountStateResponse, GatewayError> {
        if request.version != PROTOCOL_VERSION {
            return Err(GatewayError::Input(format!(
                "account-state protocol version must be {PROTOCOL_VERSION}"
            )));
        }
        // Provider snapshots can each wait for READ_TIMEOUT. Keep those reads
        // outside the mutation mutex so polling cannot starve an order or cancel.
        for attempt in 0..2 {
            match self
                .account_state_once(request.account_ref.as_deref())
                .await
            {
                Err(error @ GatewayError::Unavailable(_)) if attempt == 0 => {
                    // A failed poll may reset and retry only when no mutation is
                    // queued or active; otherwise leave reconciliation untouched.
                    let Ok(_operation) = self.operation.try_lock() else {
                        return Err(error);
                    };
                    self.reset_connection().await;
                }
                result => return result,
            }
        }
        unreachable!("bounded account-state retry returns from every attempt")
    }

    async fn account_state_once(
        &self,
        requested_account_ref: Option<&str>,
    ) -> Result<AccountStateResponse, GatewayError> {
        let (client, accounts) = self.connect_for_account(requested_account_ref).await?;
        let account_ref = select_account(&accounts, requested_account_ref)?;
        let positions = {
            let _snapshots = self.snapshots.lock().await;
            collect_account_positions(&client, &account_ref).await?
        };
        let orders = {
            let _snapshots = self.snapshots.lock().await;
            collect_account_orders(&client, &account_ref).await?
        };
        let server_link = self
            .server_link
            .lock()
            .await
            .as_ref()
            .and_then(|watch| watch.link_of(&client))
            .unwrap_or(ServerLink::Connected);
        Ok(AccountStateResponse {
            version: PROTOCOL_VERSION,
            provider: "ibkr",
            account_ref,
            positions,
            orders,
            server_link,
        })
    }

    /// Cancel one local-session IBKR order without retrying after the send boundary.
    ///
    /// IBKR cancels every order of an OCA group when one is cancelled, so an
    /// order whose group holds other exits of a held position has those
    /// siblings re-placed into a fresh group first
    /// (`linked_exits::LinkedSession::cancel_keeping_siblings`); cancelling a take profit
    /// keeps its stop loss.
    ///
    /// # Errors
    ///
    /// Returns an error only when validation or connection setup fails before
    /// cancellation is sent. Any uncertain post-send outcome is returned as an
    /// `indeterminate` response for explicit reconciliation.
    pub async fn cancel_order(
        &self,
        request: CancelOrderRequest,
    ) -> Result<OrderResponse, GatewayError> {
        validate_cancel_order_request(&request)?;
        let requested_account = request
            .account_ref
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned)
            .ok_or_else(|| {
                GatewayError::Input("accountRef is required for cancellation".to_string())
            })?;
        let operation = tokio::time::timeout(
            ORDER_OPERATION_LOCK_TIMEOUT,
            Arc::clone(&self.operation).lock_owned(),
        )
        .await
        .map_err(|_| {
            GatewayError::Unavailable(
                "IBKR cancellation could not start before its deadline".to_string(),
            )
        })?;
        let order_id = request.order_id;
        let client_id = self.config.client_id;
        // Connection, snapshots and the next order id share one deadline, so
        // a relinking cancellation still answers inside the caller's 30 s.
        let prepared = tokio::time::timeout(ORDER_PREPARATION_TIMEOUT, async {
            let (client, accounts) = self
                .connect_for_account(Some(requested_account.as_str()))
                .await?;
            let account = select_account(&accounts, Some(requested_account.as_str()))?;
            let all_open = {
                let _snapshots = self.snapshots.lock().await;
                collect_order_snapshot(&client, OrderSnapshot::AllOpen).await?
            };
            let working = working_orders(&all_open, &account);
            let Some(target) = own_working_order(&working, client_id, order_id).cloned() else {
                return Ok::<_, GatewayError>((client, account, None));
            };
            if target.order.oca_group.is_empty() {
                // A loose order cancels alone; nothing else to read.
                return Ok((
                    client,
                    account,
                    Some((
                        CancelPlan::Plain {
                            cascades: Vec::new(),
                        },
                        None,
                    )),
                ));
            }
            let position = {
                let _snapshots = self.snapshots.lock().await;
                read_position(&client, &account, target.contract_id).await?
            };
            let book = ExitBook::new(working, target.contract_id, position, client_id);
            let plan = match plan_cancel(&book, order_id, &fresh_oca_group()) {
                Ok(plan) => plan,
                Err(reason) => {
                    return Ok((client, account, Some((CancelPlan::Refused(reason), None))));
                }
            };
            let relink_from = match &plan {
                CancelPlan::Relink { .. } => Some((
                    book,
                    client
                        .next_valid_order_id()
                        .await
                        .map_err(|error| provider_error("next order id", error))?,
                )),
                _ => None,
            };
            Ok((client, account, Some((plan, relink_from))))
        })
        .await
        .map_err(|_| {
            GatewayError::Unavailable(
                "IBKR cancellation preparation timed out before submission".to_string(),
            )
        })??;
        let (client, account, Some((plan, relink_from))) = prepared else {
            let (_, account, _) = prepared;
            return Ok(cancel_response(
                &account,
                order_id,
                false,
                "rejected",
                "IBKR order is not open for the selected account",
            ));
        };
        match (plan, relink_from) {
            (CancelPlan::Refused(reason), _) => Ok(cancel_response(
                &account,
                order_id,
                false,
                "rejected",
                &format!("IBKR order {order_id} was not cancelled: {reason}"),
            )),
            (CancelPlan::Relink { group, relink }, Some((book, first_order_id))) => {
                let snapshots = Arc::clone(&self.snapshots);
                // The task keeps the operation lock and finishes re-placing
                // and cancelling even if the caller stops waiting.
                let task = tokio::spawn(async move {
                    let _operation = operation;
                    let session = LinkedSession::new(
                        &client,
                        &account,
                        client_id,
                        &snapshots,
                        linked_exits::LINKED_EXIT_BUDGET,
                    );
                    session
                        .cancel_keeping_siblings(&book, order_id, &group, &relink, first_order_id)
                        .await
                });
                Ok(task.await.unwrap_or_else(|error| {
                    cancel_indeterminate_response(
                        requested_account,
                        order_id,
                        &format!("the cancellation task failed: {error}"),
                    )
                }))
            }
            (CancelPlan::Relink { .. }, None) => Ok(cancel_response(
                &account,
                order_id,
                false,
                "rejected",
                &format!(
                    "IBKR order {order_id} was not cancelled: its linked exits could not be prepared for re-placing"
                ),
            )),
            (CancelPlan::Plain { cascades }, _) => {
                let deadline = tokio::time::Instant::now()
                    + ORDER_SUBMISSION_TIMEOUT
                    + ORDER_ACKNOWLEDGEMENT_TIMEOUT;
                let mut response = cancel_outcome_response(
                    &account,
                    order_id,
                    cancel_and_confirm(&client, order_id, deadline).await,
                );
                drop(operation);
                if response.ok && !cascades.is_empty() {
                    response.message = format!(
                        "{}. IBKR also cancelled the linked orders {} (OCA group)",
                        response.message,
                        linked_exits::order_names(cascades.iter().map(|order| order.order_id)),
                    );
                }
                Ok(response)
            }
        }
    }

    /// Subscribes to the selected account's USD-normalized account summary.
    ///
    /// One provider request carries net liquidation, total cash, available
    /// funds, gross position value, and the USD ledger whose exchange rate
    /// converts a non-USD base currency to dollars.
    ///
    /// # Errors
    ///
    /// Returns an error for transport failures, ambiguous or inaccessible
    /// account selection, or a rejected account-summary subscription.
    pub async fn account_balance_stream(
        &self,
        account_ref: Option<String>,
    ) -> Result<AccountBalanceSubscription, GatewayError> {
        let _operation = self.operation.lock().await;
        for attempt in 0..2 {
            match self
                .account_balance_stream_once(account_ref.as_deref())
                .await
            {
                Err(GatewayError::Unavailable(_)) if attempt == 0 => {
                    self.reset_connection().await;
                }
                result => return result,
            }
        }
        unreachable!("bounded read retry returns from every attempt")
    }

    async fn account_balance_stream_once(
        &self,
        account_ref: Option<&str>,
    ) -> Result<AccountBalanceSubscription, GatewayError> {
        let (client, accounts) = self.connect_for_account(account_ref).await?;
        let account_ref = select_account(&accounts, account_ref)?;
        let subscription = client
            .account_summary(
                &AccountGroup("All".to_string()),
                &ACCOUNT_SUMMARY_REQUEST_TAGS,
            )
            .await
            .map_err(|error| provider_error("account summary", error))?;
        let summaries = subscription.clone().filter_data();
        Ok(AccountBalanceSubscription {
            account_ref,
            subscription,
            summaries,
            state: AccountSummaryState::default(),
            initial_snapshot_seen: false,
        })
    }

    /// Fetch and normalize bounded historical candles.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid inputs, unavailable sessions, unsupported
    /// timeframes, or unusable provider data.
    pub async fn candles(&self, request: CandleRequest) -> Result<Vec<Candle>, GatewayError> {
        let _operation = self.operation.lock().await;
        for attempt in 0..2 {
            match self.candles_once(&request).await {
                Err(GatewayError::Unavailable(_)) if attempt == 0 => {
                    self.reset_connection().await;
                }
                result => return result,
            }
        }
        unreachable!("bounded read retry returns from every attempt")
    }

    async fn candles_once(&self, request: &CandleRequest) -> Result<Vec<Candle>, GatewayError> {
        let symbol = normalized_symbol(&request.symbol)?;
        let (client, _) = self.connect_for_account(None).await?;
        let contract = resolve_stock_contract(&client, &symbol, request.conid).await?;
        let contract = route_stock_market_data_contract(contract);
        let bar_size = history_bar(&request.timeframe)?;
        let limit = request.limit.unwrap_or(500).clamp(1, MAX_HISTORY_POINTS);
        let fallback_ms = history_bar_millis(&request.timeframe)?.saturating_mul(limit as i64);
        let duration = history_duration(request.from, request.to, fallback_ms);
        let end = history_end(request.to)?;
        let response = tokio::time::timeout(
            READ_TIMEOUT,
            client
                .historical_data(&contract, bar_size)
                .what_to_show(HistoricalWhatToShow::Trades)
                .trading_hours(TradingHours::Extended)
                .duration(duration)
                .ending(end)
                .fetch(),
        )
        .await
        .map_err(|_| GatewayError::Unavailable("IBKR historical data timed out".to_string()))?;
        let response = match response {
            Ok(response) => response,
            Err(error) if is_empty_historical_data_error(&error) => return Ok(Vec::new()),
            Err(error) => return Err(provider_error("historical data", error)),
        };
        let mut candles = response
            .bars
            .iter()
            .filter_map(normalize_bar)
            .filter(|candle| request.from.is_none_or(|from| candle.timestamp >= from))
            .filter(|candle| request.to.is_none_or(|to| candle.timestamp < to))
            .collect::<Vec<_>>();
        candles.sort_by_key(|candle| candle.timestamp);
        if candles.len() > limit {
            candles.drain(..candles.len() - limit);
        }
        if candles.is_empty() {
            return Err(GatewayError::Contract(
                "IBKR returned no usable historical candles".to_string(),
            ));
        }
        Ok(candles)
    }

    /// Subscribes to provider-paced updates for one IBKR stock candle series.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid inputs, unavailable sessions, unsupported
    /// timeframes, or a rejected historical-data stream.
    pub async fn candle_stream(
        &self,
        request: CandleRequest,
    ) -> Result<CandleSubscription, GatewayError> {
        let _operation = self.operation.lock().await;
        for attempt in 0..2 {
            match self.candle_stream_once(&request).await {
                Err(GatewayError::Unavailable(_)) if attempt == 0 => {
                    self.reset_connection().await;
                }
                result => return result,
            }
        }
        unreachable!("bounded read retry returns from every attempt")
    }

    async fn candle_stream_once(
        &self,
        request: &CandleRequest,
    ) -> Result<CandleSubscription, GatewayError> {
        let symbol = normalized_symbol(&request.symbol)?;
        let (client, _) = self.connect_for_account(None).await?;
        let contract = resolve_stock_contract(&client, &symbol, request.conid).await?;
        let contract = route_stock_market_data_contract(contract);
        let bar_size = history_bar(&request.timeframe)?;
        let timeframe_ms = history_bar_millis(&request.timeframe)?;
        let limit = request.limit.unwrap_or(500).clamp(1, MAX_HISTORY_POINTS);
        let duration = history_duration(None, None, timeframe_ms.saturating_mul(limit as i64));
        let subscription = client
            .historical_data(&contract, bar_size)
            .what_to_show(HistoricalWhatToShow::Trades)
            .trading_hours(TradingHours::Extended)
            .duration(duration)
            .stream()
            .await
            .map_err(|error| provider_error("historical data stream", error))?;
        Ok(CandleSubscription {
            updates: subscription.clone().filter_data(),
            subscription,
            timeframe_ms,
            initial_snapshot_seen: false,
        })
    }

    /// Subscribes to streaming top-of-book quotes for one IBKR stock contract.
    ///
    /// The connection requests IBKR's delayed-frozen market-data mode, which
    /// returns live data to entitled accounts and falls back to delayed or
    /// frozen data otherwise. Each quote reports the mode IBKR actually used.
    ///
    /// Setup never takes the order mutex and never resets the shared
    /// connection: a slow contract lookup must not delay an order, and one
    /// quote's timeout must not tear down every other stream. A dead client
    /// is replaced by the next connection lookup instead, and the caller's
    /// stream loop retries with backoff.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid inputs, unavailable sessions, unresolved
    /// contracts, or a rejected market-data request.
    pub async fn stock_quote_stream(
        &self,
        request: StockQuoteRequest,
    ) -> Result<StockQuoteSubscription, GatewayError> {
        let symbol = stock_quote_symbol(&request.symbol, request.conid)?;
        let (client, _) = self.connect_for_account(None).await?;
        let contract = provider_timeout(
            READ_TIMEOUT,
            "stock contract details",
            resolve_stock_contract(&client, &symbol, request.conid),
        )
        .await?;
        // The contract's tick rules, so the web app prices limits on the grid
        // order validation uses. Looked up beside the quote and retried until
        // it succeeds; until then quotes carry an empty list, which the web
        // app refuses to price a protected order from.
        let price_rules_lookup = {
            let client = Arc::clone(&client);
            let contract = contract.clone();
            let market_rules = Arc::clone(&self.market_rules);
            spawn_retrying_price_rules(
                move || {
                    let client = Arc::clone(&client);
                    let contract = contract.clone();
                    let market_rules = Arc::clone(&market_rules);
                    async move {
                        provider_timeout(READ_TIMEOUT, "stock price increments", async {
                            let details = first_contract_details(&client, &contract).await?;
                            price_rules(&client, &market_rules, &details).await
                        })
                        .await
                    }
                },
                price_rules_retry_delay,
            )
        };
        let contract = route_stock_quote_contract(contract);
        let notices = client
            .notice_stream()
            .map_err(|error| provider_error("notice stream", error))?;
        let subscription = {
            let held = lock_market_data_mode(&self.market_data_mode).await?;
            subscribe_stock_quote_line(&client, &contract, &held).await?
        };
        let canonical_symbol = contract.symbol.to_string();
        let state = StockQuoteState::new(
            if canonical_symbol.is_empty() {
                symbol
            } else {
                canonical_symbol
            },
            i64::from(contract.contract_id),
            contract.currency.to_string(),
            Vec::new(),
        );
        Ok(StockQuoteSubscription {
            price_rules: QuoteRules::new(price_rules_lookup),
            line: IbkrStockQuoteLine {
                client,
                contract,
                market_data_mode: Arc::clone(&self.market_data_mode),
                subscription,
            },
            notices,
            state,
        })
    }

    /// Subscribes to order status changes and executions of one account.
    ///
    /// Every consumer of the current TWS client shares one provider
    /// order-update subscription (TWS allows one per client); the first
    /// consumer starts it and a changed connection replaces it. The
    /// subscription replays the account's latest statuses and recent
    /// executions before live events. Like quotes, setup never takes the
    /// order mutex and never resets the shared connection.
    ///
    /// # Errors
    ///
    /// Returns an input error for ambiguous or inaccessible account
    /// selection, and an availability error when IB Gateway is unreachable
    /// or refuses the order-update subscription.
    pub async fn order_event_stream(
        &self,
        account_ref: Option<String>,
    ) -> Result<OrderEventSubscription, GatewayError> {
        let (client, accounts) = self.connect_for_account(account_ref.as_deref()).await?;
        let account = select_account(&accounts, account_ref.as_deref())?;
        let mut hub = self.order_events.lock().await;
        if let Some(current) = hub.as_ref()
            && current.serves(&client)
        {
            return Ok(current.subscribe(account));
        }
        // Drop a hub for an old connection before subscribing again.
        *hub = None;
        let started = OrderEventHub::start(&client).await?;
        let subscription = started.subscribe(account);
        *hub = Some(started);
        Ok(subscription)
    }

    /// Fetch a bounded near-the-money option chain with executable quote metadata.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid inputs, unavailable sessions, missing
    /// contracts, or unusable provider responses.
    pub async fn option_chain(
        &self,
        request: OptionChainRequest,
    ) -> Result<OptionChainResponse, GatewayError> {
        let _operation = self.operation.lock().await;
        for attempt in 0..2 {
            match self.option_chain_once(&request).await {
                Err(GatewayError::Unavailable(_)) if attempt == 0 => {
                    self.reset_connection().await;
                }
                result => return result,
            }
        }
        unreachable!("bounded read retry returns from every attempt")
    }

    async fn option_chain_once(
        &self,
        request: &OptionChainRequest,
    ) -> Result<OptionChainResponse, GatewayError> {
        let symbol = normalized_symbol(&request.symbol)?;
        if !(1..=MAX_STRIKES_PER_SIDE).contains(&request.max_strikes_per_side) {
            return Err(GatewayError::Input(format!(
                "maxStrikesPerSide must be between 1 and {MAX_STRIKES_PER_SIDE}"
            )));
        }
        let (client, _) = self.connect_for_account(None).await?;
        let mode = &*self.market_data_mode;
        let market_rules = &*self.market_rules;
        let underlying = resolve_stock_contract(&client, &symbol, None).await?;
        let underlying_quote = snapshot_quote(&client, mode, &underlying, false).await?;
        let underlying_price = underlying_quote.reference_price().ok_or_else(|| {
            GatewayError::Contract("IBKR omitted the underlying price".to_string())
        })?;
        let Some(chain) = load_smart_option_chain(&client, &underlying, &symbol).await? else {
            return Ok(OptionChainResponse {
                version: PROTOCOL_VERSION,
                underlying_symbol: symbol,
                underlying_price,
                contracts: Vec::new(),
            });
        };
        let expiry = nearest_expiry(&chain.expirations)?;
        let strikes = select_strikes(
            &chain.strikes,
            underlying_price,
            request.max_strikes_per_side,
        );
        let specs = strikes.into_iter().flat_map(|strike| {
            [OptionRight::Call, OptionRight::Put]
                .into_iter()
                .map(move |right| (strike, right))
        });
        let trading_class = chain.trading_class.clone();
        let multiplier = chain.multiplier.clone();
        let contracts = stream::iter(specs)
            .map(|(strike, right)| {
                let client = Arc::clone(&client);
                let symbol = symbol.clone();
                let expiry = expiry.clone();
                let trading_class = trading_class.clone();
                let multiplier = multiplier.clone();
                async move {
                    resolve_option_contract(
                        &client,
                        &symbol,
                        &expiry,
                        strike,
                        right,
                        &trading_class,
                        &multiplier,
                    )
                    .await
                }
            })
            .buffer_unordered(OPTION_CONTRACT_RESOLUTION_CONCURRENCY)
            .filter_map(|result| async move { skip_expected_option_contract_miss(result) })
            .try_collect::<Vec<_>>()
            .await?;
        let contracts = require_option_contracts(contracts, &symbol)?;
        let normalized = stream::iter(contracts)
            .map(|details| {
                let client = Arc::clone(&client);
                async move { normalize_option_contract(&client, mode, market_rules, details).await }
            })
            .buffer_unordered(OPTION_QUOTE_CONCURRENCY)
            .try_collect::<Vec<_>>()
            .await?;
        let mut normalized = require_option_contracts(normalized, &symbol)?;
        normalized.sort_by(|left, right| {
            left.expiry
                .cmp(&right.expiry)
                .then_with(|| left.strike.total_cmp(&right.strike))
                .then_with(|| left.right.cmp(&right.right))
        });
        Ok(OptionChainResponse {
            version: PROTOCOL_VERSION,
            underlying_symbol: symbol,
            underlying_price,
            contracts: normalized,
        })
    }

    /// Quote one listed option by contract id.
    ///
    /// The web app prices a capped option order, such as a position close
    /// carrying the venue slippage cap, from this quote, so the quote is read
    /// for each request and never cached. It has the option chain's fields
    /// and tradability rules (`normalize_option_contract`). The response
    /// names the underlying, whose option session applies.
    ///
    /// Like the chain, the read holds the operation lock and retries once on
    /// a fresh connection when the first attempt finds IB Gateway
    /// unavailable. The whole read, the wait for the lock included, is
    /// bounded by `OPTION_QUOTE_BUDGET`, so a slow Gateway never holds the
    /// lock past the web app's own deadline and blocks the order after it.
    ///
    /// # Arguments
    ///
    /// * `request` - Versioned request naming the option's IBKR conid.
    ///
    /// # Returns
    ///
    /// The option's definition, quote and tick rules, and its underlying.
    ///
    /// # Errors
    ///
    /// Returns an input error for a conid that is not a positive 32-bit id,
    /// a contract error when the conid is not a listed option, and an
    /// availability or provider error when IB Gateway cannot quote it in
    /// time.
    pub async fn option_quote(
        &self,
        request: OptionQuoteRequest,
    ) -> Result<OptionQuoteResponse, GatewayError> {
        if request.conid <= 0 || i32::try_from(request.conid).is_err() {
            return Err(GatewayError::Input(
                "option quote conid must be a positive IBKR contract id".to_string(),
            ));
        }
        tokio::time::timeout(OPTION_QUOTE_BUDGET, async {
            let _operation = self.operation.lock().await;
            for attempt in 0..2 {
                match self.option_quote_once(&request).await {
                    Err(GatewayError::Unavailable(_)) if attempt == 0 => {
                        self.reset_connection().await;
                    }
                    result => return result,
                }
            }
            unreachable!("bounded read retry returns from every attempt")
        })
        .await
        .map_err(|_| GatewayError::Unavailable("IBKR option quote timed out".to_string()))?
    }

    async fn option_quote_once(
        &self,
        request: &OptionQuoteRequest,
    ) -> Result<OptionQuoteResponse, GatewayError> {
        let (client, _) = self.connect_for_account(None).await?;
        let details = provider_timeout(
            READ_TIMEOUT,
            "option contract details",
            option_contract_details(&client, request.conid),
        )
        .await?;
        let underlying_symbol = details.contract.symbol.to_string();
        let contract =
            normalize_option_contract(&client, &self.market_data_mode, &self.market_rules, details)
                .await?;
        Ok(OptionQuoteResponse {
            version: PROTOCOL_VERSION,
            underlying_symbol,
            contract,
        })
    }

    /// Validate and submit one order without retrying an ambiguous side effect.
    ///
    /// A `reduceOnly` order is a linked exit: it is clamped to the position
    /// and placed in one IBKR OCA group with the position's other exits
    /// (`gateway::linked_exits`), so together they never close more than the
    /// position.
    ///
    /// # Errors
    ///
    /// Returns an error only before submission begins. Every uncertain state
    /// after the provider submission boundary becomes an `indeterminate`
    /// response and leaves the account blocked for manual reconciliation.
    pub async fn order(&self, request: OrderRequest) -> Result<OrderResponse, GatewayError> {
        validate_order_request(&request)?;
        let requested_account = request
            .account_ref
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned)
            .ok_or_else(|| GatewayError::Input("accountRef is required for orders".to_string()))?;
        let operation = tokio::time::timeout(
            ORDER_OPERATION_LOCK_TIMEOUT,
            Arc::clone(&self.operation).lock_owned(),
        )
        .await
        .map_err(|_| {
            GatewayError::Unavailable(
                "IBKR order operation could not start before its deadline".to_string(),
            )
        })?;
        if let Some(customer_order_id) = self
            .pending_orders
            .lock()
            .await
            .get(&requested_account)
            .cloned()
        {
            // The new request is not submitted. The response carries the earlier
            // unresolved reference so the caller can reconcile that order.
            return Ok(indeterminate_response(
                requested_account,
                Some(customer_order_id),
                "a prior IBKR submission for this account has an unresolved outcome and this order was not sent; reconcile the earlier order reference first",
            ));
        }
        let PreparedOrder {
            client,
            account,
            details,
            order_id,
            protection,
            exit,
        } = tokio::time::timeout(
            ORDER_PREPARATION_TIMEOUT,
            self.prepare_order(&request, &requested_account),
        )
        .await
        .map_err(|_| {
            GatewayError::Unavailable(
                "IBKR order preparation timed out before submission".to_string(),
            )
        })??;
        let customer_order_id = request.customer_order_id.clone();
        if let Some(plan) = exit {
            self.pending_orders
                .lock()
                .await
                .insert(account.clone(), customer_order_id.clone());
            let pending_orders = Arc::clone(&self.pending_orders);
            let snapshots = Arc::clone(&self.snapshots);
            let client_id = self.config.client_id;
            let task_account = account.clone();
            // Placing copies, the exit and cancelling originals must run to the
            // end once started, so the work owns the operation lock in its own
            // task and survives a caller that stops waiting.
            let task = tokio::spawn(async move {
                let _operation = operation;
                let session = LinkedSession::new(
                    &client,
                    &task_account,
                    client_id,
                    &snapshots,
                    linked_exits::LINKED_EXIT_BUDGET,
                );
                let outcome = session
                    .submit_exit(&details.contract, &request, plan, order_id)
                    .await;
                if outcome.status != "indeterminate" {
                    pending_orders.lock().await.remove(&task_account);
                }
                outcome
            });
            return Ok(task.await.unwrap_or_else(|error| {
                indeterminate_response(
                    account,
                    Some(customer_order_id),
                    &format!("the linked exit task failed: {error}"),
                )
            }));
        }
        if !protection.is_empty() {
            let plan = build_bracket(&request, &account, order_id, &protection);
            self.pending_orders
                .lock()
                .await
                .insert(account.clone(), customer_order_id.clone());
            let outcome =
                submit_bracket(&client, &details.contract, plan, &account, &self.snapshots).await;
            if outcome.status != "indeterminate" {
                self.pending_orders.lock().await.remove(&account);
            }
            return Ok(outcome);
        }
        let provider_order = build_order(&request, &account, &customer_order_id);
        self.pending_orders
            .lock()
            .await
            .insert(account.clone(), customer_order_id.clone());
        let events = match tokio::time::timeout(
            ORDER_SUBMISSION_TIMEOUT,
            client.place_order(order_id, &details.contract, &provider_order),
        )
        .await
        {
            Ok(Ok(events)) => events,
            Ok(Err(error)) => {
                return Ok(indeterminate_response(
                    account,
                    Some(customer_order_id),
                    &format!("the order submission transport failed: {error}"),
                ));
            }
            Err(_) => {
                return Ok(indeterminate_response(
                    account,
                    Some(customer_order_id),
                    "the order submission call timed out",
                ));
            }
        };
        let outcome = single_order_outcome(
            events,
            SubmittedOrder {
                order_id,
                account: &account,
                customer_order_id: &customer_order_id,
            },
            ORDER_OUTCOME_DEADLINES,
            || read_provider_orders_serialized(&client, &self.snapshots),
        )
        .await;
        if outcome.status != "indeterminate" {
            self.pending_orders.lock().await.remove(&account);
        }
        Ok(outcome)
    }

    /// Reconcile one earlier submission by its exact caller-generated order reference.
    ///
    /// The account's current open orders from every API client, today's
    /// completed orders, and today's executions are searched for an order whose
    /// IBKR `order_ref` equals `customerOrderId`. Only a positive match clears
    /// this process's unresolved-order block for the account. An empty or
    /// non-matching snapshot returns `not_found` and clears nothing, because a
    /// lost acknowledgement can precede the order's appearance in IBKR state.
    ///
    /// With `release`, IBKR is not queried: the operator attests that the order
    /// was checked manually, and only a block for this exact reference is
    /// dropped.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid input, unavailable sessions, account
    /// selection failures, provider read failures, or the reconciliation
    /// deadline. Errors never clear a block.
    pub async fn reconcile_order(
        &self,
        request: ReconcileOrderRequest,
    ) -> Result<ReconcileOrderResponse, GatewayError> {
        validate_reconcile_order_request(&request)?;
        let account_ref = request.account_ref.trim().to_string();
        let customer_order_id = request.customer_order_id;
        if request.release {
            let released = self
                .release_pending_order(&account_ref, &customer_order_id)
                .await;
            tracing::warn!(
                account = %account_ref,
                customer_order_id = %customer_order_id,
                released_local_block = released,
                "operator released an unresolved IBKR order reference without a provider match"
            );
            return Ok(ReconcileOrderResponse {
                version: PROTOCOL_VERSION,
                provider: "ibkr".to_string(),
                account_ref,
                customer_order_id,
                state: ReconcileOrderState::Released,
                status: None,
                order_id: None,
                message: "The unresolved IBKR order reference was released by the operator"
                    .to_string(),
            });
        }
        // Reads stay outside the mutation mutex, like account-state polling, so
        // a slow reconciliation cannot starve orders for other accounts.
        let (account, found) = tokio::time::timeout(ORDER_RECONCILIATION_TIMEOUT, async {
            let (client, accounts) = self.connect_for_account(Some(&account_ref)).await?;
            let account = select_account(&accounts, Some(&account_ref))?;
            let found = {
                let _snapshots = self.snapshots.lock().await;
                find_order_reference(&client, &account, &customer_order_id).await?
            };
            Ok::<_, GatewayError>((account, found))
        })
        .await
        .map_err(|_| {
            GatewayError::Unavailable("IBKR order reconciliation timed out".to_string())
        })??;
        let Some(found) = found else {
            return Ok(ReconcileOrderResponse {
                version: PROTOCOL_VERSION,
                provider: "ibkr".to_string(),
                account_ref: account,
                customer_order_id,
                state: ReconcileOrderState::NotFound,
                status: None,
                order_id: None,
                message: "IBKR open orders, completed orders, and executions do not contain this order reference yet".to_string(),
            });
        };
        self.release_pending_order(&account, &customer_order_id)
            .await;
        let source = match found.state {
            ReconcileOrderState::Open => "open orders",
            ReconcileOrderState::Completed => "completed orders",
            _ => "executions",
        };
        Ok(ReconcileOrderResponse {
            version: PROTOCOL_VERSION,
            provider: "ibkr".to_string(),
            account_ref: account,
            customer_order_id,
            state: found.state,
            status: Some(found.status.to_string()),
            order_id: found.order_id.map(|order_id| order_id.to_string()),
            message: match found.order_id {
                Some(order_id) => {
                    format!("IBKR {source} contain this order reference ({order_id})")
                }
                None => format!("IBKR {source} contain this order reference"),
            },
        })
    }

    /// Drop the account's unresolved-order block only when it is for this reference.
    async fn release_pending_order(&self, account_ref: &str, customer_order_id: &str) -> bool {
        let mut pending = self.pending_orders.lock().await;
        if pending
            .get(account_ref)
            .is_some_and(|pending_id| pending_id == customer_order_id)
        {
            pending.remove(account_ref);
            return true;
        }
        false
    }

    /// Resolve all fallible provider state before order submission may begin.
    async fn prepare_order(
        &self,
        request: &OrderRequest,
        requested_account: &str,
    ) -> Result<PreparedOrder, GatewayError> {
        let (client, accounts) = self.connect_for_account(Some(requested_account)).await?;
        let account = select_account(&accounts, Some(requested_account))?;
        let contract = match request.instrument_kind {
            InstrumentKind::Stock => {
                let conid = request.instrument.parse::<i64>().ok();
                resolve_stock_contract(&client, &request.instrument, conid).await?
            }
            InstrumentKind::Option => {
                let conid = request
                    .instrument
                    .parse::<i64>()
                    .ok()
                    .filter(|value| *value > 0)
                    .ok_or_else(|| {
                        GatewayError::Input("option instrument must be an IBKR conid".to_string())
                    })?;
                resolve_contract_id(&client, option_order_query(conid)?).await?
            }
        };
        let details = first_contract_details(&client, &contract).await?;
        // A stop-limit or limit-if-touched order carries both prices, and
        // IBKR rejects either one off the contract's price increment.
        let prices = [
            request.limit_price.map(|price| ("limitPrice", price)),
            request.stop_price.map(|price| ("stopPrice", price)),
        ];
        let mut protection = Vec::new();
        if prices.iter().any(Option::is_some) || request.protection.is_some() {
            let rules = price_rules(&client, &self.market_rules, &details).await?;
            for (field, price) in prices.into_iter().flatten() {
                validate_price_increment(field, price, &rules)?;
            }
            if let Some(requested) = &request.protection {
                protection = align_protection(request, requested, &rules)?;
            }
        }
        let exit = if request.reduce_only {
            Some(
                self.plan_linked_exit(&client, &account, &details, request)
                    .await?,
            )
        } else {
            None
        };
        let order_id = client
            .next_valid_order_id()
            .await
            .map_err(|error| provider_error("next order id", error))?;
        Ok(PreparedOrder {
            client,
            account,
            details,
            order_id,
            protection,
            exit,
        })
    }

    /// Read the position and its exits, and decide where a reduce-only exit goes.
    ///
    /// # Errors
    ///
    /// Returns an input error naming the reason when the exit is refused, or
    /// the provider failure of either snapshot. Nothing has been sent yet.
    async fn plan_linked_exit(
        &self,
        client: &Arc<Client>,
        account: &str,
        details: &ContractDetails,
        request: &OrderRequest,
    ) -> Result<NewExitPlan, GatewayError> {
        let contract_id = details.contract.contract_id;
        let position = {
            let _snapshots = self.snapshots.lock().await;
            read_position(client, account, contract_id).await?
        };
        let open = {
            let _snapshots = self.snapshots.lock().await;
            collect_order_snapshot(client, OrderSnapshot::AllOpen).await?
        };
        let book = ExitBook::new(
            working_orders(&open, account),
            contract_id,
            position,
            self.config.client_id,
        );
        plan_new_exit(
            &book,
            request.side == "sell",
            request.quantity.unwrap_or_default(),
            &format!("oca-{}", request.customer_order_id),
        )
        .map_err(|reason| GatewayError::Input(format!("IBKR linked exit refused: {reason}")))
    }

    async fn connect_for_account(
        &self,
        requested_account: Option<&str>,
    ) -> Result<(Arc<Client>, Vec<String>), GatewayError> {
        let mut active = self.active.lock().await;
        if let Some(current) = active.as_ref()
            && current.is_connected()
            && let Ok(accounts) = load_managed_accounts(current).await
            && account_matches(&accounts, requested_account)
        {
            return Ok((Arc::clone(current), accounts));
        }
        if let Some(current) = active.take() {
            current.disconnect().await;
        }
        let (client, accounts) = self
            .connect_new_for_account(requested_account, self.config.client_id)
            .await?;
        *active = Some(Arc::clone(&client));
        // Each connection gets its own watch, so a reconnect starts clean.
        *self.server_link.lock().await = ServerLinkWatch::start(&client);
        Ok((client, accounts))
    }

    /// Open one validated TWS client with the supplied id without retaining it.
    async fn connect_new_for_account(
        &self,
        requested_account: Option<&str>,
        client_id: i32,
    ) -> Result<(Arc<Client>, Vec<String>), GatewayError> {
        let mut failures = Vec::new();
        for address in &self.config.addresses {
            let target = address.to_string();
            let connected =
                tokio::time::timeout(CONNECT_TIMEOUT, Client::connect(&target, client_id)).await;
            let client = match connected {
                Ok(Ok(client)) => Arc::new(client),
                Ok(Err(error)) => {
                    failures.push(format!("{address}: {error}"));
                    continue;
                }
                Err(_) => {
                    failures.push(format!("{address}: connection timed out"));
                    continue;
                }
            };
            let accounts = match load_managed_accounts(&client).await {
                Ok(accounts) if !accounts.is_empty() => accounts,
                Ok(_) => {
                    failures.push(format!("{address}: no managed accounts"));
                    client.disconnect().await;
                    continue;
                }
                Err(error) => {
                    failures.push(format!("{address}: {error}"));
                    client.disconnect().await;
                    continue;
                }
            };
            if !account_matches(&accounts, requested_account) {
                failures.push(format!("{address}: requested account is unavailable"));
                client.disconnect().await;
                continue;
            }
            return Ok((client, accounts));
        }
        let suffix = if failures.is_empty() {
            String::new()
        } else {
            format!(" ({})", failures.join("; "))
        };
        Err(GatewayError::Unavailable(format!(
            "IB Gateway is unavailable on the configured TWS API sockets{suffix}"
        )))
    }

    async fn reset_connection(&self) {
        if let Some(client) = self.active.lock().await.take() {
            client.disconnect().await;
        }
    }
}

async fn provider_timeout<T, F>(
    duration: Duration,
    label: &str,
    future: F,
) -> Result<T, GatewayError>
where
    F: Future<Output = Result<T, GatewayError>>,
{
    tokio::time::timeout(duration, future)
        .await
        .map_err(|_| GatewayError::Unavailable(format!("IBKR {label} timed out")))?
}

async fn load_managed_accounts(client: &Arc<Client>) -> Result<Vec<String>, GatewayError> {
    provider_timeout(MANAGED_ACCOUNTS_TIMEOUT, "managed accounts", async {
        client
            .managed_accounts()
            .await
            .map_err(|error| provider_error("managed accounts", error))
    })
    .await
}

/// Applies provider rows until IBKR marks the end of the initial snapshot.
///
/// Returns `Ok(true)` at the end marker and `Ok(false)` when the stream closes
/// first.
async fn apply_until_snapshot_end<S>(
    summaries: &mut S,
    state: &mut AccountSummaryState,
    account_ref: &str,
) -> Result<bool, GatewayError>
where
    S: Stream<Item = Result<AccountSummaryResult, ibapi::Error>> + Unpin,
{
    while let Some(result) = summaries.next().await {
        match result.map_err(|error| provider_error("account summary", error))? {
            AccountSummaryResult::Summary(summary) => {
                state.apply(&summary, account_ref)?;
            }
            AccountSummaryResult::End => return Ok(true),
        }
    }
    Ok(false)
}

/// Applies provider rows until a tracked value for `account_ref` changes.
///
/// Returns `Ok(true)` after a change and `Ok(false)` when the stream closes.
/// Later end markers are ignored because the subscription stays open.
async fn apply_until_account_change<S>(
    summaries: &mut S,
    state: &mut AccountSummaryState,
    account_ref: &str,
) -> Result<bool, GatewayError>
where
    S: Stream<Item = Result<AccountSummaryResult, ibapi::Error>> + Unpin,
{
    while let Some(result) = summaries.next().await {
        match result.map_err(|error| provider_error("account summary", error))? {
            AccountSummaryResult::Summary(summary) => {
                if state.apply(&summary, account_ref)? {
                    return Ok(true);
                }
            }
            AccountSummaryResult::End => {}
        }
    }
    Ok(false)
}

fn account_matches(accounts: &[String], requested: Option<&str>) -> bool {
    requested
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .is_none_or(|requested| accounts.iter().any(|account| account == requested))
}

fn select_account(accounts: &[String], requested: Option<&str>) -> Result<String, GatewayError> {
    match requested.map(str::trim).filter(|value| !value.is_empty()) {
        Some(requested) if accounts.iter().any(|account| account == requested) => {
            Ok(requested.to_string())
        }
        Some(_) => Err(GatewayError::Input(
            "configured account is not returned by IBKR".to_string(),
        )),
        None if accounts.len() == 1 => Ok(accounts[0].clone()),
        None => Err(GatewayError::Input(
            "select one of the accounts returned by IBKR".to_string(),
        )),
    }
}

fn normalized_symbol(value: &str) -> Result<String, GatewayError> {
    let symbol = value.trim().to_ascii_uppercase();
    if symbol.is_empty()
        || symbol.len() > 32
        || !symbol
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || matches!(character, '.' | '-'))
    {
        return Err(GatewayError::Input(
            "symbol must be a supported IBKR ticker".to_string(),
        ));
    }
    Ok(symbol)
}

async fn resolve_stock_contract(
    client: &Arc<Client>,
    symbol: &str,
    conid: Option<i64>,
) -> Result<Contract, GatewayError> {
    if let Some(conid) = conid.filter(|value| *value > 0) {
        return resolve_contract_id(client, stock_conid_query(conid)?).await;
    }
    let query = Contract::stock(normalized_symbol(symbol)?).build();
    let details = client
        .contract_details(&query)
        .await
        .map_err(|error| contract_lookup_error("stock contract details", error))?;
    details
        .into_iter()
        .filter(|details| details.contract.security_type == SecurityType::Stock)
        .max_by_key(|details| {
            i32::from(details.contract.currency.as_str() == "USD")
                + i32::from(details.contract.exchange.as_str() == "SMART")
        })
        .map(|details| details.contract)
        .ok_or_else(|| GatewayError::Contract(format!("IBKR did not resolve stock {symbol}")))
}

/// Replaces IBKR's non-queryable stock display venue with SMART routing.
///
/// Contract detail responses may identify `BEST` as their display exchange,
/// but historical requests reject that venue. The contract identifier and
/// primary exchange continue to disambiguate the listed security, while other
/// directly queryable exchanges remain unchanged.
fn route_stock_market_data_contract(mut contract: Contract) -> Contract {
    if contract.exchange.as_str() == "BEST" {
        contract.exchange = "SMART".into();
    }
    contract
}

/// Validates the caller's symbol for a quote request.
///
/// A symbol-only request must be a searchable ticker. With a conid the
/// symbol only labels the quote until IBKR returns the canonical one, so
/// IBKR position symbols such as `BRK B` are accepted.
fn stock_quote_symbol(value: &str, conid: Option<i64>) -> Result<String, GatewayError> {
    if conid.is_none_or(|conid| conid <= 0) {
        return normalized_symbol(value);
    }
    let symbol = value.trim().to_ascii_uppercase();
    if symbol.is_empty()
        || symbol.len() > 32
        || symbol.chars().any(|character| character.is_control())
    {
        return Err(GatewayError::Input(
            "symbol must be a supported IBKR ticker".to_string(),
        ));
    }
    Ok(symbol)
}

/// Routes a resolved stock contract's quote request through SMART.
///
/// The conid and primary exchange still identify the listing, while SMART
/// works with IBKR's standard consolidated market-data bundles instead of
/// requiring a direct-exchange subscription.
fn route_stock_quote_contract(mut contract: Contract) -> Contract {
    contract.exchange = "SMART".into();
    contract
}

fn current_time_ms() -> i64 {
    Utc::now().timestamp_millis()
}

/// Accumulates IBKR ticks for one stock contract into a normalized quote.
#[derive(Debug)]
struct StockQuoteState {
    symbol: String,
    conid: i64,
    currency: String,
    bid: Option<f64>,
    ask: Option<f64>,
    last: Option<f64>,
    previous_close: Option<f64>,
    reported_type: Option<MarketDataType>,
    delayed_ticks: bool,
    updated_at_ms: i64,
    last_trade_at_ms: Option<i64>,
    price_increment_rules: Vec<PriceIncrementRule>,
}

/// Price fields whose change refreshes a quote's age.
type StockQuotePrices = (
    Option<f64>,
    Option<f64>,
    Option<f64>,
    Option<f64>,
    Option<i64>,
);

impl StockQuoteState {
    fn new(
        symbol: String,
        conid: i64,
        currency: String,
        price_increment_rules: Vec<PriceIncrementRule>,
    ) -> Self {
        Self {
            price_increment_rules,
            symbol,
            conid,
            currency,
            bid: None,
            ask: None,
            last: None,
            previous_close: None,
            reported_type: None,
            delayed_ticks: false,
            updated_at_ms: 0,
            last_trade_at_ms: None,
        }
    }

    /// Records the contract's tick rules and reports whether they changed.
    fn set_price_increment_rules(&mut self, rules: Vec<PriceIncrementRule>) -> bool {
        if self.price_increment_rules == rules {
            return false;
        }
        self.price_increment_rules = rules;
        true
    }

    fn prices(&self) -> StockQuotePrices {
        (
            self.bid,
            self.ask,
            self.last,
            self.previous_close,
            self.last_trade_at_ms,
        )
    }

    /// Applies one tick and reports whether a published quote field changed.
    ///
    /// Only a price change refreshes the quote's age. Size-only ticks change
    /// nothing, and a market-data mode change republishes the quote with its
    /// existing age, because the prices themselves are no newer.
    fn apply(&mut self, tick: TickTypes, now_ms: i64) -> bool {
        let prices_before = self.prices();
        let type_before = self.data_type();
        match tick {
            TickTypes::Price(value) => self.apply_price(value.tick_type, value.price),
            TickTypes::PriceSize(value) => self.apply_price(value.price_tick_type, value.price),
            TickTypes::MarketDataType(data_type) => {
                if data_type != MarketDataType::Unknown {
                    self.reported_type = Some(data_type);
                    self.delayed_ticks = false;
                }
            }
            TickTypes::String(value)
                if matches!(
                    value.tick_type,
                    TickType::LastTimestamp | TickType::DelayedLastTimestamp
                ) =>
            {
                if value.tick_type == TickType::DelayedLastTimestamp {
                    self.delayed_ticks = true;
                }
                if let Some(seconds) = value
                    .value
                    .trim()
                    .parse::<i64>()
                    .ok()
                    .filter(|seconds| *seconds > 0)
                {
                    self.last_trade_at_ms = Some(seconds.saturating_mul(1_000));
                }
            }
            _ => {}
        }
        let prices_changed = prices_before != self.prices();
        if prices_changed {
            self.updated_at_ms = now_ms;
        }
        prices_changed || type_before != self.data_type()
    }

    fn apply_price(&mut self, tick_type: TickType, price: f64) {
        let price = stock_quote_price(price);
        match tick_type {
            TickType::Bid => self.bid = price,
            TickType::Ask => self.ask = price,
            TickType::Last => self.last = price.or(self.last),
            TickType::Close => self.previous_close = price.or(self.previous_close),
            TickType::DelayedBid => {
                self.delayed_ticks = true;
                self.bid = price;
            }
            TickType::DelayedAsk => {
                self.delayed_ticks = true;
                self.ask = price;
            }
            TickType::DelayedLast => {
                self.delayed_ticks = true;
                self.last = price.or(self.last);
            }
            TickType::DelayedClose => {
                self.delayed_ticks = true;
                self.previous_close = price.or(self.previous_close);
            }
            _ => {}
        }
    }

    /// Resolves the market-data mode, or none until IBKR has identified it.
    ///
    /// A delayed tick is proof of delayed data even before the mode report.
    /// Undelayed ticks alone are not proof of live data because frozen data
    /// uses the same tick types, so those quotes wait for IBKR's report.
    fn data_type(&self) -> Option<QuoteDataType> {
        let reported = match self.reported_type {
            Some(MarketDataType::Realtime) => Some(QuoteDataType::Live),
            Some(MarketDataType::Frozen) => Some(QuoteDataType::Frozen),
            Some(MarketDataType::Delayed) => Some(QuoteDataType::Delayed),
            Some(MarketDataType::DelayedFrozen) => Some(QuoteDataType::DelayedFrozen),
            Some(MarketDataType::Unknown) | None => None,
        };
        match (reported, self.delayed_ticks) {
            (Some(QuoteDataType::Live), true) => Some(QuoteDataType::Delayed),
            (Some(QuoteDataType::Frozen), true) => Some(QuoteDataType::DelayedFrozen),
            (Some(data_type), _) => Some(data_type),
            (None, true) => Some(QuoteDataType::Delayed),
            (None, false) => None,
        }
    }

    /// Builds the publishable quote, or none until its mode and a price exist.
    ///
    /// Frozen values are replayed from the close, so their age is measured
    /// from the last trade when IBKR reported one rather than from arrival.
    fn quote(&self) -> Option<StockQuote> {
        let data_type = self.data_type()?;
        if self.bid.is_none()
            && self.ask.is_none()
            && self.last.is_none()
            && self.previous_close.is_none()
        {
            return None;
        }
        let updated_at_ms = match (data_type, self.last_trade_at_ms) {
            (QuoteDataType::Frozen | QuoteDataType::DelayedFrozen, Some(last_trade_at_ms)) => {
                self.updated_at_ms.min(last_trade_at_ms)
            }
            _ => self.updated_at_ms,
        };
        Some(StockQuote {
            symbol: self.symbol.clone(),
            conid: self.conid,
            currency: self.currency.clone(),
            bid: self.bid,
            ask: self.ask,
            last: self.last,
            previous_close: self.previous_close,
            data_type,
            updated_at_ms,
            last_trade_at_ms: self.last_trade_at_ms,
            price_increment_rules: self.price_increment_rules.clone(),
        })
    }
}

/// First delay before retrying a failed tick-rule lookup.
const PRICE_RULES_RETRY_INITIAL: Duration = Duration::from_secs(1);
/// Longest delay between tick-rule lookups.
const PRICE_RULES_RETRY_MAX: Duration = Duration::from_secs(60);

/// Delay before tick-rule lookup attempt `attempt` (1 for the first retry):
/// doubling from one second, capped at a minute.
fn price_rules_retry_delay(attempt: u32) -> Duration {
    PRICE_RULES_RETRY_INITIAL
        .saturating_mul(2u32.saturating_pow(attempt.saturating_sub(1).min(16)))
        .min(PRICE_RULES_RETRY_MAX)
}

/// Looks up a contract's tick rules in the background until it succeeds.
///
/// A failed lookup is never recorded as "no rules": the receiver holds none
/// until a lookup succeeds, and the task retries after `delay(attempt)`.
/// It stops after the first success, or once every receiver is dropped.
///
/// # Arguments
///
/// * `lookup` - One lookup attempt.
/// * `delay` - Wait before retry `attempt`, counting from 1.
///
/// # Returns
///
/// A receiver that holds the rules once a lookup succeeds.
fn spawn_retrying_price_rules<F, Fut, D>(
    mut lookup: F,
    delay: D,
) -> tokio::sync::watch::Receiver<Option<Vec<PriceIncrementRule>>>
where
    F: FnMut() -> Fut + Send + 'static,
    Fut: Future<Output = Result<Vec<PriceIncrementRule>, GatewayError>> + Send,
    D: Fn(u32) -> Duration + Send + 'static,
{
    let (sender, receiver) = tokio::sync::watch::channel(None);
    tokio::spawn(async move {
        let mut attempt = 0;
        loop {
            tokio::select! {
                () = sender.closed() => return,
                result = lookup() => match result {
                    Ok(rules) => {
                        let _ = sender.send(Some(rules));
                        return;
                    }
                    Err(error) => {
                        attempt += 1;
                        tracing::debug!(%error, attempt, "IBKR stock tick-rule lookup failed; retrying");
                    }
                },
            }
            tokio::select! {
                () = sender.closed() => return,
                () = tokio::time::sleep(delay(attempt)) => {}
            }
        }
    });
    receiver
}

/// Keeps a finite positive stock price; IBKR reports a missing side as `-1`.
fn stock_quote_price(price: f64) -> Option<f64> {
    (price.is_finite() && price > 0.0).then_some(price)
}

/// Outcome of waiting on one stock quote market-data line.
#[derive(Debug)]
enum StockQuoteWait {
    /// The quote changed.
    Quote(StockQuote),
    /// A live line delivered no provider item within the idle timeout.
    Idle,
    /// The line or the connection failed.
    Failed(GatewayError),
    /// IBKR ended the line.
    Ended,
}

/// Waits for the next changed quote, replacing a live line that went idle.
///
/// An idle line is not an outage: it is resubscribed in place and the wait
/// continues, so consumers see no unavailable update. Only a failed
/// resubscribe ends the wait, with that failure.
async fn next_stock_quote_resubscribing_idle<L, Ns, N>(
    line: &mut L,
    mut notices: Ns,
    state: &mut StockQuoteState,
    rules: &mut QuoteRules,
    mut now_ms: N,
    idle_timeout: Duration,
) -> Option<Result<StockQuote, GatewayError>>
where
    L: StockQuoteLine,
    Ns: Stream<Item = ibapi::Notice> + Unpin,
    N: FnMut() -> i64,
{
    /// What ended one wait.
    enum Woke {
        Rules(Result<(), tokio::sync::watch::error::RecvError>),
        Ticks(StockQuoteWait),
    }
    loop {
        // Only the tick wait races the rules: it is cancel-safe (an applied
        // tick stays in the state, an unread one in the stream). A started
        // resubscribe is never raced, because dropping it after the old line
        // is cancelled would leave the stock without a line.
        let woke = tokio::select! {
            changed = rules.receiver.changed(), if rules.open => Woke::Rules(changed),
            wait = next_stock_quote(line.ticks(), &mut notices, state, &mut now_ms, idle_timeout) => {
                Woke::Ticks(wait)
            }
        };
        match woke {
            Woke::Rules(Err(_)) => rules.open = false,
            Woke::Rules(Ok(())) => {
                let latest = rules.receiver.borrow_and_update().clone();
                // The current quote is sent again with its rules, so a quiet
                // market does not wait for its next tick.
                if let Some(latest) = latest
                    && state.set_price_increment_rules(latest)
                    && let Some(quote) = state.quote()
                {
                    return Some(Ok(quote));
                }
            }
            Woke::Ticks(StockQuoteWait::Quote(quote)) => return Some(Ok(quote)),
            Woke::Ticks(StockQuoteWait::Failed(error)) => return Some(Err(error)),
            Woke::Ticks(StockQuoteWait::Ended) => return None,
            Woke::Ticks(StockQuoteWait::Idle) => {
                if let Err(error) = line.resubscribe().await {
                    return Some(Err(error));
                }
            }
        }
    }
}

/// Waits for the next changed quote on a provider market-data stream.
///
/// `notices` carries the connection's unrouted TWS notices. Connectivity
/// loss ends the wait with an availability error. A live quote that
/// receives no provider item at all within `idle_timeout` ends it as
/// [`StockQuoteWait::Idle`]: IBKR can drop a market-data line without ending
/// its request, which would otherwise leave a stale quote looking current.
///
/// The idle deadline is one timer for the whole wait, pushed back only when
/// this line's own stream yields. Connection-wide notices (farm status, or
/// notices caused by other streams on the shared client) never extend it, so
/// a steady trickle of them cannot hide a line that went silent.
async fn next_stock_quote<S, Ns, N>(
    stream: &mut S,
    mut notices: Ns,
    state: &mut StockQuoteState,
    mut now_ms: N,
    idle_timeout: Duration,
) -> StockQuoteWait
where
    S: Stream<Item = Result<SubscriptionItem<TickTypes>, ibapi::Error>> + Unpin,
    Ns: Stream<Item = ibapi::Notice> + Unpin,
    N: FnMut() -> i64,
{
    let mut notices_open = true;
    let idle = tokio::time::sleep(idle_timeout);
    tokio::pin!(idle);
    loop {
        let watch_idle = state.data_type() == Some(QuoteDataType::Live);
        tokio::select! {
            item = stream.next() => {
                idle.as_mut().reset(tokio::time::Instant::now() + idle_timeout);
                match item {
                Some(Ok(SubscriptionItem::Data(tick))) => {
                    if state.apply(tick, now_ms())
                        && let Some(quote) = state.quote()
                    {
                        return StockQuoteWait::Quote(quote);
                    }
                }
                Some(Ok(SubscriptionItem::Notice(_))) => {}
                Some(Err(error)) => {
                    return StockQuoteWait::Failed(contract_lookup_error("stock quote stream", error));
                }
                None => return StockQuoteWait::Ended,
                }
            },
            notice = notices.next(), if notices_open => match notice {
                Some(notice) if QUOTE_CONNECTIVITY_LOSS_CODES.contains(&notice.code) => {
                    return StockQuoteWait::Failed(GatewayError::Unavailable(format!(
                        "IBKR market data connection was interrupted ({})",
                        notice.code
                    )));
                }
                Some(_) => {}
                None => notices_open = false,
            },
            () = &mut idle, if watch_idle => {
                return StockQuoteWait::Idle;
            }
        }
    }
}

/// Builds a contract-details query for one IBKR contract id and type.
///
/// ibapi sends the query's `security_type` on the wire (`sec_type`), and its
/// default is `Stock` (`STK`), so a query by conid must name the type it
/// expects: an option conid sent as `STK` resolves nothing.
///
/// # Errors
///
/// Returns an input error for a conid that is not a positive 32-bit id.
fn contract_id_query(conid: i64, security_type: SecurityType) -> Result<Contract, GatewayError> {
    let contract_id = i32::try_from(conid)
        .ok()
        .filter(|value| *value > 0)
        .ok_or_else(|| GatewayError::Input("IBKR conid is out of range".to_string()))?;
    Ok(Contract {
        contract_id,
        security_type,
        ..Default::default()
    })
}

/// Query for the listed option an option order names by conid.
fn option_order_query(conid: i64) -> Result<Contract, GatewayError> {
    contract_id_query(conid, SecurityType::Option)
}

/// Query for the listed option the single-option quote names by conid.
fn option_quote_query(conid: i64) -> Result<Contract, GatewayError> {
    contract_id_query(conid, SecurityType::Option)
}

/// Query for the stock a stock order or quote names by conid.
fn stock_conid_query(conid: i64) -> Result<Contract, GatewayError> {
    contract_id_query(conid, SecurityType::Stock)
}

/// Resolves one contract from a by-conid query ([`contract_id_query`]),
/// keeping only details of the query's security type.
///
/// # Errors
///
/// Returns a contract error when IBKR resolves no contract of that type.
async fn resolve_contract_id(
    client: &Arc<Client>,
    query: Contract,
) -> Result<Contract, GatewayError> {
    let details = client
        .contract_details(&query)
        .await
        .map_err(|error| contract_lookup_error("contract details", error))?;
    details
        .into_iter()
        .find(|details| details.contract.security_type == query.security_type)
        .map(|details| details.contract)
        .ok_or_else(|| {
            GatewayError::Contract(format!("IBKR did not resolve conid {}", query.contract_id))
        })
}

/// Looks up one listed option's details by contract id in a single request.
///
/// # Errors
///
/// Returns an input error for an out-of-range conid and a contract error
/// when IBKR resolves no option with that id.
async fn option_contract_details(
    client: &Arc<Client>,
    conid: i64,
) -> Result<ContractDetails, GatewayError> {
    let query = option_quote_query(conid)?;
    let contract_id = query.contract_id;
    client
        .contract_details(&query)
        .await
        .map_err(|error| contract_lookup_error("option contract details", error))?
        .into_iter()
        .find(|details| {
            details.contract.contract_id == contract_id
                && details.contract.security_type == SecurityType::Option
        })
        .ok_or_else(|| GatewayError::Contract(format!("IBKR did not resolve option conid {conid}")))
}

async fn first_contract_details(
    client: &Arc<Client>,
    contract: &Contract,
) -> Result<ContractDetails, GatewayError> {
    client
        .contract_details(contract)
        .await
        .map_err(|error| provider_error("contract details", error))?
        .into_iter()
        .find(|details| details.contract.contract_id == contract.contract_id)
        .ok_or_else(|| GatewayError::Contract("IBKR returned no contract details".to_string()))
}

fn history_bar(value: &str) -> Result<HistoricalBarSize, GatewayError> {
    match value.trim() {
        "1m" => Ok(HistoricalBarSize::Min),
        "2m" => Ok(HistoricalBarSize::Min2),
        "3m" => Ok(HistoricalBarSize::Min3),
        "5m" => Ok(HistoricalBarSize::Min5),
        "10m" => Ok(HistoricalBarSize::Min10),
        "15m" => Ok(HistoricalBarSize::Min15),
        "30m" => Ok(HistoricalBarSize::Min30),
        "1h" => Ok(HistoricalBarSize::Hour),
        "2h" => Ok(HistoricalBarSize::Hour2),
        "3h" => Ok(HistoricalBarSize::Hour3),
        "4h" => Ok(HistoricalBarSize::Hour4),
        "8h" => Ok(HistoricalBarSize::Hour8),
        "1d" => Ok(HistoricalBarSize::Day),
        "1w" => Ok(HistoricalBarSize::Week),
        _ => Err(GatewayError::Input(
            "timeframe is not supported by IBKR history".to_string(),
        )),
    }
}

fn history_bar_millis(value: &str) -> Result<i64, GatewayError> {
    match value.trim() {
        "1m" => Ok(60_000),
        "2m" => Ok(120_000),
        "3m" => Ok(180_000),
        "5m" => Ok(300_000),
        "10m" => Ok(600_000),
        "15m" => Ok(900_000),
        "30m" => Ok(1_800_000),
        "1h" => Ok(3_600_000),
        "2h" => Ok(7_200_000),
        "3h" => Ok(10_800_000),
        "4h" => Ok(14_400_000),
        "8h" => Ok(28_800_000),
        "1d" => Ok(86_400_000),
        "1w" => Ok(604_800_000),
        _ => Err(GatewayError::Input(
            "timeframe is not supported by IBKR history".to_string(),
        )),
    }
}

fn history_duration(from: Option<i64>, to: Option<i64>, fallback_ms: i64) -> HistoricalDuration {
    let millis = match (from, to) {
        (Some(from), Some(to)) if to > from => to - from,
        _ => fallback_ms,
    }
    .max(1_000);
    if millis <= 86_400_000 {
        HistoricalDuration::seconds(
            i32::try_from((millis + 999) / 1_000)
                .unwrap_or(i32::MAX)
                .max(1),
        )
    } else if millis <= 365 * 86_400_000 {
        HistoricalDuration::days(
            i32::try_from((millis + 86_399_999) / 86_400_000)
                .unwrap_or(365)
                .max(1),
        )
    } else {
        HistoricalDuration::years(
            i32::try_from((millis + 31_535_999_999) / 31_536_000_000)
                .unwrap_or(20)
                .clamp(1, 20),
        )
    }
}

fn history_end(to: Option<i64>) -> Result<OffsetDateTime, GatewayError> {
    match to {
        Some(to) => OffsetDateTime::from_unix_timestamp(to / 1_000).map_err(|error| {
            GatewayError::Input(format!("invalid historical end timestamp: {error}"))
        }),
        None => Ok(OffsetDateTime::now_utc()),
    }
}

fn normalize_bar(bar: &ibapi::market_data::historical::Bar) -> Option<Candle> {
    let timestamp = match bar.date {
        BarTimestamp::Date(date) => date.midnight().assume_utc().unix_timestamp() * 1_000,
        BarTimestamp::DateTime(date_time) => date_time.unix_timestamp() * 1_000,
    };
    (timestamp > 0
        && bar.open.is_finite()
        && bar.high.is_finite()
        && bar.low.is_finite()
        && bar.close.is_finite()
        && bar.open > 0.0
        && bar.high >= bar.low
        && bar.low > 0.0
        && bar.close > 0.0)
        .then_some(Candle {
            timestamp,
            open: bar.open,
            high: bar.high,
            low: bar.low,
            close: bar.close,
            volume: bar.volume.max(0.0),
            confirmed: true,
            is_snapshot: None,
        })
}

struct SelectedOptionChain {
    trading_class: String,
    multiplier: String,
    expirations: Vec<String>,
    strikes: Vec<f64>,
}

async fn load_smart_option_chain(
    client: &Arc<Client>,
    underlying: &Contract,
    symbol: &str,
) -> Result<Option<SelectedOptionChain>, GatewayError> {
    let subscription = client
        .option_chain(
            symbol,
            STOCK_OPTION_FUT_FOP_EXCHANGE,
            SecurityType::Stock,
            underlying.contract_id,
        )
        .await
        .map_err(|error| provider_error("option chain parameters", error))?;
    let mut data = subscription.filter_data();
    let chains = tokio::time::timeout(READ_TIMEOUT, async {
        let mut chains = Vec::new();
        while let Some(item) = data.next().await {
            chains.push(item.map_err(|error| provider_error("option chain parameters", error))?);
        }
        Ok::<_, GatewayError>(chains)
    })
    .await
    .map_err(|_| GatewayError::Unavailable("IBKR option chain timed out".to_string()))??;
    Ok(select_smart_option_chain(chains))
}

/// Selects the richest SMART-routable stock option parameters, when available.
fn select_smart_option_chain(chains: Vec<OptionChain>) -> Option<SelectedOptionChain> {
    chains
        .into_iter()
        .filter(|chain| chain.exchange == "SMART")
        .max_by_key(|chain| chain.expirations.len().saturating_mul(chain.strikes.len()))
        .map(|chain| SelectedOptionChain {
            trading_class: chain.trading_class,
            multiplier: chain.multiplier,
            expirations: chain.expirations,
            strikes: chain.strikes,
        })
}

fn nearest_expiry(values: &[String]) -> Result<String, GatewayError> {
    let today = Utc::now().date_naive();
    values
        .iter()
        .filter_map(|value| {
            NaiveDate::parse_from_str(value.trim(), "%Y%m%d")
                .ok()
                .filter(|date| *date >= today)
                .map(|date| (date, value.clone()))
        })
        .min_by_key(|(date, _)| *date)
        .map(|(_, value)| value)
        .ok_or_else(|| {
            GatewayError::Contract("IBKR returned no nonexpired option expiry".to_string())
        })
}

fn select_strikes(values: &[f64], price: f64, per_side: usize) -> Vec<f64> {
    let mut below = values
        .iter()
        .copied()
        .filter(|strike| strike.is_finite() && *strike <= price)
        .collect::<Vec<_>>();
    below.sort_by(|left, right| right.total_cmp(left));
    below.truncate(per_side);
    let mut above = values
        .iter()
        .copied()
        .filter(|strike| strike.is_finite() && *strike > price)
        .collect::<Vec<_>>();
    above.sort_by(f64::total_cmp);
    above.truncate(per_side);
    below.extend(above);
    below.sort_by(f64::total_cmp);
    below.dedup_by(|left, right| left.total_cmp(right).is_eq());
    below
}

async fn resolve_option_contract(
    client: &Arc<Client>,
    symbol: &str,
    expiry: &str,
    strike: f64,
    right: OptionRight,
    trading_class: &str,
    multiplier: &str,
) -> Result<ContractDetails, GatewayError> {
    let mut query = Contract::option(symbol, expiry, strike, right);
    query.trading_class = trading_class.to_string();
    query.multiplier = multiplier.to_string();
    let details = client
        .contract_details(&query)
        .await
        .map_err(|error| contract_lookup_error("option contract details", error))?;
    details
        .into_iter()
        .find(|details| details.contract.exchange.as_str() == "SMART")
        .ok_or_else(|| {
            GatewayError::Contract("IBKR did not resolve an option contract".to_string())
        })
}

fn skip_expected_option_contract_miss<T>(
    result: Result<T, GatewayError>,
) -> Option<Result<T, GatewayError>> {
    match result {
        Err(GatewayError::Contract(_)) => None,
        result => Some(result),
    }
}

fn require_option_contracts<T>(contracts: Vec<T>, symbol: &str) -> Result<Vec<T>, GatewayError> {
    if contracts.is_empty() {
        return Err(GatewayError::Contract(format!(
            "IBKR returned no usable nearby option contracts for {symbol}"
        )));
    }
    Ok(contracts)
}

#[derive(Default)]
struct MarketQuote {
    bid: Option<f64>,
    ask: Option<f64>,
    last: Option<f64>,
    mark: Option<f64>,
    close: Option<f64>,
    bid_size: Option<f64>,
    ask_size: Option<f64>,
    volume: Option<f64>,
    open_interest: Option<f64>,
    delta: Option<f64>,
    delayed: bool,
    partial_entitlement: bool,
    quote_timestamp_ms: i64,
}

impl MarketQuote {
    fn reference_price(&self) -> Option<f64> {
        self.last
            .filter(|value| *value > 0.0)
            .or_else(|| self.mark.filter(|value| *value > 0.0))
            .or_else(|| {
                Some(
                    (self.bid.filter(|value| *value > 0.0)?
                        + self.ask.filter(|value| *value > 0.0)?)
                        / 2.0,
                )
            })
            .or_else(|| self.close.filter(|value| *value > 0.0))
    }
}

/// Collects a stock snapshot or a bounded, cancellable option quote stream.
///
/// The request is sent in delayed mode under `market_data_mode`, which is
/// released before the ticks are collected.
async fn snapshot_quote(
    client: &Arc<Client>,
    market_data_mode: &Mutex<()>,
    contract: &Contract,
    option: bool,
) -> Result<MarketQuote, GatewayError> {
    let builder = client.market_data(contract);
    let builder = if option {
        builder.generic_ticks(&["100", "101"])
    } else {
        builder.snapshot()
    };
    let label = if option {
        "option market data"
    } else {
        "market-data snapshot"
    };
    let mut subscription = {
        let held = lock_market_data_mode(market_data_mode).await?;
        send_market_data_request(
            client,
            &held,
            MarketDataType::Delayed,
            label,
            builder.subscribe(),
        )
        .await?
    };
    let deadline = tokio::time::Instant::now() + QUOTE_TIMEOUT;
    let mut quote = MarketQuote::default();
    loop {
        let item = match tokio::time::timeout_at(deadline, subscription.next()).await {
            Ok(Some(item)) => item,
            Ok(None) => break,
            Err(_) if option => break,
            Err(_) => {
                return Err(GatewayError::Unavailable(
                    "IBKR market-data snapshot timed out".to_string(),
                ));
            }
        };
        match item {
            Ok(SubscriptionItem::Data(TickTypes::SnapshotEnd)) => break,
            Ok(SubscriptionItem::Data(tick)) => apply_tick(&mut quote, tick),
            Ok(SubscriptionItem::Notice(notice)) => {
                if matches!(notice.category(), NoticeCategory::DataAdvisory) {
                    quote.delayed = true;
                } else if option && apply_partial_option_notice(&mut quote, &notice) {
                } else if matches!(notice.category(), NoticeCategory::Error) {
                    return Err(GatewayError::Provider(format!(
                        "IBKR {label} failed: {}",
                        notice.message
                    )));
                }
            }
            Err(ibapi::Error::Notice(notice))
                if option && apply_partial_option_notice(&mut quote, &notice) => {}
            Err(error) => return Err(provider_error(label, error)),
        }
        if option && option_quote_collection_complete(&quote) {
            break;
        }
    }
    if option {
        subscription.cancel().await;
    }
    Ok(quote)
}

/// Records whether an option notice permits only a non-tradable partial quote.
fn apply_partial_option_notice(quote: &mut MarketQuote, notice: &ibapi::Notice) -> bool {
    if notice.code != PARTIAL_OPTION_MARKET_DATA_CODE {
        return false;
    }
    quote.partial_entitlement = true;
    true
}

/// Returns whether an option stream has every field required for its data mode.
fn option_quote_collection_complete(quote: &MarketQuote) -> bool {
    let top_of_book_complete = quote.bid.is_some()
        && quote.ask.is_some()
        && quote.bid_size.is_some()
        && quote.ask_size.is_some()
        && quote.quote_timestamp_ms > 0;
    top_of_book_complete
        && (quote.delayed
            || (quote.volume.is_some() && quote.open_interest.is_some() && quote.delta.is_some()))
}

fn positive(value: f64) -> Option<f64> {
    (value.is_finite() && value >= 0.0).then_some(value)
}

/// Normalizes every supported IB market-data event into one quote accumulator.
fn apply_tick(quote: &mut MarketQuote, tick: TickTypes) {
    match tick {
        TickTypes::Price(value) => apply_price(quote, value.tick_type, value.price),
        TickTypes::Size(value) => apply_size(quote, value.tick_type, value.size),
        TickTypes::PriceSize(value) => {
            apply_price(quote, value.price_tick_type, value.price);
            apply_size(quote, value.size_tick_type, value.size);
        }
        TickTypes::Generic(value) => match value.tick_type {
            TickType::OptionCallVolume | TickType::OptionPutVolume | TickType::Volume => {
                quote.volume = positive(value.value);
            }
            TickType::OptionCallOpenInterest
            | TickType::OptionPutOpenInterest
            | TickType::OpenInterest => {
                quote.open_interest = positive(value.value);
            }
            _ => {}
        },
        TickTypes::OptionComputation(value) => {
            if matches!(
                value.field,
                TickType::ModelOption | TickType::BidOption | TickType::AskOption
            ) {
                quote.delta = value.delta.filter(|delta| (-1.0..=1.0).contains(delta));
                if value.field == TickType::ModelOption {
                    quote.mark = value.option_price.and_then(positive);
                }
            }
            if matches!(
                value.field,
                TickType::DelayedModelOption
                    | TickType::DelayedBidOption
                    | TickType::DelayedAskOption
            ) {
                quote.delayed = true;
            }
        }
        TickTypes::MarketDataType(data_type) => {
            quote.delayed = matches!(
                data_type,
                MarketDataType::Delayed | MarketDataType::DelayedFrozen
            );
        }
        TickTypes::String(value)
            if matches!(
                value.tick_type,
                TickType::LastTimestamp | TickType::DelayedLastTimestamp
            ) =>
        {
            quote.delayed |= value.tick_type == TickType::DelayedLastTimestamp;
            quote.quote_timestamp_ms = value
                .value
                .trim()
                .parse::<i64>()
                .ok()
                .map(|seconds| seconds.saturating_mul(1_000))
                .unwrap_or(0);
        }
        _ => {}
    }
}

/// Applies one standalone or combined price tick to a quote accumulator.
fn apply_price(quote: &mut MarketQuote, tick_type: TickType, price: f64) {
    match tick_type {
        TickType::Bid => quote.bid = positive(price),
        TickType::Ask => quote.ask = positive(price),
        TickType::Last => quote.last = positive(price),
        TickType::MarkPrice => quote.mark = positive(price),
        TickType::Close => quote.close = positive(price),
        TickType::DelayedBid => {
            quote.delayed = true;
            quote.bid = positive(price);
        }
        TickType::DelayedAsk => {
            quote.delayed = true;
            quote.ask = positive(price);
        }
        TickType::DelayedLast => {
            quote.delayed = true;
            quote.last = positive(price);
        }
        TickType::DelayedClose => {
            quote.delayed = true;
            quote.close = positive(price);
        }
        _ => {}
    }
}

/// Applies one standalone or combined size tick to a quote accumulator.
fn apply_size(quote: &mut MarketQuote, tick_type: TickType, size: f64) {
    match tick_type {
        TickType::BidSize => quote.bid_size = positive(size),
        TickType::AskSize => quote.ask_size = positive(size),
        TickType::Volume | TickType::OptionCallVolume | TickType::OptionPutVolume => {
            quote.volume = positive(size);
        }
        TickType::OptionCallOpenInterest
        | TickType::OptionPutOpenInterest
        | TickType::OpenInterest => {
            quote.open_interest = positive(size);
        }
        TickType::DelayedBidSize => {
            quote.delayed = true;
            quote.bid_size = positive(size);
        }
        TickType::DelayedAskSize => {
            quote.delayed = true;
            quote.ask_size = positive(size);
        }
        TickType::DelayedVolume => {
            quote.delayed = true;
            quote.volume = positive(size);
        }
        _ => {}
    }
}

async fn normalize_option_contract(
    client: &Arc<Client>,
    market_data_mode: &Mutex<()>,
    market_rules: &MarketRuleCache,
    details: ContractDetails,
) -> Result<OptionContract, GatewayError> {
    let quote = snapshot_quote(client, market_data_mode, &details.contract, true).await?;
    let price_increment_rules = price_rules(client, market_rules, &details).await?;
    let expiry = NaiveDate::parse_from_str(
        details
            .contract
            .last_trade_date_or_contract_month
            .get(..8)
            .unwrap_or(""),
        "%Y%m%d",
    )
    .map_err(|error| GatewayError::Contract(format!("invalid IBKR option expiry: {error}")))?;
    let right = normalize_option_right(details.contract.right)?;
    let multiplier = details
        .contract
        .multiplier
        .parse::<f64>()
        .map_err(|error| {
            GatewayError::Contract(format!("invalid IBKR option multiplier: {error}"))
        })?;
    let can_trade = option_quote_can_trade(&quote, &price_increment_rules);
    Ok(OptionContract {
        conid: i64::from(details.contract.contract_id),
        local_symbol: details.contract.local_symbol,
        right,
        strike: details.contract.strike,
        expiry: expiry.format("%Y-%m-%d").to_string(),
        multiplier,
        bid: quote.bid,
        ask: quote.ask,
        bid_size: quote.bid_size,
        ask_size: quote.ask_size,
        volume: quote.volume,
        open_interest: quote.open_interest,
        delta: quote.delta,
        mark: quote.mark,
        price_increment_rules: if can_trade {
            price_increment_rules
        } else {
            Vec::new()
        },
        quote_timestamp_ms: quote.quote_timestamp_ms,
        delayed: quote.delayed,
        can_trade,
    })
}

fn normalize_option_right(right: Option<OptionRight>) -> Result<OptionContractRight, GatewayError> {
    match right {
        Some(OptionRight::Call) => Ok(OptionContractRight::Call),
        Some(OptionRight::Put) => Ok(OptionContractRight::Put),
        None | Some(_) => Err(GatewayError::Contract(
            "IBKR option contract returned an unsupported right".to_string(),
        )),
    }
}

fn option_quote_can_trade(
    quote: &MarketQuote,
    price_increment_rules: &[PriceIncrementRule],
) -> bool {
    !quote.delayed
        && !quote.partial_entitlement
        && quote.quote_timestamp_ms > 0
        && quote.bid.is_some_and(|value| value > 0.0)
        && quote.ask.is_some_and(|value| value > 0.0)
        && !price_increment_rules.is_empty()
}

/// IBKR market rules, looked up one at a time and kept for the gateway's life.
///
/// `ibapi` 3.3.0 routes market-rule answers by message type, not request id,
/// and `Client::market_rule` returns the first one to arrive. Two lookups in
/// flight on one client (two quote streams, an option chain's contracts, an
/// order's validation) could each read the other's rule and price a contract
/// on the wrong grid. Every lookup therefore goes through this cache: one
/// request at a time, the answer's rule id checked against the request (a
/// late answer to an abandoned request is refused, not used), and each rule
/// kept. Rule ids are IBKR-wide, and IBKR rarely changes a rule.
#[derive(Default)]
struct MarketRuleCache {
    /// Known rules by id. Held across a lookup, so only one is in flight.
    rules: Mutex<HashMap<i32, Vec<PriceIncrementRule>>>,
}

impl MarketRuleCache {
    /// Returns one market rule's increments, from the cache or one lookup.
    ///
    /// # Arguments
    ///
    /// * `rule_id` - IBKR market rule id.
    /// * `fetch` - One request; returns the rule id IBKR answered with and
    ///   its increments.
    ///
    /// # Errors
    ///
    /// Returns the lookup's error, a provider error when IBKR answers for a
    /// different rule, or a contract error for malformed increments. Nothing
    /// is cached on any error.
    async fn get<F, Fut>(
        &self,
        rule_id: i32,
        fetch: F,
    ) -> Result<Vec<PriceIncrementRule>, GatewayError>
    where
        F: FnOnce(i32) -> Fut,
        Fut: Future<Output = Result<(i32, Vec<PriceIncrementRule>), GatewayError>>,
    {
        let mut rules = self.rules.lock().await;
        if let Some(known) = rules.get(&rule_id) {
            return Ok(known.clone());
        }
        let (answered, increments) = fetch(rule_id).await?;
        if answered != rule_id {
            return Err(GatewayError::Provider(format!(
                "IBKR answered market rule {answered} to a request for {rule_id}"
            )));
        }
        validate_price_rules(&increments)?;
        rules.insert(rule_id, increments.clone());
        Ok(increments)
    }
}

async fn price_rules(
    client: &Arc<Client>,
    market_rules: &MarketRuleCache,
    details: &ContractDetails,
) -> Result<Vec<PriceIncrementRule>, GatewayError> {
    let smart_index = details
        .valid_exchanges
        .iter()
        .position(|exchange| exchange == "SMART")
        .unwrap_or(0);
    if let Some(rule_id) = details
        .market_rule_ids
        .get(smart_index)
        .or_else(|| details.market_rule_ids.first())
        .and_then(|value| value.trim().parse::<i32>().ok())
    {
        return market_rules
            .get(rule_id, |rule_id| async move {
                let rule = client
                    .market_rule(rule_id)
                    .await
                    .map_err(|error| provider_error("market rule", error))?;
                let increments = rule
                    .price_increments
                    .into_iter()
                    .map(|rule| PriceIncrementRule {
                        lower_edge: rule.low_edge,
                        increment: rule.increment,
                    })
                    .collect::<Vec<_>>();
                Ok((rule.market_rule_id, increments))
            })
            .await;
    }
    let rules = (details.min_tick.is_finite() && details.min_tick > 0.0)
        .then_some(vec![PriceIncrementRule {
            lower_edge: 0.0,
            increment: details.min_tick,
        }])
        .unwrap_or_default();
    validate_price_rules(&rules)?;
    Ok(rules)
}

fn validate_price_rules(rules: &[PriceIncrementRule]) -> Result<(), GatewayError> {
    if rules.iter().any(|rule| {
        !rule.lower_edge.is_finite()
            || rule.lower_edge < 0.0
            || !rule.increment.is_finite()
            || rule.increment <= 0.0
    }) {
        return Err(GatewayError::Contract(
            "IBKR returned malformed price increment rules".to_string(),
        ));
    }
    Ok(())
}

fn validate_price_increment(
    field: &str,
    price: f64,
    rules: &[PriceIncrementRule],
) -> Result<(), GatewayError> {
    if !price.is_finite() || price <= 0.0 {
        return Err(GatewayError::Input(format!("{field} must be positive")));
    }
    let increment = rules
        .iter()
        .filter(|rule| rule.lower_edge <= price)
        .max_by(|left, right| left.lower_edge.total_cmp(&right.lower_edge))
        .map(|rule| rule.increment)
        .ok_or_else(|| {
            GatewayError::Contract("IBKR rules omitted a price increment".to_string())
        })?;
    if ((price / increment).round() * increment - price).abs() > 1e-8 {
        return Err(GatewayError::Input(format!(
            "{field} violates the IBKR tick increment"
        )));
    }
    Ok(())
}

fn validate_order_request(request: &OrderRequest) -> Result<(), GatewayError> {
    if request.provider != "ibkr" {
        return Err(GatewayError::Input("provider must be ibkr".to_string()));
    }
    validate_customer_order_id(&request.customer_order_id)?;
    if request
        .connection_id
        .as_deref()
        .is_some_and(|value| value.trim().is_empty() || value.len() > 128)
    {
        return Err(GatewayError::Input(
            "connectionId must contain 1 to 128 bytes when provided".to_string(),
        ));
    }
    if request
        .market_slippage_bps
        .is_some_and(|value| value > 10_000)
    {
        return Err(GatewayError::Input(
            "marketSlippageBps cannot exceed 10000".to_string(),
        ));
    }
    if request.post_only {
        return Err(GatewayError::Input(
            "IBKR does not support post-only orders".to_string(),
        ));
    }
    if request.reduce_only && request.protection.is_some() {
        return Err(GatewayError::Input(
            "unsupported IBKR protection: a reduce-only exit cannot carry TP/SL".to_string(),
        ));
    }
    if request.reduce_only && request.quantity.is_none() {
        return Err(GatewayError::Input(
            "an IBKR reduce-only exit needs an explicit quantity".to_string(),
        ));
    }
    if !request.size_usd.is_finite() || request.size_usd <= 0.0 {
        return Err(GatewayError::Input("sizeUsd must be positive".to_string()));
    }
    if request
        .quantity
        .is_some_and(|quantity| !quantity.is_finite() || quantity <= 0.0)
    {
        return Err(GatewayError::Input("quantity must be positive".to_string()));
    }
    if request.instrument_kind == InstrumentKind::Option && request.quantity.is_none() {
        return Err(GatewayError::Input(
            "option quantity is required".to_string(),
        ));
    }
    if request.instrument_kind == InstrumentKind::Option
        && request
            .quantity
            .is_some_and(|quantity| quantity.fract() != 0.0)
    {
        return Err(GatewayError::Input(
            "option quantity must be a whole number".to_string(),
        ));
    }
    if !matches!(request.side.as_str(), "buy" | "sell") {
        return Err(GatewayError::Input("side must be buy or sell".to_string()));
    }
    let order_type = request_order_type(request)?;
    validate_order_type_fields(request, order_type)?;
    if let Some(protection) = &request.protection {
        validate_order_protection(request, protection)?;
    }
    Ok(())
}

/// Parse the request's order type.
///
/// # Errors
///
/// Returns an input error listing every supported wire name.
fn request_order_type(request: &OrderRequest) -> Result<OrderType, GatewayError> {
    OrderType::parse(&request.order_type).ok_or_else(|| {
        let names = OrderType::ALL
            .iter()
            .map(|order_type| order_type.wire_name())
            .collect::<Vec<_>>()
            .join(", ");
        GatewayError::Input(format!("orderType must be one of {names}"))
    })
}

/// Require exactly the prices and attributes the order type uses.
///
/// Every price the type does not use is refused rather than ignored, so a
/// caller can never believe IBKR received a trigger it did not.
///
/// # Errors
///
/// Returns an input error naming the first missing, unused or invalid field.
fn validate_order_type_fields(
    request: &OrderRequest,
    order_type: OrderType,
) -> Result<(), GatewayError> {
    let input = |message: String| Err(GatewayError::Input(message));
    let name = order_type.wire_name();
    if request.reduce_only && !order_type.links_as_exit() {
        // Older web builds read "IBKR does not support reduce-only" as an
        // outdated bridge, so this refusal must not use that phrase.
        return input(format!(
            "an IBKR linked exit must be a market, limit, stop or stop_limit order, not {name}; place this exit in IBKR"
        ));
    }
    if order_type.fills_at_market()
        && let Some(cap) = request.market_slippage_bps
    {
        // IBKR bounds none of these by price; a cap here would be ignored.
        return input(format!(
            "marketSlippageBps cannot bound a {name} order, which IBKR fills at market; send it without the {cap} bps cap, or as a limit-priced order"
        ));
    }
    let positive = |value: Option<f64>| value.is_some_and(|value| value.is_finite() && value > 0.0);
    if order_type.needs_limit_price() && !positive(request.limit_price) {
        return input(format!("limitPrice must be positive for a {name} order"));
    }
    if !order_type.needs_limit_price() && request.limit_price.is_some() {
        return input(format!("a {name} order takes no limitPrice"));
    }
    if order_type.needs_stop_price() && !positive(request.stop_price) {
        return input(format!("stopPrice must be positive for a {name} order"));
    }
    if !order_type.needs_stop_price() && request.stop_price.is_some() {
        return input(format!("a {name} order takes no stopPrice"));
    }
    if order_type.is_trailing() {
        if !request
            .trailing_percent
            .is_some_and(|percent| percent.is_finite() && percent > 0.0 && percent < 100.0)
        {
            return input(format!(
                "trailingPercent must be above 0 and below 100 for a {name} order"
            ));
        }
    } else if request.trailing_percent.is_some() {
        return input(format!("a {name} order takes no trailingPercent"));
    }
    if order_type == OrderType::TrailingStopLimit {
        if !positive(request.limit_offset) {
            return input("limitOffset must be positive for a trailing_stop_limit order".into());
        }
    } else if request.limit_offset.is_some() {
        return input(format!("a {name} order takes no limitOffset"));
    }
    if !order_type.accepts_cash_quantity() && request.quantity.is_none() {
        return input(format!(
            "a {name} order needs an explicit quantity; IBKR sizes only market and limit orders by cash"
        ));
    }
    if !matches!(
        request.time_in_force.as_str(),
        "day" | "gtc" | "ioc" | "fok"
    ) {
        return input("timeInForce must be day, gtc, ioc, or fok".into());
    }
    if order_type.is_auction() {
        if request.instrument_kind != InstrumentKind::Stock {
            return input(format!("a {name} order is available for stocks only"));
        }
        if request.time_in_force != "day" {
            return input(format!(
                "a {name} order needs timeInForce day; it lives only until its auction"
            ));
        }
        if request.outside_rth {
            return input(format!(
                "a {name} order executes only in its auction and cannot work outside regular trading hours"
            ));
        }
    }
    if request.algo.is_some() {
        if !matches!(order_type, OrderType::Market | OrderType::Limit) {
            return input("the Adaptive algo takes only market and limit orders".into());
        }
        if request.time_in_force != "day" {
            return input("the Adaptive algo needs timeInForce day; IBKR algos do not support GTC, IOC or FOK".into());
        }
    }
    Ok(())
}

/// Reject any attached protection this bridge cannot place as an IBKR bracket.
///
/// Only market and limit parents with an explicit whole quantity are
/// supported, because each child must close exactly the parent quantity. IOC
/// parents are refused: a partial IOC fill would leave full-size children.
fn validate_order_protection(
    request: &OrderRequest,
    protection: &OrderProtection,
) -> Result<(), GatewayError> {
    let unsupported = |detail: &str| {
        Err(GatewayError::Input(format!(
            "unsupported IBKR protection: {detail}"
        )))
    };
    if protection.take_profit.is_none() && protection.stop_loss.is_none() {
        return unsupported("protection needs a take profit or a stop loss");
    }
    if !matches!(request.order_type.as_str(), "market" | "limit") {
        return unsupported("TP/SL attaches only to market or limit orders");
    }
    if request.time_in_force == "ioc" {
        return unsupported(
            "IOC orders cannot carry TP/SL because a partial fill would leave full-size children",
        );
    }
    match request.quantity {
        Some(quantity) if quantity.fract() == 0.0 && quantity >= 1.0 => {}
        _ => {
            return unsupported(
                "TP/SL requires an explicit whole-share or whole-contract quantity",
            );
        }
    }
    let mut references = vec![request.customer_order_id.as_str()];
    for leg in [&protection.take_profit, &protection.stop_loss]
        .into_iter()
        .flatten()
    {
        if !leg.price.is_finite() || leg.price <= 0.0 {
            return unsupported("TP/SL prices must be positive");
        }
        validate_customer_order_id(&leg.customer_order_id)?;
        if references.contains(&leg.customer_order_id.as_str()) {
            return unsupported("every bracket order needs its own customerOrderId");
        }
        references.push(&leg.customer_order_id);
    }
    validate_protection_prices(
        &request.side,
        request.limit_price,
        protection.take_profit.as_ref().map(|leg| leg.price),
        protection.stop_loss.as_ref().map(|leg| leg.price),
    )
}

/// Require the take profit above and the stop loss below a long entry, or the
/// reverse for a short, so neither child can execute against the entry.
fn validate_protection_prices(
    side: &str,
    limit_price: Option<f64>,
    take_profit: Option<f64>,
    stop_loss: Option<f64>,
) -> Result<(), GatewayError> {
    let long = side == "buy";
    let invalid = |detail: &str| {
        Err(GatewayError::Input(format!(
            "unsupported IBKR protection: {detail}"
        )))
    };
    if let (Some(take_profit), Some(stop_loss)) = (take_profit, stop_loss)
        && (if long {
            take_profit <= stop_loss
        } else {
            take_profit >= stop_loss
        })
    {
        return invalid(if long {
            "take profit must be above stop loss for a buy"
        } else {
            "take profit must be below stop loss for a sell"
        });
    }
    if let Some(limit) = limit_price {
        if take_profit.is_some_and(|price| if long { price <= limit } else { price >= limit }) {
            return invalid("take profit must be beyond the limit price");
        }
        if stop_loss.is_some_and(|price| if long { price >= limit } else { price <= limit }) {
            return invalid("stop loss must be behind the limit price");
        }
    }
    Ok(())
}

/// Move each requested child price onto the contract's price increment.
///
/// Both children move toward the entry: a long's take profit rounds down and
/// its stop loss rounds up; a short's the reverse. Rounding therefore never
/// widens the requested risk. The aligned prices are validated again.
fn align_protection(
    request: &OrderRequest,
    protection: &OrderProtection,
    rules: &[PriceIncrementRule],
) -> Result<Vec<AlignedProtectionLeg>, GatewayError> {
    let long = request.side == "buy";
    let mut aligned = Vec::new();
    for (kind, leg) in [
        (ProtectionLegKind::TakeProfit, &protection.take_profit),
        (ProtectionLegKind::StopLoss, &protection.stop_loss),
    ] {
        let Some(ProtectionLegRequest {
            price,
            customer_order_id,
        }) = leg
        else {
            continue;
        };
        let round_up = match kind {
            ProtectionLegKind::TakeProfit => !long,
            ProtectionLegKind::StopLoss => long,
        };
        let field = match kind {
            ProtectionLegKind::TakeProfit => "takeProfit",
            ProtectionLegKind::StopLoss => "stopLoss",
        };
        let price = align_price(*price, rules, round_up)?;
        validate_price_increment(field, price, rules)?;
        aligned.push(AlignedProtectionLeg {
            kind,
            price,
            customer_order_id: customer_order_id.clone(),
        });
    }
    let aligned_price = |kind| {
        aligned
            .iter()
            .find(|leg: &&AlignedProtectionLeg| leg.kind == kind)
            .map(|leg| leg.price)
    };
    validate_protection_prices(
        &request.side,
        request.limit_price,
        aligned_price(ProtectionLegKind::TakeProfit),
        aligned_price(ProtectionLegKind::StopLoss),
    )?;
    Ok(aligned)
}

/// Round a positive price onto the increment that applies at that price.
fn align_price(
    price: f64,
    rules: &[PriceIncrementRule],
    round_up: bool,
) -> Result<f64, GatewayError> {
    let increment = rules
        .iter()
        .filter(|rule| rule.lower_edge <= price)
        .max_by(|left, right| left.lower_edge.total_cmp(&right.lower_edge))
        .map(|rule| rule.increment)
        .ok_or_else(|| {
            GatewayError::Contract("IBKR rules omitted a price increment".to_string())
        })?;
    let steps = price / increment;
    let nearest = steps.round();
    let steps = if (steps - nearest).abs() <= 1e-9 {
        nearest
    } else if round_up {
        steps.ceil()
    } else {
        steps.floor()
    };
    // Strip binary noise so the price matches the increment exactly on the wire.
    let aligned = (steps * increment * 1e8).round() / 1e8;
    if !aligned.is_finite() || aligned <= 0.0 {
        return Err(GatewayError::Input(
            "unsupported IBKR protection: TP/SL price rounds to zero".to_string(),
        ));
    }
    Ok(aligned)
}

/// Require the exact `sunk-<lowercase hyphenated uuid>` order-reference shape.
fn validate_customer_order_id(value: &str) -> Result<(), GatewayError> {
    let valid = value
        .strip_prefix(CUSTOMER_ORDER_ID_PREFIX)
        .and_then(|suffix| {
            Uuid::parse_str(suffix)
                .ok()
                .map(|uuid| uuid.hyphenated().to_string() == suffix)
        })
        .unwrap_or(false);
    if !valid {
        return Err(GatewayError::Input(
            "customerOrderId must be sunk- followed by a lowercase hyphenated UUID".to_string(),
        ));
    }
    Ok(())
}

fn validate_reconcile_order_request(request: &ReconcileOrderRequest) -> Result<(), GatewayError> {
    if request.version != PROTOCOL_VERSION {
        return Err(GatewayError::Input(format!(
            "reconciliation protocol version must be {PROTOCOL_VERSION}"
        )));
    }
    if request.provider != "ibkr" {
        return Err(GatewayError::Input("provider must be ibkr".to_string()));
    }
    if request
        .connection_id
        .as_deref()
        .is_some_and(|value| value.trim().is_empty() || value.len() > 128)
    {
        return Err(GatewayError::Input(
            "connectionId must contain 1 to 128 bytes when provided".to_string(),
        ));
    }
    let account_ref = request.account_ref.trim();
    if account_ref.is_empty() || account_ref.len() > 128 {
        return Err(GatewayError::Input(
            "accountRef is required for reconciliation".to_string(),
        ));
    }
    validate_customer_order_id(&request.customer_order_id)
}

/// Provider evidence that an order reference reached IBKR.
#[derive(Debug, PartialEq)]
struct FoundOrderReference {
    state: ReconcileOrderState,
    status: &'static str,
    order_id: Option<i32>,
}

/// Whether a provider account field is compatible with the searched account.
///
/// IBKR may leave the account empty on some completed-order payloads; the
/// unique order reference remains the identity in that case.
fn provider_account_matches(provider_account: &str, account_ref: &str) -> bool {
    provider_account.is_empty() || provider_account == account_ref
}

/// Normalize a provider order snapshot into a stable reconciliation status.
fn reconciled_order_status(order: &OrderData) -> &'static str {
    match order.order_state.status {
        OrderStatusKind::Filled => "filled",
        OrderStatusKind::Submitted
        | OrderStatusKind::PreSubmitted
        | OrderStatusKind::PendingCancel => "resting",
        OrderStatusKind::PendingSubmit | OrderStatusKind::ApiPending => "accepted",
        OrderStatusKind::Cancelled | OrderStatusKind::ApiCancelled | OrderStatusKind::Inactive
            if order.order.filled_quantity > 0.0 =>
        {
            "accepted"
        }
        OrderStatusKind::Cancelled | OrderStatusKind::ApiCancelled | OrderStatusKind::Inactive => {
            "rejected"
        }
    }
}

/// Whether a provider order carries this exact reference for this account.
fn order_matches_reference(order: &OrderData, account_ref: &str, customer_order_id: &str) -> bool {
    order.order.order_ref == customer_order_id
        && provider_account_matches(&order.order.account, account_ref)
}

/// Find an exact order-reference match in one open or completed order snapshot.
fn match_order_reference(
    orders: &[OrderData],
    account_ref: &str,
    customer_order_id: &str,
    state: ReconcileOrderState,
) -> Option<FoundOrderReference> {
    orders
        .iter()
        .find(|order| order_matches_reference(order, account_ref, customer_order_id))
        .map(|order| FoundOrderReference {
            state,
            status: reconciled_order_status(order),
            order_id: Some(order.order_id).filter(|order_id| *order_id > 0),
        })
}

/// Find an exact order-reference match in one execution snapshot.
fn match_execution_reference(
    executions: &[ExecutionData],
    account_ref: &str,
    customer_order_id: &str,
) -> Option<FoundOrderReference> {
    executions
        .iter()
        .find(|execution| {
            execution.execution.order_reference == customer_order_id
                && provider_account_matches(&execution.execution.account_number, account_ref)
        })
        .map(|execution| FoundOrderReference {
            state: ReconcileOrderState::Executed,
            status: "accepted",
            order_id: Some(execution.execution.order_id).filter(|order_id| *order_id > 0),
        })
}

/// Search open, completed, and executed provider state for one order reference.
async fn find_order_reference(
    client: &Arc<Client>,
    account_ref: &str,
    customer_order_id: &str,
) -> Result<Option<FoundOrderReference>, GatewayError> {
    let open = collect_order_snapshot(client, OrderSnapshot::AllOpen).await?;
    if let Some(found) = match_order_reference(
        &open,
        account_ref,
        customer_order_id,
        ReconcileOrderState::Open,
    ) {
        return Ok(Some(found));
    }
    let completed = collect_order_snapshot(client, OrderSnapshot::Completed).await?;
    if let Some(found) = match_order_reference(
        &completed,
        account_ref,
        customer_order_id,
        ReconcileOrderState::Completed,
    ) {
        return Ok(Some(found));
    }
    let executions = collect_executions(client, account_ref).await?;
    Ok(match_execution_reference(
        &executions,
        account_ref,
        customer_order_id,
    ))
}

#[derive(Clone, Copy)]
enum OrderSnapshot {
    /// Current open orders from every API client and TWS itself.
    AllOpen,
    /// Today's completed orders, including those not placed through the API.
    Completed,
}

async fn collect_order_snapshot(
    client: &Arc<Client>,
    snapshot: OrderSnapshot,
) -> Result<Vec<OrderData>, GatewayError> {
    let label = match snapshot {
        OrderSnapshot::AllOpen => "all open orders",
        OrderSnapshot::Completed => "completed orders",
    };
    let subscription = match snapshot {
        OrderSnapshot::AllOpen => client.all_open_orders().await,
        OrderSnapshot::Completed => client.completed_orders(false).await,
    }
    .map_err(|error| provider_error(label, error))?;
    let mut updates = subscription.filter_data();
    tokio::time::timeout(READ_TIMEOUT, async {
        let mut orders = Vec::new();
        while let Some(update) = updates.next().await {
            if let Orders::OrderData(order) =
                update.map_err(|error| provider_error(label, error))?
            {
                orders.push(order);
            }
        }
        Ok::<_, GatewayError>(orders)
    })
    .await
    .map_err(|_| GatewayError::Unavailable(format!("IBKR {label} snapshot timed out")))?
}

async fn collect_executions(
    client: &Arc<Client>,
    account_ref: &str,
) -> Result<Vec<ExecutionData>, GatewayError> {
    let subscription = client
        .executions(ExecutionFilter {
            account_code: account_ref.to_string(),
            ..ExecutionFilter::default()
        })
        .await
        .map_err(|error| provider_error("executions", error))?;
    let mut updates = subscription.filter_data();
    tokio::time::timeout(READ_TIMEOUT, async {
        let mut executions = Vec::new();
        while let Some(update) = updates.next().await {
            if let Executions::ExecutionData(execution) =
                update.map_err(|error| provider_error("executions", error))?
            {
                executions.push(execution);
            }
        }
        Ok::<_, GatewayError>(executions)
    })
    .await
    .map_err(|_| GatewayError::Unavailable("IBKR executions snapshot timed out".to_string()))?
}

async fn collect_account_positions(
    client: &Arc<Client>,
    account_ref: &str,
) -> Result<Vec<AccountPosition>, GatewayError> {
    let subscription = client
        .positions()
        .await
        .map_err(|error| provider_error("positions", error))?;
    let mut updates = subscription.clone().filter_data();
    let result = tokio::time::timeout(READ_TIMEOUT, async {
        let mut positions = Vec::new();
        while let Some(update) = updates.next().await {
            match update.map_err(|error| provider_error("positions", error))? {
                PositionUpdate::Position(position) => {
                    if position.account == account_ref && position.position != 0.0 {
                        positions.push(normalize_account_position(position)?);
                    }
                }
                PositionUpdate::PositionEnd => break,
            }
        }
        positions.sort_by(|left, right| {
            left.symbol
                .cmp(&right.symbol)
                .then(left.contract_id.cmp(&right.contract_id))
        });
        Ok::<_, GatewayError>(positions)
    })
    .await
    .map_err(|_| GatewayError::Unavailable("IBKR positions snapshot timed out".to_string()))?;
    subscription.cancel().await;
    result
}

fn normalize_account_position(
    position: ibapi::accounts::Position,
) -> Result<AccountPosition, GatewayError> {
    if position.contract.contract_id <= 0 {
        return Err(GatewayError::Contract(
            "IBKR position omitted its contract id".to_string(),
        ));
    }
    if !position.position.is_finite() || !position.average_cost.is_finite() {
        return Err(GatewayError::Contract(
            "IBKR position contained a non-finite value".to_string(),
        ));
    }
    let symbol = position.contract.symbol.to_string();
    if symbol.trim().is_empty() {
        return Err(GatewayError::Contract(
            "IBKR position omitted its symbol".to_string(),
        ));
    }
    let multiplier = contract_multiplier(&position.contract)?;
    Ok(AccountPosition {
        account_ref: position.account,
        contract_id: position.contract.contract_id,
        symbol,
        local_symbol: position.contract.local_symbol,
        instrument_kind: account_instrument_kind(&position.contract.security_type),
        size: position.position,
        average_price: position.average_cost / multiplier,
        multiplier,
    })
}

async fn collect_account_orders(
    client: &Arc<Client>,
    account_ref: &str,
) -> Result<Vec<AccountOpenOrder>, GatewayError> {
    let subscription = client
        .open_orders()
        .await
        .map_err(|error| provider_error("open orders", error))?;
    let mut updates = subscription.filter_data();
    tokio::time::timeout(READ_TIMEOUT, async {
        let mut orders = Vec::new();
        while let Some(update) = updates.next().await {
            match update.map_err(|error| provider_error("open orders", error))? {
                Orders::OrderData(order) if order.order.account == account_ref => {
                    if order.order_state.status.is_active()
                        || order.order_state.status == OrderStatusKind::ApiPending
                    {
                        orders.push(normalize_account_order(order)?);
                    }
                }
                Orders::OrderData(_) | Orders::OrderStatus(_) => {}
            }
        }
        orders.sort_by_key(|order| order.order_id);
        Ok::<_, GatewayError>(orders)
    })
    .await
    .map_err(|_| GatewayError::Unavailable("IBKR open-orders snapshot timed out".to_string()))?
}

fn normalize_account_order(order: OrderData) -> Result<AccountOpenOrder, GatewayError> {
    if order.order_id <= 0 || order.contract.contract_id <= 0 {
        return Err(GatewayError::Contract(
            "IBKR open order omitted its native identity".to_string(),
        ));
    }
    if !order.order.total_quantity.is_finite() || order.order.total_quantity <= 0.0 {
        return Err(GatewayError::Contract(
            "IBKR open order contained an invalid quantity".to_string(),
        ));
    }
    let symbol = order.contract.symbol.to_string();
    if symbol.trim().is_empty() {
        return Err(GatewayError::Contract(
            "IBKR open order omitted its symbol".to_string(),
        ));
    }
    let tws_type = reported_order_type(&order.order);
    let algo = reported_algo(&order.order);
    // An order with an algo the bridge does not place cannot be re-placed
    // as reported, so it is reported like an unknown type.
    let order_type = tws_type.filter(|_| algo.is_ok());
    // A trailing stop's `auxPrice` is its trailing amount, not a price; its
    // current stop is the trail stop price IBKR maintains.
    let trigger_price = if tws_type.is_some_and(OrderType::is_trailing) {
        positive_finite(order.order.trail_stop_price)
    } else {
        positive_finite(order.order.aux_price)
    };
    Ok(AccountOpenOrder {
        account_ref: order.order.account,
        order_id: order.order_id,
        contract_id: order.contract.contract_id,
        symbol,
        instrument_kind: account_instrument_kind(&order.contract.security_type),
        side: match order.order.action {
            Action::Buy | Action::SellLong => "buy".to_string(),
            Action::Sell | Action::SellShort => "sell".to_string(),
        },
        quantity: order.order.total_quantity,
        price: positive_finite(order.order.limit_price),
        trigger_price,
        status: order.order_state.status.as_str().to_string(),
        order_type,
        time_in_force: reported_time_in_force(&order.order.tif).map(str::to_string),
        outside_rth: order.order.outside_rth,
        algo: algo.unwrap_or(None),
        // Only groups with block bound what their orders close together; a
        // blank or over-long name is omitted, and the order shows as unlinked.
        oca_group: Some(order.order.oca_group).filter(|group| {
            !group.trim().is_empty()
                && group.len() <= MAX_OCA_GROUP_LENGTH
                && matches!(
                    order.order.oca_type,
                    OcaType::CancelWithBlock | OcaType::ReduceWithBlock
                )
        }),
        parent_id: Some(order.order.parent_id).filter(|parent| *parent > 0),
        filled_quantity: positive_finite(Some(order.order.filled_quantity)),
    })
}

/// The bridge order type a working TWS order is, the inverse of
/// [`build_order`]: `MKT` and `LMT` with time in force `OPG` are the opening
/// auction's types. Any other TWS type (`REL`, pegged, ...) is none.
fn reported_order_type(order: &Order) -> Option<OrderType> {
    let on_open = order.tif == TimeInForce::OnOpen;
    let order_type = match order.order_type.trim().to_ascii_uppercase().as_str() {
        "MKT" if on_open => OrderType::MarketOnOpen,
        "MKT" => OrderType::Market,
        "LMT" if on_open => OrderType::LimitOnOpen,
        "LMT" => OrderType::Limit,
        "STP" => OrderType::StopMarket,
        "STP LMT" => OrderType::StopLimit,
        "TRAIL" => OrderType::TrailingStop,
        "TRAIL LIMIT" => OrderType::TrailingStopLimit,
        "MIT" => OrderType::MarketIfTouched,
        "LIT" => OrderType::LimitIfTouched,
        "MOC" => OrderType::MarketOnClose,
        "LOC" => OrderType::LimitOnClose,
        _ => return None,
    };
    Some(order_type)
}

/// The request time in force that re-places a working order, the inverse of
/// [`build_order`]: on-open auction orders are requested as `day`. `GTD`,
/// `DTC` and auction-only time in force are none.
fn reported_time_in_force(tif: &TimeInForce) -> Option<&'static str> {
    match tif {
        TimeInForce::Day | TimeInForce::OnOpen => Some("day"),
        TimeInForce::GoodTilCanceled => Some("gtc"),
        TimeInForce::ImmediateOrCancel => Some("ioc"),
        TimeInForce::FillOrKill => Some("fok"),
        TimeInForce::GoodTilDate | TimeInForce::DayTilCanceled | TimeInForce::Auction => None,
    }
}

/// The algo routing of a working order, the inverse of [`build_order`].
///
/// # Returns
///
/// None without an algo, Adaptive with its `adaptivePriority` (IBKR's
/// `Normal` when the parameter is absent), and an error for any other algo
/// or priority, which the bridge cannot re-place.
fn reported_algo(order: &Order) -> Result<Option<OrderAlgo>, ()> {
    let strategy = order.algo_strategy.trim();
    if strategy.is_empty() {
        return Ok(None);
    }
    if !strategy.eq_ignore_ascii_case("Adaptive") {
        return Err(());
    }
    let priority = order
        .algo_params
        .iter()
        .find(|param| param.tag == "adaptivePriority")
        .map(|param| param.value.trim());
    let priority = match priority {
        None => AdaptivePriority::Normal,
        Some(value) if value.eq_ignore_ascii_case("Urgent") => AdaptivePriority::Urgent,
        Some(value) if value.eq_ignore_ascii_case("Normal") => AdaptivePriority::Normal,
        Some(value) if value.eq_ignore_ascii_case("Patient") => AdaptivePriority::Patient,
        Some(_) => return Err(()),
    };
    Ok(Some(OrderAlgo {
        strategy: AlgoStrategy::Adaptive,
        priority,
    }))
}

fn account_instrument_kind(security_type: &SecurityType) -> Option<InstrumentKind> {
    match security_type {
        SecurityType::Stock => Some(InstrumentKind::Stock),
        SecurityType::Option => Some(InstrumentKind::Option),
        _ => None,
    }
}

fn contract_multiplier(contract: &Contract) -> Result<f64, GatewayError> {
    if contract.multiplier.trim().is_empty() {
        return Ok(1.0);
    }
    let multiplier = contract.multiplier.parse::<f64>().map_err(|error| {
        GatewayError::Contract(format!("invalid IBKR contract multiplier: {error}"))
    })?;
    if !multiplier.is_finite() || multiplier <= 0.0 {
        return Err(GatewayError::Contract(
            "IBKR contract multiplier must be positive".to_string(),
        ));
    }
    Ok(multiplier)
}

fn positive_finite(value: Option<f64>) -> Option<f64> {
    value.filter(|number| number.is_finite() && *number > 0.0)
}

fn validate_cancel_order_request(request: &CancelOrderRequest) -> Result<(), GatewayError> {
    if request.version != PROTOCOL_VERSION {
        return Err(GatewayError::Input(format!(
            "cancellation protocol version must be {PROTOCOL_VERSION}"
        )));
    }
    if request.provider != "ibkr" {
        return Err(GatewayError::Input("provider must be ibkr".to_string()));
    }
    if request.order_id <= 0 {
        return Err(GatewayError::Input("orderId must be positive".to_string()));
    }
    if request
        .connection_id
        .as_deref()
        .is_some_and(|value| value.trim().is_empty() || value.len() > 128)
    {
        return Err(GatewayError::Input(
            "connectionId must contain 1 to 128 bytes when provided".to_string(),
        ));
    }
    Ok(())
}

fn cancel_response(
    account_ref: &str,
    order_id: i32,
    ok: bool,
    status: &str,
    message: &str,
) -> OrderResponse {
    OrderResponse {
        version: PROTOCOL_VERSION,
        provider: "ibkr",
        ok,
        status: status.to_string(),
        message: message.to_string(),
        account_ref: Some(account_ref.to_string()),
        order_id: Some(order_id.to_string()),
        customer_order_id: None,
        protection: None,
        exit: None,
    }
}

/// IBKR's answer to one cancellation request.
#[derive(Clone, Debug, PartialEq)]
enum CancelOutcome {
    /// IBKR confirmed the cancellation.
    Cancelled,
    /// The order filled before it could be cancelled.
    Filled,
    /// The order is not working, or IBKR refused the cancellation; the text says which.
    Rejected(String),
    /// The outcome is unknown; the text says why.
    Unknown(String),
}

/// Send one cancellation; IBKR's answer arrives on the returned stream.
///
/// # Errors
///
/// Returns the unknown outcome when the request could not be handed over.
async fn send_cancel(
    client: &Client,
    order_id: i32,
    deadline: tokio::time::Instant,
) -> Result<Subscription<CancelOrder>, CancelOutcome> {
    let send_deadline = deadline.min(tokio::time::Instant::now() + ORDER_SUBMISSION_TIMEOUT);
    match tokio::time::timeout_at(send_deadline, client.cancel_order(order_id, "")).await {
        Ok(Ok(events)) => Ok(events),
        Ok(Err(error)) => Err(CancelOutcome::Unknown(format!(
            "the cancellation transport failed: {error}"
        ))),
        Err(_) => Err(CancelOutcome::Unknown(
            "the cancellation send timed out".to_string(),
        )),
    }
}

/// Wait on a sent cancellation's stream for IBKR's definitive answer.
async fn confirm_cancel(
    mut events: Subscription<CancelOrder>,
    deadline: tokio::time::Instant,
) -> CancelOutcome {
    tokio::time::timeout_at(deadline, async {
        while let Some(event) = events.next().await {
            match event {
                Ok(SubscriptionItem::Data(CancelOrder::OrderStatus(status))) => {
                    match status.status {
                        OrderStatusKind::Cancelled | OrderStatusKind::ApiCancelled => {
                            return CancelOutcome::Cancelled;
                        }
                        OrderStatusKind::Filled => return CancelOutcome::Filled,
                        OrderStatusKind::Inactive => {
                            return CancelOutcome::Rejected(
                                "IBKR reports the order is inactive".to_string(),
                            );
                        }
                        _ => continue,
                    }
                }
                Ok(SubscriptionItem::Notice(notice)) => {
                    if notice.category() == NoticeCategory::OrderRejection {
                        return CancelOutcome::Rejected(format!(
                            "IBKR rejected the cancellation: {}",
                            notice.message
                        ));
                    }
                }
                Err(error) => {
                    return CancelOutcome::Unknown(format!(
                        "the cancellation event stream failed: {error}"
                    ));
                }
            }
        }
        CancelOutcome::Unknown(
            "the cancellation stream ended without a definitive state".to_string(),
        )
    })
    .await
    .unwrap_or_else(|_| {
        CancelOutcome::Unknown("the cancellation acknowledgement timed out".to_string())
    })
}

/// Send one cancellation and wait for IBKR's definitive answer.
///
/// The cancellation is never retried after it is sent.
///
/// # Arguments
///
/// * `client` - Connected TWS client that placed the order.
/// * `order_id` - Order id in this client's id space.
/// * `deadline` - When to stop waiting for the acknowledgement.
///
/// # Returns
///
/// The cancellation outcome.
async fn cancel_and_confirm(
    client: &Client,
    order_id: i32,
    deadline: tokio::time::Instant,
) -> CancelOutcome {
    match send_cancel(client, order_id, deadline).await {
        Ok(events) => confirm_cancel(events, deadline).await,
        Err(outcome) => outcome,
    }
}

/// Map a cancellation outcome to the order-response contract.
fn cancel_outcome_response(account: &str, order_id: i32, outcome: CancelOutcome) -> OrderResponse {
    match outcome {
        CancelOutcome::Cancelled => cancel_response(
            account,
            order_id,
            true,
            "cancelled",
            "IBKR confirmed the order cancellation",
        ),
        CancelOutcome::Filled => cancel_response(
            account,
            order_id,
            false,
            "filled",
            "IBKR reports the order filled before cancellation",
        ),
        CancelOutcome::Rejected(message) => {
            cancel_response(account, order_id, false, "rejected", &message)
        }
        CancelOutcome::Unknown(detail) => {
            cancel_indeterminate_response(account.to_string(), order_id, &detail)
        }
    }
}

/// Read the account's signed position in one contract, zero when flat.
///
/// # Errors
///
/// Returns a provider or availability error when IBKR does not complete the
/// positions snapshot.
async fn read_position(
    client: &Arc<Client>,
    account: &str,
    contract_id: i32,
) -> Result<f64, GatewayError> {
    let subscription = client
        .positions()
        .await
        .map_err(|error| provider_error("positions", error))?;
    let mut updates = subscription.clone().filter_data();
    let read = tokio::time::timeout(READ_TIMEOUT, async {
        let mut size = 0.0;
        while let Some(update) = updates.next().await {
            match update.map_err(|error| provider_error("positions", error))? {
                PositionUpdate::Position(position)
                    if position.account == account
                        && position.contract.contract_id == contract_id =>
                {
                    if !position.position.is_finite() {
                        return Err(GatewayError::Contract(
                            "IBKR position contained a non-finite value".to_string(),
                        ));
                    }
                    size = position.position;
                }
                PositionUpdate::Position(_) => {}
                PositionUpdate::PositionEnd => break,
            }
        }
        Ok::<_, GatewayError>(size)
    })
    .await;
    // Cancel the subscription on every path, a timeout included.
    subscription.cancel().await;
    read.map_err(|_| GatewayError::Unavailable("IBKR positions snapshot timed out".to_string()))?
}

/// Unused OCA group name for exits that are re-placed without a new order.
fn fresh_oca_group() -> String {
    format!(
        "oca-{CUSTOMER_ORDER_ID_PREFIX}{}",
        Uuid::new_v4().hyphenated()
    )
}

fn cancel_indeterminate_response(
    account_ref: String,
    order_id: i32,
    detail: &str,
) -> OrderResponse {
    cancel_response(
        &account_ref,
        order_id,
        false,
        "indeterminate",
        &format!(
            "IBKR cancellation outcome is unknown because {detail}. Reconcile open orders before retrying"
        ),
    )
}

/// Build the TWS order for a validated request.
///
/// The request's [`OrderType`] picks the TWS order type and the prices it
/// carries: `lmtPrice` for limit-priced types, `auxPrice` for triggers,
/// `trailingPercent` and `lmtPriceOffset` for trailing types. On-open
/// auction orders use time in force `OPG`. An Adaptive algo request becomes
/// `algoStrategy = "Adaptive"` with its `adaptivePriority`.
fn build_order(request: &OrderRequest, account: &str, order_ref: &str) -> Order {
    // Validation already refused an unknown type; fall back to the
    // least-powerful reading rather than panic if it ever slips through.
    let order_type = OrderType::parse(&request.order_type).unwrap_or(OrderType::Limit);
    let (total_quantity, cash_qty) = match request.quantity {
        Some(quantity) => (quantity, None),
        None => (0.0, Some(request.size_usd)),
    };
    let (algo_strategy, algo_params) = match request.algo {
        Some(OrderAlgo {
            strategy: AlgoStrategy::Adaptive,
            priority,
        }) => (
            "Adaptive".to_string(),
            vec![TagValue {
                tag: "adaptivePriority".to_string(),
                value: priority.tws_value().to_string(),
            }],
        ),
        None => (String::new(), Vec::new()),
    };
    Order {
        action: if request.side == "buy" {
            Action::Buy
        } else {
            Action::Sell
        },
        total_quantity,
        cash_qty,
        order_type: order_type.tws_order_type().to_string(),
        limit_price: request.limit_price,
        aux_price: request.stop_price,
        trailing_percent: request.trailing_percent,
        limit_price_offset: request.limit_offset,
        tif: if order_type.is_on_open() {
            TimeInForce::OnOpen
        } else {
            match request.time_in_force.as_str() {
                "day" => TimeInForce::Day,
                "ioc" => TimeInForce::ImmediateOrCancel,
                "fok" => TimeInForce::FillOrKill,
                _ => TimeInForce::GoodTilCanceled,
            }
        },
        outside_rth: request.outside_rth,
        algo_strategy,
        algo_params,
        account: account.to_string(),
        order_ref: order_ref.to_string(),
        transmit: true,
        override_percentage_constraints: false,
        advanced_error_override: String::new(),
        ..Default::default()
    }
}

/// One order of an IBKR bracket, in submission order.
#[derive(Clone, Debug)]
struct BracketOrder {
    order_id: i32,
    order: Order,
}

/// A bracket child tracked alongside its submitted order.
#[derive(Clone, Debug, PartialEq)]
struct BracketChild {
    kind: ProtectionLegKind,
    order_id: i32,
    customer_order_id: String,
    price: f64,
}

/// Parent and children of one IBKR bracket, ready to submit.
#[derive(Clone, Debug)]
struct BracketPlan {
    parent_customer_order_id: String,
    orders: Vec<BracketOrder>,
    children: Vec<BracketChild>,
}

/// Build the TWS bracket: the parent, then a take-profit `LMT` and a
/// stop-loss `STP` child.
///
/// Follows the TWS API bracket-order sample: children use the parent's order
/// id plus one and two, carry `parentId`, and take the opposite action for the
/// parent's exact quantity. Every order but the last is sent with
/// `transmit = false`; the last child's `transmit = true` makes TWS transmit
/// the whole bracket together, so the parent can never work alone.
///
/// The children also share one OCA group of type 2 (reduce with block): a fill
/// of one child cancels the other, and a partial fill reduces the other by the
/// filled quantity, so the pair never closes more than the parent opened.
/// Children are GTC so the protection outlives the trading day, and inherit
/// the parent's `outsideRth`, so a parent filled in extended hours is
/// protected in extended hours.
fn build_bracket(
    request: &OrderRequest,
    account: &str,
    parent_order_id: i32,
    legs: &[AlignedProtectionLeg],
) -> BracketPlan {
    let mut parent = build_order(request, account, &request.customer_order_id);
    parent.transmit = false;
    let oca_group = format!("oca-{}", request.customer_order_id);
    let mut orders = vec![BracketOrder {
        order_id: parent_order_id,
        order: parent.clone(),
    }];
    let mut children = Vec::new();
    for (index, leg) in legs.iter().enumerate() {
        let order_id = parent_order_id + 1 + index as i32;
        let (order_type, limit_price, aux_price) = match leg.kind {
            ProtectionLegKind::TakeProfit => ("LMT", Some(leg.price), None),
            ProtectionLegKind::StopLoss => ("STP", None, Some(leg.price)),
        };
        orders.push(BracketOrder {
            order_id,
            order: Order {
                action: parent.action.reverse(),
                total_quantity: parent.total_quantity,
                order_type: order_type.to_string(),
                limit_price,
                aux_price,
                tif: TimeInForce::GoodTilCanceled,
                // A parent that fills in extended hours needs children that
                // can fill (take profit) and trigger (stop loss) there too,
                // or the position is unprotected until the session opens.
                // IBKR's default trigger method suits a plain stop; where it
                // cannot honour the flag it answers 2109, a follow-up code.
                outside_rth: parent.outside_rth,
                account: account.to_string(),
                order_ref: leg.customer_order_id.clone(),
                parent_id: parent_order_id,
                oca_group: oca_group.clone(),
                oca_type: OcaType::ReduceWithBlock,
                transmit: index + 1 == legs.len(),
                override_percentage_constraints: false,
                advanced_error_override: String::new(),
                ..Default::default()
            },
        });
        children.push(BracketChild {
            kind: leg.kind,
            order_id,
            customer_order_id: leg.customer_order_id.clone(),
            price: leg.price,
        });
    }
    BracketPlan {
        parent_customer_order_id: request.customer_order_id.clone(),
        orders,
        children,
    }
}

/// Send every bracket order in plan order, keeping each order's event stream.
///
/// All sends share one submission deadline, so a bracket spends no longer
/// before its acknowledgement phase than a single order does.
///
/// # Errors
///
/// Returns the failure detail when a submission call fails or times out. Any
/// earlier order was already handed to TWS, so the caller must treat the
/// bracket as indeterminate.
async fn place_bracket_orders(
    client: &Client,
    contract: &Contract,
    plan: &BracketPlan,
) -> Result<Vec<Subscription<PlaceOrder>>, String> {
    let mut subscriptions = Vec::with_capacity(plan.orders.len());
    let deadline = tokio::time::Instant::now() + ORDER_SUBMISSION_TIMEOUT;
    for BracketOrder { order_id, order } in &plan.orders {
        match tokio::time::timeout_at(deadline, client.place_order(*order_id, contract, order))
            .await
        {
            Ok(Ok(events)) => subscriptions.push(events),
            Ok(Err(error)) => {
                return Err(format!(
                    "the bracket submission transport failed at order {order_id}: {error}"
                ));
            }
            Err(_) => {
                return Err(format!(
                    "the bracket submission call timed out at order {order_id}"
                ));
            }
        }
    }
    Ok(subscriptions)
}

/// Read IBKR's open and completed orders while no other snapshot is in flight.
///
/// # Errors
///
/// Returns the provider read failure.
async fn read_provider_orders_serialized(
    client: &Arc<Client>,
    snapshots: &Mutex<()>,
) -> Result<ProviderOrders, GatewayError> {
    let _snapshots = snapshots.lock().await;
    read_provider_orders(client).await
}

/// Submit one bracket and wait for the parent and every child to settle.
///
/// A leg whose stream a documented informational IBKR message ended (for
/// example 399, held until the session opens) is read back from IBKR's open
/// and completed orders once every undecided leg's stream has ended.
async fn submit_bracket(
    client: &Arc<Client>,
    contract: &Contract,
    plan: BracketPlan,
    account: &str,
    snapshots: &Mutex<()>,
) -> OrderResponse {
    let subscriptions = match place_bracket_orders(client, contract, &plan).await {
        Ok(subscriptions) => subscriptions,
        Err(detail) => {
            return indeterminate_response(
                account.to_string(),
                Some(plan.parent_customer_order_id.clone()),
                &detail,
            );
        }
    };
    let mut tracker = BracketTracker::new(account, &plan);
    let mut events = stream::select_all(
        subscriptions
            .into_iter()
            .enumerate()
            .map(|(leg, events)| events.map(move |event| (leg, event)).boxed()),
    );
    let finished = tokio::time::timeout(ORDER_ACKNOWLEDGEMENT_TIMEOUT, async {
        while let Some((leg, item)) = events.next().await {
            if let Some(outcome) = tracker.apply(leg, bracket_event(item)) {
                return Some(outcome);
            }
            if tracker.streams_exhausted() {
                break;
            }
        }
        None
    })
    .await;
    // Release every leg's order channel so snapshot replies reach the reader.
    drop(events);
    match finished {
        Ok(Some(outcome)) => outcome,
        Ok(None) | Err(_) if tracker.needs_follow_up() => {
            let settled = follow_up(
                ORDER_FOLLOW_UP_TIMEOUT,
                || read_provider_orders_serialized(client, snapshots),
                |orders| tracker.apply_provider_orders(orders),
            )
            .await;
            settled.unwrap_or_else(|| tracker.finish())
        }
        Ok(None) | Err(_) => tracker.finish(),
    }
}

/// Map one provider item on a bracket order's stream to a tracker event.
fn bracket_event(item: Result<SubscriptionItem<PlaceOrder>, ibapi::Error>) -> BracketEvent {
    let (notice, ended_stream) = match item {
        Ok(SubscriptionItem::Data(event)) => return BracketEvent::Data(Box::new(event)),
        Ok(SubscriptionItem::Notice(notice)) => (notice, false),
        Err(ibapi::Error::Notice(notice)) => (notice, true),
        Err(error) => return BracketEvent::StreamFailed(error.to_string()),
    };
    match classify_order_message(notice.code, &notice.message) {
        // A cancellation always forces a read-back: the leg's earlier state is stale.
        OrderMessage::FollowUp(note) if ended_stream || note.is_cancellation() => {
            BracketEvent::FollowUp(note)
        }
        OrderMessage::FollowUp(note) => BracketEvent::Note(note),
        OrderMessage::Rejected(text) => BracketEvent::Rejected(format!("[{}] {text}", notice.code)),
        OrderMessage::Unknown if ended_stream => BracketEvent::StreamFailed(format!(
            "[{}] {}",
            notice.code,
            order_messages::single_line(&notice.message)
        )),
        OrderMessage::Unknown => BracketEvent::Unexpected(format!(
            "[{}] {}",
            notice.code,
            order_messages::single_line(&notice.message)
        )),
    }
}

/// Provider event on one bracket order's stream.
#[derive(Debug)]
enum BracketEvent {
    /// Order status, open order, or execution data.
    Data(Box<PlaceOrder>),
    /// IBKR rejected this order.
    Rejected(String),
    /// IBKR attached a documented informational message; the stream continues.
    Note(ProviderNote),
    /// IBKR attached a documented informational message that ended this
    /// order's stream, or cancelled it (202), so its state must be read back
    /// from IBKR.
    FollowUp(ProviderNote),
    /// IBKR attached a message this runtime does not classify.
    Unexpected(String),
    /// The order's event stream failed.
    StreamFailed(String),
}

/// Definitive state reported for one bracket child.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ChildState {
    Working,
    Rejected,
}

/// Collects parent and child events into one normalized bracket outcome.
#[derive(Debug)]
struct BracketTracker {
    account: String,
    customer_order_id: String,
    parent_order_id: i32,
    parent: Option<OrderResponse>,
    children: Vec<(BracketChild, Option<ChildState>, Option<String>)>,
    /// Documented IBKR messages attached to the parent, kept as details.
    parent_notes: Vec<ProviderNote>,
    /// Per leg (0 is the parent): an informational message ended its stream.
    follow_up: Vec<bool>,
    /// Per leg: IBKR cancelled it (202), so only a final state settles it.
    cancelled: Vec<bool>,
    /// Per leg: its event stream ended.
    ended: Vec<bool>,
}

impl BracketTracker {
    fn new(account: &str, plan: &BracketPlan) -> Self {
        Self {
            account: account.to_string(),
            customer_order_id: plan.parent_customer_order_id.clone(),
            parent_order_id: plan.orders[0].order_id,
            parent: None,
            children: plan
                .children
                .iter()
                .cloned()
                .map(|child| (child, None, None))
                .collect(),
            parent_notes: Vec::new(),
            follow_up: vec![false; plan.children.len() + 1],
            cancelled: vec![false; plan.children.len() + 1],
            ended: vec![false; plan.children.len() + 1],
        }
    }

    /// Apply one event from stream `leg` (0 is the parent).
    ///
    /// Returns the final response as soon as it is known: at once when the
    /// parent is rejected or its outcome becomes unknowable, otherwise when the
    /// parent and every child have a definitive state.
    fn apply(&mut self, leg: usize, event: BracketEvent) -> Option<OrderResponse> {
        if leg == 0 {
            match event {
                BracketEvent::Data(event) => {
                    if self.cancelled[0] && reports_working_status(self.parent_order_id, &event) {
                        return None;
                    }
                    let outcome = order_event_outcome(
                        self.parent_order_id,
                        &self.account,
                        &self.customer_order_id,
                        *event,
                    )?;
                    if !outcome.ok {
                        return Some(with_provider_notes(outcome, &self.parent_notes));
                    }
                    self.parent = Some(outcome);
                }
                BracketEvent::Rejected(message) => {
                    return Some(definitive_order_response(
                        &self.account,
                        &self.customer_order_id,
                        self.parent_order_id,
                        false,
                        "rejected",
                        &format!("IBKR rejected the order: {message}"),
                    ));
                }
                BracketEvent::Note(note) => self.parent_notes.push(note),
                BracketEvent::FollowUp(note) => {
                    if note.is_cancellation() {
                        // An earlier working state no longer holds.
                        self.cancelled[0] = true;
                        self.parent = None;
                    }
                    self.parent_notes.push(note);
                    self.follow_up[0] = true;
                    self.ended[0] = true;
                }
                BracketEvent::Unexpected(message) if self.parent.is_none() => {
                    return Some(indeterminate_response(
                        self.account.clone(),
                        Some(self.customer_order_id.clone()),
                        &format!("IBKR returned a post-submission warning: {message}"),
                    ));
                }
                BracketEvent::StreamFailed(message) if self.parent.is_none() => {
                    return Some(indeterminate_response(
                        self.account.clone(),
                        Some(self.customer_order_id.clone()),
                        &format!("the order event stream failed: {message}"),
                    ));
                }
                BracketEvent::StreamFailed(_) => self.ended[0] = true,
                BracketEvent::Unexpected(_) => {}
            }
        } else if let Some((child, state, detail)) = self.children.get_mut(leg - 1) {
            match event {
                BracketEvent::Data(event) => {
                    if self.cancelled[leg] && reports_working_status(child.order_id, &event) {
                        return None;
                    }
                    if let Some(next) = child_event_state(child.order_id, *event) {
                        *state = Some(next);
                    }
                }
                BracketEvent::Rejected(message) => {
                    *state = Some(ChildState::Rejected);
                    *detail = Some(message);
                }
                // Informational messages never explain a child's rejection.
                BracketEvent::Note(_) => {}
                BracketEvent::FollowUp(note) => {
                    if note.is_cancellation() {
                        self.cancelled[leg] = true;
                        *state = None;
                    }
                    self.follow_up[leg] = true;
                    self.ended[leg] = true;
                }
                BracketEvent::Unexpected(message) => {
                    if *state != Some(ChildState::Rejected) {
                        *detail = Some(message);
                    }
                }
                BracketEvent::StreamFailed(message) => {
                    self.ended[leg] = true;
                    if *state != Some(ChildState::Rejected) {
                        *detail = Some(message);
                    }
                }
            }
        }
        (self.parent.is_some() && self.children.iter().all(|(_, state, _)| state.is_some()))
            .then(|| self.finish())
    }

    /// Legs (0 is the parent) without a definitive state.
    fn undecided_legs(&self) -> impl Iterator<Item = usize> + '_ {
        let parent = self.parent.is_none().then_some(0);
        let children = self
            .children
            .iter()
            .enumerate()
            .filter(|(_, (_, state, _))| state.is_none())
            .map(|(index, _)| index + 1);
        parent.into_iter().chain(children)
    }

    /// Whether some undecided leg must be read back from IBKR.
    fn needs_follow_up(&self) -> bool {
        self.undecided_legs().any(|leg| self.follow_up[leg])
    }

    /// Whether every undecided leg's stream has ended, so waiting on the
    /// remaining (settled) streams cannot change the outcome.
    fn streams_exhausted(&self) -> bool {
        let mut undecided = self.undecided_legs().peekable();
        undecided.peek().is_some() && undecided.all(|leg| self.ended[leg])
    }

    /// Settle follow-up legs from IBKR's open and completed orders.
    ///
    /// After a cancellation (202) only a final state settles a leg. Returns
    /// the final response once no follow-up leg is undecided, or at once when
    /// the parent turns out rejected.
    fn apply_provider_orders(&mut self, orders: &ProviderOrders) -> Option<OrderResponse> {
        if self.parent.is_none()
            && self.follow_up[0]
            && let Some(found) = orders.find(&self.account, &self.customer_order_id)
            && (!self.cancelled[0] || is_terminal(found.order_state.status))
        {
            match order_event_outcome(
                self.parent_order_id,
                &self.account,
                &self.customer_order_id,
                snapshot_status_event(self.parent_order_id, found),
            ) {
                Some(outcome) if !outcome.ok => {
                    self.parent_notes.extend(completed_reason(found));
                    return Some(with_provider_notes(outcome, &self.parent_notes));
                }
                Some(outcome) => self.parent = Some(outcome),
                None => {}
            }
        }
        for leg in 1..=self.children.len() {
            let (child, state, detail) = &mut self.children[leg - 1];
            if state.is_some() || !self.follow_up[leg] {
                continue;
            }
            let Some(found) = orders.find(&self.account, &child.customer_order_id) else {
                continue;
            };
            if self.cancelled[leg] && !is_terminal(found.order_state.status) {
                continue;
            }
            if let Some(next) =
                child_event_state(child.order_id, snapshot_status_event(child.order_id, found))
            {
                *state = Some(next);
                if next == ChildState::Rejected {
                    *detail = completed_reason(found).map(|reason| reason.text);
                }
            }
        }
        (!self.needs_follow_up()).then(|| self.finish())
    }

    /// Build the response from everything seen so far.
    ///
    /// An undecided parent makes the whole bracket indeterminate. An undecided
    /// child makes only its protection `unknown`.
    fn finish(&self) -> OrderResponse {
        let Some(parent) = &self.parent else {
            let detail = if self.follow_up[0] {
                format!(
                    "IBKR sent {} but did not confirm the parent order state",
                    describe_notes(&self.parent_notes)
                )
            } else {
                "the bracket acknowledgement timed out before IBKR confirmed the parent order"
                    .to_string()
            };
            return indeterminate_response(
                self.account.clone(),
                Some(self.customer_order_id.clone()),
                &detail,
            );
        };
        let filled = parent.status == "filled";
        let legs = self
            .children
            .iter()
            .map(|(child, state, _)| ProtectionLegOutcome {
                kind: child.kind,
                customer_order_id: child.customer_order_id.clone(),
                order_id: Some(child.order_id.to_string()),
                price: child.price,
                status: match state {
                    Some(ChildState::Working) if filled => ProtectionStatus::Attached,
                    Some(ChildState::Working) => ProtectionStatus::Pending,
                    Some(ChildState::Rejected) => ProtectionStatus::Rejected,
                    None => ProtectionStatus::Unknown,
                },
            })
            .collect::<Vec<_>>();
        let status = [
            ProtectionStatus::Rejected,
            ProtectionStatus::Unknown,
            ProtectionStatus::Pending,
        ]
        .into_iter()
        .find(|status| legs.iter().any(|leg| leg.status == *status))
        .unwrap_or(ProtectionStatus::Attached);
        let describe = |filter: &dyn Fn(ProtectionStatus) -> bool| {
            self.children
                .iter()
                .zip(&legs)
                .filter(|(_, leg)| filter(leg.status))
                .map(|((child, _, detail), leg)| {
                    let name = match leg.kind {
                        ProtectionLegKind::TakeProfit => "take profit",
                        ProtectionLegKind::StopLoss => "stop loss",
                    };
                    match detail {
                        Some(detail) if leg.status == ProtectionStatus::Rejected => {
                            format!("the {name} at {} ({detail})", child.price)
                        }
                        _ => format!("the {name} at {}", child.price),
                    }
                })
                .collect::<Vec<_>>()
                .join(" and ")
        };
        let message = match status {
            ProtectionStatus::Rejected => format!(
                "IBKR accepted the order but rejected {}. The position may be unprotected; check IBKR open orders",
                describe(&|status| status == ProtectionStatus::Rejected)
            ),
            ProtectionStatus::Unknown => format!(
                "IBKR accepted the order but did not confirm {}. The position may be unprotected; check IBKR open orders",
                describe(&|status| status == ProtectionStatus::Unknown)
            ),
            ProtectionStatus::Pending => {
                format!("IBKR holds {} until the order fills", describe(&|_| true))
            }
            ProtectionStatus::Attached => {
                format!("IBKR is working {}", describe(&|_| true))
            }
        };
        OrderResponse {
            protection: Some(ProtectionOutcome {
                status,
                message,
                legs,
            }),
            version: parent.version,
            provider: parent.provider,
            ok: parent.ok,
            status: parent.status.clone(),
            message: order_messages::annotated_message(
                &parent.message,
                parent.ok,
                &self.parent_notes,
            ),
            account_ref: parent.account_ref.clone(),
            order_id: parent.order_id.clone(),
            customer_order_id: parent.customer_order_id.clone(),
            exit: None,
        }
    }
}

/// Map one provider event for a bracket child to its definitive state.
///
/// A held child reports `PreSubmitted` until its parent fills, and IBKR keeps
/// stop children `PreSubmitted` until they trigger, so both working statuses
/// count as accepted. `PendingSubmit` is not yet an acceptance: IBKR can still
/// move the child to `Inactive` or reject it, so it stays undecided (and is
/// reported `unknown` if nothing better arrives before the deadline).
/// Cancellation, inactivity, or rejection mean the child is not protecting
/// the position.
fn child_event_state(order_id: i32, event: PlaceOrder) -> Option<ChildState> {
    let status = match event {
        PlaceOrder::OrderStatus(status) if status.order_id == order_id => status.status,
        PlaceOrder::OpenOrder(order) if order.order_id == order_id => order.order_state.status,
        PlaceOrder::ExecutionData(execution) if execution.execution.order_id == order_id => {
            return Some(ChildState::Working);
        }
        _ => return None,
    };
    match status {
        OrderStatusKind::PreSubmitted | OrderStatusKind::Submitted | OrderStatusKind::Filled => {
            Some(ChildState::Working)
        }
        OrderStatusKind::Cancelled | OrderStatusKind::ApiCancelled | OrderStatusKind::Inactive => {
            Some(ChildState::Rejected)
        }
        OrderStatusKind::PendingSubmit
        | OrderStatusKind::ApiPending
        | OrderStatusKind::PendingCancel => None,
    }
}

fn order_event_outcome(
    order_id: i32,
    account: &str,
    customer_order_id: &str,
    event: PlaceOrder,
) -> Option<OrderResponse> {
    match event {
        PlaceOrder::OrderStatus(status) if status.order_id == order_id => {
            let (ok, normalized, message) = match status.status {
                OrderStatusKind::Filled if status.filled > 0.0 && status.remaining <= 0.0 => {
                    (true, "filled", "IBKR order filled")
                }
                OrderStatusKind::Filled => (
                    true,
                    "accepted",
                    "IBKR accepted an execution without confirming the full fill",
                ),
                OrderStatusKind::Submitted | OrderStatusKind::PreSubmitted => {
                    (true, "resting", "IBKR order is working")
                }
                OrderStatusKind::PendingSubmit => (true, "accepted", "IBKR order accepted"),
                OrderStatusKind::Cancelled
                | OrderStatusKind::ApiCancelled
                | OrderStatusKind::Inactive
                    if status.filled > 0.0 =>
                {
                    (
                        true,
                        "accepted",
                        "IBKR reported a partial execution before cancellation",
                    )
                }
                OrderStatusKind::Cancelled
                | OrderStatusKind::ApiCancelled
                | OrderStatusKind::Inactive => {
                    (false, "rejected", "IBKR order rejected or cancelled")
                }
                OrderStatusKind::ApiPending | OrderStatusKind::PendingCancel => return None,
            };
            Some(definitive_order_response(
                account,
                customer_order_id,
                order_id,
                ok,
                normalized,
                message,
            ))
        }
        PlaceOrder::OpenOrder(order) if order.order_id == order_id => {
            let status = order.order_state.status;
            match status {
                OrderStatusKind::Submitted | OrderStatusKind::PreSubmitted => {
                    Some(definitive_order_response(
                        account,
                        customer_order_id,
                        order_id,
                        true,
                        "resting",
                        "IBKR order is working",
                    ))
                }
                OrderStatusKind::Cancelled
                | OrderStatusKind::ApiCancelled
                | OrderStatusKind::Inactive => Some(definitive_order_response(
                    account,
                    customer_order_id,
                    order_id,
                    false,
                    "rejected",
                    "IBKR order rejected or cancelled",
                )),
                _ => None,
            }
        }
        PlaceOrder::ExecutionData(execution) if execution.execution.order_id == order_id => {
            Some(definitive_order_response(
                account,
                customer_order_id,
                order_id,
                true,
                "accepted",
                "IBKR reported an execution without confirming the full fill",
            ))
        }
        _ => None,
    }
}

fn definitive_order_response(
    account: &str,
    customer_order_id: &str,
    order_id: i32,
    ok: bool,
    status: &str,
    message: &str,
) -> OrderResponse {
    OrderResponse {
        version: PROTOCOL_VERSION,
        provider: "ibkr",
        ok,
        status: status.to_string(),
        message: format!("{message} ({order_id})"),
        account_ref: Some(account.to_string()),
        order_id: Some(order_id.to_string()),
        customer_order_id: Some(customer_order_id.to_string()),
        protection: None,
        exit: None,
    }
}

fn indeterminate_response(
    account: String,
    customer_order_id: Option<String>,
    detail: &str,
) -> OrderResponse {
    // Provider text can span lines and end with its own period; keep the
    // reason one readable sentence for the reconciliation lock.
    let detail = order_messages::single_line(detail);
    OrderResponse {
        version: PROTOCOL_VERSION,
        provider: "ibkr",
        ok: false,
        status: "indeterminate".to_string(),
        message: format!(
            "IBKR order outcome is unknown: {detail}. Verify provider state before submitting again"
        ),
        account_ref: Some(account),
        order_id: None,
        customer_order_id,
        protection: None,
        exit: None,
    }
}

fn provider_error(label: &str, error: ibapi::Error) -> GatewayError {
    let message = format!("IBKR {label} failed: {error}");
    match error {
        ibapi::Error::Io(_)
        | ibapi::Error::ConnectionFailed
        | ibapi::Error::ConnectionRejected(_)
        | ibapi::Error::ConnectionReset
        | ibapi::Error::Shutdown
        | ibapi::Error::EndOfStream
        | ibapi::Error::UnexpectedEndOfStream => GatewayError::Unavailable(message),
        _ => GatewayError::Provider(message),
    }
}

/// Classifies a provider failure where IBKR may report an unknown contract.
///
/// IBKR code 200 means no security definition matches the request. That is
/// a contract failure retrying cannot fix, unlike other provider rejections.
fn contract_lookup_error(label: &str, error: ibapi::Error) -> GatewayError {
    if let ibapi::Error::Notice(notice) = &error
        && notice.code == NO_SECURITY_DEFINITION_CODE
    {
        return GatewayError::Contract(format!("IBKR {label} failed: {error}"));
    }
    provider_error(label, error)
}

/// Whether a stock quote stream should resubscribe after `error`.
///
/// Invalid requests and contracts IBKR cannot resolve are permanent. Every
/// other failure, including provider rejections such as a competing live
/// session or a missing Gateway configuration, can clear on its own, so the
/// stream retries with bounded backoff.
///
/// # Arguments
///
/// * `error` - Failure that ended the subscription or its setup.
///
/// # Returns
///
/// False only for input and contract errors.
pub fn should_retry_stock_quote(error: &GatewayError) -> bool {
    !matches!(error, GatewayError::Input(_) | GatewayError::Contract(_))
}

/// Identifies IBKR's overloaded historical-data error that means success with no bars.
fn is_empty_historical_data_error(error: &ibapi::Error) -> bool {
    match error {
        ibapi::Error::Notice(notice) => {
            notice.code == HISTORICAL_DATA_ERROR_CODE
                && notice.message.contains(HISTORICAL_NO_DATA_MESSAGE)
        }
        _ => false,
    }
}

/// IB Gateway configuration, input, availability, provider, or response-contract error.
#[derive(Debug)]
pub enum GatewayError {
    /// Gateway startup configuration is unsafe.
    Configuration(String),
    /// Browser or renderer input is invalid for the narrow local contract.
    Input(String),
    /// IB Gateway is unavailable because its local connection failed.
    Unavailable(String),
    /// An IBKR request failed without losing the local Gateway connection.
    Provider(String),
    /// Provider data did not match the expected normalized contract.
    Contract(String),
}

impl Display for GatewayError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Configuration(message)
            | Self::Input(message)
            | Self::Unavailable(message)
            | Self::Provider(message)
            | Self::Contract(message) => formatter.write_str(message),
        }
    }
}

impl std::error::Error for GatewayError {}

#[cfg(test)]
mod tests {
    use super::*;

    fn summary_row(account: &str, tag: &str, value: &str, currency: &str) -> AccountSummary {
        AccountSummary {
            account: account.to_string(),
            tag: tag.to_string(),
            value: value.to_string(),
            currency: currency.to_string(),
        }
    }

    fn apply_rows(state: &mut AccountSummaryState, rows: &[AccountSummary]) {
        for row in rows {
            state.apply(row, "DU123456").expect("valid summary row");
        }
    }

    fn assert_close(actual: Option<f64>, expected: f64) {
        let actual = actual.expect("value is present");
        assert!(
            (actual - expected).abs() < 1e-9,
            "expected {expected}, got {actual}"
        );
    }

    #[test]
    fn summarizes_a_usd_account_and_ignores_other_accounts() {
        let mut state = AccountSummaryState::default();
        apply_rows(
            &mut state,
            &[
                summary_row("DU123456", "NetLiquidation", "26.73", "USD"),
                summary_row("DU123456", "TotalCashValue", "-4.10", "USD"),
                summary_row("DU123456", "AvailableFunds", "20.00", "USD"),
                summary_row("DU123456", "GrossPositionValue", "30.83", "USD"),
                summary_row("DU123456", "ExchangeRate", "1.00", "USD"),
                summary_row("DU654321", "NetLiquidation", "999.00", "USD"),
                summary_row("DU123456", "BuyingPower", "80.00", "USD"),
            ],
        );

        let balance = state.balance("DU123456").expect("USD account is available");
        assert_eq!(balance.account_ref, "DU123456");
        assert_eq!(balance.balance_usd, 26.73);
        assert_eq!(
            balance.summary,
            AccountValueSummary {
                net_liquidation_usd: Some(26.73),
                total_cash_usd: Some(-4.10),
                available_funds_usd: Some(20.0),
                gross_position_value_usd: Some(30.83),
                native_currency: Some("USD".to_string()),
                native_net_liquidation: Some(26.73),
                fx_rate_to_usd: Some(1.0),
            }
        );
    }

    #[test]
    fn converts_a_non_usd_base_currency_with_the_usd_ledger_rate() {
        let mut state = AccountSummaryState::default();
        apply_rows(
            &mut state,
            &[
                summary_row("DU123456", "NetLiquidation", "1000", "GBP"),
                summary_row("DU123456", "TotalCashValue", "200", "GBP"),
                summary_row("DU123456", "AvailableFunds", "150", "GBP"),
                summary_row("DU123456", "GrossPositionValue", "800", "GBP"),
                // Read as 0.8 GBP per dollar (see `usd_per_base_unit`), so
                // one GBP is 1.25 USD.
                summary_row("DU123456", "ExchangeRate", "0.8", "USD"),
                // Other ledgers' rates price other currencies and are ignored.
                summary_row("DU123456", "ExchangeRate", "0.86", "EUR"),
                summary_row("DU123456", "CashBalance", "50", "USD"),
            ],
        );

        let balance = state.balance("DU123456").expect("rate converts GBP");
        let summary = balance.summary;
        assert_close(Some(balance.balance_usd), 1250.0);
        assert_close(summary.net_liquidation_usd, 1250.0);
        assert_close(summary.total_cash_usd, 250.0);
        assert_close(summary.available_funds_usd, 187.5);
        assert_close(summary.gross_position_value_usd, 1000.0);
        assert_close(summary.fx_rate_to_usd, 1.25);
        assert_eq!(summary.native_currency.as_deref(), Some("GBP"));
        assert_eq!(summary.native_net_liquidation, Some(1000.0));
    }

    #[test]
    fn reads_the_usd_rate_with_or_without_the_ledger_tag_prefix() {
        // TWS/IB Gateway 10.47+ can send per-currency ledger rows as
        // `$LEDGER-ExchangeRate`; the unprefixed name is the older default.
        for rate_tag in ["ExchangeRate", "$LEDGER-ExchangeRate"] {
            let mut state = AccountSummaryState::default();
            apply_rows(
                &mut state,
                &[
                    summary_row("DU123456", "NetLiquidation", "1000", "GBP"),
                    summary_row("DU123456", "TotalCashValue", "200", "GBP"),
                    summary_row("DU123456", rate_tag, "0.8", "USD"),
                    // A prefixed rate for another ledger still prices another
                    // currency and is ignored.
                    summary_row("DU123456", "$LEDGER-ExchangeRate", "0.86", "EUR"),
                ],
            );
            let balance = state.balance("DU123456").expect("rate converts GBP");
            assert_close(Some(balance.balance_usd), 1250.0);
            assert_close(balance.summary.total_cash_usd, 250.0);
            assert_close(balance.summary.fx_rate_to_usd, 1.25);
        }
    }

    #[test]
    fn prefixed_ledger_rows_never_stand_in_for_account_level_values() {
        let mut state = AccountSummaryState::default();
        apply_rows(
            &mut state,
            &[
                summary_row("DU123456", "NetLiquidation", "1000", "USD"),
                // Per-currency ledger values with the 10.47+ prefix are not the
                // account-level tags, even where the names would match.
                summary_row("DU123456", "$LEDGER-NetLiquidation", "5", "USD"),
                summary_row("DU123456", "$LEDGER-TotalCashValue", "5", "USD"),
                summary_row("DU123456", "$LEDGER-CashBalance", "5", "USD"),
                summary_row("DU123456", "$LEDGER-NetLiquidationByCurrency", "5", "USD"),
            ],
        );
        let summary = state.summary();
        assert_close(summary.net_liquidation_usd, 1000.0);
        assert_eq!(summary.total_cash_usd, None);
        assert_eq!(ledger_tag_name("$LEDGER-ExchangeRate"), "ExchangeRate");
        assert_eq!(ledger_tag_name("ExchangeRate"), "ExchangeRate");
        assert_eq!(ledger_tag_name("NetLiquidation"), "NetLiquidation");
    }

    #[test]
    fn refuses_to_relabel_a_non_usd_balance_without_a_rate() {
        let mut state = AccountSummaryState::default();
        apply_rows(
            &mut state,
            &[
                summary_row("DU123456", "NetLiquidation", "1000", "EUR"),
                summary_row("DU123456", "AvailableFunds", "150", "EUR"),
            ],
        );
        let summary = state.summary();
        assert_eq!(summary.net_liquidation_usd, None);
        assert_eq!(summary.available_funds_usd, None);
        assert_eq!(summary.fx_rate_to_usd, None);
        assert_eq!(summary.native_net_liquidation, Some(1000.0));
        assert!(matches!(
            state.balance("DU123456"),
            Err(GatewayError::Unavailable(message))
                if message == "IBKR reported net liquidation in EUR without a USD exchange rate"
        ));

        // A zero rate is not a usable price either.
        apply_rows(
            &mut state,
            &[summary_row("DU123456", "ExchangeRate", "0", "USD")],
        );
        assert!(state.balance("DU123456").is_err());

        let empty = AccountSummaryState::default();
        assert!(matches!(
            empty.balance("DU123456"),
            Err(GatewayError::Unavailable(message))
                if message == "IBKR did not report net liquidation for the account"
        ));
    }

    #[test]
    fn converts_pence_quoted_sterling_as_hundredths_of_a_pound() {
        let mut state = AccountSummaryState::default();
        apply_rows(
            &mut state,
            &[
                summary_row("DU123456", "NetLiquidation", "1000", "GBP"),
                summary_row("DU123456", "GrossPositionValue", "40000", "GBp"),
                summary_row("DU123456", "AvailableFunds", "10000", "GBX"),
                summary_row("DU123456", "ExchangeRate", "0.8", "USD"),
            ],
        );
        let summary = state.summary();
        assert_close(summary.net_liquidation_usd, 1250.0);
        assert_close(summary.gross_position_value_usd, 500.0);
        assert_close(summary.available_funds_usd, 125.0);
        assert_close(state.usd_per_unit("GBp"), 0.0125);
        // Pence of a currency other than the base still has no rate.
        assert_eq!(state.usd_per_unit("EUR"), None);
    }

    #[test]
    fn resolves_currency_labels_to_major_units() {
        assert_eq!(currency_major_unit("USD"), Some(("USD".to_string(), 1.0)));
        assert_eq!(currency_major_unit(" gbp "), Some(("GBP".to_string(), 1.0)));
        assert_eq!(currency_major_unit("GBp"), Some(("GBP".to_string(), 100.0)));
        assert_eq!(currency_major_unit("GBX"), Some(("GBP".to_string(), 100.0)));
        assert_eq!(currency_major_unit("gbx"), Some(("GBP".to_string(), 100.0)));
        assert_eq!(currency_major_unit("BASE"), None);
        assert_eq!(currency_major_unit(""), None);
        assert_eq!(currency_major_unit("U$D"), None);
    }

    #[test]
    fn rejects_malformed_net_liquidation_and_clears_blank_values() {
        let mut state = AccountSummaryState::default();
        assert!(matches!(
            state.apply(
                &summary_row("DU123456", "NetLiquidation", "not-a-number", "USD"),
                "DU123456",
            ),
            Err(GatewayError::Contract(message))
                if message == "IBKR returned an invalid NetLiquidation account value"
        ));
        assert!(matches!(
            state.apply(
                &summary_row("DU123456", "NetLiquidation", "inf", "USD"),
                "DU123456",
            ),
            Err(GatewayError::Contract(message))
                if message == "IBKR returned a non-finite NetLiquidation account value"
        ));

        assert!(
            state
                .apply(
                    &summary_row("DU123456", "AvailableFunds", "10", "USD"),
                    "DU123456"
                )
                .unwrap()
        );
        assert!(
            !state
                .apply(
                    &summary_row("DU123456", "AvailableFunds", "10", "USD"),
                    "DU123456"
                )
                .unwrap()
        );
        assert!(
            state
                .apply(
                    &summary_row("DU123456", "AvailableFunds", " ", "USD"),
                    "DU123456"
                )
                .unwrap()
        );
        assert_eq!(state.summary().available_funds_usd, None);
    }

    #[test]
    fn keeps_net_liquidation_available_when_a_secondary_value_is_malformed() {
        let mut state = AccountSummaryState::default();
        apply_rows(
            &mut state,
            &[
                summary_row("DU123456", "NetLiquidation", "1000", "USD"),
                summary_row("DU123456", "TotalCashValue", "-250", "USD"),
                summary_row("DU123456", "AvailableFunds", "not-a-number", "USD"),
                summary_row("DU123456", "GrossPositionValue", "inf", "USD"),
                summary_row("DU123456", "ExchangeRate", "garbage", "USD"),
            ],
        );

        let balance = state
            .balance("DU123456")
            .expect("a bad secondary value keeps the balance available");
        assert_eq!(balance.balance_usd, 1000.0);
        assert_eq!(balance.summary.total_cash_usd, Some(-250.0));
        assert_eq!(balance.summary.available_funds_usd, None);
        assert_eq!(balance.summary.gross_position_value_usd, None);
        assert_eq!(balance.summary.fx_rate_to_usd, Some(1.0));

        // A malformed value replaces an earlier good one, so a stale number is
        // never shown as current.
        assert!(
            state
                .apply(
                    &summary_row("DU123456", "TotalCashValue", "NaN-ish", "USD"),
                    "DU123456"
                )
                .expect("secondary values never fail the stream")
        );
        assert_eq!(state.summary().total_cash_usd, None);
    }

    #[tokio::test]
    async fn a_malformed_secondary_value_does_not_end_the_initial_snapshot() {
        let mut state = AccountSummaryState::default();
        let mut initial = summary_stream(vec![
            AccountSummaryResult::Summary(summary_row("DU123456", "NetLiquidation", "500", "USD")),
            AccountSummaryResult::Summary(summary_row("DU123456", "AvailableFunds", "n/a", "USD")),
            AccountSummaryResult::End,
        ]);
        assert!(
            apply_until_snapshot_end(&mut initial, &mut state, "DU123456")
                .await
                .expect("malformed available funds is not a contract error")
        );
        let balance = state.balance("DU123456").expect("net liquidation is valid");
        assert_eq!(balance.balance_usd, 500.0);
        assert_eq!(balance.summary.available_funds_usd, None);
    }

    #[test]
    fn a_malformed_usd_rate_leaves_a_non_usd_account_unavailable_not_failed() {
        let mut state = AccountSummaryState::default();
        apply_rows(
            &mut state,
            &[
                summary_row("DU123456", "NetLiquidation", "1000", "GBP"),
                summary_row("DU123456", "ExchangeRate", "x", "USD"),
            ],
        );
        // Unavailable is retried by both runtimes; a contract error is not.
        assert!(matches!(
            state.balance("DU123456"),
            Err(GatewayError::Unavailable(_))
        ));
    }

    #[test]
    fn reads_the_usd_ledger_rate_through_one_conversion_function() {
        // Under the documented assumption, 0.8 base units per dollar is 1.25
        // dollars per base unit. Inverting the assumption changes only
        // `usd_per_base_unit`, and this test with it.
        assert_close(usd_per_base_unit(0.8), 1.25);
        assert_eq!(usd_per_base_unit(0.0), None);
        assert_eq!(usd_per_base_unit(-1.0), None);
        assert_eq!(usd_per_base_unit(1e-320), None);
    }

    #[test]
    fn requests_every_tracked_value_tag_and_the_usd_ledger() {
        for tag in ACCOUNT_SUMMARY_VALUE_TAGS {
            assert!(
                ACCOUNT_SUMMARY_REQUEST_TAGS.contains(&tag),
                "{tag} is tracked but never requested"
            );
        }
        assert!(ACCOUNT_SUMMARY_REQUEST_TAGS.contains(&USD_LEDGER_TAG));
        assert_eq!(
            ACCOUNT_SUMMARY_REQUEST_TAGS.len(),
            ACCOUNT_SUMMARY_VALUE_TAGS.len() + 1
        );
    }

    fn summary_stream(
        rows: Vec<AccountSummaryResult>,
    ) -> impl Stream<Item = Result<AccountSummaryResult, ibapi::Error>> + Unpin {
        stream::iter(rows.into_iter().map(Ok))
    }

    #[tokio::test]
    async fn collects_the_initial_snapshot_then_reports_only_changes() {
        let mut state = AccountSummaryState::default();
        let mut initial = summary_stream(vec![
            AccountSummaryResult::Summary(summary_row("DU123456", "NetLiquidation", "100", "USD")),
            AccountSummaryResult::Summary(summary_row("DU123456", "AvailableFunds", "40", "USD")),
            AccountSummaryResult::End,
        ]);
        assert!(
            apply_until_snapshot_end(&mut initial, &mut state, "DU123456")
                .await
                .unwrap()
        );
        assert_eq!(state.summary().available_funds_usd, Some(40.0));

        let mut updates = summary_stream(vec![
            AccountSummaryResult::Summary(summary_row("DU123456", "NetLiquidation", "100", "USD")),
            AccountSummaryResult::Summary(summary_row("DU123456", "CashBalance", "5", "USD")),
            AccountSummaryResult::End,
            AccountSummaryResult::Summary(summary_row("DU123456", "TotalCashValue", "12", "USD")),
        ]);
        assert!(
            apply_until_account_change(&mut updates, &mut state, "DU123456")
                .await
                .unwrap()
        );
        assert_eq!(state.summary().total_cash_usd, Some(12.0));
        assert!(
            !apply_until_account_change(&mut updates, &mut state, "DU123456")
                .await
                .unwrap()
        );

        let mut closed = summary_stream(Vec::new());
        assert!(
            !apply_until_snapshot_end(&mut closed, &mut state, "DU123456")
                .await
                .unwrap()
        );
    }

    #[test]
    fn selects_one_managed_account_or_requires_an_explicit_choice() {
        let single = vec!["DU123456".to_string()];
        assert_eq!(
            select_account(&single, None).expect("one account is unambiguous"),
            "DU123456"
        );

        let multiple = vec!["DU123456".to_string(), "DU654321".to_string()];
        assert!(matches!(
            select_account(&multiple, None),
            Err(GatewayError::Input(message))
                if message == "select one of the accounts returned by IBKR"
        ));
        assert_eq!(
            select_account(&multiple, Some("DU654321")).expect("managed account is selectable"),
            "DU654321"
        );
        assert!(matches!(
            select_account(&multiple, Some("DU000000")),
            Err(GatewayError::Input(message))
                if message == "configured account is not returned by IBKR"
        ));
    }

    #[test]
    fn routes_resolved_stock_market_data_through_smart() {
        let contract = Contract {
            contract_id: 123_456,
            symbol: "INBX".into(),
            security_type: SecurityType::Stock,
            exchange: "BEST".into(),
            primary_exchange: "NASDAQ".into(),
            currency: "USD".into(),
            ..Default::default()
        };

        let routed = route_stock_market_data_contract(contract);

        assert_eq!(routed.contract_id, 123_456);
        assert_eq!(routed.exchange.as_str(), "SMART");
        assert_eq!(routed.primary_exchange.as_str(), "NASDAQ");

        let direct = Contract {
            exchange: "ARCA".into(),
            ..Default::default()
        };
        assert_eq!(
            route_stock_market_data_contract(direct).exchange.as_str(),
            "ARCA"
        );
    }

    #[test]
    fn classifies_only_hmds_no_data_as_an_empty_history_result() {
        let no_data = ibapi::Error::Notice(ibapi::Notice {
            code: 162,
            message: "Historical Market Data Service error message:HMDS query returned no data: INBX@SMART Trades".to_string(),
            error_time: None,
            advanced_order_reject_json: String::new(),
        });
        let pacing = ibapi::Error::Notice(ibapi::Notice {
            code: 162,
            message: "Historical data request pacing violation".to_string(),
            error_time: None,
            advanced_order_reject_json: String::new(),
        });
        let rejected = ibapi::Error::Notice(ibapi::Notice {
            code: 162,
            message: "Historical Market Data Service error message:No market data permissions"
                .to_string(),
            error_time: None,
            advanced_order_reject_json: String::new(),
        });

        assert!(is_empty_historical_data_error(&no_data));
        assert!(!is_empty_historical_data_error(&pacing));
        assert!(!is_empty_historical_data_error(&rejected));
        assert!(!is_empty_historical_data_error(
            &ibapi::Error::ConnectionReset
        ));
    }

    #[tokio::test]
    async fn keeps_account_summary_open_after_the_initial_snapshot() {
        let net_liquidation =
            |account: &str, value: &str| {
                Ok::<AccountSummaryResult, ibapi::Error>(AccountSummaryResult::Summary(
                    summary_row(account, "NetLiquidation", value, "USD"),
                ))
            };
        let mut summaries = stream::iter(vec![
            Ok::<AccountSummaryResult, ibapi::Error>(AccountSummaryResult::End),
            net_liquidation("DU999999", "999.00"),
            net_liquidation("DU123456", "26.73"),
            net_liquidation("DU123456", "27.50"),
        ]);
        let mut state = AccountSummaryState::default();

        assert!(
            apply_until_snapshot_end(&mut summaries, &mut state, "DU123456")
                .await
                .unwrap()
        );
        assert!(
            apply_until_account_change(&mut summaries, &mut state, "DU123456")
                .await
                .unwrap()
        );
        let first = state.balance("DU123456").expect("first streamed value");
        assert!(
            apply_until_account_change(&mut summaries, &mut state, "DU123456")
                .await
                .unwrap()
        );
        let second = state.balance("DU123456").expect("changed streamed value");

        assert_eq!(first.balance_usd, 26.73);
        assert_eq!(second.balance_usd, 27.50);
    }

    fn order_request() -> OrderRequest {
        OrderRequest {
            version: PROTOCOL_VERSION,
            connection_id: Some("connection-1".to_string()),
            provider: "ibkr".to_string(),
            instrument: "756733".to_string(),
            instrument_kind: InstrumentKind::Stock,
            side: "buy".to_string(),
            size_usd: 1_000.0,
            quantity: Some(1.0),
            order_type: "limit".to_string(),
            limit_price: Some(650.25),
            stop_price: None,
            time_in_force: "ioc".to_string(),
            market_slippage_bps: Some(25),
            trailing_percent: None,
            limit_offset: None,
            outside_rth: false,
            algo: None,
            reduce_only: false,
            post_only: false,
            account_ref: Some("U1234567".to_string()),
            customer_order_id: CUSTOMER_ORDER_ID.to_string(),
            protection: None,
        }
    }

    const CUSTOMER_ORDER_ID: &str = "sunk-0f8b6f1e-4c1d-4a53-9a3e-2d7f5c9b8e21";
    const OTHER_CUSTOMER_ORDER_ID: &str = "sunk-7a1c2e3d-5b6f-4a8b-9c0d-1e2f3a4b5c6d";

    fn test_gateway() -> Gateway {
        Gateway::new(GatewayConfig::standard(DEFAULT_BRIDGE_CLIENT_ID).expect("config"))
            .expect("gateway")
    }

    fn reconcile_request(customer_order_id: &str, release: bool) -> ReconcileOrderRequest {
        ReconcileOrderRequest {
            version: PROTOCOL_VERSION,
            connection_id: Some("connection-1".to_string()),
            provider: "ibkr".to_string(),
            account_ref: "U1234567".to_string(),
            customer_order_id: customer_order_id.to_string(),
            release,
        }
    }

    fn provider_order(
        order_ref: &str,
        account: &str,
        status: OrderStatusKind,
        filled_quantity: f64,
    ) -> OrderData {
        OrderData {
            order_id: 42,
            order: Order {
                order_ref: order_ref.to_string(),
                account: account.to_string(),
                filled_quantity,
                ..Default::default()
            },
            order_state: ibapi::orders::OrderState {
                status,
                ..Default::default()
            },
            ..Default::default()
        }
    }

    #[test]
    fn customer_order_ids_must_be_prefixed_lowercase_uuids() {
        assert!(validate_customer_order_id(CUSTOMER_ORDER_ID).is_ok());
        for invalid in [
            "",
            "sunk-test",
            "0f8b6f1e-4c1d-4a53-9a3e-2d7f5c9b8e21",
            "sunk-0F8B6F1E-4C1D-4A53-9A3E-2D7F5C9B8E21",
            "sunk-0f8b6f1e4c1d4a539a3e2d7f5c9b8e21",
            "sunk-0f8b6f1e-4c1d-4a53-9a3e-2d7f5c9b8e21 ",
        ] {
            assert!(
                matches!(
                    validate_customer_order_id(invalid),
                    Err(GatewayError::Input(_))
                ),
                "{invalid:?} must be rejected"
            );
        }
        let mut request = order_request();
        request.customer_order_id = "sunk-test".to_string();
        assert!(matches!(
            validate_order_request(&request),
            Err(GatewayError::Input(message)) if message.contains("customerOrderId")
        ));
    }

    #[test]
    fn order_reference_reconciliation_requires_an_exact_match() {
        let orders = vec![
            provider_order(
                OTHER_CUSTOMER_ORDER_ID,
                "U1234567",
                OrderStatusKind::Submitted,
                0.0,
            ),
            provider_order(
                &CUSTOMER_ORDER_ID.to_uppercase(),
                "U1234567",
                OrderStatusKind::Submitted,
                0.0,
            ),
            provider_order(
                CUSTOMER_ORDER_ID,
                "U7654321",
                OrderStatusKind::Submitted,
                0.0,
            ),
        ];
        assert_eq!(
            match_order_reference(
                &orders,
                "U1234567",
                CUSTOMER_ORDER_ID,
                ReconcileOrderState::Open,
            ),
            None
        );
        assert_eq!(
            match_order_reference(
                &[],
                "U1234567",
                CUSTOMER_ORDER_ID,
                ReconcileOrderState::Open
            ),
            None
        );

        let mut with_match = orders;
        with_match.push(provider_order(
            CUSTOMER_ORDER_ID,
            "U1234567",
            OrderStatusKind::PreSubmitted,
            0.0,
        ));
        assert_eq!(
            match_order_reference(
                &with_match,
                "U1234567",
                CUSTOMER_ORDER_ID,
                ReconcileOrderState::Open,
            ),
            Some(FoundOrderReference {
                state: ReconcileOrderState::Open,
                status: "resting",
                order_id: Some(42),
            })
        );
    }

    #[test]
    fn completed_order_reconciliation_keeps_partial_fills_accepted() {
        let cases = [
            (OrderStatusKind::Filled, 1.0, "filled"),
            (OrderStatusKind::Cancelled, 0.5, "accepted"),
            (OrderStatusKind::Cancelled, 0.0, "rejected"),
            (OrderStatusKind::Inactive, 0.0, "rejected"),
            (OrderStatusKind::ApiPending, 0.0, "accepted"),
        ];
        for (status, filled, expected) in cases {
            let order = provider_order(CUSTOMER_ORDER_ID, "", status, filled);
            let found = match_order_reference(
                std::slice::from_ref(&order),
                "U1234567",
                CUSTOMER_ORDER_ID,
                ReconcileOrderState::Completed,
            )
            .expect("an empty provider account keeps the unique reference match");
            assert_eq!(found.status, expected, "{status:?} with {filled} filled");
        }
    }

    #[test]
    fn execution_reconciliation_requires_the_exact_order_reference() {
        let execution = |order_reference: &str, account: &str| ExecutionData {
            execution: ibapi::orders::Execution {
                order_id: 42,
                order_reference: order_reference.to_string(),
                account_number: account.to_string(),
                ..Default::default()
            },
            ..Default::default()
        };
        assert_eq!(
            match_execution_reference(
                &[
                    execution(OTHER_CUSTOMER_ORDER_ID, "U1234567"),
                    execution(CUSTOMER_ORDER_ID, "U7654321"),
                ],
                "U1234567",
                CUSTOMER_ORDER_ID,
            ),
            None
        );
        assert_eq!(
            match_execution_reference(
                &[execution(CUSTOMER_ORDER_ID, "U1234567")],
                "U1234567",
                CUSTOMER_ORDER_ID,
            ),
            Some(FoundOrderReference {
                state: ReconcileOrderState::Executed,
                status: "accepted",
                order_id: Some(42),
            })
        );
    }

    #[tokio::test]
    async fn unresolved_account_block_returns_the_earlier_reference_without_submitting() {
        let gateway = test_gateway();
        gateway
            .pending_orders
            .lock()
            .await
            .insert("U1234567".to_string(), OTHER_CUSTOMER_ORDER_ID.to_string());

        let response = gateway
            .order(order_request())
            .await
            .expect("blocked orders return before connecting");

        assert_eq!(response.status, "indeterminate");
        assert_eq!(
            response.customer_order_id.as_deref(),
            Some(OTHER_CUSTOMER_ORDER_ID)
        );
        assert!(response.message.contains("was not sent"));
    }

    #[tokio::test]
    async fn operator_release_drops_only_the_matching_local_block() {
        let gateway = test_gateway();
        gateway
            .pending_orders
            .lock()
            .await
            .insert("U1234567".to_string(), CUSTOMER_ORDER_ID.to_string());

        let other = gateway
            .reconcile_order(reconcile_request(OTHER_CUSTOMER_ORDER_ID, true))
            .await
            .expect("release does not query IBKR");
        assert_eq!(other.state, ReconcileOrderState::Released);
        assert_eq!(
            gateway
                .pending_orders
                .lock()
                .await
                .get("U1234567")
                .map(String::as_str),
            Some(CUSTOMER_ORDER_ID)
        );

        gateway
            .reconcile_order(reconcile_request(CUSTOMER_ORDER_ID, true))
            .await
            .expect("release does not query IBKR");
        assert!(gateway.pending_orders.lock().await.is_empty());
    }

    #[tokio::test]
    async fn reconciliation_validates_the_order_reference_before_provider_access() {
        let gateway = test_gateway();
        let mut request = reconcile_request("sunk-test", false);
        assert!(matches!(
            gateway.reconcile_order(request).await,
            Err(GatewayError::Input(_))
        ));
        request = reconcile_request(CUSTOMER_ORDER_ID, false);
        request.account_ref = " ".to_string();
        assert!(matches!(
            gateway.reconcile_order(request).await,
            Err(GatewayError::Input(_))
        ));
    }

    #[test]
    fn standard_configuration_prefers_paper_and_uses_distinct_client_ids() {
        let bridge = GatewayConfig::standard(DEFAULT_BRIDGE_CLIENT_ID).expect("bridge config");
        let desktop = GatewayConfig::standard(DEFAULT_DESKTOP_CLIENT_ID).expect("desktop config");
        assert_eq!(bridge.addresses()[0].port(), 4002);
        assert_eq!(bridge.addresses()[1].port(), 4001);
        assert_ne!(bridge.client_id(), desktop.client_id());
        assert_ne!(bridge.client_id(), bridge.detection_client_id());
        assert_ne!(desktop.client_id(), desktop.detection_client_id());
        assert_ne!(bridge.detection_client_id(), desktop.detection_client_id());
    }

    #[test]
    fn configuration_rejects_non_loopback_and_client_zero() {
        assert!(GatewayConfig::new(vec!["0.0.0.0:4002".parse().unwrap()], 1).is_err());
        assert!(GatewayConfig::standard(0).is_err());
        assert!(GatewayConfig::standard(i32::MAX).is_err());
        assert!(parse_gateway_address("localhost:4002").is_err());
    }

    #[test]
    fn order_builder_keeps_precaution_overrides_disabled() {
        let order = build_order(&order_request(), "U1234567", "sunk-test");
        assert!(order.transmit);
        assert!(!order.override_percentage_constraints);
        assert!(order.advanced_error_override.is_empty());
        assert_eq!(order.order_ref, "sunk-test");
        assert_eq!(order.account, "U1234567");
    }

    #[test]
    fn cash_sized_stock_order_uses_cash_quantity() {
        let mut request = order_request();
        request.quantity = None;
        request.order_type = "market".to_string();
        request.limit_price = None;
        let order = build_order(&request, "U1234567", "sunk-test");
        assert_eq!(order.total_quantity, 0.0);
        assert_eq!(order.cash_qty, Some(1_000.0));
    }

    #[test]
    fn stop_market_order_uses_native_stop_trigger() {
        let mut request = order_request();
        request.order_type = "stop_market".to_string();
        request.limit_price = None;
        request.stop_price = Some(640.0);
        request.market_slippage_bps = None;

        validate_order_request(&request).expect("stop-market request is supported");
        let order = build_order(&request, "U1234567", "sunk-test");

        assert_eq!(order.order_type, "STP");
        assert_eq!(order.aux_price, Some(640.0));
        assert_eq!(order.limit_price, None);
    }

    /// A request of `order_type` carrying exactly the fields the type uses.
    fn typed_request(order_type: OrderType) -> OrderRequest {
        let mut request = order_request();
        request.order_type = order_type.wire_name().to_string();
        request.quantity = Some(10.0);
        request.time_in_force = if order_type.is_auction() {
            "day"
        } else {
            "gtc"
        }
        .to_string();
        request.limit_price = order_type.needs_limit_price().then_some(650.25);
        request.stop_price = order_type.needs_stop_price().then_some(640.0);
        request.trailing_percent = order_type.is_trailing().then_some(2.5);
        request.limit_offset = (order_type == OrderType::TrailingStopLimit).then_some(0.25);
        // Only a limit-priced type can carry a slippage cap.
        if order_type.fills_at_market() {
            request.market_slippage_bps = None;
        }
        request
    }

    #[test]
    fn every_order_type_maps_to_its_tws_order() {
        // (type, TWS orderType, TIF, lmtPrice, auxPrice, trailingPercent, lmtPriceOffset)
        let expected = [
            (
                OrderType::Market,
                "MKT",
                TimeInForce::GoodTilCanceled,
                None,
                None,
                None,
                None,
            ),
            (
                OrderType::Limit,
                "LMT",
                TimeInForce::GoodTilCanceled,
                Some(650.25),
                None,
                None,
                None,
            ),
            (
                OrderType::StopMarket,
                "STP",
                TimeInForce::GoodTilCanceled,
                None,
                Some(640.0),
                None,
                None,
            ),
            (
                OrderType::StopLimit,
                "STP LMT",
                TimeInForce::GoodTilCanceled,
                Some(650.25),
                Some(640.0),
                None,
                None,
            ),
            (
                OrderType::TrailingStop,
                "TRAIL",
                TimeInForce::GoodTilCanceled,
                None,
                None,
                Some(2.5),
                None,
            ),
            (
                OrderType::TrailingStopLimit,
                "TRAIL LIMIT",
                TimeInForce::GoodTilCanceled,
                None,
                None,
                Some(2.5),
                Some(0.25),
            ),
            (
                OrderType::MarketIfTouched,
                "MIT",
                TimeInForce::GoodTilCanceled,
                None,
                Some(640.0),
                None,
                None,
            ),
            (
                OrderType::LimitIfTouched,
                "LIT",
                TimeInForce::GoodTilCanceled,
                Some(650.25),
                Some(640.0),
                None,
                None,
            ),
            (
                OrderType::MarketOnOpen,
                "MKT",
                TimeInForce::OnOpen,
                None,
                None,
                None,
                None,
            ),
            (
                OrderType::LimitOnOpen,
                "LMT",
                TimeInForce::OnOpen,
                Some(650.25),
                None,
                None,
                None,
            ),
            (
                OrderType::MarketOnClose,
                "MOC",
                TimeInForce::Day,
                None,
                None,
                None,
                None,
            ),
            (
                OrderType::LimitOnClose,
                "LOC",
                TimeInForce::Day,
                Some(650.25),
                None,
                None,
                None,
            ),
        ];
        assert_eq!(expected.len(), OrderType::ALL.len());
        for (order_type, tws, tif, limit, aux, trailing, offset) in expected {
            let request = typed_request(order_type);
            validate_order_request(&request)
                .unwrap_or_else(|error| panic!("{order_type:?} is valid: {error}"));
            let order = build_order(&request, "U1234567", "sunk-test");
            assert_eq!(order.order_type, tws, "{order_type:?}");
            assert_eq!(order.tif, tif, "{order_type:?}");
            assert_eq!(order.limit_price, limit, "{order_type:?}");
            assert_eq!(order.aux_price, aux, "{order_type:?}");
            assert_eq!(order.trailing_percent, trailing, "{order_type:?}");
            assert_eq!(order.limit_price_offset, offset, "{order_type:?}");
            assert_eq!(order.total_quantity, 10.0);
            assert_eq!(order.cash_qty, None);
            assert!(!order.outside_rth);
            assert!(order.algo_strategy.is_empty());
            assert_eq!(OrderType::parse(order_type.wire_name()), Some(order_type));
        }
    }

    #[test]
    fn every_order_type_refuses_prices_it_does_not_use() {
        for order_type in OrderType::ALL {
            let name = order_type.wire_name();
            let refused = |request: OrderRequest, field: &str| {
                let Err(GatewayError::Input(message)) = validate_order_request(&request) else {
                    panic!("{name} must refuse {field}");
                };
                assert!(message.contains(field), "{name}: {message}");
            };
            let mut request = typed_request(order_type);
            if order_type.needs_limit_price() {
                request.limit_price = None;
            } else {
                request.limit_price = Some(650.25);
            }
            refused(request, "limitPrice");
            let mut request = typed_request(order_type);
            if order_type.needs_stop_price() {
                request.stop_price = Some(0.0);
            } else {
                request.stop_price = Some(640.0);
            }
            refused(request, "stopPrice");
            let mut request = typed_request(order_type);
            request.trailing_percent = if order_type.is_trailing() {
                Some(100.0)
            } else {
                Some(2.0)
            };
            refused(request, "trailingPercent");
            let mut request = typed_request(order_type);
            request.limit_offset = if order_type == OrderType::TrailingStopLimit {
                None
            } else {
                Some(0.5)
            };
            refused(request, "limitOffset");
            if !order_type.accepts_cash_quantity() {
                let mut request = typed_request(order_type);
                request.quantity = None;
                refused(request, "explicit quantity");
            }
        }
        let mut unknown = order_request();
        unknown.order_type = "bracket".to_string();
        let Err(GatewayError::Input(message)) = validate_order_request(&unknown) else {
            panic!("an unknown order type is refused");
        };
        assert!(message.starts_with("orderType must be one of market, limit, stop_market"));
        // Older bridges answer with this exact text; the web app reads it as
        // "update the bridge", so this release must never send it.
        assert_ne!(message, "orderType must be market, limit, or stop_market");
    }

    #[test]
    fn shared_order_type_fixtures_are_valid_orders() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../fixtures/v3/contract.json"))
                .expect("valid shared contract fixture");
        for name in [
            "orderRequest",
            "advancedOrderRequest",
            "protectedMarketOrderRequest",
            "marketOnOpenOrderRequest",
        ] {
            let request: OrderRequest =
                serde_json::from_value(fixture[name].clone()).expect("order fixture");
            validate_order_request(&request).unwrap_or_else(|error| panic!("{name}: {error}"));
        }
    }

    #[test]
    fn only_relinkable_types_can_be_linked_exits() {
        for order_type in OrderType::ALL {
            let mut request = typed_request(order_type);
            request.side = "sell".to_string();
            request.reduce_only = true;
            let result = validate_order_request(&request);
            if order_type.links_as_exit() {
                result.unwrap_or_else(|error| panic!("{order_type:?}: {error}"));
                // Every type a linked exit may use is one relinking reproduces,
                // or a market order, which fills at once.
                assert!(
                    order_type == OrderType::Market
                        || linked_exits::RELINKABLE_ORDER_TYPES
                            .contains(&order_type.tws_order_type()),
                    "{order_type:?}"
                );
            } else {
                let Err(GatewayError::Input(message)) = result else {
                    panic!("{order_type:?} must be refused as a linked exit");
                };
                assert!(message.contains("linked exit"), "{message}");
                assert!(!message.contains("IBKR does not support reduce-only"));
            }
        }
    }

    #[test]
    fn auction_orders_are_day_stock_orders_inside_their_auction() {
        for order_type in OrderType::ALL.into_iter().filter(|kind| kind.is_auction()) {
            let mut request = typed_request(order_type);
            request.time_in_force = "gtc".to_string();
            assert!(matches!(
                validate_order_request(&request),
                Err(GatewayError::Input(message)) if message.contains("timeInForce day")
            ));
            let mut request = typed_request(order_type);
            request.outside_rth = true;
            assert!(matches!(
                validate_order_request(&request),
                Err(GatewayError::Input(message)) if message.contains("outside regular trading hours")
            ));
            let mut request = typed_request(order_type);
            request.instrument_kind = InstrumentKind::Option;
            assert!(matches!(
                validate_order_request(&request),
                Err(GatewayError::Input(message)) if message.contains("stocks only")
            ));
        }
    }

    #[test]
    fn outside_rth_and_day_reach_the_tws_order() {
        let mut request = typed_request(OrderType::StopLimit);
        request.outside_rth = true;
        request.time_in_force = "day".to_string();
        validate_order_request(&request).expect("extended-hours stop limit");
        let order = build_order(&request, "U1234567", "sunk-test");
        assert!(order.outside_rth);
        assert_eq!(order.tif, TimeInForce::Day);
    }

    #[test]
    fn adaptive_algo_is_a_day_market_or_limit_order() {
        let adaptive = OrderAlgo {
            strategy: AlgoStrategy::Adaptive,
            priority: crate::contract::AdaptivePriority::Urgent,
        };
        let mut request = typed_request(OrderType::Limit);
        request.algo = Some(adaptive);
        assert!(matches!(
            validate_order_request(&request),
            Err(GatewayError::Input(message)) if message.contains("timeInForce day")
        ));
        request.time_in_force = "day".to_string();
        validate_order_request(&request).expect("adaptive day limit");
        let order = build_order(&request, "U1234567", "sunk-test");
        assert_eq!(order.algo_strategy, "Adaptive");
        assert_eq!(
            order.algo_params,
            vec![TagValue {
                tag: "adaptivePriority".to_string(),
                value: "Urgent".to_string(),
            }]
        );
        let mut market = typed_request(OrderType::Market);
        market.algo = Some(adaptive);
        market.time_in_force = "day".to_string();
        validate_order_request(&market).expect("adaptive day market");
        let mut stop = typed_request(OrderType::StopMarket);
        stop.algo = Some(adaptive);
        stop.time_in_force = "day".to_string();
        assert!(matches!(
            validate_order_request(&stop),
            Err(GatewayError::Input(message)) if message.contains("only market and limit")
        ));
    }

    #[test]
    fn option_orders_require_quantity() {
        let mut request = order_request();
        request.instrument_kind = InstrumentKind::Option;
        request.quantity = None;
        assert!(matches!(
            validate_order_request(&request),
            Err(GatewayError::Input(_))
        ));
    }

    #[test]
    fn a_slippage_cap_is_refused_on_every_type_ibkr_fills_at_market() {
        for order_type in OrderType::ALL {
            let mut request = typed_request(order_type);
            request.market_slippage_bps = Some(300);
            let result = validate_order_request(&request);
            if order_type.fills_at_market() {
                assert!(
                    matches!(
                        &result,
                        Err(GatewayError::Input(message))
                            if message.starts_with(&format!(
                                "marketSlippageBps cannot bound a {} order",
                                order_type.wire_name()
                            ))
                    ),
                    "{order_type:?}: {result:?}"
                );
                // The same order without the cap is valid.
                request.market_slippage_bps = None;
                validate_order_request(&request)
                    .unwrap_or_else(|error| panic!("{order_type:?} uncapped: {error:?}"));
            } else {
                result.unwrap_or_else(|error| panic!("{order_type:?} capped: {error:?}"));
            }
        }
        assert_eq!(
            OrderType::ALL
                .into_iter()
                .filter(|order_type| order_type.fills_at_market())
                .map(OrderType::tws_order_type)
                .collect::<Vec<_>>(),
            ["MKT", "STP", "TRAIL", "MIT", "MKT", "MOC"]
        );
    }

    #[test]
    fn market_slippage_is_bounded_before_submission() {
        let mut request = order_request();
        request.market_slippage_bps = Some(10_001);
        assert!(matches!(
            validate_order_request(&request),
            Err(GatewayError::Input(_))
        ));
    }

    #[test]
    fn reduce_only_is_a_linked_exit_with_an_explicit_quantity() {
        let mut request = order_request();
        request.side = "sell".to_string();
        request.time_in_force = "gtc".to_string();
        request.reduce_only = true;
        assert!(validate_order_request(&request).is_ok());

        request.quantity = None;
        assert!(matches!(
            validate_order_request(&request),
            Err(GatewayError::Input(message)) if message.contains("explicit quantity")
        ));

        // The phrase older bridges answer with is reserved for them: the web
        // app falls back to an unlinked exit only when it sees it.
        let mut post_only = order_request();
        post_only.post_only = true;
        let Err(GatewayError::Input(message)) = validate_order_request(&post_only) else {
            panic!("post-only must be refused");
        };
        assert_eq!(message, "IBKR does not support post-only orders");
        assert!(!message.contains("reduce-only"));
    }

    #[test]
    fn cancellation_contract_rejects_ambiguous_native_identity() {
        let valid = CancelOrderRequest {
            version: PROTOCOL_VERSION,
            connection_id: Some("ibkr-local".to_string()),
            provider: "ibkr".to_string(),
            order_id: 42,
            account_ref: Some("DU123456".to_string()),
        };
        assert!(validate_cancel_order_request(&valid).is_ok());

        let invalid_order = CancelOrderRequest {
            order_id: 0,
            ..valid
        };
        assert!(matches!(
            validate_cancel_order_request(&invalid_order),
            Err(GatewayError::Input(message)) if message.contains("orderId")
        ));
    }

    #[test]
    fn account_orders_report_only_blocking_oca_groups_with_parent_and_fill() {
        let open = |oca_group: &str, oca_type: OcaType, parent_id: i32, filled: f64| OrderData {
            order_id: 42,
            contract: Contract {
                contract_id: 265_598,
                symbol: "AAPL".into(),
                security_type: SecurityType::Stock,
                ..Default::default()
            },
            order: Order {
                account: "DU123456".to_string(),
                action: Action::Sell,
                total_quantity: 10.0,
                filled_quantity: filled,
                order_type: "STP".to_string(),
                aux_price: Some(190.0),
                oca_group: oca_group.to_string(),
                oca_type,
                parent_id,
                ..Default::default()
            },
            order_state: ibapi::orders::OrderState {
                status: OrderStatusKind::Submitted,
                ..Default::default()
            },
        };
        let linked = normalize_account_order(open("oca-sunk-a", OcaType::ReduceWithBlock, 0, 0.0))
            .expect("normalized order");
        assert_eq!(linked.oca_group.as_deref(), Some("oca-sunk-a"));
        assert_eq!(linked.parent_id, None);
        assert_eq!(linked.filled_quantity, None);
        let child = normalize_account_order(open("oca-x", OcaType::CancelWithBlock, 41, 4.0))
            .expect("normalized order");
        assert_eq!(child.oca_group.as_deref(), Some("oca-x"));
        assert_eq!(child.parent_id, Some(41));
        assert_eq!(child.filled_quantity, Some(4.0));
        // Without block the group can overfill, and a blank name is no group.
        for (group, oca_type) in [
            ("oca-y", OcaType::ReduceWithoutBlock),
            ("   ", OcaType::ReduceWithBlock),
            ("", OcaType::None),
        ] {
            let order =
                normalize_account_order(open(group, oca_type, 0, 0.0)).expect("normalized order");
            assert_eq!(order.oca_group, None, "{group:?} {oca_type:?}");
        }
    }

    #[test]
    fn account_orders_report_their_order_type_and_trailing_stop() {
        let open = |order_type: &str, tif: TimeInForce, aux: Option<f64>| OrderData {
            order_id: 42,
            contract: Contract {
                contract_id: 265_598,
                symbol: "AAPL".into(),
                security_type: SecurityType::Stock,
                ..Default::default()
            },
            order: Order {
                account: "DU123456".to_string(),
                action: Action::Sell,
                total_quantity: 10.0,
                order_type: order_type.to_string(),
                tif,
                limit_price: Some(185.0),
                aux_price: aux,
                trail_stop_price: Some(188.5),
                ..Default::default()
            },
            order_state: ibapi::orders::OrderState {
                status: OrderStatusKind::Submitted,
                ..Default::default()
            },
        };
        let gtc = TimeInForce::GoodTilCanceled;
        for (tws, tif, expected) in [
            ("LMT", gtc.clone(), Some(OrderType::Limit)),
            ("MKT", TimeInForce::Day, Some(OrderType::Market)),
            ("STP", gtc.clone(), Some(OrderType::StopMarket)),
            ("STP LMT", gtc.clone(), Some(OrderType::StopLimit)),
            ("TRAIL", gtc.clone(), Some(OrderType::TrailingStop)),
            (
                "TRAIL LIMIT",
                gtc.clone(),
                Some(OrderType::TrailingStopLimit),
            ),
            ("MIT", gtc.clone(), Some(OrderType::MarketIfTouched)),
            ("LIT", gtc.clone(), Some(OrderType::LimitIfTouched)),
            ("MKT", TimeInForce::OnOpen, Some(OrderType::MarketOnOpen)),
            ("LMT", TimeInForce::OnOpen, Some(OrderType::LimitOnOpen)),
            ("MOC", TimeInForce::Day, Some(OrderType::MarketOnClose)),
            ("LOC", TimeInForce::Day, Some(OrderType::LimitOnClose)),
            // A type the bridge never places is reported as unknown.
            ("REL", gtc.clone(), None),
        ] {
            let order = normalize_account_order(open(tws, tif, Some(190.0))).expect("normalized");
            assert_eq!(order.order_type, expected, "{tws}");
        }
        let unknown =
            normalize_account_order(open("REL", gtc.clone(), Some(190.0))).expect("normalized");
        assert_eq!(
            serde_json::to_value(&unknown).expect("serialized")["orderType"],
            serde_json::Value::Null,
            "an unknown type is null, never omitted"
        );

        // A plain stop's trigger is its `auxPrice`.
        let stop =
            normalize_account_order(open("STP", gtc.clone(), Some(190.0))).expect("normalized");
        assert_eq!(stop.trigger_price, Some(190.0));
        // An amount trailing stop's `auxPrice` is its trailing amount, so its
        // trigger is the trail stop price, as for a percent trailing stop.
        for aux in [Some(1.25), None] {
            let trailing =
                normalize_account_order(open("TRAIL", gtc.clone(), aux)).expect("normalized");
            assert_eq!(trailing.trigger_price, Some(188.5), "{aux:?}");
        }
    }

    #[test]
    fn account_orders_report_time_in_force_extended_hours_and_algo() {
        let open = |tif: TimeInForce, outside_rth: bool, algo: &str, params: Vec<TagValue>| {
            normalize_account_order(OrderData {
                order_id: 42,
                contract: Contract {
                    contract_id: 265_598,
                    symbol: "AAPL".into(),
                    security_type: SecurityType::Stock,
                    ..Default::default()
                },
                order: Order {
                    account: "DU123456".to_string(),
                    action: Action::Sell,
                    total_quantity: 10.0,
                    order_type: "LMT".to_string(),
                    limit_price: Some(200.0),
                    tif,
                    outside_rth,
                    algo_strategy: algo.to_string(),
                    algo_params: params,
                    ..Default::default()
                },
                order_state: ibapi::orders::OrderState {
                    status: OrderStatusKind::Submitted,
                    ..Default::default()
                },
            })
            .expect("normalized order")
        };
        let priority = |value: &str| {
            vec![TagValue {
                tag: "adaptivePriority".to_string(),
                value: value.to_string(),
            }]
        };

        let plain = open(TimeInForce::GoodTilCanceled, false, "", Vec::new());
        assert_eq!(plain.order_type, Some(OrderType::Limit));
        assert_eq!(plain.time_in_force.as_deref(), Some("gtc"));
        assert!(!plain.outside_rth);
        assert_eq!(plain.algo, None);
        let value = serde_json::to_value(&plain).expect("serialized");
        assert_eq!(value["timeInForce"], "gtc");
        assert_eq!(value["outsideRth"], false);
        assert_eq!(value["algo"], serde_json::Value::Null, "never omitted");

        let extended = open(TimeInForce::Day, true, "Adaptive", priority("Patient"));
        assert_eq!(extended.order_type, Some(OrderType::Limit));
        assert_eq!(extended.time_in_force.as_deref(), Some("day"));
        assert!(extended.outside_rth);
        assert_eq!(
            extended.algo,
            Some(OrderAlgo {
                strategy: AlgoStrategy::Adaptive,
                priority: AdaptivePriority::Patient,
            })
        );
        // IBKR's default priority applies when the parameter is absent.
        let default_priority = open(TimeInForce::Day, false, "Adaptive", Vec::new());
        assert_eq!(
            default_priority.algo.map(|algo| algo.priority),
            Some(AdaptivePriority::Normal)
        );

        // An algo or priority the bridge does not place, or a time in force
        // it does not send, cannot be re-placed as reported.
        for unplaceable in [
            open(TimeInForce::Day, false, "Vwap", Vec::new()),
            open(TimeInForce::Day, false, "Adaptive", priority("Hurried")),
        ] {
            assert_eq!(unplaceable.order_type, None);
            assert_eq!(unplaceable.algo, None);
        }
        for (tif, expected) in [
            (TimeInForce::ImmediateOrCancel, Some("ioc")),
            (TimeInForce::FillOrKill, Some("fok")),
            (TimeInForce::OnOpen, Some("day")),
            (TimeInForce::GoodTilDate, None),
            (TimeInForce::DayTilCanceled, None),
        ] {
            let order = open(tif.clone(), false, "", Vec::new());
            assert_eq!(order.time_in_force.as_deref(), expected, "{tif:?}");
        }
    }

    #[test]
    fn indeterminate_cancellation_requires_open_order_reconciliation() {
        let response =
            cancel_indeterminate_response("DU123456".to_string(), 42, "the local transport closed");
        assert!(!response.ok);
        assert_eq!(response.status, "indeterminate");
        assert_eq!(response.order_id.as_deref(), Some("42"));
        assert!(response.message.contains("Reconcile open orders"));
    }

    #[test]
    fn malformed_price_rules_fail_closed() {
        assert!(
            validate_price_rules(&[PriceIncrementRule {
                lower_edge: 0.0,
                increment: f64::NAN,
            }])
            .is_err()
        );
    }

    #[test]
    fn limit_prices_must_follow_the_active_market_rule() {
        let rules = vec![
            PriceIncrementRule {
                lower_edge: 0.0,
                increment: 0.01,
            },
            PriceIncrementRule {
                lower_edge: 3.0,
                increment: 0.05,
            },
        ];
        assert!(validate_price_increment("limitPrice", 2.99, &rules).is_ok());
        assert!(validate_price_increment("limitPrice", 3.03, &rules).is_err());
        assert!(validate_price_increment("limitPrice", 3.05, &rules).is_ok());
    }

    #[test]
    fn option_strikes_are_bounded_on_each_side() {
        let strikes = select_strikes(&[95.0, 97.5, 100.0, 102.5, 105.0], 100.0, 1);
        assert_eq!(strikes, vec![100.0, 102.5]);
    }

    #[test]
    fn provider_option_rights_serialize_as_canonical_contract_values() {
        let call = normalize_option_right(Some(OptionRight::Call)).expect("call is supported");
        let put = normalize_option_right(Some(OptionRight::Put)).expect("put is supported");

        assert_eq!(serde_json::to_value(call).unwrap(), "call");
        assert_eq!(serde_json::to_value(put).unwrap(), "put");
        assert!(normalize_option_right(None).is_err());
    }

    #[test]
    fn missing_smart_option_chain_is_normal_unavailability() {
        assert!(
            select_smart_option_chain(vec![OptionChain {
                exchange: "CBOE".to_string(),
                trading_class: "DAAQ".to_string(),
                multiplier: "100".to_string(),
                expirations: vec!["20260918".to_string()],
                strikes: vec![10.0],
                ..Default::default()
            }])
            .is_none()
        );
    }

    #[test]
    fn smart_option_chain_selection_keeps_the_richest_definition() {
        let selected = select_smart_option_chain(vec![
            OptionChain {
                exchange: "SMART".to_string(),
                trading_class: "SPY".to_string(),
                multiplier: "100".to_string(),
                expirations: vec!["20260918".to_string()],
                strikes: vec![600.0],
                ..Default::default()
            },
            OptionChain {
                exchange: "SMART".to_string(),
                trading_class: "SPYW".to_string(),
                multiplier: "100".to_string(),
                expirations: vec!["20260918".to_string(), "20260925".to_string()],
                strikes: vec![600.0, 605.0],
                ..Default::default()
            },
        ])
        .expect("a SMART chain should be selected");

        assert_eq!(selected.trading_class, "SPYW");
    }

    #[tokio::test]
    async fn provider_timeout_maps_elapsed_operations_to_unavailable() {
        let result = provider_timeout(
            Duration::from_millis(1),
            "test operation",
            std::future::pending::<Result<(), GatewayError>>(),
        )
        .await;
        assert!(matches!(result, Err(GatewayError::Unavailable(_))));
    }

    #[test]
    fn provider_rejections_are_distinct_from_gateway_transport_loss() {
        let rejected = provider_error(
            "market data",
            ibapi::Error::Notice(ibapi::Notice {
                code: 10189,
                message: "Requested market data requires an additional subscription".to_string(),
                error_time: None,
                advanced_order_reject_json: String::new(),
            }),
        );
        let disconnected = provider_error("market data", ibapi::Error::ConnectionReset);
        let malformed = provider_error(
            "market data",
            ibapi::Error::UnexpectedResponse("malformed tick".to_string()),
        );

        assert!(matches!(rejected, GatewayError::Provider(_)));
        assert!(matches!(disconnected, GatewayError::Unavailable(_)));
        assert!(matches!(malformed, GatewayError::Provider(_)));
    }

    #[test]
    fn option_resolution_skips_only_missing_contracts() {
        let missing = contract_lookup_error(
            "option contract details",
            ibapi::Error::Notice(ibapi::Notice {
                code: NO_SECURITY_DEFINITION_CODE,
                message: "No security definition found".to_string(),
                error_time: None,
                advanced_order_reject_json: String::new(),
            }),
        );
        assert!(matches!(missing, GatewayError::Contract(_)));
        assert!(skip_expected_option_contract_miss::<()>(Err(missing)).is_none());

        let malformed = contract_lookup_error(
            "option contract details",
            ibapi::Error::UnexpectedResponse("malformed contract details".to_string()),
        );
        assert!(matches!(
            skip_expected_option_contract_miss::<()>(Err(malformed)),
            Some(Err(GatewayError::Provider(_)))
        ));
    }

    #[test]
    fn empty_normalized_option_chain_fails_closed() {
        assert!(matches!(
            require_option_contracts::<()>(Vec::new(), "AAPL"),
            Err(GatewayError::Contract(_))
        ));
    }

    fn price_tick(tick_type: TickType, price: f64) -> TickTypes {
        TickTypes::Price(ibapi::market_data::realtime::TickPrice {
            tick_type,
            price,
            ..Default::default()
        })
    }

    fn quote_state() -> StockQuoteState {
        StockQuoteState::new("SPY".to_string(), 756_733, "USD".to_string(), Vec::new())
    }

    type TickItem = Result<SubscriptionItem<TickTypes>, ibapi::Error>;

    fn data(tick: TickTypes) -> TickItem {
        Ok(SubscriptionItem::Data(tick))
    }

    #[tokio::test]
    async fn a_failed_tick_rule_lookup_is_retried_never_cached_as_absent() {
        use std::sync::atomic::{AtomicU32, Ordering};
        let attempts = Arc::new(AtomicU32::new(0));
        let counted = Arc::clone(&attempts);
        let rules = vec![PriceIncrementRule {
            lower_edge: 0.0,
            increment: 0.05,
        }];
        let found = rules.clone();
        let mut receiver = spawn_retrying_price_rules(
            move || {
                let attempt = counted.fetch_add(1, Ordering::SeqCst) + 1;
                let found = found.clone();
                async move {
                    if attempt <= 2 {
                        Err(GatewayError::Unavailable(
                            "IBKR stock price increments timed out".to_string(),
                        ))
                    } else {
                        Ok(found)
                    }
                }
            },
            |_| Duration::from_millis(1),
        );
        // Nothing is published while lookups fail.
        assert_eq!(*receiver.borrow(), None);
        tokio::time::timeout(Duration::from_secs(5), receiver.changed())
            .await
            .expect("rules arrive")
            .expect("lookup task alive");
        assert_eq!(*receiver.borrow_and_update(), Some(rules.clone()));
        assert_eq!(attempts.load(Ordering::SeqCst), 3);

        // The quote carries them once they arrive, and only a change counts.
        let mut state = quote_state();
        assert!(state.set_price_increment_rules(rules.clone()));
        assert!(!state.set_price_increment_rules(rules.clone()));
        state.apply(price_tick(TickType::Bid, 650.2), 1_000);
        state.apply(TickTypes::MarketDataType(MarketDataType::Realtime), 1_000);
        let quote = state.quote().expect("live quote");
        assert_eq!(quote.price_increment_rules, rules);
    }

    #[tokio::test]
    async fn a_tick_rule_lookup_stops_once_the_subscription_is_gone() {
        use std::sync::atomic::{AtomicU32, Ordering};
        let attempts = Arc::new(AtomicU32::new(0));
        let counted = Arc::clone(&attempts);
        let receiver = spawn_retrying_price_rules(
            move || {
                counted.fetch_add(1, Ordering::SeqCst);
                async { Err(GatewayError::Provider("market rule failed".to_string())) }
            },
            |_| Duration::from_millis(5),
        );
        tokio::time::sleep(Duration::from_millis(30)).await;
        drop(receiver);
        tokio::time::sleep(Duration::from_millis(30)).await;
        let after_drop = attempts.load(Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(attempts.load(Ordering::SeqCst), after_drop);
        assert!(after_drop >= 2, "it retried while subscribed");
    }

    /// A fake client whose market-rule answers, like `ibapi`'s, reach every
    /// lookup in flight, and arrive out of order: rule 1 answers slowly and
    /// every other rule fast.
    #[derive(Clone)]
    struct BroadcastRuleServer {
        answers: tokio::sync::broadcast::Sender<(i32, Vec<PriceIncrementRule>)>,
        requests: Arc<std::sync::atomic::AtomicUsize>,
    }

    impl BroadcastRuleServer {
        fn new() -> Self {
            Self {
                answers: tokio::sync::broadcast::channel(16).0,
                requests: Arc::default(),
            }
        }

        fn increments(rule_id: i32) -> Vec<PriceIncrementRule> {
            vec![PriceIncrementRule {
                lower_edge: 0.0,
                increment: f64::from(rule_id) / 100.0,
            }]
        }

        fn fetch(
            &self,
            rule_id: i32,
        ) -> impl Future<Output = Result<(i32, Vec<PriceIncrementRule>), GatewayError>> + use<>
        {
            let mut answers = self.answers.subscribe();
            let sender = self.answers.clone();
            self.requests
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let delay = Duration::from_millis(if rule_id == 1 { 60 } else { 5 });
            tokio::spawn(async move {
                tokio::time::sleep(delay).await;
                let _ = sender.send((rule_id, Self::increments(rule_id)));
            });
            // Like `Client::market_rule`: the first answer to arrive.
            async move {
                answers
                    .recv()
                    .await
                    .map_err(|_| GatewayError::Unavailable("closed".to_string()))
            }
        }
    }

    #[tokio::test]
    async fn concurrent_market_rule_lookups_each_get_their_own_rule() {
        // Without serialization the fake reproduces the bug: the slow
        // lookup for rule 1 reads rule 2's faster answer.
        let server = BroadcastRuleServer::new();
        let (raced, _) = tokio::join!(server.fetch(1), server.fetch(2));
        assert_eq!(raced.expect("an answer").0, 2);

        let server = BroadcastRuleServer::new();
        let cache = MarketRuleCache::default();
        let (first, second) = tokio::join!(
            cache.get(1, |id| server.fetch(id)),
            cache.get(2, |id| server.fetch(id)),
        );
        assert_eq!(first.expect("rule 1"), BroadcastRuleServer::increments(1));
        assert_eq!(second.expect("rule 2"), BroadcastRuleServer::increments(2));
        // A known rule is not requested again.
        let again = cache
            .get(1, |id| server.fetch(id))
            .await
            .expect("cached rule 1");
        assert_eq!(again, BroadcastRuleServer::increments(1));
        assert_eq!(server.requests.load(std::sync::atomic::Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn an_answer_for_another_market_rule_is_refused_and_not_cached() {
        let cache = MarketRuleCache::default();
        let wrong = cache
            .get(3, |_| async { Ok((4, BroadcastRuleServer::increments(4))) })
            .await;
        assert!(
            matches!(&wrong, Err(GatewayError::Provider(message)) if message.contains("market rule 4 to a request for 3")),
            "{wrong:?}"
        );
        let right = cache
            .get(3, |id| async move {
                Ok((id, BroadcastRuleServer::increments(id)))
            })
            .await
            .expect("rule 3");
        assert_eq!(right, BroadcastRuleServer::increments(3));
        // Malformed increments are refused and not cached either.
        let malformed = cache
            .get(5, |id| async move {
                Ok((
                    id,
                    vec![PriceIncrementRule {
                        lower_edge: 0.0,
                        increment: 0.0,
                    }],
                ))
            })
            .await;
        assert!(matches!(malformed, Err(GatewayError::Contract(_))));
    }

    #[test]
    fn tick_rule_retries_back_off_to_a_minute() {
        assert_eq!(price_rules_retry_delay(1), Duration::from_secs(1));
        assert_eq!(price_rules_retry_delay(2), Duration::from_secs(2));
        assert_eq!(price_rules_retry_delay(4), Duration::from_secs(8));
        assert_eq!(price_rules_retry_delay(7), Duration::from_secs(60));
        assert_eq!(price_rules_retry_delay(u32::MAX), Duration::from_secs(60));
    }

    #[test]
    fn stock_quote_waits_for_ibkr_to_identify_undelayed_data() {
        let mut state = quote_state();
        // The bid is recorded, but frozen data uses the same tick types as
        // live data, so nothing is published until IBKR reports the mode.
        assert!(state.apply(price_tick(TickType::Bid, 650.2), 1_000));
        assert!(state.quote().is_none());

        assert!(state.apply(TickTypes::MarketDataType(MarketDataType::Frozen), 2_000));
        let quote = state.quote().expect("frozen quote");
        assert_eq!(quote.data_type, QuoteDataType::Frozen);
        assert_eq!(quote.bid, Some(650.2));
        // The mode report does not make the bid any newer.
        assert_eq!(quote.updated_at_ms, 1_000);
    }

    #[test]
    fn mode_changes_republish_without_refreshing_the_quote_age() {
        let mut state = quote_state();
        state.apply(TickTypes::MarketDataType(MarketDataType::Realtime), 1);
        state.apply(price_tick(TickType::Bid, 650.2), 2);
        assert!(state.apply(TickTypes::MarketDataType(MarketDataType::Frozen), 90_000));
        let quote = state.quote().unwrap();
        assert_eq!(quote.data_type, QuoteDataType::Frozen);
        assert_eq!(quote.updated_at_ms, 2);
        assert!(!state.apply(TickTypes::MarketDataType(MarketDataType::Frozen), 95_000));
    }

    #[test]
    fn frozen_quotes_age_from_the_last_trade() {
        let mut state = quote_state();
        state.apply(TickTypes::MarketDataType(MarketDataType::Frozen), 1);
        state.apply(price_tick(TickType::Last, 650.0), 5_000_000);
        state.apply(
            TickTypes::String(ibapi::market_data::realtime::TickString {
                tick_type: TickType::LastTimestamp,
                value: "1000".to_string(),
            }),
            5_000_001,
        );
        assert_eq!(state.quote().unwrap().updated_at_ms, 1_000_000);

        // Live quotes keep their arrival time even with an older last trade.
        state.apply(
            TickTypes::MarketDataType(MarketDataType::Realtime),
            5_000_002,
        );
        assert_eq!(state.quote().unwrap().updated_at_ms, 5_000_001);
    }

    #[test]
    fn stock_quote_classifies_every_market_data_mode() {
        for (reported, delayed_tick, expected) in [
            (MarketDataType::Realtime, false, QuoteDataType::Live),
            (MarketDataType::Frozen, false, QuoteDataType::Frozen),
            (MarketDataType::Delayed, false, QuoteDataType::Delayed),
            (
                MarketDataType::DelayedFrozen,
                false,
                QuoteDataType::DelayedFrozen,
            ),
            (MarketDataType::Realtime, true, QuoteDataType::Delayed),
            (MarketDataType::Frozen, true, QuoteDataType::DelayedFrozen),
        ] {
            let mut state = quote_state();
            state.apply(TickTypes::MarketDataType(reported), 1);
            let tick = if delayed_tick {
                TickType::DelayedLast
            } else {
                TickType::Last
            };
            state.apply(price_tick(tick, 650.0), 2);
            assert_eq!(state.quote().unwrap().data_type, expected);
        }
    }

    #[test]
    fn delayed_ticks_publish_before_the_mode_report() {
        let mut state = quote_state();
        assert!(state.apply(price_tick(TickType::DelayedAsk, 650.3), 5));
        let quote = state.quote().expect("delayed quote");
        assert_eq!(quote.data_type, QuoteDataType::Delayed);
        assert_eq!(quote.ask, Some(650.3));
    }

    #[test]
    fn stock_quote_maps_every_price_field_and_clears_a_missing_side() {
        let mut state = quote_state();
        state.apply(TickTypes::MarketDataType(MarketDataType::Realtime), 1);
        state.apply(price_tick(TickType::Bid, 650.2), 2);
        state.apply(price_tick(TickType::Ask, 650.25), 3);
        state.apply(price_tick(TickType::Last, 650.22), 4);
        state.apply(price_tick(TickType::Close, 648.9), 5);
        state.apply(
            TickTypes::String(ibapi::market_data::realtime::TickString {
                tick_type: TickType::LastTimestamp,
                value: "1784912401".to_string(),
            }),
            6,
        );
        let quote = state.quote().unwrap();
        assert_eq!(
            (quote.bid, quote.ask, quote.last, quote.previous_close),
            (Some(650.2), Some(650.25), Some(650.22), Some(648.9))
        );
        assert_eq!(quote.last_trade_at_ms, Some(1_784_912_401_000));

        assert!(state.apply(price_tick(TickType::Bid, -1.0), 7));
        assert_eq!(state.quote().unwrap().bid, None);
        assert!(!state.apply(price_tick(TickType::Last, 0.0), 8));
        assert_eq!(state.quote().unwrap().last, Some(650.22));
    }

    #[test]
    fn size_and_repeated_ticks_do_not_refresh_the_quote_age() {
        let mut state = quote_state();
        state.apply(TickTypes::MarketDataType(MarketDataType::Realtime), 1);
        state.apply(price_tick(TickType::Bid, 650.2), 2);
        assert!(!state.apply(
            TickTypes::Size(ibapi::market_data::realtime::TickSize {
                tick_type: TickType::BidSize,
                size: 300.0,
            }),
            3,
        ));
        assert!(!state.apply(price_tick(TickType::Bid, 650.2), 4));
        assert_eq!(state.quote().unwrap().updated_at_ms, 2);
        assert!(state.apply(
            TickTypes::PriceSize(ibapi::market_data::realtime::TickPriceSize {
                price_tick_type: TickType::Bid,
                price: 650.21,
                size_tick_type: TickType::BidSize,
                size: 100.0,
                ..Default::default()
            }),
            5,
        ));
        assert_eq!(state.quote().unwrap().updated_at_ms, 5);
    }

    #[tokio::test]
    async fn stock_quote_stream_yields_only_changed_quotes() {
        let mut state = quote_state();
        let mut ticks = stream::iter(vec![
            data(TickTypes::MarketDataType(MarketDataType::Realtime)),
            data(price_tick(TickType::Bid, 650.2)),
            data(price_tick(TickType::Bid, 650.2)),
            Ok(SubscriptionItem::Notice(ibapi::Notice {
                code: 2104,
                message: "Market data farm connection is OK".to_string(),
                error_time: None,
                advanced_order_reject_json: String::new(),
            })),
            data(price_tick(TickType::Ask, 650.25)),
            Err(ibapi::Error::Notice(ibapi::Notice {
                code: 200,
                message: "No security definition has been found".to_string(),
                error_time: None,
                advanced_order_reject_json: String::new(),
            })),
        ]);
        let mut clock = 0;
        let mut now = || {
            clock += 1;
            clock
        };

        let StockQuoteWait::Quote(first) = next_stock_quote(
            &mut ticks,
            stream::pending(),
            &mut state,
            &mut now,
            Duration::from_secs(60),
        )
        .await
        else {
            panic!("expected the first quote");
        };
        assert_eq!((first.bid, first.ask), (Some(650.2), None));
        let StockQuoteWait::Quote(second) = next_stock_quote(
            &mut ticks,
            stream::pending(),
            &mut state,
            &mut now,
            Duration::from_secs(60),
        )
        .await
        else {
            panic!("expected the second quote");
        };
        assert_eq!(second.ask, Some(650.25));
        assert!(matches!(
            next_stock_quote(
                &mut ticks,
                stream::pending(),
                &mut state,
                &mut now,
                Duration::from_secs(60),
            )
            .await,
            StockQuoteWait::Failed(GatewayError::Contract(_))
        ));
        assert!(matches!(
            next_stock_quote(
                &mut ticks,
                stream::pending(),
                &mut state,
                &mut now,
                Duration::from_secs(60),
            )
            .await,
            StockQuoteWait::Ended
        ));
    }

    fn notice(code: i32) -> ibapi::Notice {
        ibapi::Notice {
            code,
            message: format!("notice {code}"),
            error_time: None,
            advanced_order_reject_json: String::new(),
        }
    }

    #[tokio::test]
    async fn lost_market_data_connectivity_ends_the_quote_stream() {
        for code in QUOTE_CONNECTIVITY_LOSS_CODES {
            let mut state = quote_state();
            let mut ticks = stream::iter(vec![
                data(TickTypes::MarketDataType(MarketDataType::Realtime)),
                data(price_tick(TickType::Bid, 650.2)),
            ])
            .chain(stream::pending());
            let (notices, receiver) = futures::channel::mpsc::unbounded();
            let mut receiver = receiver;
            let first = next_stock_quote(
                &mut ticks,
                &mut receiver,
                &mut state,
                || 1,
                Duration::from_secs(60),
            )
            .await;
            assert!(matches!(first, StockQuoteWait::Quote(_)));
            // Farm status notices are informational; connectivity loss is not.
            notices.unbounded_send(notice(2104)).unwrap();
            notices.unbounded_send(notice(code)).unwrap();
            let second = next_stock_quote(
                &mut ticks,
                &mut receiver,
                &mut state,
                || 2,
                Duration::from_secs(60),
            )
            .await;
            assert!(
                matches!(second, StockQuoteWait::Failed(GatewayError::Unavailable(_))),
                "code {code}"
            );
        }
    }

    #[tokio::test]
    async fn silent_live_quotes_are_resubscribed_but_other_modes_may_rest() {
        let mut state = quote_state();
        let mut ticks = stream::iter(vec![
            data(TickTypes::MarketDataType(MarketDataType::Realtime)),
            data(price_tick(TickType::Bid, 650.2)),
        ])
        .chain(stream::pending());
        let idle = Duration::from_millis(20);
        let first = next_stock_quote(&mut ticks, stream::pending(), &mut state, || 1, idle).await;
        assert!(matches!(first, StockQuoteWait::Quote(_)));
        let silent = next_stock_quote(&mut ticks, stream::pending(), &mut state, || 2, idle).await;
        assert!(matches!(silent, StockQuoteWait::Idle));

        // A closed market's frozen quote legitimately stays quiet.
        let mut state = quote_state();
        let mut ticks = stream::iter(vec![
            data(TickTypes::MarketDataType(MarketDataType::Frozen)),
            data(price_tick(TickType::Last, 650.0)),
        ])
        .chain(stream::pending());
        let first = next_stock_quote(&mut ticks, stream::pending(), &mut state, || 1, idle).await;
        assert!(matches!(first, StockQuoteWait::Quote(_)));
        assert!(
            tokio::time::timeout(
                Duration::from_millis(100),
                next_stock_quote(&mut ticks, stream::pending(), &mut state, || 2, idle),
            )
            .await
            .is_err()
        );
    }

    /// Unrouted farm-status notice every `period`, forever.
    fn farm_notices_every(period: Duration) -> futures::stream::BoxStream<'static, ibapi::Notice> {
        stream::unfold((), move |()| async move {
            tokio::time::sleep(period).await;
            Some((notice(2104), ()))
        })
        .boxed()
    }

    #[tokio::test]
    async fn connection_notices_do_not_keep_a_silent_live_line_alive() {
        let mut state = quote_state();
        let mut ticks = stream::iter(vec![
            data(TickTypes::MarketDataType(MarketDataType::Realtime)),
            data(price_tick(TickType::Bid, 650.2)),
        ])
        .chain(stream::pending());
        let idle = Duration::from_millis(200);
        let first = next_stock_quote(&mut ticks, stream::pending(), &mut state, || 1, idle).await;
        assert!(matches!(first, StockQuoteWait::Quote(_)));

        // Farm-status notices arrive twice per idle window; the line itself
        // says nothing, so the wait must still end as idle.
        let started = tokio::time::Instant::now();
        let silent = tokio::time::timeout(
            Duration::from_secs(5),
            next_stock_quote(
                &mut ticks,
                farm_notices_every(idle / 2),
                &mut state,
                || 2,
                idle,
            ),
        )
        .await
        .expect("notices must not extend the idle deadline");
        assert!(matches!(silent, StockQuoteWait::Idle));
        let elapsed = started.elapsed();
        assert!(elapsed >= idle, "idle after {elapsed:?}");
        assert!(elapsed < idle * 4, "idle after {elapsed:?}");
    }

    #[tokio::test]
    async fn the_lines_own_unchanged_ticks_keep_it_alive() {
        let mut state = quote_state();
        let idle = Duration::from_millis(200);
        let repeats = stream::unfold(0_u32, move |sent| async move {
            if sent > 0 {
                tokio::time::sleep(idle / 2).await;
            }
            // The same bid again changes nothing, but it proves the line is up.
            Some((data(price_tick(TickType::Bid, 650.2)), sent + 1))
        });
        let mut ticks = stream::iter(vec![data(TickTypes::MarketDataType(
            MarketDataType::Realtime,
        ))])
        .chain(repeats)
        .boxed();
        let first = next_stock_quote(&mut ticks, stream::pending(), &mut state, || 1, idle).await;
        assert!(matches!(first, StockQuoteWait::Quote(_)));
        assert!(
            tokio::time::timeout(
                idle * 4,
                next_stock_quote(
                    &mut ticks,
                    farm_notices_every(idle / 2),
                    &mut state,
                    || 2,
                    idle
                ),
            )
            .await
            .is_err(),
            "a line that keeps ticking is not idle"
        );
    }

    /// Scripted market-data lines: each resubscribe opens the next one.
    struct FakeQuoteLine {
        ticks: futures::stream::BoxStream<'static, TickItem>,
        next_lines: std::collections::VecDeque<Result<Vec<TickItem>, GatewayError>>,
        resubscribes: usize,
        /// How long a resubscribe takes between cancelling and reopening.
        resubscribe_delay: Duration,
        /// False while the old line is cancelled and the new one not open:
        /// a resubscribe dropped mid-way leaves it false.
        line_open: bool,
    }

    impl FakeQuoteLine {
        fn new(first: Vec<TickItem>, next_lines: Vec<Result<Vec<TickItem>, GatewayError>>) -> Self {
            Self {
                ticks: stream::iter(first).chain(stream::pending()).boxed(),
                next_lines: next_lines.into(),
                resubscribes: 0,
                resubscribe_delay: Duration::ZERO,
                line_open: true,
            }
        }
    }

    impl StockQuoteLine for FakeQuoteLine {
        type Ticks = futures::stream::BoxStream<'static, TickItem>;

        fn ticks(&mut self) -> &mut Self::Ticks {
            &mut self.ticks
        }

        async fn resubscribe(&mut self) -> Result<(), GatewayError> {
            self.resubscribes += 1;
            // The real line cancels the old subscription first.
            self.line_open = false;
            tokio::time::sleep(self.resubscribe_delay).await;
            self.line_open = true;
            let line = self
                .next_lines
                .pop_front()
                .unwrap_or_else(|| Ok(Vec::new()))?;
            self.ticks = stream::iter(line).chain(stream::pending()).boxed();
            Ok(())
        }
    }

    #[tokio::test]
    async fn an_idle_live_line_is_resubscribed_without_an_outage() {
        let mut state = quote_state();
        let mut line = FakeQuoteLine::new(
            vec![
                data(TickTypes::MarketDataType(MarketDataType::Realtime)),
                data(price_tick(TickType::Bid, 650.2)),
            ],
            // The replacement line is also quiet at first, then ticks.
            vec![
                Ok(Vec::new()),
                Ok(vec![data(price_tick(TickType::Bid, 650.3))]),
            ],
        );
        let idle = Duration::from_millis(20);
        let first = next_stock_quote_resubscribing_idle(
            &mut line,
            stream::pending(),
            &mut state,
            &mut QuoteRules::closed(),
            || 1,
            idle,
        )
        .await;
        assert!(matches!(first, Some(Ok(_))));

        // Two idle periods pass; the only thing the stream yields is the next
        // quote, never an unavailable error the transports would publish.
        let next = next_stock_quote_resubscribing_idle(
            &mut line,
            stream::pending(),
            &mut state,
            &mut QuoteRules::closed(),
            || 2,
            idle,
        )
        .await;
        let Some(Ok(quote)) = next else {
            panic!("expected a quote after resubscribing, got {next:?}");
        };
        assert_eq!(quote.bid, Some(650.3));
        assert_eq!(line.resubscribes, 2);
    }

    #[tokio::test]
    async fn arriving_tick_rules_never_cut_a_resubscribe_short() {
        let mut state = quote_state();
        let mut line = FakeQuoteLine::new(
            vec![
                data(TickTypes::MarketDataType(MarketDataType::Realtime)),
                data(price_tick(TickType::Bid, 650.2)),
            ],
            vec![Ok(Vec::new())],
        );
        line.resubscribe_delay = Duration::from_millis(150);
        let (sender, receiver) = tokio::sync::watch::channel(None);
        let mut rules = QuoteRules::new(receiver);
        let idle = Duration::from_millis(20);
        let first = next_stock_quote_resubscribing_idle(
            &mut line,
            stream::pending(),
            &mut state,
            &mut rules,
            || 1,
            idle,
        )
        .await;
        assert!(matches!(first, Some(Ok(_))));

        // The line goes idle at 20 ms and resubscribes until 170 ms; the
        // rules arrive in the middle of that.
        let tick_rules = vec![PriceIncrementRule {
            lower_edge: 0.0,
            increment: 0.01,
        }];
        let sent = tick_rules.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(80)).await;
            let _ = sender.send(Some(sent));
            // Keep the sender until the wait has read the rules.
            tokio::time::sleep(Duration::from_secs(1)).await;
        });
        let next = next_stock_quote_resubscribing_idle(
            &mut line,
            stream::pending(),
            &mut state,
            &mut rules,
            || 2,
            idle,
        )
        .await;
        assert!(line.line_open, "the resubscribe finished");
        assert_eq!(line.resubscribes, 1);
        let Some(Ok(quote)) = next else {
            panic!("expected the quote with its rules, got {next:?}");
        };
        assert_eq!(quote.price_increment_rules, tick_rules);
        assert_eq!(quote.bid, Some(650.2));
    }

    #[tokio::test]
    async fn a_failed_idle_resubscribe_is_the_outage() {
        let mut state = quote_state();
        let mut line = FakeQuoteLine::new(
            vec![
                data(TickTypes::MarketDataType(MarketDataType::Realtime)),
                data(price_tick(TickType::Bid, 650.2)),
            ],
            vec![Err(GatewayError::Unavailable("IB Gateway closed".into()))],
        );
        let idle = Duration::from_millis(20);
        let _ = next_stock_quote_resubscribing_idle(
            &mut line,
            stream::pending(),
            &mut state,
            &mut QuoteRules::closed(),
            || 1,
            idle,
        )
        .await;
        let failed = next_stock_quote_resubscribing_idle(
            &mut line,
            stream::pending(),
            &mut state,
            &mut QuoteRules::closed(),
            || 2,
            idle,
        )
        .await;
        assert!(matches!(failed, Some(Err(GatewayError::Unavailable(_)))));
    }

    #[tokio::test]
    async fn connectivity_loss_still_ends_a_resubscribing_stream() {
        let mut state = quote_state();
        let mut line = FakeQuoteLine::new(
            vec![
                data(TickTypes::MarketDataType(MarketDataType::Realtime)),
                data(price_tick(TickType::Bid, 650.2)),
            ],
            Vec::new(),
        );
        let (notices, mut receiver) = futures::channel::mpsc::unbounded();
        let first = next_stock_quote_resubscribing_idle(
            &mut line,
            &mut receiver,
            &mut state,
            &mut QuoteRules::closed(),
            || 1,
            Duration::from_secs(60),
        )
        .await;
        assert!(matches!(first, Some(Ok(_))));
        notices.unbounded_send(notice(1100)).unwrap();
        let lost = next_stock_quote_resubscribing_idle(
            &mut line,
            &mut receiver,
            &mut state,
            &mut QuoteRules::closed(),
            || 2,
            Duration::from_secs(60),
        )
        .await;
        assert!(matches!(lost, Some(Err(GatewayError::Unavailable(_)))));
        assert_eq!(line.resubscribes, 0);
    }

    #[test]
    fn only_input_and_contract_failures_end_a_quote_stream() {
        assert!(!should_retry_stock_quote(&GatewayError::Input("x".into())));
        assert!(!should_retry_stock_quote(&GatewayError::Contract(
            "x".into()
        )));
        assert!(should_retry_stock_quote(&GatewayError::Unavailable(
            "x".into()
        )));
        assert!(should_retry_stock_quote(&GatewayError::Provider(
            "x".into()
        )));
        assert!(should_retry_stock_quote(&GatewayError::Configuration(
            "x".into()
        )));
        assert!(matches!(
            contract_lookup_error("stock contract details", ibapi::Error::Notice(notice(200))),
            GatewayError::Contract(_)
        ));
        assert!(matches!(
            contract_lookup_error("stock quote stream", ibapi::Error::Notice(notice(10197))),
            GatewayError::Provider(_)
        ));
    }

    #[tokio::test]
    async fn quote_setup_never_holds_the_order_lock() {
        // A socket that accepts but never completes the TWS handshake keeps
        // quote setup waiting on its connection for the whole connect timeout.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let mut held = Vec::new();
            while let Ok((socket, _)) = listener.accept().await {
                held.push(socket);
            }
        });
        let gateway =
            Arc::new(Gateway::new(GatewayConfig::new(vec![address], 1).unwrap()).unwrap());
        let quote_gateway = Arc::clone(&gateway);
        let setup = tokio::spawn(async move {
            quote_gateway
                .stock_quote_stream(StockQuoteRequest {
                    symbol: "SPY".to_string(),
                    conid: None,
                })
                .await
        });
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(
            !setup.is_finished(),
            "quote setup should still be connecting"
        );
        // An order acquires its mutex within its own deadline meanwhile.
        let order_lock =
            tokio::time::timeout(ORDER_OPERATION_LOCK_TIMEOUT, gateway.operation.lock()).await;
        assert!(order_lock.is_ok());
        drop(order_lock);
        setup.abort();
        server.abort();
    }

    #[test]
    fn stock_quote_symbols_are_strict_without_a_conid() {
        assert_eq!(stock_quote_symbol(" spy ", None).unwrap(), "SPY");
        assert!(matches!(
            stock_quote_symbol("BRK B", None),
            Err(GatewayError::Input(_))
        ));
        assert_eq!(
            stock_quote_symbol("BRK B", Some(72_063_691)).unwrap(),
            "BRK B"
        );
        assert!(matches!(
            stock_quote_symbol("", Some(1)),
            Err(GatewayError::Input(_))
        ));
        assert!(matches!(
            stock_quote_symbol("BRK B", Some(0)),
            Err(GatewayError::Input(_))
        ));
    }

    #[test]
    fn stock_quotes_route_through_smart_and_keep_the_listing() {
        let mut contract = Contract::stock("AAPL").build();
        contract.contract_id = 265_598;
        contract.exchange = "NASDAQ".into();
        contract.primary_exchange = "NASDAQ".into();
        let routed = route_stock_quote_contract(contract);
        assert_eq!(routed.exchange.as_str(), "SMART");
        assert_eq!(routed.primary_exchange.as_str(), "NASDAQ");
        assert_eq!(routed.contract_id, 265_598);
    }

    #[test]
    fn combined_delayed_price_size_ticks_supply_a_reference_quote() {
        let mut quote = MarketQuote::default();
        apply_tick(
            &mut quote,
            TickTypes::PriceSize(ibapi::market_data::realtime::TickPriceSize {
                price_tick_type: TickType::DelayedBid,
                price: 152.9,
                size_tick_type: TickType::DelayedBidSize,
                size: 100.0,
                ..Default::default()
            }),
        );
        apply_tick(
            &mut quote,
            TickTypes::PriceSize(ibapi::market_data::realtime::TickPriceSize {
                price_tick_type: TickType::DelayedAsk,
                price: 153.5,
                size_tick_type: TickType::DelayedAskSize,
                size: 300.0,
                ..Default::default()
            }),
        );

        assert_eq!(quote.reference_price(), Some(153.2));
        assert_eq!(quote.bid_size, Some(100.0));
        assert_eq!(quote.ask_size, Some(300.0));
        assert!(quote.delayed);
    }

    #[test]
    fn delayed_close_supplies_a_closed_market_reference_quote() {
        let mut quote = MarketQuote::default();
        apply_tick(
            &mut quote,
            TickTypes::Price(ibapi::market_data::realtime::TickPrice {
                tick_type: TickType::DelayedClose,
                price: 153.2,
                ..Default::default()
            }),
        );

        assert_eq!(quote.reference_price(), Some(153.2));
        assert!(quote.delayed);
    }

    #[test]
    fn option_volume_size_ticks_are_preserved() {
        let mut quote = MarketQuote::default();
        apply_tick(
            &mut quote,
            TickTypes::Size(ibapi::market_data::realtime::TickSize {
                tick_type: TickType::OptionCallVolume,
                size: 42.0,
            }),
        );

        assert_eq!(quote.volume, Some(42.0));
    }

    #[test]
    fn delayed_provider_timestamp_is_preserved_and_classified() {
        let mut quote = MarketQuote::default();
        apply_tick(
            &mut quote,
            TickTypes::String(ibapi::market_data::realtime::TickString {
                tick_type: TickType::DelayedLastTimestamp,
                value: "1785748114".to_string(),
            }),
        );

        assert_eq!(quote.quote_timestamp_ms, 1_785_748_114_000);
        assert!(quote.delayed);
    }

    #[test]
    fn by_conid_queries_name_the_security_type_ibapi_sends() {
        // ibapi encodes `security_type` as `sec_type`, defaulting to STK.
        assert_eq!(Contract::default().security_type, SecurityType::Stock);
        for (path, query, expected, wire) in [
            (
                "option quote",
                option_quote_query(900_001),
                SecurityType::Option,
                "OPT",
            ),
            (
                "option order",
                option_order_query(900_001),
                SecurityType::Option,
                "OPT",
            ),
            (
                "stock by conid",
                stock_conid_query(265_598),
                SecurityType::Stock,
                "STK",
            ),
        ] {
            let query = query.unwrap_or_else(|error| panic!("{path}: {error:?}"));
            assert_eq!(query.security_type, expected, "{path}");
            assert_eq!(query.security_type.to_string(), wire, "{path}");
            assert!(query.contract_id > 0, "{path}");
        }
        for conid in [0, -1, i64::from(i32::MAX) + 1] {
            assert!(matches!(
                option_quote_query(conid),
                Err(GatewayError::Input(_))
            ));
        }
    }

    #[test]
    fn live_option_quote_collection_waits_for_ranking_metadata() {
        let mut quote = MarketQuote {
            bid: Some(1.0),
            ask: Some(1.1),
            bid_size: Some(10.0),
            ask_size: Some(12.0),
            quote_timestamp_ms: 1,
            ..Default::default()
        };
        assert!(!option_quote_collection_complete(&quote));

        quote.volume = Some(100.0);
        quote.open_interest = Some(200.0);
        quote.delta = Some(0.5);
        assert!(option_quote_collection_complete(&quote));
    }

    #[test]
    fn delayed_option_quote_collection_does_not_wait_for_trade_metadata() {
        let quote = MarketQuote {
            bid: Some(1.0),
            ask: Some(1.1),
            bid_size: Some(10.0),
            ask_size: Some(12.0),
            delayed: true,
            quote_timestamp_ms: 1,
            ..Default::default()
        };

        assert!(option_quote_collection_complete(&quote));
    }

    #[test]
    fn partial_option_subscription_notice_allows_a_nontradable_quote() {
        let partial = ibapi::Notice {
            code: 10091,
            message: "Part of requested market data requires additional subscription".to_string(),
            error_time: None,
            advanced_order_reject_json: String::new(),
        };
        let rejected = ibapi::Notice {
            code: 321,
            message: "Error validating request".to_string(),
            error_time: None,
            advanced_order_reject_json: String::new(),
        };

        let mut quote = MarketQuote::default();
        assert!(apply_partial_option_notice(&mut quote, &partial));
        assert!(quote.partial_entitlement);

        let mut rejected_quote = MarketQuote::default();
        assert!(!apply_partial_option_notice(&mut rejected_quote, &rejected));
        assert!(!rejected_quote.partial_entitlement);
    }

    #[test]
    fn tradable_option_quote_requires_provider_timestamp() {
        let rules = vec![PriceIncrementRule {
            lower_edge: 0.0,
            increment: 0.01,
        }];
        let mut quote = MarketQuote {
            bid: Some(1.0),
            ask: Some(1.1),
            ..Default::default()
        };
        assert!(!option_quote_can_trade(&quote, &rules));

        quote.quote_timestamp_ms = 1;
        assert!(option_quote_can_trade(&quote, &rules));

        quote.partial_entitlement = true;
        assert!(!option_quote_can_trade(&quote, &rules));
    }

    #[test]
    fn maximum_option_quote_phase_fits_the_caller_deadline() {
        let maximum_contracts = MAX_STRIKES_PER_SIDE * 2 * 2;
        let waves = maximum_contracts.div_ceil(OPTION_QUOTE_CONCURRENCY);
        // Mode-lock wait, mode switch plus subscribe, and tick collection.
        let deadlines_per_wave = 3;
        let maximum_quote_phase = QUOTE_TIMEOUT
            * u32::try_from(waves * deadlines_per_wave)
                .expect("bounded option quote waves fit in u32");

        assert!(maximum_quote_phase < OPTION_CHAIN_CALLER_TIMEOUT_FLOOR);
    }

    #[test]
    fn order_phase_deadlines_finish_before_caller_deadlines() {
        // Bracket sends share one submission deadline, so this sum also
        // bounds a bracket.
        let internal_budget = ORDER_OPERATION_LOCK_TIMEOUT
            + ORDER_PREPARATION_TIMEOUT
            + ORDER_SUBMISSION_TIMEOUT
            + ORDER_ACKNOWLEDGEMENT_TIMEOUT
            + ORDER_FOLLOW_UP_TIMEOUT;
        assert!(internal_budget < ORDER_CALLER_TIMEOUT_FLOOR);
    }

    #[test]
    fn terminal_order_statuses_map_to_stable_outcomes() {
        let filled = order_event_outcome(
            42,
            "U1234567",
            "sunk-test",
            PlaceOrder::OrderStatus(ibapi::orders::OrderStatus {
                order_id: 42,
                status: OrderStatusKind::Filled,
                filled: 1.0,
                remaining: 0.0,
                ..Default::default()
            }),
        )
        .expect("filled outcome");
        assert!(filled.ok);
        assert_eq!(filled.status, "filled");

        let rejected = order_event_outcome(
            42,
            "U1234567",
            "sunk-test",
            PlaceOrder::OrderStatus(ibapi::orders::OrderStatus {
                order_id: 42,
                status: OrderStatusKind::Inactive,
                ..Default::default()
            }),
        )
        .expect("rejected outcome");
        assert!(!rejected.ok);
        assert_eq!(rejected.status, "rejected");
    }

    #[test]
    fn partial_execution_is_accepted_but_not_reported_as_filled() {
        let response = order_event_outcome(
            42,
            "U1234567",
            "sunk-test",
            PlaceOrder::ExecutionData(ibapi::orders::ExecutionData {
                execution: ibapi::orders::Execution {
                    order_id: 42,
                    shares: 1.0,
                    cumulative_quantity: 1.0,
                    ..Default::default()
                },
                ..Default::default()
            }),
        )
        .expect("execution acknowledgement");
        assert!(response.ok);
        assert_eq!(response.status, "accepted");
    }

    #[test]
    fn cancelled_partial_fill_is_accepted_but_not_reported_as_rejected() {
        let response = order_event_outcome(
            42,
            "U1234567",
            "sunk-test",
            PlaceOrder::OrderStatus(ibapi::orders::OrderStatus {
                order_id: 42,
                status: OrderStatusKind::Cancelled,
                filled: 1.0,
                remaining: 1.0,
                ..Default::default()
            }),
        )
        .expect("partial-fill cancellation outcome");
        assert!(response.ok);
        assert_eq!(response.status, "accepted");
    }

    const TAKE_PROFIT_ORDER_ID: &str = "sunk-1b2c3d4e-5f60-4a71-8b92-a3b4c5d6e7f8";
    const STOP_LOSS_ORDER_ID: &str = "sunk-2c3d4e5f-6071-4b82-9ca3-b4c5d6e7f809";

    fn bracket_request() -> OrderRequest {
        OrderRequest {
            quantity: Some(10.0),
            limit_price: Some(650.25),
            time_in_force: "gtc".to_string(),
            protection: Some(OrderProtection {
                take_profit: Some(ProtectionLegRequest {
                    price: 700.0,
                    customer_order_id: TAKE_PROFIT_ORDER_ID.to_string(),
                }),
                stop_loss: Some(ProtectionLegRequest {
                    price: 620.0,
                    customer_order_id: STOP_LOSS_ORDER_ID.to_string(),
                }),
            }),
            ..order_request()
        }
    }

    fn penny_rules() -> Vec<PriceIncrementRule> {
        vec![PriceIncrementRule {
            lower_edge: 0.0,
            increment: 0.01,
        }]
    }

    fn aligned_bracket(request: &OrderRequest) -> Vec<AlignedProtectionLeg> {
        align_protection(
            request,
            request.protection.as_ref().expect("protection"),
            &penny_rules(),
        )
        .expect("aligned protection")
    }

    #[test]
    fn protection_contract_round_trips_camel_case_fields() {
        let value = serde_json::to_value(bracket_request()).expect("serialize");
        assert_eq!(
            value["protection"],
            serde_json::json!({
                "takeProfit": { "price": 700.0, "customerOrderId": TAKE_PROFIT_ORDER_ID },
                "stopLoss": { "price": 620.0, "customerOrderId": STOP_LOSS_ORDER_ID },
            })
        );
        let parsed: OrderRequest = serde_json::from_value(value).expect("parse");
        assert_eq!(parsed.protection, bracket_request().protection);
        let plain = serde_json::to_value(order_request()).expect("serialize");
        assert_eq!(plain.get("protection"), None);
    }

    #[test]
    fn bracket_children_work_outside_rth_when_the_parent_does() {
        let mut request = bracket_request();
        request.outside_rth = true;
        let plan = build_bracket(&request, "U1234567", 100, &aligned_bracket(&request));
        assert_eq!(plan.orders.len(), 3);
        for BracketOrder { order_id, order } in &plan.orders {
            assert!(order.outside_rth, "outsideRth of {order_id}");
        }
        // The stop loss keeps IBKR's default trigger method.
        assert_eq!(
            plan.orders[2].order.trigger_method,
            Order::default().trigger_method
        );

        let request = bracket_request();
        let plan = build_bracket(&request, "U1234567", 100, &aligned_bracket(&request));
        for BracketOrder { order_id, order } in &plan.orders {
            assert!(!order.outside_rth, "outsideRth of {order_id}");
        }
    }

    #[test]
    fn bracket_follows_the_tws_sample_order_layout() {
        let request = bracket_request();
        validate_order_request(&request).expect("valid bracket");
        let plan = build_bracket(&request, "U1234567", 100, &aligned_bracket(&request));
        let [parent, take_profit, stop_loss] = plan.orders.as_slice() else {
            panic!("expected three bracket orders");
        };

        assert_eq!(parent.order_id, 100);
        assert_eq!(parent.order.action, Action::Buy);
        assert_eq!(parent.order.order_type, "LMT");
        assert_eq!(parent.order.limit_price, Some(650.25));
        assert_eq!(parent.order.total_quantity, 10.0);
        assert_eq!(parent.order.cash_qty, None);
        assert_eq!(parent.order.parent_id, 0);
        assert_eq!(parent.order.order_ref, CUSTOMER_ORDER_ID);
        assert!(!parent.order.transmit);

        assert_eq!(take_profit.order_id, 101);
        assert_eq!(take_profit.order.action, Action::Sell);
        assert_eq!(take_profit.order.order_type, "LMT");
        assert_eq!(take_profit.order.limit_price, Some(700.0));
        assert_eq!(take_profit.order.aux_price, None);
        assert_eq!(take_profit.order.total_quantity, 10.0);
        assert_eq!(take_profit.order.parent_id, 100);
        assert_eq!(take_profit.order.order_ref, TAKE_PROFIT_ORDER_ID);
        assert!(!take_profit.order.transmit);

        assert_eq!(stop_loss.order_id, 102);
        assert_eq!(stop_loss.order.action, Action::Sell);
        assert_eq!(stop_loss.order.order_type, "STP");
        assert_eq!(stop_loss.order.aux_price, Some(620.0));
        assert_eq!(stop_loss.order.limit_price, None);
        assert_eq!(stop_loss.order.total_quantity, 10.0);
        assert_eq!(stop_loss.order.parent_id, 100);
        assert_eq!(stop_loss.order.order_ref, STOP_LOSS_ORDER_ID);
        assert!(stop_loss.order.transmit);

        for child in [take_profit, stop_loss] {
            assert_eq!(child.order.tif, TimeInForce::GoodTilCanceled);
            assert_eq!(child.order.oca_group, format!("oca-{CUSTOMER_ORDER_ID}"));
            assert_eq!(child.order.oca_type, OcaType::ReduceWithBlock);
            assert_eq!(child.order.account, "U1234567");
            assert!(!child.order.override_percentage_constraints);
        }
        assert_eq!(parent.order.oca_group, "");
    }

    #[test]
    fn single_child_bracket_transmits_on_its_only_child() {
        let mut request = bracket_request();
        request.side = "sell".to_string();
        request.order_type = "market".to_string();
        request.limit_price = None;
        request.market_slippage_bps = None;
        request.protection = Some(OrderProtection {
            take_profit: None,
            stop_loss: Some(ProtectionLegRequest {
                price: 680.0,
                customer_order_id: STOP_LOSS_ORDER_ID.to_string(),
            }),
        });
        validate_order_request(&request).expect("valid short stop");
        let plan = build_bracket(&request, "U1234567", 7, &aligned_bracket(&request));
        assert_eq!(plan.orders.len(), 2);
        assert_eq!(plan.orders[0].order.order_type, "MKT");
        assert!(!plan.orders[0].order.transmit);
        assert_eq!(plan.orders[1].order_id, 8);
        assert_eq!(plan.orders[1].order.action, Action::Buy);
        assert_eq!(plan.orders[1].order.order_type, "STP");
        assert_eq!(plan.orders[1].order.parent_id, 7);
        assert!(plan.orders[1].order.transmit);
    }

    #[test]
    fn unsupported_protection_fails_before_submission() {
        let unsupported = |mutate: &dyn Fn(&mut OrderRequest)| {
            let mut request = bracket_request();
            mutate(&mut request);
            match validate_order_request(&request) {
                Err(GatewayError::Input(message)) => message,
                other => panic!("expected input error, got {other:?}"),
            }
        };
        assert!(
            unsupported(&|request| request.quantity = None).contains("unsupported IBKR protection")
        );
        assert!(unsupported(&|request| request.quantity = Some(2.5)).contains("whole"));
        assert!(unsupported(&|request| request.time_in_force = "ioc".to_string()).contains("IOC"));
        assert!(
            unsupported(&|request| {
                request.order_type = "stop_market".to_string();
                request.limit_price = None;
                request.stop_price = Some(640.0);
                request.market_slippage_bps = None;
            })
            .contains("market or limit")
        );
        assert!(
            unsupported(&|request| request.protection = Some(OrderProtection {
                take_profit: None,
                stop_loss: None,
            }))
            .contains("take profit or a stop loss")
        );
        assert!(
            unsupported(&|request| {
                if let Some(leg) = request
                    .protection
                    .as_mut()
                    .and_then(|p| p.stop_loss.as_mut())
                {
                    leg.customer_order_id = CUSTOMER_ORDER_ID.to_string();
                }
            })
            .contains("own customerOrderId")
        );
        assert!(
            unsupported(&|request| {
                if let Some(leg) = request
                    .protection
                    .as_mut()
                    .and_then(|p| p.stop_loss.as_mut())
                {
                    leg.price = 660.0;
                }
            })
            .contains("behind the limit")
        );
        assert!(
            unsupported(&|request| {
                if let Some(leg) = request
                    .protection
                    .as_mut()
                    .and_then(|p| p.take_profit.as_mut())
                {
                    leg.price = 600.0;
                }
            })
            .contains("above stop loss")
        );
        assert!(unsupported(&|request| request.reduce_only = true).contains("reduce-only"));
        assert!(
            unsupported(&|request| {
                if let Some(leg) = request
                    .protection
                    .as_mut()
                    .and_then(|p| p.take_profit.as_mut())
                {
                    leg.customer_order_id = "sunk-not-a-uuid".to_string();
                }
            })
            .contains("customerOrderId")
        );
    }

    #[test]
    fn protection_prices_round_toward_the_entry() {
        let mut request = bracket_request();
        request.protection = Some(OrderProtection {
            take_profit: Some(ProtectionLegRequest {
                price: 700.006,
                customer_order_id: TAKE_PROFIT_ORDER_ID.to_string(),
            }),
            stop_loss: Some(ProtectionLegRequest {
                price: 620.004,
                customer_order_id: STOP_LOSS_ORDER_ID.to_string(),
            }),
        });
        let legs = aligned_bracket(&request);
        assert_eq!(legs[0].price, 700.0);
        assert_eq!(legs[1].price, 620.01);

        request.side = "sell".to_string();
        request.limit_price = Some(650.0);
        request.protection = Some(OrderProtection {
            take_profit: Some(ProtectionLegRequest {
                price: 600.004,
                customer_order_id: TAKE_PROFIT_ORDER_ID.to_string(),
            }),
            stop_loss: Some(ProtectionLegRequest {
                price: 680.006,
                customer_order_id: STOP_LOSS_ORDER_ID.to_string(),
            }),
        });
        let legs = aligned_bracket(&request);
        assert_eq!(legs[0].price, 600.01);
        assert_eq!(legs[1].price, 680.0);
        assert_eq!(align_price(0.004, &penny_rules(), false).ok(), None);
    }

    fn child_status(order_id: i32, status: OrderStatusKind) -> BracketEvent {
        BracketEvent::Data(Box::new(PlaceOrder::OrderStatus(
            ibapi::orders::OrderStatus {
                order_id,
                status,
                ..Default::default()
            },
        )))
    }

    fn parent_filled() -> BracketEvent {
        BracketEvent::Data(Box::new(PlaceOrder::OrderStatus(
            ibapi::orders::OrderStatus {
                order_id: 100,
                status: OrderStatusKind::Filled,
                filled: 10.0,
                remaining: 0.0,
                ..Default::default()
            },
        )))
    }

    fn tracker() -> BracketTracker {
        let request = bracket_request();
        let plan = build_bracket(&request, "U1234567", 100, &aligned_bracket(&request));
        BracketTracker::new("U1234567", &plan)
    }

    #[test]
    fn filled_parent_with_working_children_is_attached() {
        let mut tracker = tracker();
        assert!(
            tracker
                .apply(1, child_status(101, OrderStatusKind::PreSubmitted))
                .is_none()
        );
        assert!(tracker.apply(0, parent_filled()).is_none());
        let response = tracker
            .apply(2, child_status(102, OrderStatusKind::PreSubmitted))
            .expect("complete bracket");
        assert!(response.ok);
        assert_eq!(response.status, "filled");
        assert_eq!(
            response.customer_order_id.as_deref(),
            Some(CUSTOMER_ORDER_ID)
        );
        let protection = response.protection.expect("protection outcome");
        assert_eq!(protection.status, ProtectionStatus::Attached);
        assert_eq!(protection.legs.len(), 2);
        assert_eq!(protection.legs[0].kind, ProtectionLegKind::TakeProfit);
        assert_eq!(protection.legs[0].order_id.as_deref(), Some("101"));
        assert_eq!(protection.legs[0].customer_order_id, TAKE_PROFIT_ORDER_ID);
        assert_eq!(protection.legs[1].customer_order_id, STOP_LOSS_ORDER_ID);
    }

    #[test]
    fn resting_parent_holds_children_pending() {
        let mut tracker = tracker();
        tracker.apply(0, child_status(100, OrderStatusKind::Submitted));
        tracker.apply(1, child_status(101, OrderStatusKind::PreSubmitted));
        let response = tracker
            .apply(2, child_status(102, OrderStatusKind::PreSubmitted))
            .expect("complete bracket");
        assert_eq!(response.status, "resting");
        let protection = response.protection.expect("protection outcome");
        assert_eq!(protection.status, ProtectionStatus::Pending);
        assert!(protection.message.contains("until the order fills"));
    }

    #[test]
    fn filled_parent_with_a_rejected_child_reports_partial_acceptance() {
        let mut tracker = tracker();
        tracker.apply(0, parent_filled());
        tracker.apply(1, child_status(101, OrderStatusKind::Submitted));
        let response = tracker
            .apply(
                2,
                BracketEvent::Rejected("Order rejected - stop price invalid".to_string()),
            )
            .expect("complete bracket");
        assert!(response.ok, "the parent filled");
        assert_eq!(response.status, "filled");
        let protection = response.protection.expect("protection outcome");
        assert_eq!(protection.status, ProtectionStatus::Rejected);
        assert_eq!(protection.legs[0].status, ProtectionStatus::Attached);
        assert_eq!(protection.legs[1].status, ProtectionStatus::Rejected);
        assert!(protection.message.contains("rejected the stop loss at 620"));
        assert!(protection.message.contains("stop price invalid"));
        assert!(protection.message.contains("may be unprotected"));
    }

    #[test]
    fn silent_child_is_unknown_when_the_deadline_passes() {
        let mut tracker = tracker();
        tracker.apply(0, parent_filled());
        tracker.apply(1, child_status(101, OrderStatusKind::Submitted));
        let response = tracker.finish();
        assert!(response.ok);
        let protection = response.protection.expect("protection outcome");
        assert_eq!(protection.status, ProtectionStatus::Unknown);
        assert_eq!(protection.legs[1].status, ProtectionStatus::Unknown);
        assert!(protection.message.contains("did not confirm the stop loss"));
    }

    #[test]
    fn pending_submit_child_waits_for_confirmation() {
        let mut tracker = tracker();
        tracker.apply(0, parent_filled());
        tracker.apply(1, child_status(101, OrderStatusKind::Submitted));
        assert!(
            tracker
                .apply(2, child_status(102, OrderStatusKind::PendingSubmit))
                .is_none(),
            "PendingSubmit does not settle the bracket"
        );
        let response = tracker
            .apply(2, child_status(102, OrderStatusKind::Inactive))
            .expect("the later rejection settles it");
        let protection = response.protection.expect("protection outcome");
        assert_eq!(protection.legs[1].status, ProtectionStatus::Rejected);

        let mut silent = self::tracker();
        silent.apply(0, parent_filled());
        silent.apply(1, child_status(101, OrderStatusKind::Submitted));
        silent.apply(2, child_status(102, OrderStatusKind::PendingSubmit));
        let protection = silent.finish().protection.expect("protection outcome");
        assert_eq!(protection.legs[1].status, ProtectionStatus::Unknown);
    }

    #[test]
    fn rejected_or_silent_parent_never_reports_protection() {
        let mut rejected = tracker();
        let response = rejected
            .apply(0, BracketEvent::Rejected("insufficient funds".to_string()))
            .expect("parent rejection ends the bracket");
        assert!(!response.ok);
        assert_eq!(response.status, "rejected");
        assert!(response.protection.is_none());

        let mut cancelled = tracker();
        let response = cancelled
            .apply(0, child_status(100, OrderStatusKind::Cancelled))
            .expect("parent cancellation ends the bracket");
        assert_eq!(response.status, "rejected");
        assert!(response.protection.is_none());

        let mut silent = tracker();
        silent.apply(1, child_status(101, OrderStatusKind::PreSubmitted));
        let response = silent.finish();
        assert_eq!(response.status, "indeterminate");
        assert_eq!(
            response.customer_order_id.as_deref(),
            Some(CUSTOMER_ORDER_ID)
        );
        assert!(response.protection.is_none());
    }

    /// IBKR's 399 text for a bracket parent sent outside regular hours.
    const BRACKET_HELD_UNTIL_OPEN: &str = "Order Message:\nBUY 10 SPY SMART\nWarning: Your order will not be placed at the exchange until 2026-10-01 09:30:00 US/Eastern.";

    fn stream_error(
        code: i32,
        message: &str,
    ) -> Result<SubscriptionItem<PlaceOrder>, ibapi::Error> {
        Err(ibapi::Error::Notice(ibapi::Notice {
            code,
            message: message.to_string(),
            error_time: None,
            advanced_order_reject_json: String::new(),
        }))
    }

    fn listed_order(order_ref: &str, status: OrderStatusKind, completed: &str) -> OrderData {
        let mut order = OrderData::default();
        order.order.order_ref = order_ref.to_string();
        order.order.account = "U1234567".to_string();
        order.order.total_quantity = 10.0;
        order.order_state.status = status;
        order.order_state.completed_status = completed.to_string();
        order
    }

    #[test]
    fn bracket_stream_items_are_classified_by_documented_code() {
        assert!(matches!(
            bracket_event(stream_error(399, BRACKET_HELD_UNTIL_OPEN)),
            BracketEvent::FollowUp(ProviderNote {
                code: Some(399),
                ..
            })
        ));
        assert!(matches!(
            bracket_event(Ok(SubscriptionItem::Notice(ibapi::Notice {
                code: 2109,
                message:
                    "Order Event Warning: Attribute \"Outside Regular Trading Hours\" is ignored"
                        .to_string(),
                error_time: None,
                advanced_order_reject_json: String::new(),
            }))),
            BracketEvent::Note(ProviderNote {
                code: Some(2109),
                ..
            })
        ));
        assert!(matches!(
            bracket_event(stream_error(201, "Order rejected - Reason: margin")),
            BracketEvent::Rejected(message) if message == "[201] Order rejected - Reason: margin"
        ));
        assert!(matches!(
            bracket_event(stream_error(10148, "cannot be cancelled")),
            BracketEvent::StreamFailed(message) if message.starts_with("[10148]")
        ));
    }

    #[test]
    fn held_parent_is_read_back_and_accepted_with_the_warning() {
        let mut tracker = tracker();
        assert!(
            tracker
                .apply(0, bracket_event(stream_error(399, BRACKET_HELD_UNTIL_OPEN)))
                .is_none()
        );
        assert!(!tracker.streams_exhausted(), "children are still streaming");
        assert!(
            tracker
                .apply(1, child_status(101, OrderStatusKind::PreSubmitted))
                .is_none()
        );
        assert!(
            tracker
                .apply(2, child_status(102, OrderStatusKind::PreSubmitted))
                .is_none()
        );
        assert!(tracker.streams_exhausted());

        let unlisted = ProviderOrders::default();
        assert!(tracker.apply_provider_orders(&unlisted).is_none());
        let listed = ProviderOrders {
            open: vec![listed_order(
                CUSTOMER_ORDER_ID,
                OrderStatusKind::PreSubmitted,
                "",
            )],
            completed: Vec::new(),
        };
        let response = tracker
            .apply_provider_orders(&listed)
            .expect("parent settled from open orders");
        assert!(response.ok);
        assert_eq!(response.status, "resting");
        assert_eq!(
            response.message,
            "IBKR order is working (100). IBKR warning 399: Your order will not be placed at the exchange until 2026-10-01 09:30:00 US/Eastern."
        );
        let protection = response.protection.expect("protection outcome");
        assert_eq!(protection.status, ProtectionStatus::Pending);
    }

    #[test]
    fn held_parent_then_cancelled_is_rejected_with_the_reason() {
        let mut tracker = tracker();
        tracker.apply(0, bracket_event(stream_error(399, BRACKET_HELD_UNTIL_OPEN)));
        tracker.apply(1, bracket_event(stream_error(399, BRACKET_HELD_UNTIL_OPEN)));
        tracker.apply(2, bracket_event(stream_error(399, BRACKET_HELD_UNTIL_OPEN)));
        assert!(tracker.streams_exhausted());
        let listed = ProviderOrders {
            open: Vec::new(),
            completed: vec![listed_order(
                CUSTOMER_ORDER_ID,
                OrderStatusKind::Cancelled,
                "Cancelled by System",
            )],
        };
        let response = tracker
            .apply_provider_orders(&listed)
            .expect("cancelled parent ends the bracket");
        assert!(!response.ok);
        assert_eq!(response.status, "rejected");
        assert!(response.protection.is_none());
        assert_eq!(
            response.message,
            "IBKR order rejected or cancelled (100). IBKR warning 399: Your order will not be placed at the exchange until 2026-10-01 09:30:00 US/Eastern. IBKR reason: Cancelled by System."
        );
    }

    #[test]
    fn documented_parent_rejection_code_rejects_the_bracket() {
        let response = tracker()
            .apply(
                0,
                bracket_event(stream_error(
                    201,
                    "Order rejected - Reason:\nYOUR ORDER IS NOT ACCEPTED.",
                )),
            )
            .expect("rejection ends the bracket");
        assert!(!response.ok);
        assert_eq!(response.status, "rejected");
        assert!(
            response
                .message
                .contains("[201] Order rejected - Reason: YOUR ORDER IS NOT ACCEPTED")
        );
        assert!(response.protection.is_none());
    }

    #[test]
    fn unknown_parent_stream_error_stays_indeterminate() {
        let mut tracker = tracker();
        let response = tracker
            .apply(0, bracket_event(stream_error(10148, "cannot be cancelled")))
            .expect("unknown error ends the bracket");
        assert_eq!(response.status, "indeterminate");
        assert!(
            response
                .message
                .contains("the order event stream failed: [10148] cannot be cancelled")
        );
        assert!(!tracker.needs_follow_up());
    }

    #[test]
    fn held_parent_without_provider_confirmation_stays_indeterminate() {
        let mut tracker = tracker();
        tracker.apply(0, bracket_event(stream_error(399, BRACKET_HELD_UNTIL_OPEN)));
        assert!(tracker.needs_follow_up());
        let response = tracker.finish();
        assert_eq!(response.status, "indeterminate");
        assert!(response.message.contains(
            "IBKR sent [399] Your order will not be placed at the exchange until 2026-10-01 09:30:00 US/Eastern but did not confirm the parent order state"
        ));
    }

    #[test]
    fn held_child_is_read_back_from_open_orders() {
        let mut tracker = tracker();
        tracker.apply(0, parent_filled());
        tracker.apply(1, child_status(101, OrderStatusKind::Submitted));
        assert!(
            tracker
                .apply(2, bracket_event(stream_error(399, BRACKET_HELD_UNTIL_OPEN)))
                .is_none()
        );
        assert!(tracker.streams_exhausted());
        let listed = ProviderOrders {
            open: vec![listed_order(
                STOP_LOSS_ORDER_ID,
                OrderStatusKind::PreSubmitted,
                "",
            )],
            completed: Vec::new(),
        };
        let response = tracker
            .apply_provider_orders(&listed)
            .expect("child settled from open orders");
        assert_eq!(response.status, "filled");
        let protection = response.protection.expect("protection outcome");
        assert_eq!(protection.status, ProtectionStatus::Attached);
    }

    #[test]
    fn cancellation_after_a_working_parent_is_read_back_to_a_final_state() {
        let mut tracker = tracker();
        tracker.apply(0, child_status(100, OrderStatusKind::Submitted));
        tracker.apply(1, child_status(101, OrderStatusKind::PreSubmitted));
        assert!(
            tracker
                .apply(
                    0,
                    bracket_event(stream_error(202, "Order cancelled - Reason: "))
                )
                .is_none(),
            "a cancelled parent is no longer settled"
        );
        assert!(
            tracker
                .apply(2, child_status(102, OrderStatusKind::PreSubmitted))
                .is_none()
        );
        assert!(tracker.streams_exhausted());
        let stale = ProviderOrders {
            open: vec![listed_order(
                CUSTOMER_ORDER_ID,
                OrderStatusKind::Submitted,
                "",
            )],
            completed: Vec::new(),
        };
        assert!(
            tracker.apply_provider_orders(&stale).is_none(),
            "a working snapshot cannot settle a cancelled order"
        );
        let settled = ProviderOrders {
            open: vec![listed_order(
                CUSTOMER_ORDER_ID,
                OrderStatusKind::Submitted,
                "",
            )],
            completed: vec![listed_order(
                CUSTOMER_ORDER_ID,
                OrderStatusKind::Cancelled,
                "Cancelled by Trader",
            )],
        };
        let response = tracker
            .apply_provider_orders(&settled)
            .expect("completed state settles the parent");
        assert!(!response.ok);
        assert_eq!(response.status, "rejected");
        assert!(
            response
                .message
                .ends_with("IBKR reason 202: Order cancelled. IBKR reason: Cancelled by Trader.")
        );
    }

    #[test]
    fn a_cancelled_child_is_not_left_attached() {
        let mut tracker = tracker();
        tracker.apply(0, parent_filled());
        tracker.apply(1, child_status(101, OrderStatusKind::Submitted));
        tracker.apply(2, child_status(102, OrderStatusKind::PreSubmitted));
        let mut late = tracker;
        assert!(
            late.apply(2, bracket_event(stream_error(202, "Order cancelled")))
                .is_none()
        );
        let response = late
            .apply_provider_orders(&ProviderOrders {
                open: Vec::new(),
                completed: vec![listed_order(
                    STOP_LOSS_ORDER_ID,
                    OrderStatusKind::Cancelled,
                    "",
                )],
            })
            .expect("child settled");
        let protection = response.protection.expect("protection outcome");
        assert_eq!(protection.status, ProtectionStatus::Rejected);
        assert_eq!(protection.legs[1].status, ProtectionStatus::Rejected);
    }

    #[test]
    fn a_child_rejection_reason_is_not_overwritten_by_later_messages() {
        let mut tracker = tracker();
        tracker.apply(0, parent_filled());
        tracker.apply(1, child_status(101, OrderStatusKind::Submitted));
        tracker.apply(
            2,
            bracket_event(stream_error(
                201,
                "Order rejected - Reason: stop price invalid",
            )),
        );
        let note = Ok(SubscriptionItem::Notice(ibapi::Notice {
            code: 2109,
            message: "Order Event Warning: outside RTH ignored".to_string(),
            error_time: None,
            advanced_order_reject_json: String::new(),
        }));
        tracker.apply(2, bracket_event(note));
        tracker.apply(2, BracketEvent::Unexpected("[2100] later".to_string()));
        let response = tracker.finish();
        let protection = response.protection.expect("protection outcome");
        assert!(
            protection
                .message
                .contains("([201] Order rejected - Reason: stop price invalid)"),
            "{}",
            protection.message
        );
    }

    #[test]
    fn a_follow_up_child_rejected_without_status_text_shows_no_hold_reason() {
        let mut tracker = tracker();
        tracker.apply(0, parent_filled());
        tracker.apply(1, child_status(101, OrderStatusKind::Submitted));
        tracker.apply(2, bracket_event(stream_error(399, BRACKET_HELD_UNTIL_OPEN)));
        let response = tracker
            .apply_provider_orders(&ProviderOrders {
                open: Vec::new(),
                completed: vec![listed_order(
                    STOP_LOSS_ORDER_ID,
                    OrderStatusKind::Inactive,
                    "",
                )],
            })
            .expect("child settled");
        let protection = response.protection.expect("protection outcome");
        assert_eq!(protection.status, ProtectionStatus::Rejected);
        assert!(
            !protection.message.contains("will not be placed"),
            "{}",
            protection.message
        );
    }

    #[test]
    fn ended_streams_stop_the_wait_even_without_a_follow_up() {
        let mut tracker = tracker();
        tracker.apply(0, parent_filled());
        tracker.apply(1, child_status(101, OrderStatusKind::Submitted));
        assert!(!tracker.streams_exhausted());
        tracker.apply(2, bracket_event(stream_error(10148, "unknown")));
        assert!(tracker.streams_exhausted());
        assert!(!tracker.needs_follow_up());
        let protection = tracker.finish().protection.expect("protection outcome");
        assert_eq!(protection.legs[1].status, ProtectionStatus::Unknown);
    }

    #[test]
    fn documented_parent_warning_notice_keeps_the_bracket_open() {
        let mut tracker = tracker();
        let notice = Ok(SubscriptionItem::Notice(ibapi::Notice {
            code: 2109,
            message: "Order Event Warning: Attribute \"Outside Regular Trading Hours\" is ignored based on the order type and destination. PlaceOrder is now being processed.".to_string(),
            error_time: None,
            advanced_order_reject_json: String::new(),
        }));
        assert!(tracker.apply(0, bracket_event(notice)).is_none());
        assert!(!tracker.needs_follow_up());
        tracker.apply(0, child_status(100, OrderStatusKind::Submitted));
        tracker.apply(1, child_status(101, OrderStatusKind::PreSubmitted));
        let response = tracker
            .apply(2, child_status(102, OrderStatusKind::PreSubmitted))
            .expect("complete bracket");
        assert!(response.ok);
        assert_eq!(response.status, "resting");
        assert!(
            response.message.contains(
                "IBKR warning 2109: Attribute \"Outside Regular Trading Hours\" is ignored"
            )
        );
    }

    /// Minimal protobuf field reader for asserting the exact TWS wire frames.
    mod wire {
        use std::collections::BTreeMap;

        /// One decoded protobuf field value.
        #[derive(Clone, Debug, PartialEq)]
        pub(super) enum Field {
            Varint(u64),
            Fixed64(u64),
            Bytes(Vec<u8>),
            Fixed32(u32),
        }

        fn varint(bytes: &[u8], cursor: &mut usize) -> u64 {
            let mut value = 0_u64;
            let mut shift = 0;
            loop {
                let byte = bytes[*cursor];
                *cursor += 1;
                value |= u64::from(byte & 0x7f) << shift;
                if byte & 0x80 == 0 {
                    return value;
                }
                shift += 7;
            }
        }

        /// Decode one message into its fields, keyed by field number.
        pub(super) fn decode(bytes: &[u8]) -> BTreeMap<u64, Field> {
            let mut fields = BTreeMap::new();
            let mut cursor = 0;
            while cursor < bytes.len() {
                let key = varint(bytes, &mut cursor);
                let value = match key & 7 {
                    0 => Field::Varint(varint(bytes, &mut cursor)),
                    1 => {
                        let raw = u64::from_le_bytes(bytes[cursor..cursor + 8].try_into().unwrap());
                        cursor += 8;
                        Field::Fixed64(raw)
                    }
                    2 => {
                        let len = varint(bytes, &mut cursor) as usize;
                        let raw = bytes[cursor..cursor + len].to_vec();
                        cursor += len;
                        Field::Bytes(raw)
                    }
                    5 => {
                        let raw = u32::from_le_bytes(bytes[cursor..cursor + 4].try_into().unwrap());
                        cursor += 4;
                        Field::Fixed32(raw)
                    }
                    wire_type => panic!("unexpected wire type {wire_type}"),
                };
                fields.insert(key >> 3, value);
            }
            fields
        }

        /// Encode a varint field for fake server frames.
        pub(super) fn encode_varint_field(field: u64, value: u64) -> Vec<u8> {
            let mut out = vec![(field << 3) as u8];
            let mut value = value;
            loop {
                let byte = (value & 0x7f) as u8;
                value >>= 7;
                if value == 0 {
                    out.push(byte);
                    return out;
                }
                out.push(byte | 0x80);
            }
        }

        /// Encode a short string field for fake server frames.
        pub(super) fn encode_string_field(field: u64, value: &str) -> Vec<u8> {
            let mut out = vec![((field << 3) | 2) as u8, value.len() as u8];
            out.extend_from_slice(value.as_bytes());
            out
        }

        pub(super) fn string(fields: &BTreeMap<u64, Field>, field: u64) -> Option<String> {
            match fields.get(&field) {
                Some(Field::Bytes(raw)) => Some(String::from_utf8(raw.clone()).unwrap()),
                None => None,
                other => panic!("field {field} is not a string: {other:?}"),
            }
        }

        pub(super) fn int(fields: &BTreeMap<u64, Field>, field: u64) -> Option<u64> {
            match fields.get(&field) {
                Some(Field::Varint(value)) => Some(*value),
                None => None,
                other => panic!("field {field} is not a varint: {other:?}"),
            }
        }

        pub(super) fn double(fields: &BTreeMap<u64, Field>, field: u64) -> Option<f64> {
            match fields.get(&field) {
                Some(Field::Fixed64(raw)) => Some(f64::from_bits(*raw)),
                None => None,
                other => panic!("field {field} is not a double: {other:?}"),
            }
        }
    }

    /// Serve one TWS API handshake on a loopback socket, then forward every
    /// frame the client sends (message id prefix included) until it hangs up.
    async fn fake_tws() -> (SocketAddr, tokio::sync::mpsc::UnboundedReceiver<Vec<u8>>) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        async fn read_frame(socket: &mut tokio::net::TcpStream) -> Option<Vec<u8>> {
            let len = socket.read_u32().await.ok()? as usize;
            let mut frame = vec![0; len];
            socket.read_exact(&mut frame).await.ok()?;
            Some(frame)
        }
        async fn write_frame(socket: &mut tokio::net::TcpStream, frame: &[u8]) {
            socket
                .write_u32(frame.len() as u32)
                .await
                .expect("write length");
            socket.write_all(frame).await.expect("write body");
        }
        fn protobuf_message(message_id: i32, body: &[u8]) -> Vec<u8> {
            // Protobuf frames carry the message id offset by 200.
            let mut frame = (message_id + 200).to_be_bytes().to_vec();
            frame.extend_from_slice(body);
            frame
        }

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind fake TWS");
        let address = listener.local_addr().expect("fake TWS address");
        let (frames, received) = tokio::sync::mpsc::unbounded_channel();
        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("accept client");
            let mut prefix = [0_u8; 4];
            socket.read_exact(&mut prefix).await.expect("API prefix");
            assert_eq!(&prefix, b"API\0");
            read_frame(&mut socket).await.expect("version frame");
            write_frame(
                &mut socket,
                b"225\x0020260927 09:30:00 America/New_York\x00",
            )
            .await;
            let start_api = read_frame(&mut socket).await.expect("start API frame");
            assert_eq!(
                i32::from_be_bytes(start_api[..4].try_into().unwrap()),
                71 + 200
            );
            write_frame(
                &mut socket,
                &protobuf_message(9, &wire::encode_varint_field(1, 100)),
            )
            .await;
            write_frame(
                &mut socket,
                &protobuf_message(15, &wire::encode_string_field(1, "U1234567")),
            )
            .await;
            while let Some(frame) = read_frame(&mut socket).await {
                if frames.send(frame).is_err() {
                    break;
                }
            }
        });
        (address, received)
    }

    /// Serve one TWS API handshake on a loopback socket and return every
    /// protobuf `PlaceOrder` payload the client sends afterwards.
    async fn fake_tws_place_order_frames(
        expected_orders: usize,
    ) -> (SocketAddr, tokio::task::JoinHandle<Vec<Vec<u8>>>) {
        let (address, mut frames) = fake_tws().await;
        let server = tokio::spawn(async move {
            let mut orders = Vec::new();
            while orders.len() < expected_orders {
                let frame = frames.recv().await.expect("client frame");
                if i32::from_be_bytes(frame[..4].try_into().unwrap()) == 3 + 200 {
                    orders.push(frame[4..].to_vec());
                }
            }
            orders
        });
        (address, server)
    }

    #[tokio::test]
    async fn bracket_wire_frames_match_the_tws_encoding() {
        let (address, server) = fake_tws_place_order_frames(3).await;
        let client = Client::connect(&address.to_string(), 17_654)
            .await
            .expect("connect to fake TWS");
        let request = bracket_request();
        let plan = build_bracket(&request, "U1234567", 100, &aligned_bracket(&request));
        let contract = Contract::stock("SPY").build();
        let _subscriptions = place_bracket_orders(&client, &contract, &plan)
            .await
            .expect("bracket sent");
        let frames = tokio::time::timeout(Duration::from_secs(5), server)
            .await
            .expect("fake TWS finished")
            .expect("fake TWS task");

        // PlaceOrderRequest: 1 order_id, 3 order. Order.proto field numbers:
        // 2 orderId, 4 parentId, 5 action, 6 totalQuantity, 8 orderType,
        // 9 lmtPrice, 10 auxPrice, 11 tif, 12 account, 27 ocaGroup,
        // 28 orderRef, 30 ocaType, 66 transmit.
        let expected = [
            (
                100,
                None,
                "BUY",
                "LMT",
                Some(650.25),
                None,
                "GTC",
                CUSTOMER_ORDER_ID,
                None,
                None,
                None,
            ),
            (
                101,
                Some(100),
                "SELL",
                "LMT",
                Some(700.0),
                None,
                "GTC",
                TAKE_PROFIT_ORDER_ID,
                Some(format!("oca-{CUSTOMER_ORDER_ID}")),
                Some(2),
                None,
            ),
            (
                102,
                Some(100),
                "SELL",
                "STP",
                None,
                Some(620.0),
                "GTC",
                STOP_LOSS_ORDER_ID,
                Some(format!("oca-{CUSTOMER_ORDER_ID}")),
                Some(2),
                Some(1),
            ),
        ];
        assert_eq!(frames.len(), expected.len());
        for (
            frame,
            (
                order_id,
                parent_id,
                action,
                order_type,
                limit,
                aux,
                tif,
                order_ref,
                oca_group,
                oca_type,
                transmit,
            ),
        ) in frames.iter().zip(expected)
        {
            let request = wire::decode(frame);
            assert_eq!(wire::int(&request, 1), Some(order_id));
            let Some(wire::Field::Bytes(order)) = request.get(&3) else {
                panic!("PlaceOrderRequest carries no order");
            };
            let order = wire::decode(order);
            assert_eq!(wire::int(&order, 4), parent_id, "parentId of {order_id}");
            assert_eq!(wire::string(&order, 5).as_deref(), Some(action));
            assert_eq!(wire::string(&order, 6).as_deref(), Some("10"));
            assert_eq!(wire::string(&order, 8).as_deref(), Some(order_type));
            assert_eq!(wire::double(&order, 9), limit, "lmtPrice of {order_id}");
            assert_eq!(wire::double(&order, 10), aux, "auxPrice of {order_id}");
            assert_eq!(wire::string(&order, 11).as_deref(), Some(tif));
            assert_eq!(wire::string(&order, 12).as_deref(), Some("U1234567"));
            assert_eq!(
                wire::string(&order, 27),
                oca_group,
                "ocaGroup of {order_id}"
            );
            assert_eq!(wire::string(&order, 28).as_deref(), Some(order_ref));
            assert_eq!(wire::int(&order, 30), oca_type, "ocaType of {order_id}");
            // The official clients (EClientUtils.java / client_utils.py) set
            // `transmit` only when true, so TWS reads an absent field as false.
            // Only the last child carries it and releases the whole bracket.
            assert_eq!(wire::int(&order, 66), transmit, "transmit of {order_id}");
        }
    }

    #[tokio::test]
    async fn advanced_order_wire_frames_match_the_tws_encoding() {
        let mut trailing = typed_request(OrderType::TrailingStopLimit);
        trailing.side = "sell".to_string();
        trailing.outside_rth = true;
        let mut adaptive = typed_request(OrderType::Limit);
        adaptive.time_in_force = "day".to_string();
        adaptive.outside_rth = true;
        adaptive.algo = Some(OrderAlgo {
            strategy: AlgoStrategy::Adaptive,
            priority: crate::contract::AdaptivePriority::Normal,
        });
        let on_open = typed_request(OrderType::LimitOnOpen);
        let on_close = typed_request(OrderType::MarketOnClose);
        let requests = [trailing, adaptive, on_open, on_close];
        for request in &requests {
            validate_order_request(request).expect("valid advanced request");
        }
        let (address, server) = fake_tws_place_order_frames(requests.len()).await;
        let client = Client::connect(&address.to_string(), 17_654)
            .await
            .expect("connect to fake TWS");
        let contract = Contract::stock("SPY").build();
        let mut subscriptions = Vec::new();
        for (index, request) in requests.iter().enumerate() {
            let order = build_order(request, "U1234567", CUSTOMER_ORDER_ID);
            subscriptions.push(
                client
                    .place_order(200 + index as i32, &contract, &order)
                    .await
                    .expect("order sent"),
            );
        }
        let frames = tokio::time::timeout(Duration::from_secs(5), server)
            .await
            .expect("fake TWS finished")
            .expect("fake TWS task");

        // Order.proto field numbers beyond the bracket test's: 19
        // outsideRth, 22 trailingPercent, 61 algoStrategy, 62 algoParams
        // (map entries of 1 key, 2 value), 99 lmtPriceOffset.
        let orders: Vec<_> = frames
            .iter()
            .map(|frame| {
                let request = wire::decode(frame);
                let Some(wire::Field::Bytes(order)) = request.get(&3) else {
                    panic!("PlaceOrderRequest carries no order");
                };
                wire::decode(order)
            })
            .collect();
        let [trailing, adaptive, on_open, on_close] = orders.as_slice() else {
            panic!("four orders");
        };

        assert_eq!(wire::string(trailing, 5).as_deref(), Some("SELL"));
        assert_eq!(wire::string(trailing, 8).as_deref(), Some("TRAIL LIMIT"));
        assert_eq!(wire::double(trailing, 22), Some(2.5));
        assert_eq!(wire::double(trailing, 99), Some(0.25));
        assert_eq!(wire::double(trailing, 9), None);
        assert_eq!(wire::int(trailing, 19), Some(1));

        assert_eq!(wire::string(adaptive, 8).as_deref(), Some("LMT"));
        assert_eq!(wire::string(adaptive, 11).as_deref(), Some("DAY"));
        assert_eq!(wire::int(adaptive, 19), Some(1));
        assert_eq!(wire::string(adaptive, 61).as_deref(), Some("Adaptive"));
        let Some(wire::Field::Bytes(entry)) = adaptive.get(&62) else {
            panic!("adaptive order carries no algo parameters");
        };
        let entry = wire::decode(entry);
        assert_eq!(wire::string(&entry, 1).as_deref(), Some("adaptivePriority"));
        assert_eq!(wire::string(&entry, 2).as_deref(), Some("Normal"));

        assert_eq!(wire::string(on_open, 8).as_deref(), Some("LMT"));
        assert_eq!(wire::string(on_open, 11).as_deref(), Some("OPG"));
        assert_eq!(wire::double(on_open, 9), Some(650.25));
        assert_eq!(wire::int(on_open, 19), None);

        assert_eq!(wire::string(on_close, 8).as_deref(), Some("MOC"));
        assert_eq!(wire::string(on_close, 11).as_deref(), Some("DAY"));
        assert_eq!(wire::string(on_close, 61), None);
        drop(subscriptions);
    }

    /// Protobuf ids (TWS id + 200) of the market-data frames the tests watch.
    const REQUEST_MARKET_DATA_FRAME: i32 = 1 + 200;
    const CANCEL_MARKET_DATA_FRAME: i32 = 2 + 200;
    const MARKET_DATA_TYPE_FRAME: i32 = 59 + 200;

    /// Next market-data request, cancel or mode switch the client sent.
    async fn next_market_data_frame(
        frames: &mut tokio::sync::mpsc::UnboundedReceiver<Vec<u8>>,
    ) -> (i32, Vec<u8>) {
        loop {
            let frame = frames.recv().await.expect("client frame");
            let id = i32::from_be_bytes(frame[..4].try_into().unwrap());
            if [
                REQUEST_MARKET_DATA_FRAME,
                CANCEL_MARKET_DATA_FRAME,
                MARKET_DATA_TYPE_FRAME,
            ]
            .contains(&id)
            {
                return (id, frame[4..].to_vec());
            }
        }
    }

    /// Mode a `reqMarketDataType` frame switches to (TWS numbering).
    fn switched_mode(body: &[u8]) -> Option<u64> {
        wire::int(&wire::decode(body), 1)
    }

    fn mentions(body: &[u8], symbol: &str) -> bool {
        body.windows(symbol.len())
            .any(|window| window == symbol.as_bytes())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_market_data_requests_each_send_under_their_own_mode() {
        let (address, mut frames) = fake_tws().await;
        let client = Arc::new(
            Client::connect(&address.to_string(), 17_655)
                .await
                .expect("connect to fake TWS"),
        );
        let market_data_mode = Arc::new(Mutex::new(()));
        let requests = 8;
        let tasks = (0..requests)
            .map(|index| {
                let client = Arc::clone(&client);
                let market_data_mode = Arc::clone(&market_data_mode);
                tokio::spawn(async move {
                    // A stock quote line (delayed-frozen) and option quotes
                    // (delayed) racing on one connection.
                    let (mode, symbol) = if index % 2 == 0 {
                        (MarketDataType::DelayedFrozen, "EVENS")
                    } else {
                        (MarketDataType::Delayed, "ODDS")
                    };
                    let contract = Contract::stock(symbol).build();
                    let held = lock_market_data_mode(&market_data_mode)
                        .await
                        .expect("mode lock");
                    send_market_data_request(
                        &client,
                        &held,
                        mode,
                        "test quote",
                        client.market_data(&contract).subscribe(),
                    )
                    .await
                    .expect("request sent")
                })
            })
            .collect::<Vec<_>>();
        let mut subscriptions = Vec::new();
        for task in tasks {
            subscriptions.push(task.await.expect("request task"));
        }

        // Every request directly follows its own mode switch: no other
        // request's switch can land between them.
        let mut mode = None;
        let mut sent = 0;
        while sent < requests {
            let (id, body) =
                tokio::time::timeout(Duration::from_secs(5), next_market_data_frame(&mut frames))
                    .await
                    .expect("market-data frame");
            match id {
                MARKET_DATA_TYPE_FRAME => {
                    assert_eq!(mode, None, "two mode switches without a request");
                    mode = switched_mode(&body);
                }
                REQUEST_MARKET_DATA_FRAME => {
                    let expected = if mentions(&body, "EVENS") {
                        MarketDataType::DelayedFrozen as u64
                    } else {
                        assert!(mentions(&body, "ODDS"), "unexpected contract");
                        MarketDataType::Delayed as u64
                    };
                    assert_eq!(mode.take(), Some(expected), "request under another mode");
                    sent += 1;
                }
                _ => {}
            }
        }
        drop(subscriptions);
    }

    #[tokio::test]
    async fn an_idle_resubscribe_waits_for_the_mode_before_cancelling_its_line() {
        let (address, mut frames) = fake_tws().await;
        let client = Arc::new(
            Client::connect(&address.to_string(), 17_656)
                .await
                .expect("connect to fake TWS"),
        );
        let market_data_mode = Arc::new(Mutex::new(()));
        let contract = Contract::stock("SPY").build();
        let subscription = {
            let held = lock_market_data_mode(&market_data_mode)
                .await
                .expect("mode lock");
            subscribe_stock_quote_line(&client, &contract, &held)
                .await
                .expect("first line")
        };
        for expected in [MARKET_DATA_TYPE_FRAME, REQUEST_MARKET_DATA_FRAME] {
            assert_eq!(next_market_data_frame(&mut frames).await.0, expected);
        }
        let mut line = IbkrStockQuoteLine {
            client,
            contract,
            market_data_mode: Arc::clone(&market_data_mode),
            subscription,
        };

        // Another request is between its mode switch and its send.
        let busy = market_data_mode.lock().await;
        let resubscribe = tokio::spawn(async move { line.resubscribe().await.map(|()| line) });
        let early = tokio::time::timeout(
            Duration::from_millis(300),
            next_market_data_frame(&mut frames),
        )
        .await;
        assert!(
            early.is_err(),
            "the old line was touched while another request held the mode: {early:?}"
        );

        drop(busy);
        for expected in [
            CANCEL_MARKET_DATA_FRAME,
            MARKET_DATA_TYPE_FRAME,
            REQUEST_MARKET_DATA_FRAME,
        ] {
            let (id, body) =
                tokio::time::timeout(Duration::from_secs(5), next_market_data_frame(&mut frames))
                    .await
                    .expect("resubscribe frame");
            assert_eq!(id, expected);
            if id == MARKET_DATA_TYPE_FRAME {
                assert_eq!(
                    switched_mode(&body),
                    Some(MarketDataType::DelayedFrozen as u64)
                );
            }
        }
        resubscribe
            .await
            .expect("resubscribe task")
            .expect("resubscribed");
    }
}
