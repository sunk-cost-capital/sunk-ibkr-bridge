//! Versioned JSON contract exposed to the browser.

use serde::{Deserialize, Serialize};

/// Current browser bridge JSON protocol version.
pub const PROTOCOL_VERSION: u8 = 3;

/// Request to discover the managed accounts exposed by the local Gateway.
#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DetectionRequest {
    /// Browser bridge protocol version.
    pub version: u8,
}

/// Normalized browser response for one usable local Gateway session.
#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DetectionResponse {
    /// Browser bridge protocol version.
    pub version: u8,
    /// Stable connected status.
    pub status: String,
    /// Human-readable status detail.
    pub message: String,
    /// Managed accounts in the order IBKR returned them.
    pub accounts: Vec<String>,
}

/// Request to inspect the authenticated IBKR session and select an account.
#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ProbeRequest {
    /// Browser bridge protocol version.
    pub version: u8,
    /// Requested IBKR account, or none when exactly one account must be selected.
    pub account_ref: Option<String>,
}

/// Normalized IBKR session probe result.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProbeResponse {
    /// Browser bridge protocol version.
    pub version: u8,
    /// Provider identifier, always `ibkr`.
    pub provider: &'static str,
    /// Whether the brokerage session is usable.
    pub ok: bool,
    /// Stable normalized session status.
    pub status: String,
    /// Human-readable status detail.
    pub message: String,
    /// Selected IBKR account when available.
    pub account_ref: Option<String>,
}

/// Connected state reported after the TWS API returns managed accounts.
#[derive(Debug, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum GatewayDetection {
    /// IB Gateway is reachable and its TWS API session is usable.
    Connected {
        /// Human-readable status detail.
        message: String,
        /// Managed accounts the session exposes, in the order IBKR returned them.
        ///
        /// Detection already loads these to prove the session is usable. Passing
        /// them on lets a caller resolve account selection up front: one account
        /// needs no input, several must be chosen between before a probe can
        /// succeed.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        accounts: Vec<String>,
    },
}

/// Request for bounded IBKR historical candles.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CandleRequest {
    /// Browser bridge protocol version.
    pub version: u8,
    /// Equity, ETF, or listed-option symbol.
    pub symbol: String,
    /// Known IBKR contract identifier, when available.
    pub conid: Option<i64>,
    /// Supported normalized chart timeframe.
    pub timeframe: String,
    /// Optional inclusive lower timestamp in milliseconds.
    pub from: Option<i64>,
    /// Optional exclusive upper timestamp in milliseconds.
    pub to: Option<i64>,
    /// Optional maximum number of returned candles.
    pub limit: Option<usize>,
}

/// Canonical historical candle returned by the local IBKR client.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Candle {
    /// Candle opening timestamp in milliseconds since Unix epoch.
    pub timestamp: i64,
    /// Opening price.
    pub open: f64,
    /// Highest price.
    pub high: f64,
    /// Lowest price.
    pub low: f64,
    /// Closing price.
    pub close: f64,
    /// Traded volume.
    pub volume: f64,
    /// Whether the provider reports the candle as closed.
    pub confirmed: bool,
    /// Whether a stream candle comes from subscription history instead of a live update.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub is_snapshot: Option<bool>,
}

/// Versioned collection of canonical IBKR candles.
#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CandleResponse {
    /// Browser bridge protocol version.
    pub version: u8,
    /// Canonical candles ordered by timestamp.
    pub candles: Vec<Candle>,
}

/// Request for a bounded near-the-money IBKR option chain.
#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct OptionChainRequest {
    /// Browser bridge protocol version.
    pub version: u8,
    /// Underlying equity or ETF ticker.
    pub symbol: String,
    /// Maximum nearby strikes to fetch for each option side.
    pub max_strikes_per_side: usize,
}

/// One price tier from IBKR contract increment rules.
#[derive(Debug, Clone, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PriceIncrementRule {
    /// Inclusive lower premium for this tier.
    pub lower_edge: f64,
    /// Valid price increment in this tier.
    pub increment: f64,
}

/// Canonical listed-option side exposed by the local IBKR runtime.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum OptionContractRight {
    /// Call option.
    Call,
    /// Put option.
    Put,
}

/// Normalized listed-option definition and quote.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OptionContract {
    /// IBKR contract identifier.
    pub conid: i64,
    /// Provider-local contract symbol.
    pub local_symbol: String,
    /// Normalized option side, `call` or `put`.
    pub right: OptionContractRight,
    /// Strike price.
    pub strike: f64,
    /// ISO expiration date.
    pub expiry: String,
    /// Underlying units represented by one contract.
    pub multiplier: f64,
    /// Best bid premium.
    pub bid: Option<f64>,
    /// Best ask premium.
    pub ask: Option<f64>,
    /// Best bid size in contracts.
    pub bid_size: Option<f64>,
    /// Best ask size in contracts.
    pub ask_size: Option<f64>,
    /// Current session volume.
    pub volume: Option<f64>,
    /// Current open interest.
    pub open_interest: Option<f64>,
    /// Signed option delta.
    pub delta: Option<f64>,
    /// Current mark premium.
    pub mark: Option<f64>,
    /// Provider-defined valid price increments.
    pub price_increment_rules: Vec<PriceIncrementRule>,
    /// Quote update time in milliseconds, or zero when unavailable.
    pub quote_timestamp_ms: i64,
    /// Whether market data is delayed.
    pub delayed: bool,
    /// Whether the account can trade the contract.
    pub can_trade: bool,
}

/// Normalized bounded IBKR option chain.
#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OptionChainResponse {
    /// Browser bridge protocol version.
    pub version: u8,
    /// Canonical underlying symbol.
    pub underlying_symbol: String,
    /// Underlying price used to select nearby strikes.
    pub underlying_price: f64,
    /// Normalized option definitions and quotes.
    ///
    /// Empty when IBKR reports no SMART-routable option chain for the equity.
    pub contracts: Vec<OptionContract>,
}

/// Request for a fresh quote of one listed option the account can trade.
///
/// The web app prices a capped option order (a position close carrying a
/// slippage cap) from this quote, and reads an option stop's tick rules from
/// it. A held option is often far from the money or on a later expiry, so it
/// is looked up by contract id rather than through the near-the-money chain.
#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct OptionQuoteRequest {
    /// Browser bridge protocol version.
    pub version: u8,
    /// IBKR contract identifier of the option.
    pub conid: i64,
}

/// One listed option's definition and a quote taken for this request.
///
/// The quote is a new market-data read, not a cached one, and has the same
/// fields and tradability rules as an option-chain contract: `canTrade` is
/// false, and `priceIncrementRules` empty, unless IBKR returned a live,
/// fully entitled quote with a positive bid and ask and the contract's tick
/// rules.
#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct OptionQuoteResponse {
    /// Browser bridge protocol version.
    pub version: u8,
    /// IBKR symbol of the option's underlying, whose option session applies.
    pub underlying_symbol: String,
    /// The option and its quote.
    pub contract: OptionContract,
}

/// Gateway input for one streaming IBKR stock quote subscription.
///
/// Built from the `stock_quote` WebSocket stream request or the desktop
/// runtime's subscription parameters; it is not a wire type itself.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StockQuoteRequest {
    /// Equity or ETF ticker.
    pub symbol: String,
    /// Known IBKR contract identifier, when available.
    pub conid: Option<i64>,
}

/// IBKR market-data mode that produced a stock quote.
///
/// The bridge requests IBKR's delayed-frozen mode, so an account without a
/// live subscription still receives delayed data and a closed market still
/// reports its last frozen values. Every quote carries the mode IBKR reports
/// so pricing and risk consumers can refuse anything that is not `live`.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum QuoteDataType {
    /// Real-time streaming data from a live market-data subscription.
    Live,
    /// Last real-time values retained after the market closed.
    Frozen,
    /// Data delayed by the exchange's delay (usually 15 to 20 minutes).
    Delayed,
    /// Last delayed values retained after the market closed.
    DelayedFrozen,
}

/// Normalized top-of-book quote for one IBKR stock contract.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StockQuote {
    /// Canonical IBKR symbol.
    pub symbol: String,
    /// IBKR contract identifier the quote belongs to.
    pub conid: i64,
    /// Currency the prices are quoted in.
    pub currency: String,
    /// Best bid, or none while the book has no bid.
    pub bid: Option<f64>,
    /// Best ask, or none while the book has no ask.
    pub ask: Option<f64>,
    /// Last traded price, when IBKR has reported one.
    pub last: Option<f64>,
    /// Previous session's closing price, when IBKR has reported one.
    pub previous_close: Option<f64>,
    /// Market-data mode IBKR reported for this subscription.
    pub data_type: QuoteDataType,
    /// Local wall-clock time in milliseconds of the latest price change.
    ///
    /// A market-data mode change alone does not refresh it, and frozen
    /// quotes are capped at the last trade time when IBKR reports one.
    ///
    /// The bridge and its browser run on the same machine, so a consumer's
    /// `Date.now() - updatedAtMs` is the quote's age.
    pub updated_at_ms: i64,
    /// Exchange time of the last trade in milliseconds, when IBKR reported it.
    pub last_trade_at_ms: Option<i64>,
    /// The contract's price increment tiers (IBKR market rules), the grid
    /// order validation checks limit prices against.
    ///
    /// Always serialized by a bridge that reports them, as an empty list
    /// until a lookup succeeds or when IBKR reports none for the contract.
    /// A runtime that never reports them omits the key, which is how clients
    /// tell the two apart; deserializing such a quote yields an empty list.
    #[serde(default)]
    pub price_increment_rules: Vec<PriceIncrementRule>,
}

impl StockQuote {
    /// Returns the quote's age in milliseconds at `now_ms`, never negative.
    ///
    /// # Arguments
    ///
    /// * `now_ms` - Current wall-clock time in milliseconds since the Unix epoch.
    ///
    /// # Returns
    ///
    /// Milliseconds since the latest price change.
    pub fn age_ms(&self, now_ms: i64) -> i64 {
        now_ms.saturating_sub(self.updated_at_ms).max(0)
    }
}

/// Supported IBKR instrument families for order submission.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum InstrumentKind {
    /// Equity or ETF identified by a symbol or conid.
    Stock,
    /// Listed option identified by an explicit conid.
    Option,
}

/// Validated IBKR order request shared by HTTP and native adapters.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct OrderRequest {
    /// Browser bridge protocol version.
    pub version: u8,
    /// Optional application connection identifier.
    pub connection_id: Option<String>,
    /// Provider identifier, which must be `ibkr`.
    pub provider: String,
    /// Stock symbol/conid or listed-option conid.
    pub instrument: String,
    /// Instrument family controlling symbol resolution and sizing.
    pub instrument_kind: InstrumentKind,
    /// Normalized order side, `buy` or `sell`.
    pub side: String,
    /// Requested USD budget for cash-sized stock orders.
    pub size_usd: f64,
    /// Explicit provider quantity; fractional for stocks and whole for options.
    pub quantity: Option<f64>,
    /// Normalized order type, one of the wire names of [`OrderType`].
    ///
    /// Bridge releases before `0.5.0` accept only `market`, `limit` and
    /// `stop_market`, and refuse the rest before Gateway access with
    /// `orderType must be market, limit, or stop_market`.
    pub order_type: String,
    /// Limit price, required by every limit-priced [`OrderType`] and refused
    /// by the others.
    pub limit_price: Option<f64>,
    /// Trigger price, required by stop, stop-limit, market-if-touched and
    /// limit-if-touched orders and refused by the others.
    pub stop_price: Option<f64>,
    /// Normalized time in force: `day`, `gtc`, `ioc` or `fok`.
    ///
    /// Opening and closing auction orders require `day`; the Gateway sends
    /// on-open orders with IBKR's `OPG`. `day` needs bridge `0.5.0`.
    pub time_in_force: String,
    /// Slippage bound the caller already applied, in basis points.
    ///
    /// The Gateway never prices an order from it: a caller that wants a
    /// protected market order converts it to a marketable limit itself (the
    /// web app does so from its live IBKR quote) and sends the limit; the
    /// web app sends stops without a cap. A type IBKR fills at market
    /// ([`OrderType::fills_at_market`]) is refused with a cap, because
    /// nothing would bound it.
    pub market_slippage_bps: Option<u32>,
    /// Trailing distance in percent of the price, required by
    /// `trailing_stop` and `trailing_stop_limit` and refused by the others.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trailing_percent: Option<f64>,
    /// Distance of a `trailing_stop_limit`'s limit from its trailing stop,
    /// in price units (IBKR `lmtPriceOffset`). Required by that type only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit_offset: Option<f64>,
    /// Let the order work, and a stop trigger, outside regular trading hours
    /// (IBKR `outsideRth`). Refused for auction orders, which execute only
    /// in their auction. Absent means `false`.
    #[serde(default, skip_serializing_if = "is_false")]
    pub outside_rth: bool,
    /// IBKR algo to route the order through, or none for a direct order.
    ///
    /// Only market and limit orders with `day` time in force accept one,
    /// because IBKR algos do not support GTC.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub algo: Option<OrderAlgo>,
    /// Place the order as a linked exit of the account's position in this contract.
    ///
    /// IBKR has no reduce-only attribute for stocks or options, so the
    /// Gateway enforces it: the order must close the held position, its
    /// quantity is clamped to the position, and it joins one IBKR OCA group
    /// (type 2, reduce with block) with the position's other exits, so a fill
    /// of any one reduces the rest and together they never close more than
    /// the position. Requires an explicit `quantity` and no `protection`,
    /// and a market, limit, stop or stop-limit order
    /// ([`OrderType::links_as_exit`]).
    /// Bridge releases before `0.4.0` reject `true` before Gateway access.
    pub reduce_only: bool,
    /// Unsupported post-only flag, retained for strict rejection.
    pub post_only: bool,
    /// Required IBKR account identifier.
    pub account_ref: Option<String>,
    /// Caller-generated `sunk-<uuid>` order reference.
    ///
    /// The caller persists this identifier before submission so a lost
    /// acknowledgement can later be reconciled by exact IBKR `order_ref` match.
    pub customer_order_id: String,
    /// Take-profit and stop-loss children to attach as an IBKR bracket.
    ///
    /// Absent for a plain order. Present, the parent is sent with
    /// `transmit = false` and each child closes the parent's exact quantity.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub protection: Option<OrderProtection>,
}

fn is_false(value: &bool) -> bool {
    !*value
}

/// IBKR order types the Gateway can place, by their wire names.
///
/// Each maps to one TWS order type. Every type other than `market` and
/// `limit` needs an explicit quantity, because IBKR sizes only market and
/// limit stock orders by cash. Auction types are stocks only.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderType {
    /// `MKT`.
    Market,
    /// `LMT` at `limitPrice`.
    Limit,
    /// `STP`: a market order once `stopPrice` trades.
    StopMarket,
    /// `STP LMT`: a limit order at `limitPrice` once `stopPrice` trades.
    StopLimit,
    /// `TRAIL`: a stop `trailingPercent` behind the best price since entry.
    TrailingStop,
    /// `TRAIL LIMIT`: a trailing stop that submits a limit `limitOffset`
    /// beyond the stop when it triggers.
    TrailingStopLimit,
    /// `MIT`: a market order once `stopPrice` trades, on the profit side.
    MarketIfTouched,
    /// `LIT`: a limit order at `limitPrice` once `stopPrice` trades.
    LimitIfTouched,
    /// `MKT` with time in force `OPG`: executes in the opening auction.
    MarketOnOpen,
    /// `LMT` at `limitPrice` with time in force `OPG`: opening auction.
    LimitOnOpen,
    /// `MOC`: executes in the closing auction.
    MarketOnClose,
    /// `LOC` at `limitPrice`: closing auction.
    LimitOnClose,
}

impl OrderType {
    /// Every order type, in the order the error message lists them.
    pub const ALL: [OrderType; 12] = [
        OrderType::Market,
        OrderType::Limit,
        OrderType::StopMarket,
        OrderType::StopLimit,
        OrderType::TrailingStop,
        OrderType::TrailingStopLimit,
        OrderType::MarketIfTouched,
        OrderType::LimitIfTouched,
        OrderType::MarketOnOpen,
        OrderType::LimitOnOpen,
        OrderType::MarketOnClose,
        OrderType::LimitOnClose,
    ];

    /// Parses a wire name.
    ///
    /// # Arguments
    ///
    /// * `value` - Wire name such as `stop_limit`.
    ///
    /// # Returns
    ///
    /// The order type, or none for an unknown name.
    pub fn parse(value: &str) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|order_type| order_type.wire_name() == value)
    }

    /// Wire name used in [`OrderRequest::order_type`].
    pub fn wire_name(self) -> &'static str {
        match self {
            OrderType::Market => "market",
            OrderType::Limit => "limit",
            OrderType::StopMarket => "stop_market",
            OrderType::StopLimit => "stop_limit",
            OrderType::TrailingStop => "trailing_stop",
            OrderType::TrailingStopLimit => "trailing_stop_limit",
            OrderType::MarketIfTouched => "market_if_touched",
            OrderType::LimitIfTouched => "limit_if_touched",
            OrderType::MarketOnOpen => "market_on_open",
            OrderType::LimitOnOpen => "limit_on_open",
            OrderType::MarketOnClose => "market_on_close",
            OrderType::LimitOnClose => "limit_on_close",
        }
    }

    /// TWS API order type sent to IBKR.
    pub fn tws_order_type(self) -> &'static str {
        match self {
            OrderType::Market | OrderType::MarketOnOpen => "MKT",
            OrderType::Limit | OrderType::LimitOnOpen => "LMT",
            OrderType::StopMarket => "STP",
            OrderType::StopLimit => "STP LMT",
            OrderType::TrailingStop => "TRAIL",
            OrderType::TrailingStopLimit => "TRAIL LIMIT",
            OrderType::MarketIfTouched => "MIT",
            OrderType::LimitIfTouched => "LIT",
            OrderType::MarketOnClose => "MOC",
            OrderType::LimitOnClose => "LOC",
        }
    }

    /// Whether the type carries a limit price.
    pub fn needs_limit_price(self) -> bool {
        matches!(
            self,
            OrderType::Limit
                | OrderType::StopLimit
                | OrderType::LimitIfTouched
                | OrderType::LimitOnOpen
                | OrderType::LimitOnClose
        )
    }

    /// Whether the type carries a trigger price.
    pub fn needs_stop_price(self) -> bool {
        matches!(
            self,
            OrderType::StopMarket
                | OrderType::StopLimit
                | OrderType::MarketIfTouched
                | OrderType::LimitIfTouched
        )
    }

    /// Whether the type trails the market by a percentage.
    pub fn is_trailing(self) -> bool {
        matches!(self, OrderType::TrailingStop | OrderType::TrailingStopLimit)
    }

    /// Whether the type executes only in the opening or closing auction.
    pub fn is_auction(self) -> bool {
        matches!(
            self,
            OrderType::MarketOnOpen
                | OrderType::LimitOnOpen
                | OrderType::MarketOnClose
                | OrderType::LimitOnClose
        )
    }

    /// Whether the type executes in the opening auction (IBKR `OPG`).
    pub fn is_on_open(self) -> bool {
        matches!(self, OrderType::MarketOnOpen | OrderType::LimitOnOpen)
    }

    /// Whether IBKR fills the type at market once it works: a market order,
    /// a stop, a market-if-touched, a trailing stop or an auction market
    /// order. Nothing in such an order bounds its fill price, so it cannot
    /// carry a slippage cap (`marketSlippageBps`).
    pub fn fills_at_market(self) -> bool {
        matches!(
            self,
            OrderType::Market
                | OrderType::StopMarket
                | OrderType::TrailingStop
                | OrderType::MarketIfTouched
                | OrderType::MarketOnOpen
                | OrderType::MarketOnClose
        )
    }

    /// Whether IBKR can size the type by cash instead of a quantity.
    pub fn accepts_cash_quantity(self) -> bool {
        matches!(self, OrderType::Market | OrderType::Limit)
    }

    /// Whether the type can be a linked exit (`reduceOnly`).
    ///
    /// A linked exit may later be re-placed into a new OCA group, and the
    /// Gateway can reproduce only `LMT`, `STP` and `STP LMT` exactly from
    /// IBKR's open-order report. A market exit fills at once. Trailing,
    /// touched and auction exits would leave a group Sunk cannot relink, so
    /// they are refused as linked exits.
    pub fn links_as_exit(self) -> bool {
        matches!(
            self,
            OrderType::Market | OrderType::Limit | OrderType::StopMarket | OrderType::StopLimit
        )
    }
}

/// IBKR algo strategy for an order.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AlgoStrategy {
    /// IBKR's Adaptive algo: starts inside the spread and walks toward the
    /// far side at the chosen priority, through SMART routing.
    Adaptive,
}

/// How quickly the Adaptive algo works toward the far side of the spread.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AdaptivePriority {
    /// Scans only briefly before crossing.
    Urgent,
    /// IBKR's default balance of speed and price.
    Normal,
    /// Works slowly for a better average price.
    Patient,
}

impl AdaptivePriority {
    /// Value of IBKR's `adaptivePriority` algo parameter.
    pub fn tws_value(self) -> &'static str {
        match self {
            AdaptivePriority::Urgent => "Urgent",
            AdaptivePriority::Normal => "Normal",
            AdaptivePriority::Patient => "Patient",
        }
    }
}

/// IBKR algo routing requested for an order.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct OrderAlgo {
    /// Algo strategy; only Adaptive is supported.
    pub strategy: AlgoStrategy,
    /// Adaptive priority.
    pub priority: AdaptivePriority,
}

/// One take-profit or stop-loss child requested for a bracket.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ProtectionLegRequest {
    /// Absolute take-profit limit or stop-loss trigger price.
    ///
    /// The Gateway moves it onto the contract's price increment, toward the
    /// entry, before submission; the response reports the submitted price.
    pub price: f64,
    /// Caller-generated `sunk-<uuid>` reference for this child's IBKR `order_ref`.
    ///
    /// Persisted by the caller before submission, like the parent's
    /// reference, so the child can be reconciled on its own.
    pub customer_order_id: String,
}

/// Take-profit and stop-loss children attached to one opening order.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct OrderProtection {
    /// Opposite-side limit child, or none.
    #[serde(default)]
    pub take_profit: Option<ProtectionLegRequest>,
    /// Opposite-side stop child, or none.
    #[serde(default)]
    pub stop_loss: Option<ProtectionLegRequest>,
}

/// Bracket child role.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProtectionLegKind {
    /// Opposite-side `LMT` child at the take-profit price.
    TakeProfit,
    /// Opposite-side `STP` child triggered at the stop-loss price.
    StopLoss,
}

/// How IBKR answered attached protection, overall or for one child.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProtectionStatus {
    /// The child is working against a filled parent.
    Attached,
    /// The child is accepted and activates as the parent fills.
    Pending,
    /// IBKR rejected or cancelled the child, so the position may be unprotected.
    Rejected,
    /// IBKR did not report a definitive child state before the deadline.
    Unknown,
}

/// IBKR outcome for one bracket child.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProtectionLegOutcome {
    /// Child role.
    pub kind: ProtectionLegKind,
    /// Caller-generated reference sent as the child's `order_ref`.
    pub customer_order_id: String,
    /// TWS API order identifier of the child.
    pub order_id: Option<String>,
    /// Price submitted to IBKR after tick alignment.
    pub price: f64,
    /// Child outcome.
    pub status: ProtectionStatus,
}

/// IBKR outcome for the protection attached to an accepted parent order.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProtectionOutcome {
    /// Worst child outcome: `rejected` over `unknown` over `pending` over `attached`.
    pub status: ProtectionStatus,
    /// User-facing explanation, naming the children that failed.
    pub message: String,
    /// Per-child outcomes in submission order.
    pub legs: Vec<ProtectionLegOutcome>,
}

/// One existing exit that was re-placed into a shared OCA group.
///
/// IBKR cannot move a transmitted order into an OCA group, so the Gateway
/// places a copy in the group first and then cancels the original.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RelinkedOrder {
    /// TWS order identifier of the cancelled original.
    pub from_order_id: String,
    /// TWS order identifier of the copy now working in the group.
    pub to_order_id: String,
}

/// How a linked exit (`reduceOnly`) was placed beside the position's other exits.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LinkedExitOutcome {
    /// IBKR OCA group the exit belongs to (type 2, reduce with block).
    pub oca_group: String,
    /// Quantity sent to IBKR, at most the position.
    pub quantity: f64,
    /// Absolute position quantity when the exit was placed.
    pub position_quantity: f64,
    /// TWS identifiers of the position's other working exits in the group.
    pub linked_order_ids: Vec<String>,
    /// Earlier exits re-placed into the group, original to copy.
    pub relinked: Vec<RelinkedOrder>,
    /// Problems the caller must show, such as an original that filled or
    /// could not be confirmed cancelled while it was being relinked.
    pub warnings: Vec<String>,
}

/// Normalized IBKR order outcome.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OrderResponse {
    /// Browser bridge protocol version.
    pub version: u8,
    /// Provider identifier, always `ibkr`.
    pub provider: &'static str,
    /// Whether IBKR returned a complete success acknowledgement.
    pub ok: bool,
    /// Normalized provider or reconciliation status.
    pub status: String,
    /// Human-readable outcome detail.
    pub message: String,
    /// Selected IBKR account.
    pub account_ref: Option<String>,
    /// Provider order identifier for complete acknowledgements.
    pub order_id: Option<String>,
    /// Caller-generated order reference.
    ///
    /// An `indeterminate` response for a blocked account carries the earlier
    /// unresolved reference instead of the new request's reference; the new
    /// request was not submitted.
    pub customer_order_id: Option<String>,
    /// Outcome of bracket children when the parent was accepted.
    ///
    /// Omitted for plain orders and whenever the parent itself was rejected
    /// or has an unknown outcome.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub protection: Option<ProtectionOutcome>,
    /// How a linked exit joined the position's other exits.
    ///
    /// Present only for `reduceOnly` orders IBKR accepted. Older terminals
    /// never send `reduceOnly`, so they never receive this field.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exit: Option<LinkedExitOutcome>,
}

/// Request to reconcile one earlier order submission by its exact order reference.
#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ReconcileOrderRequest {
    /// Browser bridge protocol version.
    pub version: u8,
    /// Optional application connection identifier.
    pub connection_id: Option<String>,
    /// Provider identifier, which must be `ibkr`.
    pub provider: String,
    /// IBKR account that received the earlier submission.
    pub account_ref: String,
    /// Exact `sunk-<uuid>` order reference sent with the earlier submission.
    pub customer_order_id: String,
    /// Operator attestation that the order was checked manually.
    ///
    /// When true, the provider is not queried; the local runtime only drops
    /// its in-process block if that block is for this exact order reference.
    #[serde(default)]
    pub release: bool,
}

/// Where a reconciled order reference was found.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ReconcileOrderState {
    /// IBKR lists the order among the account's current open orders.
    Open,
    /// IBKR lists the order among today's completed orders.
    Completed,
    /// IBKR lists at least one execution carrying the order reference.
    Executed,
    /// No provider snapshot contained the order reference; nothing is cleared.
    NotFound,
    /// The operator released the local block without a provider match.
    Released,
}

impl ReconcileOrderState {
    /// Whether the provider proved the order reference exists.
    pub fn is_found(self) -> bool {
        matches!(self, Self::Open | Self::Completed | Self::Executed)
    }
}

/// Result of reconciling one earlier order reference against IBKR state.
#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ReconcileOrderResponse {
    /// Browser bridge protocol version.
    pub version: u8,
    /// Provider identifier, always `ibkr`.
    pub provider: String,
    /// IBKR account that was searched.
    pub account_ref: String,
    /// Exact order reference that was searched.
    pub customer_order_id: String,
    /// Where the order reference was found, if anywhere.
    pub state: ReconcileOrderState,
    /// Normalized order outcome when the provider returned the order.
    pub status: Option<String>,
    /// TWS order identifier when the provider returned one.
    pub order_id: Option<String>,
    /// Human-readable reconciliation detail.
    pub message: String,
}

/// Request for one authoritative IBKR account snapshot.
#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AccountStateRequest {
    /// Browser bridge protocol version.
    pub version: u8,
    /// Requested IBKR account, or none when exactly one account must be selected.
    pub account_ref: Option<String>,
}

/// One normalized IBKR position with its native contract identity.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AccountPosition {
    /// Account holding the position.
    pub account_ref: String,
    /// Stable IBKR contract identifier used for exact close orders.
    pub contract_id: i32,
    /// Provider symbol used for display and chart selection.
    pub symbol: String,
    /// Provider-local symbol, including option identity when available.
    pub local_symbol: String,
    /// Executable product family, or none when this UI cannot safely close it.
    pub instrument_kind: Option<InstrumentKind>,
    /// Signed provider-native position quantity.
    pub size: f64,
    /// Average per-unit entry price after applying the contract multiplier.
    pub average_price: f64,
    /// Units represented by one provider quantity.
    pub multiplier: f64,
}

/// One normalized working IBKR order from the selected local API session.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AccountOpenOrder {
    /// Account that owns the order.
    pub account_ref: String,
    /// TWS API order identifier used for cancellation.
    pub order_id: i32,
    /// Stable IBKR contract identifier.
    pub contract_id: i32,
    /// Provider symbol used for display and position matching.
    pub symbol: String,
    /// Executable product family, or none when unsupported by this UI.
    pub instrument_kind: Option<InstrumentKind>,
    /// Normalized order side.
    pub side: String,
    /// Requested provider-native quantity.
    pub quantity: f64,
    /// Limit price when present.
    pub price: Option<f64>,
    /// Stop trigger when present.
    pub trigger_price: Option<f64>,
    /// Canonical TWS order status.
    pub status: String,
    /// Bridge order type the working order is, or null for an order this
    /// bridge cannot place as reported: a TWS type it does not place (such
    /// as `REL` or a pegged order), or an algo other than Adaptive.
    ///
    /// This and the three fields after it are always present from bridge
    /// 0.5.0, so their absence identifies an older runtime. With them a
    /// client can tell an exit it can cancel and re-place exactly, with the
    /// same type, time in force, extended-hours flag and algo, from one a
    /// re-placement would silently downgrade.
    pub order_type: Option<OrderType>,
    /// Request time in force that re-places the order: `day`, `gtc`, `ioc`
    /// or `fok` (an on-open auction order, requested as `day`, reports
    /// `day`), or null for one the bridge does not place (`GTD`, `DTC`).
    pub time_in_force: Option<String>,
    /// Whether the order may trigger and fill outside regular trading hours
    /// (TWS `outsideRth`).
    pub outside_rth: bool,
    /// Adaptive algo routing of the order, or null when it has no algo. An
    /// order with any other algo has a null [`Self::order_type`].
    pub algo: Option<OrderAlgo>,
    /// IBKR OCA group linking this order with others, omitted when none.
    ///
    /// Reported only for groups with overfill protection (OCA types 1 and
    /// 2), whose orders together close at most the group's largest order.
    /// Orders sharing a group are linked: a fill of one reduces the others,
    /// and cancelling one cancels the whole group.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub oca_group: Option<String>,
    /// Bracket parent's TWS order id, omitted for an order without one.
    ///
    /// A child whose parent is still working protects a fill that has not
    /// happened yet, so it is not an exit of the current position.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_id: Option<i32>,
    /// Quantity already filled, omitted when nothing filled.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filled_quantity: Option<f64>,
}

/// Whether IB Gateway reports a working connection to IBKR's servers.
///
/// IB Gateway keeps answering from its cache after it loses that connection,
/// so callers must not treat a read as live while the link is `lost`.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ServerLink {
    /// No outage reported.
    Connected,
    /// TWS reported the link lost (`1100`) or broken (`2110`).
    Lost,
}

/// Authoritative position and working-order snapshot for one IBKR account.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AccountStateResponse {
    /// Browser bridge protocol version.
    pub version: u8,
    /// Provider identifier, always `ibkr`.
    pub provider: &'static str,
    /// Selected IBKR account.
    pub account_ref: String,
    /// Current non-flat supported and unsupported positions.
    pub positions: Vec<AccountPosition>,
    /// Current working orders visible to this local API client.
    pub orders: Vec<AccountOpenOrder>,
    /// IB Gateway's link to IBKR's servers when the snapshot was read.
    ///
    /// Added in bridge `0.4.0`; earlier releases omit it.
    pub server_link: ServerLink,
}

/// Request to cancel one order owned by the selected IBKR account.
#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CancelOrderRequest {
    /// Browser bridge protocol version.
    pub version: u8,
    /// Optional application connection identifier.
    pub connection_id: Option<String>,
    /// Provider identifier, which must be `ibkr`.
    pub provider: String,
    /// Positive TWS API order identifier.
    pub order_id: i32,
    /// Required IBKR account identifier.
    pub account_ref: Option<String>,
}

/// Direction of an IBKR order or execution.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderEventSide {
    /// The order buys, or the execution bought (`BOT`).
    Buy,
    /// The order sells, or the execution sold (`SLD`).
    Sell,
}

/// Status change of one IBKR order, as TWS reported it to this API client.
///
/// Sent when the status, filled or remaining quantity, or average fill price
/// changes; the repeated statuses TWS sends for every open-orders read are
/// not forwarded. Order details (account, reference, contract, side and type)
/// come from the order's `openOrder` message, so a status arrives only once
/// the bridge has seen that message.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct IbkrOrderStatusEvent {
    /// Account that owns the order.
    pub account_ref: String,
    /// TWS API order identifier, unique per API client.
    pub order_id: i32,
    /// IBKR permanent order identifier, unique per account; zero when IBKR
    /// has not assigned one yet.
    pub perm_id: i64,
    /// Parent order identifier for a bracket child, or none for an order
    /// without a parent.
    pub parent_id: Option<i32>,
    /// IBKR OCA group the order belongs to, or none for an order outside a
    /// group, one whose group has no overfill protection (OCA types 1 and 2
    /// only, as on account-state orders), or one whose group name is longer
    /// than 512 characters.
    ///
    /// Sent (as `null` when none) from crate 0.6.0; older bridges omit the
    /// key. Sunk's bracket children and linked exits share a group named
    /// `oca-sunk-<uuid>`, which is how the web app names the stop of an exit
    /// that has no bracket parent (a stop loss set on a position, or a
    /// bracket child re-placed into a linked group).
    pub oca_group: Option<String>,
    /// API client that placed the order.
    pub client_id: i32,
    /// Order reference (`order_ref`), such as a Sunk `sunk-<uuid>` reference.
    pub order_ref: Option<String>,
    /// IBKR contract identifier.
    pub conid: i64,
    /// Contract symbol (the underlying for options).
    pub symbol: String,
    /// Provider-local contract symbol, such as an OCC option symbol.
    pub local_symbol: Option<String>,
    /// IBKR security type, such as `STK` or `OPT`.
    pub security_type: String,
    /// Contract currency.
    pub currency: String,
    /// Order direction.
    pub side: OrderEventSide,
    /// IBKR order type, such as `LMT`, `STP` or `MKT`.
    pub order_type: String,
    /// Canonical TWS order status, such as `Submitted`, `Filled` or `Cancelled`.
    pub status: String,
    /// Quantity filled so far.
    pub filled: f64,
    /// Quantity still working.
    pub remaining: f64,
    /// Average price of the filled quantity, once anything filled.
    pub avg_fill_price: Option<f64>,
    /// Price of the most recent fill, once anything filled.
    pub last_fill_price: Option<f64>,
}

/// One IBKR execution (a full or partial fill).
///
/// Sent once when TWS reports the execution and once more when its
/// commission report arrives with the commission and realized PnL, so
/// consumers treat it as an upsert keyed by `execId`.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct IbkrExecutionEvent {
    /// Account the execution was allocated to.
    pub account_ref: String,
    /// TWS API order identifier of the executed order.
    pub order_id: i32,
    /// IBKR permanent order identifier; zero for trades from outside IBKR.
    pub perm_id: i64,
    /// Parent order identifier when the order is a bracket child; none when
    /// it has no parent or the bridge has not seen the order's details.
    pub parent_id: Option<i32>,
    /// IBKR OCA group the order belongs to, or none for an order outside a
    /// group, one whose group has no overfill protection (OCA types 1 and 2
    /// only, as on account-state orders), one whose group name is longer
    /// than 512 characters, or when the bridge has not seen the order's
    /// details.
    ///
    /// Sent (as `null` when none) from crate 0.6.0; older bridges omit the
    /// key. Sunk's bracket children and linked exits share a group named
    /// `oca-sunk-<uuid>`, which is how the web app names the stop of an exit
    /// that has no bracket parent (a stop loss set on a position, or a
    /// bracket child re-placed into a linked group).
    pub oca_group: Option<String>,
    /// API client that placed the order.
    pub client_id: i32,
    /// Order reference (`order_ref`) carried by the execution.
    pub order_ref: Option<String>,
    /// IBKR order type, when the bridge has seen the order's details.
    pub order_type: Option<String>,
    /// IBKR contract identifier.
    pub conid: i64,
    /// Contract symbol (the underlying for options).
    pub symbol: String,
    /// Provider-local contract symbol, such as an OCC option symbol.
    pub local_symbol: Option<String>,
    /// IBKR security type, such as `STK` or `OPT`.
    pub security_type: String,
    /// Contract currency.
    pub currency: String,
    /// Execution direction.
    pub side: OrderEventSide,
    /// IBKR execution identifier; each partial fill has its own.
    pub exec_id: String,
    /// IBKR execution time in Unix milliseconds, or none when the provider
    /// time names a time zone the bridge cannot resolve.
    pub executed_at_ms: Option<i64>,
    /// Quantity of this execution.
    pub quantity: f64,
    /// Price of this execution, excluding commissions.
    pub price: f64,
    /// Quantity the order has filled including this execution.
    pub cumulative_quantity: f64,
    /// Average price of the order's fills including this execution.
    pub avg_price: f64,
    /// Whether IBKR executed it as a liquidation.
    pub liquidation: bool,
    /// Commission charged, once the commission report arrived.
    pub commission: Option<f64>,
    /// Currency of the commission and realized PnL.
    pub commission_currency: Option<String>,
    /// Realized PnL of a closing execution, once the commission report
    /// arrived; none for an opening execution.
    pub realized_pnl: Option<f64>,
}

/// One typed event from the `order_events` stream.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum IbkrOrderEvent {
    /// An order's status, fill quantities or average price changed.
    Status(IbkrOrderStatusEvent),
    /// An order executed (fully or partly), or its commission arrived.
    Execution(IbkrExecutionEvent),
}

impl IbkrOrderEvent {
    /// Account the event belongs to.
    pub fn account_ref(&self) -> &str {
        match self {
            Self::Status(status) => &status.account_ref,
            Self::Execution(execution) => &execution.account_ref,
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(
    tag = "operation",
    rename_all = "snake_case",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub(crate) enum StreamRequest {
    AccountBalance {
        version: u8,
        account_ref: Option<String>,
    },
    Candles {
        version: u8,
        symbol: String,
        conid: Option<i64>,
        timeframe: String,
    },
    StockQuote {
        version: u8,
        symbol: String,
        conid: Option<i64>,
    },
    OrderEvents {
        version: u8,
        account_ref: Option<String>,
    },
}

impl StreamRequest {
    pub(crate) fn version(&self) -> u8 {
        match self {
            Self::AccountBalance { version, .. }
            | Self::Candles { version, .. }
            | Self::StockQuote { version, .. }
            | Self::OrderEvents { version, .. } => *version,
        }
    }
}

/// USD-normalized IBKR account summary for one selected managed account.
///
/// IBKR reports these values in the account's base currency. The bridge
/// converts them with the exchange rate IBKR itself publishes in the account's
/// USD ledger, so every `*_usd` field is either a converted dollar amount or
/// `None` when IBKR did not report that value. The native net-liquidation value
/// and the rate used stay alongside so a non-USD account can be shown in its
/// own currency as well.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AccountValueSummary {
    /// Net liquidation value (`NetLiquidation`) in USD.
    pub net_liquidation_usd: Option<f64>,
    /// Total cash across currencies (`TotalCashValue`) in USD.
    pub total_cash_usd: Option<f64>,
    /// Funds available for new positions (`AvailableFunds`) in USD.
    pub available_funds_usd: Option<f64>,
    /// Gross market value of all positions (`GrossPositionValue`) in USD.
    pub gross_position_value_usd: Option<f64>,
    /// Currency IBKR reported net liquidation in, normally the base currency.
    pub native_currency: Option<String>,
    /// Net liquidation value in [`Self::native_currency`].
    pub native_net_liquidation: Option<f64>,
    /// US dollars per one unit of [`Self::native_currency`], `1` for USD.
    pub fx_rate_to_usd: Option<f64>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct AccountBalanceUpdate {
    pub(crate) provider: &'static str,
    pub(crate) ok: bool,
    pub(crate) status: &'static str,
    pub(crate) message: String,
    pub(crate) account_ref: Option<String>,
    pub(crate) balance_usd: Option<f64>,
    pub(crate) unit: &'static str,
    /// Full account summary; present only on available updates.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) summary: Option<AccountValueSummary>,
}

#[derive(Debug, Serialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub(crate) enum StreamEvent {
    Ready {
        version: u8,
        operation: &'static str,
    },
    AccountBalance {
        version: u8,
        update: AccountBalanceUpdate,
    },
    Candle {
        version: u8,
        candle: Candle,
    },
    StockQuote {
        version: u8,
        quote: StockQuote,
    },
    OrderEvent {
        version: u8,
        order: IbkrOrderEvent,
    },
    Error {
        version: u8,
        error: ErrorBody,
    },
}

#[derive(Debug, Serialize)]
pub(crate) struct ErrorEnvelope {
    pub(crate) version: u8,
    pub(crate) error: ErrorBody,
}

#[derive(Debug, Serialize)]
pub(crate) struct ErrorBody {
    pub(crate) code: &'static str,
    pub(crate) message: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;

    #[test]
    fn candle_stream_provenance_round_trips_without_changing_historical_payloads() {
        let fixture: Value = serde_json::from_str(include_str!("../fixtures/v3/contract.json"))
            .expect("valid shared contract fixture");
        let historical = fixture["candleEvent"]["candle"].clone();
        let candle: Candle = serde_json::from_value(historical.clone()).unwrap();
        assert_eq!(candle.is_snapshot, None);
        assert_eq!(serde_json::to_value(&candle).unwrap(), historical);

        for is_snapshot in [true, false] {
            let event = StreamEvent::Candle {
                version: PROTOCOL_VERSION,
                candle: Candle {
                    is_snapshot: Some(is_snapshot),
                    ..candle.clone()
                },
            };
            let serialized = serde_json::to_value(event).unwrap();
            assert_eq!(serialized["candle"]["isSnapshot"], is_snapshot);
            let parsed: Candle = serde_json::from_value(serialized["candle"].clone()).unwrap();
            assert_eq!(parsed.is_snapshot, Some(is_snapshot));
        }
    }

    #[test]
    fn stock_quote_contract_round_trips_the_shared_fixtures() {
        let fixture: Value = serde_json::from_str(include_str!("../fixtures/v3/contract.json"))
            .expect("valid shared contract fixture");
        let stream: StreamRequest =
            serde_json::from_value(fixture["stockQuoteStreamRequest"].clone())
                .expect("stock quote stream fixture");
        assert_eq!(stream.version(), PROTOCOL_VERSION);
        assert_eq!(
            serde_json::to_value(&stream).unwrap(),
            fixture["stockQuoteStreamRequest"]
        );

        let quote: StockQuote = serde_json::from_value(fixture["stockQuoteEvent"]["quote"].clone())
            .expect("stock quote event fixture");
        assert_eq!(quote.data_type, QuoteDataType::Live);
        assert_eq!(
            quote.price_increment_rules,
            vec![PriceIncrementRule {
                lower_edge: 0.0,
                increment: 0.01,
            }]
        );
        // Unknown rules are an empty list, never an omitted key: clients
        // read an omitted key as a runtime that never reports them.
        let without_rules = StockQuote {
            price_increment_rules: Vec::new(),
            ..quote.clone()
        };
        assert_eq!(
            serde_json::to_value(&without_rules).unwrap()["priceIncrementRules"],
            serde_json::json!([])
        );
        // A quote from a runtime that never reports them still parses.
        let mut older = fixture["stockQuoteEvent"]["quote"].clone();
        older
            .as_object_mut()
            .expect("quote object")
            .remove("priceIncrementRules");
        let older: StockQuote = serde_json::from_value(older).expect("older quote");
        assert!(older.price_increment_rules.is_empty());
        let event = StreamEvent::StockQuote {
            version: PROTOCOL_VERSION,
            quote,
        };
        assert_eq!(
            serde_json::to_value(event).unwrap(),
            fixture["stockQuoteEvent"]
        );
    }

    #[test]
    fn order_event_contract_round_trips_the_shared_fixtures() {
        let fixture: Value = serde_json::from_str(include_str!("../fixtures/v3/contract.json"))
            .expect("valid shared contract fixture");
        let stream: StreamRequest =
            serde_json::from_value(fixture["orderEventsStreamRequest"].clone())
                .expect("order events stream fixture");
        assert_eq!(stream.version(), PROTOCOL_VERSION);
        assert!(matches!(
            &stream,
            StreamRequest::OrderEvents { account_ref: Some(account), .. } if account == "U1234567"
        ));
        assert_eq!(
            serde_json::to_value(&stream).unwrap(),
            fixture["orderEventsStreamRequest"]
        );

        for (key, kind) in [
            ("orderStatusEvent", "status"),
            ("orderExecutionEvent", "execution"),
        ] {
            let order: IbkrOrderEvent = serde_json::from_value(fixture[key]["order"].clone())
                .unwrap_or_else(|error| panic!("{key} fixture: {error}"));
            assert_eq!(order.account_ref(), "U1234567");
            assert_eq!(serde_json::to_value(&order).unwrap()["kind"], kind);
            let event = StreamEvent::OrderEvent {
                version: PROTOCOL_VERSION,
                order,
            };
            assert_eq!(serde_json::to_value(event).unwrap(), fixture[key]);
        }
    }

    #[test]
    fn order_events_reject_unknown_fields() {
        let fixture: Value = serde_json::from_str(include_str!("../fixtures/v3/contract.json"))
            .expect("valid shared contract fixture");
        let mut request = fixture["orderEventsStreamRequest"].clone();
        request["symbol"] = Value::from("SPY");
        assert!(serde_json::from_value::<StreamRequest>(request).is_err());
        let mut order = fixture["orderExecutionEvent"]["order"].clone();
        order["secret"] = Value::from("x");
        assert!(serde_json::from_value::<IbkrOrderEvent>(order).is_err());
    }

    #[test]
    fn a_v3_bridge_without_order_events_rejects_them_as_the_fixture_shows() {
        // The 0.3.0 bridge's stream request had no `order_events` variant; its
        // serde error is what the web app recognises as an outdated bridge.
        #[derive(Debug, Deserialize)]
        #[serde(
            tag = "operation",
            rename_all = "snake_case",
            rename_all_fields = "camelCase",
            deny_unknown_fields
        )]
        #[allow(dead_code)]
        enum ReleasedStreamRequest {
            AccountBalance {
                version: u8,
                account_ref: Option<String>,
            },
            Candles {
                version: u8,
                symbol: String,
                conid: Option<i64>,
                timeframe: String,
            },
            StockQuote {
                version: u8,
                symbol: String,
                conid: Option<i64>,
            },
        }
        let fixture: Value = serde_json::from_str(include_str!("../fixtures/v3/contract.json"))
            .expect("valid shared contract fixture");
        // The web app sends `operation` first (`JSON.stringify` keeps key order).
        let request = r#"{"operation":"order_events","version":3,"accountRef":"U1234567"}"#;
        assert_eq!(
            serde_json::from_str::<Value>(request).unwrap(),
            fixture["orderEventsStreamRequest"]
        );
        let error = serde_json::from_str::<ReleasedStreamRequest>(request).unwrap_err();
        let message =
            format!("WebSocket JSON does not match the v{PROTOCOL_VERSION} contract: {error}");
        assert_eq!(
            Value::from(message),
            fixture["orderEventsOutdatedBridgeError"]["error"]["message"]
        );
    }

    #[test]
    fn stock_quote_data_types_use_snake_case_wire_names() {
        for (data_type, wire) in [
            (QuoteDataType::Live, "live"),
            (QuoteDataType::Frozen, "frozen"),
            (QuoteDataType::Delayed, "delayed"),
            (QuoteDataType::DelayedFrozen, "delayed_frozen"),
        ] {
            assert_eq!(serde_json::to_value(data_type).unwrap(), wire);
        }
    }

    #[test]
    fn stock_quote_age_is_measured_from_the_latest_update_and_never_negative() {
        let quote = StockQuote {
            symbol: "SPY".to_string(),
            conid: 756_733,
            currency: "USD".to_string(),
            bid: Some(650.2),
            ask: Some(650.25),
            last: None,
            previous_close: None,
            data_type: QuoteDataType::Live,
            updated_at_ms: 10_000,
            last_trade_at_ms: None,
            price_increment_rules: Vec::new(),
        };
        assert_eq!(quote.age_ms(12_500), 2_500);
        assert_eq!(quote.age_ms(9_000), 0);
    }

    #[test]
    fn gateway_detection_carries_every_managed_account() {
        let value = serde_json::to_value(GatewayDetection::Connected {
            message: "connected".to_string(),
            accounts: vec!["U1234567".to_string(), "U7654321".to_string()],
        })
        .expect("serialize detection");
        assert_eq!(value["accounts"][0], "U1234567");
        assert_eq!(value["accounts"][1], "U7654321");
    }

    #[test]
    fn gateway_detection_omits_an_empty_account_list() {
        let value = serde_json::to_value(GatewayDetection::Connected {
            message: "connected".to_string(),
            accounts: Vec::new(),
        })
        .expect("serialize detection");
        assert_eq!(value.get("accounts"), None);
    }

    #[test]
    fn gateway_detection_serializes_only_a_usable_connection() {
        let value = serde_json::to_value(GatewayDetection::Connected {
            message: "connected".to_string(),
            accounts: vec!["U1234567".to_string()],
        })
        .expect("serialize detection");
        assert_eq!(value["status"], "connected");
        assert_eq!(value.get("authenticated"), None);
    }

    #[test]
    fn shared_bracket_fixtures_match_rust_contract() {
        let fixture: Value = serde_json::from_str(include_str!("../fixtures/v3/contract.json"))
            .expect("valid shared contract fixture");
        let request: OrderRequest = serde_json::from_value(fixture["bracketOrderRequest"].clone())
            .expect("bracket order request fixture");
        let protection = request.protection.as_ref().expect("fixture protection");
        assert_eq!(
            protection.take_profit.as_ref().map(|leg| leg.price),
            Some(700.0)
        );
        assert_eq!(
            protection.stop_loss.as_ref().map(|leg| leg.price),
            Some(620.0)
        );
        assert_eq!(
            serde_json::to_value(&request).unwrap(),
            fixture["bracketOrderRequest"]
        );

        let leg =
            |kind, customer_order_id: &str, order_id: &str, price, status| ProtectionLegOutcome {
                kind,
                customer_order_id: customer_order_id.to_string(),
                order_id: Some(order_id.to_string()),
                price,
                status,
            };
        let response = OrderResponse {
            version: PROTOCOL_VERSION,
            provider: "ibkr",
            ok: true,
            status: "filled".to_string(),
            message: "IBKR order filled (100)".to_string(),
            account_ref: Some("U1234567".to_string()),
            order_id: Some("100".to_string()),
            customer_order_id: Some("sunk-0f8b6f1e-4c1d-4a53-9a3e-2d7f5c9b8e21".to_string()),
            protection: Some(ProtectionOutcome {
                status: ProtectionStatus::Rejected,
                message: "IBKR accepted the order but rejected the stop loss at 620 (Order rejected). The position may be unprotected; check IBKR open orders".to_string(),
                legs: vec![
                    leg(
                        ProtectionLegKind::TakeProfit,
                        "sunk-1b2c3d4e-5f60-4a71-8b92-a3b4c5d6e7f8",
                        "101",
                        700.0,
                        ProtectionStatus::Attached,
                    ),
                    leg(
                        ProtectionLegKind::StopLoss,
                        "sunk-2c3d4e5f-6071-4b82-9ca3-b4c5d6e7f809",
                        "102",
                        620.0,
                        ProtectionStatus::Rejected,
                    ),
                ],
            }),
            exit: None,
        };
        assert_eq!(
            serde_json::to_value(response).unwrap(),
            fixture["bracketOrderResponse"]
        );
    }

    #[test]
    fn shared_linked_exit_fixtures_match_rust_contract() {
        let fixture: Value = serde_json::from_str(include_str!("../fixtures/v3/contract.json"))
            .expect("valid shared contract fixture");
        let request: OrderRequest =
            serde_json::from_value(fixture["linkedExitOrderRequest"].clone())
                .expect("linked exit request fixture");
        assert!(request.reduce_only);
        assert_eq!(request.quantity, Some(10.0));
        assert_eq!(request.protection, None);
        assert_eq!(
            serde_json::to_value(&request).unwrap(),
            fixture["linkedExitOrderRequest"]
        );

        let group = "oca-sunk-0f8b6f1e-4c1d-4a53-9a3e-2d7f5c9b8e21";
        let response = OrderResponse {
            version: PROTOCOL_VERSION,
            provider: "ibkr",
            ok: true,
            status: "resting".to_string(),
            message: format!(
                "IBKR order is working (105). Linked with 1 exit order in IBKR OCA group {group}"
            ),
            account_ref: Some("U1234567".to_string()),
            order_id: Some("105".to_string()),
            customer_order_id: Some(request.customer_order_id.clone()),
            protection: None,
            exit: Some(LinkedExitOutcome {
                oca_group: group.to_string(),
                quantity: 10.0,
                position_quantity: 10.0,
                linked_order_ids: vec!["104".to_string()],
                relinked: vec![RelinkedOrder {
                    from_order_id: "88".to_string(),
                    to_order_id: "104".to_string(),
                }],
                warnings: Vec::new(),
            }),
        };
        assert_eq!(
            serde_json::to_value(response).unwrap(),
            fixture["linkedExitOrderResponse"]
        );

        // Every release before 0.4.0 refuses the flag with this exact envelope
        // before any Gateway access; the web app keys its fallback on it.
        let outdated = ErrorEnvelope {
            version: PROTOCOL_VERSION,
            error: ErrorBody {
                code: "invalid_request",
                message: "IBKR does not support reduce-only or post-only orders".to_string(),
            },
        };
        assert_eq!(
            serde_json::to_value(outdated).unwrap(),
            fixture["linkedExitOutdatedBridgeError"]
        );
    }

    #[test]
    fn shared_order_type_fixtures_match_rust_contract() {
        let fixture: Value = serde_json::from_str(include_str!("../fixtures/v3/contract.json"))
            .expect("valid shared contract fixture");
        let round_trip = |name: &str| {
            let request: OrderRequest = serde_json::from_value(fixture[name].clone())
                .unwrap_or_else(|error| panic!("{name}: {error}"));
            assert_eq!(
                serde_json::to_value(&request).unwrap(),
                fixture[name],
                "{name}"
            );
            request
        };

        let trailing = round_trip("advancedOrderRequest");
        assert_eq!(
            OrderType::parse(&trailing.order_type),
            Some(OrderType::TrailingStopLimit)
        );
        assert_eq!(trailing.trailing_percent, Some(2.5));
        assert_eq!(trailing.limit_offset, Some(0.25));
        assert!(trailing.outside_rth);
        assert_eq!(trailing.algo, None);

        let protected = round_trip("protectedMarketOrderRequest");
        assert_eq!(
            OrderType::parse(&protected.order_type),
            Some(OrderType::Limit)
        );
        assert_eq!(protected.time_in_force, "day");
        assert!(protected.outside_rth);
        assert_eq!(
            protected.algo,
            Some(OrderAlgo {
                strategy: AlgoStrategy::Adaptive,
                priority: AdaptivePriority::Normal,
            })
        );

        let on_open = round_trip("marketOnOpenOrderRequest");
        assert_eq!(
            OrderType::parse(&on_open.order_type),
            Some(OrderType::MarketOnOpen)
        );
        assert!(!on_open.outside_rth);

        // Absent flags stay absent, so a plain order is byte-for-byte what
        // bridges before 0.5.0 accept.
        let plain = round_trip("orderRequest");
        let plain = serde_json::to_value(plain).unwrap();
        for field in ["trailingPercent", "limitOffset", "outsideRth", "algo"] {
            assert_eq!(plain.get(field), None, "{field}");
        }

        // What bridges before 0.5.0 answer for the new types and fields,
        // before any Gateway access. The web app keys its update message on
        // these texts.
        let outdated = |name: &str, message: &str| {
            assert_eq!(fixture[name]["version"], PROTOCOL_VERSION);
            assert_eq!(fixture[name]["error"]["code"], "invalid_request");
            let text = fixture[name]["error"]["message"].as_str().expect("message");
            assert!(text.starts_with(message), "{name}: {text}");
        };
        outdated(
            "orderTypeOutdatedBridgeError",
            "orderType must be market, limit, or stop_market",
        );
        outdated(
            "orderFieldOutdatedBridgeError",
            &format!(
                "request JSON does not match the v{PROTOCOL_VERSION} contract: unknown field `outsideRth`"
            ),
        );
        outdated(
            "timeInForceOutdatedBridgeError",
            "unsupported IBKR timeInForce",
        );
    }

    #[test]
    fn shared_v2_fixtures_match_rust_requests() {
        let fixture: Value = serde_json::from_str(include_str!("../fixtures/v3/contract.json"))
            .expect("valid shared contract fixture");
        let probe: ProbeRequest =
            serde_json::from_value(fixture["probeRequest"].clone()).expect("probe request fixture");
        let detection: DetectionRequest =
            serde_json::from_value(fixture["detectionRequest"].clone())
                .expect("detection request fixture");
        let candles: CandleRequest = serde_json::from_value(fixture["candleRequest"].clone())
            .expect("candle request fixture");
        let options: OptionChainRequest =
            serde_json::from_value(fixture["optionChainRequest"].clone())
                .expect("option-chain request fixture");
        let order: OrderRequest =
            serde_json::from_value(fixture["orderRequest"].clone()).expect("order request fixture");
        let account_state: AccountStateRequest =
            serde_json::from_value(fixture["accountStateRequest"].clone())
                .expect("account-state request fixture");
        let cancel_order: CancelOrderRequest =
            serde_json::from_value(fixture["cancelOrderRequest"].clone())
                .expect("cancel-order request fixture");
        let reconcile_order: ReconcileOrderRequest =
            serde_json::from_value(fixture["reconcileOrderRequest"].clone())
                .expect("reconcile-order request fixture");
        let reconcile_response: ReconcileOrderResponse =
            serde_json::from_value(fixture["reconcileOrderResponse"].clone())
                .expect("reconcile-order response fixture");
        assert!(reconcile_response.state.is_found());
        let balance_stream: StreamRequest =
            serde_json::from_value(fixture["accountBalanceStreamRequest"].clone())
                .expect("account balance stream fixture");
        let candle_stream: StreamRequest =
            serde_json::from_value(fixture["candleStreamRequest"].clone())
                .expect("candle stream fixture");
        let candles_response: CandleResponse =
            serde_json::from_value(fixture["candleResponse"].clone())
                .expect("candle response fixture");
        let option_response: OptionChainResponse =
            serde_json::from_value(fixture["optionChainResponse"].clone())
                .expect("option-chain response fixture");
        let option_quote: OptionQuoteRequest =
            serde_json::from_value(fixture["optionQuoteRequest"].clone())
                .expect("option-quote request fixture");
        let option_quote_response: OptionQuoteResponse =
            serde_json::from_value(fixture["optionQuoteResponse"].clone())
                .expect("option-quote response fixture");
        assert_eq!(option_quote.version, PROTOCOL_VERSION);
        assert_eq!(option_quote_response.version, PROTOCOL_VERSION);
        assert_eq!(
            serde_json::to_value(option_quote).unwrap(),
            fixture["optionQuoteRequest"]
        );
        assert_eq!(
            serde_json::to_value(option_quote_response).unwrap(),
            fixture["optionQuoteResponse"]
        );
        let detection_response: DetectionResponse =
            serde_json::from_value(fixture["detectionResponse"].clone())
                .expect("detection response fixture");
        assert_eq!(probe.version, PROTOCOL_VERSION);
        assert_eq!(detection.version, PROTOCOL_VERSION);
        assert_eq!(candles.version, PROTOCOL_VERSION);
        assert_eq!(options.version, PROTOCOL_VERSION);
        assert_eq!(order.version, PROTOCOL_VERSION);
        assert_eq!(account_state.version, PROTOCOL_VERSION);
        assert_eq!(cancel_order.version, PROTOCOL_VERSION);
        assert_eq!(balance_stream.version(), PROTOCOL_VERSION);
        assert_eq!(candle_stream.version(), PROTOCOL_VERSION);
        assert_eq!(detection_response.version, PROTOCOL_VERSION);
        assert_eq!(
            serde_json::to_value(probe).unwrap(),
            fixture["probeRequest"]
        );
        assert_eq!(
            serde_json::to_value(detection).unwrap(),
            fixture["detectionRequest"]
        );
        assert_eq!(
            serde_json::to_value(candles).unwrap(),
            fixture["candleRequest"]
        );
        assert_eq!(
            serde_json::to_value(options).unwrap(),
            fixture["optionChainRequest"]
        );
        assert_eq!(
            serde_json::to_value(order).unwrap(),
            fixture["orderRequest"]
        );
        assert_eq!(
            serde_json::to_value(account_state).unwrap(),
            fixture["accountStateRequest"]
        );
        assert_eq!(
            serde_json::to_value(cancel_order).unwrap(),
            fixture["cancelOrderRequest"]
        );
        assert_eq!(
            serde_json::to_value(reconcile_order).unwrap(),
            fixture["reconcileOrderRequest"]
        );
        assert_eq!(
            serde_json::to_value(reconcile_response).unwrap(),
            fixture["reconcileOrderResponse"]
        );
        assert_eq!(
            serde_json::to_value(balance_stream).unwrap(),
            fixture["accountBalanceStreamRequest"]
        );
        assert_eq!(
            serde_json::to_value(candle_stream).unwrap(),
            fixture["candleStreamRequest"]
        );
        assert_eq!(
            serde_json::to_value(detection_response).unwrap(),
            fixture["detectionResponse"]
        );
        assert_eq!(
            serde_json::to_value(candles_response).unwrap(),
            fixture["candleResponse"]
        );
        assert_eq!(
            serde_json::to_value(option_response).unwrap(),
            fixture["optionChainResponse"]
        );
        let probe_response = ProbeResponse {
            version: PROTOCOL_VERSION,
            provider: "ibkr",
            ok: true,
            status: "connected".to_string(),
            message: "IB Gateway TWS API is connected".to_string(),
            account_ref: Some("U1234567".to_string()),
        };
        let order_response = OrderResponse {
            version: PROTOCOL_VERSION,
            provider: "ibkr",
            ok: true,
            status: "accepted".to_string(),
            message: "IBKR order accepted (12345)".to_string(),
            account_ref: Some("U1234567".to_string()),
            order_id: Some("12345".to_string()),
            customer_order_id: Some("sunk-0f8b6f1e-4c1d-4a53-9a3e-2d7f5c9b8e21".to_string()),
            protection: None,
            exit: None,
        };
        let cancel_order_response = OrderResponse {
            version: PROTOCOL_VERSION,
            provider: "ibkr",
            ok: true,
            status: "cancelled".to_string(),
            message: "IBKR confirmed the order cancellation".to_string(),
            account_ref: Some("U1234567".to_string()),
            order_id: Some("12345".to_string()),
            customer_order_id: None,
            protection: None,
            exit: None,
        };
        let account_state_response = AccountStateResponse {
            version: PROTOCOL_VERSION,
            provider: "ibkr",
            account_ref: "U1234567".to_string(),
            positions: vec![AccountPosition {
                account_ref: "U1234567".to_string(),
                contract_id: 756733,
                symbol: "SPY".to_string(),
                local_symbol: "SPY".to_string(),
                instrument_kind: Some(InstrumentKind::Stock),
                size: 1.25,
                average_price: 650.25,
                multiplier: 1.0,
            }],
            orders: vec![AccountOpenOrder {
                account_ref: "U1234567".to_string(),
                order_id: 12345,
                contract_id: 756733,
                symbol: "SPY".to_string(),
                instrument_kind: Some(InstrumentKind::Stock),
                side: "sell".to_string(),
                quantity: 1.25,
                price: Some(675.0),
                trigger_price: None,
                status: "Submitted".to_string(),
                order_type: Some(OrderType::Limit),
                time_in_force: Some("gtc".to_string()),
                outside_rth: false,
                algo: None,
                oca_group: Some("oca-sunk-0f8b6f1e-4c1d-4a53-9a3e-2d7f5c9b8e21".to_string()),
                parent_id: None,
                filled_quantity: None,
            }],
            server_link: ServerLink::Connected,
        };
        let error_response = ErrorEnvelope {
            version: PROTOCOL_VERSION,
            error: ErrorBody {
                code: "gateway_unavailable",
                message: "IB Gateway is unavailable".to_string(),
            },
        };
        assert_eq!(
            serde_json::to_value(probe_response).unwrap(),
            fixture["probeResponse"]
        );
        assert_eq!(
            serde_json::to_value(order_response).unwrap(),
            fixture["orderResponse"]
        );
        assert_eq!(
            serde_json::to_value(cancel_order_response).unwrap(),
            fixture["cancelOrderResponse"]
        );
        assert_eq!(
            serde_json::to_value(account_state_response).unwrap(),
            fixture["accountStateResponse"]
        );
        assert_eq!(
            serde_json::to_value(error_response).unwrap(),
            fixture["errorResponse"]
        );
        let ready_event = StreamEvent::Ready {
            version: PROTOCOL_VERSION,
            operation: "account_balance",
        };
        let account_balance_event = StreamEvent::AccountBalance {
            version: PROTOCOL_VERSION,
            update: AccountBalanceUpdate {
                provider: "ibkr",
                ok: true,
                status: "available",
                message: "IBKR account summary is available".to_string(),
                account_ref: Some("U1234567".to_string()),
                balance_usd: Some(1250.5),
                unit: "USD",
                summary: Some(AccountValueSummary {
                    net_liquidation_usd: Some(1250.5),
                    total_cash_usd: Some(400.25),
                    available_funds_usd: Some(380.0),
                    gross_position_value_usd: Some(850.25),
                    native_currency: Some("USD".to_string()),
                    native_net_liquidation: Some(1250.5),
                    fx_rate_to_usd: Some(1.0),
                }),
            },
        };
        let candle_event = StreamEvent::Candle {
            version: PROTOCOL_VERSION,
            candle: Candle {
                timestamp: 1_784_912_400_000,
                open: 650.1,
                high: 651.5,
                low: 649.75,
                close: 651.25,
                volume: 125_000.0,
                confirmed: false,
                is_snapshot: None,
            },
        };
        let stream_error_event = StreamEvent::Error {
            version: PROTOCOL_VERSION,
            error: ErrorBody {
                code: "gateway_unavailable",
                message: "IB Gateway is unavailable".to_string(),
            },
        };
        assert_eq!(
            serde_json::to_value(ready_event).unwrap(),
            fixture["readyEvent"]
        );
        assert_eq!(
            serde_json::to_value(account_balance_event).unwrap(),
            fixture["accountBalanceEvent"]
        );
        assert_eq!(
            serde_json::to_value(candle_event).unwrap(),
            fixture["candleEvent"]
        );
        assert_eq!(
            serde_json::to_value(stream_error_event).unwrap(),
            fixture["streamErrorEvent"]
        );
    }
}
