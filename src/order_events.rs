//! Streaming IBKR order status changes and executions for one account.
//!
//! TWS pushes `openOrder`, `orderStatus`, `execDetails` and
//! `commissionReport` messages for the orders an API client placed. `ibapi`
//! exposes them through one order-update subscription per client
//! ([`Client::order_update_stream`]), which also receives a copy of every
//! order message any other request on the connection triggers: each
//! open-orders read repeats every working order's status, and execution
//! snapshots repeat today's fills.
//!
//! This module turns that firehose into a bounded, typed event stream:
//!
//! - [`OrderEventNormalizer`] (pure, unit tested) correlates statuses and
//!   executions with the order details from `openOrder` and completed-order
//!   messages (account, reference, contract, side, type, parent, OCA group),
//!   forwards a status only when it changed, forwards each execution once,
//!   and forwards it again when its commission report adds the realized PnL
//!   or its order's details arrive after it. Its caches are bounded, and every event it
//!   emits fits the strict renderer contract (`@sunk/desktop-contract`):
//!   out-of-range numbers are cleared or the event is dropped, never sent.
//! - [`OrderEventHub`] owns the one provider subscription per TWS client and
//!   fans normalized events out to every consumer through a broadcast
//!   channel, so several browser tabs or desktop windows share it.
//! - [`OrderEventSubscription`] filters one consumer's events to its account
//!   and starts with a replay of the account's latest statuses and recent
//!   executions, so a consumer that (re)connects learns about fills it
//!   missed. When it starts, the hub primes that replay with this client's
//!   open orders, today's completed orders and today's executions, so bracket
//!   children keep their parent and type after a bridge, sidecar or
//!   connection restart.
//!
//! TWS warnings on the subscription (for example 399, an order held until
//! the market opens) are notices: they are logged and never end the stream.
//! Only a provider error or the end of the connection does.

use crate::contract::{IbkrExecutionEvent, IbkrOrderEvent, IbkrOrderStatusEvent, OrderEventSide};
use crate::gateway::GatewayError;
use futures::{Stream, StreamExt};
use ibapi::orders::{
    Action, CommissionReport, ExecutionData, ExecutionFilter, ExecutionSide, OcaType, OrderData,
    OrderStatus, OrderUpdate, Orders,
};
use ibapi::prelude::{Client, SubscriptionItem};
use std::collections::{HashMap, VecDeque};
use std::hash::Hash;
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;
use time::{Date, Month, PrimitiveDateTime, Time, UtcOffset};
use time_tz::{PrimitiveDateTimeExt, Tz, timezones};
use tokio::sync::broadcast;
use tokio::task::JoinHandle;

/// Orders whose details and latest status are remembered per TWS client.
const MAX_TRACKED_ORDERS: usize = 1_024;
/// Executions remembered for commission merging, deduplication and replay.
const MAX_TRACKED_EXECUTIONS: usize = 2_048;
/// Commission reports held for an execution that has not arrived yet.
const MAX_PENDING_COMMISSIONS: usize = 256;
/// Events buffered per consumer before a slow consumer is told it fell behind.
const ORDER_EVENT_CHANNEL_CAPACITY: usize = 1_024;
/// Bounds each of the hub's priming reads (open orders, completed orders,
/// executions).
const PRIME_READ_TIMEOUT: Duration = Duration::from_secs(12);
/// Longest account, order reference or execution id the renderer accepts.
const MAX_IDENTIFIER_CHARS: usize = 512;
/// Longest symbol or local symbol the renderer accepts.
const MAX_INSTRUMENT_CHARS: usize = 100;
/// Longest security type, order type or status the renderer accepts.
const MAX_LABEL_CHARS: usize = 32;
/// Longest currency code the renderer accepts.
const MAX_CURRENCY_CHARS: usize = 16;
/// Largest quantity or price the renderer accepts.
const MAX_QUANTITY_OR_PRICE: f64 = 1e9;
/// Largest commission or realized PnL magnitude the renderer accepts.
const MAX_SIGNED_AMOUNT: f64 = 1e15;
/// Largest integer JavaScript represents exactly.
const MAX_SAFE_INTEGER: i64 = 9_007_199_254_740_991;
/// Non-IANA zone names IB Gateway sends, mapped as `ibapi` maps them for the
/// session zone (its alias table is private to the crate).
const TIMEZONE_ALIASES: &[(&str, &str)] = &[
    ("中国标准时间", "Asia/Shanghai"),
    ("北京时间", "Asia/Shanghai"),
    ("China Standard Time", "Asia/Shanghai"),
    ("Greenwich Mean Time", "Europe/London"),
    ("GMT Standard Time", "Europe/London"),
    ("British Summer Time", "Europe/London"),
    ("SGT", "Asia/Singapore"),
    ("E. Europe Standard Time", "Europe/Bucharest"),
    ("Eastern European Standard Time", "Europe/Athens"),
    ("Eastern European Summer Time", "Europe/Athens"),
    ("FLE Standard Time", "Europe/Helsinki"),
    ("GTB Standard Time", "Europe/Athens"),
    ("Central European Standard Time", "Europe/Warsaw"),
    ("Central European Summer Time", "Europe/Warsaw"),
    ("W. Europe Standard Time", "Europe/Berlin"),
    ("Romance Standard Time", "Europe/Paris"),
];

/// Insertion-ordered map that forgets its oldest keys past a fixed size.
#[derive(Debug)]
struct BoundedMap<K, V> {
    values: HashMap<K, V>,
    order: VecDeque<K>,
    limit: usize,
}

impl<K: Clone + Eq + Hash, V> BoundedMap<K, V> {
    fn new(limit: usize) -> Self {
        Self {
            values: HashMap::new(),
            order: VecDeque::new(),
            limit,
        }
    }

    fn get(&self, key: &K) -> Option<&V> {
        self.values.get(key)
    }

    fn get_mut(&mut self, key: &K) -> Option<&mut V> {
        self.values.get_mut(key)
    }

    fn remove(&mut self, key: &K) -> Option<V> {
        let removed = self.values.remove(key);
        if removed.is_some() {
            self.order.retain(|candidate| candidate != key);
        }
        removed
    }

    fn insert(&mut self, key: K, value: V) {
        if self.values.insert(key.clone(), value).is_none() {
            self.order.push_back(key);
        }
        while self.order.len() > self.limit {
            if let Some(oldest) = self.order.pop_front() {
                self.values.remove(&oldest);
            }
        }
    }

    /// Values in insertion order, oldest first.
    fn values_in_order(&self) -> impl Iterator<Item = &V> {
        self.order.iter().filter_map(|key| self.values.get(key))
    }

    /// Keys in insertion order, oldest first.
    fn keys_in_order(&self) -> Vec<K> {
        self.order.iter().cloned().collect()
    }
}

/// Order details learned from an `openOrder` or completed-order message,
/// already within the renderer contract's bounds.
#[derive(Clone, Debug, PartialEq)]
struct OrderDetails {
    account_ref: String,
    perm_id: i64,
    parent_id: Option<i32>,
    oca_group: Option<String>,
    order_ref: Option<String>,
    conid: i64,
    symbol: String,
    local_symbol: Option<String>,
    security_type: String,
    currency: String,
    side: OrderEventSide,
    order_type: String,
}

/// The last status seen for one order and whether it was forwarded.
#[derive(Clone, Debug)]
struct TrackedStatus {
    status: OrderStatus,
    /// The status event last forwarded, for change detection and replay.
    forwarded: Option<IbkrOrderStatusEvent>,
}

/// TWS order identity: order ids are unique only per API client.
type OrderKey = (i32, i32);

/// Pure, bounded correlation of raw TWS order messages into typed events.
///
/// Feed it every [`OrderUpdate`] from one connection in arrival order; each
/// call returns the events that update produced, possibly none.
#[derive(Debug)]
pub(crate) struct OrderEventNormalizer {
    time_zone: Option<&'static Tz>,
    orders: BoundedMap<OrderKey, OrderDetails>,
    /// The same details by permanent order id: completed orders and
    /// executions from another session may carry no usable TWS order id.
    orders_by_perm: BoundedMap<i64, OrderDetails>,
    statuses: BoundedMap<OrderKey, TrackedStatus>,
    executions: BoundedMap<String, IbkrExecutionEvent>,
    pending_commissions: BoundedMap<String, CommissionReport>,
}

impl OrderEventNormalizer {
    /// Create a normalizer for one TWS session.
    ///
    /// `time_zone` is the session's time zone, used for execution times that
    /// do not name their own zone.
    pub(crate) fn new(time_zone: Option<&'static Tz>) -> Self {
        Self {
            time_zone,
            orders: BoundedMap::new(MAX_TRACKED_ORDERS),
            orders_by_perm: BoundedMap::new(MAX_TRACKED_ORDERS),
            statuses: BoundedMap::new(MAX_TRACKED_ORDERS),
            executions: BoundedMap::new(MAX_TRACKED_EXECUTIONS),
            pending_commissions: BoundedMap::new(MAX_PENDING_COMMISSIONS),
        }
    }

    /// Correlate one TWS order message.
    pub(crate) fn observe(&mut self, update: OrderUpdate) -> Vec<IbkrOrderEvent> {
        match update {
            OrderUpdate::OpenOrder(order) => self.observe_order_details(&order),
            OrderUpdate::OrderStatus(status) => self.observe_status(status),
            OrderUpdate::ExecutionData(execution) => self.observe_execution(execution),
            OrderUpdate::CommissionReport(report) => self.observe_commission(report),
        }
    }

    /// The latest forwarded status of every remembered order and the
    /// remembered executions of `account_ref`, oldest first.
    pub(crate) fn replay(&self, account_ref: &str) -> Vec<IbkrOrderEvent> {
        let statuses = self
            .statuses
            .values_in_order()
            .filter_map(|tracked| tracked.forwarded.clone())
            .filter(|status| status.account_ref == account_ref)
            .map(IbkrOrderEvent::Status);
        let executions = self
            .executions
            .values_in_order()
            .filter(|execution| execution.account_ref == account_ref)
            .cloned()
            .map(IbkrOrderEvent::Execution);
        statuses.chain(executions).collect()
    }

    /// Learn one order's details from an open or completed order.
    ///
    /// Forwards a status that was waiting for them, and re-sends every
    /// remembered execution of the order that now gains its parent, type or
    /// reference.
    pub(crate) fn observe_order_details(&mut self, order: &OrderData) -> Vec<IbkrOrderEvent> {
        let Some(details) = order_details(order) else {
            return Vec::new();
        };
        let key = (order.order.client_id, order.order_id);
        let perm_id = details.perm_id;
        if order.order_id > 0 {
            self.orders.insert(key, details.clone());
        }
        if perm_id > 0 {
            self.orders_by_perm.insert(perm_id, details.clone());
        }
        let mut events = Vec::new();
        // A status that arrived before the order's details is forwarded now.
        if order.order_id > 0
            && let Some(status) = self
                .statuses
                .get(&key)
                .map(|tracked| tracked.status.clone())
        {
            events.extend(self.forward_status(key, status));
        }
        for exec_id in self.executions.keys_in_order() {
            let Some(execution) = self.executions.get_mut(&exec_id) else {
                continue;
            };
            let same_order =
                (order.order_id > 0 && execution.client_id == key.0 && execution.order_id == key.1)
                    || (perm_id > 0 && execution.perm_id == perm_id);
            if !same_order || execution.account_ref != details.account_ref {
                continue;
            }
            let before = execution.clone();
            enrich_execution(execution, &details);
            if *execution != before {
                events.push(IbkrOrderEvent::Execution(execution.clone()));
            }
        }
        events
    }

    fn observe_status(&mut self, status: OrderStatus) -> Vec<IbkrOrderEvent> {
        if status.order_id <= 0 {
            return Vec::new();
        }
        let key = (status.client_id, status.order_id);
        let forwarded = self
            .statuses
            .get(&key)
            .and_then(|tracked| tracked.forwarded.clone());
        self.statuses.insert(
            key,
            TrackedStatus {
                status: status.clone(),
                forwarded,
            },
        );
        self.forward_status(key, status)
    }

    fn forward_status(&mut self, key: OrderKey, status: OrderStatus) -> Vec<IbkrOrderEvent> {
        let Some(details) = self.orders.get(&key) else {
            return Vec::new();
        };
        let Some(status_label) = bounded_text(status.status.as_str(), MAX_LABEL_CHARS) else {
            return Vec::new();
        };
        let event = IbkrOrderStatusEvent {
            account_ref: details.account_ref.clone(),
            order_id: status.order_id,
            perm_id: safe_perm_id(status.perm_id).max(details.perm_id),
            parent_id: positive_id(status.parent_id).or(details.parent_id),
            oca_group: details.oca_group.clone(),
            client_id: status.client_id,
            order_ref: details.order_ref.clone(),
            conid: details.conid,
            symbol: details.symbol.clone(),
            local_symbol: details.local_symbol.clone(),
            security_type: details.security_type.clone(),
            currency: details.currency.clone(),
            side: details.side,
            order_type: details.order_type.clone(),
            status: status_label,
            filled: bounded_quantity(status.filled),
            remaining: bounded_quantity(status.remaining),
            avg_fill_price: bounded_price(status.average_fill_price),
            last_fill_price: bounded_price(status.last_fill_price),
        };
        let Some(tracked) = self.statuses.get_mut(&key) else {
            return Vec::new();
        };
        if tracked
            .forwarded
            .as_ref()
            .is_some_and(|previous| same_status(previous, &event))
        {
            return Vec::new();
        }
        tracked.forwarded = Some(event.clone());
        vec![IbkrOrderEvent::Status(event)]
    }

    fn observe_execution(&mut self, data: ExecutionData) -> Vec<IbkrOrderEvent> {
        let execution = &data.execution;
        let (Some(exec_id), Some(account_ref), Some(quantity), Some(price)) = (
            bounded_text(&execution.execution_id, MAX_IDENTIFIER_CHARS),
            bounded_text(&execution.account_number, MAX_IDENTIFIER_CHARS),
            bounded_price(Some(execution.shares)),
            bounded_price(Some(execution.price)),
        ) else {
            return Vec::new();
        };
        let (Some(symbol), Some(security_type), Some(currency)) = (
            bounded_text(&data.contract.symbol.to_string(), MAX_INSTRUMENT_CHARS),
            bounded_text(&data.contract.security_type.to_string(), MAX_LABEL_CHARS),
            bounded_text(&data.contract.currency.to_string(), MAX_CURRENCY_CHARS),
        ) else {
            return Vec::new();
        };
        if self.executions.get(&exec_id).is_some() || data.contract.contract_id <= 0 {
            return Vec::new();
        }
        let executed_at_ms = parse_execution_time(&execution.time, self.time_zone);
        if executed_at_ms.is_none() {
            tracing::warn!(
                exec_id = %exec_id,
                "IBKR execution time {:?} has no resolvable time zone; it cannot be pushed",
                execution.time
            );
        }
        let mut event = IbkrExecutionEvent {
            account_ref,
            order_id: execution.order_id,
            perm_id: safe_perm_id(execution.perm_id),
            parent_id: None,
            oca_group: None,
            client_id: execution.client_id,
            order_ref: bounded_text(&execution.order_reference, MAX_IDENTIFIER_CHARS),
            order_type: None,
            conid: i64::from(data.contract.contract_id),
            symbol,
            local_symbol: bounded_text(&data.contract.local_symbol, MAX_INSTRUMENT_CHARS),
            security_type,
            currency,
            side: match execution.side {
                ExecutionSide::Bought => OrderEventSide::Buy,
                ExecutionSide::Sold => OrderEventSide::Sell,
            },
            exec_id: exec_id.clone(),
            executed_at_ms,
            quantity,
            price,
            cumulative_quantity: bounded_quantity(execution.cumulative_quantity),
            avg_price: bounded_quantity(execution.average_price),
            liquidation: execution.liquidation != 0,
            commission: None,
            commission_currency: None,
            realized_pnl: None,
        };
        let details = self
            .orders
            .get(&(execution.client_id, execution.order_id))
            .filter(|_| execution.order_id > 0)
            .or_else(|| {
                (event.perm_id > 0)
                    .then(|| self.orders_by_perm.get(&event.perm_id))
                    .flatten()
            })
            .filter(|details| details.account_ref == event.account_ref);
        if let Some(details) = details {
            enrich_execution(&mut event, details);
        }
        if let Some(report) = self.pending_commissions.remove(&exec_id) {
            apply_commission(&mut event, &report);
        }
        self.executions.insert(exec_id, event.clone());
        vec![IbkrOrderEvent::Execution(event)]
    }

    fn observe_commission(&mut self, report: CommissionReport) -> Vec<IbkrOrderEvent> {
        let exec_id = report.execution_id.trim().to_string();
        if exec_id.is_empty() {
            return Vec::new();
        }
        let Some(execution) = self.executions.get_mut(&exec_id) else {
            self.pending_commissions.insert(exec_id, report);
            return Vec::new();
        };
        let before = execution.clone();
        apply_commission(execution, &report);
        if *execution == before {
            return Vec::new();
        }
        vec![IbkrOrderEvent::Execution(execution.clone())]
    }
}

/// Bounded details of an open or completed order, or none when its account,
/// contract or type cannot be sent to the renderer.
fn order_details(order: &OrderData) -> Option<OrderDetails> {
    if (order.order_id <= 0 && order.order.perm_id <= 0) || order.contract.contract_id <= 0 {
        return None;
    }
    Some(OrderDetails {
        account_ref: bounded_text(&order.order.account, MAX_IDENTIFIER_CHARS)?,
        perm_id: safe_perm_id(order.order.perm_id),
        parent_id: positive_id(order.order.parent_id),
        oca_group: linking_oca_group(order),
        order_ref: bounded_text(&order.order.order_ref, MAX_IDENTIFIER_CHARS),
        conid: i64::from(order.contract.contract_id),
        symbol: bounded_text(&order.contract.symbol.to_string(), MAX_INSTRUMENT_CHARS)?,
        local_symbol: bounded_text(&order.contract.local_symbol, MAX_INSTRUMENT_CHARS),
        security_type: bounded_text(&order.contract.security_type.to_string(), MAX_LABEL_CHARS)?,
        currency: bounded_text(&order.contract.currency.to_string(), MAX_CURRENCY_CHARS)?,
        side: match order.order.action {
            Action::Buy => OrderEventSide::Buy,
            Action::Sell | Action::SellShort | Action::SellLong => OrderEventSide::Sell,
        },
        order_type: bounded_text(&order.order.order_type, MAX_LABEL_CHARS)?.to_ascii_uppercase(),
    })
}

/// Fill in what the execution message lacks from its order's details.
fn enrich_execution(execution: &mut IbkrExecutionEvent, details: &OrderDetails) {
    if execution.parent_id.is_none() {
        execution.parent_id = details.parent_id;
    }
    if execution.order_type.is_none() {
        execution.order_type = Some(details.order_type.clone());
    }
    if execution.oca_group.is_none() {
        execution.oca_group = details.oca_group.clone();
    }
    if execution.order_ref.is_none() {
        execution.order_ref = details.order_ref.clone();
    }
    if execution.perm_id == 0 {
        execution.perm_id = details.perm_id;
    }
}

fn apply_commission(execution: &mut IbkrExecutionEvent, report: &CommissionReport) {
    execution.commission = bounded_amount(Some(report.commission));
    execution.commission_currency = bounded_text(&report.currency, MAX_CURRENCY_CHARS);
    execution.realized_pnl = bounded_amount(report.realized_pnl);
}

/// Statuses are equal when nothing a consumer acts on changed.
fn same_status(left: &IbkrOrderStatusEvent, right: &IbkrOrderStatusEvent) -> bool {
    left.status == right.status
        && left.filled == right.filled
        && left.remaining == right.remaining
        && left.avg_fill_price == right.avg_fill_price
        && left.parent_id == right.parent_id
        && left.oca_group == right.oca_group
        && left.perm_id == right.perm_id
}

/// The OCA group an order shares with others, when the group bounds what its
/// orders fill together (OCA types 1 and 2, "with block"), as account-state
/// orders report it. Sunk's bracket children and linked exits are type 2.
fn linking_oca_group(order: &OrderData) -> Option<String> {
    matches!(
        order.order.oca_type,
        OcaType::CancelWithBlock | OcaType::ReduceWithBlock
    )
    .then(|| bounded_text(&order.order.oca_group, MAX_IDENTIFIER_CHARS))
    .flatten()
}

fn positive_id(value: i32) -> Option<i32> {
    (value > 0).then_some(value)
}

/// A permanent order id JavaScript can carry exactly, or zero.
fn safe_perm_id(value: i64) -> i64 {
    if (0..=MAX_SAFE_INTEGER).contains(&value) {
        value
    } else {
        0
    }
}

/// Trimmed text no longer than `max_units` UTF-16 code units (the length
/// JavaScript and zod measure), or none when empty or too long.
fn bounded_text(value: &str, max_units: usize) -> Option<String> {
    let trimmed = value.trim();
    (!trimmed.is_empty() && trimmed.encode_utf16().count() <= max_units)
        .then(|| trimmed.to_string())
}

/// A quantity or average price in `[0, 1e9]`; anything else (IBKR's unset
/// sentinel, a negative or non-finite value) reads as zero.
fn bounded_quantity(value: f64) -> f64 {
    if value.is_finite() && (0.0..=MAX_QUANTITY_OR_PRICE).contains(&value) {
        value
    } else {
        0.0
    }
}

/// A positive price or quantity no larger than `1e9`, or none.
fn bounded_price(value: Option<f64>) -> Option<f64> {
    value.filter(|price| price.is_finite() && *price > 0.0 && *price <= MAX_QUANTITY_OR_PRICE)
}

/// A commission or PnL no larger than `1e15` in magnitude, or none.
fn bounded_amount(value: Option<f64>) -> Option<f64> {
    value.filter(|amount| amount.is_finite() && amount.abs() <= MAX_SIGNED_AMOUNT)
}

/// Resolve a zone name IB Gateway sent, including its Windows and localized
/// names.
fn resolve_zone(name: &str) -> Option<&'static Tz> {
    let mapped = TIMEZONE_ALIASES
        .iter()
        .find(|(alias, _)| *alias == name)
        .map_or(name, |(_, iana)| *iana);
    let mapped = if mapped.contains('\u{FFFD}') {
        // GB2312 text decoded lossily, as ibapi treats it: a Chinese install.
        "Asia/Shanghai"
    } else {
        mapped
    };
    timezones::get_by_name(mapped).or_else(|| timezones::find_by_name(mapped).into_iter().next())
}

/// Parse a TWS execution time into Unix milliseconds.
///
/// TWS sends `yyyyMMdd HH:mm:ss` followed by the zone it used, for example
/// `20260930 14:30:01 US/Eastern` (or a Windows name such as
/// `China Standard Time`), older sessions omit the zone (the session's zone
/// applies), and a session configured for UTC sends `yyyyMMdd-HH:mm:ss`.
/// Returns none for anything else or an unknown zone, never a guessed time.
pub(crate) fn parse_execution_time(value: &str, session_zone: Option<&'static Tz>) -> Option<i64> {
    let value = value.trim();
    let date = value.get(..8)?;
    let rest = value.get(8..)?;
    let (utc, rest) = match rest.strip_prefix('-') {
        Some(rest) => (true, rest),
        None if rest.starts_with(' ') => (false, rest.trim_start()),
        None => return None,
    };
    let clock = rest.get(..8)?;
    let zone_name = rest.get(8..)?.trim();
    let date = parse_date(date)?;
    let time = parse_clock(clock)?;
    let local = PrimitiveDateTime::new(date, time);
    let instant = if utc || matches!(zone_name, "UTC" | "GMT" | "Etc/UTC") {
        local.assume_offset(UtcOffset::UTC)
    } else {
        let zone = if zone_name.is_empty() {
            session_zone?
        } else {
            resolve_zone(zone_name)?
        };
        local.assume_timezone(zone).take_first()?
    };
    let millis = i64::try_from(instant.unix_timestamp_nanos() / 1_000_000).ok()?;
    (1..=MAX_SAFE_INTEGER).contains(&millis).then_some(millis)
}

fn parse_date(value: &str) -> Option<Date> {
    if value.len() != 8 || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    let year = value.get(..4)?.parse::<i32>().ok()?;
    let month = Month::try_from(value.get(4..6)?.parse::<u8>().ok()?).ok()?;
    let day = value.get(6..8)?.parse::<u8>().ok()?;
    Date::from_calendar_date(year, month, day).ok()
}

fn parse_clock(value: &str) -> Option<Time> {
    let mut parts = value.split(':');
    let hour = parts.next()?.parse::<u8>().ok()?;
    let minute = parts.next()?.parse::<u8>().ok()?;
    let second = parts.next()?.parse::<u8>().ok()?;
    if parts.next().is_some() {
        return None;
    }
    Time::from_hms(hour, minute, second).ok()
}

/// What the hub broadcasts to its consumers. Events are shared, since every
/// receiver clones what it reads.
#[derive(Clone, Debug)]
enum HubItem {
    Event(Arc<IbkrOrderEvent>),
    Ended(String),
}

/// Normalizer and broadcast sender, updated together under one lock so a
/// new consumer's replay and live receiver never overlap or leave a gap.
#[derive(Debug)]
struct HubState {
    normalizer: OrderEventNormalizer,
    sender: broadcast::Sender<HubItem>,
    /// Why the provider subscription ended, set under the same lock as the
    /// `Ended` broadcast so no subscriber can miss it.
    ended: Option<String>,
}

impl HubState {
    fn new(time_zone: Option<&'static Tz>) -> Self {
        let (sender, _) = broadcast::channel(ORDER_EVENT_CHANNEL_CAPACITY);
        Self {
            normalizer: OrderEventNormalizer::new(time_zone),
            sender,
            ended: None,
        }
    }

    /// Broadcast what `observe` produced; no receiver is fine, later
    /// consumers get the replay.
    fn publish(&self, events: Vec<IbkrOrderEvent>) {
        for event in events {
            let _ = self.sender.send(HubItem::Event(Arc::new(event)));
        }
    }

    /// Open one consumer's subscription: the account's replay, then live
    /// events, or at once the end when the provider subscription is gone.
    fn subscribe(&self, account_ref: String) -> OrderEventSubscription {
        OrderEventSubscription {
            replay: self.normalizer.replay(&account_ref).into(),
            receiver: self.sender.subscribe(),
            ended: self.ended.clone(),
            account_ref,
        }
    }
}

/// The one order-update subscription of a TWS client, shared by every
/// order-event consumer of that client.
#[derive(Debug)]
pub(crate) struct OrderEventHub {
    client: Weak<Client>,
    state: Arc<Mutex<HubState>>,
    task: JoinHandle<()>,
    primer: JoinHandle<()>,
}

impl OrderEventHub {
    /// Subscribe to the client's order updates and start forwarding them.
    ///
    /// # Errors
    ///
    /// Returns an availability error when TWS refuses the subscription, for
    /// example while a previous hub on the same client is still releasing it.
    pub(crate) async fn start(client: &Arc<Client>) -> Result<Self, GatewayError> {
        let subscription = client.order_update_stream().await.map_err(|error| {
            GatewayError::Unavailable(format!("IBKR order updates unavailable: {error}"))
        })?;
        let state = Arc::new(Mutex::new(HubState::new(client.time_zone())));
        let task = tokio::spawn(pump_order_updates(subscription, Arc::clone(&state)));
        let primer = tokio::spawn(prime_hub(Arc::clone(client), Arc::clone(&state)));
        Ok(Self {
            client: Arc::downgrade(client),
            state,
            task,
            primer,
        })
    }

    /// Whether this hub still forwards updates for `client`.
    pub(crate) fn serves(&self, client: &Arc<Client>) -> bool {
        Weak::ptr_eq(&self.client, &Arc::downgrade(client))
            && !self.task.is_finished()
            && lock_state(&self.state).ended.is_none()
    }

    /// Open one consumer's subscription for `account_ref`.
    pub(crate) fn subscribe(&self, account_ref: String) -> OrderEventSubscription {
        lock_state(&self.state).subscribe(account_ref)
    }
}

impl Drop for OrderEventHub {
    fn drop(&mut self) {
        self.task.abort();
        self.primer.abort();
    }
}

fn lock_state(state: &Mutex<HubState>) -> std::sync::MutexGuard<'_, HubState> {
    // The lock only guards in-memory bookkeeping that cannot be left torn.
    state
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Forward every update of one provider subscription until it ends.
///
/// Notices, including warnings such as 399 (order held until the market
/// opens), are logged and skipped; only a provider error or the end of the
/// subscription ends the hub.
async fn pump_order_updates<S>(mut updates: S, state: Arc<Mutex<HubState>>)
where
    S: Stream<Item = Result<SubscriptionItem<OrderUpdate>, ibapi::Error>> + Unpin,
{
    let ended = loop {
        match updates.next().await {
            Some(Ok(SubscriptionItem::Data(update))) => {
                let mut state = lock_state(&state);
                let events = state.normalizer.observe(update);
                state.publish(events);
            }
            Some(Ok(SubscriptionItem::Notice(notice))) => {
                tracing::debug!(
                    code = notice.code,
                    "IBKR order-update notice: {}",
                    notice.message
                );
            }
            Some(Err(error)) => break format!("IBKR order updates failed: {error}"),
            None => break "IBKR order updates ended; reconnecting".to_string(),
        }
    };
    let mut state = lock_state(&state);
    state.ended = Some(ended.clone());
    let _ = state.sender.send(HubItem::Ended(ended));
}

/// Feed order details from one priming read straight into the normalizer.
///
/// Completed orders never reach the order-update subscription (it does not
/// decode them), so their details are applied here; open orders also arrive
/// on the subscription, which is harmless because details are idempotent.
async fn prime_orders<S>(mut orders: S, state: &Mutex<HubState>) -> Result<(), ibapi::Error>
where
    S: Stream<Item = Result<SubscriptionItem<Orders>, ibapi::Error>> + Unpin,
{
    while let Some(item) = orders.next().await {
        if let SubscriptionItem::Data(Orders::OrderData(order)) = item? {
            let mut state = lock_state(state);
            let events = state.normalizer.observe_order_details(&order);
            state.publish(events);
        }
    }
    Ok(())
}

/// Read this client's open orders, today's completed orders and today's
/// executions once, so the hub can replay fills that happened before it
/// started with their bracket details. Execution replies reach the hub
/// through the order-update subscription; this read only drains its copy.
async fn prime_hub(client: Arc<Client>, state: Arc<Mutex<HubState>>) {
    let open = async {
        let orders = client.open_orders().await?;
        prime_orders(orders, &state).await
    };
    if let Err(error) = bounded_prime("open orders", open).await {
        tracing::debug!("{error}");
    }
    let completed = async {
        let orders = client.completed_orders(false).await?;
        prime_orders(orders, &state).await
    };
    if let Err(error) = bounded_prime("completed orders", completed).await {
        tracing::debug!("{error}");
    }
    let executions = async {
        let mut executions = client.executions(ExecutionFilter::default()).await?;
        while let Some(item) = executions.next().await {
            item?;
        }
        Ok(())
    };
    if let Err(error) = bounded_prime("executions", executions).await {
        tracing::debug!("{error}");
    }
}

async fn bounded_prime(
    label: &str,
    read: impl std::future::Future<Output = Result<(), ibapi::Error>>,
) -> Result<(), String> {
    match tokio::time::timeout(PRIME_READ_TIMEOUT, read).await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(error)) => Err(format!("IBKR {label} priming failed: {error}")),
        Err(_) => Err(format!("IBKR {label} priming timed out")),
    }
}

/// Whether an order-event stream should be reopened after `error`.
///
/// # Arguments
///
/// * `error` - Failure that ended the subscription or its setup.
///
/// # Returns
///
/// False for invalid input (such as an ambiguous or unknown account),
/// unsafe configuration and malformed provider data, which a retry cannot
/// fix; true for availability and provider failures.
pub fn should_retry_order_events(error: &GatewayError) -> bool {
    !matches!(
        error,
        GatewayError::Input(_) | GatewayError::Configuration(_) | GatewayError::Contract(_)
    )
}

/// One consumer's order events for one account.
#[derive(Debug)]
pub struct OrderEventSubscription {
    account_ref: String,
    replay: VecDeque<IbkrOrderEvent>,
    receiver: broadcast::Receiver<HubItem>,
    /// Set when the hub had already ended at subscription time.
    ended: Option<String>,
}

impl OrderEventSubscription {
    /// Account whose events this subscription yields.
    pub fn account_ref(&self) -> &str {
        &self.account_ref
    }

    /// Waits for the account's next order event.
    ///
    /// The first events replay the account's latest order statuses and
    /// recent executions; live events follow without a gap.
    ///
    /// # Errors
    ///
    /// Returns an availability error when the provider subscription ended
    /// (connection loss) or this consumer fell more than the channel capacity
    /// behind; the caller reports the outage and subscribes again.
    pub async fn next(&mut self) -> Option<Result<IbkrOrderEvent, GatewayError>> {
        if let Some(event) = self.replay.pop_front() {
            return Some(Ok(event));
        }
        if let Some(message) = self.ended.take() {
            return Some(Err(GatewayError::Unavailable(message)));
        }
        loop {
            match self.receiver.recv().await {
                Ok(HubItem::Event(event)) if event.account_ref() == self.account_ref => {
                    return Some(Ok(Arc::unwrap_or_clone(event)));
                }
                Ok(HubItem::Event(_)) => {}
                Ok(HubItem::Ended(message)) => {
                    return Some(Err(GatewayError::Unavailable(message)));
                }
                Err(broadcast::error::RecvError::Lagged(skipped)) => {
                    return Some(Err(GatewayError::Unavailable(format!(
                        "IBKR order events fell {skipped} behind; resubscribing"
                    ))));
                }
                Err(broadcast::error::RecvError::Closed) => return None,
            }
        }
    }
}

#[cfg(test)]
mod tests;
