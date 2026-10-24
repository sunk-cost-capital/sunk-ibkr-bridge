//! Linked exits: IBKR OCA groups that keep a position's exit orders from
//! together closing more than the position.
//!
//! IBKR has no reduce-only attribute for stock or option orders (`ibapi`
//! 3.3.0's `Order` has none; `open_close` is for institutional accounts
//! only), so two independent full-size exits, such as a take profit and a
//! stop loss, can both fill and open a reverse position. IBKR's tool for this
//! is the one-cancels-all (OCA) group. Orders sharing an `ocaGroup` name with
//! `ocaType` 2 ("remaining orders are proportionately reduced in size with
//! block") are linked: a fill of one reduces every other order in the group by
//! the filled quantity, and "with block" gives overfill protection, "only one
//! order in the group will be routed at a time". Every exit in one group is at
//! most the position, so after any fill each remaining exit is at most the
//! remaining position.
//!
//! IBKR also documents two constraints this module designs around:
//!
//! - "If an order in the group is canceled, the remaining orders are also
//!   canceled." One member can never be cancelled on its own, so dropping an
//!   exit re-places its siblings into a fresh group before cancelling it.
//! - A transmitted order cannot be moved into a group, so an exit placed
//!   before linking existed is re-placed as a copy inside the group first and
//!   the original is cancelled afterwards. The copy is always placed before
//!   the original is cancelled, so the position is never without its stop;
//!   the cost is a sub-second window in which both versions are live.
//!
//! IBKR does not document whether a group name links orders placed by
//! different API clients, so only this client's groups are ever joined or
//! re-placed. Exits placed by TWS or another client are counted, never moved.
//!
//! Sources: TWS API [One Cancels All](https://interactivebrokers.github.io/tws-api/oca.html)
//! (OCA types and block), IBKR Campus
//! [OCA attribute lesson](https://www.interactivebrokers.com/campus/trading-lessons/using-the-one-cancels-another-oca-order-attribute-in-ibkrs-ibusopt/)
//! ("reduces the quantity of other orders in the group by the partially filled
//! quantity"), and IBKR's [OCA order type](https://www.interactivebrokers.com/en/trading/orders/oca.php)
//! page (cancellation of one member cancels the group; cancellations are not
//! guaranteed to reach the exchange before a fill).

use super::order_messages::ORDER_FOLLOW_UP_TIMEOUT;
use super::{
    CancelOutcome, GatewayError, ORDER_ACKNOWLEDGEMENT_TIMEOUT, ORDER_SUBMISSION_TIMEOUT,
    OrderMessage, OrderSnapshot, OutcomeDeadlines, ProviderOrders, SubmittedOrder, build_order,
    cancel_indeterminate_response, cancel_response, classify_order_message, collect_order_snapshot,
    confirm_cancel, indeterminate_response, read_position, read_provider_orders, send_cancel,
    single_order_outcome,
};
use crate::contract::{
    LinkedExitOutcome, OrderRequest, OrderResponse, PROTOCOL_VERSION, RelinkedOrder,
};
use futures::{Stream, StreamExt};
use ibapi::contracts::Contract;
use ibapi::orders::{Action, OcaType, Order, OrderData, OrderStatusKind, PlaceOrder, TimeInForce};
use ibapi::prelude::{Client, SubscriptionItem};
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::Instant;

/// OCA type of every linked exit: each fill reduces the others, with block.
pub(super) const LINKED_EXIT_OCA_TYPE: OcaType = OcaType::ReduceWithBlock;
/// Longest the work after preparation may take: placing copies and the exit,
/// cancelling originals, and verifying. Matches a plain order's submission,
/// acknowledgement and follow-up budget, so a linked exit still finishes
/// inside the 30-second caller deadline.
pub(super) const LINKED_EXIT_BUDGET: Duration = Duration::from_secs(12);
/// Most earlier exits one request re-places. More would not fit the budget.
pub(super) const MAX_RELINKED_EXITS: usize = 4;
/// Quantities within this distance are equal (IBKR sends decimals as text).
const QUANTITY_EPSILON: f64 = 1e-9;
/// Order types a copy reproduces exactly from the open-order report.
pub(super) const RELINKABLE_ORDER_TYPES: [&str; 3] = ["LMT", "STP", "STP LMT"];
/// Pause between verification reads while cancellations settle.
const VERIFY_RETRY_DELAY: Duration = Duration::from_millis(250);
/// Longest message a linked-exit response carries. The web app's order
/// journal stores at most 4,096 characters per message.
const MAX_MESSAGE_CHARS: usize = 3_800;
/// Most linked order ids one response lists.
pub(super) const MAX_LINKED_ORDER_IDS: usize = 64;

/// One working IBKR order of the selected account, as reported by IBKR.
#[derive(Clone, Debug, PartialEq)]
pub(super) struct WorkingOrder {
    /// Order id in the id space of the client that placed it.
    pub(super) order_id: i32,
    /// API client that placed the order (0 for TWS itself).
    pub(super) client_id: i32,
    /// IBKR permanent id, unique across clients and kept on completion.
    pub(super) perm_id: i64,
    /// Contract the order trades.
    pub(super) contract_id: i32,
    /// Contract as IBKR reported it with the order; a copy is routed with it.
    pub(super) contract: Contract,
    /// Bracket parent id in the placing client's id space, or 0.
    pub(super) parent_id: i32,
    /// Whether the order sells.
    pub(super) sells: bool,
    /// Quantity still to fill.
    pub(super) remaining: f64,
    /// Canonical TWS status.
    pub(super) status: OrderStatusKind,
    /// Order as IBKR reported it; the source of every copied field.
    pub(super) order: Order,
}

impl WorkingOrder {
    /// The OCA link this order shares with others, or none for a loose order.
    ///
    /// Only groups with block (types 1 and 2) bound the group to one fill at a
    /// time; a group without block can overfill, so its orders count alone.
    fn link_key(&self) -> Option<(i32, &str)> {
        let blocking = matches!(
            self.order.oca_type,
            OcaType::CancelWithBlock | OcaType::ReduceWithBlock
        );
        (blocking && !self.order.oca_group.is_empty())
            .then_some((self.client_id, self.order.oca_group.as_str()))
    }

    /// Whether this client placed the order in a linked-exit group.
    fn in_own_linked_group(&self, client_id: i32) -> bool {
        self.client_id == client_id
            && !self.order.oca_group.is_empty()
            && self.order.oca_type == LINKED_EXIT_OCA_TYPE
    }

    /// Short description for messages, such as `#41 (STP sell 100)`.
    fn describe(&self) -> String {
        format!(
            "#{} ({} {} {})",
            self.order_id,
            self.order.order_type,
            if self.sells { "sell" } else { "buy" },
            format_quantity(self.remaining)
        )
    }
}

/// Format a quantity without a trailing `.0`.
pub(super) fn format_quantity(quantity: f64) -> String {
    if quantity.fract() == 0.0 && quantity.abs() < 1e15 {
        format!("{quantity:.0}")
    } else {
        format!("{quantity}")
    }
}

/// Keep the account's working orders from an all-open-orders snapshot.
///
/// # Arguments
///
/// * `open` - IBKR's open orders from every API client and TWS.
/// * `account` - Selected account.
///
/// # Returns
///
/// Orders that are submitted, held, or in flight for the account.
pub(super) fn working_orders(open: &[OrderData], account: &str) -> Vec<WorkingOrder> {
    let mut seen = HashSet::new();
    open.iter()
        .filter(|order| order.order.account == account)
        // A snapshot can repeat an order; the latest report wins below.
        .rev()
        .filter(|order| seen.insert((order.order.client_id, order.order_id, order.order.perm_id)))
        .filter(|order| {
            order.order_state.status.is_active()
                || order.order_state.status == OrderStatusKind::ApiPending
        })
        .map(|order| WorkingOrder {
            order_id: order.order_id,
            client_id: order.order.client_id,
            perm_id: order.order.perm_id,
            contract_id: order.contract.contract_id,
            contract: order.contract.clone(),
            parent_id: order.order.parent_id,
            sells: !matches!(order.order.action, Action::Buy),
            remaining: (order.order.total_quantity - order.order.filled_quantity.max(0.0)).max(0.0),
            status: order.order_state.status,
            order: order.order.clone(),
        })
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect()
}

/// A position and every working order that would reduce it.
#[derive(Clone, Debug)]
pub(super) struct ExitBook {
    /// Signed position: positive long, negative short, zero flat.
    pub(super) position: f64,
    /// This process's TWS client id.
    pub(super) client_id: i32,
    /// Every working order of the account, in any contract.
    pub(super) orders: Vec<WorkingOrder>,
    /// Working orders in the contract on the closing side, excluding bracket
    /// children whose parent is still working (they protect a future fill).
    pub(super) exits: Vec<WorkingOrder>,
}

impl ExitBook {
    /// Collect the exits of one position.
    ///
    /// # Arguments
    ///
    /// * `orders` - Every working order of the account.
    /// * `contract_id` - Contract of the position.
    /// * `position` - Signed position quantity.
    /// * `client_id` - This process's TWS client id.
    ///
    /// # Returns
    ///
    /// The book used to plan linked exits and cancellations.
    pub(super) fn new(
        orders: Vec<WorkingOrder>,
        contract_id: i32,
        position: f64,
        client_id: i32,
    ) -> Self {
        let exits = orders
            .iter()
            .filter(|order| order.contract_id == contract_id)
            .filter(|order| position.abs() > QUANTITY_EPSILON && order.sells == (position > 0.0))
            .filter(|order| order.remaining > QUANTITY_EPSILON)
            .filter(|order| !held_by_working_parent(order, &orders))
            .cloned()
            .collect();
        Self {
            position,
            client_id,
            orders,
            exits,
        }
    }

    fn position_quantity(&self) -> f64 {
        self.position.abs()
    }
}

/// Whether a bracket child is still held by its working parent.
fn held_by_working_parent(order: &WorkingOrder, orders: &[WorkingOrder]) -> bool {
    order.parent_id > 0
        && orders
            .iter()
            .any(|parent| parent.order_id == order.parent_id && parent.client_id == order.client_id)
}

/// Most the exits can close together.
///
/// Orders in one OCA group with block close at most the group's largest
/// order, because every fill reduces (type 2) or cancels (type 1) the rest
/// and only one is routed at a time. Every other order closes its own
/// remaining quantity.
fn exposure<'a>(orders: impl IntoIterator<Item = &'a WorkingOrder>) -> f64 {
    let mut groups: HashMap<(i32, &str), f64> = HashMap::new();
    let mut loose = 0.0;
    for order in orders {
        match order.link_key() {
            Some(key) => {
                let largest = groups.entry(key).or_insert(0.0);
                *largest = largest.max(order.remaining);
            }
            None => loose += order.remaining,
        }
    }
    loose + groups.values().sum::<f64>()
}

/// How a new linked exit is placed.
#[derive(Clone, Debug, PartialEq)]
pub(super) struct NewExitPlan {
    /// OCA group the exit joins.
    pub(super) group: String,
    /// Quantity to send, at most the position.
    pub(super) quantity: f64,
    /// Absolute position quantity the plan was made for.
    pub(super) position_quantity: f64,
    /// Existing exits already in `group` that stay beside the new one.
    pub(super) joined: Vec<WorkingOrder>,
    /// This client's exits to re-place into `group` before the new exit;
    /// their originals are cancelled afterwards.
    pub(super) relink: Vec<WorkingOrder>,
}

/// Decide where a new reduce-only exit goes, or refuse it.
///
/// The exit is clamped to the position. It then joins whichever of this
/// client's linked groups (or a fresh group) keeps the most the exits can
/// close together within the position. When no choice does, every exit this
/// client placed is re-placed into a fresh group with the new one, so the
/// group again closes at most the position; exits placed by TWS or another
/// client must leave room for that group on their own.
///
/// # Arguments
///
/// * `book` - The position and its exits.
/// * `sells` - Whether the new exit sells.
/// * `requested` - Requested quantity.
/// * `fresh_group` - Unused group name for a new group.
///
/// # Returns
///
/// The placement plan.
///
/// # Errors
///
/// Returns a user-facing reason when there is no position to close, the
/// order would add to the position, or existing exits cannot be linked.
pub(super) fn plan_new_exit(
    book: &ExitBook,
    sells: bool,
    requested: f64,
    fresh_group: &str,
) -> Result<NewExitPlan, String> {
    let position_quantity = book.position_quantity();
    if position_quantity <= QUANTITY_EPSILON {
        return Err(
            "IBKR reports no position in this contract for the account, so a reduce-only exit has nothing to close"
                .to_string(),
        );
    }
    let long = book.position > 0.0;
    if sells != long {
        return Err(format!(
            "a reduce-only {} would add to the {} position instead of closing it",
            if sells { "sell" } else { "buy" },
            if long { "long" } else { "short" }
        ));
    }
    let quantity = requested.min(position_quantity);
    let current = exposure(&book.exits);
    let fits = |closable: f64| closable <= position_quantity + QUANTITY_EPSILON;

    // This client's linked groups the exit could join, best first: the
    // smallest combined exposure, then the group with the most members.
    let mut groups: HashMap<&str, Vec<&WorkingOrder>> = HashMap::new();
    for exit in book
        .exits
        .iter()
        .filter(|exit| exit.in_own_linked_group(book.client_id))
    {
        groups
            .entry(exit.order.oca_group.as_str())
            .or_default()
            .push(exit);
    }
    let best_join = groups
        .into_iter()
        .map(|(group, members)| {
            let largest = members
                .iter()
                .map(|member| member.remaining)
                .fold(0.0, f64::max);
            let closable = current - largest + largest.max(quantity);
            (closable, group, members)
        })
        .min_by(|left, right| {
            left.0
                .total_cmp(&right.0)
                .then(right.2.len().cmp(&left.2.len()))
                .then(left.1.cmp(right.1))
        });
    let fresh_closable = current + quantity;
    match best_join {
        Some((closable, group, members)) if fits(closable) && closable <= fresh_closable => {
            return Ok(NewExitPlan {
                group: group.to_string(),
                quantity,
                position_quantity,
                joined: members.into_iter().cloned().collect(),
                relink: Vec::new(),
            });
        }
        _ if fits(fresh_closable) => {
            return Ok(NewExitPlan {
                group: fresh_group.to_string(),
                quantity,
                position_quantity,
                joined: Vec::new(),
                relink: Vec::new(),
            });
        }
        _ => {}
    }

    let (own, foreign): (Vec<&WorkingOrder>, Vec<&WorkingOrder>) = book
        .exits
        .iter()
        .partition(|exit| exit.client_id == book.client_id);
    for exit in &own {
        relinkable(exit, position_quantity)?;
        // Cancelling the original cancels its whole group, so the group may
        // hold nothing but exits that are re-placed with it.
        if let Some(other) = book.orders.iter().find(|other| {
            !exit.order.oca_group.is_empty()
                && other.client_id == exit.client_id
                && other.order.oca_group == exit.order.oca_group
                && !own.iter().any(|own| own.order_id == other.order_id)
        }) {
            return Err(format!(
                "exit order {} is linked with order {} that is not an exit of this position; cancel or change them in IBKR first",
                exit.describe(),
                other.describe()
            ));
        }
    }
    if own.len() > MAX_RELINKED_EXITS {
        return Err(format!(
            "this position has {} exit orders to link; cancel some in IBKR first (at most {MAX_RELINKED_EXITS} are re-placed at once)",
            own.len()
        ));
    }
    let linked = own
        .iter()
        .map(|exit| exit.remaining)
        .fold(quantity, f64::max);
    let foreign_closable = exposure(foreign.iter().copied());
    if !fits(foreign_closable + linked) {
        let names = describe_orders(foreign.iter().copied());
        return Err(format!(
            "exit orders {names} were placed by TWS or another app, so Sunk cannot link them; with them this exit could close more than the {} position. Cancel or reduce them in IBKR first",
            format_quantity(position_quantity)
        ));
    }
    Ok(NewExitPlan {
        group: fresh_group.to_string(),
        quantity,
        position_quantity,
        joined: Vec::new(),
        relink: own.into_iter().cloned().collect(),
    })
}

/// Require that an exit this client placed can be re-placed exactly.
fn relinkable(exit: &WorkingOrder, position_quantity: f64) -> Result<(), String> {
    let name = exit.describe();
    if !matches!(
        exit.status,
        OrderStatusKind::Submitted | OrderStatusKind::PreSubmitted
    ) {
        return Err(format!(
            "exit order {name} is changing state at IBKR ({}); try again in a moment",
            exit.status.as_str()
        ));
    }
    if exit.remaining > position_quantity + QUANTITY_EPSILON {
        return Err(format!(
            "exit order {name} is larger than the {} position; cancel or reduce it in IBKR first",
            format_quantity(position_quantity)
        ));
    }
    let order_type = exit.order.order_type.as_str();
    let positive = |price: Option<f64>| price.is_some_and(|price| price.is_finite() && price > 0.0);
    let priced = match order_type {
        "LMT" => positive(exit.order.limit_price),
        "STP" => positive(exit.order.aux_price),
        "STP LMT" => positive(exit.order.limit_price) && positive(exit.order.aux_price),
        _ => false,
    };
    if !RELINKABLE_ORDER_TYPES.contains(&order_type) || !priced {
        return Err(format!(
            "exit order {name} cannot be re-placed exactly; cancel it in IBKR or place this exit there"
        ));
    }
    if !matches!(
        exit.order.tif,
        TimeInForce::GoodTilCanceled | TimeInForce::Day
    ) {
        return Err(format!(
            "exit order {name} has a time in force Sunk does not re-place ({}); cancel it in IBKR first",
            exit.order.tif
        ));
    }
    Ok(())
}

/// How cancelling one order keeps the rest of its group.
#[derive(Clone, Debug, PartialEq)]
pub(super) enum CancelPlan {
    /// Cancel as IBKR does. `cascades` are linked orders IBKR cancels with it
    /// that protect nothing yet (children of a working bracket parent) or
    /// would no longer close anything (no position is held).
    Plain {
        /// Linked orders IBKR cancels together with the target.
        cascades: Vec<WorkingOrder>,
    },
    /// Re-place the target's sibling exits into `group`, then cancel the
    /// target, which cancels the old group.
    Relink {
        /// Fresh OCA group the siblings move to.
        group: String,
        /// Sibling exits to re-place.
        relink: Vec<WorkingOrder>,
    },
    /// Do not cancel: a sibling that protects the position cannot be
    /// re-placed. The text says why.
    Refused(String),
}

/// Decide how to cancel one order without losing its linked exits.
///
/// # Arguments
///
/// * `book` - The target's position and the account's working orders.
/// * `target` - Order id to cancel, owned by this client.
/// * `fresh_group` - Unused group name for re-placed siblings.
///
/// # Returns
///
/// The cancellation plan.
///
/// # Errors
///
/// Returns a user-facing reason when a sibling that protects the position
/// cannot be re-placed.
pub(super) fn plan_cancel(
    book: &ExitBook,
    target: i32,
    fresh_group: &str,
) -> Result<CancelPlan, String> {
    let Some(order) = book
        .orders
        .iter()
        .find(|order| order.order_id == target && order.client_id == book.client_id)
    else {
        return Ok(CancelPlan::Plain {
            cascades: Vec::new(),
        });
    };
    let group = order.order.oca_group.as_str();
    if group.is_empty() {
        return Ok(CancelPlan::Plain {
            cascades: Vec::new(),
        });
    }
    let siblings: Vec<&WorkingOrder> = book
        .orders
        .iter()
        .filter(|other| {
            other.client_id == order.client_id
                && other.order.oca_group == group
                && other.order_id != order.order_id
        })
        .collect();
    let protecting: Vec<&WorkingOrder> = siblings
        .iter()
        .copied()
        .filter(|sibling| {
            book.exits.iter().any(|exit| {
                exit.order_id == sibling.order_id && exit.client_id == sibling.client_id
            })
        })
        .collect();
    if protecting.is_empty() {
        return Ok(CancelPlan::Plain {
            cascades: siblings.into_iter().cloned().collect(),
        });
    }
    if siblings.len() != protecting.len() {
        let names = describe_orders(
            siblings
                .iter()
                .copied()
                .filter(|sibling| !protecting.contains(sibling)),
        );
        return Err(format!(
            "cancelling it would also cancel linked orders {names} that Sunk cannot re-place; cancel the group in IBKR instead"
        ));
    }
    if protecting.len() > MAX_RELINKED_EXITS {
        return Err(format!(
            "it is linked with {} exit orders; cancel it in IBKR (at most {MAX_RELINKED_EXITS} are re-placed at once)",
            protecting.len()
        ));
    }
    let position_quantity = book.position_quantity();
    for sibling in &protecting {
        relinkable(sibling, position_quantity)?;
    }
    Ok(CancelPlan::Relink {
        group: fresh_group.to_string(),
        relink: protecting.into_iter().cloned().collect(),
    })
}

/// Build the copy of an exit that is re-placed into a linked group.
///
/// Only fields this module verified ([`relinkable`]) are copied: side,
/// remaining quantity, order type, limit and stop price, time in force,
/// outside-RTH, and the original `order_ref`, so the copy keeps the
/// reference the terminal journaled for it. The copy has no bracket parent;
/// a re-placed bracket child's parent has already filled.
///
/// # Arguments
///
/// * `original` - Exit being re-placed.
/// * `account` - Selected account.
/// * `group` - OCA group the copy joins.
///
/// # Returns
///
/// The order to transmit.
pub(super) fn relinked_copy(original: &WorkingOrder, account: &str, group: &str) -> Order {
    // An open-order report can carry a value in the price field an order
    // type does not use; send only the prices the type needs.
    let order_type = original.order.order_type.as_str();
    let limit_price = matches!(order_type, "LMT" | "STP LMT")
        .then_some(original.order.limit_price)
        .flatten();
    let aux_price = matches!(order_type, "STP" | "STP LMT")
        .then_some(original.order.aux_price)
        .flatten();
    Order {
        action: if original.sells {
            Action::Sell
        } else {
            Action::Buy
        },
        total_quantity: original.remaining,
        order_type: original.order.order_type.clone(),
        limit_price,
        aux_price,
        tif: original.order.tif.clone(),
        outside_rth: original.order.outside_rth,
        // An Adaptive limit exit stays Adaptive in its new group.
        algo_strategy: original.order.algo_strategy.clone(),
        algo_params: original.order.algo_params.clone(),
        account: account.to_string(),
        order_ref: original.order.order_ref.clone(),
        oca_group: group.to_string(),
        oca_type: LINKED_EXIT_OCA_TYPE,
        transmit: true,
        override_percentage_constraints: false,
        advanced_error_override: String::new(),
        ..Default::default()
    }
}

/// Put an exit order into a linked group.
///
/// # Arguments
///
/// * `order` - The exit as built for submission.
/// * `group` - OCA group it joins.
/// * `quantity` - Quantity after clamping to the position.
///
/// # Returns
///
/// The order with its OCA fields and quantity set.
pub(super) fn linked_exit_order(mut order: Order, group: &str, quantity: f64) -> Order {
    order.total_quantity = quantity;
    order.cash_qty = None;
    order.oca_group = group.to_string();
    order.oca_type = LINKED_EXIT_OCA_TYPE;
    order
}

/// IBKR's answer to placing one re-placed copy.
#[derive(Clone, Debug, PartialEq)]
pub(super) enum CopyOutcome {
    /// The copy is working.
    Working,
    /// The copy filled at least partly, so the position changed.
    Filled,
    /// IBKR rejected or cancelled the copy.
    Rejected(String),
    /// IBKR did not confirm the copy's state in time.
    Unknown(String),
}

/// Read a copy's event stream until IBKR confirms it working, filled or rejected.
///
/// A copy carries its original's `order_ref`, so a follow-up read matches
/// it by order id among this client's open orders, never by reference.
/// `PendingSubmit` is not yet a confirmation.
///
/// # Arguments
///
/// * `events` - The copy's event stream.
/// * `order_id` - The copy's order id.
/// * `deadline` - When to stop reading.
/// * `read_open` - Reads IBKR's open orders, already filtered to this copy.
///
/// # Returns
///
/// The copy's outcome.
pub(super) async fn confirm_copy<S, R, Fut>(
    mut events: S,
    order_id: i32,
    deadline: Instant,
    mut read_open: R,
) -> CopyOutcome
where
    S: Stream<Item = Result<SubscriptionItem<PlaceOrder>, ibapi::Error>> + Unpin,
    R: FnMut() -> Fut,
    Fut: Future<Output = Result<Vec<OrderData>, super::GatewayError>>,
{
    let mut needs_read = false;
    let streamed = tokio::time::timeout_at(deadline, async {
        while let Some(item) = events.next().await {
            let (notice, ended) = match item {
                Ok(SubscriptionItem::Data(event)) => match copy_event_outcome(order_id, &event) {
                    Some(outcome) => return Some(outcome),
                    None => continue,
                },
                Ok(SubscriptionItem::Notice(notice)) => (notice, false),
                Err(ibapi::Error::Notice(notice)) => (notice, true),
                Err(error) => {
                    return Some(CopyOutcome::Unknown(format!(
                        "the copy's event stream failed: {error}"
                    )));
                }
            };
            match classify_order_message(notice.code, &notice.message) {
                OrderMessage::Rejected(text) => {
                    return Some(CopyOutcome::Rejected(format!("[{}] {text}", notice.code)));
                }
                OrderMessage::FollowUp(note) if ended || note.is_cancellation() => {
                    needs_read = true;
                    if ended {
                        return None;
                    }
                }
                OrderMessage::FollowUp(_) => {}
                OrderMessage::Unknown if ended => {
                    return Some(CopyOutcome::Unknown(format!(
                        "IBKR ended the copy's stream with [{}] {}",
                        notice.code,
                        super::order_messages::single_line(&notice.message)
                    )));
                }
                OrderMessage::Unknown => {}
            }
        }
        None
    })
    .await;
    drop(events);
    match streamed {
        Ok(Some(outcome)) => return outcome,
        Ok(None) | Err(_) if !needs_read => {
            return CopyOutcome::Unknown(
                "IBKR did not confirm the copy before the deadline".to_string(),
            );
        }
        Ok(None) | Err(_) => {}
    }
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return CopyOutcome::Unknown(
                "IBKR did not list the copy among open orders before the deadline".to_string(),
            );
        }
        tokio::time::sleep(VERIFY_RETRY_DELAY.min(remaining)).await;
        if let Ok(Ok(orders)) = tokio::time::timeout_at(deadline, read_open()).await
            && let Some(found) = orders.iter().find(|order| order.order_id == order_id)
        {
            if found.order.filled_quantity > 0.0 {
                return CopyOutcome::Filled;
            }
            match found.order_state.status {
                OrderStatusKind::Submitted | OrderStatusKind::PreSubmitted => {
                    return CopyOutcome::Working;
                }
                OrderStatusKind::Filled => return CopyOutcome::Filled,
                OrderStatusKind::Cancelled
                | OrderStatusKind::ApiCancelled
                | OrderStatusKind::Inactive => {
                    return CopyOutcome::Rejected(
                        "IBKR cancelled or deactivated the copy".to_string(),
                    );
                }
                _ => {}
            }
        }
    }
}

/// Map one provider event for a copy to its outcome, if it decides one.
fn copy_event_outcome(order_id: i32, event: &PlaceOrder) -> Option<CopyOutcome> {
    let (status, filled) = match event {
        PlaceOrder::OrderStatus(status) if status.order_id == order_id => {
            (status.status, status.filled)
        }
        PlaceOrder::OpenOrder(order) if order.order_id == order_id => {
            (order.order_state.status, order.order.filled_quantity)
        }
        PlaceOrder::ExecutionData(execution) if execution.execution.order_id == order_id => {
            return Some(CopyOutcome::Filled);
        }
        _ => return None,
    };
    if filled > 0.0 {
        return Some(CopyOutcome::Filled);
    }
    match status {
        OrderStatusKind::Submitted | OrderStatusKind::PreSubmitted => Some(CopyOutcome::Working),
        OrderStatusKind::Filled => Some(CopyOutcome::Filled),
        OrderStatusKind::Cancelled | OrderStatusKind::ApiCancelled | OrderStatusKind::Inactive => {
            Some(CopyOutcome::Rejected(
                "IBKR cancelled or deactivated the copy".to_string(),
            ))
        }
        OrderStatusKind::PendingSubmit
        | OrderStatusKind::ApiPending
        | OrderStatusKind::PendingCancel => None,
    }
}

/// What became of orders that were cancelled, read back from IBKR.
#[derive(Clone, Debug, Default, PartialEq)]
pub(super) struct CancelCheck {
    /// Orders still working.
    pub(super) still_open: Vec<i32>,
    /// Orders IBKR completed as filled instead of cancelled.
    pub(super) filled: Vec<i32>,
}

/// Check cancelled orders against IBKR's open and completed orders.
///
/// Open orders are matched by order id and client; completed orders carry
/// no order id, so they are matched by permanent id.
///
/// # Arguments
///
/// * `cancelled` - Orders that should no longer be working.
/// * `orders` - IBKR's open and completed orders.
///
/// # Returns
///
/// The orders still working and those that filled.
pub(super) fn check_cancelled(cancelled: &[WorkingOrder], orders: &ProviderOrders) -> CancelCheck {
    let mut check = CancelCheck::default();
    for order in cancelled {
        let open = orders.open.iter().any(|open| {
            open.order_id == order.order_id
                && open.order.client_id == order.client_id
                && open.order_state.status != OrderStatusKind::Cancelled
                && open.order_state.status != OrderStatusKind::ApiCancelled
                && open.order_state.status != OrderStatusKind::Inactive
        });
        let filled = orders.completed.iter().any(|done| {
            order.perm_id != 0
                && done.order.perm_id == order.perm_id
                && (done.order_state.status == OrderStatusKind::Filled
                    || done.order.filled_quantity > 0.0)
        });
        if open {
            check.still_open.push(order.order_id);
        }
        if filled {
            check.filled.push(order.order_id);
        }
    }
    check
}

/// Time left before `deadline`, capped at `cap`.
pub(super) fn budget_left(deadline: Instant, cap: Duration) -> Duration {
    deadline.saturating_duration_since(Instant::now()).min(cap)
}

/// Share of [`LINKED_EXIT_BUDGET`] for placing and confirming copies.
const COPIES_PHASE: Duration = Duration::from_millis(3_500);
/// Share of [`LINKED_EXIT_BUDGET`] for cancelling originals and reading them
/// back, after the copies phase. The new exit gets the rest (at least five
/// seconds), of which [`EXIT_FOLLOW_UP_RESERVE`] is kept for a follow-up read.
const CANCELS_PHASE: Duration = Duration::from_millis(3_500);
/// Part of the exit's time kept for reading back an exit IBKR holds (399).
const EXIT_FOLLOW_UP_RESERVE: Duration = Duration::from_secs(2);

/// Provider handles shared by every step of one linked operation.
pub(super) struct LinkedSession<'a> {
    /// Connected TWS client; copies and cancellations use its order ids.
    client: &'a Arc<Client>,
    /// Selected account.
    account: &'a str,
    /// This process's TWS client id.
    client_id: i32,
    /// Serializes snapshot reads with the rest of the Gateway.
    snapshots: &'a Mutex<()>,
    /// When the operation's budget started; phases are measured from here.
    started: Instant,
    /// End of the operation's budget.
    deadline: Instant,
}

/// Copies placed so far, and the copy that stopped placement early, if any.
struct PlacedCopies {
    /// Each original with the order id of its copy.
    placed: Vec<(WorkingOrder, i32)>,
    /// The original whose copy did not come up working, with its outcome.
    stopped: Option<(WorkingOrder, CopyOutcome)>,
}

impl PlacedCopies {
    fn copy_ids(&self) -> Vec<i32> {
        self.placed.iter().map(|(_, copy)| *copy).collect()
    }

    fn originals(&self) -> Vec<WorkingOrder> {
        self.placed
            .iter()
            .map(|(original, _)| original.clone())
            .collect()
    }

    fn relinked(&self) -> Vec<RelinkedOrder> {
        self.placed
            .iter()
            .map(|(original, copy)| RelinkedOrder {
                from_order_id: original.order_id.to_string(),
                to_order_id: copy.to_string(),
            })
            .collect()
    }
}

/// What cancelling re-placed originals left behind.
#[derive(Debug, Default)]
struct Retired {
    /// Problems to report.
    warnings: Vec<String>,
    /// Whether every original is confirmed gone without a fill.
    clean: bool,
}

impl<'a> LinkedSession<'a> {
    /// Start one linked operation's budget now.
    ///
    /// # Arguments
    ///
    /// * `client` - Connected TWS client of this process.
    /// * `account` - Selected account.
    /// * `client_id` - This process's TWS client id.
    /// * `snapshots` - The Gateway's snapshot-read lock.
    /// * `budget` - Time the operation may take ([`LINKED_EXIT_BUDGET`]).
    ///
    /// # Returns
    ///
    /// The session.
    pub(super) fn new(
        client: &'a Arc<Client>,
        account: &'a str,
        client_id: i32,
        snapshots: &'a Mutex<()>,
        budget: Duration,
    ) -> Self {
        let started = Instant::now();
        Self {
            client,
            account,
            client_id,
            snapshots,
            started,
            deadline: started + budget,
        }
    }
}

impl LinkedSession<'_> {
    fn copies_deadline(&self) -> Instant {
        (self.started + COPIES_PHASE).min(self.deadline)
    }

    fn cancels_deadline(&self) -> Instant {
        (self.started + COPIES_PHASE + CANCELS_PHASE).min(self.deadline)
    }

    /// Read IBKR's open orders from every client.
    async fn open_orders(&self) -> Result<Vec<OrderData>, GatewayError> {
        let _snapshots = self.snapshots.lock().await;
        collect_order_snapshot(self.client, OrderSnapshot::AllOpen).await
    }

    /// Read IBKR's open and completed orders.
    async fn provider_orders(&self) -> Result<ProviderOrders, GatewayError> {
        let _snapshots = self.snapshots.lock().await;
        read_provider_orders(self.client).await
    }

    /// Place a copy of each exit into `group`, in order, until one fails.
    async fn place_copies(
        &self,
        relink: &[WorkingOrder],
        group: &str,
        first_order_id: i32,
    ) -> PlacedCopies {
        let mut placed = Vec::new();
        for (index, original) in relink.iter().enumerate() {
            let order_id = first_order_id + index as i32;
            let outcome = self.place_copy(original, group, order_id).await;
            match outcome {
                CopyOutcome::Working => placed.push((original.clone(), order_id)),
                CopyOutcome::Rejected(_) => {
                    return PlacedCopies {
                        placed,
                        stopped: Some((original.clone(), outcome)),
                    };
                }
                // A filled or unconfirmed copy may be live: keep it so a
                // rollback or the originals' cancellation accounts for it.
                CopyOutcome::Filled | CopyOutcome::Unknown(_) => {
                    placed.push((original.clone(), order_id));
                    return PlacedCopies {
                        placed,
                        stopped: Some((original.clone(), outcome)),
                    };
                }
            }
        }
        PlacedCopies {
            placed,
            stopped: None,
        }
    }

    /// Transmit one copy and wait until IBKR confirms it.
    async fn place_copy(&self, original: &WorkingOrder, group: &str, order_id: i32) -> CopyOutcome {
        let phase_end = self.copies_deadline();
        let copy = relinked_copy(original, self.account, group);
        let send_deadline = phase_end.min(Instant::now() + ORDER_SUBMISSION_TIMEOUT);
        let events = match tokio::time::timeout_at(
            send_deadline,
            self.client.place_order(order_id, &original.contract, &copy),
        )
        .await
        {
            Ok(Ok(events)) => events,
            Ok(Err(error)) => {
                return CopyOutcome::Unknown(format!(
                    "the copy's submission transport failed: {error}"
                ));
            }
            Err(_) => return CopyOutcome::Unknown("the copy's submission timed out".to_string()),
        };
        let client_id = self.client_id;
        confirm_copy(events, order_id, phase_end, || async move {
            Ok(self
                .open_orders()
                .await?
                .into_iter()
                .filter(|order| order.order_id == order_id && order.order.client_id == client_id)
                .collect())
        })
        .await
    }

    /// Send every cancellation first, then wait for each answer.
    async fn cancel_all(&self, order_ids: &[i32], deadline: Instant) -> Vec<(i32, CancelOutcome)> {
        let mut outcomes = Vec::new();
        let mut sent = Vec::new();
        for order_id in order_ids {
            match send_cancel(self.client, *order_id, deadline).await {
                Ok(events) => sent.push((*order_id, events)),
                Err(outcome) => outcomes.push((*order_id, outcome)),
            }
        }
        outcomes.extend(
            futures::future::join_all(sent.into_iter().map(|(order_id, events)| async move {
                (order_id, confirm_cancel(events, deadline).await)
            }))
            .await,
        );
        outcomes
    }

    /// Wait until none of this client's `order_ids` is working.
    ///
    /// Orders still listed after the first read are cancelled again.
    ///
    /// # Errors
    ///
    /// Returns a user-facing sentence when IBKR still lists an order at the deadline.
    async fn confirm_gone(&self, order_ids: &[i32], deadline: Instant) -> Result<(), String> {
        let mut retried = false;
        let mut still_open = order_ids.to_vec();
        while Instant::now() < deadline {
            if let Ok(Ok(open)) = tokio::time::timeout_at(deadline, self.open_orders()).await {
                still_open = order_ids
                    .iter()
                    .copied()
                    .filter(|order_id| {
                        open.iter().any(|order| {
                            order.order_id == *order_id
                                && order.order.client_id == self.client_id
                                && (order.order_state.status.is_active()
                                    || order.order_state.status == OrderStatusKind::ApiPending)
                        })
                    })
                    .collect();
                if still_open.is_empty() {
                    return Ok(());
                }
                if !retried {
                    retried = true;
                    self.cancel_all(&still_open, deadline).await;
                    continue;
                }
            }
            tokio::time::sleep(
                VERIFY_RETRY_DELAY.min(deadline.saturating_duration_since(Instant::now())),
            )
            .await;
        }
        Err(format!(
            "IBKR did not confirm that orders {} were cancelled; check IBKR open orders now",
            order_names(still_open.iter().copied())
        ))
    }

    /// Cancel re-placed copies again; their fresh group holds nothing else.
    async fn roll_back(&self, copy_ids: &[i32], deadline: Instant) -> Result<(), String> {
        if copy_ids.is_empty() {
            return Ok(());
        }
        self.cancel_all(copy_ids, deadline).await;
        self.confirm_gone(copy_ids, deadline).await
    }

    /// Roll back copies and describe the result as a sentence suffix.
    async fn roll_back_note(&self, copy_ids: &[i32], deadline: Instant) -> String {
        if copy_ids.is_empty() {
            return String::new();
        }
        match self.roll_back(copy_ids, deadline).await {
            Ok(()) => {
                ". The re-placed copies were cancelled again, so the earlier exits are unchanged"
                    .to_string()
            }
            Err(problem) => format!(
                ". Warning: the re-placed copies {} may still be working beside the earlier exits: {problem}",
                order_names(copy_ids.iter().copied())
            ),
        }
    }

    /// Cancel the originals of placed copies and read back that they are gone.
    ///
    /// Every cancellation is sent before any answer is awaited. One member
    /// per OCA group is cancelled, because IBKR cancels the rest of the
    /// group with it; `members` lists every original those groups hold, so
    /// any member that was not copied is reported as cancelled with it.
    async fn retire_originals(
        &self,
        originals: &[WorkingOrder],
        members: &[WorkingOrder],
        deadline: Instant,
    ) -> Retired {
        let mut retired = Retired::default();
        let mut groups = HashSet::new();
        let mut targets = Vec::new();
        for original in originals {
            if original.order.oca_group.is_empty()
                || groups.insert((original.client_id, original.order.oca_group.clone()))
            {
                targets.push(original.order_id);
            }
        }
        let dropped: Vec<i32> = members
            .iter()
            .filter(|member| {
                !member.order.oca_group.is_empty()
                    && groups.contains(&(member.client_id, member.order.oca_group.clone()))
                    && !originals
                        .iter()
                        .any(|original| original.order_id == member.order_id)
            })
            .map(|member| member.order_id)
            .collect();
        if !dropped.is_empty() {
            retired.warnings.push(format!(
                "exit orders {} could not be re-placed and IBKR cancelled them with their OCA group; arm them again",
                order_names(dropped.iter().copied())
            ));
        }
        for (order_id, outcome) in self.cancel_all(&targets, deadline).await {
            if outcome == CancelOutcome::Filled {
                retired.warnings.push(filled_warning(order_id));
            }
        }
        let mut retried = false;
        while Instant::now() < deadline {
            if let Ok(Ok(orders)) = tokio::time::timeout_at(deadline, self.provider_orders()).await
            {
                let check = check_cancelled(originals, &orders);
                for filled in &check.filled {
                    let warning = filled_warning(*filled);
                    if !retired.warnings.contains(&warning) {
                        retired.warnings.push(warning);
                    }
                }
                if check.still_open.is_empty() {
                    retired.clean = check.filled.is_empty() && dropped.is_empty();
                    return retired;
                }
                if !retried {
                    retried = true;
                    self.cancel_all(&check.still_open, deadline).await;
                    continue;
                }
            }
            tokio::time::sleep(
                VERIFY_RETRY_DELAY.min(deadline.saturating_duration_since(Instant::now())),
            )
            .await;
        }
        retired.warnings.push(format!(
            "IBKR did not confirm that earlier exit orders {} were cancelled; they may still be working beside their re-placed copies. Check IBKR open orders now",
            order_names(originals.iter().map(|original| original.order_id))
        ));
        retired
    }

    /// Re-read the position and its exits after a relink went wrong.
    ///
    /// # Returns
    ///
    /// A warning when the exits could now close more than the position, or
    /// when the state could not be read back.
    async fn recheck(&self, contract_id: i32, placed: &[i32]) -> Option<String> {
        let read = async {
            let position = {
                let _snapshots = self.snapshots.lock().await;
                read_position(self.client, self.account, contract_id).await?
            };
            let open = self.open_orders().await?;
            Ok::<_, GatewayError>((position, open))
        };
        let Ok(Ok((position, open))) = tokio::time::timeout_at(self.deadline, read).await else {
            return Some(
                "Sunk could not re-read the position's exits; check IBKR open orders".to_string(),
            );
        };
        let working = working_orders(&open, self.account);
        if position.abs() <= QUANTITY_EPSILON {
            let left = working
                .iter()
                .filter(|order| {
                    order.client_id == self.client_id && placed.contains(&order.order_id)
                })
                .map(|order| order.order_id)
                .collect::<Vec<_>>();
            return (!left.is_empty()).then(|| {
                format!(
                    "the position is now closed but orders {} are still working and would open a new position; cancel them in IBKR now",
                    order_names(left.iter().copied())
                )
            });
        }
        let book = ExitBook::new(working, contract_id, position, self.client_id);
        let closable = exposure(&book.exits);
        (closable > position.abs() + QUANTITY_EPSILON).then(|| {
            format!(
                "IBKR now lists exit orders {} that together could close {} of the {} position; cancel or reduce the extra ones in IBKR now",
                describe_orders(&book.exits),
                format_quantity(closable),
                format_quantity(position.abs())
            )
        })
    }

    /// Place a reduce-only exit according to `plan`.
    ///
    /// When earlier exits must be re-placed, their copies are placed and
    /// confirmed first, then the originals are cancelled and read back, and
    /// only then is the exit placed, so a copy and its original are both live
    /// only while the cancellations travel. If a copy is not accepted, the
    /// copies are cancelled again and nothing else changes.
    ///
    /// # Arguments
    ///
    /// * `contract` - Resolved contract of the exit.
    /// * `request` - The validated reduce-only request.
    /// * `plan` - Where the exit goes.
    /// * `first_order_id` - Next valid order id; copies use the ids before the exit's.
    ///
    /// # Returns
    ///
    /// The exit's outcome with the linked-exit details when IBKR accepted it.
    pub(super) async fn submit_exit(
        &self,
        contract: &Contract,
        request: &OrderRequest,
        plan: NewExitPlan,
        first_order_id: i32,
    ) -> OrderResponse {
        bounded(
            self.submit_exit_unbounded(contract, request, plan, first_order_id)
                .await,
        )
    }

    async fn submit_exit_unbounded(
        &self,
        contract: &Contract,
        request: &OrderRequest,
        plan: NewExitPlan,
        first_order_id: i32,
    ) -> OrderResponse {
        let customer_order_id = request.customer_order_id.as_str();
        let mut warnings = Vec::new();
        let mut copies = PlacedCopies {
            placed: Vec::new(),
            stopped: None,
        };
        if !plan.relink.is_empty() {
            copies = self
                .place_copies(&plan.relink, &plan.group, first_order_id)
                .await;
            let copy_ids = copies.copy_ids();
            match &copies.stopped {
                Some((original, CopyOutcome::Rejected(reason) | CopyOutcome::Unknown(reason))) => {
                    let note = self
                        .roll_back_note(&copy_ids, self.cancels_deadline())
                        .await;
                    return not_placed(
                        self.account,
                        customer_order_id,
                        &format!(
                            "IBKR exit was not placed: IBKR did not confirm the re-placed copy of exit order #{} ({reason}){}",
                            original.order_id,
                            if note.is_empty() {
                                ". The earlier exits were left as they were".to_string()
                            } else {
                                note
                            }
                        ),
                    );
                }
                Some((original, CopyOutcome::Filled)) => {
                    // A copy executed and the position changed. Its original
                    // and the other copied originals are now duplicates beside
                    // linked copies, so they go; no new exit is placed.
                    let retired = self
                        .retire_originals(
                            &copies.originals(),
                            &plan.relink,
                            self.cancels_deadline(),
                        )
                        .await;
                    let mut message = format!(
                        "IBKR exit was not placed: IBKR filled the re-placed copy of exit order #{} while linking this position's exits, so the position changed. Check the position before placing another exit",
                        original.order_id
                    );
                    for warning in retired
                        .warnings
                        .into_iter()
                        .chain(self.recheck(contract.contract_id, &copy_ids).await)
                    {
                        message.push_str(&format!(". Warning: {warning}"));
                    }
                    return not_placed(self.account, customer_order_id, &message);
                }
                Some((_, CopyOutcome::Working)) | None => {}
            }
            let retired = self
                .retire_originals(&copies.originals(), &plan.relink, self.cancels_deadline())
                .await;
            if !retired.clean {
                // An original filled or may still be working: the exits may
                // already exceed the position, so the new exit is not added.
                let mut message = format!(
                    "IBKR exit was not placed: the earlier exit orders {} were re-placed as {} in IBKR OCA group {}, but not every original is confirmed cancelled",
                    order_names(copies.placed.iter().map(|(original, _)| original.order_id)),
                    order_names(copy_ids.iter().copied()),
                    plan.group
                );
                for warning in retired
                    .warnings
                    .into_iter()
                    .chain(self.recheck(contract.contract_id, &copy_ids).await)
                {
                    message.push_str(&format!(". Warning: {warning}"));
                }
                return not_placed(self.account, customer_order_id, &message);
            }
            warnings.extend(retired.warnings);
        }

        let exit_order_id = first_order_id + plan.relink.len() as i32;
        let order = linked_exit_order(
            build_order(request, self.account, customer_order_id),
            &plan.group,
            plan.quantity,
        );
        let send_deadline = self.deadline.min(Instant::now() + ORDER_SUBMISSION_TIMEOUT);
        let events = match tokio::time::timeout_at(
            send_deadline,
            self.client.place_order(exit_order_id, contract, &order),
        )
        .await
        {
            Ok(Ok(events)) => events,
            Ok(Err(error)) => {
                return indeterminate_response(
                    self.account.to_string(),
                    Some(customer_order_id.to_string()),
                    &format!("the order submission transport failed: {error}"),
                );
            }
            Err(_) => {
                return indeterminate_response(
                    self.account.to_string(),
                    Some(customer_order_id.to_string()),
                    "the order submission call timed out",
                );
            }
        };
        let left = budget_left(
            self.deadline,
            ORDER_ACKNOWLEDGEMENT_TIMEOUT + ORDER_FOLLOW_UP_TIMEOUT,
        );
        let follow_up = EXIT_FOLLOW_UP_RESERVE
            .min(left / 2)
            .max(left.saturating_sub(ORDER_ACKNOWLEDGEMENT_TIMEOUT));
        let mut outcome = single_order_outcome(
            events,
            SubmittedOrder {
                order_id: exit_order_id,
                account: self.account,
                customer_order_id,
            },
            OutcomeDeadlines {
                acknowledgement: left.saturating_sub(follow_up),
                follow_up,
            },
            || self.provider_orders(),
        )
        .await;
        let relinked_note = (!copies.placed.is_empty()).then(|| {
            format!(
                "The earlier exit orders {} were re-placed as {} in IBKR OCA group {}",
                order_names(copies.placed.iter().map(|(original, _)| original.order_id)),
                order_names(copies.copy_ids().into_iter()),
                plan.group
            )
        });
        if !outcome.ok {
            if let Some(note) = relinked_note {
                outcome.message = format!("{}. {note}", outcome.message);
            }
            return outcome;
        }
        let linked_order_ids: Vec<String> = if copies.placed.is_empty() {
            plan.joined
                .iter()
                .take(MAX_LINKED_ORDER_IDS)
                .map(|order| order.order_id.to_string())
                .collect()
        } else {
            copies.copy_ids().iter().map(ToString::to_string).collect()
        };
        let mut sentences = vec![match relinked_note {
            Some(note) => note,
            None if linked_order_ids.is_empty() => {
                format!(
                    "Started IBKR OCA group {} for this position's exits",
                    plan.group
                )
            }
            None => format!(
                "Linked with exit orders {} in IBKR OCA group {}",
                order_names(plan.joined.iter().map(|order| order.order_id)),
                plan.group
            ),
        }];
        if let Some(requested) = request.quantity
            && requested > plan.quantity + QUANTITY_EPSILON
        {
            sentences.push(format!(
                "Quantity reduced from {} to the {} position",
                format_quantity(requested),
                format_quantity(plan.position_quantity)
            ));
        }
        for warning in &warnings {
            sentences.push(format!("Warning: {warning}"));
        }
        outcome.message = format!("{}. {}", outcome.message, sentences.join(". "));
        outcome.exit = Some(LinkedExitOutcome {
            oca_group: plan.group,
            quantity: plan.quantity,
            position_quantity: plan.position_quantity,
            linked_order_ids,
            relinked: copies.relinked(),
            warnings,
        });
        outcome
    }

    /// Cancel one order without losing the exits linked with it.
    ///
    /// The siblings are re-placed into `group` and confirmed first; cancelling
    /// the target then cancels its old group. If IBKR does not cancel the
    /// target, the copies are cancelled again.
    ///
    /// # Arguments
    ///
    /// * `book` - The account's working orders and the target's position.
    /// * `target` - Order to cancel.
    /// * `group` - Fresh OCA group for the siblings.
    /// * `relink` - Sibling exits to keep.
    /// * `first_order_id` - Next valid order id for the copies.
    ///
    /// # Returns
    ///
    /// The cancellation outcome.
    pub(super) async fn cancel_keeping_siblings(
        &self,
        book: &ExitBook,
        target: i32,
        group: &str,
        relink: &[WorkingOrder],
        first_order_id: i32,
    ) -> OrderResponse {
        bounded(
            self.cancel_keeping_siblings_unbounded(book, target, group, relink, first_order_id)
                .await,
        )
    }

    async fn cancel_keeping_siblings_unbounded(
        &self,
        book: &ExitBook,
        target: i32,
        group: &str,
        relink: &[WorkingOrder],
        first_order_id: i32,
    ) -> OrderResponse {
        let contract_id = relink.first().map_or(0, |sibling| sibling.contract_id);
        let copies = self.place_copies(relink, group, first_order_id).await;
        let copy_ids = copies.copy_ids();
        let mut warnings = Vec::new();
        if let Some((original, outcome)) = &copies.stopped {
            match outcome {
                CopyOutcome::Filled => warnings.push(format!(
                    "IBKR filled the re-placed copy of linked exit #{}; check the position",
                    original.order_id
                )),
                CopyOutcome::Rejected(reason) | CopyOutcome::Unknown(reason) => {
                    let detail = format!(
                        "IBKR order {target} was not cancelled: IBKR did not confirm the re-placed copy of its linked exit #{} ({reason}), and cancelling it would cancel that exit too",
                        original.order_id
                    );
                    return match self.roll_back(&copy_ids, self.cancels_deadline()).await {
                        Ok(()) => cancel_response(self.account, target, false, "rejected", &detail),
                        Err(problem) => cancel_indeterminate_response(
                            self.account.to_string(),
                            target,
                            &format!("{detail}, and {problem}"),
                        ),
                    };
                }
                CopyOutcome::Working => {}
            }
        }
        let dropped: Vec<i32> = relink
            .iter()
            .filter(|sibling| {
                !copies
                    .placed
                    .iter()
                    .any(|(original, _)| original.order_id == sibling.order_id)
            })
            .map(|sibling| sibling.order_id)
            .collect();
        if !dropped.is_empty() {
            warnings.push(format!(
                "linked exit orders {} could not be re-placed and IBKR cancelled them with the group; arm them again",
                order_names(dropped.iter().copied())
            ));
        }
        let mut outcome = self
            .cancel_all(&[target], self.cancels_deadline())
            .await
            .pop()
            .map_or_else(
                || CancelOutcome::Unknown("the cancellation was not sent".to_string()),
                |(_, outcome)| outcome,
            );
        if let CancelOutcome::Unknown(_) = &outcome
            && let Some(order) = book
                .orders
                .iter()
                .find(|order| order.order_id == target && order.client_id == self.client_id)
            && let Ok(Ok(orders)) =
                tokio::time::timeout_at(self.deadline, self.provider_orders()).await
        {
            let check = check_cancelled(std::slice::from_ref(order), &orders);
            if !check.filled.is_empty() {
                outcome = CancelOutcome::Filled;
            } else if check.still_open.is_empty() {
                outcome = CancelOutcome::Cancelled;
            }
        }
        match outcome {
            CancelOutcome::Cancelled => {
                let siblings = copies.originals();
                // The siblings left with their old group; confirm it.
                if let Err(problem) = self
                    .confirm_gone(
                        &siblings
                            .iter()
                            .map(|sibling| sibling.order_id)
                            .collect::<Vec<_>>(),
                        self.deadline,
                    )
                    .await
                {
                    warnings.push(problem);
                }
                if !warnings.is_empty() {
                    warnings.extend(self.recheck(contract_id, &copy_ids).await);
                }
                let mut message = if copy_ids.is_empty() {
                    "IBKR confirmed the order cancellation".to_string()
                } else {
                    format!(
                        "IBKR confirmed the order cancellation. Kept its linked exits {} as {} in IBKR OCA group {group}",
                        order_names(siblings.iter().map(|sibling| sibling.order_id)),
                        order_names(copy_ids.iter().copied())
                    )
                };
                for warning in warnings {
                    message.push_str(&format!(". Warning: {warning}"));
                }
                cancel_response(self.account, target, true, "cancelled", &message)
            }
            CancelOutcome::Filled => {
                let note = self.roll_back_note(&copy_ids, self.deadline).await;
                cancel_response(
                    self.account,
                    target,
                    false,
                    "filled",
                    &format!("IBKR reports the order filled before cancellation{note}"),
                )
            }
            CancelOutcome::Rejected(reason) => {
                let note = self.roll_back_note(&copy_ids, self.deadline).await;
                cancel_response(
                    self.account,
                    target,
                    false,
                    "rejected",
                    &format!("{reason}{note}"),
                )
            }
            CancelOutcome::Unknown(detail) => {
                let note = self.roll_back_note(&copy_ids, self.deadline).await;
                cancel_indeterminate_response(
                    self.account.to_string(),
                    target,
                    &format!("{detail}{note}"),
                )
            }
        }
    }
}

fn filled_warning(order_id: i32) -> String {
    format!(
        "earlier exit order #{order_id} filled before it could be cancelled; check the position and its exits in IBKR"
    )
}

/// Keep a response message within [`MAX_MESSAGE_CHARS`].
fn bounded(mut response: OrderResponse) -> OrderResponse {
    if let Some((end, _)) = response.message.char_indices().nth(MAX_MESSAGE_CHARS - 1) {
        response.message.truncate(end);
        response.message.push('…');
    }
    response
}

/// Response for an exit that was not sent to IBKR.
fn not_placed(account: &str, customer_order_id: &str, message: &str) -> OrderResponse {
    OrderResponse {
        version: PROTOCOL_VERSION,
        provider: "ibkr",
        ok: false,
        status: "rejected".to_string(),
        message: message.to_string(),
        account_ref: Some(account.to_string()),
        order_id: None,
        customer_order_id: Some(customer_order_id.to_string()),
        protection: None,
        exit: None,
    }
}

/// Most orders named in one message; the rest are counted.
const MAX_NAMED_ORDERS: usize = 8;

/// Join names, naming at most [`MAX_NAMED_ORDERS`] and counting the rest.
fn name_list(names: Vec<String>) -> String {
    let more = names.len().saturating_sub(MAX_NAMED_ORDERS);
    let mut listed = names
        .into_iter()
        .take(MAX_NAMED_ORDERS)
        .collect::<Vec<_>>()
        .join(", ");
    if more > 0 {
        listed.push_str(&format!(" and {more} more"));
    }
    listed
}

/// Format order ids as `#41, #42`, bounded.
///
/// # Arguments
///
/// * `order_ids` - TWS order ids to name.
///
/// # Returns
///
/// At most [`MAX_NAMED_ORDERS`] ids, then a count of the rest.
pub(super) fn order_names(order_ids: impl Iterator<Item = i32>) -> String {
    name_list(order_ids.map(|order_id| format!("#{order_id}")).collect())
}

/// Describe orders as `#41 (STP sell 10)`, bounded.
fn describe_orders<'a>(orders: impl IntoIterator<Item = &'a WorkingOrder>) -> String {
    name_list(orders.into_iter().map(WorkingOrder::describe).collect())
}

/// Find this client's working order by its id.
///
/// # Arguments
///
/// * `orders` - Working orders of the account.
/// * `client_id` - This process's TWS client id.
/// * `order_id` - Order id in this client's id space.
///
/// # Returns
///
/// The order, or none when this client has no such working order.
pub(super) fn own_working_order(
    orders: &[WorkingOrder],
    client_id: i32,
    order_id: i32,
) -> Option<&WorkingOrder> {
    orders
        .iter()
        .find(|order| order.client_id == client_id && order.order_id == order_id)
}

#[cfg(test)]
mod tests;
