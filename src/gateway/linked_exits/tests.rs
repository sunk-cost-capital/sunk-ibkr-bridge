//! Tests for linked IBKR exits: planning, copies, and the order of the
//! place and cancel requests a fake TWS socket receives.

use super::*;
use crate::contract::InstrumentKind;
use ibapi::contracts::SecurityType;
use std::collections::HashSet;
use std::net::SocketAddr;

const ACCOUNT: &str = "U1234567";
const CLIENT_ID: i32 = 17_654;
const OTHER_CLIENT_ID: i32 = 17_655;
const CONTRACT_ID: i32 = 756_733;
const EXIT_REFERENCE: &str = "sunk-3e4f5a6b-7c8d-4e9f-8a0b-1c2d3e4f5a6b";
const LEGACY_REFERENCE: &str = "sunk-0f8b6f1e-4c1d-4a53-9a3e-2d7f5c9b8e21";
const FRESH_GROUP: &str = "oca-sunk-3e4f5a6b-7c8d-4e9f-8a0b-1c2d3e4f5a6b";
const BRACKET_GROUP: &str = "oca-sunk-1b2c3d4e-5f60-4a71-8b92-a3b4c5d6e7f8";

fn spy() -> Contract {
    Contract {
        contract_id: CONTRACT_ID,
        symbol: "SPY".into(),
        security_type: SecurityType::Stock,
        exchange: "SMART".into(),
        currency: "USD".into(),
        ..Default::default()
    }
}

/// A working order of this account, selling 10 SPY unless changed.
fn order(order_id: i32, order_type: &str, price: f64) -> WorkingOrder {
    let (limit_price, aux_price) = match order_type {
        "STP" => (None, Some(price)),
        _ => (Some(price), None),
    };
    WorkingOrder {
        order_id,
        client_id: CLIENT_ID,
        perm_id: 1_000 + i64::from(order_id),
        contract_id: CONTRACT_ID,
        contract: spy(),
        parent_id: 0,
        sells: true,
        remaining: 10.0,
        status: OrderStatusKind::Submitted,
        order: Order {
            order_id,
            client_id: CLIENT_ID,
            perm_id: 1_000 + i64::from(order_id),
            action: Action::Sell,
            total_quantity: 10.0,
            order_type: order_type.to_string(),
            limit_price,
            aux_price,
            tif: TimeInForce::GoodTilCanceled,
            account: ACCOUNT.to_string(),
            order_ref: LEGACY_REFERENCE.to_string(),
            ..Default::default()
        },
    }
}

fn in_group(mut order: WorkingOrder, group: &str, oca_type: OcaType) -> WorkingOrder {
    order.order.oca_group = group.to_string();
    order.order.oca_type = oca_type;
    order
}

fn sized(mut order: WorkingOrder, quantity: f64) -> WorkingOrder {
    order.remaining = quantity;
    order.order.total_quantity = quantity;
    order
}

fn foreign(mut order: WorkingOrder) -> WorkingOrder {
    order.client_id = OTHER_CLIENT_ID;
    order.order.client_id = OTHER_CLIENT_ID;
    order
}

fn book(position: f64, orders: Vec<WorkingOrder>) -> ExitBook {
    ExitBook::new(orders, CONTRACT_ID, position, CLIENT_ID)
}

fn ids(orders: &[WorkingOrder]) -> Vec<i32> {
    orders.iter().map(|order| order.order_id).collect()
}

#[test]
fn a_reduce_only_exit_needs_a_position_to_close() {
    let error = plan_new_exit(&book(0.0, Vec::new()), true, 10.0, FRESH_GROUP).unwrap_err();
    assert!(error.contains("no position"), "{error}");
}

#[test]
fn a_reduce_only_exit_never_adds_to_the_position() {
    let error = plan_new_exit(&book(10.0, Vec::new()), false, 10.0, FRESH_GROUP).unwrap_err();
    assert!(error.contains("add to the long position"), "{error}");
    let error = plan_new_exit(&book(-10.0, Vec::new()), true, 10.0, FRESH_GROUP).unwrap_err();
    assert!(error.contains("add to the short position"), "{error}");
}

#[test]
fn an_exit_larger_than_the_position_is_clamped_to_it() {
    let plan = plan_new_exit(&book(10.0, Vec::new()), true, 150.0, FRESH_GROUP).unwrap();
    assert_eq!(plan.quantity, 10.0);
    assert_eq!(plan.position_quantity, 10.0);
    assert_eq!(plan.group, FRESH_GROUP);
    assert!(plan.relink.is_empty());
    // A short position is closed by buys and clamped the same way.
    let plan = plan_new_exit(&book(-4.0, Vec::new()), false, 5.0, FRESH_GROUP).unwrap();
    assert_eq!(plan.quantity, 4.0);
}

#[test]
fn the_first_exit_starts_a_fresh_group() {
    let plan = plan_new_exit(&book(10.0, Vec::new()), true, 10.0, FRESH_GROUP).unwrap();
    assert_eq!(
        plan,
        NewExitPlan {
            group: FRESH_GROUP.to_string(),
            quantity: 10.0,
            position_quantity: 10.0,
            joined: Vec::new(),
            relink: Vec::new(),
        }
    );
}

#[test]
fn a_new_exit_joins_the_existing_exits_oca_group() {
    let take_profit = in_group(
        order(101, "LMT", 700.0),
        BRACKET_GROUP,
        OcaType::ReduceWithBlock,
    );
    let stop_loss = in_group(
        order(102, "STP", 620.0),
        BRACKET_GROUP,
        OcaType::ReduceWithBlock,
    );
    let plan = plan_new_exit(
        &book(10.0, vec![take_profit, stop_loss]),
        true,
        10.0,
        FRESH_GROUP,
    )
    .unwrap();
    assert_eq!(plan.group, BRACKET_GROUP);
    assert_eq!(ids(&plan.joined), vec![101, 102]);
    assert!(plan.relink.is_empty(), "joining never re-places anything");
}

#[test]
fn a_standalone_stop_beside_a_new_take_profit_is_relinked() {
    // The pre-existing bug: a fresh take profit beside a standalone stop
    // would be two independent full-size exits.
    let legacy_stop = order(88, "STP", 620.0);
    let plan = plan_new_exit(&book(10.0, vec![legacy_stop]), true, 10.0, FRESH_GROUP).unwrap();
    assert_eq!(plan.group, FRESH_GROUP);
    assert_eq!(ids(&plan.relink), vec![88]);
    assert!(plan.joined.is_empty());
}

#[test]
fn exits_that_fit_the_position_unlinked_are_left_alone() {
    // A 4-share stop and a new 6-share take profit can never close more than 10.
    let small_stop = sized(order(88, "STP", 620.0), 4.0);
    let plan = plan_new_exit(&book(10.0, vec![small_stop]), true, 6.0, FRESH_GROUP).unwrap();
    assert!(plan.relink.is_empty());
    assert_eq!(plan.group, FRESH_GROUP);
}

#[test]
fn every_own_exit_is_relinked_when_groups_overlap() {
    // A group of 10 on a 10 position plus a loose stop: one group for all.
    let first = in_group(
        order(101, "LMT", 700.0),
        BRACKET_GROUP,
        OcaType::ReduceWithBlock,
    );
    let loose = order(88, "STP", 620.0);
    let plan = plan_new_exit(&book(10.0, vec![first, loose]), true, 10.0, FRESH_GROUP).unwrap();
    assert_eq!(plan.group, FRESH_GROUP);
    assert_eq!(ids(&plan.relink), vec![101, 88]);
}

#[test]
fn exits_placed_by_another_client_are_counted_but_never_moved() {
    let foreign_stop = foreign(order(7, "STP", 620.0));
    let error =
        plan_new_exit(&book(10.0, vec![foreign_stop]), true, 10.0, FRESH_GROUP).unwrap_err();
    assert!(error.contains("#7 (STP sell 10)"), "{error}");
    assert!(error.contains("TWS or another app"), "{error}");

    // A 4-share foreign stop leaves room for a 6-share linked group.
    let foreign_stop = sized(foreign(order(7, "STP", 620.0)), 4.0);
    let own_stop = sized(order(88, "STP", 621.0), 6.0);
    let plan = plan_new_exit(
        &book(10.0, vec![foreign_stop, own_stop]),
        true,
        6.0,
        FRESH_GROUP,
    )
    .unwrap();
    assert_eq!(ids(&plan.relink), vec![88]);
}

#[test]
fn an_own_exit_larger_than_the_position_is_refused_not_resized() {
    let oversized = sized(order(88, "STP", 620.0), 15.0);
    let error = plan_new_exit(&book(10.0, vec![oversized]), true, 10.0, FRESH_GROUP).unwrap_err();
    assert!(error.contains("larger than the 10 position"), "{error}");
}

#[test]
fn only_exactly_reproducible_exits_are_relinked() {
    let mut trailing = order(88, "TRAIL", 1.0);
    trailing.order.trailing_percent = Some(2.0);
    let error = plan_new_exit(&book(10.0, vec![trailing]), true, 10.0, FRESH_GROUP).unwrap_err();
    assert!(error.contains("cannot be re-placed exactly"), "{error}");

    let mut good_till_date = order(88, "STP", 620.0);
    good_till_date.order.tif = TimeInForce::GoodTilDate;
    let error =
        plan_new_exit(&book(10.0, vec![good_till_date]), true, 10.0, FRESH_GROUP).unwrap_err();
    assert!(error.contains("time in force"), "{error}");

    let mut moving = order(88, "STP", 620.0);
    moving.status = OrderStatusKind::PendingCancel;
    let error = plan_new_exit(&book(10.0, vec![moving]), true, 10.0, FRESH_GROUP).unwrap_err();
    assert!(error.contains("changing state"), "{error}");
}

#[test]
fn an_exit_linked_with_a_non_exit_is_not_relinked() {
    let stop = in_group(
        order(88, "STP", 620.0),
        "user-group",
        OcaType::CancelWithBlock,
    );
    let mut other_contract = in_group(
        order(89, "LMT", 10.0),
        "user-group",
        OcaType::CancelWithBlock,
    );
    other_contract.contract_id = 1;
    let error = plan_new_exit(
        &book(10.0, vec![stop, other_contract]),
        true,
        10.0,
        FRESH_GROUP,
    )
    .unwrap_err();
    assert!(error.contains("linked with order #89"), "{error}");
}

#[test]
fn bracket_children_of_a_working_parent_are_not_exits() {
    let mut parent = order(100, "LMT", 650.0);
    parent.sells = false;
    parent.order.action = Action::Buy;
    let mut take_profit = in_group(
        order(101, "LMT", 700.0),
        BRACKET_GROUP,
        OcaType::ReduceWithBlock,
    );
    take_profit.parent_id = 100;
    let mut stop_loss = in_group(
        order(102, "STP", 620.0),
        BRACKET_GROUP,
        OcaType::ReduceWithBlock,
    );
    stop_loss.parent_id = 100;
    let book = book(10.0, vec![parent, take_profit, stop_loss]);
    assert!(book.exits.is_empty());
    let plan = plan_new_exit(&book, true, 10.0, FRESH_GROUP).unwrap();
    assert_eq!(plan.group, FRESH_GROUP);
    assert!(plan.relink.is_empty());
}

#[test]
fn exposure_counts_a_blocking_group_once_and_unblocked_orders_each() {
    let reduce = [
        in_group(order(1, "LMT", 700.0), "a", OcaType::ReduceWithBlock),
        in_group(order(2, "STP", 620.0), "a", OcaType::ReduceWithBlock),
    ];
    assert_eq!(exposure(&reduce), 10.0);
    let cancel = [
        in_group(order(1, "LMT", 700.0), "b", OcaType::CancelWithBlock),
        in_group(
            sized(order(2, "STP", 620.0), 4.0),
            "b",
            OcaType::CancelWithBlock,
        ),
    ];
    assert_eq!(exposure(&cancel), 10.0);
    // Without block several orders can be routed and fill at once.
    let unblocked = [
        in_group(order(1, "LMT", 700.0), "c", OcaType::ReduceWithoutBlock),
        in_group(order(2, "STP", 620.0), "c", OcaType::ReduceWithoutBlock),
    ];
    assert_eq!(exposure(&unblocked), 20.0);
    // The same group name from two clients is not assumed to be one group.
    let split = [
        in_group(order(1, "LMT", 700.0), "a", OcaType::ReduceWithBlock),
        foreign(in_group(
            order(2, "STP", 620.0),
            "a",
            OcaType::ReduceWithBlock,
        )),
    ];
    assert_eq!(exposure(&split), 20.0);
}

#[test]
fn working_orders_keep_the_latest_report_of_each_working_order() {
    let data = |order_id: i32, account: &str, status: OrderStatusKind, filled: f64| OrderData {
        order_id,
        contract: spy(),
        order: Order {
            order_id,
            client_id: CLIENT_ID,
            perm_id: i64::from(order_id),
            action: Action::Sell,
            total_quantity: 10.0,
            filled_quantity: filled,
            account: account.to_string(),
            order_type: "STP".to_string(),
            ..Default::default()
        },
        order_state: ibapi::orders::OrderState {
            status,
            ..Default::default()
        },
    };
    let orders = working_orders(
        &[
            data(1, ACCOUNT, OrderStatusKind::Submitted, 0.0),
            data(1, ACCOUNT, OrderStatusKind::Cancelled, 0.0),
            data(2, ACCOUNT, OrderStatusKind::PreSubmitted, 4.0),
            data(3, "U7654321", OrderStatusKind::Submitted, 0.0),
            data(4, ACCOUNT, OrderStatusKind::Filled, 10.0),
        ],
        ACCOUNT,
    );
    assert_eq!(orders.len(), 1, "{orders:?}");
    assert_eq!(orders[0].order_id, 2);
    assert_eq!(orders[0].remaining, 6.0);
    assert!(orders[0].sells);
}

#[test]
fn cancelling_a_linked_exit_keeps_its_siblings() {
    let take_profit = in_group(
        order(101, "LMT", 700.0),
        BRACKET_GROUP,
        OcaType::ReduceWithBlock,
    );
    let stop_loss = in_group(
        order(102, "STP", 620.0),
        BRACKET_GROUP,
        OcaType::ReduceWithBlock,
    );
    let plan = plan_cancel(&book(10.0, vec![take_profit, stop_loss]), 101, FRESH_GROUP).unwrap();
    let CancelPlan::Relink { group, relink } = plan else {
        panic!("a linked sibling must be re-placed, got {plan:?}");
    };
    assert_eq!(group, FRESH_GROUP);
    assert_eq!(ids(&relink), vec![102]);
}

#[test]
fn cancelling_a_loose_order_or_an_unprotecting_group_is_plain() {
    let loose = order(88, "STP", 620.0);
    assert_eq!(
        plan_cancel(&book(10.0, vec![loose]), 88, FRESH_GROUP).unwrap(),
        CancelPlan::Plain {
            cascades: Vec::new()
        }
    );

    // Children of a working parent protect nothing yet; IBKR cancels them together.
    let mut parent = order(100, "LMT", 650.0);
    parent.sells = false;
    let mut take_profit = in_group(
        order(101, "LMT", 700.0),
        BRACKET_GROUP,
        OcaType::ReduceWithBlock,
    );
    take_profit.parent_id = 100;
    let mut stop_loss = in_group(
        order(102, "STP", 620.0),
        BRACKET_GROUP,
        OcaType::ReduceWithBlock,
    );
    stop_loss.parent_id = 100;
    let plan = plan_cancel(
        &book(10.0, vec![parent, take_profit, stop_loss]),
        101,
        FRESH_GROUP,
    )
    .unwrap();
    let CancelPlan::Plain { cascades } = plan else {
        panic!("pending children are cancelled together, got {plan:?}");
    };
    assert_eq!(ids(&cascades), vec![102]);

    // Without a position the siblings would open one, so they go too.
    let take_profit = in_group(
        order(101, "LMT", 700.0),
        BRACKET_GROUP,
        OcaType::ReduceWithBlock,
    );
    let stop_loss = in_group(
        order(102, "STP", 620.0),
        BRACKET_GROUP,
        OcaType::ReduceWithBlock,
    );
    let plan = plan_cancel(&book(0.0, vec![take_profit, stop_loss]), 101, FRESH_GROUP).unwrap();
    assert!(matches!(plan, CancelPlan::Plain { cascades } if ids(&cascades) == vec![102]));
}

#[test]
fn cancelling_refuses_to_drop_a_sibling_it_cannot_re_place() {
    let take_profit = in_group(
        order(101, "LMT", 700.0),
        "user-group",
        OcaType::CancelWithBlock,
    );
    let mut other_contract = in_group(
        order(103, "LMT", 10.0),
        "user-group",
        OcaType::CancelWithBlock,
    );
    other_contract.contract_id = 1;
    let stop_loss = in_group(
        order(102, "STP", 620.0),
        "user-group",
        OcaType::CancelWithBlock,
    );
    let error = plan_cancel(
        &book(10.0, vec![take_profit, stop_loss, other_contract]),
        101,
        FRESH_GROUP,
    )
    .unwrap_err();
    assert!(error.contains("#103"), "{error}");
}

#[test]
fn a_relinked_copy_reproduces_the_original_in_the_new_group() {
    let mut original = in_group(
        order(102, "STP", 620.0),
        BRACKET_GROUP,
        OcaType::ReduceWithBlock,
    );
    original.parent_id = 100;
    original.order.parent_id = 100;
    original.order.outside_rth = true;
    let copy = relinked_copy(&sized(original, 6.0), ACCOUNT, FRESH_GROUP);
    assert_eq!(copy.action, Action::Sell);
    assert_eq!(copy.total_quantity, 6.0);
    assert_eq!(copy.order_type, "STP");
    assert_eq!(copy.aux_price, Some(620.0));
    assert_eq!(copy.limit_price, None);
    assert_eq!(copy.tif, TimeInForce::GoodTilCanceled);
    assert!(copy.outside_rth);
    assert_eq!(
        copy.order_ref, LEGACY_REFERENCE,
        "journaled roles keep naming it"
    );
    assert_eq!(copy.oca_group, FRESH_GROUP);
    assert_eq!(copy.oca_type, OcaType::ReduceWithBlock);
    assert_eq!(copy.parent_id, 0, "the filled parent is not re-attached");
    assert!(copy.transmit);
    assert!(!copy.override_percentage_constraints);
    assert!(copy.advanced_error_override.is_empty());

    // A stop reported with a stray limit value is copied as a plain stop.
    let mut stop = order(88, "STP", 620.0);
    stop.order.limit_price = Some(0.0);
    let copy = relinked_copy(&stop, ACCOUNT, FRESH_GROUP);
    assert_eq!(copy.limit_price, None);
    assert_eq!(copy.aux_price, Some(620.0));
    let mut take_profit = order(89, "LMT", 700.0);
    take_profit.order.aux_price = Some(0.0);
    take_profit.order.algo_strategy = "Adaptive".to_string();
    take_profit.order.algo_params = vec![ibapi::contracts::TagValue {
        tag: "adaptivePriority".to_string(),
        value: "Patient".to_string(),
    }];
    let copy = relinked_copy(&take_profit, ACCOUNT, FRESH_GROUP);
    assert_eq!(copy.limit_price, Some(700.0));
    assert_eq!(copy.aux_price, None);
    // An Adaptive exit stays Adaptive in its new group.
    assert_eq!(copy.algo_strategy, "Adaptive");
    assert_eq!(copy.algo_params, take_profit.order.algo_params);
}

#[test]
fn a_linked_exit_order_carries_the_group_and_clamped_quantity() {
    let order = linked_exit_order(
        Order {
            total_quantity: 15.0,
            cash_qty: Some(1.0),
            ..Default::default()
        },
        BRACKET_GROUP,
        10.0,
    );
    assert_eq!(order.total_quantity, 10.0);
    assert_eq!(order.cash_qty, None);
    assert_eq!(order.oca_group, BRACKET_GROUP);
    assert_eq!(order.oca_type, OcaType::ReduceWithBlock);
}

#[test]
fn cancelled_orders_are_matched_by_id_while_open_and_by_permanent_id_when_completed() {
    let stop = order(88, "STP", 620.0);
    let take_profit = order(89, "LMT", 700.0);
    let open = |order: &WorkingOrder| OrderData {
        order_id: order.order_id,
        contract: spy(),
        order: order.order.clone(),
        order_state: ibapi::orders::OrderState {
            status: OrderStatusKind::Submitted,
            ..Default::default()
        },
    };
    let mut completed = open(&take_profit);
    completed.order_id = -1;
    completed.order_state.status = OrderStatusKind::Filled;
    let check = check_cancelled(
        &[stop.clone(), take_profit],
        &ProviderOrders {
            open: vec![open(&stop)],
            completed: vec![completed],
        },
    );
    assert_eq!(check.still_open, vec![88]);
    assert_eq!(check.filled, vec![89]);
}

fn status_event(order_id: i32, status: OrderStatusKind, filled: f64) -> PlaceOrder {
    PlaceOrder::OrderStatus(ibapi::orders::OrderStatus {
        order_id,
        status,
        filled,
        ..Default::default()
    })
}

fn notice(code: i32, message: &str) -> ibapi::Notice {
    ibapi::Notice {
        code,
        message: message.to_string(),
        error_time: None,
        advanced_order_reject_json: String::new(),
    }
}

type CopyItem = Result<SubscriptionItem<PlaceOrder>, ibapi::Error>;

async fn confirm(items: Vec<CopyItem>, open: Vec<OrderData>) -> CopyOutcome {
    confirm_copy(
        futures::stream::iter(items),
        200,
        Instant::now() + Duration::from_secs(2),
        move || {
            let open = open.clone();
            async move { Ok(open) }
        },
    )
    .await
}

#[tokio::test]
async fn a_copy_counts_only_once_ibkr_confirms_it_working() {
    let working = confirm(
        vec![
            Ok(SubscriptionItem::Data(status_event(
                200,
                OrderStatusKind::PendingSubmit,
                0.0,
            ))),
            Ok(SubscriptionItem::Data(status_event(
                999,
                OrderStatusKind::Cancelled,
                0.0,
            ))),
            Ok(SubscriptionItem::Data(status_event(
                200,
                OrderStatusKind::PreSubmitted,
                0.0,
            ))),
        ],
        Vec::new(),
    )
    .await;
    assert_eq!(working, CopyOutcome::Working);

    let filled = confirm(
        vec![Ok(SubscriptionItem::Data(status_event(
            200,
            OrderStatusKind::Submitted,
            3.0,
        )))],
        Vec::new(),
    )
    .await;
    assert_eq!(filled, CopyOutcome::Filled);

    let rejected = confirm(
        vec![Err(ibapi::Error::Notice(notice(201, "Order rejected")))],
        Vec::new(),
    )
    .await;
    assert!(matches!(rejected, CopyOutcome::Rejected(text) if text.contains("[201]")));

    let silent = confirm(Vec::new(), Vec::new()).await;
    assert!(matches!(silent, CopyOutcome::Unknown(_)));

    let unknown = confirm(
        vec![Err(ibapi::Error::Notice(notice(10_999, "Something else")))],
        Vec::new(),
    )
    .await;
    assert!(matches!(unknown, CopyOutcome::Unknown(text) if text.contains("10999")));
}

#[tokio::test]
async fn a_held_copy_is_read_back_by_its_order_id() {
    let held = OrderData {
        order_id: 200,
        contract: spy(),
        order: Order {
            order_ref: LEGACY_REFERENCE.to_string(),
            client_id: CLIENT_ID,
            ..Default::default()
        },
        order_state: ibapi::orders::OrderState {
            status: OrderStatusKind::PreSubmitted,
            ..Default::default()
        },
    };
    let outcome = confirm(
        vec![Err(ibapi::Error::Notice(notice(
            399,
            "Order Message:\nSELL 10 SPY\nWarning: Your order will not be placed at the exchange until 2026-10-01 09:30:00 US/Eastern",
        )))],
        vec![held],
    )
    .await;
    assert_eq!(outcome, CopyOutcome::Working);

    // Nothing listed for the copy's id is not a confirmation.
    let missing = confirm(
        vec![Err(ibapi::Error::Notice(notice(399, "Warning: held")))],
        Vec::new(),
    )
    .await;
    assert!(matches!(missing, CopyOutcome::Unknown(_)));
}

/// Covers both `Gateway::order` and `Gateway::cancel_order`: each takes the
/// operation lock, prepares (connection, snapshots, next order id) inside
/// the preparation deadline, then runs one linked budget.
#[test]
fn the_linked_exit_budget_fits_inside_the_caller_deadline() {
    assert!(COPIES_PHASE + CANCELS_PHASE + EXIT_FOLLOW_UP_RESERVE < LINKED_EXIT_BUDGET);
    let worst = super::super::ORDER_OPERATION_LOCK_TIMEOUT
        + super::super::ORDER_PREPARATION_TIMEOUT
        + LINKED_EXIT_BUDGET;
    assert!(
        worst < super::super::ORDER_CALLER_TIMEOUT_FLOOR,
        "{worst:?}"
    );
}

/// Minimal protobuf encoding for the fake TWS replies and request decoding.
mod wire {
    use std::collections::BTreeMap;

    pub(super) fn varint(mut value: u64) -> Vec<u8> {
        let mut out = Vec::new();
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

    pub(super) fn int_field(field: u64, value: i64) -> Vec<u8> {
        let mut out = varint(field << 3);
        out.extend(varint(value as u64));
        out
    }

    pub(super) fn string_field(field: u64, value: &str) -> Vec<u8> {
        let mut out = varint((field << 3) | 2);
        out.extend(varint(value.len() as u64));
        out.extend_from_slice(value.as_bytes());
        out
    }

    fn read_varint(bytes: &[u8], cursor: &mut usize) -> u64 {
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

    /// One decoded protobuf field value.
    #[derive(Clone, Debug)]
    pub(super) enum Field {
        Varint(u64),
        Double(f64),
        Bytes(Vec<u8>),
    }

    pub(super) fn decode(bytes: &[u8]) -> BTreeMap<u64, Field> {
        let mut fields = BTreeMap::new();
        let mut cursor = 0;
        while cursor < bytes.len() {
            let key = read_varint(bytes, &mut cursor);
            let value = match key & 7 {
                0 => Field::Varint(read_varint(bytes, &mut cursor)),
                1 => {
                    let raw = u64::from_le_bytes(bytes[cursor..cursor + 8].try_into().unwrap());
                    cursor += 8;
                    Field::Double(f64::from_bits(raw))
                }
                2 => {
                    let len = read_varint(bytes, &mut cursor) as usize;
                    let raw = bytes[cursor..cursor + len].to_vec();
                    cursor += len;
                    Field::Bytes(raw)
                }
                5 => {
                    cursor += 4;
                    continue;
                }
                wire_type => panic!("unexpected wire type {wire_type}"),
            };
            fields.insert(key >> 3, value);
        }
        fields
    }

    pub(super) fn int(fields: &BTreeMap<u64, Field>, field: u64) -> Option<u64> {
        match fields.get(&field) {
            Some(Field::Varint(value)) => Some(*value),
            _ => None,
        }
    }

    pub(super) fn string(fields: &BTreeMap<u64, Field>, field: u64) -> Option<String> {
        match fields.get(&field) {
            Some(Field::Bytes(raw)) => Some(String::from_utf8(raw.clone()).unwrap()),
            _ => None,
        }
    }

    pub(super) fn double(fields: &BTreeMap<u64, Field>, field: u64) -> Option<f64> {
        match fields.get(&field) {
            Some(Field::Double(value)) => Some(*value),
            _ => None,
        }
    }

    pub(super) fn message(fields: &BTreeMap<u64, Field>, field: u64) -> BTreeMap<u64, Field> {
        match fields.get(&field) {
            Some(Field::Bytes(raw)) => decode(raw),
            other => panic!("field {field} is not a message: {other:?}"),
        }
    }
}

/// One request the client sent the fake TWS.
#[derive(Debug, PartialEq)]
enum Sent {
    /// `placeOrder` for an order id.
    Place(i32),
    /// `cancelOrder` for an order id.
    Cancel(i32),
}

type SentReceiver = tokio::sync::mpsc::UnboundedReceiver<(Sent, Vec<u8>)>;

/// How the fake TWS answers placed orders, by order id.
#[derive(Default)]
struct Script {
    /// Refused with IBKR code 201.
    reject: HashSet<i32>,
    /// Filled at once.
    fill: HashSet<i32>,
    /// Never answered.
    silent: HashSet<i32>,
}

/// A loopback TWS that answers orders the way IB Gateway would.
///
/// Every placed order comes up `PreSubmitted` unless the script says
/// otherwise. Every cancellation is confirmed. Open-order, completed-order
/// and position snapshots are empty.
async fn scripted_tws(script: Script) -> (SocketAddr, SentReceiver) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    async fn read_frame(socket: &mut tokio::net::TcpStream) -> Option<Vec<u8>> {
        let len = socket.read_u32().await.ok()? as usize;
        let mut frame = vec![0; len];
        socket.read_exact(&mut frame).await.ok()?;
        Some(frame)
    }
    async fn write(socket: &mut tokio::net::TcpStream, message_id: i32, body: &[u8]) {
        // Protobuf frames carry the message id offset by 200.
        let mut frame = (message_id + 200).to_be_bytes().to_vec();
        frame.extend_from_slice(body);
        socket
            .write_u32(frame.len() as u32)
            .await
            .expect("write length");
        socket.write_all(&frame).await.expect("write body");
    }
    fn order_status(order_id: i32, status: &str) -> Vec<u8> {
        let filled = if status == "Filled" { "10" } else { "0" };
        let remaining = if status == "Filled" { "0" } else { "10" };
        let mut body = wire::int_field(1, i64::from(order_id));
        body.extend(wire::string_field(2, status));
        body.extend(wire::string_field(3, filled));
        body.extend(wire::string_field(4, remaining));
        body.extend(wire::int_field(6, 9_000 + i64::from(order_id)));
        body.extend(wire::int_field(9, i64::from(CLIENT_ID)));
        body
    }

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind fake TWS");
    let address = listener.local_addr().expect("fake TWS address");
    let (sent, received) = tokio::sync::mpsc::unbounded_channel();
    tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.expect("accept client");
        let mut prefix = [0_u8; 4];
        socket.read_exact(&mut prefix).await.expect("API prefix");
        read_frame(&mut socket).await.expect("version frame");
        let handshake = b"225\x0020260927 09:30:00 America/New_York\x00";
        socket
            .write_u32(handshake.len() as u32)
            .await
            .expect("write handshake length");
        socket.write_all(handshake).await.expect("write handshake");
        read_frame(&mut socket).await.expect("start API frame");
        write(&mut socket, 9, &wire::int_field(1, 100)).await;
        write(&mut socket, 15, &wire::string_field(1, ACCOUNT)).await;
        while let Some(frame) = read_frame(&mut socket).await {
            let message_id = i32::from_be_bytes(frame[..4].try_into().unwrap()) - 200;
            let body = frame[4..].to_vec();
            match message_id {
                // placeOrder
                3 => {
                    let order_id = wire::int(&wire::decode(&body), 1).expect("order id") as i32;
                    if sent.send((Sent::Place(order_id), body)).is_err() {
                        break;
                    }
                    if script.reject.contains(&order_id) {
                        let mut error = wire::int_field(1, i64::from(order_id));
                        error.extend(wire::int_field(3, 201));
                        error.extend(wire::string_field(4, "Order rejected - reason: test"));
                        write(&mut socket, 4, &error).await;
                    } else if script.fill.contains(&order_id) {
                        write(&mut socket, 3, &order_status(order_id, "Filled")).await;
                    } else if !script.silent.contains(&order_id) {
                        write(&mut socket, 3, &order_status(order_id, "PreSubmitted")).await;
                    }
                }
                // cancelOrder
                4 => {
                    let order_id = wire::int(&wire::decode(&body), 1).expect("order id") as i32;
                    if sent.send((Sent::Cancel(order_id), body)).is_err() {
                        break;
                    }
                    write(&mut socket, 3, &order_status(order_id, "Cancelled")).await;
                }
                // reqOpenOrders, reqAllOpenOrders
                5 | 16 => write(&mut socket, 53, &[]).await,
                // reqCompletedOrders
                99 => write(&mut socket, 102, &[]).await,
                // reqPositions
                61 => write(&mut socket, 62, &[]).await,
                _ => {}
            }
        }
    });
    (address, received)
}

/// Requests the fake TWS has received so far, in order.
fn drain(received: &mut SentReceiver) -> Vec<(Sent, Vec<u8>)> {
    let mut sent = Vec::new();
    while let Ok(item) = received.try_recv() {
        sent.push(item);
    }
    sent
}

/// The order inside a `PlaceOrderRequest`. Order.proto fields: 5 action,
/// 6 totalQuantity, 8 orderType, 9 lmtPrice, 10 auxPrice, 27 ocaGroup,
/// 28 orderRef, 30 ocaType.
fn placed_order(body: &[u8]) -> std::collections::BTreeMap<u64, wire::Field> {
    wire::message(&wire::decode(body), 3)
}

fn exit_request(order_type: &str, price: f64) -> OrderRequest {
    OrderRequest {
        version: PROTOCOL_VERSION,
        connection_id: Some("connection-1".to_string()),
        provider: "ibkr".to_string(),
        instrument: CONTRACT_ID.to_string(),
        instrument_kind: InstrumentKind::Stock,
        side: "sell".to_string(),
        size_usd: price * 10.0,
        quantity: Some(15.0),
        order_type: order_type.to_string(),
        limit_price: (order_type == "limit").then_some(price),
        stop_price: (order_type == "stop_market").then_some(price),
        time_in_force: "gtc".to_string(),
        // A stop cannot carry a slippage cap; IBKR fills it at market.
        market_slippage_bps: (order_type == "limit").then_some(25),
        trailing_percent: None,
        limit_offset: None,
        outside_rth: false,
        algo: None,
        reduce_only: true,
        post_only: false,
        account_ref: Some(ACCOUNT.to_string()),
        customer_order_id: EXIT_REFERENCE.to_string(),
        protection: None,
    }
}

async fn session_client(reject: &[i32]) -> (Arc<Client>, SentReceiver) {
    scripted_client(Script {
        reject: reject.iter().copied().collect(),
        ..Script::default()
    })
    .await
}

async fn scripted_client(script: Script) -> (Arc<Client>, SentReceiver) {
    let (address, received) = scripted_tws(script).await;
    let client = Client::connect(&address.to_string(), CLIENT_ID)
        .await
        .expect("connect to fake TWS");
    (Arc::new(client), received)
}

fn session<'a>(client: &'a Arc<Client>, snapshots: &'a Mutex<()>) -> LinkedSession<'a> {
    LinkedSession::new(client, ACCOUNT, CLIENT_ID, snapshots, LINKED_EXIT_BUDGET)
}

#[tokio::test]
async fn a_legacy_exit_is_re_placed_into_the_group_before_it_is_cancelled() {
    let (client, mut received) = session_client(&[]).await;
    let snapshots = Mutex::new(());
    let legacy_stop = order(88, "STP", 620.0);
    let plan = plan_new_exit(&book(10.0, vec![legacy_stop]), true, 15.0, FRESH_GROUP).unwrap();

    let response = session(&client, &snapshots)
        .submit_exit(&spy(), &exit_request("limit", 700.0), plan, 200)
        .await;

    assert!(response.ok, "{response:?}");
    assert_eq!(response.status, "resting");
    assert_eq!(response.order_id.as_deref(), Some("201"));
    let exit = response.exit.expect("linked exit outcome");
    assert_eq!(exit.oca_group, FRESH_GROUP);
    assert_eq!(exit.quantity, 10.0, "clamped to the position");
    assert_eq!(exit.linked_order_ids, vec!["200".to_string()]);
    assert_eq!(
        exit.relinked,
        vec![RelinkedOrder {
            from_order_id: "88".to_string(),
            to_order_id: "200".to_string(),
        }]
    );
    assert!(exit.warnings.is_empty(), "{:?}", exit.warnings);
    assert!(
        response
            .message
            .contains("Quantity reduced from 15 to the 10 position"),
        "{}",
        response.message
    );

    let sent = drain(&mut received);
    let order: Vec<&Sent> = sent.iter().map(|(sent, _)| sent).collect();
    // The copy first, then the original's cancellation, then the exit.
    assert_eq!(
        order,
        vec![&Sent::Place(200), &Sent::Cancel(88), &Sent::Place(201)]
    );
    let copy = placed_order(&sent[0].1);
    assert_eq!(wire::string(&copy, 5).as_deref(), Some("SELL"));
    assert_eq!(wire::string(&copy, 6).as_deref(), Some("10"));
    assert_eq!(wire::string(&copy, 8).as_deref(), Some("STP"));
    assert_eq!(wire::double(&copy, 10), Some(620.0));
    assert_eq!(wire::string(&copy, 27).as_deref(), Some(FRESH_GROUP));
    assert_eq!(wire::string(&copy, 28).as_deref(), Some(LEGACY_REFERENCE));
    assert_eq!(wire::int(&copy, 30), Some(2));
    let exit_order = placed_order(&sent[2].1);
    assert_eq!(wire::string(&exit_order, 6).as_deref(), Some("10"));
    assert_eq!(wire::string(&exit_order, 8).as_deref(), Some("LMT"));
    assert_eq!(wire::double(&exit_order, 9), Some(700.0));
    assert_eq!(wire::string(&exit_order, 27).as_deref(), Some(FRESH_GROUP));
    assert_eq!(
        wire::string(&exit_order, 28).as_deref(),
        Some(EXIT_REFERENCE)
    );
    assert_eq!(wire::int(&exit_order, 30), Some(2));
}

#[tokio::test]
async fn a_rejected_copy_leaves_the_legacy_exit_untouched() {
    let (client, mut received) = session_client(&[200]).await;
    let snapshots = Mutex::new(());
    let legacy_stop = order(88, "STP", 620.0);
    let plan = plan_new_exit(&book(10.0, vec![legacy_stop]), true, 10.0, FRESH_GROUP).unwrap();

    let response = session(&client, &snapshots)
        .submit_exit(&spy(), &exit_request("limit", 700.0), plan, 200)
        .await;

    assert!(!response.ok);
    assert_eq!(response.status, "rejected");
    assert_eq!(response.order_id, None, "the exit was never sent");
    assert!(
        response.message.contains("left as they were"),
        "{}",
        response.message
    );
    let order: Vec<Sent> = drain(&mut received)
        .into_iter()
        .map(|(sent, _)| sent)
        .collect();
    assert_eq!(order, vec![Sent::Place(200)], "no exit and no cancellation");
}

#[tokio::test]
async fn a_rejected_exit_leaves_the_earlier_exits_linked() {
    let (client, mut received) = session_client(&[201]).await;
    let snapshots = Mutex::new(());
    let legacy_stop = order(88, "STP", 620.0);
    let plan = plan_new_exit(&book(10.0, vec![legacy_stop]), true, 10.0, FRESH_GROUP).unwrap();

    let response = session(&client, &snapshots)
        .submit_exit(&spy(), &exit_request("limit", 700.0), plan, 200)
        .await;

    assert!(!response.ok);
    assert_eq!(response.status, "rejected");
    assert!(
        response
            .message
            .contains("The earlier exit orders #88 were re-placed as #200"),
        "{}",
        response.message
    );
    let order: Vec<Sent> = drain(&mut received)
        .into_iter()
        .map(|(sent, _)| sent)
        .collect();
    // The copy replaced the stop before the exit was sent, so nothing is undone.
    assert_eq!(
        order,
        vec![Sent::Place(200), Sent::Cancel(88), Sent::Place(201)]
    );
}

#[tokio::test]
async fn a_slow_exit_acknowledgement_never_leaves_an_original_beside_its_copy() {
    let (client, mut received) = scripted_client(Script {
        silent: [201].into_iter().collect(),
        ..Script::default()
    })
    .await;
    let snapshots = Mutex::new(());
    let legacy_stop = order(88, "STP", 620.0);
    let plan = plan_new_exit(&book(10.0, vec![legacy_stop]), true, 10.0, FRESH_GROUP).unwrap();

    let response = LinkedSession::new(
        &client,
        ACCOUNT,
        CLIENT_ID,
        &snapshots,
        Duration::from_secs(3),
    )
    .submit_exit(&spy(), &exit_request("limit", 700.0), plan, 200)
    .await;

    assert_eq!(response.status, "indeterminate", "{response:?}");
    let order: Vec<Sent> = drain(&mut received)
        .into_iter()
        .map(|(sent, _)| sent)
        .collect();
    // The original was cancelled before the exit was sent, so the slow
    // acknowledgement cannot strand it beside its copy.
    assert_eq!(
        order,
        vec![Sent::Place(200), Sent::Cancel(88), Sent::Place(201)]
    );
}

#[tokio::test]
async fn a_filled_copy_retires_its_group_and_places_no_exit() {
    let (client, mut received) = scripted_client(Script {
        fill: [200].into_iter().collect(),
        ..Script::default()
    })
    .await;
    let snapshots = Mutex::new(());
    let take_profit = in_group(order(101, "LMT", 700.0), "old", OcaType::ReduceWithBlock);
    let stop_loss = in_group(order(102, "STP", 620.0), "old", OcaType::ReduceWithBlock);
    let plan = NewExitPlan {
        group: FRESH_GROUP.to_string(),
        quantity: 10.0,
        position_quantity: 10.0,
        joined: Vec::new(),
        relink: vec![take_profit, stop_loss],
    };

    let response = session(&client, &snapshots)
        .submit_exit(&spy(), &exit_request("limit", 710.0), plan, 200)
        .await;

    assert!(!response.ok);
    assert_eq!(response.status, "rejected");
    assert_eq!(response.order_id, None, "the exit was never sent");
    assert!(
        response
            .message
            .contains("filled the re-placed copy of exit order #101"),
        "{}",
        response.message
    );
    assert!(
        response
            .message
            .contains("exit orders #102 could not be re-placed and IBKR cancelled them"),
        "{}",
        response.message
    );
    let order: Vec<Sent> = drain(&mut received)
        .into_iter()
        .map(|(sent, _)| sent)
        .collect();
    // The filled copy's original goes (with its group) so it cannot fill again.
    assert_eq!(order, vec![Sent::Place(200), Sent::Cancel(101)]);
}

#[tokio::test]
async fn cancelling_after_a_filled_sibling_copy_names_the_dropped_exits() {
    let (client, mut received) = scripted_client(Script {
        fill: [300].into_iter().collect(),
        ..Script::default()
    })
    .await;
    let snapshots = Mutex::new(());
    let take_profit = in_group(
        order(101, "LMT", 700.0),
        BRACKET_GROUP,
        OcaType::ReduceWithBlock,
    );
    let stop_loss = in_group(
        sized(order(102, "STP", 620.0), 5.0),
        BRACKET_GROUP,
        OcaType::ReduceWithBlock,
    );
    let second_stop = in_group(
        sized(order(103, "STP", 610.0), 5.0),
        BRACKET_GROUP,
        OcaType::ReduceWithBlock,
    );
    let book = book(10.0, vec![take_profit, stop_loss, second_stop]);
    let CancelPlan::Relink { group, relink } = plan_cancel(&book, 101, FRESH_GROUP).unwrap() else {
        panic!("the siblings must be re-placed");
    };

    let response = session(&client, &snapshots)
        .cancel_keeping_siblings(&book, 101, &group, &relink, 300)
        .await;

    assert!(response.ok, "{response:?}");
    assert!(
        response
            .message
            .contains("linked exit orders #103 could not be re-placed"),
        "{}",
        response.message
    );
    let order: Vec<Sent> = drain(&mut received)
        .into_iter()
        .map(|(sent, _)| sent)
        .collect();
    assert_eq!(order, vec![Sent::Place(300), Sent::Cancel(101)]);
}

#[test]
fn listed_order_names_are_bounded() {
    assert_eq!(order_names([41, 42].into_iter()), "#41, #42");
    let many = order_names(1..=20);
    assert!(many.ends_with("#8 and 12 more"), "{many}");
    let foreign_stops = (1..=40)
        .map(|order_id| foreign(order(order_id, "STP", 600.0)))
        .collect();
    let error = plan_new_exit(&book(10.0, foreign_stops), true, 10.0, FRESH_GROUP).unwrap_err();
    assert!(error.contains("and 32 more"), "{error}");
    assert!(error.len() < 1_000, "{}", error.len());
}

#[test]
fn only_this_clients_working_orders_can_be_cancelled() {
    let orders = vec![order(42, "STP", 620.0), foreign(order(43, "STP", 620.0))];
    assert_eq!(
        own_working_order(&orders, CLIENT_ID, 42).map(|order| order.order_id),
        Some(42)
    );
    assert!(own_working_order(&orders, CLIENT_ID, 43).is_none());
    assert!(own_working_order(&orders, OTHER_CLIENT_ID, 42).is_none());
}

#[tokio::test]
async fn joining_a_group_sends_only_the_exit() {
    let (client, mut received) = session_client(&[]).await;
    let snapshots = Mutex::new(());
    let take_profit = in_group(
        order(101, "LMT", 700.0),
        BRACKET_GROUP,
        OcaType::ReduceWithBlock,
    );
    let stop_loss = in_group(
        order(102, "STP", 620.0),
        BRACKET_GROUP,
        OcaType::ReduceWithBlock,
    );
    let plan = plan_new_exit(
        &book(10.0, vec![take_profit, stop_loss]),
        true,
        10.0,
        FRESH_GROUP,
    )
    .unwrap();

    let response = session(&client, &snapshots)
        .submit_exit(&spy(), &exit_request("stop_market", 630.0), plan, 200)
        .await;

    assert!(response.ok, "{response:?}");
    let exit = response.exit.expect("linked exit outcome");
    assert_eq!(exit.oca_group, BRACKET_GROUP);
    assert_eq!(
        exit.linked_order_ids,
        vec!["101".to_string(), "102".to_string()]
    );
    assert!(exit.relinked.is_empty());
    let sent = drain(&mut received);
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0].0, Sent::Place(200));
    let exit_order = placed_order(&sent[0].1);
    assert_eq!(wire::string(&exit_order, 8).as_deref(), Some("STP"));
    assert_eq!(
        wire::string(&exit_order, 27).as_deref(),
        Some(BRACKET_GROUP)
    );
    assert_eq!(wire::int(&exit_order, 30), Some(2));
}

#[tokio::test]
async fn cancelling_one_linked_exit_re_places_its_sibling_first() {
    let (client, mut received) = session_client(&[]).await;
    let snapshots = Mutex::new(());
    let take_profit = in_group(
        order(101, "LMT", 700.0),
        BRACKET_GROUP,
        OcaType::ReduceWithBlock,
    );
    let stop_loss = in_group(
        order(102, "STP", 620.0),
        BRACKET_GROUP,
        OcaType::ReduceWithBlock,
    );
    let book = book(10.0, vec![take_profit, stop_loss]);
    let CancelPlan::Relink { group, relink } = plan_cancel(&book, 101, FRESH_GROUP).unwrap() else {
        panic!("the sibling must be re-placed");
    };

    let response = session(&client, &snapshots)
        .cancel_keeping_siblings(&book, 101, &group, &relink, 300)
        .await;

    assert!(response.ok, "{response:?}");
    assert_eq!(response.status, "cancelled");
    assert!(
        response
            .message
            .contains("Kept its linked exits #102 as #300"),
        "{}",
        response.message
    );
    let sent = drain(&mut received);
    let order: Vec<&Sent> = sent.iter().map(|(sent, _)| sent).collect();
    assert_eq!(order, vec![&Sent::Place(300), &Sent::Cancel(101)]);
    let copy = placed_order(&sent[0].1);
    assert_eq!(wire::string(&copy, 8).as_deref(), Some("STP"));
    assert_eq!(wire::string(&copy, 27).as_deref(), Some(FRESH_GROUP));
}

#[tokio::test]
async fn a_rejected_sibling_copy_keeps_the_target_working() {
    let (client, mut received) = session_client(&[300]).await;
    let snapshots = Mutex::new(());
    let take_profit = in_group(
        order(101, "LMT", 700.0),
        BRACKET_GROUP,
        OcaType::ReduceWithBlock,
    );
    let stop_loss = in_group(
        order(102, "STP", 620.0),
        BRACKET_GROUP,
        OcaType::ReduceWithBlock,
    );
    let book = book(10.0, vec![take_profit, stop_loss]);
    let CancelPlan::Relink { group, relink } = plan_cancel(&book, 101, FRESH_GROUP).unwrap() else {
        panic!("the sibling must be re-placed");
    };

    let response = session(&client, &snapshots)
        .cancel_keeping_siblings(&book, 101, &group, &relink, 300)
        .await;

    assert!(!response.ok);
    assert_eq!(response.status, "rejected");
    assert!(
        response.message.contains("was not cancelled"),
        "{}",
        response.message
    );
    let order: Vec<Sent> = drain(&mut received)
        .into_iter()
        .map(|(sent, _)| sent)
        .collect();
    assert_eq!(
        order,
        vec![Sent::Place(300)],
        "the target is never cancelled"
    );
}
