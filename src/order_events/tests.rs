//! Fixture tests for order-event normalization, replay and the hub pump.

use super::*;
use ibapi::contracts::{Contract, SecurityType};
use ibapi::orders::{Execution, Order, OrderState, OrderStatusKind};
use ibapi::prelude::Notice;

const ACCOUNT: &str = "DU1234567";
const CLIENT: i32 = 17_654;
const PARENT_ID: i32 = 100;
const TAKE_PROFIT_ID: i32 = 101;
const STOP_LOSS_ID: i32 = 102;
const TAKE_PROFIT_REF: &str = "sunk-1b2c3d4e-5f60-4a71-8b92-a3b4c5d6e7f8";

fn new_york() -> Option<&'static Tz> {
    timezones::get_by_name("America/New_York")
}

fn stock() -> Contract {
    let mut contract = Contract::stock("SPY").build();
    contract.contract_id = 756_733;
    contract.local_symbol = "SPY".to_string();
    contract
}

fn open_order(order_id: i32, parent_id: i32, order_type: &str, order_ref: &str) -> OrderUpdate {
    let order = Order {
        order_id,
        client_id: CLIENT,
        perm_id: 900_000 + i64::from(order_id),
        action: Action::Sell,
        total_quantity: 10.0,
        order_type: order_type.to_string(),
        parent_id,
        order_ref: order_ref.to_string(),
        account: ACCOUNT.to_string(),
        ..Order::default()
    };
    OrderUpdate::OpenOrder(OrderData {
        order_id,
        contract: stock(),
        order,
        order_state: OrderState::default(),
    })
}

/// A linked-exit group name as the Gateway builds it (`oca-sunk-<uuid>`).
const LINKED_GROUP: &str = "oca-sunk-0a1b2c3d-4e5f-4a60-8b71-c2d3e4f5a6b7";

/// `open_order` placed into an OCA group of the given type.
fn grouped_order(
    order_id: i32,
    parent_id: i32,
    order_type: &str,
    group: &str,
    oca_type: ibapi::orders::OcaType,
) -> OrderUpdate {
    let mut update = open_order(order_id, parent_id, order_type, "");
    if let OrderUpdate::OpenOrder(data) = &mut update {
        data.order.oca_group = group.to_string();
        data.order.oca_type = oca_type;
    }
    update
}

fn status(order_id: i32, kind: OrderStatusKind, filled: f64, remaining: f64) -> OrderUpdate {
    OrderUpdate::OrderStatus(OrderStatus {
        order_id,
        status: kind,
        filled,
        remaining,
        average_fill_price: (filled > 0.0).then_some(700.0),
        perm_id: 900_000 + i64::from(order_id),
        parent_id: if order_id == PARENT_ID { 0 } else { PARENT_ID },
        last_fill_price: (filled > 0.0).then_some(700.0),
        client_id: CLIENT,
        ..OrderStatus::default()
    })
}

fn execution(order_id: i32, exec_id: &str, shares: f64, time: &str) -> OrderUpdate {
    OrderUpdate::ExecutionData(ExecutionData {
        request_id: -1,
        contract: stock(),
        execution: Execution {
            order_id,
            client_id: CLIENT,
            execution_id: exec_id.to_string(),
            time: time.to_string(),
            account_number: ACCOUNT.to_string(),
            side: ExecutionSide::Sold,
            shares,
            price: 700.0,
            perm_id: 900_000 + i64::from(order_id),
            cumulative_quantity: shares,
            average_price: 700.0,
            order_reference: TAKE_PROFIT_REF.to_string(),
            ..Execution::default()
        },
    })
}

fn commission(exec_id: &str, realized_pnl: Option<f64>) -> OrderUpdate {
    OrderUpdate::CommissionReport(CommissionReport {
        execution_id: exec_id.to_string(),
        commission: 1.0,
        currency: "USD".to_string(),
        realized_pnl,
        ..CommissionReport::default()
    })
}

fn only_execution(events: Vec<IbkrOrderEvent>) -> IbkrExecutionEvent {
    match events.as_slice() {
        [IbkrOrderEvent::Execution(execution)] => execution.clone(),
        other => panic!("expected one execution, got {other:?}"),
    }
}

fn only_status(events: Vec<IbkrOrderEvent>) -> IbkrOrderStatusEvent {
    match events.as_slice() {
        [IbkrOrderEvent::Status(status)] => status.clone(),
        other => panic!("expected one status, got {other:?}"),
    }
}

#[test]
fn a_take_profit_child_fill_carries_its_parent_type_reference_and_time() {
    let mut normalizer = OrderEventNormalizer::new(new_york());
    assert!(
        normalizer
            .observe(open_order(
                TAKE_PROFIT_ID,
                PARENT_ID,
                "lmt",
                TAKE_PROFIT_REF
            ))
            .is_empty()
    );
    let event = only_execution(normalizer.observe(execution(
        TAKE_PROFIT_ID,
        "0000e0d5.6578a1b2.01.01",
        10.0,
        "20260930 14:30:01 US/Eastern",
    )));
    assert_eq!(event.account_ref, ACCOUNT);
    assert_eq!(event.parent_id, Some(PARENT_ID));
    assert_eq!(event.order_type.as_deref(), Some("LMT"));
    assert_eq!(event.order_ref.as_deref(), Some(TAKE_PROFIT_REF));
    assert_eq!(event.side, OrderEventSide::Sell);
    assert_eq!(event.security_type, "STK");
    assert_eq!(event.symbol, "SPY");
    // 14:30:01 EDT is 18:30:01 UTC.
    assert_eq!(event.executed_at_ms, Some(1_790_793_001_000));
    assert_eq!(event.realized_pnl, None);
}

#[test]
fn a_commission_report_re_emits_the_execution_with_realized_pnl() {
    let mut normalizer = OrderEventNormalizer::new(new_york());
    normalizer.observe(open_order(
        TAKE_PROFIT_ID,
        PARENT_ID,
        "LMT",
        TAKE_PROFIT_REF,
    ));
    normalizer.observe(execution(TAKE_PROFIT_ID, "e1", 10.0, "20260930-18:30:01"));
    let merged = only_execution(normalizer.observe(commission("e1", Some(512.5))));
    assert_eq!(merged.exec_id, "e1");
    assert_eq!(merged.realized_pnl, Some(512.5));
    assert_eq!(merged.commission, Some(1.0));
    assert_eq!(merged.commission_currency.as_deref(), Some("USD"));
    // The same report again changes nothing.
    assert!(normalizer.observe(commission("e1", Some(512.5))).is_empty());
}

#[test]
fn a_commission_report_before_its_execution_is_applied_when_the_execution_arrives() {
    let mut normalizer = OrderEventNormalizer::new(None);
    assert!(normalizer.observe(commission("e1", Some(-20.0))).is_empty());
    let event = only_execution(normalizer.observe(execution(
        STOP_LOSS_ID,
        "e1",
        10.0,
        "20260930-18:30:01",
    )));
    assert_eq!(event.realized_pnl, Some(-20.0));
}

#[test]
fn an_unset_realized_pnl_is_not_a_value() {
    let mut normalizer = OrderEventNormalizer::new(None);
    normalizer.observe(execution(PARENT_ID, "e1", 10.0, "20260930-18:30:01"));
    let event = only_execution(normalizer.observe(commission("e1", Some(f64::MAX))));
    assert_eq!(event.realized_pnl, None);
}

#[test]
fn an_execution_is_forwarded_once_however_often_tws_repeats_it() {
    let mut normalizer = OrderEventNormalizer::new(None);
    let first = normalizer.observe(execution(PARENT_ID, "e1", 10.0, "20260930-18:30:01"));
    assert_eq!(first.len(), 1);
    // An execution snapshot (reconciliation, priming) repeats it.
    assert!(
        normalizer
            .observe(execution(PARENT_ID, "e1", 10.0, "20260930-18:30:01"))
            .is_empty()
    );
}

#[test]
fn partial_fills_are_separate_executions_of_one_order() {
    let mut normalizer = OrderEventNormalizer::new(None);
    normalizer.observe(open_order(STOP_LOSS_ID, PARENT_ID, "STP", "sunk-stop"));
    let first =
        only_execution(normalizer.observe(execution(STOP_LOSS_ID, "e1", 4.0, "20260930-18:30:01")));
    let second =
        only_execution(normalizer.observe(execution(STOP_LOSS_ID, "e2", 6.0, "20260930-18:30:02")));
    assert_eq!(first.order_type.as_deref(), Some("STP"));
    assert_eq!(second.order_id, first.order_id);
    assert_eq!(first.quantity + second.quantity, 10.0);
}

#[test]
fn an_execution_before_its_order_details_is_enriched_and_re_sent_when_they_arrive() {
    let mut normalizer = OrderEventNormalizer::new(None);
    let bare = only_execution(normalizer.observe(execution(
        TAKE_PROFIT_ID,
        "e1",
        10.0,
        "20260930-18:30:01",
    )));
    assert_eq!(bare.parent_id, None);
    assert_eq!(bare.order_type, None);
    assert_eq!(bare.order_ref.as_deref(), Some(TAKE_PROFIT_REF));

    let enriched = only_execution(normalizer.observe(open_order(
        TAKE_PROFIT_ID,
        PARENT_ID,
        "LMT",
        TAKE_PROFIT_REF,
    )));
    assert_eq!(enriched.exec_id, "e1");
    assert_eq!(enriched.parent_id, Some(PARENT_ID));
    assert_eq!(enriched.order_type.as_deref(), Some("LMT"));
    // Repeated details change nothing and send nothing.
    assert!(
        normalizer
            .observe(open_order(
                TAKE_PROFIT_ID,
                PARENT_ID,
                "LMT",
                TAKE_PROFIT_REF
            ))
            .is_empty()
    );
    // The commission re-send carries the enriched copy.
    let merged = only_execution(normalizer.observe(commission("e1", Some(512.5))));
    assert_eq!(merged.parent_id, Some(PARENT_ID));
    assert_eq!(merged.order_type.as_deref(), Some("LMT"));
    assert_eq!(merged.realized_pnl, Some(512.5));
    // The replay for a reconnecting consumer is enriched too.
    assert!(matches!(
        normalizer.replay(ACCOUNT).as_slice(),
        [IbkrOrderEvent::Execution(execution)] if execution.parent_id == Some(PARENT_ID)
    ));
}

#[test]
fn a_completed_order_known_only_by_perm_id_names_its_executions() {
    let mut normalizer = OrderEventNormalizer::new(None);
    // After a restart the replayed execution comes from another TWS session.
    let mut replayed = execution(STOP_LOSS_ID, "e1", 10.0, "20260930-18:30:01");
    if let OrderUpdate::ExecutionData(data) = &mut replayed {
        data.execution.order_id = 0;
        data.execution.client_id = 0;
    }
    assert_eq!(only_execution(normalizer.observe(replayed)).parent_id, None);
    // Completed orders carry the permanent id but no usable TWS order id.
    let OrderUpdate::OpenOrder(mut completed) = open_order(STOP_LOSS_ID, PARENT_ID, "STP", "r")
    else {
        unreachable!("open_order builds an OpenOrder");
    };
    completed.order_id = 0;
    let events = normalizer.observe_order_details(&completed);
    let enriched = only_execution(events);
    assert_eq!(enriched.parent_id, Some(PARENT_ID));
    assert_eq!(enriched.order_type.as_deref(), Some("STP"));
    // A later execution of that order is named at once.
    let mut later = execution(STOP_LOSS_ID, "e2", 1.0, "20260930-18:30:02");
    if let OrderUpdate::ExecutionData(data) = &mut later {
        data.execution.order_id = 0;
    }
    assert_eq!(
        only_execution(normalizer.observe(later)).parent_id,
        Some(PARENT_ID)
    );
}

#[test]
fn details_of_another_account_never_enrich_an_execution() {
    let mut normalizer = OrderEventNormalizer::new(None);
    normalizer.observe(execution(TAKE_PROFIT_ID, "e1", 10.0, "20260930-18:30:01"));
    let OrderUpdate::OpenOrder(mut other) = open_order(TAKE_PROFIT_ID, PARENT_ID, "LMT", "r")
    else {
        unreachable!("open_order builds an OpenOrder");
    };
    other.order.account = "U7654321".to_string();
    assert!(normalizer.observe_order_details(&other).is_empty());
}

#[test]
fn malformed_executions_are_dropped() {
    let mut normalizer = OrderEventNormalizer::new(None);
    assert!(
        normalizer
            .observe(execution(PARENT_ID, "", 10.0, ""))
            .is_empty()
    );
    assert!(
        normalizer
            .observe(execution(PARENT_ID, "e0", 0.0, ""))
            .is_empty()
    );
    assert!(
        normalizer
            .observe(execution(PARENT_ID, "e2", f64::NAN, ""))
            .is_empty()
    );
}

#[test]
fn a_status_is_forwarded_only_when_it_changes() {
    let mut normalizer = OrderEventNormalizer::new(None);
    normalizer.observe(open_order(
        TAKE_PROFIT_ID,
        PARENT_ID,
        "LMT",
        TAKE_PROFIT_REF,
    ));
    let submitted = only_status(normalizer.observe(status(
        TAKE_PROFIT_ID,
        OrderStatusKind::Submitted,
        0.0,
        10.0,
    )));
    assert_eq!(submitted.status, "Submitted");
    assert_eq!(submitted.parent_id, Some(PARENT_ID));
    assert_eq!(submitted.order_type, "LMT");
    // Every open-orders read repeats the same status.
    for _ in 0..3 {
        assert!(
            normalizer
                .observe(status(
                    TAKE_PROFIT_ID,
                    OrderStatusKind::Submitted,
                    0.0,
                    10.0
                ))
                .is_empty()
        );
        assert!(
            normalizer
                .observe(open_order(
                    TAKE_PROFIT_ID,
                    PARENT_ID,
                    "LMT",
                    TAKE_PROFIT_REF
                ))
                .is_empty()
        );
    }
    let partial = only_status(normalizer.observe(status(
        TAKE_PROFIT_ID,
        OrderStatusKind::Submitted,
        4.0,
        6.0,
    )));
    assert_eq!((partial.filled, partial.remaining), (4.0, 6.0));
    assert_eq!(partial.avg_fill_price, Some(700.0));
    let filled =
        only_status(normalizer.observe(status(TAKE_PROFIT_ID, OrderStatusKind::Filled, 10.0, 0.0)));
    assert_eq!(filled.status, "Filled");
}

#[test]
fn a_status_before_the_order_details_is_forwarded_once_they_arrive() {
    let mut normalizer = OrderEventNormalizer::new(None);
    assert!(
        normalizer
            .observe(status(
                STOP_LOSS_ID,
                OrderStatusKind::PreSubmitted,
                0.0,
                10.0
            ))
            .is_empty()
    );
    let event = only_status(normalizer.observe(open_order(STOP_LOSS_ID, PARENT_ID, "STP", "r")));
    assert_eq!(event.status, "PreSubmitted");
    assert_eq!(event.account_ref, ACCOUNT);
}

#[test]
fn the_order_cache_is_bounded() {
    let mut normalizer = OrderEventNormalizer::new(None);
    for order_id in 1..=(MAX_TRACKED_ORDERS as i32 + 10) {
        normalizer.observe(open_order(order_id, 0, "LMT", "r"));
        normalizer.observe(status(order_id, OrderStatusKind::Submitted, 0.0, 10.0));
    }
    assert_eq!(normalizer.orders.values.len(), MAX_TRACKED_ORDERS);
    assert_eq!(normalizer.statuses.values.len(), MAX_TRACKED_ORDERS);
    assert_eq!(normalizer.orders.order.len(), MAX_TRACKED_ORDERS);
    for index in 0..(MAX_TRACKED_EXECUTIONS + 10) {
        normalizer.observe(execution(1, &format!("e{index}"), 1.0, ""));
    }
    assert_eq!(normalizer.executions.values.len(), MAX_TRACKED_EXECUTIONS);
}

#[test]
fn replay_returns_one_accounts_latest_statuses_then_executions() {
    let mut normalizer = OrderEventNormalizer::new(None);
    normalizer.observe(open_order(
        TAKE_PROFIT_ID,
        PARENT_ID,
        "LMT",
        TAKE_PROFIT_REF,
    ));
    normalizer.observe(status(
        TAKE_PROFIT_ID,
        OrderStatusKind::Submitted,
        0.0,
        10.0,
    ));
    normalizer.observe(status(TAKE_PROFIT_ID, OrderStatusKind::Filled, 10.0, 0.0));
    normalizer.observe(execution(TAKE_PROFIT_ID, "e1", 10.0, "20260930-18:30:01"));
    let replay = normalizer.replay(ACCOUNT);
    assert_eq!(replay.len(), 2);
    assert!(matches!(&replay[0], IbkrOrderEvent::Status(status) if status.status == "Filled"));
    assert!(
        matches!(&replay[1], IbkrOrderEvent::Execution(execution) if execution.exec_id == "e1")
    );
    assert!(normalizer.replay("U7654321").is_empty());
}

#[test]
fn execution_times_parse_every_tws_format() {
    let zone = new_york();
    // Named zone, session zone (two spaces, as older TWS sends) and UTC.
    assert_eq!(
        parse_execution_time("20260930 14:30:01 US/Eastern", None),
        Some(1_790_793_001_000)
    );
    assert_eq!(
        parse_execution_time("20260930  14:30:01", zone),
        Some(1_790_793_001_000)
    );
    assert_eq!(
        parse_execution_time("20260930-18:30:01", None),
        Some(1_790_793_001_000)
    );
    assert_eq!(
        parse_execution_time("20260930 18:30:01 UTC", None),
        Some(1_790_793_001_000)
    );
    // Winter time: 14:30 EST is 19:30 UTC.
    assert_eq!(
        parse_execution_time("20260115 14:30:00 America/New_York", None),
        Some(1_768_505_400_000)
    );
}

#[test]
fn execution_times_never_guess() {
    assert_eq!(parse_execution_time("20260930 14:30:01", None), None);
    assert_eq!(
        parse_execution_time("20260930 14:30:01 Mars/Olympus", None),
        None
    );
    assert_eq!(parse_execution_time("2026-09-30 14:30:01", None), None);
    assert_eq!(parse_execution_time("20261330 14:30:01 UTC", None), None);
    assert_eq!(parse_execution_time("", None), None);
}

#[test]
fn order_event_retries_skip_input_errors() {
    assert!(should_retry_order_events(&GatewayError::Unavailable(
        "lost".to_string()
    )));
    assert!(should_retry_order_events(&GatewayError::Provider(
        "refused".to_string()
    )));
    for error in [
        GatewayError::Input("select one of the accounts returned by IBKR".to_string()),
        GatewayError::Configuration("bad".to_string()),
        GatewayError::Contract("bad".to_string()),
    ] {
        assert!(!should_retry_order_events(&error));
    }
}

fn hub_state() -> Arc<Mutex<HubState>> {
    Arc::new(Mutex::new(HubState::new(None)))
}

fn subscribe(state: &Arc<Mutex<HubState>>, account_ref: &str) -> OrderEventSubscription {
    lock_state(state).subscribe(account_ref.to_string())
}

fn held_until_open_notice() -> Notice {
    Notice {
        code: 399,
        message: "Order Message: Warning: your order will not be placed at the exchange until 2026-10-01 09:30:00 US/Eastern".to_string(),
        error_time: None,
        advanced_order_reject_json: String::new(),
    }
}

#[tokio::test]
async fn warning_notices_do_not_end_the_order_stream() {
    let state = hub_state();
    let mut consumer = subscribe(&state, ACCOUNT);
    let updates = futures::stream::iter(vec![
        Ok(SubscriptionItem::Data(open_order(
            TAKE_PROFIT_ID,
            PARENT_ID,
            "LMT",
            TAKE_PROFIT_REF,
        ))),
        Ok(SubscriptionItem::Notice(held_until_open_notice())),
        Ok(SubscriptionItem::Data(execution(
            TAKE_PROFIT_ID,
            "e1",
            10.0,
            "20260930-18:30:01",
        ))),
    ]);
    pump_order_updates(updates, Arc::clone(&state)).await;
    let event = consumer.next().await.expect("event").expect("not an error");
    assert!(matches!(event, IbkrOrderEvent::Execution(execution) if execution.exec_id == "e1"));
    // The provider stream then ended, which the consumer sees as an outage.
    assert!(matches!(
        consumer.next().await,
        Some(Err(GatewayError::Unavailable(_)))
    ));
}

#[tokio::test]
async fn a_provider_error_ends_the_order_stream_as_an_outage() {
    let state = hub_state();
    let mut consumer = subscribe(&state, ACCOUNT);
    let updates = futures::stream::iter(vec![Err(ibapi::Error::ConnectionReset)]);
    pump_order_updates(updates, Arc::clone(&state)).await;
    assert!(matches!(
        consumer.next().await,
        Some(Err(GatewayError::Unavailable(message))) if message.contains("failed")
    ));
}

#[tokio::test]
async fn a_consumer_sees_only_its_account_and_replays_what_it_missed() {
    let state = hub_state();
    let other_account = {
        let mut update = execution(STOP_LOSS_ID, "other", 1.0, "20260930-18:30:01");
        if let OrderUpdate::ExecutionData(data) = &mut update {
            data.execution.account_number = "U7654321".to_string();
        }
        update
    };
    pump_order_updates(
        futures::stream::iter(vec![
            Ok(SubscriptionItem::Data(other_account)),
            Ok(SubscriptionItem::Data(execution(
                TAKE_PROFIT_ID,
                "missed",
                10.0,
                "20260930-18:30:01",
            ))),
        ]),
        Arc::clone(&state),
    )
    .await;
    // Subscribing after the fill replays it; the other account's never appears.
    let mut late = subscribe(&state, ACCOUNT);
    let replayed = late.next().await.expect("replay").expect("not an error");
    assert!(
        matches!(replayed, IbkrOrderEvent::Execution(execution) if execution.exec_id == "missed")
    );
}

#[test]
fn security_type_strings_match_ibkr_codes() {
    assert_eq!(SecurityType::Stock.to_string(), "STK");
    assert_eq!(SecurityType::Option.to_string(), "OPT");
}

#[tokio::test]
async fn a_consumer_subscribing_after_the_hub_ended_is_told_at_once() {
    let state = hub_state();
    pump_order_updates(futures::stream::iter(Vec::new()), Arc::clone(&state)).await;
    assert!(lock_state(&state).ended.is_some());
    let mut late = subscribe(&state, ACCOUNT);
    let outcome = tokio::time::timeout(Duration::from_secs(1), late.next())
        .await
        .expect("an ended hub never leaves a consumer waiting");
    assert!(matches!(outcome, Some(Err(GatewayError::Unavailable(_)))));
}

#[test]
fn windows_and_localized_zone_names_resolve_like_ibapi() {
    // 14:30 in Shanghai (UTC+8) is 06:30 UTC.
    assert_eq!(
        parse_execution_time("20260930 14:30:01 China Standard Time", None),
        Some(1_790_749_801_000)
    );
    assert_eq!(
        parse_execution_time("20260930 14:30:01 中国标准时间", None),
        Some(1_790_749_801_000)
    );
    // 14:30 CEST (UTC+2) is 12:30 UTC.
    assert_eq!(
        parse_execution_time("20260930 14:30:01 W. Europe Standard Time", None),
        Some(1_790_771_401_000)
    );
}

fn huge_execution() -> OrderUpdate {
    let mut update = execution(TAKE_PROFIT_ID, "e-huge", 10.0, "20260930-18:30:01");
    if let OrderUpdate::ExecutionData(data) = &mut update {
        data.execution.cumulative_quantity = -5.0;
        data.execution.average_price = f64::MAX;
        data.execution.perm_id = i64::MAX;
        data.execution.order_reference = "r".repeat(MAX_IDENTIFIER_CHARS + 1);
        data.contract.local_symbol = "L".repeat(MAX_INSTRUMENT_CHARS + 1);
    }
    update
}

#[test]
fn out_of_range_fields_are_cleared_to_fit_the_renderer_contract() {
    let mut normalizer = OrderEventNormalizer::new(None);
    let event = only_execution(normalizer.observe(huge_execution()));
    assert_eq!(event.cumulative_quantity, 0.0);
    assert_eq!(event.avg_price, 0.0);
    assert_eq!(event.perm_id, 0);
    assert_eq!(event.order_ref, None);
    assert_eq!(event.local_symbol, None);
    let pnl = only_execution(normalizer.observe(commission("e-huge", Some(-2e15))));
    assert_eq!(pnl.realized_pnl, None);

    normalizer.observe(open_order(STOP_LOSS_ID, PARENT_ID, "STP", "r"));
    let status = only_status(normalizer.observe(status(
        STOP_LOSS_ID,
        OrderStatusKind::Submitted,
        -1.0,
        f64::INFINITY,
    )));
    assert_eq!((status.filled, status.remaining), (0.0, 0.0));
}

#[test]
fn events_that_cannot_fit_the_renderer_contract_are_dropped() {
    let mut normalizer = OrderEventNormalizer::new(None);
    for edit in [
        (|data: &mut ExecutionData| data.execution.shares = 2e9) as fn(&mut ExecutionData),
        |data| data.execution.price = 1e10,
        |data| data.execution.execution_id = "x".repeat(MAX_IDENTIFIER_CHARS + 1),
        |data| data.execution.account_number = "a".repeat(MAX_IDENTIFIER_CHARS + 1),
        |data| data.contract.currency = "C".repeat(MAX_CURRENCY_CHARS + 1).into(),
        |data| data.contract.symbol = "S".repeat(MAX_INSTRUMENT_CHARS + 1).into(),
    ] {
        let mut update = execution(TAKE_PROFIT_ID, "e-bad", 10.0, "20260930-18:30:01");
        if let OrderUpdate::ExecutionData(data) = &mut update {
            edit(data);
        }
        assert!(normalizer.observe(update).is_empty());
    }
    // Details whose order type cannot be sent never name a status.
    let OrderUpdate::OpenOrder(mut long_type) = open_order(STOP_LOSS_ID, PARENT_ID, "STP", "r")
    else {
        unreachable!("open_order builds an OpenOrder");
    };
    long_type.order.order_type = "T".repeat(MAX_LABEL_CHARS + 1);
    assert!(normalizer.observe_order_details(&long_type).is_empty());
    assert!(
        normalizer
            .observe(status(STOP_LOSS_ID, OrderStatusKind::Submitted, 0.0, 10.0))
            .is_empty()
    );
}

#[test]
fn the_shared_bounds_fixture_is_what_the_normalizer_emits_at_the_limits() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../fixtures/v3/contract.json"))
            .expect("valid shared contract fixture");
    let mut normalizer = OrderEventNormalizer::new(None);
    let mut update = execution(TAKE_PROFIT_ID, "e-limits", 10.0, "20260930-18:30:01");
    if let OrderUpdate::ExecutionData(data) = &mut update {
        data.execution.shares = MAX_QUANTITY_OR_PRICE;
        data.execution.price = MAX_QUANTITY_OR_PRICE;
        data.execution.cumulative_quantity = MAX_QUANTITY_OR_PRICE;
        data.execution.average_price = 0.0;
        data.execution.perm_id = MAX_SAFE_INTEGER;
        data.execution.order_reference = "r".repeat(MAX_IDENTIFIER_CHARS);
        data.contract.symbol = "S".repeat(MAX_INSTRUMENT_CHARS).into();
        data.contract.local_symbol = "L".repeat(MAX_INSTRUMENT_CHARS);
    }
    normalizer.observe(update);
    let event =
        only_execution(normalizer.observe(commission("e-limits", Some(-MAX_SIGNED_AMOUNT))));
    let wire = serde_json::to_value(crate::contract::StreamEvent::OrderEvent {
        version: crate::contract::PROTOCOL_VERSION,
        order: IbkrOrderEvent::Execution(event),
    })
    .expect("serialize");
    assert_eq!(wire, fixture["orderExecutionEventAtLimits"]);
}

#[test]
fn a_linked_exit_stop_carries_its_oca_group_without_a_parent() {
    use ibapi::orders::OcaType;
    let mut normalizer = OrderEventNormalizer::new(None);
    assert!(
        normalizer
            .observe(grouped_order(
                STOP_LOSS_ID,
                0,
                "STP",
                LINKED_GROUP,
                OcaType::ReduceWithBlock
            ))
            .is_empty()
    );
    let status = only_status(normalizer.observe(status(
        STOP_LOSS_ID,
        OrderStatusKind::Submitted,
        0.0,
        10.0,
    )));
    assert_eq!(status.oca_group.as_deref(), Some(LINKED_GROUP));
    let mut fill = execution(STOP_LOSS_ID, "e-stop", 10.0, "20260930-18:30:01");
    if let OrderUpdate::ExecutionData(data) = &mut fill {
        data.execution.order_reference = String::new();
    }
    let event = only_execution(normalizer.observe(fill));
    assert_eq!(event.parent_id, None);
    assert_eq!(event.order_type.as_deref(), Some("STP"));
    assert_eq!(event.oca_group.as_deref(), Some(LINKED_GROUP));
}

#[test]
fn an_execution_before_its_details_gains_its_oca_group_when_they_arrive() {
    use ibapi::orders::OcaType;
    let mut normalizer = OrderEventNormalizer::new(None);
    let bare = only_execution(normalizer.observe(execution(
        STOP_LOSS_ID,
        "e-early",
        10.0,
        "20260930-18:30:01",
    )));
    assert_eq!(bare.oca_group, None);
    let enriched = only_execution(normalizer.observe(grouped_order(
        STOP_LOSS_ID,
        0,
        "STP",
        LINKED_GROUP,
        OcaType::ReduceWithBlock,
    )));
    assert_eq!(enriched.exec_id, "e-early");
    assert_eq!(enriched.oca_group.as_deref(), Some(LINKED_GROUP));
    assert!(matches!(
        normalizer.replay(ACCOUNT).as_slice(),
        [IbkrOrderEvent::Execution(execution)]
            if execution.oca_group.as_deref() == Some(LINKED_GROUP)
    ));
}

#[test]
fn only_blocking_oca_groups_that_fit_the_renderer_contract_are_reported() {
    use ibapi::orders::OcaType;
    let cases = [
        (LINKED_GROUP.to_string(), OcaType::CancelWithBlock, true),
        (LINKED_GROUP.to_string(), OcaType::ReduceWithBlock, true),
        // Without block the group does not bound what its orders fill.
        (LINKED_GROUP.to_string(), OcaType::ReduceWithoutBlock, false),
        (LINKED_GROUP.to_string(), OcaType::None, false),
        ("   ".to_string(), OcaType::ReduceWithBlock, false),
        (
            "g".repeat(MAX_IDENTIFIER_CHARS),
            OcaType::ReduceWithBlock,
            true,
        ),
        (
            "g".repeat(MAX_IDENTIFIER_CHARS + 1),
            OcaType::ReduceWithBlock,
            false,
        ),
    ];
    for (group, oca_type, reported) in cases {
        let mut normalizer = OrderEventNormalizer::new(None);
        normalizer.observe(grouped_order(STOP_LOSS_ID, 0, "STP", &group, oca_type));
        let event = only_execution(normalizer.observe(execution(
            STOP_LOSS_ID,
            "e-group",
            10.0,
            "20260930-18:30:01",
        )));
        assert_eq!(
            event.oca_group.is_some(),
            reported,
            "{oca_type:?} group of {} chars",
            group.len()
        );
    }
}

#[test]
fn every_order_event_sends_the_oca_group_key_even_without_a_group() {
    // The web app reads an omitted key as a runtime before 0.6.0 when it has
    // not learned the bridge's release from a response header yet, so a
    // current bridge always sends it, as null when there is no group.
    let mut normalizer = OrderEventNormalizer::new(None);
    normalizer.observe(open_order(STOP_LOSS_ID, PARENT_ID, "STP", "r"));
    let status = only_status(normalizer.observe(status(
        STOP_LOSS_ID,
        OrderStatusKind::Submitted,
        0.0,
        10.0,
    )));
    let execution = only_execution(normalizer.observe(execution(
        STOP_LOSS_ID,
        "e-nogroup",
        10.0,
        "20260930-18:30:01",
    )));
    for event in [
        IbkrOrderEvent::Status(status),
        IbkrOrderEvent::Execution(execution),
    ] {
        let wire = serde_json::to_value(&event).expect("serialize");
        assert_eq!(
            wire.get("ocaGroup"),
            Some(&serde_json::Value::Null),
            "{wire}"
        );
    }
}
