//! Classification of IBKR messages bound to a submitted order, and recovery of
//! the order's outcome when IBKR ends its event stream early.
//!
//! TWS reports order warnings through the same `error` callback as failures.
//! `ibapi` 3.3.0 turns only codes 2100-2169 (and two market-data advisories)
//! into non-fatal [`SubscriptionItem::Notice`] items. Every other code bound to
//! an order, including the informational 399 "your order will not be placed at
//! the exchange until …" message, arrives as `Err(ibapi::Error::Notice)` and
//! ends that order's subscription before IBKR reports the order's status.
//!
//! Classification uses explicit lists of documented codes (see
//! [`ORDER_FOLLOW_UP_CODES`] and [`ORDER_REJECTION_CODES`]); any other code
//! keeps the fail-closed `indeterminate` outcome. A follow-up code never
//! decides the outcome by itself: the order's status does. When a follow-up
//! code ended the stream, the status is read back from IBKR's open and
//! completed orders by the order's exact `order_ref`. The message text is kept
//! as a detail on the definitive response.
//!
//! References: IBKR's [TWS API message codes](https://interactivebrokers.github.io/tws-api/message_codes.html)
//! and the current [TWS API error-code list](https://www.interactivebrokers.com/campus/ibkr-api-page/tws-api-error-codes/).

use super::{
    GatewayError, NO_SECURITY_DEFINITION_CODE, OrderResponse, collect_order_snapshot,
    definitive_order_response, indeterminate_response, order_event_outcome,
    order_matches_reference,
};
use futures::{Stream, StreamExt};
use ibapi::orders::{OrderData, OrderStatus, OrderStatusKind, PlaceOrder};
use ibapi::prelude::{Client, SubscriptionItem};
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

/// Longest the runtime reads IBKR order state after an informational message
/// ended an order's event stream.
pub(super) const ORDER_FOLLOW_UP_TIMEOUT: Duration = Duration::from_secs(4);
/// Delay before the first follow-up read, so the ended subscription releases
/// its order channel and IBKR's snapshot reply reaches the snapshot reader.
const FOLLOW_UP_FIRST_DELAY: Duration = Duration::from_millis(150);
/// Longest pause between two follow-up reads.
const FOLLOW_UP_MAX_DELAY: Duration = Duration::from_secs(1);
/// Longest provider text carried into a response message, in characters.
const MAX_PROVIDER_TEXT_CHARS: usize = 400;
/// Most IBKR notes appended to one response message. With the per-note cap
/// this keeps a message well inside the web journal's 4096-character limit.
const MAX_NOTES_PER_MESSAGE: usize = 3;
/// IBKR "Order cancelled - Reason:" code.
pub(super) const ORDER_CANCELLED_CODE: i32 = 202;

/// Order message codes that annotate an order without deciding its outcome.
///
/// The order's status decides; the message becomes a detail on the response.
///
/// - `202` "Order cancelled - Reason:": the order is not working, but a fill
///   may precede the cancellation, so only a terminal state (filled,
///   cancelled, inactive) settles it, read back with its filled quantity.
/// - `399` "Order message error": a server order message. The common one is
///   "Warning: Your order will not be placed at the exchange until <session
///   open>", sent for an order accepted outside its trading hours and held,
///   `PreSubmitted`, until the session opens. Other 399 texts can precede a
///   rejection, which is why the status, never the text, decides.
/// - `404` "Shares for this order are not immediately available for short
///   sale": the order is held while IBKR locates shares.
/// - `2109` "Attribute 'Outside Regular Trading Hours' is ignored based on the
///   order type and destination": the order is processed without the flag.
/// - `2137` "Cross Side Warning": the order flips the position between long
///   and short.
///
/// Source: <https://interactivebrokers.github.io/tws-api/message_codes.html>.
pub(super) const ORDER_FOLLOW_UP_CODES: [i32; 5] = [ORDER_CANCELLED_CODE, 399, 404, 2109, 2137];

/// Order message codes that mean IBKR refused the order, so it is not working.
///
/// - `110` "The price does not conform to the minimum price variation for
///   this contract."
/// - `200` "No security definition has been found for the request."
/// - `201` "Order rejected - Reason:", rejected by the IB servers.
/// - `203` "The security <security> is not available or allowed for this
///   account."
/// - `434` "The order size cannot be zero."
///
/// Source: <https://interactivebrokers.github.io/tws-api/message_codes.html>.
pub(super) const ORDER_REJECTION_CODES: [i32; 5] =
    [110, NO_SECURITY_DEFINITION_CODE, 201, 203, 434];

/// Text IBKR attached to one order, normalized for display.
#[derive(Clone, Debug, PartialEq)]
pub(super) struct ProviderNote {
    /// IBKR message code, or none for a completed order's status text.
    pub(super) code: Option<i32>,
    /// Single-line provider text without the order echo IBKR prefixes.
    pub(super) text: String,
}

impl ProviderNote {
    /// Whether this note explains why an order is not working.
    ///
    /// A cancellation (202) and a completed order's status text explain a
    /// rejection; every other informational code stays a warning even when
    /// the order turned out rejected, so a hold-until-open notice is never
    /// presented as the cause.
    fn explains_rejection(&self) -> bool {
        matches!(self.code, None | Some(ORDER_CANCELLED_CODE))
    }

    /// Whether this note is IBKR's cancellation of the order.
    pub(super) fn is_cancellation(&self) -> bool {
        self.code == Some(ORDER_CANCELLED_CODE)
    }
}

/// How one IBKR message bound to an order affects its outcome.
#[derive(Debug, PartialEq)]
pub(super) enum OrderMessage {
    /// Documented informational message; the order's status still decides.
    FollowUp(ProviderNote),
    /// Documented rejection; the text explains why.
    Rejected(String),
    /// Any other code: the outcome cannot be inferred from it.
    Unknown,
}

/// Classify one IBKR message bound to a submitted order.
///
/// # Arguments
///
/// * `code` - IBKR message code.
/// * `message` - Raw provider text.
///
/// # Returns
///
/// The message's effect on the order outcome.
pub(super) fn classify_order_message(code: i32, message: &str) -> OrderMessage {
    if ORDER_FOLLOW_UP_CODES.contains(&code) {
        return OrderMessage::FollowUp(ProviderNote {
            code: Some(code),
            text: provider_text(message),
        });
    }
    if ORDER_REJECTION_CODES.contains(&code) {
        return OrderMessage::Rejected(single_line(message));
    }
    OrderMessage::Unknown
}

/// Collapse provider text onto one line and bound its length.
pub(super) fn single_line(raw: &str) -> String {
    let collapsed = raw.split_whitespace().collect::<Vec<_>>().join(" ");
    let trimmed = collapsed.trim_end_matches('.').trim_end();
    match trimmed.char_indices().nth(MAX_PROVIDER_TEXT_CHARS) {
        Some((end, _)) => format!("{}…", &trimmed[..end]),
        None => trimmed.to_string(),
    }
}

/// Normalize an IBKR order message for display.
///
/// IBKR prefixes order messages with an echo of the order, for example
/// `Order Message:\nSELL 130 SPY ARCA\nWarning: Your order will not be placed
/// at the exchange until 2026-10-01 09:30:00 US/Eastern.`. The text after the
/// last `Warning:` is the part a person needs; anything else is kept whole.
fn provider_text(raw: &str) -> String {
    let line = single_line(raw);
    let text = match line.rsplit_once("Warning:") {
        Some((_, warning)) if !warning.trim().is_empty() => warning.trim().to_string(),
        _ => line,
    };
    // IBKR leaves "Order cancelled - Reason:" with an empty reason when the
    // cancellation has no explanation; drop the dangling label.
    match text.strip_suffix("Reason:") {
        Some(rest) if !rest.trim_end_matches([' ', '-']).is_empty() => {
            rest.trim_end_matches([' ', '-']).to_string()
        }
        _ => text,
    }
}

/// Append IBKR's notes to a definitive response message.
///
/// Informational notes become `IBKR warning <code>: <text>.` sentences, which
/// the web terminal shows next to an acceptance. On a response that is not
/// accepted, a cancellation (202) or completed-order status text becomes
/// `IBKR reason[ <code>]: <text>.`. At most [`MAX_NOTES_PER_MESSAGE`] distinct
/// notes are appended. Indeterminate responses are returned unchanged; their
/// detail is already in the message.
///
/// # Arguments
///
/// * `response` - Definitive outcome to annotate.
/// * `notes` - Provider notes seen for the order, in arrival order.
///
/// # Returns
///
/// The response with its message extended.
pub(super) fn with_provider_notes(
    mut response: OrderResponse,
    notes: &[ProviderNote],
) -> OrderResponse {
    if response.status != "indeterminate" {
        response.message = annotated_message(&response.message, response.ok, notes);
    }
    response
}

/// Extend a definitive response message with IBKR's notes.
///
/// # Arguments
///
/// * `message` - Base message ending with the order id in parentheses.
/// * `ok` - Whether the order was accepted, which selects reason labels.
/// * `notes` - Provider notes seen for the order, in arrival order.
///
/// # Returns
///
/// The base message followed by one sentence per distinct note, bounded.
pub(super) fn annotated_message(message: &str, ok: bool, notes: &[ProviderNote]) -> String {
    let mut sentences: Vec<String> = Vec::new();
    for note in notes {
        let label = if !ok && note.explains_rejection() {
            "IBKR reason"
        } else {
            "IBKR warning"
        };
        let sentence = match note.code {
            Some(code) => format!("{label} {code}: {}.", note.text),
            None => format!("{label}: {}.", note.text),
        };
        if !sentences.contains(&sentence) {
            sentences.push(sentence);
        }
    }
    if sentences.is_empty() {
        return message.to_string();
    }
    let omitted = sentences.len().saturating_sub(MAX_NOTES_PER_MESSAGE);
    sentences.truncate(MAX_NOTES_PER_MESSAGE);
    if omitted > 0 {
        sentences.push(format!("IBKR sent {omitted} more messages."));
    }
    format!("{message}. {}", sentences.join(" "))
}

/// Describe notes for an outcome that stayed unknown.
pub(super) fn describe_notes(notes: &[ProviderNote]) -> String {
    notes
        .iter()
        .map(|note| match note.code {
            Some(code) => format!("[{code}] {}", note.text),
            None => note.text.clone(),
        })
        .collect::<Vec<_>>()
        .join("; ")
}

/// IBKR's open and completed orders read at one moment.
#[derive(Debug, Default)]
pub(super) struct ProviderOrders {
    /// Open orders from every API client and TWS (`reqAllOpenOrders`).
    pub(super) open: Vec<OrderData>,
    /// Today's completed orders (`reqCompletedOrders`).
    pub(super) completed: Vec<OrderData>,
}

impl ProviderOrders {
    /// Find the order with this exact reference, completed orders first.
    ///
    /// Completed orders are read after open orders, so a completed entry is
    /// the newer state of an order that finished between the two reads.
    ///
    /// # Arguments
    ///
    /// * `account` - Account the order was sent for.
    /// * `order_ref` - Caller-generated `sunk-<uuid>` reference.
    ///
    /// # Returns
    ///
    /// The matching order, or none when IBKR does not list it yet.
    pub(super) fn find(&self, account: &str, order_ref: &str) -> Option<&OrderData> {
        self.completed
            .iter()
            .chain(&self.open)
            .find(|order| order_matches_reference(order, account, order_ref))
    }
}

/// Read IBKR's open and completed orders.
///
/// # Errors
///
/// Returns the provider read failure; the caller retries until its deadline.
pub(super) async fn read_provider_orders(
    client: &Arc<Client>,
) -> Result<ProviderOrders, GatewayError> {
    let open = collect_order_snapshot(client, super::OrderSnapshot::AllOpen).await?;
    let completed = collect_order_snapshot(client, super::OrderSnapshot::Completed).await?;
    Ok(ProviderOrders { open, completed })
}

/// Rebuild an order-status event from one snapshot entry.
///
/// The event carries the submitting client's order id so the normal status
/// classification applies unchanged.
pub(super) fn snapshot_status_event(order_id: i32, order: &OrderData) -> PlaceOrder {
    let filled = order.order.filled_quantity.max(0.0);
    PlaceOrder::OrderStatus(OrderStatus {
        order_id,
        status: order.order_state.status,
        filled,
        remaining: (order.order.total_quantity - filled).max(0.0),
        ..Default::default()
    })
}

/// IBKR's explanation for a completed order that is not working, if any.
pub(super) fn completed_reason(order: &OrderData) -> Option<ProviderNote> {
    let not_working = matches!(
        order.order_state.status,
        OrderStatusKind::Cancelled | OrderStatusKind::ApiCancelled | OrderStatusKind::Inactive
    );
    let text = single_line(&order.order_state.completed_status);
    (not_working && !text.is_empty()).then_some(ProviderNote { code: None, text })
}

/// Whether an order status is final: filled, cancelled, or inactive.
pub(super) fn is_terminal(status: OrderStatusKind) -> bool {
    matches!(
        status,
        OrderStatusKind::Filled
            | OrderStatusKind::Cancelled
            | OrderStatusKind::ApiCancelled
            | OrderStatusKind::Inactive
    )
}

/// Whether an event reports a working (non-final) status for this order.
///
/// After IBKR cancels an order (202), such an event is stale and must not
/// settle the outcome. Executions are fills and always count.
pub(super) fn reports_working_status(order_id: i32, event: &PlaceOrder) -> bool {
    let status = match event {
        PlaceOrder::OrderStatus(status) if status.order_id == order_id => status.status,
        PlaceOrder::OpenOrder(order) if order.order_id == order_id => order.order_state.status,
        _ => return false,
    };
    !is_terminal(status)
}

/// Read IBKR order state until `decide` settles or the budget runs out.
///
/// # Arguments
///
/// * `budget` - Longest the follow-up may take.
/// * `read` - Reads IBKR's open and completed orders.
/// * `decide` - Returns a value once the snapshot settles the order.
///
/// # Returns
///
/// The settled value, or none at the deadline. Failed reads are logged and
/// retried with backoff.
pub(super) async fn follow_up<T, R, Fut>(
    budget: Duration,
    mut read: R,
    mut decide: impl FnMut(&ProviderOrders) -> Option<T>,
) -> Option<T>
where
    R: FnMut() -> Fut,
    Fut: Future<Output = Result<ProviderOrders, GatewayError>>,
{
    tokio::time::timeout(budget, async {
        let mut delay = FOLLOW_UP_FIRST_DELAY.min(budget / 4);
        loop {
            tokio::time::sleep(delay).await;
            match read().await {
                Ok(orders) => {
                    if let Some(settled) = decide(&orders) {
                        return settled;
                    }
                }
                Err(error) => {
                    tracing::warn!(%error, "IBKR order follow-up read failed");
                }
            }
            delay = (delay * 2).min(FOLLOW_UP_MAX_DELAY);
        }
    })
    .await
    .ok()
}

/// Deadlines for turning one order's provider events into an outcome.
#[derive(Clone, Copy, Debug)]
pub(super) struct OutcomeDeadlines {
    /// Longest the order's event stream is read.
    pub(super) acknowledgement: Duration,
    /// Longest IBKR order state is read after a follow-up message.
    pub(super) follow_up: Duration,
}

/// Identity of one submitted order.
#[derive(Clone, Copy, Debug)]
pub(super) struct SubmittedOrder<'a> {
    /// Order id issued to this client.
    pub(super) order_id: i32,
    /// Account the order was sent for.
    pub(super) account: &'a str,
    /// Caller-generated `sunk-<uuid>` reference sent as `order_ref`.
    pub(super) customer_order_id: &'a str,
}

impl SubmittedOrder<'_> {
    fn indeterminate(&self, detail: &str) -> OrderResponse {
        indeterminate_response(
            self.account.to_string(),
            Some(self.customer_order_id.to_string()),
            detail,
        )
    }
}

/// How reading one order's event stream ended.
enum Streamed {
    /// The stream produced a definitive or fail-closed outcome.
    Decided(Box<OrderResponse>),
    /// The stream ended before a definitive state.
    Ended,
    /// The acknowledgement deadline passed.
    TimedOut,
}

/// Turn one order's provider events into its outcome.
///
/// Events are read until a definitive state. Documented informational codes
/// are kept as details and reading continues; when one ended the stream, the
/// order's state is read back from IBKR's open and completed orders. Unknown
/// codes, transport failures, and deadlines stay `indeterminate`.
///
/// # Arguments
///
/// * `events` - The order's event stream; dropped before any follow-up read so
///   IBKR's snapshot replies reach the snapshot reader.
/// * `order` - Identity of the submitted order.
/// * `deadlines` - Stream and follow-up deadlines.
/// * `read` - Reads IBKR's open and completed orders.
///
/// # Returns
///
/// The normalized order response.
pub(super) async fn single_order_outcome<S, R, Fut>(
    mut events: S,
    order: SubmittedOrder<'_>,
    deadlines: OutcomeDeadlines,
    read: R,
) -> OrderResponse
where
    S: Stream<Item = Result<SubscriptionItem<PlaceOrder>, ibapi::Error>> + Unpin,
    R: FnMut() -> Fut,
    Fut: Future<Output = Result<ProviderOrders, GatewayError>>,
{
    let mut notes: Vec<ProviderNote> = Vec::new();
    let streamed = tokio::time::timeout(deadlines.acknowledgement, async {
        while let Some(event) = events.next().await {
            let (notice, ended_stream) = match event {
                Ok(SubscriptionItem::Data(event)) => {
                    let cancelled = notes.iter().any(ProviderNote::is_cancellation);
                    if cancelled && reports_working_status(order.order_id, &event) {
                        continue;
                    }
                    match order_event_outcome(
                        order.order_id,
                        order.account,
                        order.customer_order_id,
                        event,
                    ) {
                        Some(outcome) => return Streamed::Decided(Box::new(outcome)),
                        None => continue,
                    }
                }
                Ok(SubscriptionItem::Notice(notice)) => (notice, false),
                Err(ibapi::Error::Notice(notice)) => (notice, true),
                Err(error) => {
                    return Streamed::Decided(Box::new(
                        order.indeterminate(&format!("the order event stream failed: {error}")),
                    ));
                }
            };
            match classify_order_message(notice.code, &notice.message) {
                OrderMessage::FollowUp(note) => notes.push(note),
                OrderMessage::Rejected(text) => {
                    return Streamed::Decided(Box::new(definitive_order_response(
                        order.account,
                        order.customer_order_id,
                        order.order_id,
                        false,
                        "rejected",
                        &format!("IBKR rejected the order: [{}] {text}", notice.code),
                    )));
                }
                OrderMessage::Unknown if ended_stream => {
                    return Streamed::Decided(Box::new(order.indeterminate(&format!(
                        "the order event stream failed: [{}] {}",
                        notice.code, notice.message
                    ))));
                }
                OrderMessage::Unknown => {
                    return Streamed::Decided(Box::new(order.indeterminate(&format!(
                        "IBKR returned a post-submission warning: [{}] {}",
                        notice.code, notice.message
                    ))));
                }
            }
        }
        Streamed::Ended
    })
    .await
    .unwrap_or(Streamed::TimedOut);
    // Release the order channel so snapshot replies for this order are routed
    // to the snapshot reader instead of the ended subscription.
    drop(events);
    match streamed {
        Streamed::Decided(outcome) => with_provider_notes(*outcome, &notes),
        Streamed::Ended if notes.is_empty() => {
            order.indeterminate("the order event stream ended without a definitive state")
        }
        Streamed::TimedOut if notes.is_empty() => {
            order.indeterminate("the order acknowledgement timed out")
        }
        Streamed::Ended | Streamed::TimedOut => {
            let cancelled = notes.iter().any(ProviderNote::is_cancellation);
            let settled = follow_up(deadlines.follow_up, read, |orders| {
                let found = orders.find(order.account, order.customer_order_id)?;
                if cancelled && !is_terminal(found.order_state.status) {
                    return None;
                }
                // Same mapping as the live stream: `PendingSubmit` is an
                // acknowledgement, while `ApiPending` and `PendingCancel` keep
                // reading. Reconciliation (`reconciled_order_status`) instead
                // counts any listed order as found, because it only proves
                // that the order reached IBKR.
                let outcome = order_event_outcome(
                    order.order_id,
                    order.account,
                    order.customer_order_id,
                    snapshot_status_event(order.order_id, found),
                )?;
                Some((outcome, completed_reason(found)))
            })
            .await;
            match settled {
                Some((outcome, reason)) => {
                    if !outcome.ok
                        && let Some(reason) = reason
                    {
                        notes.push(reason);
                    }
                    with_provider_notes(outcome, &notes)
                }
                None => order.indeterminate(&format!(
                    "IBKR sent {} but did not confirm the order state",
                    describe_notes(&notes)
                )),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contract::PROTOCOL_VERSION;
    use futures::stream;
    use ibapi::Notice;
    use std::sync::atomic::{AtomicUsize, Ordering};

    const ACCOUNT: &str = "DU1234528";
    const REFERENCE: &str = "sunk-dff9dd49-06ba-4c61-9e4a-8a6ea3fb2a3f";
    const ORDER_ID: i32 = 41;
    /// The 399 text the owner's paper Gateway sent on 2026-09-30.
    const HELD_UNTIL_OPEN: &str = "Order Message:\nSELL 130 SPY ARCA\nWarning: Your order will not be placed at the exchange until 2026-10-01 09:30:00 US/Eastern.";

    type Item = Result<SubscriptionItem<PlaceOrder>, ibapi::Error>;

    fn notice(code: i32, message: &str) -> Notice {
        Notice {
            code,
            message: message.to_string(),
            error_time: None,
            advanced_order_reject_json: String::new(),
        }
    }

    fn stream_error(code: i32, message: &str) -> Item {
        Err(ibapi::Error::Notice(notice(code, message)))
    }

    fn status(status: OrderStatusKind) -> Item {
        Ok(SubscriptionItem::Data(PlaceOrder::OrderStatus(
            OrderStatus {
                order_id: ORDER_ID,
                status,
                ..Default::default()
            },
        )))
    }

    fn provider_order(status: OrderStatusKind, filled: f64, completed: &str) -> OrderData {
        let mut order = OrderData {
            order_id: ORDER_ID,
            ..Default::default()
        };
        order.order.order_ref = REFERENCE.to_string();
        order.order.account = ACCOUNT.to_string();
        order.order.total_quantity = 130.0;
        order.order.filled_quantity = filled;
        order.order_state.status = status;
        order.order_state.completed_status = completed.to_string();
        order
    }

    fn submitted() -> SubmittedOrder<'static> {
        SubmittedOrder {
            order_id: ORDER_ID,
            account: ACCOUNT,
            customer_order_id: REFERENCE,
        }
    }

    fn deadlines() -> OutcomeDeadlines {
        OutcomeDeadlines {
            acknowledgement: Duration::from_millis(500),
            follow_up: Duration::from_millis(400),
        }
    }

    /// Run the single-order outcome over fixed events and snapshot replies.
    async fn outcome(events: Vec<Item>, snapshots: Vec<ProviderOrders>) -> (OrderResponse, usize) {
        let reads = AtomicUsize::new(0);
        let snapshots = std::sync::Mutex::new(snapshots.into_iter());
        let response = single_order_outcome(stream::iter(events), submitted(), deadlines(), || {
            reads.fetch_add(1, Ordering::SeqCst);
            let next = snapshots.lock().expect("snapshot fixture").next();
            async move { next.ok_or_else(|| GatewayError::Unavailable("no snapshot".to_string())) }
        })
        .await;
        (response, reads.load(Ordering::SeqCst))
    }

    fn open(order: OrderData) -> ProviderOrders {
        ProviderOrders {
            open: vec![order],
            completed: Vec::new(),
        }
    }

    fn completed(order: OrderData) -> ProviderOrders {
        ProviderOrders {
            open: Vec::new(),
            completed: vec![order],
        }
    }

    #[test]
    fn documented_codes_are_classified_explicitly() {
        assert_eq!(
            classify_order_message(399, HELD_UNTIL_OPEN),
            OrderMessage::FollowUp(ProviderNote {
                code: Some(399),
                text: "Your order will not be placed at the exchange until 2026-10-01 09:30:00 US/Eastern".to_string(),
            })
        );
        for code in ORDER_FOLLOW_UP_CODES {
            assert!(matches!(
                classify_order_message(code, "text"),
                OrderMessage::FollowUp(_)
            ));
        }
        assert_eq!(
            classify_order_message(201, "Order rejected - Reason:\nYOUR ORDER IS NOT ACCEPTED."),
            OrderMessage::Rejected(
                "Order rejected - Reason: YOUR ORDER IS NOT ACCEPTED".to_string()
            )
        );
        for code in ORDER_REJECTION_CODES {
            assert!(matches!(
                classify_order_message(code, "text"),
                OrderMessage::Rejected(_)
            ));
        }
        for code in [0, 103, 105, 161, 321, 2100, 2110, 10148, 10197] {
            assert_eq!(classify_order_message(code, "text"), OrderMessage::Unknown);
        }
    }

    #[test]
    fn provider_text_is_single_line_and_bounded() {
        assert_eq!(single_line("  a\n b \t c..  "), "a b c");
        let long = "x".repeat(MAX_PROVIDER_TEXT_CHARS + 10);
        assert_eq!(
            single_line(&long).chars().count(),
            MAX_PROVIDER_TEXT_CHARS + 1
        );
        assert_eq!(provider_text("Warning:"), "Warning:");
        assert_eq!(
            provider_text("Order cancelled - Reason: "),
            "Order cancelled"
        );
        assert_eq!(
            provider_text("Order cancelled - Reason: Cancelled by System"),
            "Order cancelled - Reason: Cancelled by System"
        );
    }

    #[tokio::test]
    async fn held_until_open_then_presubmitted_is_accepted_with_the_warning() {
        let (response, reads) = outcome(
            vec![
                stream_error(399, HELD_UNTIL_OPEN),
                status(OrderStatusKind::PreSubmitted),
            ],
            Vec::new(),
        )
        .await;
        assert_eq!(reads, 0, "a stream that continues needs no follow-up");
        assert!(response.ok);
        assert_eq!(response.status, "resting");
        assert_eq!(response.version, PROTOCOL_VERSION);
        assert_eq!(response.order_id.as_deref(), Some("41"));
        assert_eq!(
            response.message,
            "IBKR order is working (41). IBKR warning 399: Your order will not be placed at the exchange until 2026-10-01 09:30:00 US/Eastern."
        );
    }

    #[tokio::test]
    async fn held_until_open_that_ends_the_stream_reads_the_open_order_back() {
        let (response, reads) = outcome(
            vec![stream_error(399, HELD_UNTIL_OPEN)],
            vec![
                ProviderOrders::default(),
                open(provider_order(OrderStatusKind::PreSubmitted, 0.0, "")),
            ],
        )
        .await;
        assert_eq!(reads, 2, "an order not listed yet is read again");
        assert!(response.ok);
        assert_eq!(response.status, "resting");
        assert!(response.message.ends_with(
            "IBKR warning 399: Your order will not be placed at the exchange until 2026-10-01 09:30:00 US/Eastern."
        ));
        assert_eq!(response.customer_order_id.as_deref(), Some(REFERENCE));
    }

    #[tokio::test]
    async fn held_until_open_then_cancelled_reports_the_cancellation_with_its_reason() {
        let (streamed, _) = outcome(
            vec![
                stream_error(399, HELD_UNTIL_OPEN),
                status(OrderStatusKind::Cancelled),
            ],
            Vec::new(),
        )
        .await;
        assert!(!streamed.ok);
        assert_eq!(streamed.status, "rejected");
        assert!(streamed.message.starts_with(
            "IBKR order rejected or cancelled (41). IBKR warning 399: Your order will not be placed"
        ));

        let (read_back, _) = outcome(
            vec![stream_error(399, HELD_UNTIL_OPEN)],
            vec![completed(provider_order(
                OrderStatusKind::Cancelled,
                0.0,
                "Cancelled by System: order would cross",
            ))],
        )
        .await;
        assert!(!read_back.ok);
        assert_eq!(read_back.status, "rejected");
        assert!(
            read_back
                .message
                .ends_with("IBKR reason: Cancelled by System: order would cross.")
        );
    }

    #[tokio::test]
    async fn cancellation_after_a_fill_is_accepted_not_rejected() {
        let (response, _) = outcome(
            vec![stream_error(202, "Order cancelled - Reason: ")],
            vec![completed(provider_order(
                OrderStatusKind::Cancelled,
                30.0,
                "Cancelled",
            ))],
        )
        .await;
        assert!(response.ok);
        assert_eq!(response.status, "accepted");
        assert!(
            response
                .message
                .contains("IBKR warning 202: Order cancelled")
        );
    }

    #[tokio::test]
    async fn cancellation_is_settled_only_by_a_final_state() {
        let (response, reads) = outcome(
            vec![
                stream_error(202, "Order cancelled - Reason: "),
                status(OrderStatusKind::Submitted),
            ],
            vec![
                open(provider_order(OrderStatusKind::Submitted, 0.0, "")),
                ProviderOrders {
                    open: vec![provider_order(OrderStatusKind::Submitted, 0.0, "")],
                    completed: vec![provider_order(OrderStatusKind::Cancelled, 0.0, "")],
                },
            ],
        )
        .await;
        assert_eq!(reads, 2, "the stale working entry is read again");
        assert!(!response.ok);
        assert_eq!(response.status, "rejected");
        assert_eq!(
            response.message,
            "IBKR order rejected or cancelled (41). IBKR reason 202: Order cancelled."
        );
    }

    #[test]
    fn appended_notes_are_bounded() {
        let notes = (0..6)
            .map(|index| ProviderNote {
                code: Some(399),
                text: format!("message {index}"),
            })
            .collect::<Vec<_>>();
        let message = annotated_message("IBKR order is working (41)", true, &notes);
        assert_eq!(
            message,
            "IBKR order is working (41). IBKR warning 399: message 0. IBKR warning 399: message 1. IBKR warning 399: message 2. IBKR sent 3 more messages."
        );
        let worst = (0..50)
            .map(|index| ProviderNote {
                code: Some(399),
                text: single_line(&format!(
                    "{index}{}",
                    "x".repeat(MAX_PROVIDER_TEXT_CHARS * 2)
                )),
            })
            .collect::<Vec<_>>();
        assert!(annotated_message("base", true, &worst).chars().count() < 4096);
    }

    #[test]
    fn rejection_reasons_are_labelled_apart_from_warnings() {
        let notes = [
            ProviderNote {
                code: Some(399),
                text: "held".to_string(),
            },
            ProviderNote {
                code: None,
                text: "Cancelled by System".to_string(),
            },
        ];
        assert_eq!(
            annotated_message("rejected (1)", false, &notes),
            "rejected (1). IBKR warning 399: held. IBKR reason: Cancelled by System."
        );
    }

    #[tokio::test]
    async fn documented_rejection_is_definitive() {
        let (response, reads) = outcome(
            vec![stream_error(
                201,
                "Order rejected - Reason: YOUR ORDER IS NOT ACCEPTED. IN ORDER TO OBTAIN THE DESIRED POSITION",
            )],
            Vec::new(),
        )
        .await;
        assert_eq!(reads, 0);
        assert!(!response.ok);
        assert_eq!(response.status, "rejected");
        assert_eq!(response.order_id.as_deref(), Some("41"));
        assert!(response.message.starts_with(
            "IBKR rejected the order: [201] Order rejected - Reason: YOUR ORDER IS NOT ACCEPTED"
        ));
    }

    #[tokio::test]
    async fn unknown_stream_error_stays_indeterminate() {
        let (response, reads) = outcome(
            vec![stream_error(
                10148,
                "OrderId 41 that needs to be cancelled can not be cancelled",
            )],
            vec![open(provider_order(OrderStatusKind::PreSubmitted, 0.0, ""))],
        )
        .await;
        assert_eq!(reads, 0, "unknown codes never trigger a follow-up");
        assert!(!response.ok);
        assert_eq!(response.status, "indeterminate");
        assert_eq!(response.order_id, None);
        assert!(
            response
                .message
                .contains("the order event stream failed: [10148]")
        );
    }

    #[tokio::test]
    async fn unknown_warning_notice_stays_indeterminate() {
        let (response, _) = outcome(
            vec![Ok(SubscriptionItem::Notice(notice(
                2100,
                "New account data requested",
            )))],
            Vec::new(),
        )
        .await;
        assert_eq!(response.status, "indeterminate");
        assert!(response.message.contains("post-submission warning: [2100]"));
    }

    #[tokio::test]
    async fn documented_warning_notice_keeps_reading_the_stream() {
        let (response, _) = outcome(
            vec![
                Ok(SubscriptionItem::Notice(notice(
                    2109,
                    "Order Event Warning: Attribute \"Outside Regular Trading Hours\" is ignored based on the order type and destination. PlaceOrder is now being processed.",
                ))),
                status(OrderStatusKind::Submitted),
            ],
            Vec::new(),
        )
        .await;
        assert!(response.ok);
        assert_eq!(response.status, "resting");
        assert!(
            response.message.contains(
                "IBKR warning 2109: Attribute \"Outside Regular Trading Hours\" is ignored"
            )
        );
    }

    #[tokio::test]
    async fn follow_up_without_provider_confirmation_stays_indeterminate() {
        let (response, reads) = outcome(
            vec![stream_error(399, HELD_UNTIL_OPEN)],
            vec![
                open(provider_order(OrderStatusKind::ApiPending, 0.0, "")),
                ProviderOrders::default(),
            ],
        )
        .await;
        assert!(reads >= 2);
        assert_eq!(response.status, "indeterminate");
        assert_eq!(
            response.message,
            "IBKR order outcome is unknown: IBKR sent [399] Your order will not be placed at the exchange until 2026-10-01 09:30:00 US/Eastern but did not confirm the order state. Verify provider state before submitting again"
        );
    }

    #[tokio::test]
    async fn plain_stream_end_and_timeout_stay_indeterminate_without_follow_up() {
        let (ended, reads) = outcome(Vec::new(), vec![ProviderOrders::default()]).await;
        assert_eq!(reads, 0);
        assert!(ended.message.contains("ended without a definitive state"));

        let reads = AtomicUsize::new(0);
        let timed_out = single_order_outcome(
            stream::pending::<Item>(),
            submitted(),
            OutcomeDeadlines {
                acknowledgement: Duration::from_millis(20),
                follow_up: Duration::from_millis(20),
            },
            || {
                reads.fetch_add(1, Ordering::SeqCst);
                async { Ok(ProviderOrders::default()) }
            },
        )
        .await;
        assert_eq!(reads.load(Ordering::SeqCst), 0);
        assert!(timed_out.message.contains("acknowledgement timed out"));
    }

    #[test]
    fn snapshot_lookup_matches_the_exact_reference_and_account() {
        let mut other_account = provider_order(OrderStatusKind::PreSubmitted, 0.0, "");
        other_account.order.account = "DU0000001".to_string();
        let mut other_reference = provider_order(OrderStatusKind::PreSubmitted, 0.0, "");
        other_reference.order.order_ref = "sunk-other".to_string();
        let orders = ProviderOrders {
            open: vec![other_account, other_reference],
            completed: Vec::new(),
        };
        assert!(orders.find(ACCOUNT, REFERENCE).is_none());
    }

    #[test]
    fn snapshot_statuses_follow_the_live_stream_classification() {
        let classify = |status| {
            order_event_outcome(
                ORDER_ID,
                ACCOUNT,
                REFERENCE,
                snapshot_status_event(ORDER_ID, &provider_order(status, 0.0, "")),
            )
        };
        for status in [OrderStatusKind::ApiPending, OrderStatusKind::PendingCancel] {
            assert!(
                classify(status).is_none(),
                "{status:?} keeps the follow-up reading"
            );
        }
        // A pending submission is an acknowledgement on the live stream too.
        assert_eq!(
            classify(OrderStatusKind::PendingSubmit)
                .expect("acknowledged")
                .status,
            "accepted"
        );
        let mut filled = provider_order(OrderStatusKind::Filled, 130.0, "");
        filled.order.filled_quantity = 130.0;
        let event = snapshot_status_event(ORDER_ID, &filled);
        let outcome = order_event_outcome(ORDER_ID, ACCOUNT, REFERENCE, event).expect("filled");
        assert_eq!(outcome.status, "filled");
    }

    /// A loopback TWS that completes the API handshake, answers every
    /// `PlaceOrder` with one error frame for that order id, and answers order
    /// snapshots with the last placed order in a fixed state.
    mod fake_tws {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::{TcpListener, TcpStream};

        fn varint(mut value: u64, out: &mut Vec<u8>) {
            loop {
                let byte = (value & 0x7f) as u8;
                value >>= 7;
                if value == 0 {
                    out.push(byte);
                    return;
                }
                out.push(byte | 0x80);
            }
        }

        fn int_field(field: u64, value: u64, out: &mut Vec<u8>) {
            varint(field << 3, out);
            varint(value, out);
        }

        fn string_field(field: u64, value: &str, out: &mut Vec<u8>) {
            varint((field << 3) | 2, out);
            varint(value.len() as u64, out);
            out.extend_from_slice(value.as_bytes());
        }

        /// A protobuf frame: message id offset by 200, then the body.
        fn protobuf(message_id: i32, body: &[u8]) -> Vec<u8> {
            let mut frame = (message_id + 200).to_be_bytes().to_vec();
            frame.extend_from_slice(body);
            frame
        }

        async fn read_frame(socket: &mut TcpStream) -> Option<Vec<u8>> {
            let len = socket.read_u32().await.ok()? as usize;
            let mut frame = vec![0; len];
            socket.read_exact(&mut frame).await.ok()?;
            Some(frame)
        }

        async fn write_frame(socket: &mut TcpStream, frame: &[u8]) {
            socket.write_u32(frame.len() as u32).await.expect("length");
            socket.write_all(frame).await.expect("body");
        }

        /// Order id of a protobuf `PlaceOrderRequest` (field 1).
        fn order_id(body: &[u8]) -> u64 {
            assert_eq!(body[0], 0x08, "PlaceOrderRequest starts with its order id");
            let mut value = 0_u64;
            for (index, byte) in body[1..].iter().enumerate() {
                value |= u64::from(byte & 0x7f) << (7 * index);
                if byte & 0x80 == 0 {
                    break;
                }
            }
            value
        }

        fn message_field(field: u64, body: &[u8], out: &mut Vec<u8>) {
            varint((field << 3) | 2, out);
            varint(body.len() as u64, out);
            out.extend_from_slice(body);
        }

        /// One order as IBKR lists it: order id, account, reference, quantity
        /// and status, in the protobuf layout `ibapi` decodes.
        fn listed_order(order_id: u64, status: &str, completed: bool) -> Vec<u8> {
            let mut order = Vec::new();
            int_field(2, order_id, &mut order);
            string_field(6, "130", &mut order);
            string_field(12, super::ACCOUNT, &mut order);
            string_field(28, super::REFERENCE, &mut order);
            let mut state = Vec::new();
            string_field(1, status, &mut state);
            let mut body = Vec::new();
            if completed {
                message_field(2, &order, &mut body);
                message_field(3, &state, &mut body);
            } else {
                int_field(1, order_id, &mut body);
                message_field(3, &order, &mut body);
                message_field(4, &state, &mut body);
            }
            body
        }

        /// Start a server whose order snapshots list nothing.
        pub(super) async fn answering_orders_with(code: u64, message: &'static str) -> String {
            answering_orders_and_snapshots(code, message, None).await
        }

        /// Start the server; returns its address.
        ///
        /// `listed` is the status the last placed order has in
        /// `reqAllOpenOrders`, or none to list no open orders.
        pub(super) async fn answering_orders_and_snapshots(
            code: u64,
            message: &'static str,
            listed: Option<&'static str>,
        ) -> String {
            let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
            let address = listener.local_addr().expect("address").to_string();
            tokio::spawn(async move {
                let (mut socket, _) = listener.accept().await.expect("accept");
                let mut prefix = [0_u8; 4];
                socket.read_exact(&mut prefix).await.expect("API prefix");
                read_frame(&mut socket).await.expect("version frame");
                write_frame(
                    &mut socket,
                    b"225\x0020260930 19:33:16 America/New_York\x00",
                )
                .await;
                read_frame(&mut socket).await.expect("start API frame");
                let mut next_id = Vec::new();
                int_field(1, 41, &mut next_id);
                write_frame(&mut socket, &protobuf(9, &next_id)).await;
                let mut accounts = Vec::new();
                string_field(1, super::ACCOUNT, &mut accounts);
                write_frame(&mut socket, &protobuf(15, &accounts)).await;
                let mut placed = None;
                while let Some(frame) = read_frame(&mut socket).await {
                    match i32::from_be_bytes(frame[..4].try_into().expect("id")) - 200 {
                        // PlaceOrder
                        3 => {
                            let id = order_id(&frame[4..]);
                            placed = Some(id);
                            let mut error = Vec::new();
                            int_field(1, id, &mut error);
                            int_field(3, code, &mut error);
                            string_field(4, message, &mut error);
                            write_frame(&mut socket, &protobuf(4, &error)).await;
                        }
                        // RequestAllOpenOrders: OpenOrder frames, then OpenOrderEnd.
                        16 => {
                            if let (Some(id), Some(status)) = (placed, listed) {
                                write_frame(
                                    &mut socket,
                                    &protobuf(5, &listed_order(id, status, false)),
                                )
                                .await;
                            }
                            write_frame(&mut socket, &protobuf(53, &[])).await;
                        }
                        // RequestCompletedOrders: nothing completed, then CompletedOrdersEnd.
                        99 => write_frame(&mut socket, &protobuf(102, &[])).await,
                        _ => {}
                    }
                }
            });
            address
        }
    }

    /// After the 399 ended the order's subscription, the bridge's own snapshot
    /// reader must receive IBKR's `OpenOrder` for that order: the dropped
    /// subscription releases its order channel, so `ibapi` routes the reply
    /// to the shared `reqAllOpenOrders` reader instead.
    #[tokio::test]
    async fn read_back_after_a_held_order_reaches_the_snapshot_reader() {
        let address =
            fake_tws::answering_orders_and_snapshots(399, HELD_UNTIL_OPEN, Some("PreSubmitted"))
                .await;
        let client = Arc::new(Client::connect(&address, 17_654).await.expect("connect"));
        let contract = ibapi::contracts::Contract::stock("SPY").build();
        let order = ibapi::orders::Order {
            action: ibapi::orders::Action::Sell,
            total_quantity: 130.0,
            order_type: "MKT".to_string(),
            account: ACCOUNT.to_string(),
            order_ref: REFERENCE.to_string(),
            ..Default::default()
        };
        let events = client
            .place_order(ORDER_ID, &contract, &order)
            .await
            .expect("sent");
        let response = single_order_outcome(
            events,
            submitted(),
            OutcomeDeadlines {
                acknowledgement: Duration::from_secs(2),
                follow_up: Duration::from_secs(3),
            },
            || read_provider_orders(&client),
        )
        .await;
        assert!(response.ok, "{}", response.message);
        assert_eq!(response.status, "resting");
        assert_eq!(
            response.message,
            "IBKR order is working (41). IBKR warning 399: Your order will not be placed at the exchange until 2026-10-01 09:30:00 US/Eastern."
        );
    }

    /// Reproduces the owner's 2026-09-30 report against the real `ibapi`
    /// routing: a 399 for the order arrives as a stream error that ends the
    /// order's subscription, and the outcome is recovered from order state.
    #[tokio::test]
    async fn ibapi_delivers_held_until_open_as_a_stream_ending_error() {
        let address = fake_tws::answering_orders_with(399, HELD_UNTIL_OPEN).await;
        let client = Client::connect(&address, 17_654).await.expect("connect");
        let contract = ibapi::contracts::Contract::stock("SPY").build();
        let order = ibapi::orders::Order {
            action: ibapi::orders::Action::Sell,
            total_quantity: 130.0,
            order_type: "MKT".to_string(),
            account: ACCOUNT.to_string(),
            order_ref: REFERENCE.to_string(),
            ..Default::default()
        };

        let mut raw = client
            .place_order(ORDER_ID, &contract, &order)
            .await
            .expect("sent");
        match tokio::time::timeout(Duration::from_secs(5), raw.next()).await {
            Ok(Some(Err(ibapi::Error::Notice(notice)))) => assert_eq!(notice.code, 399),
            other => panic!("expected a 399 stream error, got {other:?}"),
        }
        assert!(
            raw.next().await.is_none(),
            "ibapi ends the stream after the error"
        );
        drop(raw);

        let events = client
            .place_order(ORDER_ID + 1, &contract, &order)
            .await
            .expect("sent");
        let response = single_order_outcome(
            events,
            SubmittedOrder {
                order_id: ORDER_ID + 1,
                ..submitted()
            },
            deadlines(),
            || async { Ok(open(provider_order(OrderStatusKind::PreSubmitted, 0.0, ""))) },
        )
        .await;
        assert!(response.ok, "{}", response.message);
        assert_eq!(response.status, "resting");
        assert!(response.message.contains("IBKR warning 399: Your order will not be placed at the exchange until 2026-10-01 09:30:00 US/Eastern."));
    }
}
