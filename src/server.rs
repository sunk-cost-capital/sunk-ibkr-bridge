//! Axum server exposing authenticated, versioned HTTP and WebSocket contracts.

use crate::contract::{
    AccountBalanceUpdate, AccountStateRequest, CancelOrderRequest, CandleRequest, CandleResponse,
    DetectionRequest, DetectionResponse, ErrorBody, ErrorEnvelope, GatewayDetection,
    OptionChainRequest, OptionQuoteRequest, OrderRequest, PROTOCOL_VERSION, ProbeRequest,
    ReconcileOrderRequest, StockQuoteRequest, StreamEvent, StreamRequest,
};
use crate::gateway::{Gateway, GatewayConfig, GatewayError, should_retry_stock_quote};
use crate::order_events::should_retry_order_events;
use crate::security::{
    SecurityConfig, SecurityError, SecurityState, WEBSOCKET_PROTOCOL,
    protocol_version_mismatch_message,
};
use axum::Router;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Request, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use futures::{SinkExt, StreamExt};
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tower_http::limit::RequestBodyLimitLayer;
use tower_http::timeout::TimeoutLayer;
use tower_http::trace::TraceLayer;

const MAX_BODY_BYTES: usize = 64 * 1024;
const ACCESS_HEADERS: &str = "content-type, x-sunk-bridge-version, x-sunk-bridge-timestamp, x-sunk-bridge-nonce, x-sunk-bridge-signature";
const STREAM_PATH: &str = "/v1/client-exchange/stream";
/// Response header naming this bridge's crate release, such as `0.5.0`.
///
/// The web app gates optional request fields on it: a bridge that omits it
/// predates `0.5.0`. It is a header rather than a body field so web builds
/// whose response schemas reject unknown keys keep working.
pub(crate) const RELEASE_HEADER: &str = "x-sunk-bridge-release";
const STREAM_SETUP_TIMEOUT: Duration = Duration::from_secs(10);
const STREAM_RECONNECT_MAX_DELAY: Duration = Duration::from_secs(30);

/// Complete listener, gateway, and request-security configuration.
#[derive(Clone, Debug)]
pub struct BridgeConfig {
    /// Loopback socket used by the browser-facing HTTP and WebSocket server.
    pub listen: SocketAddr,
    /// Same-machine IB Gateway TWS socket connection.
    pub gateway: GatewayConfig,
    /// Exact web-origin allowlist and shared request-signing token.
    pub security: SecurityConfig,
}

#[derive(Debug)]
struct AppState {
    gateway: Gateway,
    security: SecurityState,
}

/// Build the bridge router for a validated configuration.
///
/// # Errors
///
/// Returns an error when the IB Gateway client configuration is invalid.
pub fn build_router(config: BridgeConfig) -> Result<Router, GatewayError> {
    let state = Arc::new(AppState {
        gateway: Gateway::new(config.gateway)?,
        security: SecurityState::new(config.security),
    });
    Ok(Router::new()
        .route(
            "/v1/client-exchange/detect",
            post(detect).options(preflight),
        )
        .route("/v1/client-exchange/probe", post(probe).options(preflight))
        .route(
            "/v1/client-exchange/market-data/candles",
            post(candles).options(preflight),
        )
        .route(
            "/v1/client-exchange/market-data/options/chain",
            post(option_chain).options(preflight),
        )
        .route(
            "/v1/client-exchange/market-data/options/quote",
            post(option_quote).options(preflight),
        )
        .route(
            "/v1/client-exchange/account-state",
            post(account_state).options(preflight),
        )
        .route("/v1/client-exchange/orders", post(order).options(preflight))
        .route(
            "/v1/client-exchange/orders/cancel",
            post(cancel_order).options(preflight),
        )
        .route(
            "/v1/client-exchange/orders/reconcile",
            post(reconcile_order).options(preflight),
        )
        .route(STREAM_PATH, get(stream).options(stream_preflight))
        .fallback(fallback)
        .layer(RequestBodyLimitLayer::new(MAX_BODY_BYTES))
        .layer(TimeoutLayer::with_status_code(
            StatusCode::GATEWAY_TIMEOUT,
            Duration::from_secs(50),
        ))
        .layer(TraceLayer::new_for_http())
        .with_state(state))
}

async fn detect(State(state): State<Arc<AppState>>, request: Request) -> Response {
    authenticated_json(
        state,
        request,
        |state, _input: DetectionRequest| async move {
            match state.gateway.detect().await? {
                GatewayDetection::Connected { message, accounts } => Ok(DetectionResponse {
                    version: PROTOCOL_VERSION,
                    status: "connected".to_string(),
                    message,
                    accounts,
                }),
            }
        },
    )
    .await
}

/// Run the bridge until a shutdown signal is received.
///
/// # Errors
///
/// Returns an error when the listener is not loopback, cannot bind, or the
/// HTTP server exits unexpectedly.
pub async fn run(config: BridgeConfig) -> Result<(), Box<dyn std::error::Error>> {
    if !config.listen.ip().is_loopback() {
        return Err("bridge listener must be loopback".into());
    }
    let listen = config.listen;
    let router = build_router(config)?;
    let listener = tokio::net::TcpListener::bind(listen).await?;
    tracing::info!(address = %listener.local_addr()?, "IBKR browser bridge listening");
    axum::serve(listener, router)
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    Ok(())
}

async fn shutdown_signal() {
    let control_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    #[cfg(unix)]
    let terminate = async {
        if let Ok(mut signal) =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        {
            signal.recv().await;
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! {
        () = control_c => {},
        () = terminate => {},
    }
}

async fn probe(State(state): State<Arc<AppState>>, request: Request) -> Response {
    authenticated_json(state, request, |state, input: ProbeRequest| async move {
        state.gateway.probe(input.account_ref).await
    })
    .await
}

async fn candles(State(state): State<Arc<AppState>>, request: Request) -> Response {
    authenticated_json(state, request, |state, input: CandleRequest| async move {
        state
            .gateway
            .candles(input)
            .await
            .map(|candles| CandleResponse {
                version: PROTOCOL_VERSION,
                candles,
            })
    })
    .await
}

async fn option_chain(State(state): State<Arc<AppState>>, request: Request) -> Response {
    authenticated_json(
        state,
        request,
        |state, input: OptionChainRequest| async move { state.gateway.option_chain(input).await },
    )
    .await
}

async fn option_quote(State(state): State<Arc<AppState>>, request: Request) -> Response {
    authenticated_json(
        state,
        request,
        |state, input: OptionQuoteRequest| async move { state.gateway.option_quote(input).await },
    )
    .await
}

async fn account_state(State(state): State<Arc<AppState>>, request: Request) -> Response {
    authenticated_json(
        state,
        request,
        |state, input: AccountStateRequest| async move { state.gateway.account_state(input).await },
    )
    .await
}

async fn order(State(state): State<Arc<AppState>>, request: Request) -> Response {
    authenticated_json(state, request, |state, input: OrderRequest| async move {
        state.gateway.order(input).await
    })
    .await
}

async fn reconcile_order(State(state): State<Arc<AppState>>, request: Request) -> Response {
    authenticated_json(
        state,
        request,
        |state, input: ReconcileOrderRequest| async move {
            state.gateway.reconcile_order(input).await
        },
    )
    .await
}

async fn cancel_order(State(state): State<Arc<AppState>>, request: Request) -> Response {
    authenticated_json(
        state,
        request,
        |state, input: CancelOrderRequest| async move { state.gateway.cancel_order(input).await },
    )
    .await
}

async fn stream(
    State(state): State<Arc<AppState>>,
    websocket: WebSocketUpgrade,
    headers: HeaderMap,
) -> Response {
    let allowed_origin = state
        .security
        .validate_origin(&headers)
        .ok()
        .map(ToString::to_string);
    if let Err(error) = state
        .security
        .authenticate_websocket(&headers, STREAM_PATH)
        .await
    {
        let (status, code) = security_error_status(&error);
        return error_response(status, code, &error.to_string(), allowed_origin.as_deref());
    }
    websocket
        .max_message_size(MAX_BODY_BYTES)
        .protocols([WEBSOCKET_PROTOCOL])
        .on_upgrade(move |socket| run_stream(socket, state))
}

async fn run_stream(socket: WebSocket, state: Arc<AppState>) {
    let (mut sender, mut receiver) = socket.split();
    let message = match tokio::time::timeout(STREAM_SETUP_TIMEOUT, receiver.next()).await {
        Ok(Some(Ok(Message::Text(message)))) => message,
        Ok(Some(Ok(_))) => {
            let _ = send_stream_error(
                &mut sender,
                "invalid_request",
                "first WebSocket message must be a JSON subscription",
            )
            .await;
            return;
        }
        Ok(Some(Err(_))) | Ok(None) => return,
        Err(_) => {
            let _ = send_stream_error(
                &mut sender,
                "invalid_request",
                "WebSocket subscription timed out",
            )
            .await;
            return;
        }
    };
    let request = match serde_json::from_str::<StreamRequest>(&message) {
        Ok(request) if request.version() == PROTOCOL_VERSION => request,
        Ok(request) => {
            let _ = send_stream_error(
                &mut sender,
                "unsupported_version",
                &protocol_version_mismatch_message(Some(request.version())),
            )
            .await;
            return;
        }
        Err(error) => {
            let _ = send_stream_error(
                &mut sender,
                "invalid_request",
                &format!("WebSocket JSON does not match the v{PROTOCOL_VERSION} contract: {error}"),
            )
            .await;
            return;
        }
    };

    match request {
        StreamRequest::AccountBalance { account_ref, .. } => {
            if !send_stream_event(
                &mut sender,
                &StreamEvent::Ready {
                    version: PROTOCOL_VERSION,
                    operation: "account_balance",
                },
            )
            .await
            {
                return;
            }
            run_account_balance_stream(&mut sender, &mut receiver, state, account_ref).await;
        }
        StreamRequest::Candles {
            symbol,
            conid,
            timeframe,
            ..
        } => {
            if !send_stream_event(
                &mut sender,
                &StreamEvent::Ready {
                    version: PROTOCOL_VERSION,
                    operation: "candles",
                },
            )
            .await
            {
                return;
            }
            run_candle_stream(
                &mut sender,
                &mut receiver,
                state,
                CandleRequest {
                    version: PROTOCOL_VERSION,
                    symbol,
                    conid,
                    timeframe,
                    from: None,
                    to: None,
                    limit: None,
                },
            )
            .await;
        }
        StreamRequest::StockQuote { symbol, conid, .. } => {
            if !send_stream_event(
                &mut sender,
                &StreamEvent::Ready {
                    version: PROTOCOL_VERSION,
                    operation: "stock_quote",
                },
            )
            .await
            {
                return;
            }
            run_stock_quote_stream(
                &mut sender,
                &mut receiver,
                state,
                StockQuoteRequest { symbol, conid },
            )
            .await;
        }
        StreamRequest::OrderEvents { account_ref, .. } => {
            if !send_stream_event(
                &mut sender,
                &StreamEvent::Ready {
                    version: PROTOCOL_VERSION,
                    operation: "order_events",
                },
            )
            .await
            {
                return;
            }
            run_order_event_stream(&mut sender, &mut receiver, state, account_ref).await;
        }
    }
}

/// Streams one account's order statuses and executions, resubscribing with
/// backoff after a lost connection or provider failure. Invalid input (an
/// ambiguous or unknown account) ends the socket.
async fn run_order_event_stream(
    sender: &mut futures::stream::SplitSink<WebSocket, Message>,
    receiver: &mut futures::stream::SplitStream<WebSocket>,
    state: Arc<AppState>,
    requested_account_ref: Option<String>,
) {
    let mut reconnect_delay = Duration::from_secs(1);
    let mut account_ref = requested_account_ref;
    loop {
        let subscription = tokio::select! {
            message = receiver.next() => {
                if stream_client_ended(message) { return; }
                continue;
            }
            result = state.gateway.order_event_stream(account_ref.clone()) => result,
        };
        match subscription {
            Ok(mut subscription) => {
                // Keep the resolved account across reconnects.
                account_ref = Some(subscription.account_ref().to_string());
                loop {
                    let update = tokio::select! {
                        message = receiver.next() => {
                            if stream_client_ended(message) { return; }
                            continue;
                        }
                        update = subscription.next() => update,
                    };
                    match update {
                        Some(Ok(order)) => {
                            reconnect_delay = Duration::from_secs(1);
                            if !send_stream_event(
                                sender,
                                &StreamEvent::OrderEvent {
                                    version: PROTOCOL_VERSION,
                                    order,
                                },
                            )
                            .await
                            {
                                return;
                            }
                        }
                        Some(Err(error)) => {
                            let should_retry = should_retry_order_events(&error);
                            if !send_gateway_stream_error(sender, &error).await || !should_retry {
                                return;
                            }
                            break;
                        }
                        None => {
                            if !send_stream_error(
                                sender,
                                "gateway_unavailable",
                                "IBKR order stream ended; reconnecting",
                            )
                            .await
                            {
                                return;
                            }
                            break;
                        }
                    }
                }
            }
            Err(error) => {
                let should_retry = should_retry_order_events(&error);
                if !send_gateway_stream_error(sender, &error).await || !should_retry {
                    return;
                }
            }
        }
        if wait_for_reconnect(receiver, reconnect_delay).await {
            return;
        }
        reconnect_delay = reconnect_delay
            .saturating_mul(2)
            .min(STREAM_RECONNECT_MAX_DELAY);
    }
}

async fn run_account_balance_stream(
    sender: &mut futures::stream::SplitSink<WebSocket, Message>,
    receiver: &mut futures::stream::SplitStream<WebSocket>,
    state: Arc<AppState>,
    requested_account_ref: Option<String>,
) {
    let mut reconnect_delay = Duration::from_secs(1);
    let mut account_ref = requested_account_ref.clone();
    loop {
        let subscription = tokio::select! {
            message = receiver.next() => {
                if stream_client_ended(message) { return; }
                continue;
            }
            result = state.gateway.account_balance_stream(account_ref.clone()) => result,
        };
        match subscription {
            Ok(mut subscription) => loop {
                let update = tokio::select! {
                    message = receiver.next() => {
                        if stream_client_ended(message) {
                            subscription.cancel().await;
                            return;
                        }
                        continue;
                    }
                    update = subscription.next() => update,
                };
                match update {
                    Some(Ok(balance)) => {
                        account_ref = Some(balance.account_ref.clone());
                        reconnect_delay = Duration::from_secs(1);
                        if !send_stream_event(
                            sender,
                            &StreamEvent::AccountBalance {
                                version: PROTOCOL_VERSION,
                                update: AccountBalanceUpdate {
                                    provider: "ibkr",
                                    ok: true,
                                    status: "available",
                                    message: "IBKR account summary is available".to_string(),
                                    account_ref: Some(balance.account_ref),
                                    balance_usd: Some(balance.balance_usd),
                                    unit: "USD",
                                    summary: Some(balance.summary),
                                },
                            },
                        )
                        .await
                        {
                            subscription.cancel().await;
                            return;
                        }
                    }
                    Some(Err(error)) => {
                        let should_retry = should_retry_stream(&error);
                        if !send_balance_unavailable(
                            sender,
                            account_ref.clone(),
                            &error.to_string(),
                        )
                        .await
                        {
                            subscription.cancel().await;
                            return;
                        }
                        subscription.cancel().await;
                        if !should_retry {
                            return;
                        }
                        break;
                    }
                    None => {
                        if !send_balance_unavailable(
                            sender,
                            account_ref.clone(),
                            "IBKR account stream ended; reconnecting",
                        )
                        .await
                        {
                            subscription.cancel().await;
                            return;
                        }
                        subscription.cancel().await;
                        break;
                    }
                }
            },
            Err(error) => {
                let should_retry = should_retry_stream(&error);
                if !send_balance_unavailable(sender, account_ref.clone(), &error.to_string()).await
                {
                    return;
                }
                if !should_retry {
                    return;
                }
            }
        }
        if wait_for_reconnect(receiver, reconnect_delay).await {
            return;
        }
        reconnect_delay = reconnect_delay
            .saturating_mul(2)
            .min(STREAM_RECONNECT_MAX_DELAY);
    }
}

async fn run_candle_stream(
    sender: &mut futures::stream::SplitSink<WebSocket, Message>,
    receiver: &mut futures::stream::SplitStream<WebSocket>,
    state: Arc<AppState>,
    request: CandleRequest,
) {
    let mut reconnect_delay = Duration::from_secs(1);
    loop {
        let subscription = tokio::select! {
            message = receiver.next() => {
                if stream_client_ended(message) { return; }
                continue;
            }
            result = state.gateway.candle_stream(request.clone()) => result,
        };
        match subscription {
            Ok(mut subscription) => loop {
                let update = tokio::select! {
                    message = receiver.next() => {
                        if stream_client_ended(message) {
                            subscription.cancel().await;
                            return;
                        }
                        continue;
                    }
                    update = subscription.next() => update,
                };
                match update {
                    Some(Ok(candle)) => {
                        reconnect_delay = Duration::from_secs(1);
                        if !send_stream_event(
                            sender,
                            &StreamEvent::Candle {
                                version: PROTOCOL_VERSION,
                                candle,
                            },
                        )
                        .await
                        {
                            subscription.cancel().await;
                            return;
                        }
                    }
                    Some(Err(error)) => {
                        let should_retry = should_retry_stream(&error);
                        if !send_gateway_stream_error(sender, &error).await {
                            subscription.cancel().await;
                            return;
                        }
                        subscription.cancel().await;
                        if !should_retry {
                            return;
                        }
                        break;
                    }
                    None => {
                        subscription.cancel().await;
                        break;
                    }
                }
            },
            Err(error) => {
                let should_retry = should_retry_stream(&error);
                if !send_gateway_stream_error(sender, &error).await {
                    return;
                }
                if !should_retry {
                    return;
                }
            }
        }
        if wait_for_reconnect(receiver, reconnect_delay).await {
            return;
        }
        reconnect_delay = reconnect_delay
            .saturating_mul(2)
            .min(STREAM_RECONNECT_MAX_DELAY);
    }
}

/// Streams one stock quote, resubscribing with backoff after any failure
/// except an invalid request or unresolvable contract, which end the socket.
async fn run_stock_quote_stream(
    sender: &mut futures::stream::SplitSink<WebSocket, Message>,
    receiver: &mut futures::stream::SplitStream<WebSocket>,
    state: Arc<AppState>,
    request: StockQuoteRequest,
) {
    let mut reconnect_delay = Duration::from_secs(1);
    loop {
        let subscription = tokio::select! {
            message = receiver.next() => {
                if stream_client_ended(message) { return; }
                continue;
            }
            result = state.gateway.stock_quote_stream(request.clone()) => result,
        };
        match subscription {
            Ok(mut subscription) => loop {
                let update = tokio::select! {
                    message = receiver.next() => {
                        if stream_client_ended(message) {
                            subscription.cancel().await;
                            return;
                        }
                        continue;
                    }
                    update = subscription.next() => update,
                };
                match update {
                    Some(Ok(quote)) => {
                        reconnect_delay = Duration::from_secs(1);
                        if !send_stream_event(
                            sender,
                            &StreamEvent::StockQuote {
                                version: PROTOCOL_VERSION,
                                quote,
                            },
                        )
                        .await
                        {
                            subscription.cancel().await;
                            return;
                        }
                    }
                    Some(Err(error)) => {
                        let should_retry = should_retry_stock_quote(&error);
                        if !send_gateway_stream_error(sender, &error).await {
                            subscription.cancel().await;
                            return;
                        }
                        subscription.cancel().await;
                        if !should_retry {
                            return;
                        }
                        break;
                    }
                    None => {
                        subscription.cancel().await;
                        if !send_stream_error(
                            sender,
                            "gateway_unavailable",
                            "IBKR quote stream ended; reconnecting",
                        )
                        .await
                        {
                            return;
                        }
                        break;
                    }
                }
            },
            Err(error) => {
                let should_retry = should_retry_stock_quote(&error);
                if !send_gateway_stream_error(sender, &error).await {
                    return;
                }
                if !should_retry {
                    return;
                }
            }
        }
        if wait_for_reconnect(receiver, reconnect_delay).await {
            return;
        }
        reconnect_delay = reconnect_delay
            .saturating_mul(2)
            .min(STREAM_RECONNECT_MAX_DELAY);
    }
}

async fn send_balance_unavailable(
    sender: &mut futures::stream::SplitSink<WebSocket, Message>,
    account_ref: Option<String>,
    message: &str,
) -> bool {
    send_stream_event(
        sender,
        &StreamEvent::AccountBalance {
            version: PROTOCOL_VERSION,
            update: AccountBalanceUpdate {
                provider: "ibkr",
                ok: false,
                status: "unavailable",
                message: message.to_string(),
                account_ref,
                balance_usd: None,
                unit: "USD",
                summary: None,
            },
        },
    )
    .await
}

async fn send_gateway_stream_error(
    sender: &mut futures::stream::SplitSink<WebSocket, Message>,
    error: &GatewayError,
) -> bool {
    send_stream_error(sender, gateway_error_code(error), &error.to_string()).await
}

async fn send_stream_error(
    sender: &mut futures::stream::SplitSink<WebSocket, Message>,
    code: &'static str,
    message: &str,
) -> bool {
    send_stream_event(
        sender,
        &StreamEvent::Error {
            version: PROTOCOL_VERSION,
            error: ErrorBody {
                code,
                message: message.to_string(),
            },
        },
    )
    .await
}

async fn send_stream_event(
    sender: &mut futures::stream::SplitSink<WebSocket, Message>,
    event: &StreamEvent,
) -> bool {
    let Ok(message) = serde_json::to_string(event) else {
        return false;
    };
    sender.send(Message::Text(message.into())).await.is_ok()
}

async fn wait_for_reconnect(
    receiver: &mut futures::stream::SplitStream<WebSocket>,
    delay: Duration,
) -> bool {
    tokio::select! {
        message = receiver.next() => stream_client_ended(message),
        () = tokio::time::sleep(delay) => false,
    }
}

fn stream_client_ended(message: Option<Result<Message, axum::Error>>) -> bool {
    matches!(
        message,
        None | Some(Err(_))
            | Some(Ok(Message::Text(_)
                | Message::Binary(_)
                | Message::Close(_)))
    )
}

fn gateway_error_code(error: &GatewayError) -> &'static str {
    match error {
        GatewayError::Input(_) => "invalid_request",
        GatewayError::Unavailable(_) => "gateway_unavailable",
        GatewayError::Provider(_) => "provider_error",
        GatewayError::Contract(_) => "gateway_contract",
        GatewayError::Configuration(_) => "configuration",
    }
}

fn should_retry_stream(error: &GatewayError) -> bool {
    matches!(error, GatewayError::Unavailable(_))
}

async fn authenticated_json<Input, Output, Handler, HandlerFuture>(
    state: Arc<AppState>,
    request: Request,
    handler: Handler,
) -> Response
where
    Input: DeserializeOwned + HasVersion,
    Output: Serialize,
    Handler: FnOnce(Arc<AppState>, Input) -> HandlerFuture,
    HandlerFuture: std::future::Future<Output = Result<Output, GatewayError>>,
{
    let uri = request.uri().clone();
    let headers = request.headers().clone();
    let allowed_origin = state
        .security
        .validate_origin(&headers)
        .ok()
        .map(ToString::to_string);
    let body = match axum::body::to_bytes(request.into_body(), MAX_BODY_BYTES).await {
        Ok(body) => body,
        Err(_) => {
            return error_response(
                StatusCode::PAYLOAD_TOO_LARGE,
                "body_too_large",
                "request body exceeds 64 KiB",
                allowed_origin.as_deref(),
            );
        }
    };
    let origin = match state.security.authenticate(&headers, &uri, &body).await {
        Ok(origin) => origin,
        Err(error) => {
            let (status, code) = security_error_status(&error);
            return error_response(status, code, &error.to_string(), allowed_origin.as_deref());
        }
    };
    let input: Input = match serde_json::from_slice(&body) {
        Ok(input) => input,
        Err(error) => {
            return error_response(
                StatusCode::BAD_REQUEST,
                "invalid_request",
                &format!("request JSON does not match the v{PROTOCOL_VERSION} contract: {error}"),
                Some(&origin),
            );
        }
    };
    if input.version() != PROTOCOL_VERSION {
        return error_response(
            StatusCode::BAD_REQUEST,
            "unsupported_version",
            &protocol_version_mismatch_message(Some(input.version())),
            Some(&origin),
        );
    }
    match handler(state, input).await {
        Ok(output) => json_response(StatusCode::OK, &output, Some(&origin)),
        Err(error) => {
            let (status, code) = match error {
                GatewayError::Input(_) => (StatusCode::BAD_REQUEST, "invalid_request"),
                GatewayError::Unavailable(_) => {
                    (StatusCode::SERVICE_UNAVAILABLE, "gateway_unavailable")
                }
                GatewayError::Provider(_) => (StatusCode::BAD_GATEWAY, "provider_error"),
                GatewayError::Contract(_) => (StatusCode::BAD_GATEWAY, "gateway_contract"),
                GatewayError::Configuration(_) => {
                    (StatusCode::INTERNAL_SERVER_ERROR, "configuration")
                }
            };
            error_response(status, code, &error.to_string(), Some(&origin))
        }
    }
}

/// Map a request-security failure to its HTTP status and stable error code.
///
/// A protocol mismatch is not an authentication failure: it gets its own
/// `unsupported_version` code so the web app can tell the user which side to
/// update instead of reporting a bad token.
fn security_error_status(error: &SecurityError) -> (StatusCode, &'static str) {
    match error {
        SecurityError::Origin | SecurityError::Host => (StatusCode::FORBIDDEN, "forbidden"),
        SecurityError::Replay => (StatusCode::CONFLICT, "replay"),
        SecurityError::Version { .. } => (StatusCode::BAD_REQUEST, "unsupported_version"),
        _ => (StatusCode::UNAUTHORIZED, "unauthorized"),
    }
}

trait HasVersion {
    fn version(&self) -> u8;
}

impl HasVersion for ProbeRequest {
    fn version(&self) -> u8 {
        self.version
    }
}
impl HasVersion for DetectionRequest {
    fn version(&self) -> u8 {
        self.version
    }
}
impl HasVersion for CandleRequest {
    fn version(&self) -> u8 {
        self.version
    }
}
impl HasVersion for OptionChainRequest {
    fn version(&self) -> u8 {
        self.version
    }
}
impl HasVersion for OptionQuoteRequest {
    fn version(&self) -> u8 {
        self.version
    }
}
impl HasVersion for OrderRequest {
    fn version(&self) -> u8 {
        self.version
    }
}

impl HasVersion for AccountStateRequest {
    fn version(&self) -> u8 {
        self.version
    }
}

impl HasVersion for CancelOrderRequest {
    fn version(&self) -> u8 {
        self.version
    }
}

impl HasVersion for ReconcileOrderRequest {
    fn version(&self) -> u8 {
        self.version
    }
}

async fn preflight(State(state): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    preflight_for(state, headers, "POST", "POST, OPTIONS")
}

async fn stream_preflight(State(state): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    preflight_for(state, headers, "GET", "GET, OPTIONS")
}

fn preflight_for(
    state: Arc<AppState>,
    headers: HeaderMap,
    expected_method: &'static str,
    allowed_methods: &'static str,
) -> Response {
    if let Err(error) = state.security.validate_host(&headers) {
        return error_response(StatusCode::FORBIDDEN, "forbidden", &error.to_string(), None);
    }
    let origin = match state.security.validate_origin(&headers) {
        Ok(origin) => origin,
        Err(error) => {
            return error_response(StatusCode::FORBIDDEN, "forbidden", &error.to_string(), None);
        }
    };
    if headers
        .get(header::ACCESS_CONTROL_REQUEST_METHOD)
        .and_then(|value| value.to_str().ok())
        != Some(expected_method)
    {
        return error_response(
            StatusCode::METHOD_NOT_ALLOWED,
            "method_not_allowed",
            &format!("preflight method must be {expected_method}"),
            Some(origin),
        );
    }
    let requested_headers = headers
        .get(header::ACCESS_CONTROL_REQUEST_HEADERS)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default();
    if requested_headers
        .split(',')
        .map(|value| value.trim().to_ascii_lowercase())
        .filter(|value| !value.is_empty())
        .any(|value| !ACCESS_HEADERS.split(", ").any(|allowed| allowed == value))
    {
        return error_response(
            StatusCode::FORBIDDEN,
            "forbidden_headers",
            "preflight requested an unsupported header",
            Some(origin),
        );
    }
    let mut response = StatusCode::NO_CONTENT.into_response();
    add_cors_headers(response.headers_mut(), origin);
    response.headers_mut().insert(
        header::ACCESS_CONTROL_ALLOW_METHODS,
        HeaderValue::from_static(allowed_methods),
    );
    response.headers_mut().insert(
        header::ACCESS_CONTROL_ALLOW_HEADERS,
        HeaderValue::from_static(ACCESS_HEADERS),
    );
    if headers
        .get("access-control-request-private-network")
        .and_then(|value| value.to_str().ok())
        == Some("true")
    {
        response.headers_mut().insert(
            "access-control-allow-private-network",
            HeaderValue::from_static("true"),
        );
    }
    response
}

async fn fallback(State(state): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    let origin = state.security.validate_origin(&headers).ok();
    error_response(
        StatusCode::NOT_FOUND,
        "not_found",
        "bridge route does not exist",
        origin,
    )
}

fn json_response<T: Serialize>(status: StatusCode, body: &T, origin: Option<&str>) -> Response {
    let mut response = (status, axum::Json(body)).into_response();
    if let Some(origin) = origin {
        add_cors_headers(response.headers_mut(), origin);
    }
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response.headers_mut().insert(
        RELEASE_HEADER,
        HeaderValue::from_static(env!("CARGO_PKG_VERSION")),
    );
    response
}

fn error_response(
    status: StatusCode,
    code: &'static str,
    message: &str,
    origin: Option<&str>,
) -> Response {
    json_response(
        status,
        &ErrorEnvelope {
            version: PROTOCOL_VERSION,
            error: ErrorBody {
                code,
                message: message.to_string(),
            },
        },
        origin,
    )
}

fn add_cors_headers(headers: &mut HeaderMap, origin: &str) {
    if let Ok(origin) = HeaderValue::from_str(origin) {
        headers.insert(header::ACCESS_CONTROL_ALLOW_ORIGIN, origin);
        headers.insert(header::VARY, HeaderValue::from_static("Origin"));
        // Lets the page read the release header across origins.
        headers.insert(
            header::ACCESS_CONTROL_EXPOSE_HEADERS,
            HeaderValue::from_static(RELEASE_HEADER),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DEFAULT_BRIDGE_CLIENT_ID;
    use crate::security::{HEADER_NONCE, HEADER_SIGNATURE, HEADER_TIMESTAMP, HEADER_VERSION};
    use axum::body::Body;
    use axum::http::{Method, Request};
    use hmac::{Hmac, Mac};
    use sha2::{Digest, Sha256};
    use std::time::{SystemTime, UNIX_EPOCH};
    use tower::ServiceExt;

    type HmacSha256 = Hmac<Sha256>;
    const TOKEN: &str = "0123456789abcdef0123456789abcdef";
    const ORIGIN: &str = "https://sunkcostcapital.com";

    fn router() -> Router {
        build_router(BridgeConfig {
            listen: "127.0.0.1:17654".parse().unwrap(),
            gateway: GatewayConfig::standard(DEFAULT_BRIDGE_CLIENT_ID).unwrap(),
            security: SecurityConfig::new(TOKEN.to_string(), vec![ORIGIN.to_string()]).unwrap(),
        })
        .unwrap()
    }

    fn signed_request(path: &str, nonce: &str, body: &'static str) -> Request<Body> {
        versioned_request(path, nonce, body, PROTOCOL_VERSION, PROTOCOL_VERSION)
    }

    fn now_millis() -> String {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis()
            .to_string()
    }

    fn sign(
        version: u8,
        method: &str,
        path: &str,
        timestamp: &str,
        nonce: &str,
        body: &str,
    ) -> String {
        let body_hash = hex::encode(Sha256::digest(body.as_bytes()));
        let canonical =
            format!("v{version}\n{method}\n{path}\n{ORIGIN}\n{timestamp}\n{nonce}\n{body_hash}");
        let mut hmac = HmacSha256::new_from_slice(TOKEN.as_bytes()).unwrap();
        hmac.update(canonical.as_bytes());
        hex::encode(hmac.finalize().into_bytes())
    }

    /// Build a POST whose version header and signed canonical version may differ.
    fn versioned_request(
        path: &str,
        nonce: &str,
        body: &'static str,
        header_version: u8,
        signed_version: u8,
    ) -> Request<Body> {
        let timestamp = now_millis();
        let signature = sign(signed_version, "POST", path, &timestamp, nonce, body);
        Request::builder()
            .method(Method::POST)
            .uri(path)
            .header("host", "127.0.0.1:17654")
            .header("origin", ORIGIN)
            .header("content-type", "application/json")
            .header(HEADER_VERSION, header_version.to_string())
            .header(HEADER_TIMESTAMP, timestamp)
            .header(HEADER_NONCE, nonce)
            .header(HEADER_SIGNATURE, signature)
            .body(Body::from(body))
            .unwrap()
    }

    async fn json_body(response: Response) -> serde_json::Value {
        let bytes = axum::body::to_bytes(response.into_body(), MAX_BODY_BYTES)
            .await
            .unwrap();
        serde_json::from_slice(&bytes).unwrap()
    }

    /// Perform a real HTTP/1.1 WebSocket upgrade against a served router and
    /// return the raw response head and any body.
    async fn raw_websocket_handshake(subprotocol_version: &str, signed_version: u8) -> String {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let _ = axum::serve(listener, router()).await;
        });
        let timestamp = now_millis();
        let nonce = "0123456789abcdef0123456789abcdef";
        let signature = sign(signed_version, "GET", STREAM_PATH, &timestamp, nonce, "");
        let request = format!(
            "GET {STREAM_PATH} HTTP/1.1\r\n\
             Host: {address}\r\n\
             Origin: {ORIGIN}\r\n\
             Connection: Upgrade\r\n\
             Upgrade: websocket\r\n\
             Sec-WebSocket-Version: 13\r\n\
             Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\
             Sec-WebSocket-Protocol: sunk-ibkr-v{subprotocol_version}, timestamp-{timestamp}, nonce-{nonce}, signature-{signature}\r\n\
             \r\n"
        );
        let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
        stream.write_all(request.as_bytes()).await.unwrap();
        let mut received = Vec::new();
        let mut buffer = [0_u8; 4096];
        loop {
            let read = tokio::time::timeout(Duration::from_secs(5), stream.read(&mut buffer))
                .await
                .expect("bridge answered the handshake")
                .unwrap();
            received.extend_from_slice(&buffer[..read]);
            let text = String::from_utf8_lossy(&received).to_string();
            if let Some(head_end) = text.find("\r\n\r\n") {
                let length = text[..head_end]
                    .lines()
                    .find_map(|line| {
                        line.to_ascii_lowercase()
                            .strip_prefix("content-length:")
                            .map(|value| value.trim().parse::<usize>().unwrap())
                    })
                    .unwrap_or(0);
                if received.len() >= head_end + 4 + length || read == 0 {
                    server.abort();
                    return text;
                }
            }
            if read == 0 {
                server.abort();
                return text;
            }
        }
    }

    #[tokio::test]
    async fn upgrades_the_v3_websocket_subprotocol() {
        let response = raw_websocket_handshake("3", 3).await;
        assert!(
            response.starts_with("HTTP/1.1 101"),
            "unexpected handshake response: {response}"
        );
        assert!(
            response
                .to_ascii_lowercase()
                .contains("sec-websocket-protocol: sunk-ibkr-v3"),
            "bridge must select the v3 subprotocol: {response}"
        );
    }

    #[tokio::test]
    async fn rejects_the_v2_websocket_subprotocol_with_a_version_message() {
        let response = raw_websocket_handshake("2", 2).await;
        assert!(
            response.starts_with("HTTP/1.1 400"),
            "unexpected handshake response: {response}"
        );
        assert!(response.contains(r#""code":"unsupported_version""#));
        assert!(response.contains(r#""version":3"#));
        assert!(response.contains("speaks protocol v3, but the request used v2"));
    }

    #[tokio::test]
    async fn every_response_names_and_exposes_the_bridge_release() {
        let response = router()
            .oneshot(versioned_request(
                "/v1/client-exchange/probe",
                "00112233445566778899aabbccddeef1",
                r#"{"version":2,"accountRef":null}"#,
                2,
                2,
            ))
            .await
            .unwrap();
        // Errors carry it too, so the app learns the release from any reply.
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(
            response.headers().get(RELEASE_HEADER),
            Some(&HeaderValue::from_static(env!("CARGO_PKG_VERSION")))
        );
        assert_eq!(
            response
                .headers()
                .get(header::ACCESS_CONTROL_EXPOSE_HEADERS),
            Some(&HeaderValue::from_static(RELEASE_HEADER))
        );
    }

    #[tokio::test]
    async fn rejects_v2_requests_with_a_version_mismatch_message() {
        let response = router()
            .oneshot(versioned_request(
                "/v1/client-exchange/probe",
                "00112233445566778899aabbccddeef0",
                r#"{"version":2,"accountRef":null}"#,
                2,
                2,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(
            response.headers().get(header::ACCESS_CONTROL_ALLOW_ORIGIN),
            Some(&HeaderValue::from_static(ORIGIN))
        );
        let body = json_body(response).await;
        assert_eq!(body["version"], 3);
        assert_eq!(body["error"]["code"], "unsupported_version");
        assert_eq!(
            body["error"]["message"],
            "This IBKR bridge speaks protocol v3, but the request used v2. \
             Reload the Sunk web app so both sides match."
        );
    }

    #[tokio::test]
    async fn tells_a_newer_web_app_to_update_this_bridge() {
        let response = router()
            .oneshot(versioned_request(
                "/v1/client-exchange/probe",
                "00112233445566778899aabbccddeef1",
                r#"{"version":4,"accountRef":null}"#,
                4,
                4,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = json_body(response).await;
        assert_eq!(body["error"]["code"], "unsupported_version");
        assert!(
            body["error"]["message"]
                .as_str()
                .unwrap()
                .contains("Update the IBKR bridge")
        );
    }

    #[tokio::test]
    async fn rejects_a_v2_signature_presented_as_v3() {
        let response = router()
            .oneshot(versioned_request(
                "/v1/client-exchange/probe",
                "00112233445566778899aabbccddeef2",
                r#"{"version":3,"accountRef":null}"#,
                PROTOCOL_VERSION,
                2,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        let body = json_body(response).await;
        assert_eq!(body["error"]["code"], "unauthorized");
        assert_eq!(
            body["error"]["message"],
            "bridge request signature is invalid"
        );
    }

    #[tokio::test]
    async fn rejects_a_v2_body_inside_a_v3_envelope() {
        let response = router()
            .oneshot(signed_request(
                "/v1/client-exchange/probe",
                "00112233445566778899aabbccddeef3",
                r#"{"version":2,"accountRef":null}"#,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = json_body(response).await;
        assert_eq!(body["error"]["code"], "unsupported_version");
        assert!(
            body["error"]["message"]
                .as_str()
                .unwrap()
                .contains("request used v2")
        );
    }

    #[tokio::test]
    async fn supports_private_network_preflight_for_allowed_origin() {
        let response = router()
            .oneshot(
                Request::builder()
                    .method(Method::OPTIONS)
                    .uri("/v1/client-exchange/probe")
                    .header("host", "127.0.0.1:17654")
                    .header("origin", ORIGIN)
                    .header("access-control-request-method", "POST")
                    .header("access-control-request-headers", ACCESS_HEADERS)
                    .header("access-control-request-private-network", "true")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        assert_eq!(
            response
                .headers()
                .get("access-control-allow-private-network")
                .unwrap(),
            "true"
        );
    }

    #[tokio::test]
    async fn supports_private_network_preflight_for_stream() {
        let response = router()
            .oneshot(
                Request::builder()
                    .method(Method::OPTIONS)
                    .uri(STREAM_PATH)
                    .header("host", "127.0.0.1:17654")
                    .header("origin", ORIGIN)
                    .header("access-control-request-method", "GET")
                    .header("access-control-request-private-network", "true")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        assert_eq!(
            response
                .headers()
                .get(header::ACCESS_CONTROL_ALLOW_METHODS)
                .unwrap(),
            "GET, OPTIONS"
        );
        assert_eq!(
            response
                .headers()
                .get("access-control-allow-private-network")
                .unwrap(),
            "true"
        );
    }

    #[tokio::test]
    async fn rejects_replayed_authenticated_request() {
        let nonce = "00112233445566778899aabbccddeeff";
        let first = router()
            .oneshot(signed_request("/v1/client-exchange/probe", nonce, "{}"))
            .await
            .unwrap();
        assert_eq!(first.status(), StatusCode::BAD_REQUEST);
        let app = router();
        let first = app
            .clone()
            .oneshot(signed_request("/v1/client-exchange/probe", nonce, "{}"))
            .await
            .unwrap();
        assert_eq!(first.status(), StatusCode::BAD_REQUEST);
        let second = app
            .oneshot(signed_request("/v1/client-exchange/probe", nonce, "{}"))
            .await
            .unwrap();
        assert_eq!(second.status(), StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn rejects_unallowed_origin_and_non_loopback_host() {
        let origin_response = router()
            .oneshot(
                Request::builder()
                    .method(Method::OPTIONS)
                    .uri("/v1/client-exchange/probe")
                    .header("host", "127.0.0.1:17654")
                    .header("origin", "https://attacker.example")
                    .header("access-control-request-method", "POST")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(origin_response.status(), StatusCode::FORBIDDEN);
        let host_response = router()
            .oneshot(
                Request::builder()
                    .method(Method::OPTIONS)
                    .uri("/v1/client-exchange/probe")
                    .header("host", "192.168.1.2:17654")
                    .header("origin", ORIGIN)
                    .header("access-control-request-method", "POST")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(host_response.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn rejects_unsigned_and_oversized_requests_before_gateway_access() {
        let unsigned = router()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/v1/client-exchange/probe")
                    .header("host", "127.0.0.1:17654")
                    .header("origin", ORIGIN)
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"version":3,"accountRef":null}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(unsigned.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(
            unsigned.headers().get(header::ACCESS_CONTROL_ALLOW_ORIGIN),
            Some(&HeaderValue::from_static(ORIGIN))
        );

        let oversized = router()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/v1/client-exchange/probe")
                    .header("host", "127.0.0.1:17654")
                    .header("origin", ORIGIN)
                    .body(Body::from(vec![b'a'; MAX_BODY_BYTES + 1]))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(oversized.status(), StatusCode::PAYLOAD_TOO_LARGE);
    }

    #[tokio::test]
    async fn option_quote_route_validates_the_conid_before_gateway_access() {
        let path = "/v1/client-exchange/market-data/options/quote";
        let zero = router()
            .oneshot(signed_request(
                path,
                "00112233445566778899aabbccdd0001",
                r#"{"version":3,"conid":0}"#,
            ))
            .await
            .unwrap();
        assert_eq!(zero.status(), StatusCode::BAD_REQUEST);
        let body = json_body(zero).await;
        assert_eq!(body["error"]["code"], "invalid_request");
        assert_eq!(
            body["error"]["message"],
            "option quote conid must be a positive IBKR contract id"
        );

        let extra = router()
            .oneshot(signed_request(
                path,
                "00112233445566778899aabbccdd0002",
                r#"{"version":3,"conid":900001,"symbol":"SPY"}"#,
            ))
            .await
            .unwrap();
        assert_eq!(extra.status(), StatusCode::BAD_REQUEST);
        let body = json_body(extra).await;
        assert_eq!(body["error"]["code"], "invalid_request");
        assert!(
            body["error"]["message"]
                .as_str()
                .is_some_and(|message| message.contains("unknown field `symbol`"))
        );
    }

    #[tokio::test]
    async fn exposes_no_arbitrary_gateway_proxy_route() {
        let response = router()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/v1/client-exchange/proxy")
                    .header("host", "127.0.0.1:17654")
                    .header("origin", ORIGIN)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[test]
    fn ends_a_subscription_when_the_client_sends_another_data_frame() {
        assert!(stream_client_ended(Some(Ok(Message::Text("{}".into())))));
        assert!(stream_client_ended(Some(Ok(Message::Binary(
            vec![].into()
        )))));
        assert!(!stream_client_ended(Some(Ok(Message::Ping(vec![].into())))));
        assert!(!stream_client_ended(Some(Ok(Message::Pong(vec![].into())))));
        assert!(stream_client_ended(Some(Ok(Message::Close(None)))));
        assert!(stream_client_ended(None));
    }

    #[test]
    fn retries_only_unavailable_gateway_stream_errors() {
        assert!(should_retry_stream(&GatewayError::Unavailable(
            "gateway disconnected".to_string()
        )));
        for error in [
            GatewayError::Configuration("invalid configuration".to_string()),
            GatewayError::Input("invalid input".to_string()),
            GatewayError::Provider("provider rejected request".to_string()),
            GatewayError::Contract("invalid provider response".to_string()),
        ] {
            assert!(!should_retry_stream(&error));
        }
    }
}
