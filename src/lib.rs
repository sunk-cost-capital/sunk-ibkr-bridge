//! Loopback-only browser bridge for the Interactive Brokers TWS API.
//!
//! The crate exposes narrow authenticated HTTP and WebSocket boundaries for a
//! production HTTPS web application. It never binds to a non-loopback address
//! and never accepts arbitrary provider addresses or IBKR API operations from
//! a browser.

#![warn(missing_docs)]

mod contract;
mod gateway;
mod order_events;
mod security;
mod server;

pub use contract::{
    AccountOpenOrder, AccountPosition, AccountStateRequest, AccountStateResponse,
    AccountValueSummary, AdaptivePriority, AlgoStrategy, CancelOrderRequest, Candle, CandleRequest,
    CandleResponse, DetectionRequest, DetectionResponse, GatewayDetection, IbkrExecutionEvent,
    IbkrOrderEvent, IbkrOrderStatusEvent, InstrumentKind, LinkedExitOutcome, OptionChainRequest,
    OptionChainResponse, OptionContract, OptionContractRight, OptionQuoteRequest,
    OptionQuoteResponse, OrderAlgo, OrderEventSide, OrderProtection, OrderRequest, OrderResponse,
    OrderType, PROTOCOL_VERSION, PriceIncrementRule, ProbeRequest, ProbeResponse,
    ProtectionLegKind, ProtectionLegOutcome, ProtectionLegRequest, ProtectionOutcome,
    ProtectionStatus, QuoteDataType, ReconcileOrderRequest, ReconcileOrderResponse,
    ReconcileOrderState, RelinkedOrder, ServerLink, StockQuote, StockQuoteRequest,
};
pub use gateway::{
    AccountBalance, AccountBalanceSubscription, CandleSubscription, DEFAULT_BRIDGE_CLIENT_ID,
    DEFAULT_DESKTOP_CLIENT_ID, DEFAULT_LIVE_GATEWAY_ADDRESS, DEFAULT_PAPER_GATEWAY_ADDRESS,
    Gateway, GatewayConfig, GatewayError, StockQuoteSubscription, parse_gateway_address,
    should_retry_stock_quote,
};
pub use order_events::{OrderEventSubscription, should_retry_order_events};
pub use security::{SecurityConfig, SecurityError};
pub use server::{BridgeConfig, build_router, run};
