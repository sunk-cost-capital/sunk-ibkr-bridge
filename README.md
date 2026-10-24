# Sunk IBKR Bridge

`sunk-ibkr-bridge` is a loopback-only adapter that lets an explicitly allowed
HTTPS web application use the TWS socket API exposed by IB Gateway on the same
computer.

```text
HTTPS web app -> http://127.0.0.1:17654 -> 127.0.0.1:4002 or :4001
```

The core `Gateway` implementation is also used by the Electron native sidecar,
so browser and desktop paths share contract resolution, market data, validation,
connection recovery, and order-safety behavior.

## Run

Install [IB Gateway](https://www.interactivebrokers.com/en/trading/ibgateway-latest.php),
sign in, and enable socket API clients. The bridge tries the standard paper port
`4002` before the standard live port `4001`.

Then run:

```bash
SUNK_IBKR_BRIDGE_TOKEN='at-least-32-random-bytes' \
  cargo run --release --locked
```

When no token is configured, the bridge generates a temporary 32-byte token and
prints it once. Copy that token into the web app's IBKR connection settings. Set
a stable high-entropy token through the environment for normal use.

The production origin is allowed by default. Development origins must be
explicit:

```bash
cargo run --locked -- \
  --allow-origin https://sunkcostcapital.com \
  --allow-origin http://localhost:3000
```

Available configuration:

- `--listen 127.0.0.1:17654` — browser-facing loopback socket.
- `--gateway-address 127.0.0.1:4002` — optional explicit IB Gateway loopback
  socket instead of paper-first discovery.
- `--client-id 17654` — optional positive non-zero TWS client ID.
- `--allow-origin ORIGIN` — exact HTTP(S) origin; repeatable.
- `--token TOKEN` or `SUNK_IBKR_BRIDGE_TOKEN` — request-signing secret with at
  least 32 bytes.
- `SUNK_IBKR_GATEWAY_ADDRESS` and `SUNK_IBKR_CLIENT_ID` — process-local advanced
  equivalents for the gateway address and client ID.

## Browser protocol

Version 3 exposes only these high-level operations, all backed by the shared
`Gateway` TWS socket implementation:

- `POST /v1/client-exchange/detect`
- `POST /v1/client-exchange/probe`
- `POST /v1/client-exchange/market-data/candles`
- `POST /v1/client-exchange/market-data/options/chain`
- `POST /v1/client-exchange/market-data/options/quote` — one listed option by
  `conid`, quoted for the request (the web app prices capped option closes
  from it)
- `POST /v1/client-exchange/orders`
- `POST /v1/client-exchange/orders/reconcile`
- `GET /v1/client-exchange/stream` — authenticated WebSocket subscriptions for
  account summaries (net liquidation, total cash, available funds and gross
  position value in USD), provider-paced candles, tagged stock quotes, and one
  account's order statuses and executions.

There is no generic proxy route. Browser bodies cannot select a gateway address,
IBKR API operation, or arbitrary account. The bridge discovers accounts from IB
Gateway and verifies a configured account against that result.

Every POST requires these headers:

- `X-Sunk-Bridge-Version: 3`
- `X-Sunk-Bridge-Timestamp: <unix milliseconds>`
- `X-Sunk-Bridge-Nonce: <16 random bytes as lowercase hex>`
- `X-Sunk-Bridge-Signature: <HMAC-SHA256 as lowercase hex>`

The HMAC input is:

```text
v3\nPOST\n<path>\n<origin>\n<timestamp>\n<nonce>\n<sha256-hex-of-body>
```

The bridge checks the exact `Origin`, rejects non-loopback `Host` headers,
accepts each nonce once within a short timestamp window, limits bodies to 64 KiB,
and answers Private Network Access preflights only for configured origins.

Browser WebSockets cannot set those request headers. The stream handshake sends
the same version, timestamp, nonce, and HMAC signature as non-secret WebSocket
subprotocol values; the bridge token never appears in the URL or handshake.
The handshake offers the `sunk-ibkr-v3` subprotocol and the bridge selects it.
After the upgrade, the first frame must be one exact v3 account-balance,
candle, stock-quote, or order-events subscription. Stock-quote events carry bid, ask, last,
previous close, the IBKR market-data mode (`live`, `frozen`, `delayed`,
`delayed_frozen`), and the local time of the latest change for age checks.
Provider failures are typed events, and both the bridge and browser reconnect
their separate provider and local-transport boundaries with bounded backoff.

The bridge acknowledges every subscription it understands with a `ready` event
before any other frame, errors included. Live stock quotes need bridge release
`0.3.0` or newer, the first release with the `stock_quote` operation. The
published `v0.2.0` release speaks protocol v3 but lacks it: it answers a quote
subscription with an `invalid_request` error before `ready` (serde's "unknown
variant `stock_quote`"), which the web app reports as "Update your IBKR bridge
to 0.3.0 or newer to stream live quotes", with a link to the bridge releases,
instead of retrying. Account balances, candles, options and orders keep working
on `0.2.0`.

The `order_events` subscription (`{"operation":"order_events","version":3,
"accountRef":"U1234567"}`) needs `0.4.0` or newer. It streams `order_event`
frames whose `order` is a `status` (status, filled and remaining quantity,
average fill price) or an `execution` (execution id, time, quantity, price,
realized PnL once the commission report arrives) of one account's orders,
with parent id, order reference, order type and (from `0.6.0`) OCA group, so
bracket take-profit and stop-loss fills, and stop losses set on a position
(linked exits in an `oca-sunk-<uuid>` group with no parent), can be told
apart. It starts with a replay of the account's
latest statuses and recent executions. Older releases answer it with
`invalid_request` before `ready`; the web app then turns IBKR TP/SL pushes off
for the account ("Update your IBKR bridge to 0.4.0 or newer to push IBKR
take-profit and stop-loss fills") without retrying. See
[`docs/ibkr-gateway-contract.md`](docs/ibkr-gateway-contract.md#order-events-and-tpsl-fill-push).

## Protocol versions

Every version bump is incompatible in both directions, because the hosted web
app and this locally installed bridge update independently. The version is part
of the signed request string (`v3\n...`), so a signature made for another
version never verifies. A request or WebSocket handshake that names another
version is refused before any IB Gateway access with HTTP 400 and
`unsupported_version`; the error envelope's `version` is this bridge's version
and the message says which side is behind. The web app uses that envelope
version to tell the user to update an older bridge.

| Protocol | Bridge crate | Change |
| --- | --- | --- |
| 3 | 0.6.0 | Order events (`status` and `execution`) add `ocaGroup`: the order's IBKR OCA group with overfill protection (types 1 and 2), always sent and `null` when none. The web app names a parentless `STP`/`STP LMT` in an `oca-sunk-<uuid>` group a stop loss, so stop losses set on a position push, and reads the `X-Sunk-Bridge-Release` header to tell users on older bridges that only bracket fills push. Web builds before this support reject the unknown key and drop every order event, so production web must ship it before this bridge (or a desktop app bundling it) is published. Additive: older bridges keep streaming without it. |
| 3 | 0.5.0 | Account-balance updates add an optional `summary`: net liquidation, total cash, available funds and gross position value in USD. Web builds before this summary support reject the field, as they reject the new account-state order fields, so production web must ship this support before a bridge with it is published. Adds the `stop_limit`, `trailing_stop`, `trailing_stop_limit`, `market_if_touched`, `limit_if_touched`, `market_on_open`, `limit_on_open`, `market_on_close` and `limit_on_close` order types, `day` time in force, the optional order fields `trailingPercent`, `limitOffset`, `outsideRth` and `algo` (Adaptive), `orderType`, `timeInForce`, `outsideRth` and `algo` on account-state orders (`orderType` is `null` for an order the bridge cannot place as reported; the web terminal moves only a `limit` or `stop_market` exit it can re-place with the same time in force, extended hours and Adaptive routing), a trailing stop's `trailStopPrice` as its account-state `triggerPrice`, `priceIncrementRules` on stock quotes (the contract's IBKR market rules, always sent, empty until a retried lookup succeeds; the web app prices protected market limits on them, and reads an omitted key as a runtime that never sends them), and the `X-Sunk-Bridge-Release` response header the web app gates them on. Additive: older bridges refuse only orders that use them, before any Gateway access, and the web app does not send them there. |
| 3 | 0.4.0 | Adds the `order_events` WebSocket subscription, linked exits (`reduceOnly` orders join one OCA group per position; responses add `exit`), `ocaGroup`, `parentId` and `filledQuantity` on account-state orders, and `serverLink` (IB Gateway's link to IBKR's servers, from TWS 1100/1101/1102/2110) on account state. Additive: `0.2.0` and `0.3.0` reject only that subscription and `reduceOnly: true`. |
| 3 | 0.3.0 | Adds the `stock_quote` WebSocket subscription. Additive: `0.2.0` rejects only that subscription. |
| 3 | 0.2.0 | Orders require `customerOrderId` and may carry `protection`. |
| 2 | 0.1.0 | Signed HTTP and WebSocket contract. |

A new optional field keeps the protocol version, but it is compatible in one
direction only. A 0.2.0 bridge still streams net liquidation to a web app that
knows the account `summary`, which asks for 0.5.0 only to show the rest of it.
Web builds from before `summary` reject the unknown key, close the stream and
reconnect in a loop, so their IBKR balance disappears. **Promote the web build
that includes the account summary to production before publishing bridge
0.5.0.** Likewise, **promote the web build that accepts `ocaGroup` on order
events before publishing bridge 0.6.0 or a desktop app that bundles it**;
older web builds reject those events, so no IBKR stop fill would push.

To update, stop the running bridge, get the current build from the
[bridge releases](https://github.com/sunk-cost-capital/sunk-ibkr-bridge/releases/latest)
(the web app links there when it detects an older bridge) or pull the latest
source, and start it again with the same token (see [Run](#run)).
`sunk-ibkr-bridge --version` prints the crate version.

## Trading safety

All operations share one serialized IB Gateway connection. Read-only candles and
option-chain operations reconnect and retry once after an availability failure.
Order submission is never retried after the provider boundary.

Each order carries a caller-generated `sunk-<uuid>` `customerOrderId`, sent to
IBKR as the order's `order_ref`. The web app persists it before sending the
order. Provider precaution and advanced-rejection overrides remain disabled.
Partial execution proof reports `accepted`; only an explicit complete fill
reports `filled`.

Orders can be market, limit, stop, stop-limit, trailing stop (limit),
market/limit-if-touched, or opening and closing auction orders (MOO, LOO,
MOC, LOC), optionally working outside regular trading hours (`outsideRth`) or
routed through IBKR's Adaptive algo (bridge `0.5.0` and newer). Each type
requires exactly the prices it uses and refuses the rest; see
[Order types](docs/ibkr-gateway-contract.md#order-types). The bridge
never prices an order itself: the web app turns a stock market order into a
protected marketable limit from its live quote before sending it
([Protected market orders](docs/ibkr-gateway-contract.md#protected-market-orders)).

An order may attach TP/SL as `protection`. The bridge places it as a TWS
bracket: the parent with `transmit = false`, then take-profit `LMT` and
stop-loss `STP` children with `parentId`, the opposite action and the parent's
exact quantity, sharing an OCA group; only the last child transmits. Each child
has its own `customerOrderId`. The response reports each child as `attached`,
`pending`, `rejected`, or `unknown`. Protection a bracket cannot express fails
before submission with `unsupported IBKR protection`.

IBKR has no reduce-only attribute for stocks or options, so an order with
`reduceOnly: true` is a linked exit (bridge `0.4.0` and newer). The bridge
clamps it to the account's position in the contract and places it in one IBKR
OCA group with the position's other exits, type 2 (reduce with block): a fill
of one reduces the others by the filled quantity, so together they never
close more than the position or reverse it. Exits this bridge placed earlier
without a group are re-placed into the group first, and only then are the
originals cancelled; the position is never left without its stop.
Cancelling one linked exit re-places its siblings before IBKR cancels the old
group. Exits placed by TWS or another API client are counted but never moved;
when they leave no room the exit is refused with the reason. Older bridges
refuse `reduceOnly: true` before any Gateway access, and the web app then
places position exits unlinked with a warning. See
[`docs/ibkr-gateway-contract.md`](docs/ibkr-gateway-contract.md#linked-exits-reduce-only).

A timeout, transport loss, warning, or incomplete event stream after submission
returns `indeterminate` and blocks later orders for that account in the current
process. `POST /v1/client-exchange/orders/reconcile` searches the account's
open orders from every API client, today's completed orders, and today's
executions for that exact `order_ref`; only a match clears the block, and an
empty result clears nothing. The web app's persisted intent keeps the account
blocked across bridge and browser restarts until reconciliation finds the
order or the operator clears it (`release: true`). See
[`docs/ibkr-gateway-contract.md`](docs/ibkr-gateway-contract.md) for the
full operational contract.

## Licensing

Current source is licensed under GPL-3.0-only. The immutable
[`source-v0.6.0`](https://github.com/sunk-cost-capital/sunk-ibkr-bridge/tree/source-v0.6.0)
snapshot and its release source archive remain available under their original
MIT license. Existing release binaries and checksums are unchanged.
`SOURCE.json` identifies the upstream source of this tree; the subsequent license
change is recorded in this repository history.

## Source and releases

This bridge is [GPL-3.0-only licensed](LICENSE). You can inspect, modify, and build it
without installing the Sunk desktop app or accessing the private Sunk repository.
IB Gateway itself is supplied separately by Interactive Brokers.

Install [Rust](https://rustup.rs/), then build from this repository:

```bash
git clone https://github.com/sunk-cost-capital/sunk-ibkr-bridge.git
cd sunk-ibkr-bridge
cargo build --release --locked
```

Run `target/release/sunk-ibkr-bridge` (`.exe` on Windows), sign in to IB Gateway,
and paste the bridge token into the web app's IBKR connection settings.
Prebuilt downloads are on the [releases page](https://github.com/sunk-cost-capital/sunk-ibkr-bridge/releases).

`SOURCE.json` records the exact source revision, bridge version, and Rust toolchain.
Each release has an immutable `source-v<version>` tag. New binary release tags
`v<version>` point to the same source commit. The original binary-only `v0.2.0`
and `v0.6.0` tags are preserved; use their `source-v<version>` tag when available.
Source is exported from the shared bridge package, with only its registry
dependency closure in `Cargo.lock`; no private application source is required.

## Verify

```bash
cargo test --locked
cargo clippy --locked --all-targets -- -D warnings
```

The checked-in fixtures in `fixtures/v3` are consumed by both Rust and browser
tests. They verify protocol serialization, not live IBKR authentication,
entitlements, or order routing. A live smoke must remain read-only; use a paper
account for the first manually supervised order test.
