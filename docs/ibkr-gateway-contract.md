# IB Gateway TWS API operational contract

This document defines the direct Interactive Brokers TWS socket API contract
used by Sunk. It describes the application boundary and fail-closed behavior in
this repository; it is not evidence of market-data entitlements or successful
paper/live order routing.

## Gateway and process boundary

- Install and sign in to [IB Gateway](https://www.interactivebrokers.com/en/trading/ibgateway-latest.php).
  In the desktop app, the shell can instead start and supervise the user's own
  IB Gateway through their own [IBC](https://github.com/IbcAlpha/IBC) install
  (IBKR connection editor; see "IB Gateway launch (IBC)" in
  `apps/electron-desktop/README.md`). It writes IBC's config with
  `OverrideTwsApiPort` 4001 (live) or 4002 (paper), `ReadOnlyApi=no` and
  `AcceptIncomingConnectionAction=reject`, which relies on IB Gateway's default
  trust of `127.0.0.1` for the native runtime's connections. The IBKR login is
  kept in the OS keychain and reaches IBC only through an owner-only config
  file that loses it once the Gateway is logged in.
- Enable socket API clients in IB Gateway. API read-only mode may remain enabled
  for data-only use, but it must be disabled before IB Gateway will accept API
  orders. See IBKR's [installation and API configuration guide](https://www.interactivebrokers.com/campus/trading-lessons/installing-configuring-tws-for-the-api/).
- Sunk connects only to explicit IPv4 loopback sockets. It never accepts a
  browser-selected or renderer-selected IBKR host.
- Default discovery tries the paper socket `127.0.0.1:4002` before the live
  socket `127.0.0.1:4001`. These are IB Gateway's standard ports; an advanced
  process-local override may set `SUNK_IBKR_GATEWAY_ADDRESS` to another explicit
  loopback socket.
- The browser bridge uses TWS client ID `17654`; the Electron native sidecar
  uses `17655`. Each process reserves its configured ID plus `100` for transient
  discovery, so autodetection never duplicates an attached session's client ID.
  The processes must not share a client ID because IB Gateway uses it to
  distinguish simultaneous API clients. `SUNK_IBKR_CLIENT_ID` is an advanced
  process-local override and must remain positive, non-zero, and low enough to
  reserve the discovery ID.
- A successful connection is not enough for discovery. The adapter also
  requires IB Gateway to return at least one managed account. Account probes
  select only an account returned by that session.
- Desktop autodetection closes its temporary TWS client before returning. A
  detected Gateway is available for the user to connect; discovery alone does
  not create a saved Sunk connection or leave a client attached in IB Gateway.

The official [TWS API connection lesson](https://www.interactivebrokers.com/campus/trading-lessons/essential-components-of-tws-api-programs/)
documents the host, port, and unique client-ID requirements.

## Shared application boundary

`packages/ibkr-web-bridge/src/gateway.rs` owns the one IBKR implementation used
by both local application paths:

```text
hosted browser -> authenticated loopback HTTP bridge -> Gateway -> IB Gateway
Electron renderer -> versioned native sidecar protocol -> Gateway -> IB Gateway
```

The `Gateway` boundary hides socket selection, connection reuse, managed-account
selection, account-value normalization, contract resolution, market-data
normalization, and order outcome classification. Browser and Electron callers
can request only the supported high-level operations; they cannot select
arbitrary TWS API methods or provider addresses.

Each process keeps one long-lived `Gateway` facade. Explicit account probes,
account-value subscriptions, market data, and orders reuse its stateful TWS
session, while account-independent autodetection uses a temporary client that is
disconnected before the result is returned. Request setup is serialized so order
events cannot race with other TWS requests; established account and candle
subscriptions then continue on the shared provider connection. Account values
stay client-local: Electron publishes typed sidecar events and the browser bridge
publishes typed authenticated WebSocket events. Neither sends personal account
data to the hosted BFF.

## Connection recovery

- Discovery opens a fresh bounded connection. Account probes reconnect when the
  cached socket is no longer usable.
- Account-value subscription setup, historical-candle, and option-chain reads
  retry once only when the first attempt fails as unavailable. The cached
  connection is discarded before the retry.
- Native candle and option-chain reads return validated unavailable state after
  that retry is exhausted. The renderer pauses IBKR-only controls, keeps the
  rest of the terminal usable, and retries later without exposing socket
  addresses, provider errors, or Electron IPC internals.
- Connected-provider request rejections, including market-data permission
  failures, remain `provider_error` failures. They do not trigger the Gateway
  disconnected state or the read-only reconnect retry.
- An established account-value stream emits an unavailable state and reconnects
  with bounded exponential backoff after provider or sidecar transport loss.
  Renderer unsubscribe and app shutdown cancel the provider subscription.
- An established stock-quote stream emits an unavailable state and resubscribes
  with the same bounded backoff after Gateway loss and after TWS reports lost
  market-data connectivity (1100, 1101, 1300). A `live` quote that receives
  no tick at all for 60 seconds is resubscribed in place without an outage
  (see below). Provider rejections (for example a
  competing live session) and missing Gateway configuration also retry with
  backoff. Only invalid requests and contracts IBKR cannot resolve (code 200)
  end the stream; the next consumer that opens the quote starts a new one.
  Quote setup never takes the order mutex and never resets the shared
  connection, so a slow contract lookup cannot delay an order or tear down
  other streams. IBKR's market-data mode is connection-wide, so a separate
  lock covers only one mode switch plus one `reqMktData` send (bounded by the
  6-second quote timeout, and released before any ticks are read). Each
  stock-quote line, stock snapshot and option quote takes it for its own
  request, so an option chain never holds it for the whole chain, and a quote
  line never subscribes under the chain's delayed mode. An idle live line
  takes the lock before cancelling its old request, so it is without a line
  only for the cancel, the switch and one send.
- Browser account-value, candle and stock-quote streams use the same provider
  reconnect behavior. A separate bounded browser reconnect loop covers
  bridge-process or localhost WebSocket restarts, using a fresh signed nonce on
  every handshake.
  Its backoff resets only when the stream delivers data, not on the bridge's
  `ready` acknowledgement, so a stream that fails right after `ready` keeps
  backing off.
- Contract errors and invalid inputs are not retried.
- IB Gateway keeps answering API reads from its cache after it loses its
  own connection to IBKR's servers, so a successful read does not prove the
  account is live. Each new TWS connection starts a watch on its system
  messages (request id -1):
  - `1100` (connectivity between IB and TWS lost) marks the link `lost`;
    only `1101` or `1102` (restored, data lost or maintained) clears it.
  - `2110` (connectivity between TWS and server broken) marks it `lost`
    too; `1101`/`1102` or a farm-OK message (`2104`, `2106`, `2158`)
    clears it, since IBKR documents no code of its own for that.

  Account-state responses report the result as `serverLink: connected |
  lost` (bridge `0.4.0` and the matching desktop app; older runtimes omit
  it). Messages sent before the watch subscribed are not seen, so a link
  already down when the connection opened reads as `connected` until IBKR
  next reports it. The web terminal treats `lost` as a disconnected session
  (cause `server_link_lost`, "IB Gateway lost its link to IBKR"): it keeps
  the last live snapshot as stale and pauses IBKR trading until a read
  reports the link `connected`. Sources: TWS API
  [message codes](https://interactivebrokers.github.io/tws-api/message_codes.html)
  and IBKR Campus [system message codes](https://www.interactivebrokers.com/docs/tws-api/doc/error-handling/system-message-codes).
- Order submission is never retried after crossing the `placeOrder` boundary.

The retry distinction is deliberate: a duplicate market-data read is safe, but
a repeated order after a lost acknowledgement could create a second trade.

## Contract and market-data behavior

- Account values use one account-summary subscription for `NetLiquidation`,
  `TotalCashValue`, `AvailableFunds`, `GrossPositionValue`, and `$LEDGER:USD`,
  keeping each stream to a single request under IBKR's limit of two concurrent
  account-summary subscriptions. Rows for other managed accounts are ignored.
  Rows are collected until `AccountSummaryEnd`, which marks the end of the
  initial snapshot (not of the subscription), so the first update is one
  coherent summary; if the marker is late, the rows received within the read
  deadline are used when net liquidation is among them. After that each changed
  value emits a new update. IBKR generally publishes account-summary changes
  about every three minutes, so the UI does not promise tick-level balance
  freshness.
- The four summary values arrive in the account's base currency. They are
  converted to USD with the `ExchangeRate` row of the account's own USD ledger;
  no third-party or hard-coded FX rate is used. `GBp`/`GBX` labels are read as
  pence (1/100 of `GBP`). An update is available only when net liquidation is a
  finite value that converts to USD. A non-USD account without a USD ledger
  rate is reported as temporarily unavailable and retried, never relabeled as
  dollars. A malformed net liquidation is a contract error. Any other summary
  value that IBKR does not report, or reports as a malformed number, is sent as
  `null` and shown as unavailable; a malformed USD rate counts as a missing
  rate.
- **FX direction.** IBKR documents a ledger's `ExchangeRate` as the rate of
  that currency to the base currency, and its ledger examples price in base
  units (a USD-base account shows EUR at 1.155135). The USD ledger's rate is
  therefore base-currency units per one US dollar, and the bridge divides by it
  (`usd_per_base_unit` in `packages/ibkr-web-bridge/src/gateway.rs`). It has not
  been observed on a live non-USD-base account: the paper account probed on
  2026-09-30 is USD-base, where every rate is 1.
- **`$LEDGER-` tag prefix.** TWS and IB Gateway 10.47 and later can prepend
  `$LEDGER-` to per-currency account values ("Prepend '$LEDGER-' prefix to
  per-currency account values" under Global Configuration › API › Settings; on
  by default for new installs, off for upgrades), and account-summary rows for
  `$LEDGER` requests have been seen with it too. The bridge accepts the USD
  ledger's rate as `ExchangeRate` or `$LEDGER-ExchangeRate`. Account-level tags
  (`NetLiquidation` and the others) keep their names under either setting, and
  only those exact names are read for them.
- Available updates carry `balanceUsd` (USD net liquidation) and a `summary`
  with the four USD values, the native currency and net liquidation, and the
  rate used. Bridges before 0.5.0 and older desktop runtimes omit `summary`; the
  web app then shows net liquidation with a note naming the update.
- **Release order for `summary`.** The field is compatible in one direction
  only. A web build that knows `summary` accepts updates with or without it, so
  new web with an old bridge is safe. Web builds released before `summary`
  parse bridge events strictly: they reject the unknown key, close the stream
  and reconnect in a loop, so the IBKR balance disappears. Promote the web
  build that includes `summary` to production before publishing bridge 0.5.0 or
  later. The desktop app is not affected because its sidecar and web contract
  ship together.
- Stocks resolve through SMART/USD contract details. A supplied conid must
  resolve to the expected security type.
- Options resolve from IBKR option-chain definitions, choose the nearest valid
  expiry, and bound nearby strikes to at most 20 per side. Stock option-chain
  discovery leaves IBKR's futures/FOP exchange parameter empty, then selects
  the returned SMART chain. Each candidate is resolved to a concrete
  listed-option contract before use. When IBKR returns no SMART chain, the read
  succeeds with an empty contract list so callers can treat the equity as
  non-optionable; transport and malformed-contract failures remain errors.
- Underlying quotes use snapshots without generic ticks. Option quotes use
  bounded streaming requests because IBKR rejects generic option volume and
  open-interest ticks in snapshot mode. The adapter cancels every option stream
  after collecting the available top-of-book and ranking metadata or reaching
  its deadline. Underlying reference prices prefer the provider's last, mark,
  or bid/ask midpoint, then fall back to its close or delayed close when the
  market is closed.
- Standalone and combined price/size ticks share one normalization path,
  including IBKR's delayed tick types and provider timestamps. Missing or
  malformed underlying price and contract data is rejected rather than
  invented.
- IBKR code `10091` means part of an option quote requires another market-data
  subscription. Any fields returned with that notice remain usable for display,
  but the incomplete contract is non-tradable instead of failing the entire
  chain. Delayed option quotes and quotes without a provider timestamp are also
  non-tradable; the adapter never substitutes the local clock.
- Stock quotes stream top-of-book bid, ask, last and previous close for one
  SMART-routed stock contract. The bridge requests IBKR's delayed-frozen
  market-data mode, so an entitled account receives live data, an account
  without a subscription receives delayed data, and a closed market reports
  its last values. Each quote is tagged `live`, `frozen`, `delayed` or
  `delayed_frozen` from IBKR's market-data-type report; a delayed tick type
  downgrades the tag, and undelayed ticks are withheld until IBKR reports the
  mode because frozen data uses the same tick types as live data. A
  non-positive bid or ask (IBKR sends `-1`) clears that side; a non-positive
  last or close keeps the previous value. Price and mode changes are
  published; only a price change refreshes `updatedAtMs`, the local wall
  clock the same-machine consumer uses to compute the quote's age. Size-only
  ticks and a mode report change nothing about the age. Frozen values are
  replayed from the close, so their age is measured from the last trade when
  IBKR reports one.
- Stock quote outages: TWS connectivity notices `1100`, `1101` and `1300`
  end the line and are reported as unavailable before the stream
  resubscribes with backoff. A `live` line that delivers no provider item at
  all for 60 seconds may have been dropped silently, so it is resubscribed in
  place on the same connection and resolved contract. That is not an outage
  and is not reported; only a failed resubscribe is. Only the line's own items
  reset the 60-second window; connection-wide notices such as farm status do
  not.
- The web terminal uses a quote in these ways:
  - Streams: one quote stream per IBKR-routed stock or ETF that the user is
    viewing, has starred in the watchlist, or holds as a position. Every
    consumer of the same contract shares one IBKR market-data line.
    Positions stream by conid and instruments by symbol; when a symbol
    stream's first quote names a contract that already streams, the symbol
    stream stops and its consumers join the existing line. A stream released
    and re-requested in the same render (a position added or closed) keeps
    running.
  - Price: the quote's price is the last trade, else the bid/ask midpoint,
    else one side of the book. The previous close is shown but never used as
    a price, as FD Terminal prices only from ask, last or bid.
  - Pricing: order previews, the header and watchlist chips use the quote as
    the instrument's market price only while it is streaming, `live`, USD,
    and no older than 15 seconds in the regular US session (60 seconds
    outside it; the limit in force changes at the open and the close).
    Otherwise the instrument's market source prices them. When no other
    source has a price at all, the header, watchlist chip and order-size
    preview fall back to the IBKR quote whatever its mode, and the header,
    chip and ticket show its tag and age.
  - Position marks: IBKR account state has no mark price, so stock positions
    are marked at the USD quote's price whatever its mode, as FD Terminal
    does, and each row shows the tag and age. A quote kept through an outage
    stays struck through with its growing age. A listing quoted in another
    currency is not converted; its row keeps the account snapshot's mark and
    is tagged with the currency and `not marked`.
  - Market-order warnings: the ticket warns when the quote is not `live`,
    older than those limits, or disconnected, matching FD Terminal's
    protected-market-order thresholds.
- Historical requests are bounded to 1,000 normalized bars and use IBKR
  `TRADES` data including extended trading hours. See the official
  [historical-bar contract](https://interactivebrokers.github.io/tws-api/historical_bars.html).
- Limit prices are validated against the current market-rule increments returned
  for the resolved contract.

IBKR's [contract details](https://interactivebrokers.github.io/tws-api/contract_details.html)
and [financial instruments](https://interactivebrokers.github.io/tws-api/contracts.html)
references describe the provider-side contract resolution semantics.

## Order submission and outcomes

Before submission, Sunk validates the provider, account, instrument family,
side, size/quantity, order type, time in force, slippage bound, limit price, and
unsupported flags. Listed options require an explicit positive whole-contract
quantity and conid. Stock orders may use an explicit quantity or cash quantity.

The provider order:

- uses an IBKR-issued next valid order ID;
- carries the caller's `sunk-<uuid>` `customerOrderId` as its `order_ref`; the
  runtime rejects any other shape before submission;
- is transmitted immediately, except a bracket parent (see below);
- does not override percentage constraints; and
- leaves advanced-error overrides empty, so precautionary or advanced rejection
  checks are never auto-approved.

### Order types

From bridge `0.5.0` (`OrderType` in
`packages/ibkr-web-bridge/src/contract.rs`) an order's `orderType` is one of:

| `orderType` | TWS order | Requires |
| --- | --- | --- |
| `market` | `MKT` | — |
| `limit` | `LMT` | `limitPrice` |
| `stop_market` | `STP` | `stopPrice` |
| `stop_limit` | `STP LMT` | `stopPrice`, `limitPrice` |
| `trailing_stop` | `TRAIL` | `trailingPercent` |
| `trailing_stop_limit` | `TRAIL LIMIT` | `trailingPercent`, `limitOffset` (`lmtPriceOffset`) |
| `market_if_touched` | `MIT` | `stopPrice` (the touch trigger) |
| `limit_if_touched` | `LIT` | `stopPrice`, `limitPrice` |
| `market_on_open` | `MKT`, time in force `OPG` | `timeInForce: "day"` |
| `limit_on_open` | `LMT`, time in force `OPG` | `limitPrice`, `timeInForce: "day"` |
| `market_on_close` | `MOC` | `timeInForce: "day"` |
| `limit_on_close` | `LOC` | `limitPrice`, `timeInForce: "day"` |

A price or trailing field the type does not use is refused, never ignored.
Every type except `market` and `limit` needs an explicit `quantity`, because
IBKR sizes only those two by cash. Auction types (on open and on close) are
stocks only and refuse `outsideRth`. Both prices of a stop-limit or
limit-if-touched order are checked against the contract's price increment.
`timeInForce` is `day`, `gtc`, `ioc` or `fok`.

Two attributes apply across types:

- `outsideRth: true` sets IBKR's `outsideRth`, so the order works, and a stop
  triggers, in extended hours. IBKR answers `2109` when it ignores the flag
  for a type or destination; the order is processed without it.
- `algo: {"strategy": "adaptive", "priority": "urgent" | "normal" |
  "patient"}` routes a `market` or `limit` order through IBKR's
  [Adaptive algo](https://www.interactivebrokers.com/campus/trading-lessons/adaptive/)
  (`algoStrategy = "Adaptive"`, `adaptivePriority`). IBKR algos do not take
  GTC, so it requires `timeInForce: "day"`.

Both are omitted from the wire when unused, so a plain order is byte-for-byte
what earlier bridges accept. Bridges before `0.5.0` refuse the new types with
`orderType must be market, limit, or stop_market`, `day` with `unsupported
IBKR timeInForce`, and the new fields as unknown JSON fields, all as
`invalid_request` before any Gateway access.

The web app therefore sends a new field only to a runtime known to take it
(`fitIbkrOrderToRuntime` in `~/lib/ibkr-order-types`):

- From `0.5.0` every bridge response, errors included, carries
  `X-Sunk-Bridge-Release: <crate version>`, exposed to the page through
  `Access-Control-Expose-Headers`. It is a header, not a body field, so web
  builds whose response schemas are strict keep working. The web app records
  it per bridge origin from every signed request; a reply without it comes
  from an older bridge. Before its first reply the bridge is probed once.
- The desktop shell names the runtime it bundles as `ibkrRuntimeRelease`
  (`IBKR_RUNTIME_RELEASE` in `@sunk/desktop-contract`); an older shell lacks
  it.
- For an older runtime, `outsideRth` and the Adaptive algo are omitted (an
  Adaptive `day` order becomes GTC), so ordinary market, limit and stop orders
  keep working, and an order of a new type is refused before it is journaled
  or sent. The ticket disables EXT and ADAPT and the new types with the
  update reason.
- A runtime that claims `0.5.0` but still refuses gets the same "update the
  bridge" message; nothing else is sent.

Bracket TP/SL children inherit the parent's `outsideRth`, so a parent filled in
extended hours has a take profit that can fill, and a stop loss that can
trigger, in extended hours too. A linked exit (`reduceOnly`) must be a
`market`, `limit`, `stop_market` or `stop_limit` order: the Gateway re-places
exits into a new OCA group and can reproduce only `LMT`, `STP` and `STP LMT`
exactly, so trailing, touched and auction exits are refused before Gateway
access.

A relinked exit (see [Linked exits](#linked-exits-reduce-only)) keeps its
`outsideRth` and Adaptive routing. Account-state open orders report a
trailing stop's current stop (`trailStopPrice`) as `triggerPrice`, never its
`auxPrice`, which for an amount trailing stop is the trailing amount.

Account-state open orders also report how each order is placed, so an exit
can be re-placed exactly:

- `orderType`: the bridge order type the working TWS order is (`MKT`/`LMT`
  with time in force `OPG` are the on-open types), or `null` for an order the
  bridge cannot place as reported: a TWS type it does not place (`REL`,
  pegged) or an algo other than Adaptive.
- `timeInForce`: `day`, `gtc`, `ioc` or `fok` (an on-open order reports
  `day`, as it is requested), or `null` for `GTD` or `DTC`.
- `outsideRth`: TWS `outsideRth`.
- `algo`: the Adaptive algo with its priority (`normal` when IBKR omits it),
  or `null`.

All four are always present from `0.5.0`, so their absence identifies an
older runtime.

A trailing stop's `triggerPrice` is its current trail stop price, and the web
terminal names its exits from the type: a trailing stop or stop is the
stop loss, and an if-touched order the take profit, whatever their price
against entry. A trailing stop that has risen past entry therefore stays the
stop loss, and arming a take profit beside it places a fresh order.

The web terminal moves a take profit or stop loss from the chart by
cancelling it and placing a `limit` or `stop_market` exit. That replacement
keeps the replaced exit's `timeInForce` (`day` or `gtc`), `outsideRth` and
Adaptive routing. Only an exit it can reproduce is draggable: a `limit` or
`stop_market` order with time in force `day` or `gtc`, from a runtime that
reports all four fields. Any other exit is drawn and can be cancelled, but
its line is read-only, and a drop that would replace it is refused. That
covers trailing, stop-limit, touched, auction and unknown exits, `ioc`,
`fok` or unreported time in force, and every exit from a runtime before
`0.5.0`, whose missing report cannot tell an extended-hours stop from a
regular-hours one. Adaptive takes no stop, so a drop that would turn an
Adaptive take profit into a stop loss is refused too.

The order stream is interpreted conservatively:

- `filled` requires an explicit filled status with positive filled quantity and
  no remaining quantity;
- an execution without full-fill proof reports `accepted`, never `filled`;
- submitted/pre-submitted reports `resting`;
- a cancelled or inactive order with a nonzero fill reports `accepted`, while
  the same terminal states with no fill report `rejected`;
- a timeout, transport loss, unclassified IBKR message, malformed/ended event
  stream, or otherwise incomplete post-send outcome reports `indeterminate`.

TWS sends order warnings through the same error callback as failures, and
`ibapi` 3.3.0 surfaces every order-bound code outside 2100-2169 as a stream
error that ends the order's subscription. The runtime classifies IBKR message
codes from explicit lists taken from IBKR's
[message codes](https://interactivebrokers.github.io/tws-api/message_codes.html)
(`packages/ibkr-web-bridge/src/gateway/order_messages.rs`):

- Informational, the order may still be live: `399` (order message, notably
  "Your order will not be placed at the exchange until <session open>" for an
  order sent outside regular trading hours and held `PreSubmitted`), `404`
  (held while shares are located for a short sale), `2109` (outside-RTH flag
  ignored), `2137` (cross-side warning), and `202` (cancelled, which may
  follow a fill). These never decide the outcome. Reading continues until a
  definitive status; when the message ended the stream, the runtime drops the
  subscription and reads `reqAllOpenOrders` and `reqCompletedOrders` for the
  exact `order_ref` (a completed entry wins over an open one) until a
  definitive status or a four-second follow-up deadline. Without one the
  outcome stays `indeterminate`. After a `202` only a final state (filled,
  cancelled, inactive) counts, so a stale working status never turns a
  cancelled order into an acceptance.
- Rejections: `110`, `200`, `201`, `203`, `434` report `rejected` with IBKR's
  text.
- Every other code reports `indeterminate`.

A definitive response keeps the protocol shape and carries IBKR's text in
`message`: each distinct informational message is appended as one `IBKR
warning <code>: <text>.` sentence, for example `IBKR order is working (41).
IBKR warning 399: Your order will not be placed at the exchange until
2026-10-01 09:30:00 US/Eastern.`. On a rejected or cancelled order, the
cancellation (`202`) and IBKR's completed-order status text are appended as
`IBKR reason[ <code>]: <text>.` instead; other informational codes keep the
warning label, so a hold notice is never presented as the cause. At most
three sentences are appended (each at most 400 characters), followed by
`IBKR sent <n> more messages.` when there were more.
The web ticket shows an accepted response with warnings as accepted (or
filled), held until the stated open when IBKR names one, with a dismissible
notice; it does not pause personal orders. Older terminals show the same
message as a plain acceptance.

Order execution uses separate one-second operation-lock, fourteen-second
preparation, two-second submission-start (shared by every order of a bracket),
six-second acknowledgement, and four-second follow-up deadlines. The 27-second
internal maximum finishes before the browser and Electron 30-second caller
deadlines and the bridge's 50-second HTTP deadline. Only pre-submission
deadline failures are returned as availability errors. Once submission may have
started, timeout responses retain the generated `customerOrderId` and the
account reconciliation block.

### Protected market orders

The web app never sends an IBKR stock `market` order as a plain `MKT` unless
asked to. Before journaling it (`~/lib/ibkr-protected-market`, called from
`executeJournaledIbkrOrder`), it prices FD Terminal's protected order from the
shared live IBKR quote stream:

- the reference is the ask for a buy and the bid for a sell (last, then the
  other side, when one is missing);
- the cap is 10 bps during the regular US session and 50 bps outside it, or
  the caller's `marketSlippageBps` (the ticket's IOC cap, a position close's
  venue percentage); a spread wider than 1% of the mid raises any cap to 150
  bps;
- the limit is the most aggressive valid tick within the cap, rounded toward
  the reference on the contract's own grid, so it never exceeds the cap and
  stays marketable (at or above the ask for a buy, at or below the bid for a
  sell). A bridge that reports tick rules always sends `priceIncrementRules`
  on its stock quotes: the contract's IBKR market rules that order
  validation checks limits against, looked up beside the quote and retried
  with backoff (1 s doubling to 60 s) until a lookup succeeds. `ibapi`
  answers market-rule requests by message type, not request id, so the
  bridge runs every market-rule lookup (quotes, option chains, order
  validation) one at a time, refuses an answer for a different rule id, and
  keeps each rule for its lifetime. Until then,
  or when IBKR reports none, the list is empty and a protected order is
  refused rather than guessed. Support is read from the quote itself, not
  the release: a runtime that never sends tick rules (0.5.0 builds before
  this field, and older) omits the key, and there cents from $1 and 0.0001
  below (whole yen for JPY) are assumed, which the bridge's own increment
  check guards before anything is sent;
- with TP/SL attached, the limit also stays inside them as the bridge aligns
  and checks them (a buy's limit below its take profit and above its stop
  loss, a sell's the reverse), stepping back from the cap when needed. When
  no marketable tick fits, the order is refused in any session, before
  journaling, rather than sent with a parent the bridge rejects or without
  its TP/SL;
- the quote must be `live` and at most 15 seconds old in the regular session
  (60 outside it), from a stream that is currently delivering, and carry a
  positive bid, ask or last and its tick rules (a quote with only the
  previous close, or an empty rule list while the bridge's lookup is
  pending, is still arriving, and the quote line stays open meanwhile); the
  app waits up to 4 seconds for one.

The order goes out as `limit` with `outsideRth: true`, keeps an IOC or FOK
time in force and is otherwise a `day` order, and during the regular session
a USD order is routed through Adaptive (normal priority), as FD does for USD
SMART orders. Without a qualifying quote the order is refused during the
regular session. Outside it only a missing or stale quote sends it to the
next opening auction as `market_on_open`; every other pricing failure (a
delayed or frozen quote, no usable price, no tick rules, a TP/SL conflict)
is refused.
An auction order is unpriced, needs a share quantity and cannot be a linked
exit or a bracket parent. So the fallback refuses an order with an explicit
cap (`marketSlippageBps`, which no auction order would honour and a current
bridge refuses on one), a cash-sized order, a position close (`reduceOnly`) and
an order with TP/SL, rather than drop its cap, quantity, link or protection.
Nothing is journaled or sent for a refused order.

The conversion runs before the order is previewed. With order confirmation
on, the dialog shows the protected limit or opening-auction order, and that
exact order is sent; nothing re-prices it after confirmation. An order that
cannot be priced is refused before any dialog opens. The journaled send
refuses a stock market order that was not priced this way, so no entry path
can send one unprotected. The response message says
how the order was protected.

`instantMarket: true` (the ticket and quick buys set it when IBKR slippage
protection is off in Settings › Trading) keeps a plain `MKT` during the
regular session only. The ticket does not set it while its own IOC cap is
engaged, because an engaged cap is always a bounded order, and a request
carrying both `instantMarket` and `marketSlippageBps` is refused. Option orders are unaffected; a capped option
buy is already an IOC limit priced from its own quote, and a capped option
close is bounded as [Slippage caps on stops and option orders](#slippage-caps-on-stops-and-option-orders)
describes. `outsideRth`, `day`, Adaptive and
market-on-open need bridge `0.5.0`, so for an older runtime (see
[Order types](#order-types)) the protected limit goes out as a plain limit IOC
(GTC for a bracket parent) with the same cap and a notice naming the update,
and the market-on-open fallback is refused before anything is sent.

FD reference: `backend/dist/connectors/ibkr.js` (`getProtectedStockLimitPrice`,
`placeOrder`).

### Slippage caps on stops and option orders

A slippage cap (`marketSlippageBps`) bounds an order only when the order has
a limit. IBKR fills a `market`, `stop_market` (`STP`), `market_if_touched`
(`MIT`), `trailing_stop` (`TRAIL`), `market_on_open` and `market_on_close`
order at market, so the bridge refuses any of them carrying a cap
(`OrderType::fills_at_market`) before anything reaches IB Gateway. The web
app handles such an order first (`~/lib/ibkr-slippage-cap`, run in
`prepareClientExchangeOrder` beside the protected market step, so a
confirmation shows the order sent), and the journaled send refuses one that
skipped it:

- **IBKR stops ignore the cap** (owner's decision, 2026-10-17): a stop must
  fill even through a gap, which a stop-limit would not. The positions UI
  sends every exit with the venue's slippage percentage, and a desktop
  shortcut with its profile's cap, but both leave the cap off an IBKR
  `stop_market`, `market_if_touched` or `trailing_stop` (`ibkrSlippageCapFor`),
  so a stop loss set or moved from the positions UI, the chart's TP/SL lines
  or a shortcut goes out as a plain `STP`. Settings › Trading, the ticket's
  TP/SL risk note and the shortcut editor say so next to the cap. A capped
  IBKR stop from any other path is refused, not sent with a cap. TP/SL
  attached to an entry are bridge-built `STP` legs and never carried one.
- A capped option `market` order (a position close from the positions UI or
  the feed command line) becomes an IOC `limit` priced from a quote of that
  contract, read for the order through
  `POST /v1/client-exchange/market-data/options/quote` (desktop:
  `fetchIbkrOptionQuote`): the bid less the cap, rounded up onto the
  option's tick, for a sell; the ask plus the cap, rounded down, for a buy.
  The quote must be usable: the underlying's option session is open (until
  16:15 ET for SPY, QQQ and IWM, the cash close otherwise; the response
  names the underlying), the data is live, and IBKR reports the contract
  tradable (`canTrade`: a positive bid and ask under a full entitlement and
  known tick rules). The quote's `quoteTimestampMs` is IBKR's last-trade
  time and is not an age check: the quote itself is read for the order. The
  bid/ask spread as a share of the mid must not be wider than the cap, since
  the touch itself would then already be further from the contract's value
  than the cap allows. Anything else is refused with the reason; the order
  never falls back to market. A runtime without the quote read refuses the
  order and names the update. The read is bounded at 15 seconds in the
  runtime, lock wait and retry included (the desktop sidecar waits 18, the
  web app 20), so a slow IB Gateway never keeps the order lock past the web
  app's deadline.
- A capped auction market order has no limit to bound and is refused.

### Attached TP/SL (bracket orders)

An order may carry `protection` with a take-profit and/or stop-loss child,
each with an absolute price and its own `sunk-<uuid>` `customerOrderId`. The
runtime places it as a TWS [bracket order](https://interactivebrokers.github.io/tws-api/bracket_order.html):

- the parent uses the IBKR-issued order ID and `transmit = false`;
- children use the next IDs, `parentId` = the parent ID, the opposite action,
  the parent's exact `totalQuantity`, `LMT` at the take profit or `STP`
  triggered at the stop loss, GTC, and their own `order_ref`;
- only the last child has `transmit = true`, which makes TWS transmit the whole
  bracket, so the parent can never work alone;
- the children share an OCA group (`oca-<parent customerOrderId>`, type 2,
  reduce with block): a fill of one reduces or cancels the other, so together
  they never close more than the parent opened. See the
  [OCA reference](https://interactivebrokers.github.io/tws-api/oca.html).

Child prices are moved onto the contract's price increment toward the entry
(never widening the requested risk). Anything a bracket cannot express fails
before submission with `unsupported IBKR protection`: a cash-sized or
fractional quantity, an IOC parent (a partial fill would leave full-size
children), a stop parent, reduce-only, missing or duplicated child references,
and levels that are inverted or already through a limit entry.

The response describes the parent as before and adds `protection` once the
parent is accepted: `attached` (filled parent, working children), `pending`
(children held until the parent fills), `rejected` (a child was rejected or
cancelled), or `unknown` (a child was not confirmed before the acknowledgement
deadline), with one entry per child in `legs`. A rejected or unknown child with
an accepted parent is a partial acceptance; the ticket warns that the position
may be unprotected. A rejected parent rejects the bracket; a parent without a
definitive state makes the whole bracket `indeterminate`. A leg whose stream
an informational code ended (a parent sent outside regular hours gets `399`),
or that IBKR cancelled (`202`), is read back from open and completed orders by
its own `order_ref` once every undecided leg's stream has ended, under the same
follow-up deadline.

### Linked exits (reduce-only)

IBKR has no reduce-only attribute for stock or option orders (`ibapi` 3.3.0's
`Order` has none; `openClose` is for institutional accounts only). Two exits
armed separately, such as a take profit beside a stop loss, a limit close, or
a market close beside resting TP/SL, are therefore independent orders: if one
fills, the other stays live and can open a reverse position. Bridge release
`0.4.0` (and the desktop app built from the same `Gateway`) closes this gap
with IBKR's one-cancels-all (OCA) groups.

IBKR semantics this relies on:

- Orders sharing an `ocaGroup` with `ocaType` 2 are linked: "remaining orders
  are proportionately reduced in size with block". IBKR reduces "the quantity
  of other orders in the group by the partially filled quantity", and "with
  block" gives overfill protection: "only one order in the group will be
  routed at a time". Sources: TWS API
  [One Cancels All](https://interactivebrokers.github.io/tws-api/oca.html) and
  IBKR Campus [OCA attribute lesson](https://www.interactivebrokers.com/campus/trading-lessons/using-the-one-cancels-another-oca-order-attribute-in-ibkrs-ibusopt/).
- "If an order in the group is canceled, the remaining orders are also
  canceled", and a transmitted order cannot be moved into a group. Source:
  IBKR [OCA order type](https://www.interactivebrokers.com/en/trading/orders/oca.php).
- The same page warns that cancellations are not guaranteed to reach the
  exchange before an order executes, so linking bounds the risk; it is not a
  venue-side reduce-only guarantee.
- IBKR does not document whether a group name links orders placed by
  different API clients, so the runtime never joins or re-places another
  client's orders.

An order with `reduceOnly: true` is a linked exit. It needs an explicit
`quantity` and no `protection`. Before submission the runtime reads the
account's position in the contract (`reqPositions`) and every working order
from every client (`reqAllOpenOrders`), serialized with other snapshot reads,
and then:

1. refuses the exit when there is no position or the order would add to it;
2. clamps the quantity to the position;
3. counts how much the position's exits could close together: orders in one
   OCA group with block count once at the group's largest order, every other
   order at its own size; bracket children whose parent is still working are
   not exits yet;
4. joins this client's linked group (or starts `oca-<customerOrderId>`) when
   that keeps the total within the position, and otherwise re-places every
   exit this client placed into a fresh group with the new one.

Re-placing never cancels first. Copies of the earlier exits (same side,
remaining quantity, type, prices, time in force, outside-RTH and `order_ref`)
are placed in the fresh group and confirmed working. Then the originals are
cancelled, one per old OCA group because IBKR cancels the rest with it, with
every cancellation sent before any answer is awaited, and read back from open
and completed orders. Only then is the new exit placed. An original and its
copy are therefore both live only while the cancellations travel, and a slow
or failed new exit never leaves a duplicate behind. The relink itself stands
even if IBKR then rejects the new exit. If a copy is not confirmed, the copies
are cancelled again and the earlier exits are left as they were. If a copy
fills at once (its price was already through), the position has changed: the
originals of placed copies are cancelled so none can fill again, any exit of
those groups that was not copied is named as cancelled with its group, and no
new exit is placed. If an original fills or cannot be confirmed cancelled,
the new exit is not placed either. The response says so and names the orders
to check. After any of these races the runtime re-reads the position and its
exits and warns when they could exceed it.

Re-placing is refused, with a reason, before anything is sent when:

- an exit is larger than the position;
- exits placed by TWS or another client leave no room for the group;
- an exit has an order type other than `LMT`, `STP` or `STP LMT`;
- an exit has a time in force other than DAY or GTC;
- an exit is linked with a non-exit order;
- more than four exits would need re-placing.

A refusal is an `invalid_request` that names the orders (at most eight, then a
count).

The response adds `exit` with:

- the OCA group;
- the quantity sent and the position quantity;
- the other exits in the group;
- the re-placed orders, original to copy;
- warnings.

Messages are bounded to 3,800 characters for the web journal.

Deadlines: preparation (connection, snapshots, order ids) shares the order's
14-second preparation deadline. The rest shares one 12-second budget: 3.5
seconds for copies, 3.5 seconds for cancelling originals, and the remainder
for the new exit, of which part is kept for reading back an exit IBKR holds
(399). With the one-second lock this finishes inside the 30-second caller
deadline. The work after preparation runs in its own task, which holds the
order lock, so a caller that stops waiting (a closed tab, an aborted request)
cannot interrupt it between placing copies and cancelling originals.

Cancelling an order (`POST /v1/client-exchange/orders/cancel` on the bridge,
`cancelClientExchangeOrder` on desktop) that shares an OCA group with exits of
a held position re-places those siblings into a fresh group first, then
cancels the order, which cancels its old group. Cancelling or moving a take
profit therefore keeps its stop loss. The same deadlines apply, and a loose
order is cancelled after a single snapshot read as before.

Siblings are cancelled with the order, as IBKR does, and the message names
them, when:

- they protect nothing yet, because they are children of a working bracket
  parent; or
- they would no longer close anything, because no position is held.

If the cancellation cannot be confirmed, the response is `indeterminate`.
The web terminal then blocks further orders on that connection until the user
has checked IBKR open orders, as for any indeterminate cancellation.

Account-state open orders report `ocaGroup` when an order belongs to a group
with overfill protection (types 1 and 2), and `parentId` and
`filledQuantity` when they apply. The web terminal counts a position's
working exits by remaining quantity, leaving out children of a working
bracket parent. It marks the position:

- `OCA` when all its exits share one such group within the position;
- `Exits exceed position` when they could together close more than the
  position.

Linked chart lines are labelled `Take profit · OCA`. Dragging a linked
exit on the chart cancels it (keeping its siblings, above) and places the
new one. A warning from that cancellation is shown with the new exit's
result.

Release order: the web app that accepts `exit` and the account-state
`ocaGroup`, `parentId`, `filledQuantity` and `serverLink` (strict schemas)
and handles older runtimes deploys first, then bridge `0.4.0` and the
desktop app are released. Every runtime before them refuses `reduceOnly` with
`invalid_request` "IBKR does not support reduce-only or post-only orders"
before any Gateway access (the desktop app surfaces the same text). For an
exit sized from the position (position close, limit close, chart TP/SL) the
web app then places the earlier unlinked order and warns that exits are not
linked and which update links them; a reduce-only ticket order, which the user
sized, is refused instead.

An indeterminate outcome retains the `customerOrderId` and blocks later orders
for that account in the current process. While that block exists, a new order
for the account is not sent: the response is `indeterminate` and carries the
earlier unresolved `customerOrderId`, not the new one. The order is never
automatically resubmitted. See IBKR's [order submission and callback contract](https://interactivebrokers.github.io/tws-api/order_submission.html).

## Durable order intents and reconciliation

The runtime block is process-local. The durable block lives in the renderer
(`apps/web/src/lib/ibkr-order-intents.ts`), in browser-local storage for the
hosted web app and the Electron renderer alike. Each intent has its own key
(`sunk.ibkr-order-intent.v1:<intentId>`), so tabs that write concurrently
cannot overwrite each other's intents:

1. Before an order leaves the renderer, an intent is persisted with its
   `customerOrderId`, account, connection, instrument, side, size, prices, and
   `status: submitting`. If storage exists but cannot be written, the order is
   not sent.
2. The response moves the intent to `acknowledged`, `rejected`, or
   `indeterminate`. Failures that provably precede submission become
   `not_sent`; a lost response becomes `indeterminate`. An intent still
   `submitting` after a restart is treated as unresolved.
3. `submitting` and `indeterminate` intents block every IBKR order for the
   same account, in every tab and after renderer, desktop, or bridge restarts.
   The execution ticket shows the reason, including the exact reference.
4. Unresolved intents reconcile on startup (while the IBKR connection is
   active) and before every new order for the account, through the same local
   runtime: `POST /v1/client-exchange/orders/reconcile` on the bridge or the
   `reconcileIbkrOrder` sidecar method on desktop. The runtime searches
   `reqAllOpenOrders` (open orders from every API client and TWS), then
   `reqCompletedOrders` (today's completed orders), then `reqExecutions` for
   the account (today's executions), and matches the exact `order_ref`. These
   are the TWS socket equivalents of the Client Portal's forced
   `/iserver/account/orders` refresh and trades endpoints.
5. Only an exact reference match for the same account resolves an intent and
   clears the runtime's matching block. `not_found`, a mismatched reference,
   or a failed read keeps the block and retries with exponential backoff
   (5 seconds doubling to 5 minutes). Orders older than the provider's
   current-day completed-order and execution windows therefore stay blocked
   until the operator clears them.
6. The escape hatch is an explicit operator clear ("I checked IBKR"). It is
   logged with the reference, marks the intent `released`, and asks the
   runtime to drop its block for that exact reference (`release: true`). If the
   runtime cannot be reached, the release is retried before the next order;
   if the runtime still answers with the earlier reference, the renderer
   adopts it as a new unresolved intent.

Bracket children are journaled with their parent before sending, as their own
intents (`role: take_profit | stop_loss`, `parentIntentId`), with the child
references sent in `protection`. They follow the parent: `not_sent` and
`indeterminate` parents make every child the same. For an accepted parent, a
working child becomes `acknowledged`, a rejected child `rejected`, and an
unreported child `indeterminate`, so it blocks the account and reconciles by its
own reference exactly like an order. Shells without order references cannot
carry TP/SL; the renderer refuses such an order before journaling it.

Desktop shells older than this contract do not expose `reconcileIbkrOrder`
and reject a renderer-supplied `customerOrderId`. For them the renderer still
persists the intent before sending, adopts the shell-generated reference from
the response, and waits for an operator clear instead of reconciling.

## Browser bridge protocol

The browser-facing adapter binds to `127.0.0.1:17654` by default and exposes
only these version 3 operations:

- `POST /v1/client-exchange/detect`
- `POST /v1/client-exchange/probe`
- `POST /v1/client-exchange/market-data/candles`
- `POST /v1/client-exchange/market-data/options/chain`
- `POST /v1/client-exchange/market-data/options/quote` — one listed option
  by `conid`, quoted for the request, with its `underlyingSymbol`, the
  chain's fields and tradability rule, and its tick rules even when it is
  not tradable; a bridge without it answers `not_found`
- `POST /v1/client-exchange/orders`
- `POST /v1/client-exchange/orders/reconcile`
- `GET /v1/client-exchange/stream` — WebSocket account-value, live-candle,
  stock-quote and order-event subscriptions.

There is no generic proxy route. Every request is authenticated with an exact
origin allowlist, a versioned HMAC signature, a short timestamp window, and a
single-use nonce. HTTP requests carry the signature in headers. Browser
WebSocket handshakes carry equivalent non-secret signature fields in subprotocol
values because the WebSocket constructor cannot set custom headers; the shared
token is never placed in the URL or handshake. The server also enforces loopback
`Host`, a 64 KiB body/message limit, and origin-scoped Private Network Access
preflight responses.

The first WebSocket frame selects one typed operation. Account selection is
still restricted to the managed accounts returned by the connected Gateway:
one is selected automatically, while several require an explicit account id.
Every accepted HTTP or WebSocket operation calls the shared `Gateway` facade,
which in turn uses `ibapi` against the IB Gateway TWS socket. Unsupported or
arbitrary TWS methods cannot be forwarded through the bridge.

A peer on another protocol version is refused with `unsupported_version` before
any Gateway access, and the web app tells the user to update an older bridge.
See the bridge README for the version history.

The `stock_quote` stream is an additive v3 operation: bridge release `0.3.0` is
the first to serve it. The published `0.2.0` release speaks v3 but rejects a
quote subscription with `invalid_request` before its `ready` acknowledgement.
The web app treats exactly that as an outdated bridge, reports
`bridge_update_required` naming `0.3.0`, links the bridge releases, and does
not retry; every other v3 operation keeps working on `0.2.0`.

The `order_events` stream is a second additive v3 operation: bridge release
`0.4.0` is the first to serve it. Every release before it (`0.2.0`, `0.3.0`)
rejects the subscription with `invalid_request` before `ready` (serde's
"unknown variant `order_events`"). The web app reports
`bridge_update_required` naming `0.4.0` and switches IBKR take-profit and
stop-loss pushes off for that account without retrying; quotes, balances,
candles, options and orders keep working. See
[Order events](#order-events-and-tpsl-fill-push).

Protocol v3 fixtures are shared by Rust and browser tests. They prove contract
serialization and request authentication, not a live IBKR session.

## Order events and TP/SL fill push

IBKR executes attached TP/SL itself (see
[Attached TP/SL](#attached-tpsl-bracket-orders)), so the terminal only learns
that a child filled from IB Gateway. The `order_events` stream (browser bridge
WebSocket operation; `subscribeIbkrOrderEvents` on the desktop sidecar)
forwards one account's order activity:

- **One provider subscription per TWS client.** TWS allows one order-update
  subscription per API client, so the `Gateway` keeps one, started by the
  first consumer and shared through a bounded broadcast channel by every
  browser tab or desktop window of that process. A changed connection
  replaces it. Setup never takes the order mutex or resets the connection.
- **What it sees.** TWS reports order status and executions to the API client
  that placed the order, so the browser bridge (client `17654`) sees orders
  placed through the bridge and the desktop sidecar (client `17655`) those
  placed from the desktop app, including their bracket children. Orders
  entered in TWS or by another API client are reported live only when IBKR
  sends them to this client; a snapshot read on the same connection
  (reconciliation, the replay's executions read) may surface them too, and
  they are forwarded like any other order of the account.
- **Events.** `status` events carry the account, TWS order id, permanent
  order id, parent id, OCA group, client id, order reference, contract (conid, symbol,
  local symbol, security type, currency), side, order type, canonical TWS
  status, filled and remaining quantity and average and last fill price. A
  status is sent only when the status, quantities or average price change,
  so the repeated statuses of each open-orders read are not forwarded.
  Details come from the order's `openOrder` or completed-order message; a
  status that arrives first waits for them, and an execution that arrives
  first is sent again once they arrive (matched by TWS order id or permanent
  order id) with its parent, type, OCA group and reference filled in. `execution` events carry the same identity plus the
  execution id, IBKR execution time, quantity, price, cumulative quantity,
  average price and liquidation flag. Each execution is sent once, and again
  when its commission report adds the commission and realized PnL, so
  consumers upsert by `execId`. Partial fills are separate executions of the
  same order. The execution time is parsed from TWS's
  `yyyyMMdd HH:mm:ss <zone>` (IANA, or the Windows and localized names
  `ibapi` maps for the session zone), the session zone, or the UTC
  `yyyyMMdd-HH:mm:ss` form; an unknown zone yields no time rather than a
  guessed one, and the bridge logs a warning because such a fill cannot be
  pushed.
- **OCA group.** From bridge `0.6.0` (`IBKR_RUNTIME_RELEASE` for the
  desktop sidecar) both event kinds carry `ocaGroup`: the order's IBKR OCA
  group when it has overfill protection (OCA types 1 and 2, as on
  account-state orders) and fits 512 characters, otherwise `null`. The key
  is always sent; older bridges omit it, and the renderer schema accepts
  both. The bridge gives Sunk's bracket children and linked exits a group
  named `oca-sunk-<uuid>` (see [Linked exits](#linked-exits-reduce-only));
  bracket parents are never in it.
- **Bounds.** Every event fits the strict renderer schema
  (`@sunk/desktop-contract`): out-of-range quantities, prices, permanent ids,
  references or local symbols are cleared, and an execution or order whose
  identity cannot fit (an oversized symbol, account, currency or type, a
  quantity or price above 1e9) is dropped. Consumers also skip one
  out-of-contract order event with a warning instead of closing the bridge
  socket or restarting the desktop sidecar.
- **Replay.** Each subscription starts with the account's latest order
  statuses and recent executions. When the shared subscription starts it
  reads this client's open orders, today's completed orders and today's
  executions once, so a consumer that reconnects (a reload, a bridge or
  sidecar restart, a new Gateway connection) learns about fills it missed,
  with their bracket parent and type. Caches are bounded
  (1,024 orders, 2,048 executions, 256 early commission reports); a consumer
  that falls more than 1,024 events behind is told to resubscribe.
- **Recovery.** Losing IB Gateway or the provider subscription is reported as
  `gateway_unavailable` and the stream resubscribes with the usual bounded
  backoff. TWS warnings on the subscription, such as `399` (an order held
  until the market opens), are notices and never end the stream. An
  ambiguous or unknown account ends it. A subscription that starts while
  the shared one is ending is told so at once and resubscribes.

The web app (`apps/web/src/lib/ibkr-stop-fills.ts`) names an execution a stop
fill when its order reference is a bracket leg it journaled (role
`take_profit` or `stop_loss`), or IBKR reports a parent order and the leg's
type names it (`LMT` take-profit, `STP`/`STP LMT` stop-loss, `TRAIL` trailing
stop), or it has no parent but sits in an `oca-sunk-<uuid>` group as an
`STP`/`STP LMT` (a stop loss set on a position, or a bracket stop re-placed
into a linked group, whose copy has no parent), or IBKR executed it as a
liquidation. Entry (parent) fills and plain orders are never stops. A
parentless `LMT` in a Sunk group is not named unless this browser journaled
it as a bracket take-profit: a take profit set on a position and a manual
limit close are the same order, and the other venues leave plain reduce-only
limits out for the same reason. It relays the fill through `push.reportStopFill`
(venue `ibkr`) with the shared stop-fill detector: an order's executions,
partial fills included, are grouped and reported once, keyed by the IBKR
permanent order id, and the backend claims each fill once per user, so
several tabs and devices push it once. Stock fills are named by ticker,
option fills by their OCC symbol; realized PnL is reported only when IBKR
reports it in USD; a share class is written with a dot (`BRK B` as
`BRK.B`). Like the other personal venues, this needs an open terminal
connected to the account; a browser without the bridge token saved does not
open the stream at all.

What a user's runtime supports is gated like the order set: on the bridge's
`X-Sunk-Bridge-Release` header (recorded from any HTTP reply, such as the
navbar's account-state read) or the desktop shell's bundled
`ibkrRuntimeRelease` (`apps/web/src/lib/ibkr-stop-fill-status.ts`). The push
settings show a notice under "My position stops" for each saved IBKR
account whose fills do not all push:

| Runtime | What pushes | Notice |
| --- | --- | --- |
| Bridge before `0.4.0` (answers `order_events` with `invalid_request` before `ready`), desktop app without the native stream | Nothing | Off until the user updates the bridge (to `0.6.0`, with a releases link) or the desktop app |
| Bridge `0.4.0`–`0.5.x` (no header, or a header below `0.6.0`), desktop runtime below `0.6.0` | Bracket TP/SL and liquidations | Stop losses set on a position push once the user updates |
| `0.6.0` and newer | Bracket TP/SL, position stop losses, liquidations | None |

When the page later records a release that serves the stream (`0.5.0` or
newer) for a bridge that refused it, the account's stream restarts, so
installing the update needs no reload; only a newly recorded release does
this, so a bridge that keeps refusing the stream is never retried in a loop.
While the bridge's release is not known yet, an order event without the
`ocaGroup` key marks a runtime before `0.6.0`. A stream that ended for
another reason (no saved bridge token, an ambiguous or unknown account)
shows its own message. A reconnecting stream shows
nothing; the IBKR connection status covers it.

## Verification boundary

Automated tests cover configuration safety, account-selection behavior, HTTP
and WebSocket authentication/replay rejection, strict stream parsing and
reconnect cleanup, protocol serialization, bounded reads, contract
normalization, market-rule validation, order construction, order status
classification, order-event normalization (status deduplication,
commission merging, execution-time parsing, replay, warning notices)
from constructed TWS messages, and linked-exit planning plus the order of
place and cancel requests (copy, then exit, then original; copies cancelled
again on a rejection) against a scripted loopback TWS socket. Linked exits
have not been exercised against a live or paper IB Gateway: the OCA
behaviour above is IBKR's documented behaviour, not observed evidence. A read-only smoke may prove that the user's running IB Gateway
is discovered, returns managed accounts, and emits account/candle updates.

Never use an automated smoke to place an order. Before enabling execution for a
new environment, use a paper account and manually verify entitlements, contract
selection, order warnings, accepted/resting/filled transitions, cancellations,
and indeterminate reconciliation.
