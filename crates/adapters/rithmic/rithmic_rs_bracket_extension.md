# `rithmic-rs` Bracket Extension Decision

This note captures the final adapter-side decision for richer native Rithmic bracket support
beyond the current `RithmicBracketOrder` convenience API.

## Decision

- Keep extending the forked `BurnOutTrader/rithmic-rs` dependency.
- Do not revive or expand the vendored in-tree copy under `vendor/`.
- Do not add a second internal `rithmic-rs` implementation in this repository.

## Why

The current fork already owns the wire encoding path used by the adapter:

- typed entry point: `src/api/rithmic_command_types.rs`
- protobuf encoding: `src/api/sender_api.rs`
- raw protobuf model: `src/rti.rs`

The adapter should stay focused on Nautilus integration behavior, not duplicate
venue-protocol maintenance. The right customization point is the fork that
already serializes `RequestBracketOrder`.

## Current Gap

The typed `RithmicBracketOrder` wrapper is intentionally narrow:

- `action`
- `duration`
- `exchange`
- `localid`
- `price_type`
- `price`
- `profit_ticks`
- `quantity`
- `stop_ticks`
- `symbol`

The current sender path hard-codes a static target+stop bracket and only emits:

- account identity
- trade route
- entry side / type / duration
- quantity
- `target_quantity`
- `stop_quantity`
- `target_ticks`
- `stop_ticks`
- `price` for non-market entries
- `user_tag`

That is enough for the adapter's current native bracket subset, but it leaves a
lot of the actual Rithmic request surface unused.

## Raw `RequestBracketOrder` Surface

The raw protobuf in the fork already exposes materially richer fields:

### Entry controls

- `price_type`
  - `LIMIT`
  - `MARKET`
  - `STOP_LIMIT`
  - `STOP_MARKET`
  - `MARKET_IF_TOUCHED`
  - `LIMIT_IF_TOUCHED`
- `price`
- `trigger_price`

### Conditional / if-touched controls

- `if_touched_symbol`
- `if_touched_exchange`
- `if_touched_condition`
- `if_touched_price_field`
- `if_touched_price`

### Exit management controls

- `break_even_ticks`
- `break_even_trigger_ticks`
- `trailing_stop_trigger_ticks`
- `trailing_stop_by_last_trade_price`
- `target_market_order_if_touched`
- `stop_market_on_reject`

### Time-based lifecycle controls

- `release_at_ssboe`
- `release_at_usecs`
- `cancel_at_ssboe`
- `cancel_at_usecs`
- `cancel_after_secs`
- `target_market_at_ssboe`
- `target_market_at_usecs`
- `stop_market_at_ssboe`
- `stop_market_at_usecs`
- `target_market_order_after_secs`

## What This Can Improve

Extending the fork around the raw request can unlock native helper support for:

- stop-entry brackets
- stop-limit entry brackets
- market-if-touched / limit-if-touched entry brackets
- cross-instrument or alternate-price-field `if_touched` triggers
- venue-native break-even and trailing-stop behavior
- timed release / cancel bracket workflows

These are all real venue features already visible in the protobuf surface.

## What This Does Not Solve

The raw request still models target/stop exits as tick distances, not absolute
child prices. That means it does **not** give the adapter a lossless mapping for
Nautilus high-level market-entry brackets where the child orders are expressed
as absolute prices before the parent fill price is known.

That is the key reason the current adapter still rejects high-level
market-entry brackets:

- Nautilus high-level bracket input gives absolute child prices.
- Native Rithmic bracket exits are relative ticks.
- For a market parent, the actual entry fill price is unknown at submission
  time.

Any adapter-side conversion would be an approximation based on a quote snapshot,
not a faithful translation.

## Recommended Fork API Shape

Add a new richer typed request in the fork instead of overloading
`RithmicBracketOrder` directly.

Suggested direction:

```rust
pub struct RithmicBracketRequest {
    pub entry: RithmicBracketEntry,
    pub exits: RithmicBracketExitPlan,
    pub trigger: Option<RithmicBracketTrigger>,
    pub lifecycle: Option<RithmicBracketLifecycle>,
    pub localid: String,
}

pub struct RithmicBracketEntry {
    pub symbol: String,
    pub exchange: String,
    pub action: request_bracket_order::TransactionType,
    pub quantity: i32,
    pub duration: request_bracket_order::Duration,
    pub price_type: request_bracket_order::PriceType,
    pub price: Option<f64>,
    pub trigger_price: Option<f64>,
}

pub struct RithmicBracketExitPlan {
    pub bracket_type: request_bracket_order::BracketType,
    pub target_quantity: Vec<i32>,
    pub target_ticks: Vec<i32>,
    pub stop_quantity: Vec<i32>,
    pub stop_ticks: Vec<i32>,
    pub break_even_ticks: Option<i32>,
    pub break_even_trigger_ticks: Option<i32>,
    pub trailing_stop_trigger_ticks: Option<i32>,
    pub trailing_stop_by_last_trade_price: Option<bool>,
    pub target_market_order_if_touched: Option<bool>,
    pub stop_market_on_reject: Option<bool>,
}
```

Then:

- keep `RithmicBracketOrder` as a backward-compatible convenience wrapper
- implement `From<RithmicBracketOrder> for RithmicBracketRequest`
- add `request_bracket_order_request(...)` and
  `place_bracket_order_request_for_account(...)`

## Adapter Implications

Once the fork exposes this richer API, the Nautilus adapter can safely expand:

- raw helper coverage first
- then adapter-local high-level translation only for shapes with deterministic
  semantics

The adapter should **not** claim support for high-level market-entry bracket
lists unless it either:

- gains a venue-native API that accepts absolute child prices, or
- intentionally adopts a documented approximation policy

The current recommendation is to avoid that approximation.

## Minimum Fork Test Coverage

The fork should add encode/decode tests for:

- market bracket entry omits `price`
- stop / stop-limit bracket entry carries `trigger_price`
- `MARKET_IF_TOUCHED` and `LIMIT_IF_TOUCHED` encode correctly
- `if_touched_*` fields encode correctly
- break-even / trailing fields encode correctly
- timed release / cancel fields encode correctly
- account override paths still honor explicit `FCM` / `IB` / account identity

## Beta Release Readout

For this repository, the adapter-local beta hardening work is complete after:

- reconnect and reconciliation hardening
- same-login shared-gateway identity fixes
- high-level OCO and limit-entry bracket routing
- restart-safe native bracket child-ID persistence
- explicit documentation of remaining market-entry and broader live-routing
  boundaries

Remaining work now falls into two categories:

- upstream/fork enhancement in `BurnOutTrader/rithmic-rs`
- broader Nautilus live-routing changes outside this adapter crate

For the current stable beta / production pilot run, this means:

- ship the adapter with the current OCO + limit-entry native helper subset
- keep high-level market-entry brackets explicitly unsupported
- treat any richer raw `RequestBracketOrder` work as a future fork enhancement, not a blocker for
  this adapter release
