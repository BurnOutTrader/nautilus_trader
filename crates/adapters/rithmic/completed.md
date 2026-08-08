# Rithmic Adapter — Completed Work

Guide: [docs/integrations/rithmic.md](../../../docs/integrations/rithmic.md)

Items moved from [devplan.md](devplan.md) after completion.

---

## Beta Gates Completed

The final beta gates that previously lived in `devplan.md` are complete:

1. Forced logout or plant disconnect does not require a manual process restart. ✅
2. Reconnect restores all currently implemented live data subscriptions and re-runs execution
   reconciliation automatically. ✅
3. Rust v2 execution clients emit real engine events, account state, and report-generation output,
   rather than relying on default no-op trait behavior. ✅
4. Python wrapper and Rust v2 capability docs match reality exactly, with no hidden soft-fail
   surfaces. ✅
5. Connect / disconnect / reconnect cycles do not leave stale gateway handles, orphan tasks, or
   duplicate sessions behind. ✅

Historical "Residual follow-up" notes preserved below are implementation history only; they do not
represent active adapter-local beta blockers after the finalized decision record above.

As of 2026-04-13, no additional adapter-local feature work is planned beyond routine maintenance,
regression fixes, and compatibility updates.

### 100. 2026-04-03 completion pass: finalized Rithmic beta decision record ✅

**Files:** `crates/adapters/rithmic/devplan.md`,
`crates/adapters/rithmic/multi_login_plan.md`,
`crates/adapters/rithmic/rithmic_rs_bracket_extension.md`,
`crates/adapters/rithmic/completed.md`

Closed the remaining planning ambiguity around whether the Rithmic adapter still had open
adapter-local beta blockers or unresolved design questions for the current production beta handoff.

**Delivered:**

- converted `devplan.md` from a mixed implementation backlog into an explicit beta decision record
  stating that adapter-local hardening is complete and documenting the shipped support contract
- replaced the older multi-login implementation roadmap with the actual supported production
  routing contract for:
  - multiple accounts under one login/system
  - multiple Rithmic systems/logins in one Nautilus node
- tightened the bracket extension note so the fork strategy and the explicit non-support decision
  for high-level market-entry brackets are unambiguous for the beta release
- kept the external boundaries explicit:
  - broader same-venue live-routing work remains outside this adapter branch
  - richer raw `RequestBracketOrder` helpers remain a future fork enhancement, not a beta blocker

---

## P0 — Correctness / Integration Blockers

### 0zy. 2026-04-03 completion pass: bracket-extension design stays in the forked `rithmic-rs` dependency ✅

**Files:** `crates/adapters/rithmic/rithmic_rs_bracket_extension.md`,
`crates/adapters/rithmic/devplan.md`,
`crates/adapters/rithmic/completed.md`

Closed the adapter-side planning question around how to pursue richer native

Rithmic bracket support beyond the current helper API.

**Delivered:**

- documented the decision to keep extending the forked
  `BurnOutTrader/rithmic-rs` dependency rather than reviving a second internal
  `rithmic-rs` implementation in this repository
- captured the currently unused raw `RequestBracketOrder` capabilities already
  present in the forked protobuf model, including triggered entry, `if_touched`,
  break-even, trailing, and timed lifecycle fields
- documented the key semantic boundary clearly:
  raw `RequestBracketOrder` can improve native conditional bracket support, but
  it still does not provide a lossless adapter translation for Nautilus
  high-level market-entry brackets expressed with absolute child prices
- recorded the recommended next fork API shape and minimum encoding test
  coverage so the remaining work is concrete rather than exploratory

- reduced the remaining Rithmic beta-release blockers in this repository to the
  broader live-routing issue outside the adapter crate

### 0zz. 2026-04-14 completion pass: reject unsupported native bracket state paths explicitly ✅

**Files:** `crates/adapters/rithmic/src/python/execution.rs`,
`nautilus_trader/adapters/rithmic/config.py`,
`nautilus_trader/adapters/rithmic/execution.py`,
`tests/integration_tests/adapters/rithmic/test_execution_client.py`,
`examples/live/rithmic/rithmic_conformance_keepalive.py`,
`docs/integrations/rithmic.md`,
`crates/adapters/rithmic/devplan.md`,
`crates/adapters/rithmic/completed.md`

Closed the remaining adapter-local documentation/runtime mismatch where
`native_bracket_state_path` was still presented as active behavior on the PyO3
execution surface even though that path was not implemented end to end.

**Delivered:**

- kept `native_bracket_state_path` in config as an explicit reserved field for a
  future persisted-native-bracket implementation
- made the raw PyO3 `RithmicExecutionClient` constructor fail fast with a clear
  error when `native_bracket_state_path` is supplied
- made the thin Python execution wrapper fail fast with the same unsupported
  contract instead of silently carrying a placeholder setting
- updated the integration guide and examples so the supported execution contract
  matches the actual code path
- kept the remaining boundary explicit: high-level market-entry brackets still
  remain outside the current adapter-local native bracket translation subset

**Validation / notes:**

- `cargo test -p rithmic-nt --features python`
- `uv run --active --no-sync pytest tests/integration_tests/adapters/rithmic/test_execution_client.py -q`

### 0z. 2026-04-03 completion pass: shared gateway identity includes `FCM` / `IB` login context ✅

**Files:** `crates/adapters/rithmic/src/shared_gateway.rs`,

`crates/adapters/rithmic/multi_login_plan.md`,
`crates/adapters/rithmic/devplan.md`,
`crates/adapters/rithmic/completed.md`

Closed a correctness gap in the adapter-local shared-gateway registry used by
same-login multi-account Rithmic clients.

**Delivered:**

- added `fcm_id` and `ib_id` to the live shared-gateway registry key so the
  adapter no longer merges distinct upstream login contexts that should not
  share one Rithmic gateway session
- kept `account_id` excluded from that key so multiple execution accounts can

  still share a gateway under the same login identity
- added focused regression coverage proving:
  - history enablement still does not split the key

  - account ID still does not split the key
  - differing `FCM` / `IB` values do split the key and prevent gateway reuse
- updated the multi-login design notes so the documented key shape now matches
  the merged-plant shared-gateway behavior in code

**Validation / notes:**

- `cargo test -p rithmic-nt --lib`

### 0y. 2026-04-03 completion pass: supported high-level live `SubmitOrderList` routing for native OCO and limit-entry brackets ✅

**Files:** `crates/adapters/rithmic/src/execution/client.rs`,
`crates/adapters/rithmic/src/execution/live.rs`,
`crates/adapters/rithmic/src/execution.rs`,
`crates/adapters/rithmic/src/python/execution.rs`,
`nautilus_trader/adapters/rithmic/execution.py`,

`nautilus_trader/adapters/rithmic/_rithmic.pyi`,
`tests/integration_tests/adapters/rithmic/test_execution_wrapper.py`,
`docs/integrations/rithmic.md`,
`crates/adapters/rithmic/devplan.md`,
`crates/adapters/rithmic/completed.md`

Closed the remaining adapter-local gap where both the Rust v2 `LiveNode`
path and the Python `TradingNode` wrapper still denied or no-op'd
high-level `SubmitOrderList` flows entirely.

**Delivered:**

- added adapter-local native order-list routing for the supported live shapes:
  two-leg OCO lists and three-order limit-entry brackets
- accepted both `OCO` and `OUO` child contingencies for high-level bracket

  children to match the current Nautilus order-factory outputs across Python
  and Rust
- added a new PyO3 binding method for bracket-list submission and tracked the
  parent/child client-order mapping so native Rithmic bracket child events are
  remapped back onto the original Nautilus stop/target child order IDs
- updated the Rust v2 execution event loop to normalize native bracket child

  notifications before local order-state application and downstream emission
- kept unsupported high-level shapes explicit rather than silent:
  market-entry brackets, triggered/post-only/quote-quantity/trailing
  variations, and other non-native list topologies now reject clearly

**Validation / notes:**

- `cargo check -p rithmic-nt --features python --lib`
- `cargo test -p rithmic-nt --lib`
- `cargo test -p rithmic-nt --features python --lib`
- `uv run pytest -q tests/integration_tests/adapters/rithmic`

### 0x. 2026-04-03 completion pass: Python import/front-month parity and high-level backtest helpers ✅

**Files:** `nautilus_trader/adapters/rithmic/custom.py`,

`nautilus_trader/adapters/rithmic/providers.py`,
`nautilus_trader/adapters/rithmic/backtest.py`,
`nautilus_trader/adapters/rithmic/__init__.py`,
`examples/backtest/rithmic/rithmic_download_bars.py`,
`examples/backtest/rithmic/rithmic_backtest_high_level.py`,
`tests/integration_tests/adapters/rithmic/test_custom.py`,
`tests/integration_tests/adapters/rithmic/test_backtest.py`,
`tests/integration_tests/adapters/rithmic/test_providers.py`,
`docs/integrations/rithmic.md`

Closed the remaining Python adapter/runtime mismatches blocking a clean
high-level Rithmic operator workflow.

**Delivered:**

- fixed the Python custom-data import path by restoring eager `InstrumentId`

  annotations for the Rithmic custom dataclasses, allowing schema generation
  and adapter import to succeed again under the current custom-data registry
- added `RithmicInstrumentProvider.load_front_month_async(...)` on the Python
  wrapper so the documented/provider-level live workflow matches the public API
- added reusable Rithmic helper functions plus script examples for:
  `BacktestNode.setup_download_engine(...)` historical bar ingestion into a
  `ParquetDataCatalog` and a matching high-level `BacktestNode` EMA run
- updated the integration guide so the common Nautilus high-level backtest
  path now points to concrete Rithmic scripts rather than only the notebook

**Validation / notes:**

- added focused Python regression coverage for custom-data schema import,
  provider front-month loading, and catalog/backtest helper behavior

### 0w. 2026-04-03 completion pass: adapter-only explicit `client_id` routing contract for same-venue multi-account Rithmic ✅

**Files:** `crates/adapters/rithmic/src/factories.rs`,
`docs/integrations/rithmic.md`,
`crates/adapters/rithmic/devplan.md`,

`crates/adapters/rithmic/completed.md`

Closed the remaining adapter-local ambiguity around running multiple Rithmic

execution clients on the same `RITHMIC` venue without touching Nautilus core.

**Delivered:**

- added adapter-side factory coverage proving two same-venue Rithmic execution
  clients for the same `system_name` receive distinct execution `client_id`
  and adapter `account_id` values while keeping venue `RITHMIC`
- documented the supported operator contract explicitly:
  same-venue multi-account routing works only through explicit `client_id`
  targeting, not implicit venue-only inference

- documented the `TradingNode` naming convention for this adapter-only path:
  use distinct client config keys without hyphens and derive routing IDs with
  `get_rithmic_exec_client_id(...)`

**Validation / notes:**

- `cargo test -p rithmic-nt test_factory_supports_multiple_exec_clients_for_same_system_name --lib`

### 0v. 2026-04-03 completion pass: consume merged account-scoped `rithmic-rs` APIs in the adapter ✅

**Files:** `Cargo.lock`,
`crates/adapters/rithmic/src/python/execution.rs`,
`crates/adapters/rithmic/devplan.md`,
`crates/adapters/rithmic/completed.md`

Completed the adapter-local follow-up required after the merged
`BurnOutTrader/rithmic-rs` account-scoped order / PnL API work landed on
`main`.

**Delivered:**

- refreshed the workspace lockfile so `nautilus-trader` now resolves
  `rithmic-rs` from `BurnOutTrader/rithmic-rs` `main` at commit `096433f3`
- switched the Python execution wrapper request paths from gateway-global calls
  to explicit account-scoped calls for:
  `place_order`, `modify_order`, `cancel_order`, `cancel_all_orders`,
  `show_orders`, `replay_executions`, native `bracket` / `OCO` submission, and
  native `show_brackets` / `show_bracket_stops`

- updated Python local submission callback events to use the wrapper execution
  client's account ID rather than the shared gateway's stored default account
- added focused unit coverage proving the wrapper-side account helper preserves
  gateway `fcm_id` / `ib_id` while using the explicit execution wrapper
  `account_id`

**Validation / notes:**

- `cargo check -p rithmic-nt --features python --lib`
- `cargo test -p rithmic-nt --features python --lib`

### 0u. 2026-04-03 completion pass: Python TradingNode supports multiple same-adapter client instances ✅

**Files:** `nautilus_trader/live/node_builder.py`,
`tests/unit_tests/live/test_node_builder.py`,
`crates/adapters/rithmic/devplan.md`,
`crates/adapters/rithmic/completed.md`

Completed the cross-cutting follow-up needed for multi-account Rithmic
setups in the Python `TradingNode` path where config keys previously collided
by truncating after the adapter prefix.

**Delivered:**

- updated `TradingNodeBuilder` to use full config keys (for example
  `RITHMIC-APEX`, `RITHMIC-TOPSTEP`) as client instance names
- kept factory resolution by adapter prefix so existing factory registration
  patterns stay compatible
- added focused unit coverage proving two same-adapter instances build with
  distinct client names for both data and execution clients

**Validation / notes:**

- `uv run pytest -q tests/unit_tests/live/test_node_builder.py`
  (`2 passed`)

### 0t. 2026-04-03 completion pass: Rust/PyO3 dotenv helper for `RITHMIC_*` env bootstrapping ✅

**Files:** `crates/adapters/rithmic/src/config.rs`,
`crates/adapters/rithmic/src/python/config.rs`,
`nautilus_trader/adapters/rithmic/config.py`,
`nautilus_trader/adapters/rithmic/__init__.py`,
`examples/live/rithmic/bracket_submission.py`,
`examples/live/rithmic/oco_submission.py`,
`examples/live/rithmic/order_submission.py`,

`examples/live/rithmic/rithmic_conformance_keepalive.py`,
`examples/live/rithmic/rithmic_data_tester.py`,
`examples/live/rithmic/rithmic_ema_cross.py`,
`examples/live/rithmic/rithmic_ema_cross_two_accounts.py`,

`examples/live/rithmic/rithmic_exec_tester.py`,
`tests/integration_tests/adapters/rithmic/conftest.py`,
`crates/adapters/rithmic/devplan.md`,
`crates/adapters/rithmic/completed.md`

Added a Rust-native dotenv loader for Rithmic credentials and exposed it
through the PyO3 config wrappers so users can load `.env` values without
manually exporting every `RITHMIC_*` variable.

**Delivered:**

- added `load_rithmic_env_file(path: Option<&str>) -> Result<usize>` in
  `config.rs`
- loader only imports `RITHMIC_*` keys and preserves existing process env
  values instead of overwriting
- exposed `load_env_file(path=None)` as a static method on both
  `RithmicDataClientConfig` and `RithmicExecClientConfig` PyO3 wrappers

- added Python convenience wrapper `load_rithmic_env_file(...)` and exported it
  from `nautilus_trader.adapters.rithmic`
- updated all Rithmic live examples and live integration conftest to call the
  helper before reading env values

**Validation / notes:**

- added Rust unit coverage for dotenv loading behavior in
  `crates/adapters/rithmic/src/config.rs`

### 0s. 2026-04-03 completion pass: resolved `MarketDataEvent` large-enum layout ✅

**Files:** `crates/adapters/rithmic/src/data/client.rs`,
`crates/adapters/rithmic/src/gateway.rs`,
`crates/adapters/rithmic/src/data/live.rs`,
`crates/adapters/rithmic/src/python/events.rs`,
`crates/adapters/rithmic/devplan.md`,
`crates/adapters/rithmic/completed.md`

Evaluated the remaining structural `clippy` warning on `MarketDataEvent` and
implemented user-friendly/shared indirection for the largest payload variant.

**Decision and rationale:**

- chose indirection for `Depth10` via `Arc<OrderBookDepth10>` rather than
  keeping a large by-value variant
- this keeps event cloning/movement lighter across broadcast/event fan-out paths
  while preserving external behavior and Python-facing API outputs

**Delivered:**

- updated `MarketDataEvent::Depth10` to hold `Arc<OrderBookDepth10>`

- updated gateway event construction and downstream consumers in Rust and PyO3
- removed the final adapter-local `large_enum_variant` warning for
  `rithmic-nt`

**Validation / notes:**

- `cargo check -p rithmic-nt --features python --lib`
- `cargo test -p rithmic-nt --lib`
- `cargo clippy -p rithmic-nt --features python --lib -- -W clippy::all`
  (no warnings emitted for `rithmic-nt` in this pass)

### 0r. 2026-04-03 completion pass: helper-signature cleanup for remaining `too_many_arguments` warnings ✅

**Files:** `crates/adapters/rithmic/src/execution/live.rs`,
`crates/adapters/rithmic/src/python/data.rs`,
`crates/adapters/rithmic/src/python/execution.rs`,
`crates/adapters/rithmic/devplan.md`,

`crates/adapters/rithmic/completed.md`

Completed targeted helper signature refactors to remove the remaining
`too_many_arguments` warnings while preserving behavior and external APIs.

**Delivered:**

- wrapped execution event-loop dependencies into a single
  `ExecutionEventLoopContext` struct and updated runtime + test call sites
- replaced Python data helper multi-arg subscription toggles with a compact
  `ExtraMarketDataSubscriptionCmd` request struct

- replaced Python execution tracked-order helper multi-arg updates with a
  `TrackedOrderPatch` struct
- reduced `clippy` output for `rithmic-nt` to a single structural
  warning class (`large_enum_variant`)

**Validation / notes:**

- `cargo check -p rithmic-nt --features python --lib`
- `cargo test -p rithmic-nt --lib`
- `cargo clippy -p rithmic-nt --features python --lib -- -W clippy::all`

### 0q. 2026-04-03 completion pass: second clippy cleanup slice (warning reduction) ✅

**Files:** `crates/adapters/rithmic/src/data/client.rs`,
`crates/adapters/rithmic/src/data/live.rs`,
`crates/adapters/rithmic/src/execution/client.rs`,
`crates/adapters/rithmic/src/execution/live.rs`,
`crates/adapters/rithmic/src/python/data.rs`,

`crates/adapters/rithmic/devplan.md`,
`crates/adapters/rithmic/completed.md`

Applied a second behavior-preserving cleanup slice to continue Rust-quality
alignment and reduce adapter-local `clippy` noise in core data/execution paths.

**Delivered:**

- collapsed nested `if let` patterns in data subscription and instrument-load
  flows (`data/client.rs`, `data/live.rs`, `execution/client.rs`)

- simplified report-construction `Option` handling in execution live paths with
  `map_or(...)` / `map_or_else(...)` and removed a redundant clone
- removed unnecessary `Result` wrapping in `order_book_from_snapshot(...)` and
  updated all adapter-local call sites accordingly
- reduced `cargo clippy -p rithmic-nt --features python --lib -- -W clippy::all`
  warnings for `rithmic-nt` from the prior broad style set down to

  structural items (`large_enum_variant` and `too_many_arguments`)

**Validation / notes:**

- `cargo check -p rithmic-nt --features python --lib`
- `cargo test -p rithmic-nt --lib`
- `cargo clippy -p rithmic-nt --features python --lib -- -W clippy::all`

### 0p. 2026-04-03 completion pass: low-risk clippy optimization slice ✅

**Files:** `crates/adapters/rithmic/src/data/live.rs`,
`crates/adapters/rithmic/src/execution/live.rs`,
`crates/adapters/rithmic/src/factories.rs`,

`crates/adapters/rithmic/src/python/events.rs`,
`crates/adapters/rithmic/src/python/instruments.rs`,
`crates/adapters/rithmic/devplan.md`,
`crates/adapters/rithmic/completed.md`

Applied a first behavior-preserving cleanup slice from `clippy` findings to
improve Rust/PyO3 implementation quality while keeping adapter behavior and
Python API surface unchanged.

**Delivered:**

- removed redundant clones/copies on `Copy` types in live data/event wrappers
- simplified several `Option` chains with `map_or(...)` / `map_or_else(...)`
  equivalents where they were purely stylistic cleanups
- removed unnecessary `Result` wrapping in internal helpers where no error path

  existed
- tightened small control-flow patterns (direct returns and concise `if let`
  chains) in adapter-local code
- updated dev plan with explicit remaining clippy-style cleanup classes

**Validation / notes:**

- `cargo check -p rithmic-nt --features python --lib`
- `cargo test -p rithmic-nt --lib`

- `cargo clippy -p rithmic-nt --features python --lib -- -W clippy::all`
  now reports fewer warnings for this crate after the cleanup slice

### 0o. 2026-04-03 completion pass: final PyO3 `py_*` naming alignment slice ✅

**Files:** `crates/adapters/rithmic/src/python/events.rs`,
`crates/adapters/rithmic/src/python/config.rs`,

`crates/adapters/rithmic/src/python/execution.rs`,
`crates/adapters/rithmic/src/python/gateway.rs`,
`crates/adapters/rithmic/src/python/instruments.rs`,
`crates/adapters/rithmic/devplan.md`,

`crates/adapters/rithmic/completed.md`

Completed the final remaining adapter-local PyO3 naming follow-up by aligning
all Python-exposed `#[pymethods]` internals in the Rithmic Python bindings to
`py_*` while preserving the existing Python API names.

**Delivered:**

- aligned `python/events.rs` getters to explicit `#[getter(...)]` attributes

  with `py_*` internal method names and explicit `#[pyo3(name = ...)]` for
  Python methods
- added explicit `#[pyo3(name = ...)]` for Python-exposed methods and dunder
  methods where needed to pin the exported API surface, including remaining
  `__repr__` wrappers in `python/config.rs`, `python/execution.rs`,
  `python/gateway.rs`, and `python/instruments.rs`
- updated the adapter-local dev plan to remove the now-complete events-file
  naming follow-up

**Validation / notes:**

- `cargo check -p rithmic-nt --features python --lib`

- `cargo test -p rithmic-nt --lib`

### 0n. 2026-04-03 completion pass: third PyO3 `py_*` naming alignment slice ✅

**Files:** `crates/adapters/rithmic/src/python/data.rs`,
`crates/adapters/rithmic/devplan.md`,
`crates/adapters/rithmic/completed.md`

Completed the data-client portion of the PyO3 internal naming alignment by
moving Python-exposed `#[pymethods]` internals in `python/data.rs` to `py_*`

while preserving Python-visible API names.

**Delivered:**

- aligned `python/data.rs` Python-exposed method internals to `py_*`
- pinned exported Python names with `#[pyo3(name = ...)]` and explicit
  `#[getter(...)]` attributes where applicable
- updated adapter-local planning notes so only `python/events.rs` remains in

  this naming-alignment follow-up

**Validation / notes:**

- `cargo test -p rithmic-nt --lib`
- `cargo check -p rithmic-nt --features python --lib`

### 0m. 2026-04-03 completion pass: second PyO3 `py_*` naming alignment slice ✅

**Files:** `crates/adapters/rithmic/src/python/enums.rs`,
`crates/adapters/rithmic/src/python/execution.rs`,
`crates/adapters/rithmic/devplan.md`,
`crates/adapters/rithmic/completed.md`

Extended the PyO3 internal naming alignment to the execution-client wrapper

surface and enum wrappers while keeping all Python-visible names unchanged.

**Delivered:**

- aligned Python-exposed methods in `python/execution.rs` to `py_*` internals
  and pinned exported method names with `#[pyo3(name = ...)]`
- aligned enum `__repr__` methods in `python/enums.rs` to `py_*` internals with
  explicit `#[pyo3(name = "__repr__")]`
- updated adapter-local planning notes so remaining alignment scope is now only
  `python/data.rs` and `python/events.rs`

**Validation / notes:**

- `cargo test -p rithmic-nt --lib`
- `cargo check -p rithmic-nt --features python --lib`

### 0l. 2026-04-03 completion pass: first PyO3 `py_*` naming alignment slice ✅

**Files:** `crates/adapters/rithmic/src/python/config.rs`,
`crates/adapters/rithmic/src/python/gateway.rs`,
`crates/adapters/rithmic/devplan.md`,
`crates/adapters/rithmic/completed.md`

Started the PyO3 internal naming-alignment follow-up by converting the
configuration and gateway wrapper method internals to the Rust guide's `py_*`
pattern while preserving the externally visible Python API names.

**Delivered:**

- renamed Python-exposed internals in `python/config.rs` and `python/gateway.rs`
  to `py_*`
- kept Python-side method and property names stable using
  `#[pyo3(name = ...)]` and explicit `#[getter(...)]` attributes
- updated adapter-local planning notes so the remaining unaligned files are
  explicit for the next passes

**Validation / notes:**

- `cargo test -p rithmic-nt --lib`
- `cargo check -p rithmic-nt --features python --lib`

### 0k. 2026-04-03 completion pass: ahash hot-path tracking in PyO3 wrappers ✅

**Files:** `crates/adapters/rithmic/src/python/data.rs`,

`crates/adapters/rithmic/src/python/execution.rs`,
`crates/adapters/rithmic/src/python/gateway.rs`,
`crates/adapters/rithmic/devplan.md`,
`crates/adapters/rithmic/completed.md`

Aligned the Rithmic PyO3 wrapper hot-path tracking collections with the Rust
developer guide by moving the clearly event-loop-local maps and sets from the
standard hasher to `ahash`.

**Delivered:**

- switched Python data-client subscription and extra-feed tracking from std
  `HashMap` / `HashSet` to `AHashMap` / `AHashSet`
- switched Python execution-client tracked-order storage from std `HashMap` to

  `AHashMap`
- switched Python gateway balance and position snapshot caches from std
  `HashMap` to `AHashMap`
- updated the adapter-local dev plan to leave the new PyO3 naming audit as the
  remaining adapter-local follow-up discovered during this review

**Validation / notes:**

- `cargo test -p rithmic-nt --lib`
- `cargo check -p rithmic-nt --features python --lib`
- `cargo test -p rithmic-nt --all-features --lib` still aborts in this
  checkout during PyO3-linked test process startup with missing

  `_PyBaseObject_Type`, which appears to be a local Python linkage/runtime
  issue rather than a Rust compile failure inside the adapter

### 0j. 2026-04-03 completion pass: mocked shared-gateway multi-account execution coverage ✅

**Files:** `crates/adapters/rithmic/src/execution/live.rs`,
`crates/adapters/rithmic/src/shared_gateway.rs`,

`crates/adapters/rithmic/devplan.md`,
`crates/adapters/rithmic/completed.md`

Added the missing mocked coverage for one shared Rithmic gateway serving multiple account-scoped
execution clients under the same login/system, and fixed the adapter-side account filter the new
coverage exposed in the live execution PnL path.

**Delivered:**

- added shared-gateway mock coverage proving multiple account-scoped execution clients reuse one
  upstream order connection and one upstream PnL connection for the same login/system
- verified account-scoped order subscriptions, PnL subscriptions, PnL snapshot requests, and order
  query requests are all issued per venue account even when the gateway transport is shared
- added multi-account execution-loop coverage proving order reports and position reports are
  filtered by venue account before being emitted into per-client state
- fixed the live execution PnL filter so venue account events are matched against the venue account
  identity, while generated Nautilus reports still use the adapter-local account ID

**Validation / notes:**

- `cargo test -p rithmic-nt execution_event_loop_filters_reports_by_venue_account --lib`
- `cargo test -p rithmic-nt shared_gateway_reuses_order_and_pnl_connections_across_accounts --lib`
- `cargo test -p rithmic-nt --lib`
- `cargo test -p rithmic-nt --test live_client`

### 0i. 2026-04-02 completion pass: Python live custom-data parity for Rithmic ✅

**Files:** `crates/adapters/rithmic/src/python/data.rs`,
`crates/adapters/rithmic/src/python/events.rs`,
`nautilus_trader/adapters/rithmic/__init__.py`,
`nautilus_trader/adapters/rithmic/_rithmic.pyi`,
`nautilus_trader/adapters/rithmic/custom.py`,
`nautilus_trader/adapters/rithmic/data.py`,
`tests/integration_tests/adapters/rithmic/test_data_client.py`,
`crates/adapters/rithmic/devplan.md`,
`crates/adapters/rithmic/completed.md`

Removed the remaining Python-wrapper gap for live Rithmic custom market-data feeds so the
TradingNode path can subscribe to the same semantic ticker surfaces as the Rust v2 adapter.

**Delivered:**

- added Python-native Rithmic custom data classes for trade statistics, quote statistics,
  indicator prices, open interest, end-of-day prices, order price limits, and symbol margin rate
- extended the PyO3 market-data bridge so custom events expose stable type names plus structured
  JSON payloads instead of only an untyped marker string
- added semantic subscribe and unsubscribe methods for those feeds on the PyO3 `RithmicDataClient`
  and replayed them during reconnect resubscription
- taught the Python `RithmicLiveDataClient` to route `SubscribeData` / `UnsubscribeData` for those

  custom datatypes and emit typed Nautilus `CustomData(...)` objects back through `_handle_data(...)`
- added focused Python integration coverage for custom subscription routing and custom event
  conversion

**Validation / notes:**

- `cargo test -p rithmic-nt --features python --lib`
- `python3 -m compileall nautilus_trader/adapters/rithmic/__init__.py nautilus_trader/adapters/rithmic/custom.py nautilus_trader/adapters/rithmic/data.py tests/integration_tests/adapters/rithmic/test_data_client.py`
- `uv run --active --no-sync python -m pytest tests/integration_tests/adapters/rithmic/test_data_client.py -q`
  currently fails in this checkout before the Rithmic tests run because the broader local Python

  environment is missing `nautilus_trader.model.instruments.tokenized_asset`

### 0h. 2026-04-02 completion pass: point `rithmic-rs` at the BurnOutTrader `main` branch ✅

**Files:** `crates/adapters/rithmic/Cargo.toml`,
`crates/adapters/rithmic/devplan.md`,
`crates/adapters/rithmic/completed.md`

Switched the adapter off the vendored path dependency and onto the GitHub fork at
`BurnOutTrader/rithmic-rs`, tracking the `main` branch.

**Delivered:**

- created and pushed `develop` on `BurnOutTrader/rithmic-rs`, seeded from the Nautilus vendored
  adapter patch so the fork has a branch for Nautilus-specific changes
- opened and merged a PR from `develop` into `main`, so `main` now carries that working
  Nautilus baseline
- `rithmic-nt` now depends on `https://github.com/BurnOutTrader/rithmic-rs` from the
  `main` branch instead of resolving `rithmic-rs` from `crates/adapters/rithmic/vendor/`

- updated the adapter-local planning notes so they no longer describe the semantic ticker
  subscription surface as being provided only by a vendored local patch

**Residual follow-up:**

- Cargo will still record the currently resolved fork commit in `Cargo.lock`; refreshing to a newer
  `main` branch commit remains an explicit `cargo update -p rithmic-rs`
- upstreaming the semantic ticker subscription surface to `pbeets/rithmic-rs` is intentionally

  deferred until we have more Nautilus runtime confidence, but the fork-backed `main` branch is
  now the shipped dependency surface for this adapter

### 0g. 2026-04-02 completion pass: deterministic Rithmic replay fixtures and regression coverage ✅

**Files:** `crates/adapters/rithmic/src/data/live.rs`,
`crates/adapters/rithmic/src/gateway.rs`,
`crates/adapters/rithmic/test_data/custom_data_events.json`,
`crates/adapters/rithmic/test_data/depth_snapshot_delta.json`,
`crates/adapters/rithmic/test_data/history_time_bar_replay.json`,
`crates/adapters/rithmic/test_data/market_data_quote_trade.json`,
`crates/adapters/rithmic/tests/common/mod.rs`,
`crates/adapters/rithmic/devplan.md`,
`crates/adapters/rithmic/completed.md`

Captured the small deterministic Rithmic fixture set that remained on the beta follow-up list and
wired it into the non-live regression suite.

**Delivered:**

- added file-backed quote/trade and time-bar replay samples under

  `crates/adapters/rithmic/test_data/` and reused them in the mock ticker/history plant tests
- added deterministic depth snapshot-plus-delta fixtures covering reconnect bootstrap and local
  mixed snapshot/delta order-book application
- added fixture-driven gateway coverage for the remaining mapped Rithmic custom-data event types:
  trade statistics, quote statistics, indicator prices, open interest, end-of-day prices, order
  price limits, and symbol margin rate
- updated the Rithmic dev plan so deterministic replay coverage no longer remains listed as open

**Residual follow-up:**

- the captured fixtures are intentionally small and targeted; a larger catalog of live-captured
  venue samples can still be added later if broader parser/backtest scenarios become necessary

**Regression coverage / validation:**

- `cargo test -p rithmic-nt --lib gateway::tests::test_depth_snapshot_fixture_emits_clear_then_snapshot_adds`
- `cargo test -p rithmic-nt --lib gateway::tests::test_remaining_custom_data_fixture_samples_transform_to_custom_events`

- `cargo test -p rithmic-nt --lib data::live::tests::order_book_snapshot_and_delta_fixtures_produce_expected_depth10`
- `cargo test -p rithmic-nt --test gateway`

### 0f. 2026-04-02 completion pass: remove built-in Rithmic app-name fallback ✅

**Files:** `crates/adapters/rithmic/src/common/credential.rs`,
`crates/adapters/rithmic/src/config.rs`,
`crates/adapters/rithmic/src/gateway.rs`,
`crates/adapters/rithmic/src/python/config.rs`,
`crates/adapters/rithmic/src/python/gateway.rs`,
`nautilus_trader/adapters/rithmic/config.py`,
`docs/integrations/rithmic.md`,
`examples/live/rithmic/rithmic_data_tester.py`,
`examples/live/rithmic/rithmic_exec_tester.py`,
`tests/integration_tests/adapters/rithmic/conftest.py`,
`tests/integration_tests/adapters/rithmic/test_config.py`,
`tests/integration_tests/adapters/rithmic/test_data_client.py`,
`tests/integration_tests/adapters/rithmic/test_execution_client.py`,
`tests/integration_tests/adapters/rithmic/test_factories.py`

Removed the adapter's built-in fallback app-name path so users now have to supply their own
Rithmic conformance application name.

**Delivered:**

- env-driven Rust and Python config loading now require `RITHMIC_APP_NAME` instead of silently
  injecting a branded default
- gateway config validation now rejects empty `app_name` when building the live Rithmic login
  config, so raw programmatic callers cannot connect accidentally with an implicit fallback
- raw PyO3 constructors no longer default the app name to `NautilusTrader`
- integration docs and live-test guards now state that `RITHMIC_APP_NAME` must be present

**Residual follow-up:**

- historical Git references to the retired fallback string still exist in older branch history and
  would require deliberate history rewriting to scrub fully

### 0e. 2026-04-02 completion pass: ticker subscription methods and wrapper depth parity ✅

**Files:** `crates/adapters/rithmic/Cargo.toml`,
`crates/adapters/rithmic/src/data.rs`,
`crates/adapters/rithmic/src/data/client.rs`,
`crates/adapters/rithmic/src/data/live.rs`,
`crates/adapters/rithmic/src/gateway.rs`,
`crates/adapters/rithmic/src/python/data.rs`,

`crates/adapters/rithmic/vendor/rithmic-rs/src/plants/ticker_plant.rs`,
`nautilus_trader/adapters/rithmic/data.py`,
`tests/integration_tests/adapters/rithmic/test_data_client.py`,
`docs/integrations/rithmic.md`,
`crates/adapters/rithmic/devplan.md`,
`crates/adapters/rithmic/completed.md`

Completed the remaining adapter gaps that were left open after the first market-data expansion
pass.

**Delivered:**

- vendored the pinned `rithmic-rs` revision into the adapter and patched the ticker-plant handle
  to expose explicit per-feed ticker subscription methods directly in this repository
- Rust v2 data clients now explicitly subscribe and unsubscribe `MarketMode` /

  `InstrumentStatus` plus the live Rithmic custom-data feeds (`TradeStatistics`,
  `QuoteStatistics`, `IndicatorPrices`, `OpenInterest`, `EndOfDayPrices`,
  `OrderPriceLimits`, `SymbolMarginRate`) instead of only converting those messages if they happen
  to arrive
- reconnect/unsubscribe bookkeeping now tracks those extra ticker-plant subscriptions alongside
  quote/trade, bars, and order-book feeds
- the PyO3 data binding now exposes instrument-status subscriptions, book-depth10 subscriptions,
  and request-time depth snapshots
- the thin Python `TradingNode` wrapper now supports order-book depth subscriptions, explicit
  order-book snapshot requests, and reconnect snapshot replay for subscribed depth books
- updated the public Rithmic guide and adapter-local planning docs so they no longer describe the
  generic subscription path or Python wrapper depth snapshot helpers as open gaps

**Moved from `devplan.md` detail:**

- **Depth / MBO bootstrap is now snapshot + delta on the Rust adapter path.**
  - Book subscriptions now request `request_depth_by_order_snapshot(...)` when the first order-book
    subscription is established and again during reconnect resubscription.

  - Snapshot rows are transformed into synthetic Nautilus `OrderBookDelta` events with
    `CLEAR -> ADD* -> F_SNAPSHOT -> F_LAST`, so downstream order books start from a complete state
    before applying live deltas.
  - The Rust v2 live client now maintains local order-book state and emits `OrderBookDepth10`
    snapshots when subscribed on the engine-facing path.
- **Rust v2 quote/trade conversion now preserves Nautilus semantics.**
  - Venue timestamps are now carried through to Nautilus `QuoteTick` and `TradeTick` instead of

    being replaced with local `now`.
  - Parsed Rithmic aggressor side is now preserved instead of being collapsed to
    `AggressorSide::NoAggressor`.
- **Additional `rithmic-rs` market-data surfaces are now mapped where the adapter can support them.**
  - `OrderBook` -> Nautilus `OrderBookDepth10`

  - `MarketMode` -> Nautilus `InstrumentStatus`
  - `TradeStatistics`, `QuoteStatistics`, `IndicatorPrices`, `OpenInterest`,
    `EndOfDayPrices`, `OrderPriceLimits`, and `SymbolMarginRate` -> adapter custom data types
  - `request_get_volume_at_price(...)` -> request-only `RithmicVolumeAtPrice` custom data
- **Explicit ticker subscription methods are now available in this checkout.**
  - The adapter now ships a local vendored `rithmic-rs` patch that exposes semantic

    subscribe/unsubscribe handle methods for `MarketMode` / `InstrumentStatus`, `OrderBook`, and
    the additional Rithmic statistics and indicator feeds.
  - Rust v2 live clients now use those explicit methods instead of relying on a public generic
    `UpdateBits` helper or only converting those venue messages opportunistically.
- **Python wrapper order-book parity is now in place.**
  - The thin Python wrapper now exposes book-depth subscriptions, request-time depth snapshots,
    and reconnect snapshot replay for subscribed depth books.
  - The wrapper also now supports venue instrument-status subscriptions instead of leaving
    `MarketMode` as a Rust-only path.

**Residual follow-up:**

- the semantic ticker subscription patch is now shipped through
  `BurnOutTrader/rithmic-rs` on the `stable` branch; upstreaming to `pbeets/rithmic-rs` is now a
  later optional follow-up rather than an active adapter blocker
- broader deterministic replay/fixture coverage for the expanded custom-data surfaces still

  remains a separate follow-up

**Regression coverage / validation:**

- `cargo check -p rithmic-nt --features python`
- `cargo test -p rithmic-nt --features python --lib`
- `uv run --active --no-sync python -m pytest tests/integration_tests/adapters/rithmic/test_data_client.py -q`
- `python3 -m compileall nautilus_trader/adapters/rithmic/data.py tests/integration_tests/adapters/rithmic/test_data_client.py`

### 0d. 2026-04-02 implementation pass: Rithmic depth bootstrap and market-data surface expansion ✅

**Files:** `crates/adapters/rithmic/src/data.rs`,
`crates/adapters/rithmic/src/data/client.rs`,
`crates/adapters/rithmic/src/data/custom.rs`,
`crates/adapters/rithmic/src/data/live.rs`,

`crates/adapters/rithmic/src/gateway.rs`,
`crates/adapters/rithmic/src/lib.rs`,
`crates/adapters/rithmic/src/python/events.rs`,
`nautilus_trader/adapters/rithmic/data.py`,
`devplan.md`,
`completed.md`

Implemented the market-data hardening follow-up that was identified in the 2026-04-02 planning
audit.

**Delivered:**

- order-book subscriptions now bootstrap from `request_depth_by_order_snapshot(...)` on initial
  subscribe and reconnect, then convert snapshot rows into Nautilus `OrderBookDelta` application
  boundaries before live `DepthByOrder` deltas continue
- Rust v2 data conversion now preserves venue timestamps on `QuoteTick` and `TradeTick`, and keeps
  the parsed trade aggressor side instead of collapsing it to `NoAggressor`

- Rust v2 data clients now support order-book depth10 subscription and book snapshot requests on
  the engine-facing path
- `rithmic-rs` `OrderBook` updates now convert into Nautilus `OrderBookDepth10`
- `rithmic-rs` `MarketMode` updates now convert into Nautilus `InstrumentStatus`
- added adapter custom-data types and conversion plumbing for `TradeStatistics`,
  `QuoteStatistics`, `IndicatorPrices`, `OpenInterest`, `EndOfDayPrices`,

  `OrderPriceLimits`, `SymbolMarginRate`, and request-only `RithmicVolumeAtPrice`
- Python/PyO3 event bindings now recognize depth10, instrument-status, and custom-data events so
  the wrapper path does not silently discard those event categories
- updated `devplan.md` to reflect what is now implemented versus what remains blocked by the
  current public `rithmic-rs` ticker-plant API

**Residual follow-up:**

- the current public `rithmic-rs` ticker-plant handle still does not expose a generic per-symbol
  market-data-update request API, so some upstream surfaces can be converted if received but
  cannot yet be requested cleanly by the adapter without upstream expansion or a local fork
- Python wrapper parity for low-level book-depth snapshot/request helpers is still narrower than
  the raw Rust v2 engine path

**Regression coverage / validation:**

- `cargo check -p rithmic-nt --features python`
- `cargo test -p rithmic-nt --features python --lib`

- `python3 -m compileall nautilus_trader/adapters/rithmic/data.py`

### 0c. 2026-04-02 planning audit: depth/MBO coverage and upstream market-data gaps ✅

**Files:** `devplan.md`, `completed.md`

Audited the current Rithmic adapter against the checked-out `rithmic-rs` market-data surface and
extended the production-beta planning docs with the concrete follow-up work.

**Delivered:**

- identified that the adapter currently converts live `DepthByOrder` updates into Nautilus
  `OrderBookDelta`, but does not yet use the upstream `request_depth_by_order_snapshot(...)` or
  `DepthByOrderEndEvent` bootstrap path
- identified a Rust v2 conversion gap where quote/trade events are parsed with venue timestamps in
  `gateway.rs` but converted to Nautilus ticks with local `now` timestamps in `data/live.rs`
- identified a Rust v2 conversion gap where parsed Rithmic trade aggressor side is currently
  collapsed to `AggressorSide::NoAggressor`
- catalogued additional `rithmic-rs` market-data surfaces not currently exposed by the adapter:
  `OrderBook`, `TradeStatistics`, `QuoteStatistics`, `IndicatorPrices`, `OpenInterest`,

  `EndOfDayPrices`, `MarketMode`, `OrderPriceLimits`, `SymbolMarginRate`,
  `request_get_volume_at_price(...)`, and `subscribe_by_underlying(...)`
- added a dedicated `devplan.md` section for beta-production follow-up covering depth/MBO
  bootstrap, field-mapping verification, Rust v2 Nautilus type integrity, and unsupported upstream
  data-surface review

**Validation:**

- documentation/planning audit only; no code changes or tests were run

### 0b. 2026-04-02 beta completion pass: docs, ordering, and TickBar parity ✅

**Files:** `crates/adapters/rithmic/src/data/live.rs`,
`crates/adapters/rithmic/src/execution/live.rs`,
`crates/adapters/rithmic/src/python/data.rs`,
`docs/integrations/rithmic.md`,
`nautilus_trader/adapters/rithmic/execution.py`,

`tests/integration_tests/adapters/rithmic/test_data_client.py`,
`tests/integration_tests/adapters/rithmic/test_factories.py`,
`devplan.md`

Completed the remaining beta-blocking items from `devplan.md`.

**Delivered:**

- runtime-path documentation now separates raw bindings, Rust v2 `LiveNode`, and Python
  `TradingNode` wrapper capabilities instead of blending them together
- docs now state the then-current execution boundary for that pass: Rust v2 and Python wrapper
  paths rejected general `SubmitOrderList`, while native bracket / OCO helpers remained
  low-level binding workflows
- historical TickBar replay now uses the newer `rithmic-rs` `load_tick_bars(...)` path, restoring
  configurable historical `N-TICK` support across the raw binding, Rust v2, and Python wrapper
  runtime paths
- Python wrapper PnL normalization warnings now emit once per stable mismatch delta rather than
  repeating indefinitely through long-running sessions

- Rust v2 and Python wrapper reconciliation report generation now returns deterministic
  chronological order for order, fill, and position recovery, which fixes the external-order
  replay ordering issue that could underflow position duration during engine reconciliation
- added deterministic non-live regression coverage for stable mismatch handling, chronological
  reconciliation ordering, and historical `N-TICK` TickBar request parity on the wrapper path

**Regression coverage / validation:**

- `cargo test -p rithmic-nt --lib`
- `cargo check -p rithmic-nt --features python`
- `uv run --active --no-sync python -m pytest tests/integration_tests/adapters/rithmic/test_factories.py tests/integration_tests/adapters/rithmic/test_data_client.py -q`
- `python3 -m compileall nautilus_trader/adapters/rithmic/data.py nautilus_trader/adapters/rithmic/execution.py`

### 0. 2026-04-01 production hardening pass: reconnect and reconciliation ✅

**Files:** `crates/adapters/rithmic/src/config.rs`,
`crates/adapters/rithmic/src/data/client.rs`,
`crates/adapters/rithmic/src/data/live.rs`,
`crates/adapters/rithmic/src/execution/client.rs`,
`crates/adapters/rithmic/src/execution/live.rs`,

`crates/adapters/rithmic/src/factories.rs`,
`crates/adapters/rithmic/src/gateway.rs`,
`crates/adapters/rithmic/src/python/config.rs`,
`crates/adapters/rithmic/src/python/data.rs`,
`crates/adapters/rithmic/src/python/events.rs`,
`crates/adapters/rithmic/src/python/gateway.rs`,

`nautilus_trader/adapters/rithmic/data.py`,
`nautilus_trader/adapters/rithmic/execution.py`

Completed the final production-hardening implementation pass that was previously listed as active
work in `devplan.md`.

**Delivered:**

- forced logout, close-frame, heartbeat-timeout, and plant-channel shutdown now trigger adapter-led
  reconnect recovery instead of requiring a manual restart
- reconnect recovery now restores all tracked live data surfaces, including quotes, trades,
  time bars, tick bars, and book deltas
- Rust v2 execution clients now bootstrap PnL snapshot, live-order query, and bounded execution
  replay on connect/reconnect
- Rust v2 execution clients now emit account state and reconciliation reports, including
  `OrderStatusReport`, `FillReport`, `PositionStatusReport`, and `ExecutionMassStatus`

- monotonic order-state/report filtering was added so regressive post-fill reports are dropped
  rather than reaching the engine
- lifecycle cleanup now tears down gateways, clears stale handles/state, and prevents reuse after
  disconnect
- Python wrapper clients now handle connection-state/reconnect events explicitly and re-bootstrap

  their local recovery state
- PyO3 data bindings now expose book-delta events and reconnect resubscription helpers for Python
  strategies
- execution config/factory parity was tightened with explicit `trader_id` and bounded
  `execution_replay_lookback_secs`

**Regression coverage / validation:**

- added Rust unit coverage for regressive snapshot suppression after fills
- `cargo test -p rithmic-nt --lib`
- `cargo check -p rithmic-nt --features python`
- `python3 -m compileall nautilus_trader/adapters/rithmic/data.py nautilus_trader/adapters/rithmic/execution.py`

**Operational note:** no live venue tests were run as part of this implementation move; live
validation is now allowed and should be used selectively on the remaining follow-up items.

### 1. N-tick bars: `BarAggregation::Tick` mapped in `data/live.rs` ✅

**Files:** `data/live.rs`, `gateway.rs`, `test_data_client.py`

- `subscribe_bars` → `BarAggregation::Tick` calls `history_handle().subscribe_tick_bar_updates(…, BarType::TickBar, BarSubType::Regular, &period.to_string(), Request::Subscribe)`
- `unsubscribe_bars` → matching `Request::Unsubscribe` path
- `request_bars` → `BarAggregation::Tick` calls `history_handle().load_ticks(symbol, exchange, start_sec, end_sec)`, parses `ResponseTickBarReplay` → `Bar` using `data_bar_ssboe`/`data_bar_usecs` timestamps, emits `BarsResponse`
- Live tests: `test_subscribe_tick_bars_after_connect`, `test_unsubscribe_tick_bars`

### 2. `request_trades` — NOT SUPPORTED by rithmic-rs ✅

**File:** `data/live.rs`

Overridden with explicit `log::warn` explaining that rithmic-rs history plant exposes tick bar replay (`RequestTickBarReplay`) but not individual historical trade ticks (no `RequestTickByTick` equivalent). Strategies get a clear message instead of silence.

### 3. `subscribe_book_deltas` / order book depth ✅

**Files:** `data/client.rs`, `gateway.rs`, `data/live.rs`

- Added `BookDelta` struct to `MarketDataEvent` in `client.rs`
- Updated `transform_market_data_message` in `gateway.rs` to return `Vec<MarketDataEvent>` and handle `RithmicMessage::DepthByOrder` — maps `UpdateType::New→ADD`, `Change→UPDATE`, `Delete→REMOVE`; `TransactionType::Buy→BUY`, `Sell→SELL`; emits one `BookDelta` per array entry
- Implemented `subscribe_book_deltas` → `gateway.subscribe_order_book()` in `data/live.rs`
- Implemented `unsubscribe_book_deltas` → `gateway.unsubscribe_order_book()` in `data/live.rs`
- `convert_market_data_event` maps `BookDelta` → Nautilus `OrderBookDelta` and emits via data sender

---

## P1 — Completeness / Nautilus Parity

### 4–5. Python `TradingNode` compatibility wrappers restored ✅

**Files:** `nautilus_trader/adapters/rithmic/__init__.py`,
`nautilus_trader/adapters/rithmic/data.py`,
`nautilus_trader/adapters/rithmic/execution.py`,
`nautilus_trader/adapters/rithmic/factories.py`,
`examples/live/rithmic/rithmic_data_tester.py`,
`examples/live/rithmic/rithmic_exec_tester.py`,

`tests/integration_tests/adapters/rithmic/test_factories.py`,
`tests/integration_tests/adapters/rithmic/test_node.py`

Implemented the required split path for v2 PyO3:

- `nautilus_trader.adapters.rithmic` now exports Python wrapper factories that subclass
  `LiveDataClientFactory` / `LiveExecClientFactory`
- raw PyO3 `RithmicDataClientFactory` / `RithmicExecClientFactory` remain available from

  `nautilus_trader.adapters.rithmic.bindings` for the Rust/registry path
- added thin Python `RithmicLiveDataClient` / `RithmicLiveExecutionClient` wrappers so Python
  `TradingNode` can drive the Rust-backed gateway and client objects without moving the heavy
  adapter logic out of Rust

**Wrapper scope kept intentionally thin:**

- data wrapper supports instrument loading, quote/trade subscriptions, external bar subscriptions,
  and historical bar requests
- execution wrapper supports standard single-order submit / modify / cancel flows, account

  snapshots, reconciliation report generation, and venue-order / fill report caching

**Example fixes included:**

- `rithmic_data_tester.py` and `rithmic_exec_tester.py` now use synchronous `main()` entrypoints so
  `node.run()` blocks correctly
- `rithmic_exec_tester.py` now resolves instruments with a data-only gateway instead of enabling
  the order plant with an empty account ID

**Tests added/updated:**

- adapter exports are verified to be Python `TradingNode` factories rather than raw binding
  factories
- wrapper factories are verified to create wrapper live clients
- `TradingNode.build()` is verified to accept the wrapper factories
- wrapper historical-bar requests are verified to convert PyO3 bars into Nautilus Python `Bar`
  objects before handing them to the Python data engine
- wrapper PnL snapshots are verified to normalize inconsistent Rithmic `available` balances to the
  Nautilus `AccountBalance(total, locked, free)` invariant

**Verified:**

- `issubclass(RithmicDataClientFactory, LiveDataClientFactory) == True`
- `issubclass(RithmicExecClientFactory, LiveExecClientFactory) == True`
- wrapper/node integration tests passed before live-credential validation

### 4–5b. Wrapper data/account normalization fixes and live validation ✅

**Files:** `nautilus_trader/adapters/rithmic/data.py`,

`nautilus_trader/adapters/rithmic/execution.py`,
`nautilus_trader/adapters/rithmic/providers.py`,
`tests/integration_tests/adapters/rithmic/test_data_client.py`,
`tests/integration_tests/adapters/rithmic/test_factories.py`,
`tests/integration_tests/adapters/rithmic/test_providers.py`

Follow-up fixes were required after the first wrapper pass:

- instrument/provider loading now converts PyO3 instrument objects into Nautilus Python instrument
  objects before caching them, fixing mixed Python/PyO3 currency model errors during connect
- historical bar requests and live bar/quote/trade callbacks now convert PyO3 market-data objects
  into Nautilus Python model objects before they enter the Python data engine

- empty execution replay windows (`Replay executions failed: no data`) are treated as normal
  empty-history startup conditions instead of fatal errors
- wrapper PnL snapshots now normalize inconsistent Rithmic `available` values to Nautilus'
  required `free = total - locked` invariant while preserving the raw reported value in `info`

**Live validation:**

- full live integration suite with real credentials: `pytest tests/integration_tests/adapters/rithmic -q -rs`
  → `71 passed`
- live EMA example smoke:

  - both wrapper clients connected
  - execution reconciliation completed
  - historical warmup returned `Bar[121]`
  - live external bars continued flowing without PyO3 conversion errors

### 6. Execution client: modify/cancel/bracket audit ✅

**File:** `execution/live.rs`

- `modify_order`, `cancel_order`, `cancel_all_orders`, `batch_cancel_orders` all wired correctly
- Fixed: `disconnect()` now calls `gateway.disconnect()` (was leaving TCP open)
- Fixed: `modify_order` warns when `trigger_price` is set (rithmic-rs has no stop-price modify)
- historical note for that audit: `submit_order_list` (bracket/OCO) was still explicitly rejected
  with `log::warn` before the later 2026-04-03 order-list parity pass above
- Report generation (`generate_*`): TODOs intact, tracked separately as TASK-17

**Python `TradingNode` note:** this historical section predates the later
2026-04-03 order-list parity pass above, which added supported high-level OCO
and limit-entry bracket routing on the thin wrapper path

### 7. Execution reconciliation `avg_px` recovery hardening ✅

**Files:** `nautilus_trader/adapters/rithmic/execution.py`,
`tests/integration_tests/adapters/rithmic/test_factories.py`,
`docs/integrations/rithmic.md`

Live reconnect/reconciliation uncovered a real wrapper gap: Rithmic can replay filled external
orders with `filled_qty > 0` while omitting cumulative average fill price on the order snapshot.
That left `OrderStatusReport.avg_px=None`, triggered the execution engine warning
`report.avg_px was None when a value was expected`, and weakened inferred-fill recovery when only a
partial fill history was available locally.

**Fixes:**

- fill reports are now built/stored before the wrapper generates the corresponding fill-status
  report
- fill-status reports now backfill `avg_px` from the observed fill stream when the venue omits
  cumulative average price
- when multiple fills are observed, the wrapper recomputes a weighted average from the known fill
  reports instead of leaving `avg_px` empty

**Tests added:**

- first fill without venue `avg_price` still produces a usable status-report `avg_px`
- subsequent fills without venue `avg_price` recompute weighted `avg_px` from accumulated fill
  reports

**Live validation:**

- sourced `.env` live wrapper connect previously surfaced 4 filled external orders with
  `filled_qty > 0 && avg_px is None`
- after the fix, the same recovery path produced populated `avg_px` values for all 4 orders

### 7b. Local submit event enrichment for sparse Rithmic notifications ✅

**Files:** `crates/adapters/rithmic/src/python/execution.rs`,
`crates/adapters/rithmic/src/python/instruments.rs`

Live local-submit testing uncovered a second production gap on the thin Python wrapper path:
Rithmic can emit an immediate submitted notification with placeholder values such as
`quantity=0`, empty symbol/exchange strings, and zero price fields before the wrapper has any
reconciled venue state. The Python `OrderStatusReport` constructor rejects `quantity=0`, which

caused callback crashes on locally submitted orders.

**Fixes:**

- the Rust PyO3 execution client now keeps full tracked local-order metadata, not just the venue
  order ID and side
- execution events are enriched in Rust before dispatch to Python, replacing sparse placeholder
  submit values with tracked local order metadata
- the enrichment applies at the PyO3 boundary so the Python wrapper remains thin and does not need
  local shadow-state hacks
- the PyO3 instrument bindings now import the `Instrument` trait explicitly so the Python-featured
  crate test/build path compiles cleanly

**Tests added:**

- submitted events with entirely missing context are backfilled from tracked local orders
- submitted events with sparse placeholder values (`quantity=0`, empty strings, zero price) are
  rewritten from tracked local orders before reaching Python

**Verified:**

- `cargo test -p rithmic-nt --features python --lib python::execution::tests`
- rebuilt extension with `uv run --active --no-sync build.py`

### 7. `request_instrument` / `request_instruments` ✅

**File:** `data/live.rs`

- `request_instrument`: clones ticker handle, calls `get_reference_data` + `get_auxilliary_reference_data`, emits `InstrumentResponse`
- `request_instruments`: iterates `KNOWN_EXCHANGES` via `search_symbols`, emits `InstrumentsResponse`
- `subscribe_instrument_status`: left as default no-op (Rithmic doesn't send status events)
- `instruments::parse` module made public for cross-module use

---

## P3 — Correctness Hardening

### 10. Historical tick bar replay error handling ✅

**File:** `data/live.rs`

Replaced IIFE async-closure pattern (which caused type-inference compile errors) with explicit `match` chains in both the tick-bar and time-bar paths of `request_bars`. On any failure (missing history plant, network error, parse error), the handler now logs an error and sends an empty `BarsResponse` — preventing strategies from hanging indefinitely waiting for a response that would never arrive.

### 11. `unsubscribe_all` venue-side cleanup ✅

**Files:** `data/client.rs`, `data/live.rs`

Added `RithmicDataClient::unsubscribe_all_async()` which iterates all active market-data subscriptions (key = `"exchange:symbol"`) and bar subscriptions (key = `"exchange:symbol:BarType:period"`), sends the corresponding unsubscribe request to the Rithmic ticker/history plant, then clears local tracking. Wired into `RithmicLiveDataClient::disconnect()` so the venue stops pushing data before the connection is torn down.

### 12. Reconnection does not re-subscribe ✅

**Files:** `data/client.rs`, `data/live.rs`

Added `RithmicDataClient::resubscribe_all()` which iterates every active market-data and bar subscription and calls the gateway directly (bypassing the normal dedup logic that would short-circuit since local state still shows "subscribed"). Handles `MarketDataEvent::Reconnected` in the event loop in `connect()` — when the gateway emits this after successful backoff reconnect, `resubscribe_all()` is called so strategies resume receiving data automatically.

### 14. Audit depth/MBO book delta implementation ✅

**Files:** `data/client.rs`, `data/live.rs`, `gateway.rs`

Three bugs found and fixed:

1. **Missing `F_LAST` flag**: All `OrderBookDelta` were emitted with `flags=0`. NautilusTrader requires `RecordFlag::F_LAST` (0x80) on the last delta in each batch to signal when to apply the update. The final delta in each `DepthByOrder` message now carries this flag; all preceding deltas carry `0`.

2. **Panic on zero-size Add/Update**: `depth_size` used `unwrap_or(0)`, which could produce a zero-size quantity. `OrderBookDelta::new()` panics if size ≤ 0 for Add/Update. Switched to `new_checked()` with warn-and-skip instead.

3. **`BookDelta::flags` field added**: Intermediate struct needed a `flags: u8` field so the gateway can communicate `F_LAST` to the live client.

All other fields verified correct against rithmic-rs proto: `update_type` mapping, `transaction_type` mapping, `depth_order_priority` as `order_id`, `ssboe/usecs` → `ts_event`, `now_nanos()` → `ts_init`, per-message `sequence_number`, precision fallback.

---

## P2 — Backtest/Live Parity

### 13. VolumeProfileBars as CustomData ✅

**Files:** `data/volume_profile.rs` (new), `data.rs`, `data/live.rs`, `lib.rs`

- Defined `RithmicMinuteVolumeProfileBar` struct in `data/volume_profile.rs` implementing `CustomDataTrait`
- Fields: standard OHLCV + `volume/bid_volume/ask_volume/num_trades` + `poc_price` (computed) + parallel `profile_price/profile_bid_volume/profile_ask_volume` arrays
- `compute_poc()` derives Point of Control (highest bid+ask combined volume) at parse time
- `VOLUME_PROFILE_TYPE_NAME = "RithmicMinuteVolumeProfileBar"` constant for `DataType` matching
- Full `CustomDataTrait` impl: `type_name`, `to_json`, `from_json`, `clone_arc`, `eq_arc`, JSON round-trip
- `data.rs`: added `pub mod volume_profile` and re-exported `RithmicMinuteVolumeProfileBar`
- `lib.rs`: re-exported `RithmicMinuteVolumeProfileBar` at crate root
- `data/live.rs`:
  - `ensure_custom_data_json_registered::<RithmicMinuteVolumeProfileBar>()` called on `connect()` (idempotent)
  - `subscribe()` override: warns that volume profile is historical-only, directs users to `request_data`
  - `request_data()` override: parses `InstrumentId` from `data_type.identifier()`, optional `period` (minutes, default 1) from `data_type.metadata()["period"]`, calls `history_handle().load_volume_profile_minute_bars(...)`, parses `ResponseVolumeProfileMinuteBars` responses into `Vec<RithmicMinuteVolumeProfileBar>`, emits `CustomDataResponse` → `DataEvent::Response(DataResponse::Data(...))`
- 4 unit tests for `compute_poc`, empty arrays, JSON round-trip, `CustomDataTrait` methods
- 21/21 tests pass, 0 warnings

---

## P4 — Architecture / Design

### 15. Eliminate `Arc<RwLock<RithmicGateway>>` from `RithmicInstrumentProvider` ✅

**Files:** `instruments/provider.rs`, `python/instruments.rs`

**Investigation finding:** Full `Clone` on `RithmicGateway` via `Arc<Mutex<>>` wrapping was the
wrong design. Both `RithmicTickerPlantHandle` and `RithmicHistoryPlantHandle` are already `Clone`
— `data/live.rs` already uses `.cloned()` on them before every await. No gateway.rs changes needed.

**Root cause of `Arc<RwLock<>>` in instrument provider:** The provider held a `.read()` lock guard
across async await points (ticker queries), which required `tokio::sync::RwLock`. Existing consumers
using plain `Arc<RithmicGateway>` (data/client, account/position providers) already avoided this by
cloning handles before awaiting.

**Fix applied:**

- `RithmicInstrumentProvider` changed from `Arc<tokio::sync::RwLock<RithmicGateway>>` to `Arc<RithmicGateway>`
- All query methods now clone the ticker handle once up front (`gateway.ticker_handle().cloned()`) and drop any borrow before the first `.await` — matching the existing pattern in `data/live.rs`
- `load_instrument_async` folded its two separate lock-held-across-await blocks into a single handle clone at method entry
- Docstring example updated to show pre-connect-then-Arc pattern
- `python/instruments.rs`: `PyRithmicInstrumentProvider` reimplemented to own its logic directly using `Arc<RwLock<>>` gateway (Python v1 path keeps the RwLock since it calls `connect()`/`disconnect()` through the same Arc). Stores its own `DashMap` cache and free helper functions `load_exchange_with_handle` / `load_instrument_with_handle` that take a pre-cloned handle — no lock held across await anywhere.

**Channel choices confirmed correct (no changes):**

- Event buses (market_data, execution, pnl): `unbounded` — burst-tolerant, correct
- `shutdown_tx`: `mpsc::channel(1)` — signal semantics, correct
- `data/live.rs` shutdown: `oneshot::Sender` — semantically ideal for one-shot stop

**100/100 unit tests pass.** 2 pre-existing integration test failures in `tests/data_client.rs`
(`subscribe_quotes_*`) — mock server doesn't handle template ID 14 (`RequestReferenceData`) sent
during `ensure_instrument_info`; unrelated to this change.

---

## Infrastructure

### Node testers migrated to v2 builder pattern ✅

**Files:** `examples/node_data_tester.rs`, `examples/node_exec_tester.rs`

- Migrated `DataTesterConfig` and `ExecTesterConfig` from `.new()` + `.with_*()` chaining to `.builder()` + `.build()` pattern
- Matches OKX v2 reference pattern exactly
- Added `.subscribe_bars(true)` flag to data tester

---

## Already Done Prior to This Plan

| Component | Status |
|-----------|--------|
| `data/live.rs` — `DataClient` trait (subscribe quotes, trades, bars, unsubscribe, request_bars) | ✅ |
| `execution/live.rs` — `ExecutionClient` trait (basic orders wired) | ✅ |
| `factories.rs` — `LiveNode` wiring (`RithmicDataClientFactory`, `RithmicExecClientFactory`) | ✅ |
| PyO3 bindings — gateway, data, exec (`python/gateway.rs`, `python/data.rs`, `python/execution.rs`) | ✅ |
| Python `config.py` — `RithmicDataClientConfig`, `RithmicExecClientConfig` | ✅ |
| Python `providers.py` — `RithmicInstrumentProvider` with front-month lookup | ✅ |
| Time bars (Second/Minute/Daily/Weekly) — live subscribe + historical request_bars | ✅ |
| Tick bars (TickBar, period=N) — live subscribe via `BarSubType::Regular` + `type_specifier=N` | ✅ |
| `GatewayConfig::from_env ENABLE_HISTORY` — fixed hardcoded false | ✅ |
| Live tests passing — 62/62 including bar subscribe/unsubscribe | ✅ |
