# Rithmic Adapter Beta Decision Record

Reference docs:

<https://nautilustrader.io/docs/latest/developer_guide/adapters/>

- <https://nautilustrader.io/docs/nightly/concepts/rust/#system-implementations>
- <https://nautilustrader.io/docs/nightly/developer_guide/ffi/>em-implementations>
- <https://nautilustrader.io/docs/nightly/developer_guide/ffi/>

**Guide:** See the public [Rithmic integration guide](../../../docs/integrations/rithmic.md).

**Completed work:** See [completed.md](completed.md) for the implementation record.

## Beta Release Status

Adapter-local beta hardening for the Rithmic adapter is complete in this repository.

There are **no remaining adapter-local code blockers** in `crates/adapters/rithmic/`,
`nautilus_trader/adapters/rithmic/`, or the adapter-local tests/docs that should block a stable
beta / production pilot run.

This file is no longer an active implementation backlog. It records the shipped support contract,
the final design decisions, and the non-blocking boundaries for this beta.

As of 2026-04-13, no additional adapter-local implementation work is planned beyond routine
maintenance, regression fixes, and compatibility upkeep.

## Final Decisions

### Runtime support

- both runtime paths remain supported:
  - Rust `LiveNode` / registry / v2 PyO3 path
  - Python `TradingNode` wrapper path
- the adapter is considered sufficiently aligned with the Nautilus v2 Rust guide:
  venue behavior lives in Rust, and the Python path is a thin wrapper over the Rust/PyO3 surface
- no Nautilus execution-engine special-casing is part of the shipped beta contract for Rithmic

### Live topology

- one shared gateway is keyed by the upstream Rithmic login/session identity
- the shared-gateway key includes:
  - environment
  - username
  - password
  - system name
  - app name and app version
  - `fcm_id`
  - `ib_id`
  - named server / alternate server
  - URL overrides
- the shared-gateway key intentionally excludes `account_id` so multiple execution accounts can
  share one login/system gateway
- the current `LazyLock<Mutex<HashMap<...>>>` registry is acceptable for beta; no `DashMap`
  migration is required for the shipped adapter

### Routing identity contract

- venue remains `RITHMIC`
- data `ClientId` is derived from the normalized `system_name`
- execution `ClientId` is derived from normalized `system_name + account_id`
- the adapter-local `AccountId` is `"{exec_client_id}-{venue_account_id}"`
- explicit `client_id` routing is the supported path for every non-default same-venue command:
  - market-data subscriptions
  - data requests
  - order submission / modify / cancel flows
  - report/query operations
- the Python `TradingNode` path should use distinct client config keys without hyphens

### Multi-account and multi-login support

- multiple accounts under one Rithmic login/system are supported as a **live-only** feature for
  explicit copy-trading or multi-account routing
- multiple different Rithmic systems/logins are supported in one Nautilus node through distinct
  profiles/client IDs and explicit routing
- separate Nautilus processes are an operational choice for stronger isolation, not an adapter
  requirement for distinct Rithmic systems/logins
- multi-account same-venue routing is **not** a backtest feature in this beta contract

See [multi_login_plan.md](multi_login_plan.md) for the concrete shipped routing contract and
operator guidance.

### Native order-list and bracket scope

- supported high-level live list shapes are limited to:
  - native OCO lists
  - three-order limit-entry brackets
- market-entry brackets and broader list topologies remain intentionally unsupported for this beta
- the adapter will **not** approximate high-level market-entry bracket child prices from quote
  snapshots for beta; unsupported shapes should continue to reject explicitly
- `native_bracket_state_path` remains reserved for future persisted native
  bracket-state support; the current PyO3 execution paths reject it explicitly
  rather than silently ignoring it

### Dependency strategy

- continue extending the forked `BurnOutTrader/rithmic-rs` dependency on `main`
- do **not** revive or expand a second internal `rithmic-rs` implementation in this repository
- the vendored `vendor/rithmic-rs` tree is not the active customization path for this beta
- richer raw `RequestBracketOrder` coverage in the fork is a non-blocking enhancement, not a beta
  blocker for this adapter release

See [rithmic_rs_bracket_extension.md](rithmic_rs_bracket_extension.md) for the bracket-specific
fork decision.

## Maintenance-Only Status

No additional adapter-local implementation work is planned at this time beyond maintenance. The
items below are retained only as archived possibilities if priorities change in the future:

- broader live-routing changes for multiple execution clients on one venue in Nautilus core
  (`beads-planning-nfe`)
- richer fork-level `RequestBracketOrder` helper APIs if we later choose to expose more native
  conditional bracket shapes
- upstreaming forked `rithmic-rs` changes to another upstream is optional and not part of this
  beta release decision
- `BarAggregation::Volume` / `VolumeBar` support
- broader high-level order-list translation beyond the current native OCO / limit-entry subset
- a larger catalog of live-captured deterministic fixtures if broader parser/backtest cases need
  it later
- committed raw transport samples for Rithmic protobuf payloads should be added
  in a future pass so every adapter-handled message family can be replay-tested
  from sanitized fixtures in `test_data/`
- any future approximation policy for market-entry bracket translation

## Scope Ownership

For this adapter branch, changes should stay confined to:

- `crates/adapters/rithmic/src/`
- `nautilus_trader/adapters/rithmic/`
- `tests/integration_tests/adapters/rithmic/`
- `docs/integrations/rithmic.md`
- adapter-local planning notes such as:
  - [multi_login_plan.md](multi_login_plan.md)
  - [rithmic_rs_bracket_extension.md](rithmic_rs_bracket_extension.md)

## Licensing And Attribution

For repo-owned Rithmic adapter code in this repository:

- do not add or retain Nautech Systems / NautilusTrader copyright banners
- do not copy `rithmic-rs`, Rithmic, ProjectX, or other third-party license headers into
  adapter-owned files
- use the repository's current adapter-local notice and attribution for new or rewritten
  Rithmic-owned files
- preserve third-party notices only in vendored code, direct upstream imports, or files where the
  original license text must legally remain attached

Practical rule:

- if a file lives in our adapter-owned code, it should not carry someone else's header block
- if a file is vendored or directly imported from upstream, keep the upstream notice and keep that
  file clearly separated from adapter-owned code

## Notes

- `nautilus_trader.adapters.rithmic` exports the Python `TradingNode` wrapper factories, while the
  raw PyO3 factories remain available from `nautilus_trader.adapters.rithmic.bindings`
- `BarSubType::Regular` + `type_specifier=N` remains the correct Rithmic n-tick API
- Rithmic remains a protobuf-based binary protocol through `rithmic-rs`
