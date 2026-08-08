# Rithmic Multi-Login / Multi-Account Beta Contract

This note records the **shipped adapter contract** for running multiple Rithmic sessions or
accounts without Nautilus engine changes.

It replaces the earlier phased implementation roadmap. For the current beta, the design questions
below are considered resolved.

## Final Decisions

- keep the venue fixed at `RITHMIC`
- do not modify Nautilus core routing semantics for this adapter branch
- use explicit `client_id` routing for every non-default same-venue data or execution command
- use one shared gateway per upstream Rithmic login/system identity
- allow multiple execution accounts under one login/system by sharing that gateway and routing by
  account-scoped execution `ClientId`
- keep multi-account support scoped to **live** operation, not backtesting

## Supported Topologies

### One login/system, multiple accounts

Supported for live copy-trading or explicit multi-account routing.

Contract:

- one gateway per login/system
- one upstream order plant and one upstream PnL plant per shared gateway
- multiple account-scoped execution clients may attach to that gateway
- each execution client is bound to one venue `account_id`
- execution and PnL fan-out are filtered by venue account before downstream emission

### Multiple different systems/logins in one Nautilus node

Supported.

Contract:

- each distinct login/system gets its own gateway/session identity
- one node may host multiple Rithmic profiles/logins concurrently
- one node may use one Rithmic data profile and multiple Rithmic execution profiles
- when more than one Rithmic client is configured, non-default data and execution commands must be
  routed explicitly by `client_id`

Separate Nautilus processes are optional for isolation, but they are not required by the adapter.

## Identity Model

The adapter treats the normalized Rithmic `system_name` as the shared session identity and layers
account-scoped execution identities above it.

Example:

- Rithmic `system_name`: `Apex`
- normalized shared session/data client key: `APEX`
- venue account: `PA-123456`
- execution client ID: `APEX_PA_123456`
- adapter-local Nautilus account ID: `APEX_PA_123456-PA-123456`

Venue-facing identities remain unchanged:

- venue remains `Venue("RITHMIC")`
- venue account ID sent to Rithmic remains the real venue account
- market-data and execution instruments remain on venue `RITHMIC`

## Shared Gateway Contract

The gateway registry key includes:

- environment
- username
- password
- system name
- app name and app version
- `fcm_id`
- `ib_id`
- server / alternate server
- URL overrides

The gateway registry key excludes:

- `account_id`

That exclusion is intentional so multiple execution accounts can share the same upstream
login/system session. `fcm_id` and `ib_id` are included so distinct broker/login contexts do not
collapse into one shared gateway accidentally.

For beta, the current shared registry implementation based on
`LazyLock<Mutex<HashMap<...>>>` is accepted as correct and sufficient.

## Operator Contract

### Configuration naming

- avoid hyphens in Python `TradingNode` config keys
- use distinct names for each configured client, for example:
  - `RITHMIC_APEX_DATA`
  - `RITHMIC_APEX_EXEC_1`
  - `RITHMIC_APEX_EXEC_2`
  - `RITHMIC_TOPSTEP_EXEC`

### Explicit routing

Explicit `client_id` is the supported path for:

- actor / strategy market-data subscriptions
- actor / strategy data requests
- order submission and lifecycle commands
- non-default same-venue execution routing generally

Exactly one Rithmic client may act as the implicit/default route in a node. Every additional
Rithmic client should be addressed explicitly.

### Backtest boundary

This multi-account routing model is a live-only feature. Backtests should continue to use the
single-account / single-client workflows already documented in the public Rithmic guide.

## Not Supported In This Contract

- automatic venue-only inference between multiple Rithmic execution clients on the same venue
- backtest parity for multiple accounts on one simulated `RITHMIC` venue/client
- cross-process coordination for multiple Nautilus nodes trading the same account
- adapter-side pseudo-venues or engine special-casing for Rithmic

## External Follow-Up

These are no longer open adapter design questions for the beta branch:

- broader Nautilus live-routing enhancement for same-venue multi-client inference belongs to
  `beads-planning-nfe`, outside this adapter
- richer native bracket entry helpers belong in the forked `rithmic-rs` dependency, not in a new
  internal protocol layer in this repository
