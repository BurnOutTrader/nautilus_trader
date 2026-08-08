# ProjectX

This fork provides ProjectX support through an independent external adapter for NautilusTrader with
a Rust-native backend and thin PyO3 wrappers. The adapter supports live market data, live
execution, historical bar requests, and reconciliation with reconnect handling.

> [!IMPORTANT]
>
> This is an independent external adapter. It is not affiliated with, endorsed by, or supported by
> Nautech Systems Pty Ltd or the official NautilusTrader project.
>
> For official ProjectX product and API information, see the
> [Gateway docs](https://gateway.docs.projectx.com/docs/intro) and
> [API reference](https://gateway.docs.projectx.com/docs/category/api-reference).

> [!WARNING]
>
> **Beta status.** The current ProjectX adapter is beta software. Adapter-local implementation for
> the current Nautilus scope is complete, but live operation still depends on ProjectX / Topstep
> account readiness, entitlements, and upstream gateway behavior. Use it cautiously for live
> trading.

> [!NOTE]
>
> ProjectX support in this adapter is currently pinned to Topstep. Sim flows may validate cleanly
> while live contract discovery or account registration still fails when live entitlements or
> account state are unavailable. Run the probe and validation helpers before relying on a live
> deployment.

> [!NOTE]
>
> ProjectX session tokens are valid for 24 hours, and the gateway enforces authenticated request
> rate limits, including 50 `/api/History/retrieveBars` requests per 30 seconds and 200 other
> requests per 60 seconds. Long-running clients should expect token revalidation and backoff on
> `429` responses. See the official [validate session](https://gateway.docs.projectx.com/docs/getting-started/validate-session/)
> and [rate limits](https://gateway.docs.projectx.com/docs/getting-started/rate-limits) docs.

> [!NOTE]
>
> Adapter implementation tracking lives alongside the adapter source:
> [dev plan](../../crates/adapters/projectx/devplan.md) and
> [completed work](../../crates/adapters/projectx/completed.md).

> [!NOTE]
>
> From the repository root, sync the Python project into the same root virtual environment used by
> the extension build:
>
> ```bash
> UV_PROJECT_ENVIRONMENT="$PWD/.venv" \
>   uv sync --project python --all-groups --all-extras --inexact \
>   --no-install-package nautilus_trader
> ```
>
> After that, run adapter-local checks with
> `UV_PROJECT_ENVIRONMENT="$PWD/.venv" uv run --project python --no-sync ...` so pytest and Ruff use
> the same dependency set as the project workflows.

## Overview

The ProjectX adapter includes:

- `ProjectXHttpClient`: low-level authenticated HTTP transport.
- `ProjectXDataClientFactory`: Rust-native live data client factory for `LiveNode`.
- `ProjectXExecutionClientFactory`: live execution client factory for `LiveNode`.
- `ProjectXDataClientConfig` and `ProjectXExecClientConfig`: Rust/PyO3 client configs for the live runtime path.
- `download_bars_to_catalog(...)` and `download_bars_to_catalog_async(...)`: typed historical
  download helpers backed by `ProjectXHttpClient` and `ParquetDataCatalog`.

The adapter package re-exports the generic config and factory names directly from the compiled
PyO3 module. They are the live-runtime contract.
The raw SignalR client is deliberately Rust-only: live clients own and continuously drain its
event stream so Python callers cannot create an unconsumed market-data queue.

## Examples

### Rust Examples

- Pure Rust historical bar download example: [`crates/adapters/projectx/examples/download_bars.rs`](../../crates/adapters/projectx/examples/download_bars.rs)
- Pure Rust backtest strategy example: [`crates/adapters/projectx/examples/backtest_bar_strategy.rs`](../../crates/adapters/projectx/examples/backtest_bar_strategy.rs)
- Pure Rust backtest EMA-cross example: [`crates/adapters/projectx/examples/backtest_ema_cross.rs`](../../crates/adapters/projectx/examples/backtest_ema_cross.rs)
- Pure Rust live data strategy example: [`crates/adapters/projectx/examples/node_quote_probe.rs`](../../crates/adapters/projectx/examples/node_quote_probe.rs)
- Pure Rust live exec strategy example: [`crates/adapters/projectx/examples/node_exec_tester.rs`](../../crates/adapters/projectx/examples/node_exec_tester.rs)
- Pure Rust live EMA-cross example: [`crates/adapters/projectx/examples/node_ema_cross.rs`](../../crates/adapters/projectx/examples/node_ema_cross.rs)

### Python Examples

- Backtest catalog download helper: `examples/backtest/projectx/projectx_download_bars.py`
- Backtest high-level API helper: `examples/backtest/projectx/projectx_backtest_high_level.py`
- Notebook (historical bars to Parquet): `examples/live/projectx/notebooks/projectx_historical_bars_to_parquet.py`
- Live exec tester: `examples/live/projectx/projectx_exec_tester.py` (PyO3 importable strategy)
- Live EMA-cross example with historical warmup: `examples/live/projectx/projectx_ema_cross.py`

### Python Helpers

- Live market-data probe helper: [`examples/live/projectx/projectx_live_data_probe.py`](../../examples/live/projectx/projectx_live_data_probe.py)
- Live order-book probe helper: [`examples/live/projectx/projectx_orderbook_probe.py`](../../examples/live/projectx/projectx_orderbook_probe.py)
- Live market-data catalog capture helper: [`examples/live/projectx/projectx_live_data_capture.py`](../../examples/live/projectx/projectx_live_data_capture.py)
- Live validation harness: `examples/live/projectx/projectx_validation_runner.py`
- Front-month resolver helper: `examples/live/projectx/projectx_front_month_resolver.py`
- Instrument snapshot export helper: `examples/live/projectx/projectx_instrument_snapshot.py`
- Instrument provider helper: `examples/live/projectx/projectx_instrument_provider.py`
- Contract parity check helper: `examples/live/projectx/projectx_contract_parity_check.py`

Companion workflow guide:

- [ProjectX data workflows](projectx_data_workflows.md) covers historical downloads, live probes,
  live order-book inspection, live capture, and the exact Nautilus object types written to the
  catalog.

ProjectX API documentation:

- [Gateway docs](https://gateway.docs.projectx.com/docs/intro)
- [API reference](https://gateway.docs.projectx.com/docs/category/api-reference)

## Credentials

Set credentials through environment variables:

```bash
export PROJECTX_USERNAME="your_username"
export PROJECTX_API_KEY="your_api_key"
```

These are consumed automatically by the ProjectX config constructors when `user_name`/`api_key`
are omitted.

You can also call `load_projectx_env()` (Rust/PyO3-backed) to load these two keys from a local
`.env` file without manually exporting them first:

```python
from nautilus_trader.adapters.projectx import load_projectx_env

load_projectx_env()  # defaults to ".env"
```

## Environment

ProjectX is pinned to Topstep in this adapter, so examples/configs do not need an environment
parameter.

## Front-month helper

`await ProjectXHttpClient.resolve_front_month_instrument(product_root, live=False)` resolves a
front-month Nautilus futures instrument from the ProjectX available-contract set for a given
product root, preferring `activeContract=true` entries and then nearest expiry when multiple
contracts match.

The call returns a typed `FuturesContract` and raises when no matching contract is available. See
`examples/live/projectx/projectx_front_month_resolver.py` for a JSON-friendly reporting wrapper.

## Instrument snapshot helper

`await ProjectXHttpClient.available_instruments(live=False, active_only=False, product_root=None)`
returns typed Nautilus futures instruments. Their `id`, price increment, and `info` metadata expose
the canonical instrument ID and raw provider metadata. The snapshot example writes JSON fields such
as:

- `id` (for example `MESM26.PROJECTX`)
- `publicSymbol`
- `name`
- `symbolId`
- `tickSize`
- `tickValue`
- `activeContract`

The raw contract ID remains available as `instrument.info["projectx_contract_id"]`. The
`projectx_instrument_snapshot.py` example converts the typed objects to JSON and writes the snapshot
to disk for reuse in live configuration and research workflows.

This JSON snapshot helper is supplemental only. Canonical Nautilus persistence for ProjectX
instruments should be the normal catalog/instrument path used by the backtest and live capture
helpers.

For ProjectX futures, strategies, configs, downloads, and serialized Nautilus instruments should
use the canonical ProjectX instrument format `MNQM26.PROJECTX`.

When ProjectX exposes both a short vendor `name` such as `MNQM6` and a more precise contract id
such as `CON.F.US.MNQ.M26`, the adapter canonicalizes the Nautilus-facing instrument to
`MNQM26.PROJECTX`. Vendor aliases are still accepted on adapter ingress for subscription lookup and
execution reconciliation, but everything entering or leaving Nautilus persistence uses the
canonical `MNQM26.PROJECTX` convention.

### Cross-feed symbol helpers

When you drive execution from ProjectX but source market data from Databento or Rithmic, use the
adapter-local symbol helpers rather than hand-rolling month/year conversion:

```python
from nautilus_trader.adapters.projectx import databento_to_projectx_adapter_symbol
from nautilus_trader.adapters.projectx import databento_to_projectx_contract_id
from nautilus_trader.adapters.projectx import projectx_to_rithmic_symbol
from nautilus_trader.adapters.projectx import rithmic_to_projectx_symbol


databento_to_projectx_adapter_symbol("MNQM26")  # "MNQM26"
databento_to_projectx_contract_id("MNQM26")     # "CON.F.US.MNQ.M26"
projectx_to_rithmic_symbol("CON.F.US.MNQ.M26")  # "MNQM6"
rithmic_to_projectx_symbol("MNQM6")             # "MNQM26"
```

Notes:

- `*_projectx_adapter_symbol(...)` returns the canonical Nautilus / adapter symbol such as
  `MNQM26`, which is what you should use in `InstrumentId(..., PROJECTX)` and adapter-facing live
  configs.
- `databento_to_projectx_contract_id(...)` returns the raw ProjectX contract id used by low-level
  venue APIs.
- Conversions from one-digit vendor expiries such as `MNQM6` to two-digit symbols such as
  `MNQM26` resolve the decade against the current UTC year by default. For deterministic tests or
  historical tooling, use `projectx_to_databento_symbol_with_year(...)` or
  `rithmic_to_projectx_symbol_with_year(...)`.

## Backtest catalog workflow

ProjectX historical backtests follow the standard Nautilus high-level path:

1. Download an instrument definition and historical external bars into a Parquet catalog.
2. Run `BacktestNode` against that catalog with a normal `BacktestRunConfig`.

The helper scripts above demonstrate the full flow:

- `examples/backtest/projectx/projectx_download_bars.py`
  uses `download_bars_to_catalog(...)`, resolves the exact instrument through
  `ProjectXHttpClient`, requests historical bars, and writes both through the typed
  `ParquetDataCatalog` instrument/bar methods to `<NAUTILUS_PATH>/catalog`
- `examples/backtest/projectx/projectx_backtest_high_level.py`
  reads the resulting catalog and runs a standard `BacktestNode` EMA-cross configuration

The shipped download script is configured by editing module-level constants rather than
example-specific environment variables. Its default request window is the previous UTC trading
week, from Monday `00:00` through Friday `23:59`.

Live-history fallback to `live=false` is explicit in both the Python and Rust
download helpers. Leave it disabled for normal production backfills; enable it
only when you intentionally want the helper to retry a rejected live-history
request against the sim history source. It is a download-helper fallback, not a
general live-runtime recovery path.

For the current helper settings and the exact Nautilus objects written to the catalog, see
[ProjectX data workflows](projectx_data_workflows.md).

## Pure Rust strategy examples

ProjectX also supports the Rust-native v2 path directly, without PyO3 strategy code:

- [`crates/adapters/projectx/examples/download_bars.rs`](../../crates/adapters/projectx/examples/download_bars.rs)
  - self-contained Rust historical downloader for ProjectX instruments plus external bars
  - writes directly into a local `ParquetDataCatalog` using the adapter-local helper path
- `crates/adapters/projectx/examples/backtest_bar_strategy.rs`
  - self-contained Rust `BacktestNode` example
  - writes a synthetic ProjectX futures instrument plus external bars into a temporary catalog
  - runs a Rust strategy which subscribes to ProjectX bars through the normal backtest engine
- [`crates/adapters/projectx/examples/backtest_ema_cross.rs`](../../crates/adapters/projectx/examples/backtest_ema_cross.rs)
  - reads ProjectX catalog data and runs the reusable Rust EMA-cross bar strategy through `BacktestNode`
  - intended as the Rust equivalent of the high-level Python backtest EMA example
- `crates/adapters/projectx/examples/node_quote_probe.rs`
  - Rust `LiveNode` example using `ProjectXDataClientFactory`
  - resolves the instrument from `PROJECTX_INSTRUMENT_ID` or the ProjectX front-month contract set
  - runs a Rust strategy which requests the instrument and subscribes to quotes, trades, and depth
- `crates/adapters/projectx/examples/node_exec_tester.rs`
  - Rust `LiveNode` example using both `ProjectXDataClientFactory` and
    `ProjectXExecutionClientFactory`
  - runs the Rust `ExecTester` strategy directly against ProjectX
  - defaults to `dry_run=true` unless `PROJECTX_EXEC_DRY_RUN=false` is set explicitly
- [`crates/adapters/projectx/examples/node_ema_cross.rs`](../../crates/adapters/projectx/examples/node_ema_cross.rs)
  - Rust `LiveNode` example using ProjectX data and execution client factories together
  - warms from catalog bars, then trades the reusable Rust EMA-cross strategy live with internal bars

Repository proof coverage for this path lives in:

- `crates/adapters/projectx/tests/rust_strategy_paths.rs`

That test coverage verifies:

- a pure Rust strategy can be registered on a `LiveNode` built with `ProjectXDataClientFactory`
- a pure Rust execution strategy can be registered on a `LiveNode` built with
  `ProjectXExecutionClientFactory`
- a pure Rust bar strategy can run through `BacktestNode` against ProjectX-formatted catalog data

## Live market-data probe workflow

[`examples/live/projectx/projectx_live_data_probe.py`](../../examples/live/projectx/projectx_live_data_probe.py)
is the supported operational smoke/soak helper for the current ProjectX v2 runtime path:

1. It uses the module-level `INSTRUMENT_ID` constant in the script to choose the contract.
2. It starts a Rust-native `LiveNode` with `ProjectXDataClientFactory`.
3. A small PyO3 importable strategy requests the instrument and subscribes to quotes, trades, and
   book deltas.
4. The helper reports instrument readiness, observed event counts, and reconnect lifecycle markers.

ProjectX live depth behavior depends on the feed entitlement and the stream semantics actually
returned by the gateway:

- if the account only receives top-of-book updates, the adapter treats `BestBid`, `BestAsk`,
  `NewBestBid`, and `NewBestAsk` events as replaceable best-price slots instead of accumulating a
  stale ladder
- if the account receives indexed market-depth updates, the adapter reconstructs a deeper ladder
  from those indexed levels automatically
- the requested `DEPTH_LEVELS` constant still caps how many levels Nautilus asks the gateway to
  send, so buying deeper depth may also require increasing that example setting

[`examples/live/projectx/projectx_orderbook_probe.py`](../../examples/live/projectx/projectx_orderbook_probe.py)
is the easiest way to inspect which mode you are actually receiving. It can print either raw
deltas or a managed book built from those deltas.

If you want to resolve a front month before running the probe, use the separate
`projectx_front_month_resolver.py` or provider helper flows first and then update the probe
script's `INSTRUMENT_ID`.

If the configured instrument never becomes available, the helper now prints `Status: timeout` with
the lifecycle/error context instead of raising a raw traceback. Set `FAIL_ON_TIMEOUT = True` if
you want the CLI to exit non-zero on that condition.

Edit the module-level constants in `projectx_live_data_probe.py` for:

- `INSTRUMENT_ID`
- `MARKET_DATA_LIVE`
- `CAPTURE_SECONDS`
- `READY_TIMEOUT_SECONDS`
- `FIRST_DATA_WAIT_SECONDS`
- `DEPTH_LEVELS`
- `CAPTURE_QUOTES`
- `CAPTURE_TRADES`
- `CAPTURE_DEPTH`
- `UNSUBSCRIBE_ON_STOP`
- `LOG_DATA`
- `FAIL_ON_TIMEOUT`

For the current helper settings and the exact data semantics, see
[ProjectX data workflows](projectx_data_workflows.md).

## Live catalog capture workflow

[`examples/live/projectx/projectx_live_data_capture.py`](../../examples/live/projectx/projectx_live_data_capture.py)
captures ProjectX live market data into a Parquet catalog through the supported `LiveNode` / v2
PyO3 runtime path:

1. It uses the module-level `INSTRUMENT_ID` constant in the script to choose the contract.
2. It starts a Rust-native `LiveNode` with `ProjectXDataClientFactory`.
3. A small PyO3 importable strategy requests the instrument and subscribes to quotes, trades, and
   book deltas.
4. After the timed capture window, the helper writes the captured instrument, quotes, trades, and
   book deltas into `CATALOG_PATH`, which defaults to `<NAUTILUS_PATH>/catalog`.
5. The helper prints both in-process event counts and catalog-count deltas after the capture
   window ends.

The catalog stores Nautilus-normalized objects from the strategy callbacks, not raw ProjectX HTTP
or SignalR payloads. In practice this means:

- quotes are stored as Nautilus `QuoteTick`
- trades are stored as Nautilus `TradeTick`
- depth is stored as Nautilus `OrderBookDelta` rows derived from the adapter mapping
- replaying that catalog in a backtest reuses the captured Nautilus interpretation of the feed; it
  does not reparse raw ProjectX venue messages later

If the instrument never becomes available, the helper prints `Status: timeout`, leaves the catalog
unchanged, and exits cleanly when `FAIL_ON_TIMEOUT` is `False`.

This keeps ProjectX catalog capture on the same Rust-backed client behavior used by production
`LiveNode`, instead of introducing a separate Python `TradingNode` wrapper stack just for catalog
persistence.

Edit the module-level constants in `projectx_live_data_capture.py` for:

- `CATALOG_PATH`
- `INSTRUMENT_ID`
- `MARKET_DATA_LIVE`
- `CAPTURE_SECONDS`
- `READY_TIMEOUT_SECONDS`
- `FIRST_DATA_WAIT_SECONDS`
- `DEPTH_LEVELS`
- `CAPTURE_QUOTES`
- `CAPTURE_TRADES`
- `CAPTURE_DEPTH`
- `UNSUBSCRIBE_ON_STOP`
- `LOG_DATA`
- `FAIL_ON_TIMEOUT`

For the current helper settings and the exact Nautilus objects written to the catalog, see
[ProjectX data workflows](projectx_data_workflows.md).

## Validation harness

`examples/live/projectx/projectx_validation_runner.py` orchestrates the remaining operational
ProjectX checks into a single JSON report:

1. front-month resolution
2. provider load / available-instruments coverage
3. contract parity against `searchById`
4. optional repeated reconnect/probe iterations using the `LiveNode` data probe helper

The runner supports sim and live modes independently and writes a consolidated report for archive
and handoff.

Operational note:

- if `PROJECTX_MARKET_DATA_LIVE=true` returns zero contracts from both the front-month and provider
  helpers, treat that as a live entitlement/feed-availability problem first. The probe helper will
  then typically time out waiting for the instrument request because the live contract catalog is
  empty.
- when no instrument can be resolved from the provider/front-month helpers and
  `PROJECTX_INSTRUMENT_ID` is not set, the reconnect soak is skipped rather than falling back to a
  stale hard-coded contract.

Edit the module-level constants in `projectx_validation_runner.py` for:

- `OUTPUT_PATH`
- `PRODUCT_ROOT`
- `INSTRUMENT_ID`
- `ACTIVE_ONLY`
- `CONTRACT_PARITY_LIMIT`
- `RUN_SIM`
- `RUN_LIVE`
- `RUN_FRONT_MONTH`
- `RUN_PROVIDER`
- `RUN_PARITY`
- `RUN_SOAK`
- `SOAK_ITERATIONS`
- `SOAK_CAPTURE_SECONDS`
- `SOAK_SLEEP_SECONDS`
- `SOAK_READY_TIMEOUT_SECONDS`
- `SOAK_FIRST_DATA_WAIT_SECONDS`
- `SOAK_DEPTH_LEVELS`
- `SOAK_QUOTES`
- `SOAK_TRADES`
- `SOAK_DEPTH`
- `SOAK_UNSUBSCRIBE_ON_STOP`
- `SOAK_LOG_DATA`

## Instrument provider helper

`ProjectXHttpClient.available_instruments(live=False, active_only=False, product_root=None)`
returns PyO3 Nautilus instrument objects built from ProjectX available-contract metadata.

`ProjectXInstrumentProvider` is a small async wrapper with `load_all_async(filters=...)` and
`get_all()` surfaces.

The provider accepts filters:

- `product_root` (for example `MES`)
- `active_only` (`True` to include only active contracts)

## Contract parity check helper

`projectx_contract_parity_check.py` compares `/api/Contract/available` contracts against
`/api/Contract/searchById` for the same IDs and reports payload mismatches on:

- `name`
- `symbolId`
- `tickSize`
- `activeContract`

Edit the module-level constants in `projectx_contract_parity_check.py`:

- `LIVE`
- `LIMIT`
- `PRODUCT_ROOT`
- `OUTPUT_PATH`

## Product support

| Product Type      | Data Feed | Trading | Notes                                 |
| ----------------- | --------- | ------- | ------------------------------------- |
| Futures contracts | ✓         | ✓       | Contract symbols map to `*.PROJECTX`. |

## Data capability

### Subscriptions (real-time)

| Data Type         | Supported | Notes                                                                                                                   |
| ----------------- | --------- | ----------------------------------------------------------------------------------------------------------------------- |
| `QuoteTick`       | ✓         | `SubscribeContractQuotes`                                                                                               |
| `TradeTick`       | ✓         | `SubscribeContractTrades`                                                                                               |
| `OrderBookDeltas` | ✓         | `SubscribeContractMarketDepth`                                                                                          |
| `Bar`             | -         | Venue‑native streaming bars are not currently exposed by ProjectX WebSocket; use INTERNAL bars for live bar strategies. |

### Requests (historical)

| Data Type | Supported | Notes                                |
| --------- | --------- | ------------------------------------ |
| `Bar`     | ✓         | Maps to `/api/History/retrieveBars`. |

Bar requests are built from standard Nautilus `BarType` values. The adapter derives the ProjectX
request shape from that bar type, so normal `request_bars(...)` calls do not need venue-specific
params.

For live bar strategies, the supported pattern is to warm up with `request_bars(...)` and then
trade on matching `-INTERNAL` bars so Nautilus can aggregate the live ProjectX trade stream into
bars after startup.

## Orders capability

| Capability                        | Supported | Notes                                                                                    |
| --------------------------------- | --------- | ---------------------------------------------------------------------------------------- |
| Submit `MARKET` / `LIMIT`         | ✓         | Quantity must be whole contracts; GTC only.                                              |
| Submit `STOP_MARKET`              | ✓         | Requires a trigger price; GTC only.                                                      |
| Submit stop-limit/trailing orders | -         | Rejected before `OrderSubmitted`; upstream v2 placement cannot preserve these semantics. |
| Modify order                      | ✓         | Via ProjectX modify endpoint.                                                            |
| Cancel order                      | ✓         | Via ProjectX cancel endpoint.                                                            |
| Reconciliation                    | ✓         | Startup + reconnect with monotonic diff emission.                                        |
| Position close surface            | ✓         | Use standard `close_position(...)` / `close_all_positions(...)`.                         |

## Live node setup

ProjectX currently follows the Rust-native live-node path. Use `LiveNode` / `LiveNodeBuilder`
with instantiated ProjectX factory objects rather than the pure-Python `TradingNode`
`add_*_client_factory(...)` path.
ProjectX is pinned to Topstep, so no environment value is required in config.

```python
from nautilus_trader.adapters.projectx import ProjectXDataClientConfig
from nautilus_trader.adapters.projectx import ProjectXDataClientFactory
from nautilus_trader.adapters.projectx import ProjectXExecClientConfig
from nautilus_trader.adapters.projectx import ProjectXExecutionClientFactory
from nautilus_trader.common import Environment
from nautilus_trader.live import LiveNode
from nautilus_trader.model import AccountType
from nautilus_trader.model import TraderId

trader_id = TraderId("TRADER-001")

node = (
    LiveNode.builder("TRADER-001", trader_id, Environment.LIVE)
    .add_data_client(
        None,
        ProjectXDataClientFactory(),
        ProjectXDataClientConfig(
            user_name=None,  # Uses PROJECTX_USERNAME
            api_key=None,    # Uses PROJECTX_API_KEY
            market_data_live=False,
        ),
    )
    .add_exec_client(
        None,
        ProjectXExecutionClientFactory(),
        ProjectXExecClientConfig(
            trader_id=trader_id.value,
            account_id="PROJECTX-12345",
            user_name=None,  # Uses PROJECTX_USERNAME
            api_key=None,    # Uses PROJECTX_API_KEY
            account_type=AccountType.MARGIN,
        ),
    )
    .build()
)
```

### Account selection

ProjectX execution subscribes and reconciles only the configured `account_id` for the current
login. This keeps the live adapter aligned with the example flow and avoids pulling unrelated
orders or positions from other Topstep accounts under the same credentials.

You can pass the raw Topstep account label such as `PRAC-V2-EXAMPLE-ACCOUNT`. The adapter
canonicalizes it internally to a venue-scoped Nautilus account ID with issuer `PROJECTX`.

If `/api/Account/search` returns multiple accounts and the configured `account_id` does not match
one of them, the client fails fast during connect instead of silently choosing another account.

If you need a one-off override on an execution command under a multi-account login, use generic
command params such as `account_id="PROJECTX-12345"` or `account_id_num=12345`.

The PyO3 exec tester also uses the configured `account_id` to perform startup cleanup at the
account scope for its instrument. If the demo account already has open ProjectX orders or
positions for that contract, the example waits for that exposure to clear before submitting a new
entry order.

The exec tester is now a bounded smoke test rather than a manual `Ctrl-C` flow. It starts the
`LiveNode`, waits for either a terminal order outcome or a startup/timeout failure, then stops and
prints a `Status` summary.

During Ctrl-C / stop, the example skips redundant cancel attempts for orders whose submission is
still in flight. This reduces cancel-rejected shutdown noise while venue state is resolving.

Relevant execution-smoke environment variables:

- `PROJECTX_EXEC_READY_TIMEOUT`
- `PROJECTX_EXEC_TIMEOUT_SECONDS`
- `PROJECTX_EXEC_FLATTEN_SETTLE_SECONDS`

## Account type guidance

ProjectX accounts should be modeled as USD margin accounts in Nautilus.

ProjectX account snapshots currently expose balance but not detailed margin/free-margin fields, so
the adapter emits a USD margin account with empty margin balances until the upstream API exposes
those fields explicitly.

## Position closing

Use the standard Nautilus position-close APIs for live/backtest parity:

```python
from nautilus_trader.model import TimeInForce

# Close a single position
self.close_position(
    position=position,
    client_id=client_id,
    time_in_force=TimeInForce.GTC,
    reduce_only=False,
)

# Close all open positions for an instrument
self.close_all_positions(
    instrument_id=instrument_id,
    client_id=client_id,
    time_in_force=TimeInForce.GTC,
    reduce_only=False,
)
```

For a partial reduction, submit a standard market order with the desired reduction size against the
existing `position_id`.

ProjectX v2 does not expose time-in-force or reduce-only fields on its placement request. The
adapter therefore supports GTC orders only and denies reduce-only requests before emitting
`OrderSubmitted`. Position-close helpers use an ordinary opposing market order and callers must
account for the resulting race risk if the position can change concurrently.

## Historical bars to Parquet

Use the ProjectX download helpers for the supported historical-catalog flow:

1. `examples/backtest/projectx/projectx_download_bars.py`
   uses the async-capable ProjectX HTTP helper and typed `ParquetDataCatalog.write_instruments` /
   `write_bars` methods to store the instrument definition and historical bars.
2. `examples/live/projectx/notebooks/projectx_historical_bars_to_parquet.py`
   shows the same bar-request path from a research notebook.

Both examples use the standard catalog layout: set `NAUTILUS_PATH` to the parent workspace and the
ProjectX catalog will live at `<NAUTILUS_PATH>/catalog`.

For the current helper configuration and the exact catalog object types, see
[ProjectX data workflows](projectx_data_workflows.md).

The download helper's `market_data_live=...` argument controls the ProjectX contract and history
source. It is explicit and independent of the `ProjectXDataClientConfig` used by a live node.

Downloaded ProjectX instruments are serialized into the catalog using the same canonical Nautilus
symbology used by strategies, for example `MNQM26.PROJECTX` with raw symbol `MNQM26`.

## Backtesting with downloaded ProjectX bars

The ProjectX adapter is a live adapter. A typical research workflow is:

1. Download instruments and bars with `download_bars_to_catalog(...)` (or its async variant).
2. Run backtests from the resulting Parquet catalog.

```python
from nautilus_trader.backtest import BacktestDataConfig
from nautilus_trader.model import BarType
from nautilus_trader.model import InstrumentId


instrument_id = InstrumentId.from_str("MESM26.PROJECTX")
bar_type = BarType.from_str(f"{instrument_id}-1-MINUTE-LAST-EXTERNAL")
data_config = BacktestDataConfig(
    catalog_path="./catalog_projectx",
    data_type="Bar",
    instrument_id=instrument_id,
    bar_spec=bar_type.spec,
    start_time=1_775_192_400_000_000_000,
    end_time=1_775_193_000_000_000_000,
)
```
