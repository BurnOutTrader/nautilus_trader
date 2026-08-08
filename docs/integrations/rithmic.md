# Rithmic

[Rithmic](https://www.rithmic.com) provides low-latency futures market data and order routing
across supported FCMs and exchanges. This fork provides Rithmic support through an independent
external adapter for NautilusTrader, covering live market data ingest, instrument loading,
historical bar requests, and live order execution.

> [!IMPORTANT]
>
> This is an independent external adapter. It is not affiliated with, endorsed by, or supported by
> Nautech Systems Pty Ltd or the official NautilusTrader project.
>
> For official Rithmic product and onboarding information, see
> [Rithmic APIs](https://www.rithmic.com/apis).
>
> Access beyond `Rithmic Test` (for example `Rithmic 01`, `Rithmic Paper Trading`, or any system
> with real market data) requires two things from Rithmic: you must pass conformance, and you must
> have a user ID for the specific system you want to log into successfully. For the simplest
> Nautilus-side conformance helper, see
> [`examples/live/rithmic/rithmic_conformance_keepalive.py`](../../examples/live/rithmic/rithmic_conformance_keepalive.py).

> [!WARNING]
>
> **Beta status.** The current Rithmic adapter is beta software. It has reached a minimally working state,
> but it remains under active testing and development and should be used cautiously for live trading.

> [!NOTE]
>
> Rithmic servers undergo scheduled maintenance on weekends and during exchange maintenance periods.
> During these periods connections might be terminated and it might not be possible to test or deploy live connections.

> [!NOTE]
>
> Adapter implementation tracking lives alongside the adapter source:
> [dev plan](../../crates/adapters/rithmic/devplan.md) and
> [completed work](../../crates/adapters/rithmic/completed.md).

> [!NOTE]
>
> Local Python validation for this adapter expects the workflow environment to
> be synced first:
> `uv sync --all-groups --all-extras --inexact --no-install-package nautilus_trader`.
> After that, run adapter-local checks with `uv run --no-sync ...` so pytest and
> Ruff see the same dependency set as the project workflows.

## Overview

This guide assumes a trader is setting up for both live market data feeds and trade execution.
The Rithmic adapter includes multiple components, which can be used together or separately
depending on the use case.

- `RithmicGateway`: Low-level gateway connectivity to the Rithmic plants.
- `RithmicInstrumentProvider`: Instrument parsing and loading functionality.
- `RithmicDataClient`: Low-level Rust market-data client.
- `RithmicExecutionClient`: Low-level Rust execution client.
- `RithmicLiveDataClient`: Rust Nautilus live data-client implementation.
- `RithmicLiveExecClient`: Rust Nautilus live execution-client implementation.
- `RithmicDataClientFactory` / `RithmicExecClientFactory`: Rust factories projected to Python
  through a thin PyO3 layer for `LiveNode`.

> [!NOTE]
>
> Most users will define a live trading node configuration and will not need to work with
> the lower-level components directly.

### Runtime paths

The adapter exposes three related paths:

- Pure Rust v2 `LiveNode` path:
  - `rithmic_nt::RithmicDataClientFactory`
  - `rithmic_nt::RithmicExecClientFactory`
  - `rithmic_nt::get_rithmic_data_client_id(...)`
  - `rithmic_nt::get_rithmic_exec_client_id(...)`
  - `rithmic_nt::get_rithmic_adapter_account_id(...)`
- Python v2 `LiveNode` path:
  - `nautilus_trader.adapters.rithmic.RithmicDataClientFactory`
  - `nautilus_trader.adapters.rithmic.RithmicExecClientFactory`
  - Rust `RithmicLiveDataClient` / `RithmicLiveExecClient` implementations behind the PyO3 factories
- Raw binding path:
  - `nautilus_trader.adapters.rithmic.RithmicGateway`
  - `nautilus_trader.adapters.rithmic.RithmicDataClient`
  - `nautilus_trader.adapters.rithmic.RithmicExecutionClient`
  - explicit low-level helper flows such as the native bracket / OCO examples

Venue-specific transport, state, reconnect, reconciliation, market-data, and report-generation
behavior lives in Rust. The pure Rust and PyO3 `LiveNode` paths use the same Rust live-client
implementations and therefore share the same capability surface. Python contains configuration,
backtest, and example orchestration helpers rather than a second adapter implementation.

The current runnable Python examples in `examples/live/rithmic/` primarily use the PyO3
`LiveNode` path.

> [!NOTE]
>
> `nautilus_trader.adapters.rithmic.RithmicDataClientFactory` and
> `nautilus_trader.adapters.rithmic.RithmicExecClientFactory` require a build whose compiled PyO3
> module includes the Rithmic adapter.

### Runtime-path capability summary

| Capability | Raw binding path | Rust v2 `LiveNode` path (pure Rust or PyO3) |
|------------|------------------|--------------------------------------------|
| Quotes / trades | ✓ | ✓ |
| Live external bars | ✓ | ✓ |
| Historical time bars | ✓ | ✓ |
| Historical `N‑TICK` bars | ✓ | ✓ |
| Order book deltas | ✓ | ✓ |
| Order book snapshots / depth ladders | ✓ | ✓ |
| Single‑order execution | ✓ | ✓ |
| Auto reconnect and execution re‑bootstrap | manual / low‑level | ✓ |
| General Nautilus `SubmitOrderList` | - | Limited |
| Explicit native bracket / OCO helpers | ✓ | - |

`Limited` order-list support means adapter-local translation for:

- two-leg OCO lists
- three-order limit-entry brackets with one `OTO` entry, one `STOP_MARKET`
  reduce-only child, and one `LIMIT` reduce-only child

Market-entry brackets and other list shapes still reject explicitly on the
Rust v2 path.
`native_bracket_state_path` is currently reserved for future persisted native
bracket-state support and is rejected explicitly on the current PyO3 execution
paths if it is configured.

## Examples

Live example scripts are available in [`examples/live/rithmic/`](../../examples/live/rithmic/).

### Rust Examples

- [`crates/adapters/rithmic/examples/instrument_discovery_probe.rs`](../../crates/adapters/rithmic/examples/instrument_discovery_probe.rs) to probe exchange permissions, the hard-coded supported root list, symbol search, and concurrent reference-data fan-out before building a catalog for a real login.
- [`crates/adapters/rithmic/examples/write_instruments_catalog.rs`](../../crates/adapters/rithmic/examples/write_instruments_catalog.rs) to discover supported futures across the enabled exchanges on the current login and write those instrument definitions into a local `ParquetDataCatalog`. It defaults to `RITHMIC_TRADEABLE_ONLY=true` for live catalog refreshes; set `RITHMIC_TRADEABLE_ONLY=0` when you need the supported historical futures set.
- [`crates/adapters/rithmic/examples/download_bars.rs`](../../crates/adapters/rithmic/examples/download_bars.rs) for a pure Rust historical downloader which writes a Rithmic instrument plus external bars into a local `ParquetDataCatalog`.
- [`crates/adapters/rithmic/examples/backtest_ema_cross.rs`](../../crates/adapters/rithmic/examples/backtest_ema_cross.rs) for the corresponding pure Rust catalog-backed EMA-cross backtest run.
- [`crates/adapters/rithmic/examples/node_data_tester.rs`](../../crates/adapters/rithmic/examples/node_data_tester.rs) for a pure Rust `LiveNode` data-client smoke run using the Rust `DataTester` actor.
- [`crates/adapters/rithmic/examples/node_exec_tester.rs`](../../crates/adapters/rithmic/examples/node_exec_tester.rs) for a pure Rust `LiveNode` execution smoke run using the Rust `ExecTester` strategy.
- [`crates/adapters/rithmic/examples/node_ema_cross.rs`](../../crates/adapters/rithmic/examples/node_ema_cross.rs) for a pure Rust `LiveNode` EMA-cross strategy with bounded historical warmup and live external bars.

### Python Examples

- `examples/backtest/rithmic/rithmic_download_bars.py` for the standard adapter-level historical helper flow, which resolves a concrete Rithmic contract and writes historical external bars directly into a `ParquetDataCatalog`.
- `examples/backtest/rithmic/rithmic_backtest_high_level.py` for the corresponding high-level `BacktestRunConfig` / `BacktestNode` EMA-cross run against that catalog.
- [`rithmic_conformance_keepalive.py`](../../examples/live/rithmic/rithmic_conformance_keepalive.py) for the simplest API-conformance flow: pass only a Rithmic username and password, force the Rithmic test route, connect, and idle until manual shutdown. This uses the raw gateway path intentionally so it does not add live-node subscriptions, reconciliation, or order-flow behavior on top of the required connectivity check.
- `rithmic_data_tester.py` for a `LiveNode` / PyO3 data-client smoke run with optional bar warmup and live external bars.
- `rithmic_exec_tester.py` for a `LiveNode` / PyO3 execution smoke run.
- `rithmic_ema_cross.py` for a full live `LiveNode` / PyO3 EMA-cross strategy on a configured futures contract with native 15-second external bars and bounded historical warmup by default.
- `rithmic_ema_cross_two_accounts.py` for live copy-trading into two accounts under the same Rithmic login/system from one `LiveNode`.
- `rithmic_ema_cross_two_systems.py` for live copy-trading into two different Rithmic profiles, systems, or logins from one `LiveNode`.
- [`notebooks/rithmic_contracts_fetch_to_parquet.ipynb`](../../examples/live/rithmic/notebooks/rithmic_contracts_fetch_to_parquet.ipynb) for the optional advanced workflow: explicit symbol probing, manual day-partitioned parquet export, synthetic-root continuous-history export, and a simple continuous-data EMA backtest. Use this notebook when you need continuous-root preprocessing or custom parquet layout rather than the standard catalog examples.

### Python Helpers

- [`rithmic_orderbook_probe.py`](../../examples/live/rithmic/rithmic_orderbook_probe.py) for a `LiveNode` / PyO3 order-book inspection helper which can print raw `OrderBookDeltas`, a managed book, or both.
- [`rithmic_live_data_capture.py`](../../examples/live/rithmic/rithmic_live_data_capture.py) for a timed live-market-data capture helper which writes normalized Nautilus instruments, quotes, trades, book deltas, and optional bars into a local `ParquetDataCatalog`.
- `order_submission.py` for a low-level safe working-order submit/modify/cancel flow.
- `bracket_submission.py` for a low-level native bracket smoke run.
- `oco_submission.py` for a low-level native OCO smoke run.

The Rust `instrument_discovery_probe.rs` and `write_instruments_catalog.rs` examples cover the
instrument-catalog bootstrap path. The Rust `download_bars.rs` and `backtest_ema_cross.rs`
examples cover the catalog-backed pure Rust historical and backtest path. The Rust
`node_data_tester.rs`, `node_exec_tester.rs`, and `node_ema_cross.rs` examples use the pure Rust
`LiveNode` path directly. The conformance
keepalive, order, bracket, and OCO examples use the raw binding path directly. The current Python
data tester, order-book probe, live capture, execution tester, and EMA-cross examples use the
PyO3 `LiveNode` path so they exercise the same Rust live clients used by the Rust v2 runtime.

Companion workflow guide:

- [Rithmic data workflows](rithmic_data_workflows.md) covers historical downloads, live probes,
  live order-book inspection, live capture, and the exact Nautilus object types written to the
  catalog.

For the common high-level backtest workflow described in the Nautilus
[Backtest (high-level API)](https://nautilustrader.io/docs/latest/getting_started/backtest_high_level/)
guide, start with the instrument-catalog bootstrap examples if you need discovery across the
enabled exchanges on a login. Then use the specific-contract bar download examples, and switch to
the notebook only when you need continuous-root preprocessing or manual parquet layout control.

Those script examples follow the standard `ParquetDataCatalog.from_env()` layout: set
`NAUTILUS_PATH` to the parent workspace and the Rithmic catalog will live at
`<NAUTILUS_PATH>/catalog`.

Once the data is in a local parquet catalog, pure Rust strategies can backtest it with
the normal Rust `BacktestNode` or `BacktestEngine` path exactly like any other catalog-backed
dataset.

If you want to run the notebook outside this repository, copy it into a workspace where
NautilusTrader is already installed, using a build/version that includes the Rithmic adapter.

## Products

The current adapter is futures-focused.

| Product Type | Supported | Notes |
|--------------|-----------|-------|
| Futures market data | ✓ | Live quote ticks, live trade ticks, instrument definitions, historical bars, and historical trade ticks synthesized from 1-tick replay bars. |
| Futures execution | ✓ | Standard live order submission and reconciliation are available through the Rust v2 `LiveNode` path in Rust and Python. High‑level `SubmitOrderList` supports native two‑leg OCO lists and three‑order limit‑entry brackets. Market‑entry brackets and other list shapes remain unsupported. |
| Spot / cash products | - | Not exposed through the current adapter surface. |
| Options workflows | Limited | The adapter does not currently provide a complete options‑specific operator guide or examples. |

### Hard-coded supported futures roots

The adapter only supports this hard-coded futures list for bulk discovery, catalog writes, and
live `request_instruments()` responses:

- `CME`: `ES`, `MES`, `NQ`, `MNQ`, `RTY`, `M2K`, `NKD`, `EMD`, `MYM`, `MBT`, `MET`, `6A`, `6B`,
  `6C`, `6E`, `6J`, `6S`, `E7`, `M6E`, `M6A`, `6M`, `6N`, `M6B`, `HE`, `LE`, `GF`
- `CBOT`: `YM`, `MYM`, `ZC`, `ZW`, `ZS`, `ZM`, `ZL`, `ZT`, `ZF`, `ZN`, `TN`, `ZB`, `UB`
- `NYMEX`: `CL`, `QM`, `NG`, `QG`, `MCL`, `RB`, `HO`, `PL`, `MNG`
- `COMEX`: `GC`, `SI`, `HG`, `MGC`, `SIL`, `MHG`

`MYM` is retried on both `CBOT` and `CME` because the resolved route depends on the connected
Rithmic system. Anything outside this list is intentionally rejected by the adapter's bulk
instrument parsing and discovery paths.

## Environments

The adapter supports the following Rithmic environments:

| Environment | Config value | Description |
|-------------|--------------|-------------|
| Demo | `RithmicEnv.DEMO` | Demo / paper trading plants. |
| Live | `RithmicEnv.LIVE` | Production trading plants. |
| Test | `RithmicEnv.TEST` | Alternate test routing when provided by your setup. |

The public adapter boundary is v2 PyO3-first:

- `RithmicDataClientConfig` and `RithmicExecClientConfig` are the raw PyO3 config classes used by
  `LiveNode`
- `RithmicDataClientFactory` and `RithmicExecClientFactory` construct the Rust live clients; Python
  does not maintain a separate live-client implementation

## Symbology

### Contract symbology

Use the exact futures contract symbol and exchange together with the Rithmic venue.

```python
from nautilus_trader.model.identifiers import InstrumentId

instrument_id = InstrumentId.from_str("MNQM6.CME.RITHMIC")
```

Rithmic uses the canonical `{contract}.{exchange}.RITHMIC` form:

- contract: `MNQM6`
- exchange: `CME`
- venue: `RITHMIC`

The exchange component prevents collisions when the same contract code is listed on more than one
Rithmic exchange. The venue-native contract code is preserved as the instrument `raw_symbol`
(`MNQM6` in this example). The legacy `{contract}.RITHMIC` form is accepted as an input alias only
when the cached contract resolves to exactly one exchange; emitted instruments and events always
use the canonical exchange-qualified ID.

### Cross-feed symbol helpers

When you source market data from Databento or ProjectX but route live requests through Rithmic,
use the adapter-local symbol helpers to normalize the venue contract code:

```python
from nautilus_trader.adapters.rithmic import databento_to_rithmic_symbol
from nautilus_trader.adapters.rithmic import projectx_to_rithmic_symbol
from nautilus_trader.adapters.rithmic import rithmic_to_databento_symbol
from nautilus_trader.adapters.rithmic import rithmic_to_projectx_symbol


databento_to_rithmic_symbol("MNQM26")            # "MNQM6"
projectx_to_rithmic_symbol("CON.F.US.MNQ.M26")   # "MNQM6"
rithmic_to_databento_symbol("MNQM6")             # "MNQM26"
rithmic_to_projectx_symbol("MNQM6")              # "MNQM26"
```

Notes:

- These helpers operate on the symbol component only, not a full `InstrumentId`.
- `rithmic_to_databento_symbol(...)` and `rithmic_to_projectx_symbol(...)` expand the one-digit
  Rithmic expiry against the current UTC year by default. For deterministic tests or explicit
  historical tooling, use `rithmic_to_databento_symbol_with_year(...)` or
  `rithmic_to_projectx_symbol_with_year(...)`.

### Live front-month workflow

The adapter does **not** treat a root alias such as `MNQ.CME.RITHMIC` as a live tradable contract
ID. Live requests and orders should use an exact contract such as `MNQM6.CME.RITHMIC`.

The supported live workflow today is:

1. Start with a product root and exchange, such as `MNQ` and `CME`.
2. Resolve the active contract through `load_front_month_async(...)`.
3. Use the returned live `InstrumentId`, such as `MNQM6.CME.RITHMIC`.
4. Use that resolved contract ID for live subscriptions, bar requests, and order submission.

`FuturesContract.activation_ns` is populated from Rithmic auxiliary reference data when
`first_trading_date` is present. Some contracts still arrive without that field, in which case
`activation_ns` remains `0`. The adapter does **not** infer an activation timestamp from
`symbol_name`, `underlying_symbol`, or the contract code because that would be venue- and
product-specific guesswork rather than a reliable first-trade date.

For live trading, treat the Rithmic adapter as front-month oriented: resolve the active contract
with `load_front_month_async(...)`, then subscribe and route orders against that concrete contract
ID for the current session.

The notebook helpers under `examples/live/rithmic/notebooks/` follow this pattern.

```python
from nautilus_trader.adapters.rithmic import RithmicInstrumentProvider
from nautilus_trader.adapters.rithmic.config import RithmicDataClientConfig
from nautilus_trader.model.identifiers import InstrumentId


async def resolve_front_month_instrument_id(
    profile: str,
    product: str,
    exchange: str,
) -> InstrumentId:
    config = RithmicDataClientConfig.from_env(profile)
    provider = RithmicInstrumentProvider(config)
    contract = await provider.load_front_month_async(product, exchange)
    return contract.id
```

If you want to start from a root such as `MNQ`, resolve the front month first, then pass the
resolved exact contract ID into the live node or strategy.

### Backtest symbology

Backtest and catalog flows are intentionally separate from live front-month resolution. Use the
instrument IDs that exist in your local parquet/catalog data. If your local Rithmic dataset is
stored under a chosen backtest symbol workflow, keep using that dataset directly rather than
expecting the live adapter to rewrite it.

If your downstream logic depends on a non-zero `activation_ns`, validate that field explicitly when
loading historical contracts. Rithmic does not provide `first_trading_date` consistently across all
contracts, so some stored `FuturesContract` rows will legitimately keep `activation_ns=0`.

### Instrument catalog discovery

For exchange-scoped or full-login catalog builds, the supported production path is:

1. Call `list_exchanges(username)` to enumerate the exchanges enabled for the current login.
2. Intersect the enabled exchanges with the adapter's hard-coded supported exchange set:
   `CME`, `CBOT`, `NYMEX`, and `COMEX`.
3. For each enabled supported exchange, iterate the hard-coded supported futures roots for that
   exchange and resolve the current front-month contract for that root.
4. Load concrete reference data for the resolved contract and filter by `is_tradeable` when
   requested.

This path writes one current `FuturesContract` per supported root. It does not build a full
historical contract chain.

If you need the broader contract chain for historical work or manual contract selection, use the
raw discovery helpers instead of forcing the adapter to resolve every match into a full
`FuturesContract`. `RithmicInstrumentProvider.discover_exchange_symbols_async(...)`,
`discover_product_symbols_async(...)`, and `discover_all_symbols_async()` return raw
`RithmicInstrumentSymbol` rows from the supported-root `search_symbols(...)` flow. Each row carries
the concrete venue symbol, exchange, product code, optional description, optional instrument type,
optional expiration date, and a convenience `instrument_id` string. Load full reference data only
for the specific symbols you actually want with the binding-level
`load_instrument_async(symbol, exchange)` or the high-level provider `load_async(...)`.

`RithmicInstrumentProvider.load_all_async()` and
`RithmicInstrumentProvider.load_exchange_async()` return that same supported-root current-contract
set. For live catalog refreshes where you only want currently tradeable contracts, use
`RithmicInstrumentProvider.load_all_tradeable_async()`,
`RithmicInstrumentProvider.load_exchange_tradeable_async()`, or the high-level Python provider
filter `{"tradeable_only": True}`.

The adapter no longer relies on venue-wide `get_product_codes(...)` enumeration, empty-string
exchange snapshots, or the older search-and-fan-out discovery path for the production catalog and
live instrument-request flow. The same supported-root front-month bootstrap is also used when the
live data client handles `request_instruments()`.
On the `LiveNode` path, `request_instruments()` triggers an on-demand provider load. The live
front-month request returns one current `FuturesContract` per supported hard-coded root rather than
a full historical contract-chain scan.

### High-level backtest helper flow

If you want the standard Nautilus high-level backtest path without writing your own ingestion
boilerplate:

1. If you need contract discovery or want a reusable instrument catalog, run
   [`write_instruments_catalog.rs`](../../crates/adapters/rithmic/examples/write_instruments_catalog.rs)
   first. It defaults to `RITHMIC_TRADEABLE_ONLY=1` for live catalog refreshes; set
   `RITHMIC_TRADEABLE_ONLY=0` when you want the supported current-contract set without filtering
   out non-tradeable contracts. Set
   `RITHMIC_EXCHANGE` when you want to scope the catalog build to one venue. Use
   [`instrument_discovery_probe.rs`](../../crates/adapters/rithmic/examples/instrument_discovery_probe.rs)
   first when validating a new login or entitlement set, or when diagnosing whether the broader
   search/reference-data path is healthy enough for future full-chain restoration work.
2. Run `examples/backtest/rithmic/rithmic_download_bars.py` or
   [`download_bars.rs`](../../crates/adapters/rithmic/examples/download_bars.rs) to request a
   specific Rithmic instrument plus historical external bars into `<NAUTILUS_PATH>/catalog`.
3. Run `examples/backtest/rithmic/rithmic_backtest_high_level.py` or
   [`backtest_ema_cross.rs`](../../crates/adapters/rithmic/examples/backtest_ema_cross.rs) to
   execute a normal catalog-backed backtest over that data.
4. Use the notebook only when you need synthetic continuous roots, deeper root-symbol history, or
   manual day-partitioned parquet layout across many expiries.

For manual historical/non-front-month selection, the lighter-weight pattern is:

```python
provider = RithmicInstrumentProvider(config)
contracts = await provider.discover_product_symbols_async("ES", "CME")

for contract in contracts[:5]:
    print(contract.symbol, contract.exchange, contract.expiration_date)

selected = contracts[0]
await provider.load_async(
    InstrumentId.from_str(selected.instrument_id),
    filters={"exchange": selected.exchange},
)
instrument = provider.find(InstrumentId.from_str(selected.instrument_id))
```

That keeps the expensive full reference-data resolution step scoped to the exact contracts you
intend to download or trade.

If you already know the concrete contract ID you want, you can skip the instrument-discovery step
and start directly with the specific-contract bar download examples.

### Futures month codes

Rithmic futures symbols use standard month codes:

- `F` = January
- `G` = February
- `H` = March
- `J` = April
- `K` = May
- `M` = June
- `N` = July
- `Q` = August
- `U` = September
- `V` = October
- `X` = November
- `Z` = December

### Supported exchange hints

The provider recognizes exchange hints in either filters or symbology suffixes for the currently
supported exchanges:

- `CME`
- `CBOT`
- `NYMEX`
- `COMEX`

## Market data capability

### Data surfaces

| Capability | Status | Notes |
|------------|--------|-------|
| Instrument definition loading | ✓ | Via `RithmicInstrumentProvider` and the live data client provider path. |
| Live quote ticks | ✓ | Subscribes to the Rithmic ticker plant. |
| Live trade ticks | ✓ | Subscribes to the Rithmic ticker plant. |
| Historical quote ticks | - | Not supported by the Rithmic API exposed through this adapter. |
| Historical trade ticks | ✓ | Synthesized from 1-tick bar replay on the history plant. Aggressor side is always `NO_AGGRESSOR` and trade IDs are synthetic replay IDs. |
| Historical bars | ✓ | Time bars and `N‑TICK` bars via the history plant when `enable_history=True`. Large requests can still truncate venue‑side and require paging. Time-bar requests that reach the active interval can include the current open/in-progress bar. |
| Volume profile bars | ✓ | Historical‑only. Returned as `RithmicMinuteVolumeProfileBar` custom data via `request_data`. Requires `enable_history=True`. |
| Live external bar subscriptions | ✓ | Time bars and tick bars via the history plant when `enable_history=True`. |
| Internal bars | ✓ | Still the simplest live strategy pattern: subscribe to ticks and consolidate inside Nautilus. |
| Order book deltas / depth | ✓ | Incremental order‑book deltas, bootstrapped snapshots, and `OrderBookDepth10` subscriptions are supported on the raw binding and Rust v2 live-client paths. |
| Instrument status / close updates | Limited | `MarketMode` is mapped to Nautilus `InstrumentStatus`. Exchange close updates are not exposed as a separate venue feed. |
| Rithmic statistics / indicator custom data | Limited | Rust v2 live clients can explicitly subscribe to `TradeStatistics`, `QuoteStatistics`, `IndicatorPrices`, `OpenInterest`, `EndOfDayPrices`, `OrderPriceLimits`, and `SymbolMarginRate` as Nautilus custom data. |
| Funding, mark price, index price feeds | - | Not provided by the current adapter. |

### LiveNode limitations

The high-level Rust live-client path intentionally does not expose every low-level feature.

- Market data:
  - no historical quote-tick requests
  - historical trade-tick requests are synthesized from 1-tick bar replay, so aggressor side is
    always `NO_AGGRESSOR`
  - Rithmic-specific custom market-data subscriptions use Nautilus `DataType` requests
- Execution:
  - standard single-order submit / modify / cancel is supported
  - automatic forced-logout / reconnect recovery replays account snapshot, open-order query, and bounded execution history on reconnect
  - locally submitted orders are enriched from Rust-side tracked order metadata before reports are
    built, which hardens sparse immediate Rithmic submit notifications
  - `SubmitOrderList` supports only:
    - two-leg OCO lists
    - three-order limit-entry brackets with `OTO` parent plus `STOP_MARKET` / `LIMIT`
      reduce-only children using `OCO` or `OUO` contingencies
  - market-entry brackets and other list shapes still reject explicitly
  - raw native bracket and OCO helper examples remain available when you need
    venue-specific low-level workflows instead of high-level translation

### Live quote/trade helper semantics

For the raw binding-level `RithmicDataClient` surface, `subscribe_quotes(symbol, exchange)`,
`subscribe_trades(symbol, exchange)`, and `subscribe(symbol, exchange)` all route through the same
combined gateway market-data helper. That helper requests both `BBO` and `LAST_TRADE`, and the
single ticker-plant feed carries both best-bid-offer updates and last-trade updates.

This is a current Nautilus adapter-helper choice, not a claim that upstream `rithmic-rs` lacks
distinct `BBO` and `LAST_TRADE` wire bits.

In practice this means:

- any of those three client methods is sufficient to start receiving both quote and trade updates
- repeated calls on the same `RithmicDataClient` instance are deduplicated client-side and treated
  as idempotent
- if you bypass that client guard and send duplicate raw `gateway.subscribe_market_data(...)`
  requests for the same symbol/exchange pair, Rithmic can reject the second request with a message
  such as
  `Subscription rejected: update bit type already exists`

That rejection is not a connectivity failure. It means the same combined ticker subscription is
already active for that instrument.

### Live order-book probe and catalog capture helpers

[`examples/live/rithmic/rithmic_orderbook_probe.py`](../../examples/live/rithmic/rithmic_orderbook_probe.py)
is the quickest way to inspect the live order-book stream a Rithmic account is actually receiving:

- it resolves the configured or front-month futures contract, starts a data-only `LiveNode`, and
  subscribes to `OrderBookDeltas`
- `STREAM_MODE="deltas"` prints raw Nautilus `OrderBookDeltas`; `STREAM_MODE="book"` prints a
  managed book built from those deltas; `STREAM_MODE="both"` prints both
- `STREAM_MODE="depth"` is treated as the managed-book mode because the current PyO3 strategy
  surface does not expose the legacy `OrderBookDepth10` callback directly
- the visible ladder still depends on the exchange entitlement plus the requested `DEPTH_LEVELS`
  value in the example

[`examples/live/rithmic/rithmic_live_data_capture.py`](../../examples/live/rithmic/rithmic_live_data_capture.py)
uses that same `LiveNode` / PyO3 runtime path to persist a timed capture into a local
`ParquetDataCatalog`.

The catalog stores Nautilus-normalized callback objects, not raw Rithmic ticker/history plant
payloads. In practice this means:

- instruments are stored as Nautilus instrument definitions
- quotes are stored as Nautilus `QuoteTick`
- trades are stored as Nautilus `TradeTick`
- depth is stored as Nautilus `OrderBookDelta` rows
- optional live and historical bars are stored as Nautilus `Bar` objects, with overlapping bars
  deduplicated before the catalog write

Backtests replay those stored Nautilus objects directly. They do not reparse raw Rithmic venue
messages later.

For the broader historical-download and live-capture helper flow, see
[Rithmic data workflows](rithmic_data_workflows.md).

### Historical bar requests

Historical bar requests require the history plant to be enabled on the data client:

```python
data_config = RithmicDataClientConfig(
    ...,
    enable_history=True,
)
```

If `enable_history=False`, the live node can still stream quotes and trades, but both
`request_bars()` and live external `subscribe_bars()` calls will be rejected. This is useful for
live-only nodes that do not need the history plant.

Custom second-bar resolutions such as `15-SECOND-LAST-EXTERNAL` are supported through this path.

> [!WARNING]
>
> Rithmic historical API usage is plan-limited. On basic Rithmic plans, historical downloads are
> typically capped at **20 GB per month**. Rithmic sends warning emails to the account's registered
> email address when API usage approaches that limit or when their access rules are being breached.
> Do not ignore those emails. Temporary restrictions can be applied automatically if usage continues
> after warnings are sent.
>
> If you are downloading large windows, prefer smaller batched requests and monitor the registered
> email inbox for notices from Rithmic.

Current historical external bar limits:

- only `EXTERNAL` bars are supported
- only `LAST` price bars are supported
- supported time aggregations are `SECOND`, `MINUTE`, `DAY`, and `WEEK`
- supported historical `TickBar` aggregation is `N-TICK`

Historical trade-tick requests are implemented separately by replaying `1-TICK` history and
converting each replay bar into a Nautilus `TradeTick`.

> [!WARNING]
>
> Historical Rithmic trade ticks are **synthetic replay ticks**, not venue-native tick-by-tick
> executions. The adapter requests `1-TICK` bar replay and maps each replay bar to a
> `TradeTick` so the data can be used for warmup and catalog downloads.
>
> Because these records come from replay bars rather than native trade ticks:
>
> - aggressor side is always `NO_AGGRESSOR`
> - trade IDs are synthetic replay IDs
> - historical quote ticks remain unsupported
> - large windows should still be requested in bounded batches because venue-side truncation can
>   occur

> [!NOTE]
>
> Rithmic history requests can also be truncated venue-side. A large date-range request is not
> guaranteed to return the full requested window in a single response; the vendor may return only a
> partial segment of the available bars. Treat each history response as a page of data rather than as
> proof that the full range was delivered.
> In practice, compare the timestamp of the last returned bar with the requested end time. If the
> response stops early, issue another request from the last returned bar onward (or from the next bar
> boundary if you want to avoid a duplicate boundary bar) and continue until the requested range is
> fully covered. A round-number result such as `10000` bars can be a useful signal that truncation
> occurred, but the more reliable check is whether the returned bars actually span the requested
> window.
> Rithmic exposes a `request_key`/resume flow for truncated replies, but not every adapter surface
> currently drives that path automatically. For large bar or replay-tick backfills, callers should
> still prefer bounded batches instead of assuming one request will return the full range.

> [!NOTE]
>
> Time-bar history requests can include the current open/in-progress final bar when the requested
> end reaches the active interval. If you are building a closed-only historical dataset, verify
> `bar.ts_event + bar_interval <= cutoff_time` before writing the last bar into a catalog or using
> it in a backtest window.
>

### Volume profile bars

The Rithmic history plant exposes volume-profile minute bars through
`load_volume_profile_minute_bars`. Because Nautilus has no built-in `BarAggregation` for this data
shape, the adapter models it as a `CustomData` type: `RithmicMinuteVolumeProfileBar`.

Each bar carries standard OHLCV fields plus a full per-price-level volume breakdown:

| Field | Type | Description |
|-------|------|-------------|
| `instrument_id` | `InstrumentId` | e.g. `ESM5.CME.RITHMIC` |
| `open_price` / `high_price` / `low_price` / `close_price` | `f64` | Standard OHLC |
| `volume` | `u64` | Total volume for the bar |
| `bid_volume` / `ask_volume` | `u64` | Bid‑side and ask‑side volume |
| `num_trades` | `u64` | Trade count |
| `poc_price` | `Option<f64>` | Point of Control — price level with the highest combined bid+ask volume |
| `profile_price` | `Vec<f64>` | Price levels (parallel array) |
| `profile_bid_volume` | `Vec<i32>` | Bid volume at each price level |
| `profile_ask_volume` | `Vec<i32>` | Ask volume at each price level |
| `ts_event` | `UnixNanos` | Bar close time |
| `ts_init` | `UnixNanos` | Receive time |

**Requirements:**

- `enable_history=True` must be set on the data client config.
- Volume profile bars are **historical-only** — `subscribe` is a no-op with a warning. Use
  `request_data` to pull a historical range.

**Requesting from a strategy (Rust):**

```rust
use nautilus_model::data::DataType;

let data_type = DataType::new(
    "RithmicMinuteVolumeProfileBar",
    None,                                    // optional metadata — see below
    Some("ESM5.CME.RITHMIC".to_string()), // instrument_id as identifier
);
actor.request_data(client_id, data_type, Some(start), Some(end), None, None);
```

The response arrives in `on_historical_data` as a `CustomData` payload containing
`Vec<RithmicMinuteVolumeProfileBar>`. Down-cast with:

```rust
if let Some(bars) = data.as_any().downcast_ref::<Vec<RithmicMinuteVolumeProfileBar>>() {
    for bar in bars {
        println!("POC: {:?}", bar.poc_price);
    }
}
```

**Bar period:**

The default bar period is 1 minute. To request a different period, pass it as metadata:

```rust
use nautilus_core::Params;
use serde_json::json;

let metadata: Params = serde_json::from_value(json!({"period": 5})).unwrap();
let data_type = DataType::new(
    "RithmicMinuteVolumeProfileBar",
    Some(metadata),
    Some("ESM5.CME.RITHMIC".to_string()),
);
```

**JSON round-trip:**

`RithmicMinuteVolumeProfileBar` implements full JSON serialization / deserialization via
`CustomDataTrait::to_json` / `from_json`. The type is registered on `connect()` so it can be
deserialized by type name from the catalog or message bus.

### Synthetic continuous-root workflow for backtests

The Rithmic historical API has an important practical limitation for futures backtesting:

- individual contract history is often shallower than the corresponding root-symbol history
- older contract definitions may not always be available through normal instrument-info lookups

This matters because a specific contract such as `MNQM6` can be valid for live trading and bounded
recent history, while the root symbol `MNQ` can still return a materially larger continuous history
window. In the current notebook workflow, that continuous root series is expected to extend back to
**June 2019**, which is deeper than many individual contract responses.

The recommended example for this workflow is
[`examples/live/rithmic/notebooks/rithmic_contracts_fetch_to_parquet.ipynb`](https://github.com/nautechsystems/nautilus_trader/tree/develop/examples/live/rithmic/notebooks/rithmic_contracts_fetch_to_parquet.ipynb).

That notebook demonstrates the following pattern:

1. Discover real futures contracts for a root such as `MNQ`.
2. Persist the real Nautilus `FuturesContract` objects for specific-contract backtests.
3. Derive one synthetic root instrument definition from a discovered real contract definition.
4. Rename that copied instrument to the root symbol and use it only as a backtest placeholder.
5. Request historical bars with the root symbol itself, such as `MNQ`, to capture the larger
   continuous history that Rithmic exposes for the root.
6. Save those bars against the synthetic root instrument ID so a backtest can treat the result as
   a continuous contract series.

This synthetic root flow is intentionally a **backtest-only** workflow. It is not a live-trading
symbology shortcut and should not be confused with the live front-month resolution workflow
described earlier in this guide.

The notebook also shows why the synthetic root is needed:

- it derives usable instrument metadata from a real contract because older contracts may not have
  retrievable symbol info
- it stores real contract data and synthetic continuous-root data separately so users can backtest
  either workflow
- it pages historical requests because Rithmic can truncate large history responses
- it should filter the final bar if you only want closed historical candles, because raw requests
  can now include the current open bar

To use that notebook, copy it into a workspace where NautilusTrader is installed and importable,
using a version/build that already contains the Rithmic adapter. The notebook is not intended to be
a standalone artifact without a Nautilus installation.

### Live external bars

The adapter now supports venue-fed live time bars and tick bars through Nautilus
`subscribe_bars()`, with the same history-plant dependency as historical bar requests.

Current live external bar limits:

- only `EXTERNAL` bars are supported
- only `LAST` price bars are supported
- supported aggregations are `SECOND`, `MINUTE`, `DAY`, `WEEK`, and `TICK`

Example:

```python
from nautilus_trader.model.data import BarType


bar_type = BarType.from_str("MNQM6.CME.RITHMIC-1-MINUTE-LAST-EXTERNAL")
strategy.subscribe_bars(bar_type, params={"exchange": "CME"})
```

Tick-bar example:

```python
from nautilus_trader.model.data import BarType


bar_type = BarType.from_str("MNQM6.CME.RITHMIC-233-TICK-LAST-EXTERNAL")
strategy.subscribe_bars(bar_type, params={"exchange": "CME"})
```

Use this path when you specifically want venue-fed candles. For many live strategies, internal bars
from quote/trade ticks are still the more robust default because they do not depend on the history
plant being enabled or permissioned on the venue side.

### Live strategy pattern

For live strategies, the recommended pattern is:

1. Resolve the active contract first.
2. Subscribe to quote ticks and trade ticks for that contract.
3. Choose one of:
   - use Nautilus internal aggregation to build bars locally
   - subscribe to live external `LAST` bars with `enable_history=True`
4. If you need tick-driven indicator warmup before the live stream starts, request bounded
   historical trade ticks first. These are replayed from `1-TICK` history, so their aggressor
   side will always be `NO_AGGRESSOR`.

This is the pattern used by the current `rithmic_ema_cross.py` and related live-copy examples.

## Execution capability

### Order types

| Order Type | Supported | Notes |
|------------|-----------|-------|
| `MARKET` | ✓ | Supported for direct order submission. |
| `LIMIT` | ✓ | Supported for direct order submission and native bracket entry. |
| `STOP_MARKET` | ✓ | Supported for direct order submission and native bracket stop legs. |
| `STOP_LIMIT` | ✓ | Supported for direct order submission. |

### Time in force

| Time in Force | Supported |
|---------------|-----------|
| `DAY` | ✓ |
| `GTC` | ✓ |
| `IOC` | ✓ |
| `FOK` | ✓ |

### Order and reconciliation flows

| Capability | Status | Notes |
|------------|--------|-------|
| Submit order | ✓ | Single-order submission through the Rust v2 execution client in Rust or Python. |
| Modify order | ✓ | Venue order ID required once the order is working. |
| Cancel order | ✓ | Venue order ID required once the order is working. |
| Cancel all orders | ✓ | Cancels all open orders for the configured account connection. |
| Batch cancel | ✓ | Supported through `BatchCancelOrders`. |
| Execution replay | ✓ | Bounded replay on connect for recent order/fill state. |
| Open‑order snapshot recovery | ✓ | Reconcile active working orders on connect. |
| Filled‑order snapshot recovery | ✓ | When Rithmic omits cumulative average fill price on reconnect snapshots, the adapter backfills `avg_px` from observed fill reports so reconciliation can still rebuild fills. |
| Account / PnL snapshots | ✓ | Primary account balances and positions are rebuilt from the PnL plant. |
| Forced logout / reconnect recovery | ✓ | The Rust v2 execution client automatically reconnects and reruns its bootstrap after forced logout, channel close, or heartbeat-driven reconnect. |
| Shared multi-account fan-out | ✓ | Configure one execution client per Rithmic account and route by explicit client ID or exact account ID. |

### Native `SubmitOrderList` routing

The adapter implements a **limited** adapter-local `SubmitOrderList` bridge in the Rust v2
execution client, exposed identically through PyO3.

Supported high-level list shapes today:

- two-leg OCO lists
- three-order limit-entry brackets with:
  - one `OTO` entry order
  - one `STOP_MARKET` reduce-only child
  - one `LIMIT` reduce-only child
  - child contingencies of either `OCO` or `OUO`

Unsupported list shapes still reject explicitly rather than soft-failing:

- market-entry brackets
- triggered entry orders
- quote-quantity, post-only, trailing-stop, or non-whole-contract variants in
  those native list shapes
- other multi-order topologies that do not map cleanly onto the venue-native
  OCO or bracket APIs

The market-entry bracket restriction is intentional, not a silent adapter gap:
Rithmic's native bracket request still models exits as tick distances from the
entry rather than absolute child prices, so Nautilus high-level market-entry
lists cannot be translated losslessly.

Lower-level native helper functionality on the raw binding path is still
available:

- `examples/live/rithmic/bracket_submission.py` uses explicit native bracket helper calls
- `examples/live/rithmic/oco_submission.py` uses explicit native OCO helper calls

If you need a market-entry native bracket today, or you need to bypass the
adapter's high-level routing contract entirely, use those raw helper workflows
or decompose the list in the strategy.

### Current execution boundaries

- Adapter-created high-level native OCO and bracket lists keep their child-ID
  mapping for the current live process lifetime, including reconnects handled
  without a full process restart.
- `native_bracket_state_path` is not active on the current PyO3 execution
  paths. If it is configured, the adapter now rejects the setting explicitly
  instead of silently ignoring it.
- Venue-only child attribution for native brackets created outside the adapter remains limited by
  available Rithmic metadata.

## Configuration

The adapter reads canonical `RITHMIC_*` environment variables and also supports profile-scoped
overrides through `RITHMIC_{PROFILE}_*`, with the profile-specific values checked first.

### `.env` loading (recommended)

The adapter now includes a Rust/PyO3 helper that loads `RITHMIC_*` keys from a local `.env` file,
so users do not need to manually `export` each variable every run.

- Python helper: `nautilus_trader.adapters.rithmic.load_rithmic_env_file(path: str | None = None)`
- Rust/PyO3 static methods:
  - `RithmicDataClientConfig.load_env_file(path=None)`
  - `RithmicExecClientConfig.load_env_file(path=None)`

Behavior:

- Only keys starting with `RITHMIC_` are loaded into an adapter-owned cache; the helper does not
  mutate the process environment.
- Existing process environment variables are not overwritten and take precedence over cached
  dotenv values.
- Default call with no `path` loads `.env` in the current working directory.
- Profile-scoped keys such as `RITHMIC_APEX_USERNAME` are supported naturally because they are still
  `RITHMIC_*` keys.

### Core adapter env keys

These keys are consumed by adapter config loading (`from_env` / `from_env_with_profile`) across the
Rust and Python integration paths.

| Env key | Required | Used by | Notes |
|---------|----------|---------|-------|
| `RITHMIC_USERNAME` | Yes | Data + Execution | Login username. |
| `RITHMIC_PASSWORD` | Yes | Data + Execution | Login password. |
| `RITHMIC_SYSTEM_NAME` | Yes | Data + Execution | Exact Rithmic `System` value. |
| `RITHMIC_APP_NAME` | Yes | Data + Execution | API onboarding app name from Rithmic. |
| `RITHMIC_ACCOUNT_ID` | Yes (execution only) | Execution | Required for execution clients. |
| `RITHMIC_ENV` | No | Data + Execution | Connection environment selector: `demo` (default), `live`, or `test`. |
| `RITHMIC_APP_VERSION` | No | Data + Execution | Defaults to `1.0`. |
| `RITHMIC_FCM_ID` | Yes | Data + Execution | Required operationally by Rithmic login. |
| `RITHMIC_IB_ID` | Yes | Data + Execution | Required operationally by Rithmic login. |
| `RITHMIC_SERVER` | No | Data + Execution | Primary named route; defaults to `Chicago` for demo/live, `Test` for test. |
| `RITHMIC_ALT_SERVER` | No | Data + Execution | Optional named alternate route. |
| `RITHMIC_ENABLE_HISTORY` | No | Data | Defaults to enabled unless explicitly `false`, `0`, or `no`. |
| `RITHMIC_EXECUTION_REPLAY_LOOKBACK_SECS` | No | Execution | Replay window on connect/reconnect; default `86400`. |
| `RITHMIC_NATIVE_BRACKET_STATE_PATH` | No | Execution | Reserved JSON path for future persisted native bracket state. Current PyO3 execution paths reject it if set. |
| `RITHMIC_TRADER_ID` | No (Rust v2 path) | Execution | Defaults to `TRADER‑001` when omitted. |
| `RITHMIC_PROFILES` | No | Data + Execution | Comma‑separated profiles (`Apex,Paper`) for multi‑profile config loading. |

### Profile-scoped override pattern

Any supported key can be scoped per profile using:

- `RITHMIC_{PROFILE}_{KEY}`

For example:

- `RITHMIC_APEX_USERNAME`
- `RITHMIC_APEX_PASSWORD`
- `RITHMIC_APEX_SYSTEM_NAME`
- `RITHMIC_APEX_ACCOUNT_ID`

When a profile is supplied to `from_env(profile=...)`, the adapter checks profile-scoped keys first,
then falls back to canonical `RITHMIC_{KEY}` keys.

Important distinction:

- `RITHMIC_ENV` selects environment (`demo` / `live` / `test`).
- `RITHMIC_SYSTEM_NAME` is your broker/login system string (for example `Apex`, `paper_trading`, etc.).

### Example-specific env keys

Only a small number of additional environment variables are used by the current example scripts
beyond the core adapter configuration:

| Env key | Used by example(s) | Purpose |
|---------|--------------------|---------|
| `RITHMIC_PROFILE` | multiple examples | Selects which profile name to pass into helper config builders. |
| `NAUTILUS_PATH` | `rithmic_download_bars.py`, `rithmic_backtest_high_level.py`, `rithmic_live_data_capture.py` | Parent workspace used to place or read `<NAUTILUS_PATH>/catalog`. |

The current `rithmic_data_tester.py`, `rithmic_exec_tester.py`, `rithmic_ema_cross.py`,
`rithmic_live_data_capture.py`, raw order/bracket/OCO helpers, and `rithmic_conformance_keepalive.py`
are configured primarily by editing module-level constants in the script files rather than by
introducing extra example-specific environment variables.

For the current data-download and live-capture helper configuration, see
[Rithmic data workflows](rithmic_data_workflows.md).

> [!NOTE]
>
> `RITHMIC_APP_NAME` and `RITHMIC_APP_VERSION` are not arbitrary local labels. To obtain valid values,
> you generally need to contact Rithmic, request API access, and complete their conformance process.
> At present, that conformance step is typically just connecting to the test API endpoint as directed
> by Rithmic support. The required details are provided by Rithmic during the API onboarding flow.
>
> Start here: [Rithmic APIs](https://www.rithmic.com/apis) and
> [Rithmic API Request](https://www.rithmic.com/api-request).

> [!NOTE]
>
> `RITHMIC_FCM_ID` and `RITHMIC_IB_ID` should be treated as required for production/demo operator
> setup. While some adapter code paths can parse missing values, real Rithmic credentials normally
> require both fields to authenticate and route correctly.

For the simplest Nautilus-side conformance run, use
[`examples/live/rithmic/rithmic_conformance_keepalive.py`](../../examples/live/rithmic/rithmic_conformance_keepalive.py).
That script intentionally maps the vendor's "test URL" instruction to the adapter's
`RithmicEnv.TEST` plus the named `Test` server route, accepts only
`RITHMIC_USERNAME` and `RITHMIC_PASSWORD` from the user, and then keeps the session open
until you stop it manually.
It also assumes the current conformance requirement is only to stay connected.
If Rithmic changes that requirement, the user would need to code a custom strategy or
workflow to satisfy the updated conformance process.

Example shell setup:

```bash
export RITHMIC_ENV=demo
export RITHMIC_USERNAME="your_username"
export RITHMIC_PASSWORD="your_password"
export RITHMIC_SYSTEM_NAME="your_system_name"  # Exact System value from RTrader Pro > File > User Profile
export RITHMIC_ACCOUNT_ID="your_account"
export RITHMIC_APP_NAME="your_rithmic_app_name"
export RITHMIC_FCM_ID="your_fcm_id"            # Exact FCM value from RTrader Pro > File > User Profile
export RITHMIC_IB_ID="your_ib_id"              # Exact IB value from RTrader Pro > File > User Profile
export RITHMIC_SERVER="Chicago"
export RITHMIC_ALT_SERVER="Sydney"             # Secondary route if you want one
```

Example `.env` (no secrets):

```dotenv
# Profile selector for examples that pass profile into from_env(...)
RITHMIC_PROFILE=Apex

# Data defaults
RITHMIC_ENABLE_HISTORY=true
RITHMIC_APP_NAME=YOUR_APP_NAME
RITHMIC_APP_VERSION=1.0

# Canonical credentials (fallback)
RITHMIC_ENV=demo
RITHMIC_SYSTEM_NAME=YOUR_SYSTEM_NAME
RITHMIC_USERNAME=YOUR_USERNAME
RITHMIC_PASSWORD=YOUR_PASSWORD
RITHMIC_ACCOUNT_ID=YOUR_ACCOUNT_ID
RITHMIC_FCM_ID=YOUR_FCM_ID
RITHMIC_IB_ID=YOUR_IB_ID

# Optional profile-scoped override set
RITHMIC_APEX_ENV=demo
RITHMIC_APEX_SYSTEM_NAME=YOUR_SYSTEM_NAME
RITHMIC_APEX_USERNAME=YOUR_USERNAME
RITHMIC_APEX_PASSWORD=YOUR_PASSWORD
RITHMIC_APEX_ACCOUNT_ID=YOUR_ACCOUNT_ID
RITHMIC_APEX_FCM_ID=YOUR_FCM_ID
RITHMIC_APEX_IB_ID=YOUR_IB_ID
```

`RITHMIC_PROFILE` is only a local environment-variable namespace. The actual broker-facing values
are `RITHMIC_*_SYSTEM_NAME`, `RITHMIC_*_FCM_ID`, and `RITHMIC_*_IB_ID`.

> [!NOTE]
>
> Do not guess the Rithmic `System`, `FCM`, or `IB` values from the broker or prop-firm brand
> name alone. Some connections use `paper_trading`, Apex uses `Apex`, and other Rithmic brokers may
> use non-obvious identifiers even on standard demo or live accounts.
>
> To find the correct values, sign in to the RTrader Pro desktop application and open
> `File > User Profile`, then copy `System`, `FCM`, and `IB` exactly as shown. Treat them as
> case-sensitive.

### Server endpoint selection

Use named server selection through `RITHMIC_SERVER` and `RITHMIC_ALT_SERVER` or the equivalent
`server=` / `alt_server=` config fields. Server names are matched case-insensitively.

If `RITHMIC_SERVER` is omitted, the adapter defaults the primary route to `Chicago` for demo/live
and `Test` for test environments.

Supported server names:

- `Chicago`
- `Sydney`
- `Sao Paulo`
- `Colo75`
- `Frankfurt`
- `Hong Kong`
- `Ireland`
- `Mumbai`
- `Seoul`
- `Cape Town`
- `Tokyo`
- `Singapore`
- `Test`

### Data client configuration

The most important data-client options are:

- `enable_history`: enable this only when the node needs historical bar requests.
- `server`: primary route name. Defaults to `Chicago` for demo/live and `Test` for test.
- `alt_server`: optional named alternate route.
- `instrument_provider.load_all`: preload the full instrument snapshot on connect.
- `instrument_provider.load_ids`: preload a selected live contract set.
- `instrument_provider.filters`: usually include the target futures exchange, such as `{"exchange": "CME"}`. Live nodes can also add `{"tradeable_only": True}` to avoid caching non-tradeable futures during startup; historical/backtest discovery should usually omit that flag.

### Execution client configuration

The most important execution-client options are:

- `account_id`: the Rithmic account this execution client is allowed to control.
- `server`: primary route name. Defaults to `Chicago` for demo/live and `Test` for test.
- `alt_server`: optional named alternate route.
- `execution_replay_lookback_secs`: bounded replay window used during reconnect reconciliation.
- `native_bracket_state_path`: reserved JSON file path for future persisted
  native bracket state support. The current PyO3 execution paths reject it if
  it is configured.

Regular operator setups should use named `server` / `alt_server` selection.

### Multiple accounts on the same `RITHMIC` venue

The adapter supports multiple Rithmic execution clients on the same venue in one node. Nautilus
routes by an explicit `client_id` first, then by an exact account when the command or cached order
provides one, and finally by the venue-default client.

This is a **live-only operational feature** intended for explicit multi-account routing and
copy-trading style setups under one Rithmic login/system. It is **not** a backtest feature.
Nautilus routes an explicit `client_id` or exact account match before using the venue-default
client, so venue-only commands remain intentionally unambiguous.

Supported contract:

- the venue remains `RITHMIC` for all Rithmic clients
- each execution account gets a distinct adapter `client_id` derived from `system_name` plus
  `account_id`
- strategies or actors should pass that `client_id` explicitly on order and execution commands for
  every non-default Rithmic account; exact account routing is also supported
- each PyO3 `LiveNode.add_exec_client(...)` call must use its distinct derived client ID

Not supported:

- automatic venue-only inference between multiple Rithmic execution clients on the same venue
- implicit selection of a non-default account when neither `client_id` nor `account_id` is supplied

Example derived identities for `system_name="Apex"`:

- data client ID: `APEX`
- execution client ID for `PA-123456`: `APEX_PA_123456`
- execution client ID for `PA-654321`: `APEX_PA_654321`
- adapter account IDs:
  - `RITHMIC-APEX_PA_123456-PA-123456`
  - `RITHMIC-APEX_PA_654321-PA-654321`

Use the helper functions from `nautilus_trader.adapters.rithmic.config` to derive the routing IDs
the same way as the adapter:

```python
from nautilus_trader.adapters.rithmic.config import get_rithmic_exec_client_id
from nautilus_trader.model.identifiers import ClientId

client_id = ClientId(get_rithmic_exec_client_id("Apex", "PA-123456"))
strategy.submit_order(order, client_id=client_id)
```

The complete adapter-only example is
`examples/live/rithmic/rithmic_ema_cross_two_accounts.py`, which runs one shared data client and
two execution clients under the same Rithmic login/system while routing orders explicitly by
`client_id`. It uses the default profile for the primary account and a `SECONDARY` profile for the
second account. Configure both profiles with identical login, system, server, and application
values but distinct `ACCOUNT_ID` values; the matching session identity makes the clients reuse one
upstream gateway without relying on Python process-environment mutation.

### Multiple Rithmic systems/logins in one node

One Nautilus node can also host multiple Rithmic profiles, systems, or logins at the same time.
This remains a **live-only operational feature**.

Supported contract:

- each distinct Rithmic profile/login/system creates its own adapter `client_id`
- the shared gateway registry only reuses transport when the upstream login identity matches
- one node can use one Rithmic data profile and multiple Rithmic execution profiles for
  copy-trading
- if you configure more than one Rithmic data client in the same node, route data requests and
  subscriptions explicitly by `client_id`

Recommended operator pattern:

- configure `RITHMIC_PROFILES` with two named profiles such as `Apex,Topstep`
- provide the corresponding `RITHMIC_APEX_*` and `RITHMIC_TOPSTEP_*` credentials and account IDs
- use one profile as the market-data source and route every non-default execution command by
  explicit `client_id`
- run separate Nautilus processes only when you want stronger operational isolation; the adapter
  does not require that for distinct Rithmic systems

Example derived identities:

- data client ID for `Apex`: `APEX`
- execution client ID for `Topstep` account `PA-654321`: `TOPSTEP_PA_654321`

Use the same helper functions when routing both data and execution commands:

```python
from nautilus_trader.adapters.rithmic.config import get_rithmic_data_client_id
from nautilus_trader.adapters.rithmic.config import get_rithmic_exec_client_id
from nautilus_trader.model.identifiers import ClientId

apex_data = ClientId(get_rithmic_data_client_id("Apex"))
topstep_exec = ClientId(get_rithmic_exec_client_id("Topstep", "PA-654321"))

self.request_instrument(self.config.instrument_id, client_id=apex_data)
self.request_bars(self.config.bar_type, start=start, client_id=apex_data)
self.subscribe_bars(self.config.bar_type, client_id=apex_data)
self.submit_order(order, client_id=topstep_exec)
```

The complete operator example is
`examples/live/rithmic/rithmic_ema_cross_two_systems.py`, which runs one primary Rithmic data
profile and copies the same orders into two different Rithmic systems/logins from one node.

## Live node example

The current Python examples use the PyO3 `LiveNode` path. The snippet below mirrors the helper flow
used by the repository's `rithmic_data_tester.py` and `rithmic_ema_cross.py` examples.

```python
from examples.live.rithmic.rithmic_live_node_helpers import TRADER_ID
from examples.live.rithmic.rithmic_live_node_helpers import RithmicDataClientFactory
from examples.live.rithmic.rithmic_live_node_helpers import RithmicExecClientFactory
from examples.live.rithmic.rithmic_live_node_helpers import build_data_client_config
from examples.live.rithmic.rithmic_live_node_helpers import build_data_client_id
from examples.live.rithmic.rithmic_live_node_helpers import build_exec_client_config
from examples.live.rithmic.rithmic_live_node_helpers import build_exec_client_id
from examples.live.rithmic.rithmic_live_node_helpers import load_rithmic_env_file
from nautilus_trader.core.nautilus_pyo3.common import Environment
from nautilus_trader.live import LiveNode
from nautilus_trader.model.identifiers import InstrumentId

load_rithmic_env_file()

profile = None
instrument_id = InstrumentId.from_str("MNQM6.CME.RITHMIC")
data_client_id = build_data_client_id(profile)
exec_client_id = build_exec_client_id(profile)

node = (
    LiveNode.builder("TRADER-001", TRADER_ID, Environment.LIVE)
    .add_data_client(
        data_client_id,
        RithmicDataClientFactory(),
        build_data_client_config(
            profile,
            enable_history=True,
        ),
    )
    .add_exec_client(
        exec_client_id,
        RithmicExecClientFactory(),
        build_exec_client_config(profile),
    )
    .build()
)
```

See the `examples/live/rithmic/` scripts for complete runnable node and low-level smoke examples.
