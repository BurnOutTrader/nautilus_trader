# Rithmic Data Workflows

This companion guide covers the current Rithmic example flows for downloading, inspecting, and
storing market data. The main [Rithmic integration guide](rithmic.md) focuses on the adapter
surfaces and capability matrix; this guide focuses on the repo helpers.

## Configuration style

The current runnable Python scripts in `examples/live/rithmic/` are configured primarily by
editing module-level constants in the script files.

Core credentials and routing still come from canonical `RITHMIC_*` environment variables.
The most common additions are:

- `RITHMIC_PROFILE` to select profile-scoped credentials
- `RITHMIC_ACCOUNT_ID_2` or `RITHMIC_{PROFILE}_ACCOUNT_ID_2` for the two-account EMA example
- `NAUTILUS_PATH` for helpers that write or read `<NAUTILUS_PATH>/catalog`

## Instrument catalog bootstrap

Use these Rust helpers when you need to discover what the current login can actually trade and
build a reusable instrument catalog before requesting bars:

- [`crates/adapters/rithmic/examples/instrument_discovery_probe.rs`](../../crates/adapters/rithmic/examples/instrument_discovery_probe.rs)
- [`crates/adapters/rithmic/examples/write_instruments_catalog.rs`](../../crates/adapters/rithmic/examples/write_instruments_catalog.rs)

The supported discovery path is:

1. Enumerate enabled exchanges with `list_exchanges(username)`.
2. Intersect the result with the adapter's hard-coded supported exchange set:
   `CME`, `CBOT`, `NYMEX`, and `COMEX`.
3. Iterate the hard-coded supported futures roots for that exchange and resolve the current
   front-month contract for each root.
4. Load concrete reference data for the resolved contract and keep either the tradeable set or the
   full supported-root current-contract set, depending on the caller.

This is now the provider-backed catalog workflow used by `load_all_async()` and
`load_exchange_async()`. The live `request_instruments()` path uses that same supported-root
front-month bootstrap for live instrument enumeration. Empty exchange-wide symbol searches,
`get_product_codes(exchange)` enumeration, and the older search-and-fan-out discovery path are no
longer the documented production catalog flow.
When you need the larger contract list for historical work, use the raw discovery helpers instead:
`discover_exchange_symbols_async(...)`, `discover_product_symbols_async(...)`, or
`discover_all_symbols_async()`. Those return `RithmicInstrumentSymbol` rows from the supported-root
`search_symbols(...)` flow without forcing the adapter to resolve every contract into a full
`FuturesContract`.
The Python live client now loads that same supported set on demand for `request_instruments()` and
`subscribe_instruments()`, merging any request filters over the configured provider defaults
instead of relying on startup preload state. Those live requests now default to front-month
contracts only, and `front_month_only=False` currently falls back to the provider-backed catalog
load rather than a full-chain scan. Exact contracts use `SYM.RITHMIC` identifiers, while root
products remain an explicit front-month-resolution step.

`write_instruments_catalog.rs` writes Nautilus `Instrument` definitions into
`<NAUTILUS_PATH>/catalog`. It defaults to `RITHMIC_TRADEABLE_ONLY=true`, which is the recommended
mode for live catalog refreshes. Set `RITHMIC_TRADEABLE_ONLY=0` when you want the supported
current-contract set without tradeable filtering. Set `RITHMIC_EXCHANGE` when you want to build
the catalog for one venue at a time instead of every enabled exchange. The supported futures roots
are fixed to the hard-coded list documented in [rithmic.md](rithmic.md#hard-coded-supported-futures-roots).

If your goal is “show me the whole supported contract list and let me choose,” skip the catalog
writer and use the raw discovery helpers first, then call `load_instrument_async(symbol, exchange)`
only for the contracts you actually plan to backfill.

Activation timestamps have an important limitation: `FuturesContract.activation_ns` is only filled
when Rithmic auxiliary reference data includes `first_trading_date`. The adapter does not infer an
activation date from `symbol_name`, `underlying_symbol`, or the expiry code, so some contracts in
the catalog will retain `activation_ns=0`.

## Historical downloads

Once you already know the concrete contract ID, or once you have written instrument definitions
with the catalog bootstrap example, use these helpers for the standard historical bar workflow:

- [`crates/adapters/rithmic/examples/download_bars.rs`](../../crates/adapters/rithmic/examples/download_bars.rs)
- [`examples/backtest/rithmic/rithmic_download_bars.py`](../../examples/backtest/rithmic/rithmic_download_bars.py)
- [`crates/adapters/rithmic/examples/backtest_ema_cross.rs`](../../crates/adapters/rithmic/examples/backtest_ema_cross.rs)
- [`examples/backtest/rithmic/rithmic_backtest_high_level.py`](../../examples/backtest/rithmic/rithmic_backtest_high_level.py)
- [`examples/live/rithmic/notebooks/rithmic_contracts_fetch_to_parquet.ipynb`](../../examples/live/rithmic/notebooks/rithmic_contracts_fetch_to_parquet.ipynb)

Recommended sequence:

1. For live catalog refreshes, run `write_instruments_catalog.rs` with its default
   `RITHMIC_TRADEABLE_ONLY=true` behavior.
   For live trading itself, prefer resolving the current front month at startup instead of relying
   on arbitrary catalog rows from older expiries.
2. For historical/backtest catalog bootstrap, either skip the supported catalog build and download
   the exact contracts you need, or run `write_instruments_catalog.rs` with
   `RITHMIC_TRADEABLE_ONLY=0`.
3. Run one of the specific-contract bar download examples for the instrument you actually want to
   backtest.
4. Run one of the backtest examples against the resulting catalog.
5. Use the notebook only when you need explicit symbol probing across many expiries, synthetic
   continuous-root data, or manual day-partitioned parquet output.

The shipped download script is configured by editing:

- `PROFILE`
- `CATALOG_PATH`
- `INSTRUMENT_ID`
- `BAR_SPEC`
- `REQUEST_LIMIT`
- `REQUEST_START`
- `REQUEST_END`

Its default request window is the previous UTC trading week:

- start: previous Monday `00:00`
- end: previous Friday `23:59`

Example API usage:

```python
from pathlib import Path

from nautilus_trader.adapters.rithmic import download_bars_to_catalog
from nautilus_trader.adapters.rithmic import download_trade_ticks_to_catalog


bars_result = download_bars_to_catalog(
    profile=None,
    catalog_path=Path("/tmp/rithmic/catalog"),
    instrument_id="MNQM6.RITHMIC",
    product_code=None,
    bar_spec="1-MINUTE-LAST",
    start_time="2026-03-30T00:00:00Z",
    end_time="2026-04-03T23:59:00Z",
    limit=0,
)

ticks_result = download_trade_ticks_to_catalog(
    profile=None,
    catalog_path=Path("/tmp/rithmic/catalog"),
    instrument_id="MNQM6.RITHMIC",
    product_code=None,
    start_time="2026-04-03T13:30:00Z",
    end_time="2026-04-03T14:00:00Z",
    limit=0,
)
```

What gets written:

- `download_bars_to_catalog(...)` writes Nautilus `Instrument` and `Bar`
- `download_trade_ticks_to_catalog(...)` writes Nautilus `Instrument` and `TradeTick`

The specific-contract download helpers do not attempt broad exchange discovery. They assume you
already know the exact instrument ID you want, or that you resolved it from the instrument catalog
bootstrap flow above. For exact contract IDs such as `MNQM6.RITHMIC`, omit `exchange`. Provide
`exchange` only when resolving a front month from `product_code`, or when you want to be explicit
for an ambiguous root such as `MYM`.

Internally these helpers open one direct historical Rithmic session for the request, resolve the
contract once, and write Nautilus `Instrument`, `Bar`, or `TradeTick` rows into the catalog. They
do not rely on the generic `BacktestNode.download_data(...)` actor path.

Current limitation:

- historical quote-tick requests are still unsupported
- historical trade-tick requests are synthesized from `1-TICK` replay bars, so every stored
  `TradeTick` has `NO_AGGRESSOR`
- large trade-tick windows should still be downloaded in bounded batches because venue-side
  truncation can occur

Volume-profile minute bars are a separate `request_data(...)` custom-data path and are stored as
Rithmic custom data rather than standard Nautilus `Bar` rows.

## Live probes and order-book inspection

The current runnable Python helpers use the `LiveNode` + PyO3 registry path rather than the older
`TradingNode` compatibility wrappers.

Use these helpers to inspect the live feed:

- [`examples/live/rithmic/rithmic_data_tester.py`](../../examples/live/rithmic/rithmic_data_tester.py)
- [`examples/live/rithmic/rithmic_orderbook_probe.py`](../../examples/live/rithmic/rithmic_orderbook_probe.py)

`rithmic_data_tester.py` can observe quotes, trades, depth, live external bars, and optional
historical warmup bars in one timed run.

`rithmic_orderbook_probe.py` can print:

- raw `OrderBookDeltas`
- a managed book built from those deltas
- or both

Because the current PyO3 strategy surface does not expose the legacy raw `OrderBookDepth10`
callback directly, `STREAM_MODE="depth"` is treated as the managed-book view.

The order-book probe resolves a front month automatically when `INSTRUMENT_ID` is `None`; otherwise
it uses the configured concrete instrument ID.

## Live capture to catalog

Use [`examples/live/rithmic/rithmic_live_data_capture.py`](../../examples/live/rithmic/rithmic_live_data_capture.py)
to persist a timed live capture into a local `ParquetDataCatalog`.

The script is configured by editing:

- `CATALOG_PATH`
- `PROFILE`
- `PRODUCT_CODE`
- `EXCHANGE`
- `INSTRUMENT_ID`
- `BAR_SPEC`
- `CAPTURE_SECONDS`
- `READY_TIMEOUT_SECONDS`
- `FIRST_DATA_WAIT_SECONDS`
- `DEPTH_LEVELS`
- `CAPTURE_QUOTES`
- `CAPTURE_TRADES`
- `CAPTURE_DEPTH`
- `CAPTURE_BARS`
- `REQUEST_BARS`
- `HISTORICAL_LOOKBACK_MINUTES`
- `UNSUBSCRIBE_ON_STOP`
- `LOG_DATA`
- `FAIL_ON_TIMEOUT`

What gets written:

- Nautilus `Instrument`
- Nautilus `QuoteTick`
- Nautilus `TradeTick`
- Nautilus `OrderBookDelta`
- Nautilus `Bar`

When both historical warmup bars and live bars overlap, the helper deduplicates them before the
catalog write.

## Storage and replay semantics

Rithmic download and capture flows store Nautilus-normalized objects, not raw ticker/history plant
payloads.

In practice this means:

- bar downloads replay as Nautilus `Instrument` and `Bar`
- trade-tick downloads replay as Nautilus `Instrument` and `TradeTick`, with historical aggressor
  side always set to `NO_AGGRESSOR`
- live capture replays as Nautilus `Instrument`, `QuoteTick`, `TradeTick`, `OrderBookDelta`, and
  `Bar`
- venue-specific custom data such as `RithmicMinuteVolumeProfileBar` remains custom data rather
  than being coerced into a standard Nautilus bar/tick type
- the catalog is not a raw Rithmic packet archive
