# ProjectX Data Workflows

This companion guide covers the current ProjectX example flows for downloading,
inspecting, and storing market data. The main [ProjectX integration guide](projectx.md)
focuses on the adapter surfaces and runtime setup; this guide focuses on the repo helpers.

## Configuration style

The current ProjectX example scripts are configured primarily by editing module-level constants
inside the script files.

Credentials still come from the canonical adapter environment variables:

- `PROJECTX_USERNAME`
- `PROJECTX_API_KEY`

Catalog-producing helpers also use `NAUTILUS_PATH` to place data under
`<NAUTILUS_PATH>/catalog`.

## Historical bar downloads

Use these helpers for the standard historical catalog flow:

- [`examples/backtest/projectx/projectx_download_bars.py`](../../examples/backtest/projectx/projectx_download_bars.py)
- [`examples/backtest/projectx/projectx_backtest_high_level.py`](../../examples/backtest/projectx/projectx_backtest_high_level.py)
- [`examples/live/projectx/notebooks/projectx_historical_bars_to_parquet.py`](../../examples/live/projectx/notebooks/projectx_historical_bars_to_parquet.py)

The shipped download script is configured by editing:

- `CATALOG_PATH`
- `INSTRUMENT_ID`
- `BAR_SPEC`
- `REQUEST_LIMIT`
- `MARKET_DATA_LIVE`
- `REQUEST_START`
- `REQUEST_END`

Its default request window is the previous UTC trading week:

- start: previous Monday `00:00`
- end: previous Friday `23:59`

Example API usage:

```python
from pathlib import Path

from nautilus_trader.adapters.projectx import download_bars_to_catalog
from nautilus_trader.model import InstrumentId


result = download_bars_to_catalog(
    catalog_path=Path("/tmp/projectx/catalog"),
    instrument_id=InstrumentId.from_str("MNQM26.PROJECTX"),
    bar_spec="1-MINUTE-LAST",
    start_time="2026-03-30T00:00:00Z",
    end_time="2026-04-03T23:59:00Z",
    limit=500,
    market_data_live=False,
)
```

What gets written:

- Nautilus `Instrument`
- Nautilus `Bar`

## Live probes and order-book inspection

Use these helpers when you want to inspect the live ProjectX feed rather than persist it:

- [`examples/live/projectx/projectx_live_data_probe.py`](../../examples/live/projectx/projectx_live_data_probe.py)
- [`examples/live/projectx/projectx_orderbook_probe.py`](../../examples/live/projectx/projectx_orderbook_probe.py)

The shipped probe scripts currently target the module-level `INSTRUMENT_ID` constant rather than
resolving a front month internally. If you want a dynamic contract first, use:

- [`examples/live/projectx/projectx_front_month_resolver.py`](../../examples/live/projectx/projectx_front_month_resolver.py)
- [`examples/live/projectx/projectx_instrument_provider.py`](../../examples/live/projectx/projectx_instrument_provider.py)

and then update the probe script's `INSTRUMENT_ID`.

Useful probe constants include:

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

`projectx_orderbook_probe.py` can print either raw deltas or a managed book. Because the current
PyO3 strategy surface does not expose the legacy raw `OrderBookDepth10` callback, `STREAM_MODE="depth"`
is treated as the managed-book view built from `OrderBookDeltas`.

Live depth behavior depends on the feed entitlement and the stream semantics returned by ProjectX:

- top-of-book-only feeds are treated as replaceable best bid / best ask slots
- indexed depth feeds are reconstructed into deeper ladders automatically

## Live capture to catalog

Use [`examples/live/projectx/projectx_live_data_capture.py`](../../examples/live/projectx/projectx_live_data_capture.py)
to persist a timed live capture into a local `ParquetDataCatalog`.

The script is configured by editing:

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

What gets written:

- Nautilus `Instrument`
- Nautilus `QuoteTick`
- Nautilus `TradeTick`
- Nautilus `OrderBookDelta`

The current ProjectX live capture helper does not write bars because ProjectX live bar subscriptions
are not exposed through the current WebSocket surface. Live bar strategies should still warm up
with historical `request_bars(...)` and then trade on internal bars.

## Storage and replay semantics

ProjectX download and capture flows store Nautilus-normalized objects, not raw ProjectX HTTP or
SignalR payloads.

In practice this means:

- backtests replay Nautilus `Instrument`, `Bar`, `QuoteTick`, `TradeTick`, and `OrderBookDelta`
  objects from the catalog
- the replayed data reflects the adapter's interpretation of the ProjectX feed at capture time
- the catalog is not a raw venue packet archive
