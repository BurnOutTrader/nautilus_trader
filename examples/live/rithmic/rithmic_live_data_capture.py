#!/usr/bin/env python3
# mypy: disable-error-code="arg-type,index"
# -------------------------------------------------------------------------------------------------
#  Copyright (C) 2026 Kevin Monaghan. All rights reserved.
#
#  Licensed under the GNU Lesser General Public License Version 3.0 or later.
#  You may not use this file except in compliance with the License.
#  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
# -------------------------------------------------------------------------------------------------

from __future__ import annotations

import os
import sys
import threading
import time
from pathlib import Path
from typing import TYPE_CHECKING
from uuid import uuid4

_REPO_ROOT = Path(__file__).resolve().parents[3]

if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from nautilus_trader._libnautilus.common import Environment
from nautilus_trader.live import LiveNode
from nautilus_trader.model import BarType, InstrumentId
from nautilus_trader.persistence import ParquetDataCatalog

from examples.live.live_node_run_helpers import (
    interrupt_live_node_run,
    sleep_or_cancel,
    start_live_node_monitor,
    wait_for_event_or_cancel,
)
from examples.live.rithmic.rithmic_data_capture import (
    clear_capture_state,
    register_capture_state,
    snapshot_capture_state,
    wait_for_capture_data,
    wait_for_capture_stop,
)
from examples.live.rithmic.rithmic_live_node_helpers import (
    TRADER_ID,
    RithmicDataClientFactory,
    build_data_client_config,
    build_data_client_id,
    load_rithmic_env_file,
    resolve_instrument_id,
)

if TYPE_CHECKING:
    from nautilus_trader.config import ImportableStrategyConfig
else:
    from nautilus_trader._libnautilus.trading import ImportableStrategyConfig


load_rithmic_env_file()


def _require_example_root() -> Path:
    nautilus_path = os.environ.get("NAUTILUS_PATH")

    if not nautilus_path:
        raise RuntimeError(
            "Set NAUTILUS_PATH to the Rithmic example root, "
            "for example /tmp/nautilus-data/examples/rithmic",
        )
    return Path(nautilus_path).expanduser()


CATALOG_PATH = _require_example_root() / "catalog"
PROFILE = None
PRODUCT_CODE = "MNQ"
EXCHANGE = "CME"
INSTRUMENT_ID: InstrumentId | None = None
BAR_SPEC = "1-MINUTE-LAST-EXTERNAL"
CAPTURE_SECONDS = 30.0
READY_TIMEOUT_SECONDS = 60.0
FIRST_DATA_WAIT_SECONDS = 10.0
DEPTH_LEVELS = 10
CAPTURE_QUOTES = True
CAPTURE_TRADES = True
CAPTURE_DEPTH = True
CAPTURE_BARS = True
REQUEST_BARS = True
HISTORICAL_LOOKBACK_MINUTES = 30
UNSUBSCRIBE_ON_STOP = True
LOG_DATA = False
FAIL_ON_TIMEOUT = False

_STRATEGY_PATH = "examples.live.rithmic.rithmic_data_capture:RithmicDataCaptureStrategy"
_CONFIG_PATH = (
    "examples.live.rithmic.rithmic_data_capture:RithmicDataCaptureStrategyConfig"
)


def _catalog_counts(
    path: Path,
    instrument_id: InstrumentId,
    bar_type: BarType | None,
) -> dict[str, int]:
    if not path.exists():
        return {
            "instruments": 0,
            "quotes": 0,
            "trades": 0,
            "book_deltas": 0,
            "bars": 0,
        }

    catalog = ParquetDataCatalog(str(path))
    identifier = instrument_id.value
    bars = catalog.bars(bar_types=[str(bar_type)]) if bar_type is not None else []
    return {
        "instruments": len(catalog.instruments(instrument_ids=[identifier])),
        "quotes": len(catalog.quote_ticks(instrument_ids=[identifier])),
        "trades": len(catalog.trade_ticks(instrument_ids=[identifier])),
        "book_deltas": len(catalog.order_book_deltas(instrument_ids=[identifier])),
        "bars": len(bars),
    }


def _print_capture_summary(summary: dict[str, object]) -> None:
    strategy_counts = summary["strategy_counts"]
    after_counts = summary["after_counts"]
    print(f"Status: {summary['status']}")
    print(f"Catalog path: {summary['catalog_path']}")
    print(f"Instrument ID: {summary['instrument_id']}")
    print(f"Data client ID: {summary['data_client_id']}")
    print(f"Bar type: {summary['bar_type']}")
    print(f"Capture seconds: {summary['capture_seconds']}")
    print(
        "Streams enabled: "
        f"quotes={summary['capture_quotes']} "
        f"trades={summary['capture_trades']} "
        f"depth={summary['capture_depth']} "
        f"bars={summary['capture_bars']} "
        f"request_bars={summary['request_bars']}",
    )
    print(
        "Strategy counts this run: "
        f"instruments={strategy_counts['instruments']} "
        f"quotes={strategy_counts['quotes']} "
        f"trades={strategy_counts['trades']} "
        f"book_deltas={strategy_counts['book_deltas']} "
        f"book_batches={strategy_counts['book_batches']} "
        f"bars={strategy_counts['bars']} "
        f"historical_bars={strategy_counts['historical_bars']}",
    )
    print(
        "Catalog totals: "
        f"instruments={after_counts['instruments']} "
        f"quotes={after_counts['quotes']} "
        f"trades={after_counts['trades']} "
        f"book_deltas={after_counts['book_deltas']} "
        f"bars={after_counts['bars']}",
    )
    print(
        "Catalog delta: "
        f"instruments={summary['catalog_delta']['instruments']} "
        f"quotes={summary['catalog_delta']['quotes']} "
        f"trades={summary['catalog_delta']['trades']} "
        f"book_deltas={summary['catalog_delta']['book_deltas']} "
        f"bars={summary['catalog_delta']['bars']}",
    )
    print(f"Instrument ready: {summary['instrument_ready']}")
    print(f"Saw data: {summary['saw_data']}")

    if summary["warning"] is not None:
        print(summary["warning"])

    if summary["error"] is not None:
        print(f"Error: {summary['error']}")

    if summary["lifecycle"]:
        print(f"Lifecycle: {', '.join(summary['lifecycle'])}")

    if summary["errors"]:
        print(f"Errors: {summary['errors']}")
    print(f"Parquet files present: {summary['parquet_file_count']}")

    if summary["last_parquet_file"] is not None:
        print(f"Last parquet file: {summary['last_parquet_file']}")


def _bar_identity(bar: object) -> tuple[str, int, int]:
    return (
        str(bar.bar_type),
        int(bar.ts_event),
        int(bar.ts_init),
    )


def _merge_captured_bars(snapshot: dict[str, object]) -> list[object]:
    merged: dict[tuple[str, int, int], object] = {}

    for group_name in ("historical_bars", "bars"):
        for bar in snapshot[group_name]:
            merged[_bar_identity(bar)] = bar

    bars = list(merged.values())
    bars.sort(key=lambda bar: (int(bar.ts_init), int(bar.ts_event)))
    return bars


def _write_capture_to_catalog(
    *,
    catalog_path: Path,
    before_counts: dict[str, int],
    snapshot: dict[str, object],
) -> None:
    catalog = ParquetDataCatalog(str(catalog_path))

    captured_instrument = snapshot["instrument"]

    if captured_instrument is not None and before_counts["instruments"] == 0:
        catalog.write_data([captured_instrument])

    captured_quotes = snapshot["quotes"]

    if captured_quotes:
        catalog.write_data(captured_quotes)

    captured_trades = snapshot["trades"]

    if captured_trades:
        catalog.write_data(captured_trades)

    captured_book_deltas = snapshot["book_deltas"]

    if captured_book_deltas:
        catalog.write_data(captured_book_deltas)

    captured_bars = _merge_captured_bars(snapshot)

    if captured_bars:
        catalog.write_data(captured_bars)


def run_capture(
    *,
    profile: str | None,
    catalog_path: Path,
    instrument_id: InstrumentId,
    bar_type: BarType | None,
    capture_seconds: float,
    ready_timeout_seconds: float = 60.0,
    first_data_wait_seconds: float = 10.0,
    depth_levels: int = 10,
    capture_quotes: bool = True,
    capture_trades: bool = True,
    capture_depth: bool = True,
    capture_bars: bool = True,
    request_bars: bool = True,
    historical_lookback_minutes: int = 30,
    unsubscribe_on_stop: bool = True,
    log_data: bool = False,
) -> dict[str, object]:
    if capture_bars and bar_type is None:
        raise ValueError("bar_type is required when capture_bars=True")

    catalog_path = catalog_path.expanduser()
    catalog_path.mkdir(parents=True, exist_ok=True)
    before_counts = _catalog_counts(catalog_path, instrument_id, bar_type)
    data_client_id = build_data_client_id(profile)
    state_key = f"rithmic-data-capture-{uuid4().hex}"
    register_capture_state(
        key=state_key,
        instrument_id=instrument_id.value,
    )

    history_enabled = bool(
        capture_bars and bar_type is not None and bar_type.is_externally_aggregated()
    )
    request_bars = bool(request_bars and history_enabled)

    node = (
        LiveNode.builder("TESTER-001", TRADER_ID, Environment.LIVE)
        .with_reconciliation(False)
        .with_timeout_connection(20)
        .with_timeout_disconnection_secs(10)
        .with_delay_post_stop_secs(5)
        .add_data_client(
            data_client_id,
            RithmicDataClientFactory(),
            build_data_client_config(
                profile,
                enable_history=history_enabled,
            ),
        )
        .build()
    )

    node.add_strategy_from_config(
        ImportableStrategyConfig(
            strategy_path=_STRATEGY_PATH,
            config_path=_CONFIG_PATH,
            config={
                "instrument_id": instrument_id.value,
                "data_client_id": data_client_id,
                "strategy_id": f"RITHMIC-DATA-CAPTURE-{uuid4().hex[:8].upper()}",
                "state_key": state_key,
                "bar_type": str(bar_type)
                if capture_bars and bar_type is not None
                else None,
                "subscribe_quotes": capture_quotes,
                "subscribe_trades": capture_trades,
                "subscribe_depth": capture_depth,
                "subscribe_bars": capture_bars,
                "request_bars": request_bars,
                "historical_lookback_minutes": historical_lookback_minutes,
                "depth_levels": depth_levels,
                "unsubscribe_on_stop": unsubscribe_on_stop,
                "log_data": log_data,
                "log_events": True,
                "log_commands": False,
            },
        ),
    )

    initial_wait_warning = None
    timed_out = False
    timeout_error = None
    monitor_cancel = threading.Event()
    snapshot: dict[str, object]

    def monitor_run() -> None:
        nonlocal initial_wait_warning, timed_out, timeout_error

        ready_deadline = time.monotonic() + ready_timeout_seconds
        ready_snapshot = snapshot_capture_state(state_key)

        while (
            not monitor_cancel.is_set()
            and time.monotonic() < ready_deadline
            and not ready_snapshot["instrument_ready"]
            and not ready_snapshot["data_seen"]
        ):
            ready_snapshot = snapshot_capture_state(state_key)
            if ready_snapshot["instrument_ready"] or ready_snapshot["data_seen"]:
                break
            monitor_cancel.wait(0.1)

        if monitor_cancel.is_set():
            return

        if not ready_snapshot["instrument_ready"] and not ready_snapshot["data_seen"]:
            timed_out = True
            timeout_error = (
                "Timed out waiting for initial Rithmic instrument or data response for "
                f"{instrument_id}. lifecycle={ready_snapshot['lifecycle']} "
                f"errors={ready_snapshot['errors']}"
            )
            interrupt_live_node_run()
            return

        if not ready_snapshot["instrument_ready"] and ready_snapshot["data_seen"]:
            initial_wait_warning = "Received Rithmic data before the instrument response; continuing capture."

        initial_wait = min(first_data_wait_seconds, max(capture_seconds, 0.0))

        if (
            initial_wait > 0
            and not ready_snapshot["data_seen"]
            and not wait_for_event_or_cancel(
                lambda timeout: wait_for_capture_data(state_key, timeout),
                initial_wait,
                cancel=monitor_cancel,
            )
        ):
            if monitor_cancel.is_set():
                return
            initial_wait_warning = (
                "No live data observed during initial wait window; continuing capture."
            )

        remaining = max(capture_seconds - initial_wait, 0.0)

        if remaining > 0:
            sleep_or_cancel(remaining, cancel=monitor_cancel)

        if not monitor_cancel.is_set():
            interrupt_live_node_run()

    try:
        try:
            monitor = start_live_node_monitor(
                monitor_run,
                name=f"{state_key}-monitor",
            )
            node.run()
        finally:
            monitor_cancel.set()

            if "monitor" in locals():
                monitor.join(timeout=1.0)
            wait_for_capture_stop(state_key, timeout=5)

        snapshot = snapshot_capture_state(state_key)

        if not timed_out:
            _write_capture_to_catalog(
                catalog_path=catalog_path,
                before_counts=before_counts,
                snapshot=snapshot,
            )
    finally:
        clear_capture_state(state_key)

    after_counts = _catalog_counts(catalog_path, instrument_id, bar_type)
    parquet_files = sorted((catalog_path / "data").rglob("*.parquet"))
    merged_bars = _merge_captured_bars(snapshot)
    summary = {
        "catalog_path": str(catalog_path),
        "instrument_id": instrument_id.value,
        "data_client_id": data_client_id,
        "bar_type": str(bar_type) if bar_type is not None else None,
        "capture_seconds": capture_seconds,
        "status": "timeout" if timed_out else "ok",
        "capture_quotes": capture_quotes,
        "capture_trades": capture_trades,
        "capture_depth": capture_depth,
        "capture_bars": capture_bars,
        "request_bars": request_bars,
        "depth_levels": depth_levels,
        "before_counts": before_counts,
        "after_counts": after_counts,
        "catalog_delta": {
            key: after_counts[key] - before_counts[key] for key in before_counts
        },
        "strategy_counts": {
            "instruments": 1 if snapshot["instrument"] is not None else 0,
            "instrument_events": snapshot["instrument_events"],
            "quotes": snapshot["quote_count"],
            "trades": snapshot["trade_count"],
            "book_batches": snapshot["book_batch_count"],
            "book_deltas": snapshot["book_delta_count"],
            "bars": snapshot["bar_count"],
            "historical_bars": snapshot["historical_bar_count"],
            "merged_bars": len(merged_bars),
        },
        "instrument_ready": snapshot["instrument_ready"],
        "saw_data": snapshot["data_seen"],
        "started_at_unix": snapshot["started_at_unix"],
        "stopped_at_unix": snapshot["stopped_at_unix"],
        "last_event_at_unix": snapshot["last_event_at_unix"],
        "lifecycle": snapshot["lifecycle"],
        "errors": snapshot["errors"],
        "parquet_file_count": len(parquet_files),
        "last_parquet_file": str(parquet_files[-1]) if parquet_files else None,
        "warning": initial_wait_warning,
        "error": timeout_error,
    }
    _print_capture_summary(summary)
    return summary


def main() -> None:
    try:
        instrument_id = resolve_instrument_id(
            PROFILE,
            INSTRUMENT_ID,
            PRODUCT_CODE,
            EXCHANGE,
        )
        bar_type = (
            BarType.from_str(f"{instrument_id}-{BAR_SPEC}") if CAPTURE_BARS else None
        )
    except Exception as exc:  # noqa: BLE001 (CLI boundary reports any adapter failure)
        print(f"Rithmic instrument resolution failed: {exc}")

        if FAIL_ON_TIMEOUT:
            raise SystemExit(1) from None
        return

    summary = run_capture(
        profile=PROFILE,
        catalog_path=CATALOG_PATH,
        instrument_id=instrument_id,
        bar_type=bar_type,
        capture_seconds=CAPTURE_SECONDS,
        ready_timeout_seconds=READY_TIMEOUT_SECONDS,
        first_data_wait_seconds=FIRST_DATA_WAIT_SECONDS,
        depth_levels=DEPTH_LEVELS,
        capture_quotes=CAPTURE_QUOTES,
        capture_trades=CAPTURE_TRADES,
        capture_depth=CAPTURE_DEPTH,
        capture_bars=CAPTURE_BARS,
        request_bars=REQUEST_BARS,
        historical_lookback_minutes=HISTORICAL_LOOKBACK_MINUTES,
        unsubscribe_on_stop=UNSUBSCRIBE_ON_STOP,
        log_data=LOG_DATA,
    )

    if FAIL_ON_TIMEOUT and summary["status"] != "ok":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
