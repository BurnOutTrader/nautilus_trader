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
import threading
from pathlib import Path
from typing import TYPE_CHECKING
from uuid import uuid4

from examples.live.live_node_run_helpers import interrupt_live_node_run
from examples.live.live_node_run_helpers import sleep_or_cancel
from examples.live.live_node_run_helpers import start_live_node_monitor
from examples.live.live_node_run_helpers import wait_for_event_or_cancel
from examples.live.projectx.projectx_data_capture import clear_capture_state
from examples.live.projectx.projectx_data_capture import register_capture_state
from examples.live.projectx.projectx_data_capture import snapshot_capture_state
from examples.live.projectx.projectx_data_capture import wait_for_capture_data
from examples.live.projectx.projectx_data_capture import wait_for_capture_instrument
from examples.live.projectx.projectx_data_capture import wait_for_capture_stop

from nautilus_trader._libnautilus.common import Environment
from nautilus_trader._libnautilus.model import TraderId
from nautilus_trader.adapters.projectx import PROJECTX_CLIENT_ID
from nautilus_trader.adapters.projectx import ProjectXDataClientConfig
from nautilus_trader.adapters.projectx import ProjectXDataClientFactory
from nautilus_trader.adapters.projectx import load_projectx_env
from nautilus_trader.live import LiveNode
from nautilus_trader.model import InstrumentId
from nautilus_trader.persistence import ParquetDataCatalog


if TYPE_CHECKING:
    from nautilus_trader.config import ImportableStrategyConfig
else:
    from nautilus_trader._libnautilus.trading import ImportableStrategyConfig


load_projectx_env()


def _require_example_root() -> Path:
    nautilus_path = os.environ.get("NAUTILUS_PATH")

    if not nautilus_path:
        raise RuntimeError(
            "Set NAUTILUS_PATH to the ProjectX example root, "
            "for example /tmp/nautilus-data/examples/projectx",
        )
    return Path(nautilus_path).expanduser()


CATALOG_PATH = _require_example_root() / "catalog"
INSTRUMENT_ID = InstrumentId.from_str("MNQM26.PROJECTX")
CAPTURE_SECONDS = 30.0
READY_TIMEOUT_SECONDS = 60.0
FIRST_DATA_WAIT_SECONDS = 10.0
DEPTH_LEVELS = 10
MARKET_DATA_LIVE = False
CAPTURE_QUOTES = True
CAPTURE_TRADES = True
CAPTURE_DEPTH = True
UNSUBSCRIBE_ON_STOP = True
LOG_DATA = False
FAIL_ON_TIMEOUT = False
TRADER_ID = TraderId("TESTER-001")
_STRATEGY_PATH = "examples.live.projectx.projectx_data_capture:ProjectXDataCaptureStrategy"
_CONFIG_PATH = "examples.live.projectx.projectx_data_capture:ProjectXDataCaptureStrategyConfig"


def _catalog_counts(path: Path, instrument_id: InstrumentId) -> dict[str, int]:
    if not path.exists():
        return {
            "instruments": 0,
            "quotes": 0,
            "trades": 0,
            "book_deltas": 0,
        }

    catalog = ParquetDataCatalog(str(path))
    identifier = instrument_id.value
    return {
        "instruments": len(catalog.instruments(instrument_ids=[identifier])),
        "quotes": len(catalog.quote_ticks(instrument_ids=[identifier])),
        "trades": len(catalog.trade_ticks(instrument_ids=[identifier])),
        "book_deltas": len(catalog.order_book_deltas(instrument_ids=[identifier])),
    }


def _print_capture_summary(
    summary: dict[str, object],
) -> None:
    strategy_counts = summary["strategy_counts"]
    after_counts = summary["after_counts"]
    print(f"Status: {summary['status']}")
    print(f"Catalog path: {summary['catalog_path']}")
    print(f"Instrument ID: {summary['instrument_id']}")
    print(f"Market data live: {summary['market_data_live']}")
    print(f"Capture seconds: {summary['capture_seconds']}")
    print(
        "Streams enabled: "
        f"quotes={summary['capture_quotes']} "
        f"trades={summary['capture_trades']} "
        f"depth={summary['capture_depth']}",
    )
    print(
        "Strategy counts this run: "
        f"instruments={strategy_counts['instruments']} "
        f"quotes={strategy_counts['quotes']} "
        f"trades={strategy_counts['trades']} "
        f"book_deltas={strategy_counts['book_deltas']} "
        f"book_batches={strategy_counts['book_batches']}",
    )
    print(
        "Catalog totals: "
        f"instruments={after_counts['instruments']} "
        f"quotes={after_counts['quotes']} "
        f"trades={after_counts['trades']} "
        f"book_deltas={after_counts['book_deltas']}",
    )
    print(
        "Catalog delta: "
        f"instruments={summary['catalog_delta']['instruments']} "
        f"quotes={summary['catalog_delta']['quotes']} "
        f"trades={summary['catalog_delta']['trades']} "
        f"book_deltas={summary['catalog_delta']['book_deltas']}",
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


def run_capture(
    *,
    catalog_path: Path,
    instrument_id: InstrumentId,
    capture_seconds: float,
    market_data_live: bool,
    ready_timeout_seconds: float = 60.0,
    first_data_wait_seconds: float = 10.0,
    depth_levels: int = 10,
    capture_quotes: bool = True,
    capture_trades: bool = True,
    capture_depth: bool = True,
    unsubscribe_on_stop: bool = True,
    log_data: bool = False,
) -> dict[str, object]:
    catalog_path = catalog_path.expanduser()
    catalog_path.mkdir(parents=True, exist_ok=True)
    before_counts = _catalog_counts(catalog_path, instrument_id)
    state_key = f"projectx-data-capture-{uuid4().hex}"
    register_capture_state(
        key=state_key,
        instrument_id=instrument_id.value,
        market_data_live=market_data_live,
    )

    node = (
        LiveNode.builder("TESTER-001", TRADER_ID, Environment.LIVE)
        .with_reconciliation(False)
        .with_timeout_connection(20)
        .with_timeout_disconnection_secs(10)
        .with_delay_post_stop_secs(5)
        .add_data_client(
            None,
            ProjectXDataClientFactory(),
            ProjectXDataClientConfig(
                user_name=None,
                api_key=None,
                http_timeout_secs=30,
                market_data_live=market_data_live,
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
                "client_id": PROJECTX_CLIENT_ID.value,
                "strategy_id": f"PROJECTX-DATA-CAPTURE-{uuid4().hex[:8].upper()}",
                "state_key": state_key,
                "subscribe_quotes": capture_quotes,
                "subscribe_trades": capture_trades,
                "subscribe_depth": capture_depth,
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

        if not wait_for_event_or_cancel(
            lambda timeout: wait_for_capture_instrument(state_key, timeout),
            ready_timeout_seconds,
            cancel=monitor_cancel,
        ):
            if monitor_cancel.is_set():
                return

            timed_out = True
            timeout_snapshot = snapshot_capture_state(state_key)
            timeout_error = (
                "Timed out waiting for ProjectX instrument response for "
                f"{instrument_id}. lifecycle={timeout_snapshot['lifecycle']} "
                f"errors={timeout_snapshot['errors']}"
            )
            interrupt_live_node_run()
            return

        initial_wait = min(first_data_wait_seconds, max(capture_seconds, 0.0))

        if initial_wait > 0 and not wait_for_event_or_cancel(
            lambda timeout: wait_for_capture_data(state_key, timeout),
            initial_wait,
            cancel=monitor_cancel,
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

    after_counts = _catalog_counts(catalog_path, instrument_id)
    parquet_files = sorted((catalog_path / "data").rglob("*.parquet"))
    summary = {
        "catalog_path": str(catalog_path),
        "instrument_id": instrument_id.value,
        "market_data_live": market_data_live,
        "capture_seconds": capture_seconds,
        "status": "timeout" if timed_out else "ok",
        "capture_quotes": capture_quotes,
        "capture_trades": capture_trades,
        "capture_depth": capture_depth,
        "depth_levels": depth_levels,
        "before_counts": before_counts,
        "after_counts": after_counts,
        "catalog_delta": {key: after_counts[key] - before_counts[key] for key in before_counts},
        "strategy_counts": {
            "instruments": 1 if snapshot["instrument"] is not None else 0,
            "instrument_events": snapshot["instrument_events"],
            "quotes": snapshot["quote_count"],
            "trades": snapshot["trade_count"],
            "book_batches": snapshot["book_batch_count"],
            "book_deltas": snapshot["book_delta_count"],
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
        instrument_id = INSTRUMENT_ID
    except Exception as exc:
        print(f"ProjectX instrument resolution failed: {exc}")

        if FAIL_ON_TIMEOUT:
            raise SystemExit(1) from None
        return

    summary = run_capture(
        catalog_path=CATALOG_PATH,
        instrument_id=instrument_id,
        capture_seconds=CAPTURE_SECONDS,
        market_data_live=MARKET_DATA_LIVE,
        ready_timeout_seconds=READY_TIMEOUT_SECONDS,
        first_data_wait_seconds=FIRST_DATA_WAIT_SECONDS,
        depth_levels=DEPTH_LEVELS,
        capture_quotes=CAPTURE_QUOTES,
        capture_trades=CAPTURE_TRADES,
        capture_depth=CAPTURE_DEPTH,
        unsubscribe_on_stop=UNSUBSCRIBE_ON_STOP,
        log_data=LOG_DATA,
    )

    if FAIL_ON_TIMEOUT and summary["status"] != "ok":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
