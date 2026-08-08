#!/usr/bin/env python3
# mypy: disable-error-code="arg-type,attr-defined,index"
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

import sys
import threading
import time
from pathlib import Path
from typing import TYPE_CHECKING

_REPO_ROOT = Path(__file__).resolve().parents[3]

if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from nautilus_trader._libnautilus.common import Environment
from nautilus_trader.live import LiveNode
from nautilus_trader.model import BarType, InstrumentId

from examples.live.live_node_run_helpers import (
    interrupt_live_node_run,
    sleep_or_cancel,
    start_live_node_monitor,
    wait_for_event_or_cancel,
)
from examples.live.rithmic.rithmic_data_probe import (
    clear_probe_state,
    register_probe_state,
    snapshot_probe_state,
    wait_for_probe_data,
    wait_for_probe_stop,
)
from examples.live.rithmic.rithmic_live_node_helpers import (
    TRADER_ID,
    RithmicDataClientFactory,
    build_data_client_config,
    build_data_client_id,
    load_rithmic_env_file,
)

if TYPE_CHECKING:
    from nautilus_trader.config import ImportableStrategyConfig
else:
    from nautilus_trader._libnautilus.trading import ImportableStrategyConfig


# *** THIS IS A LIVE DATA PROBE WITH NO ALPHA ADVANTAGE WHATSOEVER. ***
# *** IT IS NOT INTENDED TO BE USED TO TRADE LIVE WITH REAL MONEY.    ***
# *** request_bars=True uses the Rithmic historical API. On basic plans, historical usage is   ***
# *** typically capped at 20 GB per month. Watch the registered Rithmic email inbox for usage  ***
# *** warnings because ignoring them can trigger automatic temporary restrictions.              ***

load_rithmic_env_file()

PROFILE = None
INSTRUMENT_ID = InstrumentId.from_str("MNQM6.CME.RITHMIC")
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

_STRATEGY_PATH = "examples.live.rithmic.rithmic_data_probe:RithmicDataProbeStrategy"
_CONFIG_PATH = "examples.live.rithmic.rithmic_data_probe:RithmicDataProbeStrategyConfig"


def _print_probe_summary(summary: dict[str, object]) -> None:
    strategy_counts = summary["strategy_counts"]
    print(f"Status: {summary['status']}")
    print(f"Instrument ID: {summary['instrument_id']}")
    print(f"Data client ID: {summary['data_client_id']}")
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
        f"bars={strategy_counts['bars']} "
        f"historical_bars={strategy_counts['historical_bars']}",
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


def run_probe(
    *,
    profile: str | None,
    instrument_id: InstrumentId,
    bar_type: BarType,
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
    data_client_id = build_data_client_id(profile)
    history_enabled = capture_bars and bar_type.is_externally_aggregated()
    state_key = f"rithmic-data-probe-{int(time.time() * 1_000_000)}"
    register_probe_state(
        key=state_key,
        instrument_id=instrument_id.value,
    )

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
                "strategy_id": f"RITHMIC-DATA-PROBE-{int(time.time())}",
                "state_key": state_key,
                "bar_type": str(bar_type) if capture_bars else None,
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

    def monitor_run() -> None:
        nonlocal initial_wait_warning, timed_out, timeout_error

        ready_deadline = time.monotonic() + ready_timeout_seconds
        ready_snapshot = snapshot_probe_state(state_key)

        while (
            not monitor_cancel.is_set()
            and time.monotonic() < ready_deadline
            and not ready_snapshot["instrument_ready"]
            and not ready_snapshot["data_seen"]
        ):
            time.sleep(0.1)
            ready_snapshot = snapshot_probe_state(state_key)

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
            initial_wait_warning = "Received Rithmic data before the instrument response; continuing probe."

        capture_deadline = time.monotonic() + capture_seconds
        initial_wait = min(first_data_wait_seconds, max(capture_seconds, 0.0))

        if (
            initial_wait > 0
            and not ready_snapshot["data_seen"]
            and not wait_for_event_or_cancel(
                lambda timeout: wait_for_probe_data(state_key, timeout),
                initial_wait,
                cancel=monitor_cancel,
            )
        ):
            if monitor_cancel.is_set():
                return
            initial_wait_warning = (
                "No live data observed during initial wait window; continuing probe."
            )

        remaining = capture_deadline - time.monotonic()

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
            wait_for_probe_stop(state_key, timeout=5)

        snapshot = snapshot_probe_state(state_key)
        summary = {
            "instrument_id": instrument_id.value,
            "data_client_id": data_client_id,
            "capture_seconds": capture_seconds,
            "status": "timeout" if timed_out else "ok",
            "capture_quotes": capture_quotes,
            "capture_trades": capture_trades,
            "capture_depth": capture_depth,
            "capture_bars": capture_bars,
            "request_bars": request_bars,
            "depth_levels": depth_levels,
            "strategy_counts": {
                "instruments": snapshot["instrument_count"],
                "quotes": snapshot["quote_count"],
                "trades": snapshot["trade_count"],
                "book_deltas": snapshot["book_delta_count"],
                "bars": snapshot["bar_count"],
                "historical_bars": snapshot["historical_bar_count"],
            },
            "instrument_ready": snapshot["instrument_ready"],
            "saw_data": snapshot["data_seen"],
            "started_at_unix": snapshot["started_at_unix"],
            "stopped_at_unix": snapshot["stopped_at_unix"],
            "last_event_at_unix": snapshot["last_event_at_unix"],
            "lifecycle": snapshot["lifecycle"],
            "errors": snapshot["errors"],
            "warning": initial_wait_warning,
            "error": timeout_error,
        }
        _print_probe_summary(summary)
        return summary
    finally:
        clear_probe_state(state_key)


def main() -> None:
    instrument_id = INSTRUMENT_ID
    bar_type = BarType.from_str(f"{instrument_id}-{BAR_SPEC}")
    request_bars = REQUEST_BARS and bar_type.is_externally_aggregated()

    summary = run_probe(
        profile=PROFILE,
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
        request_bars=request_bars,
        historical_lookback_minutes=HISTORICAL_LOOKBACK_MINUTES,
        unsubscribe_on_stop=UNSUBSCRIBE_ON_STOP,
        log_data=LOG_DATA,
    )

    if FAIL_ON_TIMEOUT and summary["status"] != "ok":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
