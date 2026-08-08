#!/usr/bin/env python3
# mypy: disable-error-code="arg-type,attr-defined,index"
# ruff: noqa: E402
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

from examples.live.live_node_run_helpers import interrupt_live_node_run
from examples.live.live_node_run_helpers import sleep_or_cancel
from examples.live.live_node_run_helpers import start_live_node_monitor
from examples.live.live_node_run_helpers import wait_for_event_or_cancel

from examples.live.rithmic.rithmic_exec_strategy import clear_exec_state
from examples.live.rithmic.rithmic_exec_strategy import register_exec_state
from examples.live.rithmic.rithmic_exec_strategy import snapshot_exec_state
from examples.live.rithmic.rithmic_exec_strategy import wait_for_exec_instrument
from examples.live.rithmic.rithmic_exec_strategy import wait_for_exec_stop
from examples.live.rithmic.rithmic_exec_strategy import wait_for_exec_terminal
from examples.live.rithmic.rithmic_live_node_helpers import TRADER_ID
from examples.live.rithmic.rithmic_live_node_helpers import RithmicDataClientFactory
from examples.live.rithmic.rithmic_live_node_helpers import RithmicExecClientFactory
from examples.live.rithmic.rithmic_live_node_helpers import build_adapter_account_id
from examples.live.rithmic.rithmic_live_node_helpers import build_data_client_config
from examples.live.rithmic.rithmic_live_node_helpers import build_data_client_id
from examples.live.rithmic.rithmic_live_node_helpers import build_exec_client_config
from examples.live.rithmic.rithmic_live_node_helpers import build_exec_client_id
from examples.live.rithmic.rithmic_live_node_helpers import load_rithmic_env_file
from nautilus_trader._libnautilus.model import OrderSide
from nautilus_trader._libnautilus.model import TimeInForce
from nautilus_trader._libnautilus.common import Environment
from nautilus_trader.live import LiveNode
from nautilus_trader.model import InstrumentId


if TYPE_CHECKING:
    from nautilus_trader.config import ImportableStrategyConfig
else:
    from nautilus_trader._libnautilus.trading import ImportableStrategyConfig


# *** THIS IS A LIVE CLIENT SMOKE TEST WITH NO ALPHA ADVANTAGE WHATSOEVER. ***
# *** IT IS NOT INTENDED TO BE USED TO TRADE LIVE WITH REAL MONEY. ***

load_rithmic_env_file()

PROFILE = None
INSTRUMENT_ID = InstrumentId.from_str("MNQM6.RITHMIC")
ORDER_QTY = "1"
ENTRY_SIDE = OrderSide.BUY
ENTRY_TIME_IN_FORCE = TimeInForce.IOC
FLATTEN_AFTER_FILL = True
LOG_DATA = False
READY_TIMEOUT_SECONDS = 30.0
TERMINAL_TIMEOUT_SECONDS = 45.0
FLATTEN_SETTLE_SECONDS = 2.0

_STRATEGY_PATH = "examples.live.rithmic.rithmic_exec_strategy:RithmicExecStrategy"
_CONFIG_PATH = "examples.live.rithmic.rithmic_exec_strategy:RithmicExecStrategyConfig"


def _build_node(profile: str | None, instrument_id: InstrumentId, state_key: str) -> LiveNode:
    data_config = build_data_client_config(profile)
    exec_config = build_exec_client_config(profile)
    data_client_id = build_data_client_id(profile)
    exec_client_id = build_exec_client_id(profile)
    adapter_account_id = build_adapter_account_id(profile)

    node = (
        LiveNode.builder("TESTER-001", TRADER_ID, Environment.LIVE)
        .with_reconciliation(True)
        .with_timeout_connection(20)
        .with_timeout_reconciliation(10)
        .with_timeout_portfolio(10)
        .with_timeout_disconnection_secs(10)
        .with_delay_post_stop_secs(5)
        .add_data_client(
            None,
            RithmicDataClientFactory(),
            data_config,
        )
        .add_exec_client(
            None,
            RithmicExecClientFactory(),
            exec_config,
        )
        .build()
    )

    node.add_strategy_from_config(
        ImportableStrategyConfig(
            strategy_path=_STRATEGY_PATH,
            config_path=_CONFIG_PATH,
            config={
                "instrument_id": str(instrument_id),
                "data_client_id": data_client_id,
                "exec_client_id": exec_client_id,
                "account_id": adapter_account_id,
                "strategy_id": "RITHMIC-EXEC-001",
                "state_key": state_key,
                "entry_side": str(ENTRY_SIDE),
                "entry_qty": ORDER_QTY,
                "entry_time_in_force": str(ENTRY_TIME_IN_FORCE),
                "cleanup_on_start": True,
                "cleanup_on_stop": True,
                "flatten_after_fill": FLATTEN_AFTER_FILL,
                "subscribe_quotes": True,
                "subscribe_trades": True,
                "unsubscribe_on_stop": True,
                "log_data": LOG_DATA,
                "log_events": True,
                "log_commands": True,
            },
        ),
    )
    return node


def _print_exec_summary(summary: dict[str, object]) -> None:
    print(f"Status: {summary['status']}")
    print(f"Rithmic data client ID: {summary['data_client_id']}")
    print(f"Rithmic exec client ID: {summary['exec_client_id']}")
    print(f"Rithmic execution account: {summary['account_id']}")
    print(f"Rithmic instrument: {summary['instrument_id']}")
    print(f"Rithmic order qty: {summary['order_qty']}")
    print(f"Rithmic entry side: {summary['entry_side']}")
    print(f"Rithmic entry time in force: {summary['entry_time_in_force']}")
    print(f"Instrument ready: {summary['instrument_ready']}")
    print(f"Terminal status: {summary['terminal_status']}")

    if summary["terminal_reason"] is not None:
        print(f"Terminal reason: {summary['terminal_reason']}")
    print(
        "Order counts: "
        f"submitted={summary['order_counts']['submitted']} "
        f"accepted={summary['order_counts']['accepted']} "
        f"rejected={summary['order_counts']['rejected']} "
        f"canceled={summary['order_counts']['canceled']} "
        f"filled={summary['order_counts']['filled']}",
    )
    print(f"Position events: {summary['position_event_count']}")

    if summary["lifecycle"]:
        print(f"Lifecycle: {', '.join(summary['lifecycle'])}")

    if summary["errors"]:
        print(f"Errors: {summary['errors']}")


def run_exec_smoke(  # noqa: C901
    profile: str | None,
    instrument_id: InstrumentId,
) -> dict[str, object]:
    data_client_id = build_data_client_id(profile)
    exec_client_id = build_exec_client_id(profile)
    adapter_account_id = build_adapter_account_id(profile)
    state_key = f"rithmic-exec-smoke-{int(time.time() * 1_000_000)}"
    register_exec_state(
        key=state_key,
        instrument_id=instrument_id.value,
        account_id=adapter_account_id,
    )
    node = _build_node(profile, instrument_id, state_key)
    instrument_ready = False
    terminal_observed = False
    monitor_cancel = threading.Event()

    def monitor_run() -> None:
        nonlocal instrument_ready, terminal_observed

        instrument_ready = wait_for_event_or_cancel(
            lambda timeout: wait_for_exec_instrument(state_key, timeout),
            READY_TIMEOUT_SECONDS,
            cancel=monitor_cancel,
        )

        if monitor_cancel.is_set() or not instrument_ready:
            if not monitor_cancel.is_set():
                interrupt_live_node_run()
            return

        terminal_observed = wait_for_event_or_cancel(
            lambda timeout: wait_for_exec_terminal(state_key, timeout),
            TERMINAL_TIMEOUT_SECONDS,
            cancel=monitor_cancel,
        )

        if monitor_cancel.is_set():
            return

        snapshot = snapshot_exec_state(state_key)

        if (
            terminal_observed
            and snapshot["terminal_status"] == "filled"
            and FLATTEN_AFTER_FILL
            and FLATTEN_SETTLE_SECONDS > 0
        ):
            sleep_or_cancel(FLATTEN_SETTLE_SECONDS, cancel=monitor_cancel)

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
            wait_for_exec_stop(state_key, timeout=5)

        snapshot = snapshot_exec_state(state_key)
        status = "ok"
        terminal_reason = snapshot["terminal_reason"]

        if snapshot["started_at_unix"] is None:
            status = "startup_failed"
            terminal_reason = terminal_reason or (
                "Strategy did not start; check LiveNode logs for client connection failures."
            )
        elif not instrument_ready:
            status = "instrument_timeout"
        elif not terminal_observed:
            status = "timeout"
        elif snapshot["terminal_status"] != "filled":
            status = str(snapshot["terminal_status"] or "unknown")

        summary = {
            "status": status,
            "data_client_id": data_client_id,
            "exec_client_id": exec_client_id,
            "account_id": adapter_account_id,
            "instrument_id": instrument_id.value,
            "order_qty": ORDER_QTY,
            "entry_side": str(ENTRY_SIDE),
            "entry_time_in_force": str(ENTRY_TIME_IN_FORCE),
            "instrument_ready": snapshot["instrument_ready"],
            "terminal_status": snapshot["terminal_status"],
            "terminal_reason": terminal_reason,
            "order_counts": {
                "submitted": snapshot["order_submitted_count"],
                "accepted": snapshot["order_accepted_count"],
                "rejected": snapshot["order_rejected_count"],
                "canceled": snapshot["order_canceled_count"],
                "filled": snapshot["order_filled_count"],
            },
            "position_event_count": snapshot["position_event_count"],
            "started_at_unix": snapshot["started_at_unix"],
            "stopped_at_unix": snapshot["stopped_at_unix"],
            "last_event_at_unix": snapshot["last_event_at_unix"],
            "lifecycle": snapshot["lifecycle"],
            "errors": snapshot["errors"],
        }
        _print_exec_summary(summary)
        return summary
    finally:
        clear_exec_state(state_key)


def main() -> None:
    instrument_id = INSTRUMENT_ID
    summary = run_exec_smoke(PROFILE, instrument_id)

    if summary["status"] != "ok":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
