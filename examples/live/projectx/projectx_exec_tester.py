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

import os
import sys
import threading
import time
from pathlib import Path
from typing import TYPE_CHECKING

_REPO_ROOT = str(Path(__file__).resolve().parents[3])
sys.path[:] = [_REPO_ROOT, *(path for path in sys.path if path != _REPO_ROOT)]

from nautilus_trader._libnautilus.common import Environment
from nautilus_trader._libnautilus.model import (
    AccountType,
    OrderSide,
    TimeInForce,
    TraderId,
)
from nautilus_trader.adapters.projectx import (
    PROJECTX_CLIENT_ID,
    ProjectXDataClientConfig,
    ProjectXDataClientFactory,
    ProjectXExecClientConfig,
    ProjectXExecutionClientFactory,
    load_projectx_env,
)
from nautilus_trader.live import LiveNode
from nautilus_trader.model import InstrumentId

from examples.live.live_node_run_helpers import (
    interrupt_live_node_run,
    sleep_or_cancel,
    start_live_node_monitor,
    wait_for_event_or_cancel,
)
from examples.live.projectx.projectx_exec_strategy import (
    clear_exec_state,
    register_exec_state,
    snapshot_exec_state,
    wait_for_exec_instrument,
    wait_for_exec_stop,
    wait_for_exec_terminal,
)

if TYPE_CHECKING:
    from nautilus_trader.config import ImportableStrategyConfig
else:
    from nautilus_trader._libnautilus.trading import ImportableStrategyConfig


# *** THIS IS A LIVE CLIENT SMOKE TEST WITH NO ALPHA ADVANTAGE WHATSOEVER. ***
# *** IT IS NOT INTENDED TO BE USED TO TRADE LIVE WITH REAL MONEY. ***

load_projectx_env()


def _resolve_exec_account_id() -> str:
    return (
        os.getenv("PROJECTX_EXEC_ACCOUNT_ID")
        or os.getenv("PROJECTX_ACCOUNT_ID")
        or "PRAC-V2-EXAMPLE-ACCOUNT"
    )


MARKET_DATA_LIVE = False
INSTRUMENT_ID = InstrumentId.from_str("MNQM26.PROJECTX")
ACCOUNT_ID = _resolve_exec_account_id()
ORDER_QTY = "1"
ENTRY_SIDE = OrderSide.BUY
ENTRY_TIME_IN_FORCE = TimeInForce.GTC
FLATTEN_AFTER_FILL = True
LOG_DATA = False
READY_TIMEOUT_SECONDS = 30.0
TERMINAL_TIMEOUT_SECONDS = 45.0
FLATTEN_SETTLE_SECONDS = 2.0
TRADER_ID = TraderId("TESTER-001")


def _build_node(instrument_id: InstrumentId, state_key: str) -> LiveNode:
    data_client_config = ProjectXDataClientConfig(
        user_name=None,  # Uses PROJECTX_USERNAME
        api_key=None,  # Uses PROJECTX_API_KEY
        http_timeout_secs=30,
        market_data_live=MARKET_DATA_LIVE,
    )

    exec_client_config = ProjectXExecClientConfig(
        trader_id=TRADER_ID.value,
        account_id=ACCOUNT_ID,
        user_name=None,  # Uses PROJECTX_USERNAME
        api_key=None,  # Uses PROJECTX_API_KEY
        http_timeout_secs=30,
        account_type=AccountType.MARGIN,
    )

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
            ProjectXDataClientFactory(),
            data_client_config,
        )
        .add_exec_client(
            None,
            ProjectXExecutionClientFactory(),
            exec_client_config,
        )
        .build()
    )

    node.add_strategy_from_config(
        ImportableStrategyConfig(
            strategy_path="examples.live.projectx.projectx_exec_strategy:ProjectXExecStrategy",
            config_path="examples.live.projectx.projectx_exec_strategy:ProjectXExecStrategyConfig",
            config={
                "instrument_id": str(instrument_id),
                "client_id": PROJECTX_CLIENT_ID.value,
                "account_id": ACCOUNT_ID,
                "strategy_id": "PROJECTX-EXEC-001",
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
    print(f"ProjectX market_data_live: {summary['market_data_live']}")
    print(f"ProjectX execution account: {summary['account_id']}")
    print(f"ProjectX instrument: {summary['instrument_id']}")
    print(f"ProjectX order qty: {summary['order_qty']}")
    print(f"ProjectX entry side: {summary['entry_side']}")
    print(f"ProjectX entry time in force: {summary['entry_time_in_force']}")
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


def run_exec_smoke(instrument_id: InstrumentId) -> dict[str, object]:
    state_key = f"projectx-exec-smoke-{int(time.time() * 1_000_000)}"
    register_exec_state(
        key=state_key,
        instrument_id=instrument_id.value,
        account_id=ACCOUNT_ID,
    )
    node = _build_node(instrument_id, state_key)
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
            "market_data_live": MARKET_DATA_LIVE,
            "account_id": ACCOUNT_ID,
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


if __name__ == "__main__":
    instrument_id = INSTRUMENT_ID
    print("ProjectX environment: topstep (pinned)")
    print(
        "ProjectX accepts raw Topstep account labels and canonicalizes them internally."
    )
    print("ProjectX execution subscribes and reconciles only the configured account.")
    print(
        "ProjectX startup cleanup is account-scoped and waits for prior exposure to flatten before submitting a new entry order.",
    )
    print(
        "ProjectX stop cleanup skips redundant inflight cancels to reduce shutdown noise."
    )
    print(
        "ProjectX contracts are translated from venue IDs like CON.F.US.MES.M26 "
        "into Nautilus IDs using the resolved public contract symbol before the strategy starts.",
    )
    print()
    summary = run_exec_smoke(instrument_id)

    if summary["status"] != "ok":
        raise SystemExit(1)
