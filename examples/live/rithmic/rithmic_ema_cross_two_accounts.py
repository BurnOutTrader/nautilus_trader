#!/usr/bin/env python3
# mypy: disable-error-code="attr-defined"
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

"""
Run this workflow to:
1. Uses one shared Rithmic data client
2. Builds two Rithmic execution clients under the same login/system
3. Uses one signal source to copy the same EMA-cross trades into both accounts

Warning:
    This example can submit live orders to both configured accounts.
    Use demo accounts first.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import TYPE_CHECKING


_REPO_ROOT = Path(__file__).resolve().parents[3]

if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from examples.live.rithmic.rithmic_live_node_helpers import TRADER_ID
from examples.live.rithmic.rithmic_live_node_helpers import RithmicDataClientFactory
from examples.live.rithmic.rithmic_live_node_helpers import RithmicExecClientFactory
from examples.live.rithmic.rithmic_live_node_helpers import build_adapter_account_id
from examples.live.rithmic.rithmic_live_node_helpers import build_data_client_config
from examples.live.rithmic.rithmic_live_node_helpers import build_data_client_id
from examples.live.rithmic.rithmic_live_node_helpers import build_exec_client_config
from examples.live.rithmic.rithmic_live_node_helpers import build_exec_client_id
from examples.live.rithmic.rithmic_live_node_helpers import load_rithmic_env_file
from examples.live.rithmic.rithmic_live_node_helpers import normalize_rithmic_client_component
from examples.live.rithmic.rithmic_live_node_helpers import schedule_stop
from nautilus_trader._libnautilus.common import Environment
from nautilus_trader.live import LiveNode
from nautilus_trader.model import BarType
from nautilus_trader.model import InstrumentId


if TYPE_CHECKING:
    from nautilus_trader.config import ImportableStrategyConfig
else:
    from nautilus_trader._libnautilus.trading import ImportableStrategyConfig


load_rithmic_env_file()

PROFILE = None
INSTRUMENT_ID = InstrumentId.from_str("MNQM6.RITHMIC")
BAR_SPEC = "15-SECOND-LAST-EXTERNAL"
TRADE_SIZE = "1"
FAST_EMA_PERIOD = 10
SLOW_EMA_PERIOD = 20
WARMUP_MINUTES = 30
RUN_SECONDS = 0

_STRATEGY_PATH = "examples.live.rithmic.rithmic_ema_cross_strategy:RithmicEMACrossStrategy"
_CONFIG_PATH = "examples.live.rithmic.rithmic_ema_cross_strategy:RithmicEMACrossStrategyConfig"


def _profile_env_candidates(key: str, profile: str | None) -> list[str]:
    candidates: list[str] = []

    if profile:
        profile_token = normalize_rithmic_client_component(profile)
        candidates.append(f"RITHMIC_{profile_token}_{key}")
    candidates.append(f"RITHMIC_{key}")
    return candidates


def _required_secondary_account_id(profile: str | None) -> str:
    for key in _profile_env_candidates("ACCOUNT_ID_2", profile):
        value = os.environ.get(key)

        if value:
            return value

    missing_key = _profile_env_candidates("ACCOUNT_ID_2", profile)[0]
    raise ValueError(f"{missing_key} environment variable not set")


def main() -> None:
    profile = PROFILE
    instrument_id = INSTRUMENT_ID
    bar_type = BarType.from_str(f"{instrument_id}-{BAR_SPEC.strip().upper()}")
    warmup_minutes = int(WARMUP_MINUTES)
    run_seconds = int(RUN_SECONDS)
    secondary_account_id = _required_secondary_account_id(profile)

    if warmup_minutes < 0:
        raise ValueError("WARMUP_MINUTES cannot be negative")

    if run_seconds < 0:
        raise ValueError("RUN_SECONDS cannot be negative")

    history_enabled = bar_type.is_externally_aggregated()
    request_bars = history_enabled and warmup_minutes > 0
    data_client_id = build_data_client_id(profile)
    primary_exec_client_id = build_exec_client_id(profile)
    primary_adapter_account_id = build_adapter_account_id(profile)
    secondary_exec_client_id = build_exec_client_id(profile, account_id=secondary_account_id)
    secondary_adapter_account_id = build_adapter_account_id(
        profile,
        account_id=secondary_account_id,
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
            RithmicDataClientFactory(),
            build_data_client_config(
                profile,
                enable_history=history_enabled,
            ),
        )
        .add_exec_client(
            None,
            RithmicExecClientFactory(),
            build_exec_client_config(profile),
        )
        .add_exec_client(
            None,
            RithmicExecClientFactory(),
            build_exec_client_config(profile, account_id=secondary_account_id),
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
                "strategy_id": "RITHMIC-EMA-COPY-001",
                "bar_type": str(bar_type),
                "trade_size": TRADE_SIZE,
                "fast_ema_period": FAST_EMA_PERIOD,
                "slow_ema_period": SLOW_EMA_PERIOD,
                "warmup_minutes": warmup_minutes,
                "request_bars": request_bars,
                "routes": [
                    {
                        "label": "account-1",
                        "exec_client_id": primary_exec_client_id,
                        "account_id": primary_adapter_account_id,
                    },
                    {
                        "label": "account-2",
                        "exec_client_id": secondary_exec_client_id,
                        "account_id": secondary_adapter_account_id,
                    },
                ],
                "unsubscribe_on_stop": True,
                "cleanup_on_stop": True,
                "log_data": False,
                "log_events": True,
                "log_commands": True,
            },
        ),
    )

    print("Rithmic Live EMA Cross (Two Accounts)")
    print("=" * 50)
    print(f"Profile: {profile or '<default>'}")
    print(f"Configured instrument: {instrument_id}")
    print(f"Data client ID: {data_client_id}")
    print(f"Primary exec/account: {primary_exec_client_id} / {primary_adapter_account_id}")
    print(f"Secondary exec/account: {secondary_exec_client_id} / {secondary_adapter_account_id}")
    print(f"Bar type: {bar_type}")
    print(f"Trade size per account: {TRADE_SIZE}")
    print(f"Fast/slow EMA periods: {FAST_EMA_PERIOD}/{SLOW_EMA_PERIOD}")

    if request_bars:
        print(f"Historical warmup: {warmup_minutes} minutes")
    elif history_enabled:
        print("Historical warmup: disabled (live bars only)")
    else:
        print("Historical warmup: disabled")

    if run_seconds > 0:
        print(f"Auto-stop after: {run_seconds} seconds")
    else:
        print("Auto-stop after: disabled")
    print()
    print("WARNING: this example can submit live orders to both configured accounts.")
    print("Use demo accounts first.")

    stop_timer = schedule_stop(node, run_seconds)
    try:
        node.run()
    finally:
        if stop_timer is not None:
            stop_timer.cancel()


if __name__ == "__main__":
    main()
