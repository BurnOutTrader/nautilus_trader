# mypy: disable-error-code="attr-defined,valid-type"
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

import threading
from pathlib import Path

from nautilus_trader._libnautilus.model import TraderId
from nautilus_trader.adapters.rithmic import (
    RithmicDataClientConfig,
    RithmicDataClientFactory,
    RithmicEnv,
    RithmicExecClientConfig,
    RithmicExecClientFactory,
    resolve_front_month_instrument_id,
)
from nautilus_trader.adapters.rithmic import (
    load_rithmic_env_file as load_binding_rithmic_env_file,
)
from nautilus_trader.adapters.rithmic.config import (
    get_rithmic_adapter_account_id,
    get_rithmic_data_client_id,
    get_rithmic_exec_client_id,
    normalize_rithmic_client_component,
)
from nautilus_trader.live import LiveNode
from nautilus_trader.model import InstrumentId

from examples.live.live_node_run_helpers import schedule_live_node_interrupt

TRADER_ID = TraderId("TESTER-001")
_REPO_ROOT = Path(__file__).resolve().parents[3]
_DEFAULT_ENV_FILE = _REPO_ROOT / ".env"

__all__ = [
    "TRADER_ID",
    "RithmicDataClientConfig",
    "RithmicDataClientFactory",
    "RithmicEnv",
    "RithmicExecClientConfig",
    "RithmicExecClientFactory",
    "build_adapter_account_id",
    "build_data_client_config",
    "build_data_client_id",
    "build_exec_client_config",
    "build_exec_client_id",
    "get_rithmic_adapter_account_id",
    "get_rithmic_data_client_id",
    "get_rithmic_exec_client_id",
    "load_rithmic_env_file",
    "normalize_rithmic_client_component",
    "resolve_front_month_instrument_id",
    "resolve_instrument_id",
    "schedule_stop",
]


def resolve_instrument_id(
    profile: str | None,
    instrument_id: InstrumentId | None,
    product_code: str,
    exchange: str,
) -> InstrumentId:
    if instrument_id is not None:
        return instrument_id
    return resolve_front_month_instrument_id(profile, product_code, exchange)


def _resolve_env_file_path(path: str | None) -> Path | None:
    if path is None:
        return _DEFAULT_ENV_FILE if _DEFAULT_ENV_FILE.exists() else None

    candidate = Path(path).expanduser()

    if not candidate.is_absolute():
        candidate = Path.cwd() / candidate
    return candidate.resolve()


def load_rithmic_env_file(path: str | None = None) -> int:
    resolved_path = _resolve_env_file_path(path)
    return int(
        load_binding_rithmic_env_file(
            None if resolved_path is None else str(resolved_path)
        )
    )


def build_data_client_config(
    profile: str | None,
    *,
    enable_history: bool = False,
) -> RithmicDataClientConfig:
    return RithmicDataClientConfig.from_env(profile, enable_history=enable_history)


def build_exec_client_config(
    profile: str | None,
    *,
    account_id: str | None = None,
) -> RithmicExecClientConfig:
    return RithmicExecClientConfig.from_env(
        profile,
        account_id=account_id,
        trader_id=TRADER_ID.value,
    )


def build_data_client_id(profile: str | None) -> str:
    config = build_data_client_config(profile)
    return get_rithmic_data_client_id(config.system_name)


def build_exec_client_id(
    profile: str | None,
    *,
    account_id: str | None = None,
) -> str:
    config = build_exec_client_config(profile, account_id=account_id)
    return get_rithmic_exec_client_id(config.system_name, config.account_id)


def build_adapter_account_id(
    profile: str | None,
    *,
    account_id: str | None = None,
) -> str:
    config = build_exec_client_config(profile, account_id=account_id)
    return get_rithmic_adapter_account_id(config.system_name, config.account_id)


def schedule_stop(node: LiveNode, run_seconds: int) -> threading.Timer | None:
    del node

    if run_seconds <= 0:
        return None
    return schedule_live_node_interrupt(run_seconds)
