#!/usr/bin/env python3
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

import os
import threading
from pathlib import Path
from typing import TYPE_CHECKING

from examples.live.live_node_run_helpers import schedule_live_node_interrupt

from nautilus_trader.adapters.rithmic import RithmicDataClientConfig
from nautilus_trader.adapters.rithmic import RithmicDataClientFactory
from nautilus_trader.adapters.rithmic import RithmicEnv
from nautilus_trader.adapters.rithmic import RithmicExecClientConfig
from nautilus_trader.adapters.rithmic import RithmicExecClientFactory
from nautilus_trader.adapters.rithmic import load_rithmic_env_file as load_binding_rithmic_env_file
from nautilus_trader.adapters.rithmic import resolve_front_month_instrument_id
from nautilus_trader._libnautilus.model import TraderId
from nautilus_trader.live import LiveNode
from nautilus_trader.model import InstrumentId


if TYPE_CHECKING:
    pass
else:
    pass


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
    "build_data_client_config",
    "build_data_client_id",
    "build_exec_client_config",
    "build_exec_client_id",
    "get_rithmic_adapter_account_id",
    "get_rithmic_data_client_id",
    "get_rithmic_exec_client_id",
    "load_rithmic_env_file",
    "resolve_front_month_instrument_id",
    "resolve_instrument_id",
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


def _sync_python_env_file(path: Path) -> None:
    if not path.exists():
        return

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()

        if not line or line.startswith("#"):
            continue

        if line.startswith("export "):
            line = line[7:].lstrip()

        if "=" not in line:
            continue

        key, raw_value = line.split("=", 1)
        key = key.strip()

        if not key:
            continue

        value = raw_value.strip()

        if value and value[0] in {'"', "'"} and value[-1] == value[0]:
            value = value[1:-1]
        else:
            value = value.split(" #", 1)[0].rstrip()

        os.environ[key] = value


def load_rithmic_env_file(path: str | None = None) -> int:
    resolved_path = _resolve_env_file_path(path)

    if resolved_path is not None:
        _sync_python_env_file(resolved_path)

    return int(load_binding_rithmic_env_file(None if resolved_path is None else str(resolved_path)))


def normalize_rithmic_client_component(value: str) -> str:
    normalized = "".join(char.upper() if char.isalnum() else "_" for char in value.strip())
    normalized = "_".join(part for part in normalized.split("_") if part)

    if not normalized:
        raise ValueError("Rithmic client component cannot be empty")

    if "-" in normalized:
        raise ValueError("Normalized Rithmic client component must not contain '-'")
    return normalized


def get_rithmic_data_client_id(system_name: str) -> str:
    return normalize_rithmic_client_component(system_name)


def get_rithmic_exec_client_id(system_name: str, account_id: str) -> str:
    system_key = normalize_rithmic_client_component(system_name)
    account_key = normalize_rithmic_client_component(account_id)
    return f"{system_key}_{account_key}"


def get_rithmic_adapter_account_id(system_name: str, account_id: str) -> str:
    client_id = get_rithmic_exec_client_id(system_name, account_id)
    return f"RITHMIC-{client_id}-{account_id}"


def _candidate_env_keys(key: str, profile: str | None = None) -> list[str]:
    candidates: list[str] = []

    if profile:
        candidates.append(f"RITHMIC_{normalize_rithmic_client_component(profile)}_{key}")
    candidates.append(f"RITHMIC_{key}")
    return candidates


def _required_env(key: str, profile: str | None = None) -> str:
    for candidate in _candidate_env_keys(key, profile):
        value = os.environ.get(candidate)

        if value:
            return value
    missing_key = _candidate_env_keys(key, profile)[0]
    raise ValueError(f"{missing_key} environment variable not set")


def _optional_env(key: str, profile: str | None = None) -> str | None:
    for candidate in _candidate_env_keys(key, profile):
        value = os.environ.get(candidate)

        if value:
            return value
    return None


def _parse_rithmic_env(value: str | None) -> object:
    if value is None:
        return RithmicEnv.DEMO

    token = value.strip().lower()

    if token in {"demo", "paper"}:
        return RithmicEnv.DEMO

    if token in {"live", "prod", "production"}:
        return RithmicEnv.LIVE

    if token == "test":
        return RithmicEnv.TEST

    raise ValueError(f"Invalid Rithmic environment {value!r}; expected demo, live, or test")


def build_data_client_config(
    profile: str | None,
    *,
    enable_history: bool = False,
) -> RithmicDataClientConfig:
    return RithmicDataClientConfig(
        environment=_parse_rithmic_env(_optional_env("ENV", profile)),
        username=_required_env("USERNAME", profile),
        password=_required_env("PASSWORD", profile),
        system_name=_required_env("SYSTEM_NAME", profile),
        app_name=_required_env("APP_NAME", profile),
        app_version=_optional_env("APP_VERSION", profile) or "1.0",
        fcm_id=_optional_env("FCM_ID", profile),
        ib_id=_optional_env("IB_ID", profile),
        server=_optional_env("SERVER", profile),
        alt_server=_optional_env("ALT_SERVER", profile),
        enable_history=enable_history,
    )


def build_exec_client_config(
    profile: str | None,
    *,
    account_id: str | None = None,
) -> RithmicExecClientConfig:
    resolved_account_id = _required_env("ACCOUNT_ID", profile) if account_id is None else account_id
    return RithmicExecClientConfig(
        environment=_parse_rithmic_env(_optional_env("ENV", profile)),
        username=_required_env("USERNAME", profile),
        password=_required_env("PASSWORD", profile),
        system_name=_required_env("SYSTEM_NAME", profile),
        account_id=resolved_account_id,
        trader_id=_optional_env("TRADER_ID", profile) or TRADER_ID.value,
        app_name=_required_env("APP_NAME", profile),
        app_version=_optional_env("APP_VERSION", profile) or "1.0",
        fcm_id=_optional_env("FCM_ID", profile),
        ib_id=_optional_env("IB_ID", profile),
        server=_optional_env("SERVER", profile),
        alt_server=_optional_env("ALT_SERVER", profile),
        execution_replay_lookback_secs=int(
            _optional_env("EXECUTION_REPLAY_LOOKBACK_SECS", profile) or "86400",
        ),
    )


def build_data_client_id(profile: str | None) -> str:
    return get_rithmic_data_client_id(_required_env("SYSTEM_NAME", profile))


def build_exec_client_id(
    profile: str | None,
    *,
    account_id: str | None = None,
) -> str:
    system_name = _required_env("SYSTEM_NAME", profile)
    resolved_account_id = account_id or _required_env("ACCOUNT_ID", profile)
    return get_rithmic_exec_client_id(system_name, resolved_account_id)


def build_adapter_account_id(
    profile: str | None,
    *,
    account_id: str | None = None,
) -> str:
    system_name = _required_env("SYSTEM_NAME", profile)
    resolved_account_id = account_id or _required_env("ACCOUNT_ID", profile)
    return get_rithmic_adapter_account_id(system_name, resolved_account_id)


def schedule_stop(node: LiveNode, run_seconds: int) -> threading.Timer | None:
    del node

    if run_seconds <= 0:
        return None
    return schedule_live_node_interrupt(run_seconds)
