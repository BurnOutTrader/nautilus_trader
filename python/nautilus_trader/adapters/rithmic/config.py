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
Configuration helpers for the Rithmic adapter.
"""

from __future__ import annotations

import os
from typing import Any

from nautilus_trader._libnautilus.rithmic import RithmicDataClientConfig
from nautilus_trader._libnautilus.rithmic import RithmicEnv


RITHMIC_PROFILES_ENV = "RITHMIC_PROFILES"
RITHMIC_ENV_ENV = "RITHMIC_ENV"
RITHMIC_USERNAME_ENV = "RITHMIC_USERNAME"
RITHMIC_PASSWORD_ENV = "RITHMIC_PASSWORD"
RITHMIC_SYSTEM_NAME_ENV = "RITHMIC_SYSTEM_NAME"
RITHMIC_APP_NAME_ENV = "RITHMIC_APP_NAME"
RITHMIC_APP_VERSION_ENV = "RITHMIC_APP_VERSION"
RITHMIC_ACCOUNT_ID_ENV = "RITHMIC_ACCOUNT_ID"
RITHMIC_FCM_ID_ENV = "RITHMIC_FCM_ID"
RITHMIC_IB_ID_ENV = "RITHMIC_IB_ID"
RITHMIC_SERVER_ENV = "RITHMIC_SERVER"


def load_rithmic_env_file(path: str | None = None) -> int:
    """
    Load `RITHMIC_*` keys from a dotenv file via the PyO3 bindings.

    Existing environment variables are not overwritten.

    """
    return int(RithmicDataClientConfig.load_env_file(path))


def _normalize_profile_token(profile: str) -> str:
    if "," in profile:
        raise ValueError(
            f"Multiple Rithmic env profiles must be configured via {RITHMIC_PROFILES_ENV}",
        )
    normalized = "".join(char.upper() if char.isalnum() else "_" for char in profile.strip())
    normalized = "_".join(part for part in normalized.split("_") if part)

    if not normalized:
        raise ValueError("Rithmic env profile cannot be empty")
    return normalized


def normalize_rithmic_client_component(value: str) -> str:
    normalized = _normalize_profile_token(value)

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
    return f"{client_id}-{account_id}"


def _candidate_env_keys(
    key: str,
    profile: str | None = None,
) -> list[str]:
    candidates: list[str] = []

    if profile:
        candidates.append(f"RITHMIC_{_normalize_profile_token(profile)}_{key}")
    candidates.append(f"RITHMIC_{key}")
    return candidates


def _optional_env(
    key: str,
    profile: str | None = None,
) -> str | None:
    for candidate in _candidate_env_keys(key, profile):
        value = os.environ.get(candidate)

        if value:
            return value
    return None


def _required_env(
    key: str,
    profile: str | None = None,
) -> str:
    value = _optional_env(key, profile)

    if value:
        return value
    missing_key = _candidate_env_keys(key, profile)[0]
    raise ValueError(f"{missing_key} environment variable not set")


def _optional_int_env(
    key: str,
    profile: str | None = None,
) -> int | None:
    value = _optional_env(key, profile)

    if value is None:
        return None
    return int(value)


def get_rithmic_profiles_from_env() -> list[str]:
    value = os.environ.get(RITHMIC_PROFILES_ENV)

    if not value:
        return []

    profiles: list[str] = []
    seen: set[str] = set()

    for profile in (part.strip() for part in value.split(",")):
        if not profile:
            continue
        normalized = _normalize_profile_token(profile)

        if normalized in seen:
            continue
        seen.add(normalized)
        profiles.append(profile)
    return profiles


def parse_rithmic_env(value: str | None) -> Any:
    """
    Parse a Rithmic environment token into the PyO3 `RithmicEnv` type.
    """
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


def rithmic_env_token(environment: Any) -> str:
    """
    Return the canonical lowercase token for a Rithmic environment value.
    """
    if environment is None:
        return "demo"

    normalized = to_binding_environment(environment)
    token = str(normalized).rpartition(".")[2].lower()

    if token in {"demo", "live", "test"}:
        return token

    raise TypeError(f"Cannot normalize Rithmic environment token from {environment!r}")


def to_binding_environment(environment: Any) -> Any:
    """
    Normalize supported environment tokens into the PyO3 `RithmicEnv` type.
    """
    if isinstance(environment, RithmicEnv):
        return environment

    if isinstance(environment, str):
        return parse_rithmic_env(environment)

    name = getattr(environment, "name", None)

    if isinstance(name, str) and hasattr(RithmicEnv, name):
        return getattr(RithmicEnv, name)

    raise TypeError(f"Cannot convert Rithmic environment {environment!r}")
