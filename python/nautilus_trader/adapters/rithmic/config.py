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

from typing import Any

from nautilus_trader._libnautilus.rithmic import RithmicDataClientConfig
from nautilus_trader._libnautilus.rithmic import RithmicEnv
from nautilus_trader._libnautilus.rithmic import (
    get_rithmic_adapter_account_id as _binding_get_rithmic_adapter_account_id,
)
from nautilus_trader._libnautilus.rithmic import (
    get_rithmic_data_client_id as _binding_get_rithmic_data_client_id,
)
from nautilus_trader._libnautilus.rithmic import (
    get_rithmic_exec_client_id as _binding_get_rithmic_exec_client_id,
)
from nautilus_trader._libnautilus.rithmic import (
    get_rithmic_profiles_from_env as _binding_get_rithmic_profiles_from_env,
)
from nautilus_trader._libnautilus.rithmic import (
    normalize_rithmic_client_component as _binding_normalize_rithmic_client_component,
)
from nautilus_trader._libnautilus.rithmic import parse_rithmic_env as _binding_parse_rithmic_env


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

    Values are stored in an adapter-owned cache; the process environment is not mutated and any
    existing process value keeps precedence.

    """
    return int(RithmicDataClientConfig.load_env_file(path))


def normalize_rithmic_client_component(value: str) -> str:
    return _binding_normalize_rithmic_client_component(value)


def get_rithmic_data_client_id(system_name: str) -> str:
    return _binding_get_rithmic_data_client_id(system_name)


def get_rithmic_exec_client_id(system_name: str, account_id: str) -> str:
    return _binding_get_rithmic_exec_client_id(system_name, account_id)


def get_rithmic_adapter_account_id(system_name: str, account_id: str) -> str:
    return _binding_get_rithmic_adapter_account_id(system_name, account_id)


def get_rithmic_profiles_from_env() -> list[str]:
    return list(_binding_get_rithmic_profiles_from_env())


def parse_rithmic_env(value: str | None) -> RithmicEnv:
    """
    Parse a Rithmic environment token into the PyO3 `RithmicEnv` type.
    """
    return _binding_parse_rithmic_env(value)


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


def to_binding_environment(environment: Any) -> RithmicEnv:
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
