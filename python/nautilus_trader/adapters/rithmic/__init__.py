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
Rithmic gateway integration.

This is an independent external adapter. It is not affiliated with, endorsed by, or supported by
Nautech Systems Pty Ltd or the official NautilusTrader project.

The Rust `rithmic_nt` adapter crate owns the transport, state, reconnect, and authentication
logic, while Python re-exports the Rust bindings and adds pure-Python helpers.

"""

from nautilus_trader._fixup import fixup_module_names
from nautilus_trader._libnautilus.rithmic import *  # noqa: F403 (undefined-local-with-import-star)
from nautilus_trader.adapters.rithmic.backtest import build_external_bar_type
from nautilus_trader.adapters.rithmic.backtest import download_bars_to_catalog
from nautilus_trader.adapters.rithmic.backtest import normalize_rithmic_bar_spec
from nautilus_trader.adapters.rithmic.backtest import resolve_catalog_backtest_window
from nautilus_trader.adapters.rithmic.backtest import resolve_catalog_instrument_id
from nautilus_trader.adapters.rithmic.backtest import resolve_download_instrument_id
from nautilus_trader.adapters.rithmic.backtest import resolve_front_month_instrument_id
from nautilus_trader.adapters.rithmic.config import RITHMIC_ACCOUNT_ID_ENV
from nautilus_trader.adapters.rithmic.config import RITHMIC_APP_NAME_ENV
from nautilus_trader.adapters.rithmic.config import RITHMIC_APP_VERSION_ENV
from nautilus_trader.adapters.rithmic.config import RITHMIC_ENV_ENV
from nautilus_trader.adapters.rithmic.config import RITHMIC_FCM_ID_ENV
from nautilus_trader.adapters.rithmic.config import RITHMIC_IB_ID_ENV
from nautilus_trader.adapters.rithmic.config import RITHMIC_PASSWORD_ENV
from nautilus_trader.adapters.rithmic.config import RITHMIC_SERVER_ENV
from nautilus_trader.adapters.rithmic.config import RITHMIC_SYSTEM_NAME_ENV
from nautilus_trader.adapters.rithmic.config import RITHMIC_USERNAME_ENV
from nautilus_trader.adapters.rithmic.config import get_rithmic_adapter_account_id
from nautilus_trader.adapters.rithmic.config import get_rithmic_data_client_id
from nautilus_trader.adapters.rithmic.config import get_rithmic_exec_client_id
from nautilus_trader.adapters.rithmic.config import get_rithmic_profiles_from_env
from nautilus_trader.adapters.rithmic.config import load_rithmic_env_file
from nautilus_trader.adapters.rithmic.config import parse_rithmic_env
from nautilus_trader.adapters.rithmic.config import rithmic_env_token
from nautilus_trader.adapters.rithmic.config import to_binding_environment
from nautilus_trader.adapters.rithmic.constants import RITHMIC
from nautilus_trader.adapters.rithmic.constants import RITHMIC_CLIENT_ID
from nautilus_trader.adapters.rithmic.constants import RITHMIC_VENUE
from nautilus_trader.adapters.rithmic.symbols import databento_to_rithmic_symbol
from nautilus_trader.adapters.rithmic.symbols import projectx_to_rithmic_symbol
from nautilus_trader.adapters.rithmic.symbols import rithmic_to_databento_symbol
from nautilus_trader.adapters.rithmic.symbols import rithmic_to_databento_symbol_with_year
from nautilus_trader.adapters.rithmic.symbols import rithmic_to_projectx_symbol
from nautilus_trader.adapters.rithmic.symbols import rithmic_to_projectx_symbol_with_year


__all__ = [
    "RITHMIC",
    "RITHMIC_ACCOUNT_ID_ENV",
    "RITHMIC_APP_NAME_ENV",
    "RITHMIC_APP_VERSION_ENV",
    "RITHMIC_CLIENT_ID",
    "RITHMIC_ENV_ENV",
    "RITHMIC_FCM_ID_ENV",
    "RITHMIC_IB_ID_ENV",
    "RITHMIC_PASSWORD_ENV",
    "RITHMIC_SERVER_ENV",
    "RITHMIC_SYSTEM_NAME_ENV",
    "RITHMIC_USERNAME_ENV",
    "RITHMIC_VENUE",
    "RithmicDataClient",
    "RithmicDataClientConfig",
    "RithmicDataClientFactory",
    "RithmicEnv",
    "RithmicEnvironment",
    "RithmicExecClientConfig",
    "RithmicExecClientFactory",
    "RithmicExecutionClient",
    "RithmicGateway",
    "RithmicInstrumentProvider",
    "RithmicInstrumentSymbol",
    "build_external_bar_type",
    "databento_to_rithmic_symbol",
    "download_bars_to_catalog",
    "get_rithmic_adapter_account_id",
    "get_rithmic_data_client_id",
    "get_rithmic_exec_client_id",
    "get_rithmic_profiles_from_env",
    "load_rithmic_env_file",
    "normalize_rithmic_bar_spec",
    "parse_rithmic_env",
    "projectx_to_rithmic_symbol",
    "resolve_catalog_backtest_window",
    "resolve_catalog_instrument_id",
    "resolve_download_instrument_id",
    "resolve_front_month_instrument_id",
    "rithmic_env_token",
    "rithmic_to_databento_symbol",
    "rithmic_to_databento_symbol_with_year",
    "rithmic_to_projectx_symbol",
    "rithmic_to_projectx_symbol_with_year",
    "to_binding_environment",
]

fixup_module_names(globals(), __name__)
del fixup_module_names
