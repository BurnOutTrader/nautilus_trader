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
ProjectX gateway integration primitives.

This is an independent external adapter. It is not affiliated with, endorsed by, or supported by
Nautech Systems Pty Ltd or the official NautilusTrader project.

The Rust `projectx_nt` adapter crate owns the transport, state, reconnect, and authentication
logic (built on the published `projectx-client` v2 crate), while Python only re-exports the
Rust bindings and adds pure-Python helpers.

"""

from nautilus_trader._fixup import fixup_module_names
from nautilus_trader._libnautilus.projectx import *  # noqa: F403 (undefined-local-with-import-star)
from nautilus_trader.adapters.projectx.backtest import ProjectXCatalogDownloadResult
from nautilus_trader.adapters.projectx.backtest import build_external_bar_type
from nautilus_trader.adapters.projectx.backtest import download_bars_to_catalog
from nautilus_trader.adapters.projectx.providers import ProjectXInstrumentProvider


__all__ = [
    "PROJECTX",
    "PROJECTX_CLIENT_ID",
    "PROJECTX_VENUE",
    "ProjectXCatalogDownloadResult",
    "ProjectXConfig",
    "ProjectXDataClientConfig",
    "ProjectXDataClientFactory",
    "ProjectXExecClientConfig",
    "ProjectXExecutionClientFactory",
    "ProjectXHttpClient",
    "ProjectXHub",
    "ProjectXInstrumentProvider",
    "ProjectXSubscription",
    "ProjectXWsClient",
    "build_external_bar_type",
    "databento_to_projectx_adapter_symbol",
    "databento_to_projectx_contract_id",
    "download_bars_to_catalog",
    "load_projectx_env",
    "projectx_to_databento_symbol",
    "projectx_to_databento_symbol_with_year",
    "projectx_to_rithmic_symbol",
    "rithmic_to_projectx_symbol",
    "rithmic_to_projectx_symbol_with_year",
]

fixup_module_names(globals(), __name__)
del fixup_module_names
