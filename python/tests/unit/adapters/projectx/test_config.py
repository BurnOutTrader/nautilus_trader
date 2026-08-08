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

from nautilus_trader.adapters.projectx import ProjectXConfig
from nautilus_trader.adapters.projectx import ProjectXDataClientConfig as PackageDataClientConfig
from nautilus_trader.adapters.projectx import ProjectXExecClientConfig as PackageExecClientConfig
from nautilus_trader.adapters.projectx import ProjectXHub


def test_public_generic_configs_are_pyo3_exports():
    assert PackageDataClientConfig.__module__ == "nautilus_trader.adapters.projectx"
    assert PackageExecClientConfig.__module__ == "nautilus_trader.adapters.projectx"


def test_projectx_config_constructs_with_environment():
    config = ProjectXConfig(
        environment="TopstepX",
        user_name="test-user",
        api_key="test-api-key",
    )
    assert config is not None


def test_data_client_config_fields_round_trip():
    config = PackageDataClientConfig(
        user_name="test-user",
        api_key="test-api-key",
        market_data_live=True,
    )

    assert config.market_data_live is True


def test_exec_client_config_constructs():
    config = PackageExecClientConfig(
        user_name="test-user",
        api_key="test-api-key",
        account_id="PRAC-V2-64413-98419885",
        trader_id="TRADER-001",
    )

    assert config.transport is not None


def test_projectx_hub_enum_values():
    assert ProjectXHub.Market is not None
    assert ProjectXHub.User is not None
