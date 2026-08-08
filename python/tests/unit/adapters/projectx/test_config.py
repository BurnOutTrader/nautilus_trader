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

import pytest

from nautilus_trader.adapters.projectx import ProjectXConfig
from nautilus_trader.adapters.projectx import ProjectXDataClientConfig as PackageDataClientConfig
from nautilus_trader.adapters.projectx import ProjectXExecClientConfig as PackageExecClientConfig
from nautilus_trader.model import AccountType


def test_public_generic_configs_are_pyo3_exports():
    assert PackageDataClientConfig.__module__ == "nautilus_trader.adapters.projectx"
    assert PackageExecClientConfig.__module__ == "nautilus_trader.adapters.projectx"


def test_projectx_config_constructs_with_environment():
    config = ProjectXConfig(
        environment="TopstepX",
        user_name="test-user",
        api_key="test-api-key",
        http_proxy_url="http://proxy-user:proxy-secret@example.com",
    )
    assert config.http_timeout_secs == 60
    assert config.max_retries == 3
    assert config.retry_delay_initial_ms == 1_000
    assert config.retry_delay_max_ms == 10_000
    assert config.http_proxy_url == "http://proxy-user:proxy-secret@example.com"
    assert "test-user" not in repr(config)
    assert "test-api-key" not in repr(config)
    assert "proxy-secret" not in repr(config)


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

    assert str(config.trader_id) == "TRADER-001"
    assert str(config.account_id) == "PROJECTX-PRAC-V2-64413-98419885"
    assert config.account_type == AccountType.MARGIN
    assert config.transport is not None


def test_exec_client_config_rejects_non_margin_account_type():
    with pytest.raises(ValueError, match="require AccountType::Margin"):
        PackageExecClientConfig(
            user_name="test-user",
            api_key="test-api-key",
            account_id="PRAC-V2-64413-98419885",
            trader_id="TRADER-001",
            account_type=AccountType.CASH,
        )


@pytest.mark.parametrize("trader_id", ["", "bad", "💥"])
def test_exec_client_config_rejects_invalid_trader_id(trader_id):
    with pytest.raises(ValueError, match="value"):
        PackageExecClientConfig(
            user_name="test-user",
            api_key="test-api-key",
            account_id="PRAC-V2-64413-98419885",
            trader_id=trader_id,
        )


@pytest.mark.parametrize("account_id", ["", "PROJECTX-", "💥"])
def test_exec_client_config_rejects_invalid_account_id(account_id):
    with pytest.raises(ValueError, match="value"):
        PackageExecClientConfig(
            user_name="test-user",
            api_key="test-api-key",
            account_id=account_id,
            trader_id="TRADER-001",
        )


def test_config_constructors_reject_unknown_fields():
    with pytest.raises(TypeError):
        ProjectXConfig(
            user_name="test-user",
            api_key="test-api-key",
            unknown_setting=True,
        )
