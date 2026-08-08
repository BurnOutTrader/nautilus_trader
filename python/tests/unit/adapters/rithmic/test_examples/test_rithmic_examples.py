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

import importlib
import sys


def _reload(module_name: str):
    sys.modules.pop(module_name, None)
    return importlib.import_module(module_name)


def test_rithmic_importable_strategy_modules_use_generic_names():
    exec_module = _reload("examples.live.rithmic.rithmic_exec_strategy")
    probe_module = _reload("examples.live.rithmic.rithmic_data_probe")
    capture_module = _reload("examples.live.rithmic.rithmic_data_capture")
    orderbook_module = _reload("examples.live.rithmic.rithmic_orderbook_probe")
    ema_module = _reload("examples.live.rithmic.rithmic_ema_cross_strategy")

    assert exec_module.RithmicExecStrategyConfig is not None
    assert exec_module.RithmicExecStrategy is not None
    assert probe_module.RithmicDataProbeStrategyConfig is not None
    assert probe_module.RithmicDataProbeStrategy is not None
    assert capture_module.RithmicDataCaptureStrategyConfig is not None
    assert capture_module.RithmicDataCaptureStrategy is not None
    assert orderbook_module.RithmicOrderBookProbeStrategyConfig is not None
    assert orderbook_module.RithmicOrderBookProbeStrategy is not None
    assert ema_module.RithmicEMACrossStrategyConfig is not None
    assert ema_module.RithmicEMACrossStrategy is not None


def test_rithmic_live_wrappers_reference_generic_strategy_paths(monkeypatch, tmp_path):
    monkeypatch.setenv("NAUTILUS_PATH", str(tmp_path))

    probe_module = _reload("examples.live.rithmic.rithmic_data_tester")
    capture_module = _reload("examples.live.rithmic.rithmic_live_data_capture")
    exec_module = _reload("examples.live.rithmic.rithmic_exec_tester")
    ema_module = _reload("examples.live.rithmic.rithmic_ema_cross")

    assert "pyo3" not in probe_module._STRATEGY_PATH.lower()
    assert "pyo3" not in probe_module._CONFIG_PATH.lower()
    assert "pyo3" not in capture_module._STRATEGY_PATH.lower()
    assert "pyo3" not in capture_module._CONFIG_PATH.lower()
    assert "pyo3" not in ema_module._STRATEGY_PATH.lower()
    assert "pyo3" not in ema_module._CONFIG_PATH.lower()
    assert "rithmic_exec_strategy" in exec_module._STRATEGY_PATH
    assert "rithmic_exec_strategy" in exec_module._CONFIG_PATH


def test_rithmic_capture_defaults_use_front_month_resolution_inputs(monkeypatch, tmp_path):
    monkeypatch.setenv("NAUTILUS_PATH", str(tmp_path))

    capture_module = _reload("examples.live.rithmic.rithmic_live_data_capture")
    orderbook_module = _reload("examples.live.rithmic.rithmic_orderbook_probe")

    assert capture_module.PROFILE is None
    assert capture_module.PRODUCT_CODE == "MNQ"
    assert capture_module.EXCHANGE == "CME"
    assert capture_module.INSTRUMENT_ID is None
    assert capture_module.BAR_SPEC == "1-MINUTE-LAST-EXTERNAL"
    assert capture_module.CAPTURE_BARS is True
    assert capture_module.REQUEST_BARS is True
    assert orderbook_module.PRODUCT_CODE == "MNQ"
    assert orderbook_module.EXCHANGE == "CME"
    assert orderbook_module.INSTRUMENT_ID is None


def test_rithmic_live_node_helpers_build_pyo3_configs(monkeypatch):
    monkeypatch.setenv("RITHMIC_USERNAME", "user")
    monkeypatch.setenv("RITHMIC_PASSWORD", "pass")
    monkeypatch.setenv("RITHMIC_SYSTEM_NAME", "Apex")
    monkeypatch.setenv("RITHMIC_APP_NAME", "OwnApp")
    monkeypatch.setenv("RITHMIC_ACCOUNT_ID", "ACC-001")

    helpers = _reload("examples.live.rithmic.rithmic_live_node_helpers")

    data_config = helpers.build_data_client_config(None, enable_history=True)
    exec_config = helpers.build_exec_client_config(None)

    assert isinstance(data_config, helpers.RithmicDataClientConfig)
    assert isinstance(exec_config, helpers.RithmicExecClientConfig)
    assert repr(data_config.environment) == repr(helpers.RithmicEnv.DEMO)
    assert repr(exec_config.environment) == repr(helpers.RithmicEnv.DEMO)
    assert data_config.enable_history is True
    assert exec_config.account_id == "ACC-001"
