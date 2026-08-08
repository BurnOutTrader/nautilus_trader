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
from pathlib import Path
from types import SimpleNamespace

from nautilus_trader.model import InstrumentId


REPO_ROOT = Path(__file__).resolve().parents[5]
PROJECTX_EXAMPLES = REPO_ROOT / "examples" / "live" / "projectx"


def _reload(module_name: str):
    sys.modules.pop(module_name, None)
    return importlib.import_module(module_name)


class _FakeNode:
    def __init__(self):
        self.run_called = False
        self.strategy_configs = []

    def add_strategy_from_config(self, config):
        self.strategy_configs.append(config)

    def run(self):
        self.run_called = True


class _FakeBuilder:
    def __init__(self, captured: dict[str, object]):
        self._captured = captured
        self.node = _FakeNode()

    def with_logging(self, _logging):
        return self

    def with_data_engine_config(self, config):
        self._captured["time_bars_build_with_no_updates"] = config.time_bars_build_with_no_updates
        return self

    def with_reconciliation(self, _enabled):
        return self

    def with_position_check_interval_secs(self, _secs):
        return self

    def with_timeout_connection(self, _secs):
        return self

    def with_timeout_reconciliation(self, _secs):
        return self

    def with_timeout_portfolio(self, _secs):
        return self

    def with_timeout_disconnection_secs(self, _secs):
        return self

    def with_delay_post_stop_secs(self, _secs):
        return self

    def add_data_client(self, *_args, **_kwargs):
        return self

    def add_exec_client(self, *_args, **_kwargs):
        return self

    def build(self):
        return self.node


def _install_projectx_ema_main_fakes(monkeypatch, ema_module, captured: dict[str, object]) -> None:
    class FakeLiveNode:
        @staticmethod
        def builder(*_args, **_kwargs):
            builder = _FakeBuilder(captured)
            captured["node"] = builder.node
            return builder

    monkeypatch.setattr(ema_module, "LiveNode", FakeLiveNode)
    monkeypatch.setattr(ema_module, "ProjectXDataClientFactory", object)
    monkeypatch.setattr(ema_module, "ProjectXExecutionClientFactory", object)
    monkeypatch.setattr(ema_module, "ProjectXDataClientConfig", lambda **kwargs: kwargs)
    monkeypatch.setattr(ema_module, "ProjectXExecClientConfig", lambda **kwargs: kwargs)
    monkeypatch.setattr(ema_module, "schedule_stop", lambda _node, _run_seconds: None)


def test_projectx_importable_strategy_modules_use_generic_names():
    exec_module = _reload("examples.live.projectx.projectx_exec_strategy")
    probe_module = _reload("examples.live.projectx.projectx_data_probe")
    orderbook_module = _reload("examples.live.projectx.projectx_orderbook_probe")
    capture_module = _reload("examples.live.projectx.projectx_data_capture")
    ema_module = _reload("examples.live.projectx.projectx_ema_cross")

    assert exec_module.ProjectXExecStrategyConfig is not None
    assert exec_module.ProjectXExecStrategy is not None
    assert probe_module.ProjectXDataProbeStrategyConfig is not None
    assert probe_module.ProjectXDataProbeStrategy is not None
    assert orderbook_module.ProjectXOrderBookProbeStrategyConfig is not None
    assert orderbook_module.ProjectXOrderBookProbeStrategy is not None
    assert capture_module.ProjectXDataCaptureStrategyConfig is not None
    assert capture_module.ProjectXDataCaptureStrategy is not None
    assert ema_module.ProjectXEMACrossStrategyConfig is not None
    assert ema_module.ProjectXEMACrossStrategy is not None


def test_projectx_live_wrappers_reference_generic_strategy_paths(monkeypatch, tmp_path):
    monkeypatch.setenv("NAUTILUS_PATH", str(tmp_path))

    probe_module = _reload("examples.live.projectx.projectx_live_data_probe")
    capture_module = _reload("examples.live.projectx.projectx_live_data_capture")
    exec_module = _reload("examples.live.projectx.projectx_exec_tester")
    ema_module = _reload("examples.live.projectx.projectx_ema_cross")

    assert "pyo3" not in probe_module._STRATEGY_PATH.lower()
    assert "pyo3" not in probe_module._CONFIG_PATH.lower()
    assert "pyo3" not in capture_module._STRATEGY_PATH.lower()
    assert "pyo3" not in capture_module._CONFIG_PATH.lower()
    assert "pyo3" not in ema_module._STRATEGY_PATH.lower()
    assert "pyo3" not in ema_module._CONFIG_PATH.lower()
    assert any(
        isinstance(value, str) and "projectx_exec_strategy" in value
        for value in exec_module._build_node.__code__.co_consts
    )


def test_projectx_legacy_pyo3_example_modules_are_removed():
    assert not (PROJECTX_EXAMPLES / "projectx_pyo3_exec_strategy.py").exists()
    assert not (PROJECTX_EXAMPLES / "projectx_pyo3_data_probe.py").exists()
    assert not (PROJECTX_EXAMPLES / "projectx_pyo3_data_capture.py").exists()


def test_projectx_example_defaults_use_two_digit_contract_years(monkeypatch, tmp_path):
    monkeypatch.setenv("NAUTILUS_PATH", str(tmp_path))

    probe_module = _reload("examples.live.projectx.projectx_live_data_probe")
    orderbook_module = _reload("examples.live.projectx.projectx_orderbook_probe")
    capture_module = _reload("examples.live.projectx.projectx_live_data_capture")
    exec_module = _reload("examples.live.projectx.projectx_exec_tester")
    ema_module = _reload("examples.live.projectx.projectx_ema_cross")

    assert probe_module.INSTRUMENT_ID.value == "MNQM26.PROJECTX"
    assert orderbook_module.INSTRUMENT_ID.value == "MNQM26.PROJECTX"
    assert capture_module.INSTRUMENT_ID.value == "MNQM26.PROJECTX"
    assert exec_module.INSTRUMENT_ID.value == "MNQM26.PROJECTX"
    assert ema_module.INSTRUMENT_ID.value == "MNQM26.PROJECTX"


def test_projectx_instrument_resolver_prefers_front_month_instrument_id():
    resolver_module = _reload("examples.live.projectx.projectx_instrument_resolver")

    front_month = SimpleNamespace(
        id=InstrumentId.from_str("MNQM26.PROJECTX"),
    )
    result = resolver_module._instrument_id_from_front_month(front_month)

    assert result is not None
    assert result.value == "MNQM26.PROJECTX"


def test_projectx_ema_bar_logger_emits_structured_phase_payload():
    ema_module = _reload("examples.live.projectx.projectx_ema_cross")
    emitted: list[tuple[str, dict, object, str]] = []

    strategy = SimpleNamespace(
        _fast_ema=10.25,
        _slow_ema=10.0,
        _object_payload=lambda _: {"ts_init": 123},
        _emit_structured=lambda prefix, payload, color, level="info": emitted.append(
            (prefix, payload, color, level),
        ),
    )

    ema_module.ProjectXEMACrossStrategy._log_bar_state(
        strategy,
        bar=object(),
        close=10.5,
        phase="warming_up",
    )

    assert len(emitted) == 1
    prefix, payload, color, level = emitted[0]
    assert prefix == "BAR"
    assert payload["ts_init"] == 123
    assert payload["close"] == 10.5
    assert payload["fast_ema"] == 10.25
    assert payload["slow_ema"] == 10.0
    assert payload["phase"] == "warming_up"
    assert color == ema_module.LogColor.BLUE
    assert level == "info"


def test_projectx_ema_config_exposes_separate_warmup_history_modes():
    ema_module = _reload("examples.live.projectx.projectx_ema_cross")

    config = ema_module.ProjectXEMACrossStrategyConfig(
        warmup_history_live=True,
        warmup_contract_live=False,
    )

    assert config.warmup_history_live is True
    assert config.warmup_contract_live is False


def test_projectx_live_examples_import_public_pyo3_config_types():
    probe_module = _reload("examples.live.projectx.projectx_live_data_probe")
    exec_module = _reload("examples.live.projectx.projectx_exec_tester")

    assert probe_module.ProjectXDataClientConfig.__module__.endswith("nautilus_trader.adapters.projectx")
    assert exec_module.ProjectXDataClientConfig.__module__.endswith("nautilus_trader.adapters.projectx")
    assert exec_module.ProjectXExecClientConfig.__module__.endswith("nautilus_trader.adapters.projectx")


def test_projectx_ema_defaults_to_sim_history_for_warmup():
    ema_module = _reload("examples.live.projectx.projectx_ema_cross")

    config = ema_module.ProjectXEMACrossStrategyConfig()

    assert ema_module.WARMUP_HISTORY_LIVE is False
    assert config.warmup_history_live is False
    assert config.warmup_contract_live is False


def test_projectx_ema_main_disables_internal_bar_fill_forward(monkeypatch):
    ema_module = _reload("examples.live.projectx.projectx_ema_cross")
    captured: dict[str, object] = {}

    _install_projectx_ema_main_fakes(monkeypatch, ema_module, captured)

    ema_module.main()

    assert captured["time_bars_build_with_no_updates"] is False
    assert captured["node"].run_called is True
    assert len(captured["node"].strategy_configs) == 1


def test_projectx_ema_historical_bars_prime_without_submitting_signals():
    ema_module = _reload("examples.live.projectx.projectx_ema_cross")

    live_bar_type = object()
    warmup_bar_type = object()
    logged_phases: list[str] = []
    signal_calls: list[dict] = []

    strategy = SimpleNamespace(
        config=SimpleNamespace(bar_type=live_bar_type),
        _warmup_bar_type=warmup_bar_type,
        _prepare_bar_processing=lambda **kwargs: (0.0, 1.0, "FLAT", [], []),
        _log_bar_state=lambda **kwargs: logged_phases.append(kwargs["phase"]),
        _handle_signal_transition=lambda **kwargs: signal_calls.append(kwargs),
        _fast_ema=2.0,
        _slow_ema=1.0,
    )
    historical_bar = SimpleNamespace(bar_type=warmup_bar_type, close="25200.0", ts_init=123)

    ema_module.ProjectXEMACrossStrategy._process_bar(strategy, historical_bar)

    assert logged_phases == ["priming"]
    assert signal_calls == []
