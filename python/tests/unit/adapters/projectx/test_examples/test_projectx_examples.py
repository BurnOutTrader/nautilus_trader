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

import asyncio
import importlib
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace

import pytest

from nautilus_trader.model import AccountType
from nautilus_trader.model import BookType
from nautilus_trader.model import InstrumentId
from nautilus_trader.model import OmsType


REPO_ROOT = Path(__file__).resolve().parents[6]
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

    def with_exec_engine_config(self, config):
        self._captured["reconciliation"] = config.reconciliation
        self._captured["position_check_interval_secs"] = config.position_check_interval_secs
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


def test_projectx_live_strategies_construct_with_base_owned_config():
    modules_and_types = [
        (
            _reload("examples.live.projectx.projectx_data_capture"),
            "ProjectXDataCaptureStrategyConfig",
            "ProjectXDataCaptureStrategy",
        ),
        (
            _reload("examples.live.projectx.projectx_data_probe"),
            "ProjectXDataProbeStrategyConfig",
            "ProjectXDataProbeStrategy",
        ),
        (
            _reload("examples.live.projectx.projectx_ema_cross"),
            "ProjectXEMACrossStrategyConfig",
            "ProjectXEMACrossStrategy",
        ),
        (
            _reload("examples.live.projectx.projectx_exec_strategy"),
            "ProjectXExecStrategyConfig",
            "ProjectXExecStrategy",
        ),
        (
            _reload("examples.live.projectx.projectx_orderbook_probe"),
            "ProjectXOrderBookProbeStrategyConfig",
            "ProjectXOrderBookProbeStrategy",
        ),
    ]

    for module, config_name, strategy_name in modules_and_types:
        config = getattr(module, config_name)()
        strategy = getattr(module, strategy_name)(config)
        assert strategy.config is config


def test_projectx_validation_runner_imports_real_helpers():
    runner_module = _reload("examples.live.projectx.projectx_validation_runner")

    assert callable(runner_module.resolve_front_month_contract)
    assert callable(runner_module._instrument_id_from_contract)


@pytest.mark.parametrize(
    "script_name",
    [
        "projectx_ema_cross.py",
        "projectx_exec_tester.py",
        "projectx_instrument_resolver.py",
        "projectx_live_data_capture.py",
        "projectx_live_data_probe.py",
        "projectx_orderbook_probe.py",
        "projectx_validation_runner.py",
    ],
)
def test_projectx_runnable_script_imports_outside_repo_root(script_name, tmp_path):
    script_path = PROJECTX_EXAMPLES / script_name
    env = {**os.environ, "NAUTILUS_PATH": str(tmp_path)}
    completed = subprocess.run(
        [
            sys.executable,
            "-c",
            "import runpy, sys; runpy.run_path(sys.argv[1], run_name='projectx_smoke')",
            str(script_path),
        ],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == 0, completed.stderr


def test_projectx_provider_snapshot_uses_loaded_instrument_ids(monkeypatch):
    provider_module = _reload("examples.live.projectx.projectx_instrument_provider")

    class FakeHttpClient:
        def __init__(self, _config):
            self.started = False
            self.stopped = False

        async def start(self):
            self.started = True

        def stop(self):
            self.stopped = True

    class FakeProvider:
        def __init__(self, client, live):
            self.client = client
            self.live = live

        async def load_all_async(self, filters):
            self.filters = filters

        def get_all(self):
            return [
                SimpleNamespace(id=InstrumentId.from_str("MNQU26.PROJECTX")),
                SimpleNamespace(id=InstrumentId.from_str("MNQM26.PROJECTX")),
            ]

    monkeypatch.setattr(provider_module, "ProjectXConfig", lambda **kwargs: kwargs)
    monkeypatch.setattr(provider_module, "ProjectXHttpClient", FakeHttpClient)
    monkeypatch.setattr(provider_module, "ProjectXInstrumentProvider", FakeProvider)

    result = asyncio.run(
        provider_module.load_provider_snapshot(
            live=False,
            product_root="MNQ",
            active_only=True,
        ),
    )

    assert result["instrument_ids"] == ["MNQM26.PROJECTX", "MNQU26.PROJECTX"]
    assert result["count"] == 2


def test_projectx_high_level_backtest_uses_v2_config_api(monkeypatch, tmp_path):
    monkeypatch.setenv("NAUTILUS_PATH", str(tmp_path))
    backtest_module = _reload("examples.backtest.projectx.projectx_backtest_high_level")
    instrument_id = InstrumentId.from_str("MNQM26.PROJECTX")
    bar_type = backtest_module.BarType.from_str(
        "MNQM26.PROJECTX-1-MINUTE-LAST-EXTERNAL",
    )

    config, strategies = backtest_module._build_run_config(
        instrument_id=instrument_id,
        bar_type=bar_type,
        start_time=1,
        end_time=2,
    )

    venue = config.venues[0]
    data = config.data[0]
    assert venue.oms_type == OmsType.NETTING
    assert venue.account_type == AccountType.MARGIN
    assert venue.book_type == BookType.L1_MBP
    assert data.data_type == "Bar"
    assert data.instrument_id == instrument_id
    assert data.bar_spec == bar_type.spec
    assert data.start_time == 1
    assert data.end_time == 2
    assert len(strategies) == 1
    strategy_module_name, strategy_class_name = strategies[0].strategy_path.split(":", 1)
    config_module_name, config_class_name = strategies[0].config_path.split(":", 1)
    strategy_class = getattr(importlib.import_module(strategy_module_name), strategy_class_name)
    config_class = getattr(importlib.import_module(config_module_name), config_class_name)
    strategy = strategy_class(config_class(**strategies[0].config))
    assert strategy is not None


@pytest.mark.parametrize("product_root", ["MNQ", "NQ"])
def test_projectx_contract_parity_uses_shared_product_root_filter(monkeypatch, product_root):
    parity_module = _reload("examples.live.projectx.projectx_contract_parity_check")
    queries: list[str | None] = []
    instrument = SimpleNamespace(
        info={
            "projectx_contract_id": f"CON.F.US.{product_root}.M26",
            "projectx_name": f"{product_root}M26",
            "projectx_symbol_id": f"{product_root}M26",
            "active_contract": True,
        },
        price_increment="0.25",
    )

    class FakeHttpClient:
        def __init__(self, _config):
            pass

        async def start(self):
            pass

        def stop(self):
            pass

        async def available_instruments(self, *, live, active_only, product_root=None):
            queries.append(product_root)
            return [instrument]

        async def contract_by_id(self, *, contract_id):
            assert contract_id == instrument.info["projectx_contract_id"]
            return instrument

    monkeypatch.setattr(parity_module, "ProjectXConfig", lambda **kwargs: kwargs)
    monkeypatch.setattr(parity_module, "ProjectXHttpClient", FakeHttpClient)

    result = asyncio.run(
        parity_module.run_contract_parity_check(
            live=False,
            product_root=product_root,
        ),
    )

    assert queries == [product_root]
    assert result["checked"] == 1
    assert result["mismatches"] == []


def test_projectx_notebook_uses_async_download_and_v2_backtest_config():
    notebook_path = PROJECTX_EXAMPLES / "notebooks" / "projectx_historical_bars_to_parquet.py"
    source = notebook_path.read_text(encoding="utf-8")

    assert "await download_bars_to_catalog_async(" in source
    assert "download_bars_to_catalog," not in source
    assert 'data_type="Bar"' in source
    assert "bar_spec=bar_type.spec" in source
    assert "data_cls=" not in source


def test_projectx_live_capture_uses_typed_v2_catalog_methods(monkeypatch, tmp_path):
    monkeypatch.setenv("NAUTILUS_PATH", str(tmp_path))
    capture_module = _reload("examples.live.projectx.projectx_live_data_capture")
    writes: dict[str, list[object]] = {
        "instruments": [],
        "quotes": [],
        "trades": [],
        "book_deltas": [],
    }
    queries: list[tuple[str, list[str]]] = []

    class FakeCatalog:
        def __init__(self, _path):
            pass

        def instruments(self, *, instrument_ids):
            queries.append(("instruments", instrument_ids))
            return writes["instruments"]

        def query_quote_ticks(self, *, identifiers):
            queries.append(("quotes", identifiers))
            return writes["quotes"]

        def query_trade_ticks(self, *, identifiers):
            queries.append(("trades", identifiers))
            return writes["trades"]

        def query_order_book_deltas(self, *, identifiers):
            queries.append(("book_deltas", identifiers))
            return writes["book_deltas"]

        def write_instruments(self, data):
            writes["instruments"].extend(data)

        def write_quote_ticks(self, data):
            writes["quotes"].extend(data)

        def write_trade_ticks(self, data):
            writes["trades"].extend(data)

        def write_order_book_deltas(self, data):
            writes["book_deltas"].extend(data)

    monkeypatch.setattr(capture_module, "ParquetDataCatalog", FakeCatalog)
    instrument_id = InstrumentId.from_str("MNQM26.PROJECTX")
    capture_module._write_capture_to_catalog(
        catalog_path=tmp_path,
        before_counts={"instruments": 0, "quotes": 0, "trades": 0, "book_deltas": 0},
        snapshot={
            "instrument": object(),
            "quotes": [object()],
            "trades": [object()],
            "book_deltas": [object()],
        },
    )

    counts = capture_module._catalog_counts(tmp_path, instrument_id)

    assert counts == {"instruments": 1, "quotes": 1, "trades": 1, "book_deltas": 1}
    assert queries == [
        ("instruments", [instrument_id.value]),
        ("quotes", [instrument_id.value]),
        ("trades", [instrument_id.value]),
        ("book_deltas", [instrument_id.value]),
    ]


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


def test_projectx_instrument_resolver_reads_front_month_contract_snapshot():
    resolver_module = _reload("examples.live.projectx.projectx_instrument_resolver")

    result = resolver_module._instrument_id_from_contract(
        {"instrument_id": "MNQM26.PROJECTX"},
    )

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

    assert probe_module.ProjectXDataClientConfig.__module__.endswith(
        "nautilus_trader.adapters.projectx"
    )
    assert exec_module.ProjectXDataClientConfig.__module__.endswith(
        "nautilus_trader.adapters.projectx"
    )
    assert exec_module.ProjectXExecClientConfig.__module__.endswith(
        "nautilus_trader.adapters.projectx"
    )


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
    assert captured["reconciliation"] is True
    assert captured["position_check_interval_secs"] == 30.0
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


def test_projectx_ema_warmup_uses_datetime_request_bounds():
    ema_module = _reload("examples.live.projectx.projectx_ema_cross")
    captured: dict[str, object] = {}
    now_ns = 1_775_520_000_000_000_000
    strategy = SimpleNamespace(
        _started=False,
        _instrument_ready=True,
        _warmup_bar_type=object(),
        config=SimpleNamespace(
            request_bars=True,
            warmup_minutes=30,
            client_id=ema_module.ClientId("PROJECTX"),
            warmup_history_live=False,
            warmup_contract_live=False,
            bar_type=object(),
        ),
        clock=SimpleNamespace(timestamp_ns=lambda: now_ns),
        request_bars=lambda **kwargs: captured.update({"request": kwargs}),
        subscribe_bars=lambda **kwargs: captured.update({"subscribe": kwargs}),
    )

    ema_module.ProjectXEMACrossStrategy._start_strategy(strategy)

    request = captured["request"]
    assert isinstance(request["start"], datetime)
    assert isinstance(request["end"], datetime)
    assert request["start"] < request["end"]
    assert request["end"] == ema_module.unix_nanos_to_dt(now_ns)


def test_projectx_ema_cleanup_passes_client_order_ids():
    ema_module = _reload("examples.live.projectx.projectx_ema_cross")
    client_order_ids = [
        ema_module.ClientOrderId("O-001"),
        ema_module.ClientOrderId("O-002"),
    ]
    captured: dict[str, object] = {}
    strategy = SimpleNamespace(
        config=SimpleNamespace(
            account_id=None,
            client_id=ema_module.ClientId("PROJECTX"),
        ),
        _active_orders=lambda: [
            SimpleNamespace(client_order_id=client_order_id) for client_order_id in client_order_ids
        ],
        _object_payload=lambda _order: {},
        _emit_structured=lambda *_args: None,
        cancel_orders=lambda **kwargs: captured.update(kwargs),
    )

    ema_module.ProjectXEMACrossStrategy._cancel_active_orders(strategy)

    assert captured["client_order_ids"] == client_order_ids
    assert "orders" not in captured


def test_projectx_exec_cleanup_passes_client_order_id():
    exec_module = _reload("examples.live.projectx.projectx_exec_strategy")
    client_order_id = exec_module.ClientOrderId("O-001")
    captured: dict[str, object] = {}
    strategy = SimpleNamespace(
        config=SimpleNamespace(
            account_id=None,
            client_id=exec_module.ClientId("PROJECTX"),
            instrument_id=exec_module.InstrumentId.from_str("MNQM26.PROJECTX"),
        ),
        _cleanup_cancel_requests=set(),
        _open_orders=lambda: [
            SimpleNamespace(client_order_id=client_order_id, time_in_force=None),
        ],
        _runtime_snapshot=dict,
        _info=lambda _message: None,
        cancel_order=lambda *args, **kwargs: captured.update(
            {"args": args, "kwargs": kwargs},
        ),
    )

    exec_module.ProjectXExecStrategy._cancel_open_orders(strategy)

    assert captured["args"] == (client_order_id,)
    assert captured["kwargs"]["client_id"] == strategy.config.client_id
