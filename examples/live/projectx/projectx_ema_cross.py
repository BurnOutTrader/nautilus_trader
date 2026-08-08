#!/usr/bin/env python3
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
Run this example with INTERNAL bars for live execution and matching
historical bars on startup so the strategy can trade immediately once the first
new live bar arrives.

Warning:
    This example can submit live orders to the configured account.
    Use a demo/sim account first.
"""

from __future__ import annotations

import json
import os
import sys
from contextlib import suppress
from pathlib import Path
from typing import TYPE_CHECKING
from uuid import uuid4

_REPO_ROOT = str(Path(__file__).resolve().parents[3])
sys.path[:] = [_REPO_ROOT, *(path for path in sys.path if path != _REPO_ROOT)]

from nautilus_trader._libnautilus.common import Environment, LogColor, LoggerConfig
from nautilus_trader._libnautilus.core import UUID4
from nautilus_trader._libnautilus.model import (
    AccountId,
    AccountType,
    AggregationSource,
    Bar,
    BarType,
    ClientId,
    ClientOrderId,
    InstrumentId,
    MarketOrder,
    OrderSide,
    Quantity,
    StrategyId,
    TimeInForce,
    TraderId,
)
from nautilus_trader._libnautilus.trading import Strategy, StrategyConfig
from nautilus_trader.adapters.projectx import (
    PROJECTX_CLIENT_ID,
    ProjectXDataClientConfig,
    ProjectXDataClientFactory,
    ProjectXExecClientConfig,
    ProjectXExecutionClientFactory,
    load_projectx_env,
)
from nautilus_trader.config import LiveDataEngineConfig, LiveExecEngineConfig
from nautilus_trader.core.datetime import unix_nanos_to_dt
from nautilus_trader.live import LiveNode

from examples.live.live_node_run_helpers import schedule_live_node_interrupt

if TYPE_CHECKING:
    from nautilus_trader.config import ImportableStrategyConfig
else:
    from nautilus_trader._libnautilus.trading import ImportableStrategyConfig


load_projectx_env()


def _resolve_exec_account_id() -> str:
    return (
        os.getenv("PROJECTX_EXEC_ACCOUNT_ID")
        or os.getenv("PROJECTX_ACCOUNT_ID")
        or "PRAC-V2-EXAMPLE-ACCOUNT"
    )


INSTRUMENT_ID = InstrumentId.from_str("MNQM26.PROJECTX")
ACCOUNT_ID = _resolve_exec_account_id()
BAR_SPEC = "15-SECOND-LAST-INTERNAL"
TRADE_SIZE = "1"
FAST_EMA_PERIOD = 10
SLOW_EMA_PERIOD = 20
WARMUP_MINUTES = 30
MARKET_DATA_LIVE = False
WARMUP_HISTORY_LIVE = False
WARMUP_CONTRACT_LIVE = MARKET_DATA_LIVE
RUN_SECONDS = 0
TRADER_ID = TraderId("TESTER-001")

_MODULE_PATH = "examples.live.projectx.projectx_ema_cross"
_STRATEGY_PATH = f"{_MODULE_PATH}:ProjectXEMACrossStrategy"
_CONFIG_PATH = f"{_MODULE_PATH}:ProjectXEMACrossStrategyConfig"
_LOGGING_SPEC = (
    "stdout=Info;"
    "nautilus_execution::order_manager=Warn;"
    "nautilus_execution::reconciliation=Warn;"
    "nautilus_portfolio::portfolio=Warn;"
    "nautilus_trading::strategy=Warn"
)


def _build_example_logging() -> LoggerConfig:
    # Keep the example focused on strategy callbacks instead of framework plumbing.
    return LoggerConfig.from_spec(_LOGGING_SPEC)


def _serialize_log_payload(payload) -> str:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)


def _coerce_instrument_id(value: str | InstrumentId) -> InstrumentId:
    if isinstance(value, InstrumentId):
        return value
    return InstrumentId.from_str(value)


def _coerce_client_id(value: str | ClientId) -> ClientId:
    if isinstance(value, ClientId):
        return value
    return ClientId(value)


def _coerce_price(value) -> float:
    if hasattr(value, "as_double"):
        return value.as_double()
    return float(value)


def _coerce_account_id(value: str | AccountId | None) -> AccountId | None:
    if value is None or value == "":
        return None

    if isinstance(value, AccountId):
        return value

    if value.startswith("PROJECTX-"):
        return AccountId(value)
    return AccountId(f"PROJECTX-{value}")


def _coerce_strategy_id(value: str | StrategyId) -> StrategyId:
    if isinstance(value, StrategyId):
        return value
    return StrategyId(value)


def _coerce_bar_type(value: str | BarType) -> BarType:
    if isinstance(value, BarType):
        return value
    return BarType.from_str(value)


class ProjectXEMACrossStrategyConfig(StrategyConfig):
    def __new__(
        cls,
        instrument_id: str | InstrumentId = "MNQM26.PROJECTX",
        client_id: str | ClientId = "PROJECTX",
        account_id: str | AccountId | None = None,
        strategy_id: str | StrategyId = "PROJECTX-EMA-001",
        bar_type: str | BarType = "MNQM26.PROJECTX-15-SECOND-LAST-INTERNAL",
        trade_size: str | Quantity = "1",
        fast_ema_period: int = 10,
        slow_ema_period: int = 20,
        warmup_minutes: int = 30,
        warmup_history_live: bool = False,
        warmup_contract_live: bool = False,
        request_bars: bool = True,
        unsubscribe_on_stop: bool = True,
        cleanup_on_stop: bool = True,
        log_data: bool = False,
        log_events: bool = True,
        log_commands: bool = True,
    ):
        parsed_instrument_id = _coerce_instrument_id(instrument_id)
        parsed_client_id = _coerce_client_id(client_id)
        parsed_account_id = _coerce_account_id(account_id)
        parsed_strategy_id = _coerce_strategy_id(strategy_id)
        parsed_bar_type = _coerce_bar_type(bar_type)
        parsed_trade_size = (
            trade_size
            if isinstance(trade_size, Quantity)
            else Quantity.from_str(str(trade_size))
        )

        config = super().__new__(
            cls,
            strategy_id=parsed_strategy_id,
            external_order_claims=[parsed_instrument_id],
            manage_stop=cleanup_on_stop,
            market_exit_time_in_force=TimeInForce.GTC,
            market_exit_reduce_only=False,
            log_events=log_events,
            log_commands=log_commands,
        )
        config.instrument_id = parsed_instrument_id
        config.client_id = parsed_client_id
        config.account_id = parsed_account_id
        config.bar_type = parsed_bar_type
        config.trade_size = parsed_trade_size
        config.fast_ema_period = int(fast_ema_period)
        config.slow_ema_period = int(slow_ema_period)
        config.warmup_minutes = int(warmup_minutes)
        config.warmup_history_live = bool(warmup_history_live)
        config.warmup_contract_live = bool(warmup_contract_live)
        config.request_bars = bool(request_bars)
        config.unsubscribe_on_stop = bool(unsubscribe_on_stop)
        config.cleanup_on_stop = bool(cleanup_on_stop)
        config.log_data = bool(log_data)
        return config


class ProjectXEMACrossStrategy(Strategy):
    def __init__(self, config: ProjectXEMACrossStrategyConfig):
        super().__init__(config)
        self._started = False
        self._instrument_ready = False
        self._warmup_complete = not config.request_bars or config.warmup_minutes <= 0
        self._warmup_bar_type = BarType(
            config.instrument_id,
            config.bar_type.spec,
            AggregationSource.EXTERNAL,
        )
        self._fast_ema = 0.0
        self._slow_ema = 0.0
        self._bar_count = 0
        self._historical_bar_count = 0
        self._fast_alpha = 2.0 / (int(config.fast_ema_period) + 1)
        self._slow_alpha = 2.0 / (int(config.slow_ema_period) + 1)

    def on_start(self):
        instrument = self.cache.instrument(self.config.instrument_id)

        if instrument is not None:
            self.on_instrument(instrument)
        else:
            self.request_instrument(
                instrument_id=self.config.instrument_id,
                client_id=self.config.client_id,
            )

    def on_stop(self):
        if self.config.cleanup_on_stop:
            self._cancel_active_orders()
            self._close_open_positions()

        if self.config.unsubscribe_on_stop:
            self.unsubscribe_bars(
                bar_type=self.config.bar_type,
                client_id=self.config.client_id,
            )

    def on_instrument(self, instrument):
        if instrument.id != self.config.instrument_id:
            return
        self._instrument_ready = True
        self._start_strategy()

    def on_order_accepted(self, event):
        self._log_order_event(event, color=LogColor.BLUE)

        if self.config.log_data:
            self.log.info(repr(event), LogColor.CYAN)

    def on_order_rejected(self, event):
        self._log_order_event(event, color=LogColor.RED, level="error")

    def on_order_submitted(self, event):
        self._log_order_event(event, color=LogColor.BLUE)

    def on_order_canceled(self, event):
        self._log_order_event(event, color=LogColor.YELLOW, level="warning")

    def on_order_filled(self, event):
        self._log_order_event(event, color=LogColor.GREEN)

        if self.config.log_data:
            self.log.info(repr(event), LogColor.CYAN)

    def on_position_event(self, event):
        self._log_position_event(event, color=LogColor.CYAN)

    def on_bar(self, bar: Bar):
        if bar.bar_type != self.config.bar_type:
            return
        self._process_bar(bar)

    def on_historical_data(self, data):
        if self._warmup_complete:
            return

        if isinstance(data, Bar) and data.bar_type == self._warmup_bar_type:
            self._process_bar(data)

    def on_historical_bars(self, bars):
        if self._warmup_complete:
            return
        warmup_bars = [bar for bar in bars if bar.bar_type == self._warmup_bar_type]

        for bar in warmup_bars:
            self._process_bar(bar)

        if warmup_bars:
            self._historical_bar_count += len(warmup_bars)
            self.log.info(
                "Loaded "
                f"{len(warmup_bars)} historical warmup bars "
                f"(history_live={self.config.warmup_history_live} "
                f"contract_live={self.config.warmup_contract_live})",
                LogColor.BLUE,
            )
        self._complete_warmup()

    def on_time_event(self, event):
        # The Rust strategy runtime dispatches timer callbacks to Python even when
        # this example has nothing custom to do for them.
        return

    def _process_bar(self, bar: Bar) -> None:
        if bar.bar_type not in (self.config.bar_type, self._warmup_bar_type):
            return

        is_live_bar = bar.bar_type == self.config.bar_type
        close = _coerce_price(bar.close)
        prepared = self._prepare_bar_processing(
            bar=bar,
            close=close,
            is_live_bar=is_live_bar,
        )

        if prepared is None:
            return

        prev_fast, prev_slow, portfolio_state, open_positions, active_orders = prepared
        phase = "priming" if not is_live_bar else "ready"

        if is_live_bar and active_orders:
            phase = "pending_orders"

        fast_above = self._fast_ema > self._slow_ema
        was_above = prev_fast > prev_slow

        if is_live_bar and not active_orders:
            if fast_above and not was_above:
                phase = "bull_cross"
            elif not fast_above and was_above:
                phase = "bear_cross"

        self._log_bar_state(
            bar=bar,
            close=close,
            phase=phase,
        )

        if active_orders or not is_live_bar:
            return

        self._handle_signal_transition(
            fast_above=fast_above,
            was_above=was_above,
            portfolio_state=portfolio_state,
            open_positions=open_positions,
        )

    def _start_strategy(self):
        if self._started or not self._instrument_ready:
            return

        self._started = True

        if self.config.request_bars and self.config.warmup_minutes > 0:
            warmup_ns = int(self.config.warmup_minutes) * 60 * 1_000_000_000
            request_started_ns = self.clock.timestamp_ns()
            self.request_bars(
                bar_type=self._warmup_bar_type,
                start=unix_nanos_to_dt(request_started_ns - warmup_ns),
                end=unix_nanos_to_dt(request_started_ns),
                client_id=self.config.client_id,
                params={
                    "live": self.config.warmup_history_live,
                    "contract_live": self.config.warmup_contract_live,
                },
            )

        self.subscribe_bars(
            bar_type=self.config.bar_type,
            client_id=self.config.client_id,
        )

    def _submit_market(self, order_side: OrderSide):
        order = MarketOrder(
            trader_id=self.trader_id,
            strategy_id=self.strategy_id,
            instrument_id=self.config.instrument_id,
            client_order_id=ClientOrderId(f"PXEMA-{uuid4().hex[:24].upper()}"),
            order_side=order_side,
            quantity=self.config.trade_size,
            init_id=UUID4(),
            ts_init=self.clock.timestamp_ns(),
            time_in_force=TimeInForce.GTC,
            reduce_only=False,
            quote_quantity=False,
            tags=["projectx", "ema", "example"],
        )
        payload = self._object_payload(order)
        payload.update(
            {
                "action": "submit_entry",
                "account_id": self.config.account_id,
                "client_id": self.config.client_id,
                "fast_ema": self._fast_ema,
                "slow_ema": self._slow_ema,
            },
        )
        self._emit_structured("ORDER_REQUEST", payload, LogColor.BLUE)
        self.submit_order(
            order=order,
            client_id=self.config.client_id,
        )

    def _prepare_bar_processing(
        self,
        *,
        bar: Bar,
        close: float,
        is_live_bar: bool,
    ) -> tuple[float, float, str, list, list] | None:
        ema_state = self._advance_ema(close)

        if ema_state is None:
            if is_live_bar:
                self._log_bar_state(bar=bar, close=close, phase="priming")
            return None

        portfolio_state = self._portfolio_state()
        open_positions = self._open_positions()
        active_orders = self._active_orders()

        if self._bar_count < int(self.config.slow_ema_period):
            if is_live_bar:
                self._log_bar_state(
                    bar=bar,
                    close=close,
                    phase="priming",
                )
            return None

        return (
            ema_state[0],
            ema_state[1],
            portfolio_state,
            open_positions,
            active_orders,
        )

    def _advance_ema(self, close: float) -> tuple[float, float] | None:
        self._bar_count += 1

        if self._bar_count == 1:
            self._fast_ema = close
            self._slow_ema = close
            return None

        prev_fast = self._fast_ema
        prev_slow = self._slow_ema
        self._fast_ema = self._fast_alpha * close + (1.0 - self._fast_alpha) * prev_fast
        self._slow_ema = self._slow_alpha * close + (1.0 - self._slow_alpha) * prev_slow
        return prev_fast, prev_slow

    def _complete_warmup(self) -> None:
        if self._warmup_complete:
            return

        self._warmup_complete = True

        if self._historical_bar_count == 0:
            self.log.warning(
                "No historical warmup bars received; priming from live bars only "
                f"(history_live={self.config.warmup_history_live} "
                f"contract_live={self.config.warmup_contract_live})",
                LogColor.YELLOW,
            )
        self.log.info(  # noqa: PLE1205 (nautilus logger takes a positional color)
            "Historical warmup complete; live ProjectX execution enabled",
            LogColor.GREEN,
        )

    def _handle_signal_transition(
        self,
        *,
        fast_above: bool,
        was_above: bool,
        portfolio_state: str,
        open_positions: list,
    ) -> None:
        if fast_above and not was_above:
            if portfolio_state == "FLAT":
                self._submit_market(OrderSide.BUY)
            elif portfolio_state == "SHORT":
                self._close_open_positions(open_positions)
        elif not fast_above and was_above:
            if portfolio_state == "FLAT":
                self._submit_market(OrderSide.SELL)
            elif portfolio_state == "LONG":
                self._close_open_positions(open_positions)

    def _close_open_positions(self, positions=None):
        positions = self._open_positions() if positions is None else positions

        for position in positions:
            payload = self._object_payload(position)
            payload.update(
                {
                    "action": "close_position",
                    "account_id": self.config.account_id,
                    "client_id": self.config.client_id,
                    "time_in_force": TimeInForce.GTC,
                    "reduce_only": False,
                },
            )
            self._emit_structured("POSITION_REQUEST", payload, LogColor.YELLOW)
            self.close_position(
                position=position,
                client_id=self.config.client_id,
                time_in_force=TimeInForce.GTC,
                reduce_only=False,
            )

    def _cancel_active_orders(self):
        orders = self._active_orders()

        if not orders:
            return

        for order in orders:
            payload = self._object_payload(order)
            payload.update(
                {
                    "action": "cancel_order",
                    "account_id": self.config.account_id,
                    "client_id": self.config.client_id,
                },
            )
            self._emit_structured("ORDER_REQUEST", payload, LogColor.YELLOW)

        self.cancel_orders(
            client_order_ids=[order.client_order_id for order in orders],
            client_id=self.config.client_id,
        )

    def _portfolio_state(self) -> str:
        positions = self._open_positions()

        if not positions:
            return "FLAT"

        net_position = sum(position.signed_qty for position in positions)

        if net_position > 0:
            return "LONG"

        if net_position < 0:
            return "SHORT"
        return "FLAT"

    def _net_position(self) -> float:
        return float(sum(position.signed_qty for position in self._open_positions()))

    def _active_orders(self) -> list:
        cache_kwargs = self._cache_query_kwargs()
        return (
            list(self.cache.orders_open(**cache_kwargs))
            + list(self.cache.orders_emulated(**cache_kwargs))
            + list(self.cache.orders_inflight(**cache_kwargs))
        )

    def _open_positions(self) -> list:
        return list(self.cache.positions_open(**self._cache_query_kwargs()))

    def _cache_query_kwargs(self) -> dict:
        kwargs = {"instrument_id": self.config.instrument_id}

        if self.config.account_id is not None:
            kwargs["account_id"] = self.config.account_id
        else:
            kwargs["strategy_id"] = self.strategy_id
        return kwargs

    def _emit_structured(
        self,
        prefix: str,
        payload,
        color,
        *,
        level: str = "info",
    ) -> None:
        message = f"{prefix} {_serialize_log_payload(payload)}"
        getattr(self.log, level)(message, color)

    def _object_payload(self, obj) -> dict:
        if obj is None:
            return {}

        if isinstance(obj, dict):
            return dict(obj)

        if hasattr(obj, "to_dict"):
            with suppress(Exception):
                return dict(obj.to_dict())

        return {"repr": repr(obj)}

    def _log_order_event(
        self,
        event,
        *,
        color,
        level: str = "info",
    ) -> None:
        self._emit_structured(
            "ORDER_EVENT", self._object_payload(event), color, level=level
        )

    def _log_position_event(self, event, *, color) -> None:
        self._emit_structured("POSITION_EVENT", self._object_payload(event), color)

    def _log_bar_state(
        self,
        *,
        bar: Bar,
        close: float,
        phase: str,
    ) -> None:
        payload = self._object_payload(bar)
        payload.update(
            {
                "phase": phase,
                "close": close,
                "fast_ema": self._fast_ema,
                "slow_ema": self._slow_ema,
            },
        )

        self._emit_structured("BAR", payload, LogColor.BLUE)


def schedule_stop(node: LiveNode, run_seconds: int):
    del node

    if run_seconds <= 0:
        return None
    return schedule_live_node_interrupt(run_seconds)


def main() -> None:
    instrument_id = INSTRUMENT_ID
    bar_spec = BAR_SPEC.strip().upper()
    run_seconds = int(RUN_SECONDS)
    warmup_minutes = int(WARMUP_MINUTES)

    if not bar_spec.endswith("-INTERNAL"):
        raise ValueError(
            "ProjectX live EMA example requires INTERNAL bars because ProjectX does not "
            "stream venue-native bars over WebSocket",
        )

    bar_type = BarType.from_str(f"{instrument_id}-{bar_spec}")

    node = (
        LiveNode.builder("TESTER-001", TRADER_ID, Environment.LIVE)
        .with_logging(_build_example_logging())
        .with_data_engine_config(
            LiveDataEngineConfig(
                # Do not emit INTERNAL time bars when the underlying futures stream is idle.
                time_bars_build_with_no_updates=False,
            ),
        )
        .with_exec_engine_config(
            LiveExecEngineConfig(
                reconciliation=True,
                position_check_interval_secs=30.0,
            ),
        )
        .with_timeout_connection(20)
        .with_timeout_reconciliation(10)
        .with_timeout_portfolio(10)
        .with_timeout_disconnection_secs(10)
        .with_delay_post_stop_secs(5)
        .add_data_client(
            None,
            ProjectXDataClientFactory(),
            ProjectXDataClientConfig(
                user_name=None,
                api_key=None,
                http_timeout_secs=30,
                market_data_live=MARKET_DATA_LIVE,
            ),
        )
        .add_exec_client(
            None,
            ProjectXExecutionClientFactory(),
            ProjectXExecClientConfig(
                trader_id=TRADER_ID.value,
                account_id=ACCOUNT_ID,
                user_name=None,
                api_key=None,
                http_timeout_secs=30,
                account_type=AccountType.MARGIN,
            ),
        )
        .build()
    )

    node.add_strategy_from_config(
        ImportableStrategyConfig(
            strategy_path=_STRATEGY_PATH,
            config_path=_CONFIG_PATH,
            config={
                "instrument_id": str(instrument_id),
                "client_id": PROJECTX_CLIENT_ID.value,
                "account_id": ACCOUNT_ID,
                "strategy_id": "PROJECTX-EMA-001",
                "bar_type": str(bar_type),
                "trade_size": TRADE_SIZE,
                "fast_ema_period": FAST_EMA_PERIOD,
                "slow_ema_period": SLOW_EMA_PERIOD,
                "warmup_minutes": warmup_minutes,
                "warmup_history_live": WARMUP_HISTORY_LIVE,
                "warmup_contract_live": WARMUP_CONTRACT_LIVE,
                "request_bars": warmup_minutes > 0,
                "unsubscribe_on_stop": True,
                "cleanup_on_stop": True,
                "log_data": False,
                "log_events": True,
                "log_commands": True,
            },
        ),
    )

    print("ProjectX Live EMA Cross")
    print("=" * 50)
    print(f"Instrument ID: {instrument_id}")
    print(f"Account ID: {ACCOUNT_ID}")
    print(f"Bar type: {bar_type}")
    print(f"Trade size: {TRADE_SIZE}")
    print(f"Fast/slow EMA periods: {FAST_EMA_PERIOD}/{SLOW_EMA_PERIOD}")

    if warmup_minutes > 0:
        print(f"Historical warmup: {warmup_minutes} minutes")
        print(f"Warmup request live: {WARMUP_HISTORY_LIVE}")
        print(f"Warmup contract live: {WARMUP_CONTRACT_LIVE}")
    else:
        print("Historical warmup: disabled")

    if run_seconds > 0:
        print(f"Auto-stop after: {run_seconds} seconds")
    else:
        print("Auto-stop after: disabled")
    print()
    print(
        "ProjectX uses INTERNAL live bars for execution and warms them from matching "
        "EXTERNAL historical bars requested on start. Historical warmup should use "
        "ProjectX non-live/sim history even during live execution.",
    )
    print("WARNING: this example can submit live orders to the configured account.")
    print("Use a demo/sim account first.")

    stop_timer = schedule_stop(node, run_seconds)

    try:
        node.run()
    finally:
        if stop_timer is not None:
            stop_timer.cancel()


if __name__ == "__main__":
    main()
