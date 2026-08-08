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

from __future__ import annotations

import json
from collections.abc import Sequence
from contextlib import suppress
from dataclasses import dataclass
from typing import TypedDict
from uuid import uuid4

from nautilus_trader._libnautilus.core import UUID4
from nautilus_trader._libnautilus.model import AccountId
from nautilus_trader._libnautilus.model import Bar
from nautilus_trader._libnautilus.model import BarType
from nautilus_trader._libnautilus.model import ClientId
from nautilus_trader._libnautilus.model import ClientOrderId
from nautilus_trader._libnautilus.model import InstrumentId
from nautilus_trader._libnautilus.common import LogColor
from nautilus_trader._libnautilus.model import MarketOrder
from nautilus_trader._libnautilus.model import OrderSide
from nautilus_trader._libnautilus.model import Quantity
from nautilus_trader._libnautilus.model import StrategyId
from nautilus_trader._libnautilus.model import TimeInForce
from nautilus_trader._libnautilus.trading import Strategy
from nautilus_trader._libnautilus.trading import StrategyConfig


@dataclass(frozen=True)
class RithmicRoute:
    label: str
    exec_client_id: ClientId
    account_id: AccountId


class RouteConfig(TypedDict):
    label: str
    exec_client_id: str | ClientId
    account_id: str | AccountId


def _coerce_instrument_id(value: str | InstrumentId) -> InstrumentId:
    if isinstance(value, InstrumentId):
        return value
    return InstrumentId.from_str(value)


def _coerce_client_id(value: str | ClientId) -> ClientId:
    if isinstance(value, ClientId):
        return value
    return ClientId(value)


def _coerce_account_id(value: str | AccountId) -> AccountId:
    if isinstance(value, AccountId):
        return value
    return AccountId(value)


def _coerce_strategy_id(value: str | StrategyId) -> StrategyId:
    if isinstance(value, StrategyId):
        return value
    return StrategyId(value)


def _coerce_bar_type(value: str | BarType) -> BarType:
    if isinstance(value, BarType):
        return value
    return BarType.from_str(value)


def _coerce_quantity(value: str | Quantity) -> Quantity:
    if isinstance(value, Quantity):
        return value
    return Quantity.from_str(str(value))


def _coerce_price(value) -> float:
    return value.as_double()


def _serialize_log_payload(payload) -> str:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)


def _coerce_routes(routes: Sequence[RithmicRoute | RouteConfig]) -> tuple[RithmicRoute, ...]:
    if isinstance(routes, tuple) and all(isinstance(route, RithmicRoute) for route in routes):
        return routes

    parsed: list[RithmicRoute] = []

    for route in routes:
        if isinstance(route, RithmicRoute):
            parsed.append(route)
            continue

        parsed.append(
            RithmicRoute(
                label=str(route["label"]),
                exec_client_id=_coerce_client_id(route["exec_client_id"]),
                account_id=_coerce_account_id(route["account_id"]),
            ),
        )

    if not parsed:
        raise ValueError("At least one execution route is required")
    return tuple(parsed)


class RithmicEMACrossStrategyConfig(StrategyConfig):
    def __new__(
        cls,
        instrument_id: str | InstrumentId = "MNQM6.RITHMIC",
        data_client_id: str | ClientId = "RITHMIC",
        strategy_id: str | StrategyId = "RITHMIC-EMA-001",
        bar_type: str | BarType = "MNQM6.RITHMIC-15-SECOND-LAST-EXTERNAL",
        trade_size: str | Quantity = "1",
        fast_ema_period: int = 10,
        slow_ema_period: int = 20,
        warmup_minutes: int = 30,
        request_bars: bool = True,
        routes: Sequence[RithmicRoute | RouteConfig] = (),
        unsubscribe_on_stop: bool = True,
        cleanup_on_stop: bool = True,
        log_data: bool = False,
        log_events: bool = True,
        log_commands: bool = True,
    ):
        parsed_instrument_id = _coerce_instrument_id(instrument_id)
        parsed_strategy_id = _coerce_strategy_id(strategy_id)

        config = super().__new__(
            cls,
            strategy_id=parsed_strategy_id,
            external_order_claims=[parsed_instrument_id],
            manage_stop=False,
            market_exit_time_in_force=TimeInForce.IOC,
            market_exit_reduce_only=True,
            log_events=log_events,
            log_commands=log_commands,
        )
        config.instrument_id = parsed_instrument_id
        config.data_client_id = _coerce_client_id(data_client_id)
        config.bar_type = _coerce_bar_type(bar_type)
        config.trade_size = _coerce_quantity(trade_size)
        config.fast_ema_period = int(fast_ema_period)
        config.slow_ema_period = int(slow_ema_period)
        config.warmup_minutes = int(warmup_minutes)
        config.request_bars = bool(request_bars)
        config.routes = _coerce_routes(routes)
        config.unsubscribe_on_stop = bool(unsubscribe_on_stop)
        config.cleanup_on_stop = bool(cleanup_on_stop)
        config.log_data = bool(log_data)
        return config


class RithmicEMACrossStrategy(Strategy):
    def __init__(self, config: RithmicEMACrossStrategyConfig):
        super().__init__(config)
        self.config = config
        self._started = False
        self._instrument_ready = False
        self._warmup_complete = not config.request_bars or config.warmup_minutes <= 0
        self._warmup_cutoff_ns: int | None = None
        self._fast_ema = 0.0
        self._slow_ema = 0.0
        self._bar_count = 0
        self._last_processed_bar_ts_event: int | None = None
        self._last_signal_is_bullish: bool | None = None
        self._fast_alpha = 2.0 / (int(config.fast_ema_period) + 1)
        self._slow_alpha = 2.0 / (int(config.slow_ema_period) + 1)

    def on_start(self):
        instrument = self.cache.instrument(self.config.instrument_id)

        if instrument is not None:
            self.on_instrument(instrument)
        else:
            self.request_instrument(
                instrument_id=self.config.instrument_id,
                client_id=self.config.data_client_id,
            )

    def on_stop(self):
        if self.config.cleanup_on_stop:
            for route in self.config.routes:
                self._cancel_route_orders(route)
                self._close_route_positions(route)

        if self.config.unsubscribe_on_stop:
            self.unsubscribe_bars(
                bar_type=self.config.bar_type,
                client_id=self.config.data_client_id,
            )

    def on_instrument(self, instrument):
        if instrument.id != self.config.instrument_id:
            return
        self._instrument_ready = True
        self._start_strategy()

    def on_order_rejected(self, event):
        self._log_order_event(event, color=LogColor.RED, level="error")

    def on_order_submitted(self, event):
        self._log_order_event(event, color=LogColor.BLUE)

    def on_order_accepted(self, event):
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
        if isinstance(data, Bar) and data.bar_type == self.config.bar_type:
            self._process_bar(data)

    def on_historical_bars(self, bars):
        warmup_bars = [bar for bar in bars if bar.bar_type == self.config.bar_type]

        for bar in warmup_bars:
            self._process_bar(bar)

        if warmup_bars:
            self.log.info(
                f"Loaded {len(warmup_bars)} historical warmup bars",
                LogColor.BLUE,
            )

    def on_time_event(self, event):
        return None

    def _start_strategy(self):
        if self._started or not self._instrument_ready:
            return

        self._started = True

        if self.config.request_bars and self.config.warmup_minutes > 0:
            warmup_ns = int(self.config.warmup_minutes) * 60 * 1_000_000_000
            self._warmup_cutoff_ns = self.clock.timestamp_ns()
            self.request_bars(
                bar_type=self.config.bar_type,
                start=self._warmup_cutoff_ns - warmup_ns,
                end=self._warmup_cutoff_ns,
                client_id=self.config.data_client_id,
            )

        self.subscribe_bars(
            bar_type=self.config.bar_type,
            client_id=self.config.data_client_id,
        )

    def _update_ema_state(self, *, ts_event: int, close: float) -> None:
        self._last_processed_bar_ts_event = ts_event
        self._bar_count += 1

        if self._bar_count == 1:
            self._fast_ema = close
            self._slow_ema = close
            return

        self._fast_ema = self._fast_alpha * close + (1.0 - self._fast_alpha) * self._fast_ema
        self._slow_ema = self._slow_alpha * close + (1.0 - self._slow_alpha) * self._slow_ema

    def _process_bar(self, bar: Bar) -> None:
        if (
            self._last_processed_bar_ts_event is not None
            and bar.ts_event <= self._last_processed_bar_ts_event
        ):
            return

        if (
            not self._warmup_complete
            and self._warmup_cutoff_ns is not None
            and bar.ts_event > self._warmup_cutoff_ns
        ):
            self._warmup_complete = True
            self.log.info(
                "Historical warmup complete; live Rithmic execution enabled",
                LogColor.GREEN,
            )

        close = _coerce_price(bar.close)
        was_first_bar = self._bar_count == 0
        self._update_ema_state(ts_event=bar.ts_event, close=close)
        is_bullish = self._fast_ema >= self._slow_ema
        previous_signal = self._last_signal_is_bullish
        self._last_signal_is_bullish = is_bullish

        if was_first_bar:
            if self._warmup_complete:
                self._log_warmup_bar(bar, close)
            return

        if not self._warmup_complete:
            return

        if self._bar_count < int(self.config.slow_ema_period):
            self._log_warmup_bar(bar, close)
            return

        crossed = previous_signal is not None and previous_signal != is_bullish

        for route in self.config.routes:
            self._rebalance_route(route, is_bullish=is_bullish, crossed=crossed)

        signal = "BUY" if is_bullish else "SELL"
        payload = self._object_payload(bar)
        payload.update(
            {
                "close": close,
                "fast_ema": self._fast_ema,
                "slow_ema": self._slow_ema,
                "signal": signal,
                "crossed": crossed,
                "phase": "live",
            },
        )
        self._emit_structured("BAR", payload, LogColor.BLUE)

    def _log_warmup_bar(self, bar: Bar, close: float) -> None:
        remaining = max(int(self.config.slow_ema_period) - self._bar_count, 0)
        payload = self._object_payload(bar)
        payload.update(
            {
                "close": close,
                "fast_ema": self._fast_ema,
                "slow_ema": self._slow_ema,
                "remaining": remaining,
                "phase": "warmup",
            },
        )
        self._emit_structured("BAR", payload, LogColor.BLUE)

    def _rebalance_route(self, route: RithmicRoute, *, is_bullish: bool, crossed: bool) -> None:
        if not crossed or self._has_active_orders(route):
            return

        portfolio_state = self._route_portfolio_state(route)

        if is_bullish:
            if portfolio_state == "FLAT":
                self._submit_route_order(route, OrderSide.BUY)
            elif portfolio_state == "SHORT":
                self._close_route_positions(route)
        else:
            if portfolio_state == "FLAT":
                self._submit_route_order(route, OrderSide.SELL)
            elif portfolio_state == "LONG":
                self._close_route_positions(route)

    def _submit_route_order(self, route: RithmicRoute, order_side: OrderSide):
        order = MarketOrder(
            trader_id=self.trader_id,
            strategy_id=self.strategy_id,
            instrument_id=self.config.instrument_id,
            client_order_id=ClientOrderId(f"RTHEMA-{uuid4().hex[:24].upper()}"),
            order_side=order_side,
            quantity=self.config.trade_size,
            init_id=UUID4(),
            ts_init=self.clock.timestamp_ns(),
            time_in_force=TimeInForce.IOC,
            reduce_only=False,
            quote_quantity=False,
            tags=["rithmic", "ema", "example", route.label],
        )
        payload = self._object_payload(order)
        payload.update(
            {
                "action": "submit_entry",
                "route": route.label,
                "account_id": route.account_id,
                "client_id": route.exec_client_id,
                "fast_ema": self._fast_ema,
                "slow_ema": self._slow_ema,
            },
        )
        self._emit_structured("ORDER_REQUEST", payload, LogColor.BLUE)
        self.submit_order(order=order, client_id=route.exec_client_id)

    def _close_route_positions(self, route: RithmicRoute):
        for position in self._open_positions(route):
            payload = self._object_payload(position)
            payload.update(
                {
                    "action": "close_position",
                    "route": route.label,
                    "account_id": route.account_id,
                    "client_id": route.exec_client_id,
                    "time_in_force": TimeInForce.IOC,
                    "reduce_only": True,
                },
            )
            self._emit_structured("POSITION_REQUEST", payload, LogColor.YELLOW)
            self.close_position(
                position=position,
                client_id=route.exec_client_id,
                time_in_force=TimeInForce.IOC,
                reduce_only=True,
            )

    def _cancel_route_orders(self, route: RithmicRoute):
        orders = self._active_orders(route)

        if orders:
            for order in orders:
                payload = self._object_payload(order)
                payload.update(
                    {
                        "action": "cancel_order",
                        "route": route.label,
                        "account_id": route.account_id,
                        "client_id": route.exec_client_id,
                    },
                )
                self._emit_structured("ORDER_REQUEST", payload, LogColor.YELLOW)
            self.cancel_orders(orders=orders, client_id=route.exec_client_id)

    def _route_portfolio_state(self, route: RithmicRoute) -> str:
        positions = self._open_positions(route)

        if not positions:
            return "FLAT"

        net_position = sum(position.signed_qty for position in positions)

        if net_position > 0:
            return "LONG"

        if net_position < 0:
            return "SHORT"
        return "FLAT"

    def _route_net_position(self, route: RithmicRoute) -> float:
        return float(sum(position.signed_qty for position in self._open_positions(route)))

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
        self._emit_structured("ORDER_EVENT", self._object_payload(event), color, level=level)

    def _log_position_event(self, event, *, color) -> None:
        self._emit_structured("POSITION_EVENT", self._object_payload(event), color)

    def _has_active_orders(self, route: RithmicRoute) -> bool:
        return len(self._active_orders(route)) > 0

    def _active_orders(self, route: RithmicRoute) -> list:
        return (
            list(self.cache.orders_open(**self._cache_query_kwargs(route)))
            + list(self.cache.orders_emulated(**self._cache_query_kwargs(route)))
            + list(self.cache.orders_inflight(**self._cache_query_kwargs(route)))
        )

    def _open_positions(self, route: RithmicRoute) -> list:
        return list(self.cache.positions_open(**self._cache_query_kwargs(route)))

    def _cache_query_kwargs(self, route: RithmicRoute) -> dict:
        return {
            "instrument_id": self.config.instrument_id,
            "account_id": route.account_id,
        }
