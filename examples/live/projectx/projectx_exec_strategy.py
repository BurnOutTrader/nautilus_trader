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

import threading
import time
from dataclasses import dataclass, field
from typing import Any
from uuid import uuid4

from nautilus_trader._libnautilus.common import LogColor
from nautilus_trader._libnautilus.core import UUID4
from nautilus_trader._libnautilus.model import (
    AccountId,
    ClientId,
    ClientOrderId,
    InstrumentId,
    MarketOrder,
    OrderSide,
    Quantity,
    StrategyId,
    TimeInForce,
)
from nautilus_trader._libnautilus.trading import Strategy, StrategyConfig


@dataclass
class _ExecState:
    instrument_id: str
    account_id: str | None
    instrument_ready: threading.Event = field(default_factory=threading.Event)
    terminal: threading.Event = field(default_factory=threading.Event)
    stopped: threading.Event = field(default_factory=threading.Event)
    started_at_unix: float | None = None
    stopped_at_unix: float | None = None
    last_event_at_unix: float | None = None
    instrument_count: int = 0
    order_submitted_count: int = 0
    order_accepted_count: int = 0
    order_rejected_count: int = 0
    order_canceled_count: int = 0
    order_filled_count: int = 0
    position_event_count: int = 0
    terminal_status: str | None = None
    terminal_reason: str | None = None
    lifecycle: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    lock: threading.Lock = field(default_factory=threading.Lock)


_STATE_LOCK = threading.Lock()
_EXEC_STATES: dict[str, _ExecState] = {}


def register_exec_state(
    *, key: str, instrument_id: str, account_id: str | None
) -> None:
    with _STATE_LOCK:
        _EXEC_STATES[key] = _ExecState(
            instrument_id=instrument_id,
            account_id=account_id,
        )


def clear_exec_state(key: str) -> None:
    with _STATE_LOCK:
        _EXEC_STATES.pop(key, None)


def wait_for_exec_instrument(key: str, timeout: float) -> bool:
    return _require_exec_state(key).instrument_ready.wait(timeout=timeout)


def wait_for_exec_terminal(key: str, timeout: float) -> bool:
    return _require_exec_state(key).terminal.wait(timeout=timeout)


def wait_for_exec_stop(key: str, timeout: float) -> bool:
    return _require_exec_state(key).stopped.wait(timeout=timeout)


def snapshot_exec_state(key: str) -> dict[str, Any]:
    state = _require_exec_state(key)
    with state.lock:
        return {
            "instrument_id": state.instrument_id,
            "account_id": state.account_id,
            "instrument_ready": state.instrument_ready.is_set(),
            "terminal": state.terminal.is_set(),
            "stopped": state.stopped.is_set(),
            "started_at_unix": state.started_at_unix,
            "stopped_at_unix": state.stopped_at_unix,
            "last_event_at_unix": state.last_event_at_unix,
            "instrument_count": state.instrument_count,
            "order_submitted_count": state.order_submitted_count,
            "order_accepted_count": state.order_accepted_count,
            "order_rejected_count": state.order_rejected_count,
            "order_canceled_count": state.order_canceled_count,
            "order_filled_count": state.order_filled_count,
            "position_event_count": state.position_event_count,
            "terminal_status": state.terminal_status,
            "terminal_reason": state.terminal_reason,
            "lifecycle": list(state.lifecycle),
            "errors": list(state.errors),
        }


def _require_exec_state(key: str) -> _ExecState:
    with _STATE_LOCK:
        state = _EXEC_STATES.get(key)

    if state is None:
        raise RuntimeError(f"Unknown ProjectX exec state key: {key}")
    return state


def _append_lifecycle(key: str, entry: str) -> None:
    state = _require_exec_state(key)
    with state.lock:
        state.lifecycle.append(entry)
        state.last_event_at_unix = time.time()


def _append_error(key: str, error: str) -> None:
    state = _require_exec_state(key)
    with state.lock:
        state.errors.append(error)
        state.last_event_at_unix = time.time()


def _mark_started(key: str) -> None:
    state = _require_exec_state(key)
    now = time.time()
    with state.lock:
        state.started_at_unix = now
        state.lifecycle.append("start")
        state.last_event_at_unix = now


def _mark_stopped(key: str) -> None:
    state = _require_exec_state(key)
    now = time.time()
    with state.lock:
        state.stopped_at_unix = now
        state.lifecycle.append("stop")
        state.last_event_at_unix = now
        state.stopped.set()


def _mark_instrument_ready(key: str) -> None:
    state = _require_exec_state(key)
    now = time.time()
    with state.lock:
        state.instrument_count += 1
        state.last_event_at_unix = now
        state.instrument_ready.set()


def _mark_terminal_event(
    key: str,
    *,
    status: str,
    reason: str | None = None,
) -> None:
    state = _require_exec_state(key)
    with state.lock:
        state.terminal_status = status
        state.terminal_reason = reason
        state.last_event_at_unix = time.time()
        state.terminal.set()


def _mark_order_submitted(key: str) -> None:
    state = _require_exec_state(key)
    with state.lock:
        state.order_submitted_count += 1
        state.last_event_at_unix = time.time()


def _mark_order_accepted(key: str) -> None:
    state = _require_exec_state(key)
    with state.lock:
        state.order_accepted_count += 1
        state.last_event_at_unix = time.time()


def _mark_order_rejected(key: str, reason: str | None) -> None:
    state = _require_exec_state(key)
    with state.lock:
        state.order_rejected_count += 1
        state.last_event_at_unix = time.time()
    _mark_terminal_event(key, status="rejected", reason=reason)


def _mark_order_canceled(key: str, reason: str | None) -> None:
    state = _require_exec_state(key)
    with state.lock:
        state.order_canceled_count += 1
        state.last_event_at_unix = time.time()
    _mark_terminal_event(key, status="canceled", reason=reason)


def _mark_order_filled(key: str) -> None:
    state = _require_exec_state(key)
    with state.lock:
        state.order_filled_count += 1
        state.last_event_at_unix = time.time()
    _mark_terminal_event(key, status="filled")


def _mark_position_event(key: str) -> None:
    state = _require_exec_state(key)
    with state.lock:
        state.position_event_count += 1
        state.last_event_at_unix = time.time()


def _coerce_order_side(value: str | OrderSide) -> OrderSide:
    if isinstance(value, OrderSide):
        return value

    return getattr(OrderSide, value.upper())


def _coerce_time_in_force(value: str | TimeInForce) -> TimeInForce:
    if isinstance(value, TimeInForce):
        return value

    return getattr(TimeInForce, value.upper())


def _coerce_account_id(value: str | AccountId | None) -> AccountId | None:
    if value is None or value == "":
        return None

    if isinstance(value, AccountId):
        return value

    if value.startswith("PROJECTX-"):
        return AccountId(value)
    return AccountId(f"PROJECTX-{value}")


class ProjectXExecStrategyConfig(StrategyConfig):
    def __new__(
        cls,
        instrument_id: str | InstrumentId = "MESM26.PROJECTX",
        client_id: str | ClientId = "PROJECTX",
        account_id: str | AccountId | None = None,
        strategy_id: str | StrategyId = "PROJECTX-EXEC-001",
        state_key: str = "projectx-exec-smoke",
        entry_side: str | OrderSide = "BUY",
        entry_qty: str | Quantity = "1",
        entry_time_in_force: str | TimeInForce = "GTC",
        cleanup_on_start: bool = True,
        cleanup_on_stop: bool = True,
        flatten_after_fill: bool = True,
        subscribe_quotes: bool = True,
        subscribe_trades: bool = True,
        unsubscribe_on_stop: bool = True,
        log_data: bool = False,
        log_events: bool = True,
        log_commands: bool = True,
    ):
        parsed_instrument_id = (
            instrument_id
            if isinstance(instrument_id, InstrumentId)
            else InstrumentId.from_str(instrument_id)
        )
        parsed_strategy_id = (
            strategy_id
            if isinstance(strategy_id, StrategyId)
            else StrategyId(strategy_id)
        )
        parsed_client_id = (
            client_id if isinstance(client_id, ClientId) else ClientId(client_id)
        )
        parsed_entry_qty = (
            entry_qty
            if isinstance(entry_qty, Quantity)
            else Quantity.from_str(str(entry_qty))
        )
        parsed_entry_side = _coerce_order_side(entry_side)
        parsed_time_in_force = _coerce_time_in_force(entry_time_in_force)

        config = super().__new__(
            cls,
            strategy_id=parsed_strategy_id,
            external_order_claims=[parsed_instrument_id],
            manage_stop=False,
            market_exit_time_in_force=TimeInForce.GTC,
            market_exit_reduce_only=False,
            log_events=log_events,
            log_commands=log_commands,
        )
        config.instrument_id = parsed_instrument_id
        config.client_id = parsed_client_id
        config.account_id = _coerce_account_id(account_id)
        config.state_key = state_key
        config.entry_side = parsed_entry_side
        config.entry_qty = parsed_entry_qty
        config.entry_time_in_force = parsed_time_in_force
        config.cleanup_on_start = cleanup_on_start
        config.cleanup_on_stop = cleanup_on_stop
        config.flatten_after_fill = flatten_after_fill
        config.subscribe_quotes = subscribe_quotes
        config.subscribe_trades = subscribe_trades
        config.unsubscribe_on_stop = unsubscribe_on_stop
        config.log_data = log_data
        return config


class ProjectXExecStrategy(Strategy):
    def __init__(self, config: ProjectXExecStrategyConfig):
        super().__init__(config)
        self._entry_submitted = False
        self._flatten_requested = False
        self._instrument_ready = False
        self._startup_cleanup_logged = False
        self._cleanup_cancel_requests: set[str] = set()
        self._cleanup_close_requests: set[str] = set()

    def on_start(self):
        _mark_started(self.config.state_key)
        self._info(
            f"Starting ProjectX execution strategy for {self.config.instrument_id} "
            f"via client {self.config.client_id} snapshot={self._runtime_snapshot()}",
        )

        if self.config.cleanup_on_start:
            self._cancel_open_orders()
            self._close_open_positions()

        if self.config.subscribe_quotes:
            self.subscribe_quotes(
                instrument_id=self.config.instrument_id,
                client_id=self.config.client_id,
            )

        if self.config.subscribe_trades:
            self.subscribe_trades(
                instrument_id=self.config.instrument_id,
                client_id=self.config.client_id,
            )

        instrument = self.cache.instrument(self.config.instrument_id)

        if instrument is not None:
            self.on_instrument(instrument)
        else:
            self.request_instrument(
                instrument_id=self.config.instrument_id,
                client_id=self.config.client_id,
            )
            _append_error(
                self.config.state_key,
                f"Instrument not found in cache at start: {self.config.instrument_id}",
            )
            self._error(f"Instrument not found in cache: {self.config.instrument_id}")

    def on_stop(self):
        self._info(
            f"Stopping ProjectX execution strategy snapshot={self._runtime_snapshot()}"
        )

        if self.config.cleanup_on_stop:
            self._cancel_open_orders(stop_phase=True)
            self._close_open_positions()

        if self.config.unsubscribe_on_stop and self.config.subscribe_quotes:
            self.unsubscribe_quotes(
                instrument_id=self.config.instrument_id,
                client_id=self.config.client_id,
            )

        if self.config.unsubscribe_on_stop and self.config.subscribe_trades:
            self.unsubscribe_trades(
                instrument_id=self.config.instrument_id,
                client_id=self.config.client_id,
            )

        _mark_stopped(self.config.state_key)

    def on_time_event(self, event):
        return None

    def on_instrument(self, instrument):
        self._info(
            f"Instrument ready: {instrument} snapshot={self._runtime_snapshot()}"
        )
        self._instrument_ready = True
        _mark_instrument_ready(self.config.state_key)
        self._maybe_submit_entry_order()

    def on_quote(self, quote):
        if self.config.log_data:
            self._info(f"Quote: {quote}")
        self._maybe_submit_entry_order()

    def on_trade(self, trade):
        if self.config.log_data:
            self._info(f"Trade: {trade}")
        self._maybe_submit_entry_order()

    def on_order_submitted(self, event):
        _mark_order_submitted(self.config.state_key)
        self._info(self._format_order_event("Order submitted", event))

    def on_order_accepted(self, event):
        _mark_order_accepted(self.config.state_key)
        self._info(self._format_order_event("Order accepted", event))
        self._maybe_submit_entry_order()

    def on_order_rejected(self, event):
        _append_error(self.config.state_key, f"Order rejected: {event}")
        _mark_order_rejected(
            self.config.state_key,
            getattr(event, "reason", None),
        )
        self._cleanup_close_requests.clear()
        self._refresh_cleanup_state()
        self._error(
            self._format_order_event("Order rejected", event, include_reason=True)
        )
        self._maybe_submit_entry_order()

    def on_order_canceled(self, event):
        _mark_order_canceled(
            self.config.state_key,
            getattr(event, "reason", None),
        )
        self._cleanup_close_requests.clear()
        self._refresh_cleanup_state()
        self._info(
            self._format_order_event("Order canceled", event, include_reason=True)
        )
        self._maybe_submit_entry_order()

    def on_order_filled(self, event):
        _mark_order_filled(self.config.state_key)
        self._refresh_cleanup_state()
        self._info(self._format_order_event("Order filled", event, include_fill=True))

        if self.config.flatten_after_fill and not self._flatten_requested:
            self._flatten_requested = True
            self._info(
                f"Flattening position for {self.config.instrument_id} "
                f"snapshot={self._runtime_snapshot()}",
            )
            self.close_all_positions(
                instrument_id=self.config.instrument_id,
                client_id=self.config.client_id,
                time_in_force=TimeInForce.GTC,
                reduce_only=False,
            )
        self._maybe_submit_entry_order()

    def on_position_event(self, event):
        _mark_position_event(self.config.state_key)
        self._refresh_cleanup_state()
        self._info(self._format_position_event(event))
        self._maybe_submit_entry_order()

    def _submit_entry_order(self):
        if self._entry_submitted:
            return

        order = MarketOrder(
            trader_id=self.trader_id,
            strategy_id=self.strategy_id,
            instrument_id=self.config.instrument_id,
            client_order_id=ClientOrderId(f"PX-{uuid4().hex[:24].upper()}"),
            order_side=self.config.entry_side,
            quantity=self.config.entry_qty,
            init_id=UUID4(),
            ts_init=self.clock.timestamp_ns(),
            time_in_force=self.config.entry_time_in_force,
            reduce_only=False,
            quote_quantity=False,
            tags=["projectx", "exec", "example"],
        )
        self._info(
            f"Submitting entry account={self.config.account_id} via={self.config.client_id} "
            f"side={self.config.entry_side.name} qty={self.config.entry_qty} "
            f"client_order_id={order.client_order_id} snapshot={self._runtime_snapshot()}",
        )

        self.submit_order(
            order=order,
            client_id=self.config.client_id,
        )
        self._entry_submitted = True

    def _maybe_submit_entry_order(self):
        if self._entry_submitted or not self._instrument_ready:
            return

        self._refresh_cleanup_state()

        if self.config.cleanup_on_start and self._has_open_state():
            if self._has_open_positions():
                self._close_open_positions()

            if not self._startup_cleanup_logged:
                account_scope = (
                    f" account {self.config.account_id.value}"
                    if self.config.account_id is not None
                    else " strategy-owned state"
                )
                self._info(
                    f"Waiting for startup cleanup to finish for{account_scope} "
                    f"on {self.config.instrument_id} snapshot={self._runtime_snapshot()}",
                )
                self._startup_cleanup_logged = True
            return

        self._submit_entry_order()

    def _cancel_open_orders(self, stop_phase: bool = False):
        orders = self._open_orders()
        inflight_order_ids: set[str] = set()

        if stop_phase:
            cache_kwargs = self._cache_query_kwargs()
            inflight_order_ids = {
                order.client_order_id.value
                for order in self.cache.orders_inflight(**cache_kwargs)
            }

        if not orders:
            self._info(
                f"No {self.config.instrument_id} orders to cancel "
                f"snapshot={self._runtime_snapshot()}",
            )
            return

        skipped_inflight = 0

        for order in orders:
            if (
                stop_phase
                and order.client_order_id.value in self._cleanup_cancel_requests
            ):
                continue

            if stop_phase and order.client_order_id.value in inflight_order_ids:
                skipped_inflight += 1
                continue
            key = order.client_order_id.value

            if key in self._cleanup_cancel_requests:
                continue
            self._info(
                f"Canceling order account={self.config.account_id} "
                f"client_order_id={order.client_order_id} tif={getattr(order, 'time_in_force', None)} "
                f"snapshot={self._runtime_snapshot()}",
            )
            self.cancel_order(order.client_order_id, client_id=self.config.client_id)
            self._cleanup_cancel_requests.add(key)

        if stop_phase and skipped_inflight > 0:
            self._info(
                "Shutdown cleanup skipped redundant cancels for "
                f"{skipped_inflight} inflight orders "
                f"snapshot={self._runtime_snapshot()}",
            )

    def _close_open_positions(self):
        if self._has_open_orders():
            return

        positions = self._open_positions()

        if not positions:
            self._info(
                f"No {self.config.instrument_id} positions to close "
                f"snapshot={self._runtime_snapshot()}",
            )
            return

        for position in positions:
            key = position.id.value

            if key in self._cleanup_close_requests:
                continue
            self._info(
                f"Closing position account={self.config.account_id} position_id={position.id} "
                f"qty={position.quantity} avg_px_open={position.avg_px_open} "
                f"snapshot={self._runtime_snapshot()}",
            )
            self.close_position(
                position=position,
                client_id=self.config.client_id,
                time_in_force=TimeInForce.GTC,
                reduce_only=False,
            )
            self._cleanup_close_requests.add(key)

    def _has_open_state(self) -> bool:
        return self._has_open_orders() or self._has_open_positions()

    def _has_open_orders(self) -> bool:
        return len(self._open_orders()) > 0

    def _has_open_positions(self) -> bool:
        return len(self._open_positions()) > 0

    def _open_orders(self) -> list:
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

    def _runtime_snapshot(self) -> str:
        return (
            f"state={self._portfolio_state()}"
            f"/net={self._net_position():g}"
            f"/pos={len(self._open_positions())}"
            f"/ord={len(self._open_orders())}"
            f"/balances={self._balance_summary()}"
            f"/margins={self._margin_summary()}"
        )

    def _latest_account_event(self) -> dict | None:
        if self.config.account_id is None:
            return None

        account = self.cache.account(self.config.account_id)

        if account is None or not hasattr(account, "to_dict"):
            return None

        account_dict = account.to_dict()
        events = account_dict.get("events") or []

        if not events:
            return None

        return events[-1]

    def _balance_summary(self) -> str:
        account_event = self._latest_account_event()

        if not account_event:
            return "unavailable"

        balances = account_event.get("balances") or []

        if not balances:
            return "none"

        formatted = []

        for balance in balances:
            currency = balance.get("currency", "?")
            total = balance.get("total", "?")
            free = balance.get("free", "?")
            locked = balance.get("locked", "?")
            formatted.append(f"{currency}:t={total},f={free},l={locked}")

        return ",".join(sorted(formatted))

    def _margin_summary(self) -> str:
        account_event = self._latest_account_event()

        if not account_event:
            return "unavailable"

        margins = account_event.get("margins") or []

        if not margins:
            return "none"

        formatted = []

        for margin in margins:
            instrument_id = margin.get("instrument_id", "?")
            initial = margin.get("initial", "?")
            maintenance = margin.get("maintenance", "?")
            currency = margin.get("currency", "?")
            formatted.append(f"{instrument_id}:i={initial},m={maintenance},{currency}")

        return ",".join(sorted(formatted))

    def _format_order_event(
        self,
        prefix: str,
        event,
        *,
        include_reason: bool = False,
        include_fill: bool = False,
    ) -> str:
        parts = [prefix]
        account_id = getattr(event, "account_id", None)
        client_order_id = getattr(event, "client_order_id", None)
        order_side = getattr(event, "order_side", None)
        last_qty = getattr(event, "last_qty", None)
        last_px = getattr(event, "last_px", None)
        reason = getattr(event, "reason", None)

        if account_id is not None:
            parts.append(f"account={account_id}")

        if client_order_id is not None:
            parts.append(f"client_order_id={client_order_id}")

        if include_fill and order_side is not None:
            parts.append(f"side={order_side}")

        if include_fill and last_qty is not None:
            parts.append(f"last_qty={last_qty}")

        if include_fill and last_px is not None:
            parts.append(f"last_px={last_px}")

        if include_reason and reason:
            parts.append(f"reason={reason}")

        parts.append(f"snapshot={self._runtime_snapshot()}")
        return " ".join(parts)

    def _format_position_event(self, event) -> str:
        parts = ["Position event"]
        account_id = getattr(event, "account_id", None)
        position_id = getattr(event, "position_id", None)

        if account_id is not None:
            parts.append(f"account={account_id}")

        if position_id is not None:
            parts.append(f"position_id={position_id}")

        parts.append(f"snapshot={self._runtime_snapshot()}")
        return " ".join(parts)

    def _refresh_cleanup_state(self):
        current_order_ids = {
            order.client_order_id.value for order in self._open_orders()
        }
        current_position_ids = {
            position.id.value for position in self._open_positions()
        }
        self._cleanup_cancel_requests.intersection_update(current_order_ids)
        self._cleanup_close_requests.intersection_update(current_position_ids)

    def _info(self, message: str):
        self.log.info(message, LogColor.NORMAL)

    def _error(self, message: str):
        self.log.error(message, LogColor.RED)
