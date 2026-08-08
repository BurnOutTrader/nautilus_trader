// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2026 Kevin Monaghan. All rights reserved.
//
//  Licensed under the GNU Lesser General Public License Version 3.0 or later.
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Python bindings for event types.
//!
//! This module exposes market data and execution events to Python,
//! enabling callbacks and event streaming from Rust to Python.

#[cfg(feature = "python")]
use nautilus_core::python::to_pyvalue_err;
#[cfg(feature = "python")]
use nautilus_model::{
    data::{
        Bar as NautilusBar, BarType, BookOrder, InstrumentStatus,
        OrderBookDelta as NautilusOrderBookDelta, OrderBookDepth10, QuoteTick as NautilusQuoteTick,
        TradeTick as NautilusTradeTick, bar::BarTypeParseError,
    },
    enums::{AggressorSide, BookAction, OrderSide as NautilusOrderSide},
    identifiers::{InstrumentId as NautilusInstrumentId, TradeId},
    types::{Price, Quantity},
};
#[cfg(feature = "python")]
use pyo3::prelude::*;

use crate::{
    data::{
        BookDelta, MarketDataEvent, QuoteTick, RithmicBarType, TimeBar as LiveTimeBar, TradeTick,
        custom::{
            END_OF_DAY_PRICES_TYPE_NAME, INDICATOR_PRICES_TYPE_NAME, OPEN_INTEREST_TYPE_NAME,
            ORDER_PRICE_LIMITS_TYPE_NAME, QUOTE_STATISTICS_TYPE_NAME, RithmicCustomData,
            SYMBOL_MARGIN_RATE_TYPE_NAME, TRADE_STATISTICS_TYPE_NAME,
        },
    },
    execution::{
        ExecutionEvent, OrderAccepted, OrderCancelled, OrderFilled, OrderModified, OrderRejected,
        OrderSubmitted,
    },
    providers::{AccountEvent, PositionEvent},
};

// Market data events.

/// Python wrapper for QuoteTick (best bid/offer update).
#[cfg(feature = "python")]
#[pyclass(name = "QuoteTick", skip_from_py_object)]
#[pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.rithmic")]
#[derive(Clone)]
pub(crate) struct PyQuoteTick {
    inner: QuoteTick,
}

#[cfg(feature = "python")]
#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl PyQuoteTick {
    /// Instrument symbol.
    #[getter(symbol)]
    fn py_symbol(&self) -> &str {
        &self.inner.symbol
    }

    /// Exchange code.
    #[getter(exchange)]
    fn py_exchange(&self) -> &str {
        &self.inner.exchange
    }

    /// Best bid price.
    #[getter(bid_price)]
    fn py_bid_price(&self) -> f64 {
        self.inner.bid_price
    }

    /// Best ask price.
    #[getter(ask_price)]
    fn py_ask_price(&self) -> f64 {
        self.inner.ask_price
    }

    /// Bid size.
    #[getter(bid_size)]
    fn py_bid_size(&self) -> f64 {
        self.inner.bid_size
    }

    /// Ask size.
    #[getter(ask_size)]
    fn py_ask_size(&self) -> f64 {
        self.inner.ask_size
    }

    /// Price precision (number of decimal places).
    #[getter(price_precision)]
    fn py_price_precision(&self) -> u8 {
        self.inner.price_precision
    }

    /// Size precision (number of decimal places).
    #[getter(size_precision)]
    fn py_size_precision(&self) -> u8 {
        self.inner.size_precision
    }

    /// Event timestamp in nanoseconds.
    #[getter(ts_event)]
    fn py_ts_event(&self) -> u64 {
        self.inner.ts_event
    }

    /// Initialization timestamp in nanoseconds.
    #[getter(ts_init)]
    fn py_ts_init(&self) -> u64 {
        self.inner.ts_init
    }

    /// Converts this Rithmic `QuoteTick` into a NautilusTrader `QuoteTick`.
    ///
    /// Parameters
    /// ----------
    /// instrument_id : str
    ///     The NautilusTrader instrument ID string (e.g. ``"ESH5.RITHMIC"``).
    /// price_precision : int, optional
    ///     Number of decimal places for prices. If not provided, uses stored value.
    /// size_precision : int, optional
    ///     Number of decimal places for sizes. If not provided, uses stored value.
    ///
    /// Returns
    /// -------
    /// nautilus_trader.model.data.QuoteTick
    #[pyo3(signature = (instrument_id, price_precision = None, size_precision = None))]
    #[pyo3(name = "to_nautilus_quote_tick")]
    fn py_to_nautilus_quote_tick(
        &self,
        instrument_id: &str,
        price_precision: Option<u8>,
        size_precision: Option<u8>,
    ) -> PyResult<NautilusQuoteTick> {
        let instrument_id = NautilusInstrumentId::from(instrument_id);
        let price_prec = price_precision.unwrap_or(self.inner.price_precision);
        let size_prec = size_precision.unwrap_or(self.inner.size_precision);
        NautilusQuoteTick::new_checked(
            instrument_id,
            Price::new(self.inner.bid_price, price_prec),
            Price::new(self.inner.ask_price, price_prec),
            Quantity::new(self.inner.bid_size, size_prec),
            Quantity::new(self.inner.ask_size, size_prec),
            self.inner.ts_event.into(),
            self.inner.ts_init.into(),
        )
        .map_err(to_pyvalue_err)
    }

    #[pyo3(name = "__repr__")]
    fn py_repr(&self) -> String {
        format!(
            "QuoteTick(symbol={}, exchange={}, bid={:.6}, ask={:.6}, bid_size={}, ask_size={})",
            self.inner.symbol,
            self.inner.exchange,
            self.inner.bid_price,
            self.inner.ask_price,
            self.inner.bid_size,
            self.inner.ask_size,
        )
    }
}

impl From<QuoteTick> for PyQuoteTick {
    fn from(tick: QuoteTick) -> Self {
        Self { inner: tick }
    }
}

/// Python wrapper for TradeTick (last trade).
#[cfg(feature = "python")]
#[pyclass(name = "TradeTick", skip_from_py_object)]
#[pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.rithmic")]
#[derive(Clone)]
pub(crate) struct PyTradeTick {
    inner: TradeTick,
}

#[cfg(feature = "python")]
#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl PyTradeTick {
    /// Instrument symbol.
    #[getter(symbol)]
    fn py_symbol(&self) -> &str {
        &self.inner.symbol
    }

    /// Exchange code.
    #[getter(exchange)]
    fn py_exchange(&self) -> &str {
        &self.inner.exchange
    }

    /// Trade price.
    #[getter(price)]
    fn py_price(&self) -> f64 {
        self.inner.price
    }

    /// Trade size.
    #[getter(size)]
    fn py_size(&self) -> f64 {
        self.inner.size
    }

    /// Aggressor side ("BUY" or "SELL").
    #[getter(aggressor_side)]
    fn py_aggressor_side(&self) -> &str {
        &self.inner.aggressor_side
    }

    /// Trade ID.
    #[getter(trade_id)]
    fn py_trade_id(&self) -> &str {
        &self.inner.trade_id
    }

    /// Price precision (number of decimal places).
    #[getter(price_precision)]
    fn py_price_precision(&self) -> u8 {
        self.inner.price_precision
    }

    /// Size precision (number of decimal places).
    #[getter(size_precision)]
    fn py_size_precision(&self) -> u8 {
        self.inner.size_precision
    }

    /// Event timestamp in nanoseconds.
    #[getter(ts_event)]
    fn py_ts_event(&self) -> u64 {
        self.inner.ts_event
    }

    /// Initialization timestamp in nanoseconds.
    #[getter(ts_init)]
    fn py_ts_init(&self) -> u64 {
        self.inner.ts_init
    }

    /// Converts this Rithmic `TradeTick` into a NautilusTrader `TradeTick`.
    ///
    /// Parameters
    /// ----------
    /// instrument_id : str
    ///     The NautilusTrader instrument ID string (e.g. ``"ESH5.RITHMIC"``).
    /// price_precision : int, optional
    ///     Number of decimal places for prices. If not provided, uses stored value.
    /// size_precision : int, optional
    ///     Number of decimal places for sizes. If not provided, uses stored value.
    ///
    /// Returns
    /// -------
    /// nautilus_trader.model.data.TradeTick
    #[pyo3(signature = (instrument_id, price_precision = None, size_precision = None))]
    #[pyo3(name = "to_nautilus_trade_tick")]
    fn py_to_nautilus_trade_tick(
        &self,
        instrument_id: &str,
        price_precision: Option<u8>,
        size_precision: Option<u8>,
    ) -> NautilusTradeTick {
        let instrument_id = NautilusInstrumentId::from(instrument_id);
        let trade_id = TradeId::new(&self.inner.trade_id);
        let aggressor_side = match self.inner.aggressor_side.as_str() {
            "BUY" => AggressorSide::Buyer,
            "SELL" => AggressorSide::Seller,
            _ => AggressorSide::NoAggressor,
        };
        let price_prec = price_precision.unwrap_or(self.inner.price_precision);
        let size_prec = size_precision.unwrap_or(self.inner.size_precision);
        NautilusTradeTick {
            instrument_id,
            price: Price::new(self.inner.price, price_prec),
            size: Quantity::new(self.inner.size, size_prec),
            aggressor_side,
            trade_id,
            ts_event: self.inner.ts_event.into(),
            ts_init: self.inner.ts_init.into(),
        }
    }

    #[pyo3(name = "__repr__")]
    fn py_repr(&self) -> String {
        format!(
            "TradeTick(symbol={}, exchange={}, price={:.6}, size={}, side={})",
            self.inner.symbol,
            self.inner.exchange,
            self.inner.price,
            self.inner.size,
            self.inner.aggressor_side,
        )
    }
}

impl From<TradeTick> for PyTradeTick {
    fn from(tick: TradeTick) -> Self {
        Self { inner: tick }
    }
}

impl PyTradeTick {
    /// Creates a synthetic trade tick from a 1-tick replay bar.
    ///
    /// Rithmic historical tick replay is delivered as `ResponseTickBarReplay`
    /// messages rather than native tick-by-tick trades, so the aggressor side
    /// is always unknown on this path.
    pub(crate) fn from_tick_replay(
        tick: &rithmic_rs::rti::ResponseTickBarReplay,
        sequence: usize,
    ) -> Option<Self> {
        let ts_event = tick_timestamp_nanos(&tick.data_bar_ssboe, &tick.data_bar_usecs);
        if ts_event == 0 {
            return None;
        }
        let price = tick
            .close_price
            .or(tick.open_price)
            .or(tick.high_price)
            .or(tick.low_price)
            .unwrap_or(0.0);

        Some(Self {
            inner: TradeTick {
                symbol: tick.symbol.clone().unwrap_or_default(),
                exchange: tick.exchange.clone().unwrap_or_default(),
                price,
                size: tick.volume.unwrap_or(0) as f64,
                aggressor_side: "NO_AGGRESSOR".to_string(),
                trade_id: format!("replay:{ts_event}:{sequence}"),
                price_precision: 2,
                size_precision: 0,
                ts_event,
                ts_init: ts_event,
            },
        })
    }
}

// Execution events.

/// Python wrapper for OrderSubmitted event.
#[cfg(feature = "python")]
#[pyclass(name = "OrderSubmitted", skip_from_py_object)]
#[pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.rithmic")]
#[derive(Clone)]
pub(crate) struct PyOrderSubmitted {
    inner: OrderSubmitted,
}

#[cfg(feature = "python")]
fn side_text(side: Option<rithmic_rs::OrderSide>) -> Option<String> {
    side.map(|value| value.to_string())
}

#[cfg(feature = "python")]
fn order_type_text(order_type: Option<rithmic_rs::OrderType>) -> Option<String> {
    order_type.map(|value| value.to_string())
}

#[cfg(feature = "python")]
fn time_in_force_text(time_in_force: Option<rithmic_rs::TimeInForce>) -> Option<String> {
    time_in_force.map(|value| value.to_string())
}

#[cfg(feature = "python")]
#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl PyOrderSubmitted {
    /// Whether the source notification was a Rithmic snapshot/replay payload.
    #[getter(is_snapshot)]
    fn py_is_snapshot(&self) -> bool {
        self.inner.context.is_snapshot
    }

    /// Client order ID.
    #[getter(client_order_id)]
    fn py_client_order_id(&self) -> &str {
        &self.inner.client_order_id
    }

    /// Venue order ID (may be None until accepted).
    #[getter(venue_order_id)]
    fn py_venue_order_id(&self) -> Option<&str> {
        self.inner.venue_order_id.as_deref()
    }

    /// Account ID.
    #[getter(account_id)]
    fn py_account_id(&self) -> &str {
        &self.inner.account_id
    }

    /// Instrument symbol when available.
    #[getter(symbol)]
    fn py_symbol(&self) -> Option<&str> {
        self.inner.context.symbol.as_deref()
    }

    /// Exchange code when available.
    #[getter(exchange)]
    fn py_exchange(&self) -> Option<&str> {
        self.inner.context.exchange.as_deref()
    }

    /// Order side when available.
    #[getter(side)]
    fn py_side(&self) -> Option<String> {
        side_text(self.inner.context.side)
    }

    /// Order type when available.
    #[getter(order_type)]
    fn py_order_type(&self) -> Option<String> {
        order_type_text(self.inner.context.order_type)
    }

    /// Time in force when available.
    #[getter(time_in_force)]
    fn py_time_in_force(&self) -> Option<String> {
        time_in_force_text(self.inner.context.time_in_force)
    }

    /// Original quantity when available.
    #[getter(quantity)]
    fn py_quantity(&self) -> Option<f64> {
        self.inner.context.quantity
    }

    /// Cumulative filled quantity when available.
    #[getter(filled_qty)]
    fn py_filled_qty(&self) -> Option<f64> {
        self.inner.context.filled_qty
    }

    /// Remaining quantity when available.
    #[getter(leaves_qty)]
    fn py_leaves_qty(&self) -> Option<f64> {
        self.inner.context.leaves_qty
    }

    /// Order price when available.
    #[getter(price)]
    fn py_price(&self) -> Option<f64> {
        self.inner.context.price
    }

    /// Stop or trigger price when available.
    #[getter(trigger_price)]
    fn py_trigger_price(&self) -> Option<f64> {
        self.inner.context.trigger_price
    }

    /// Average fill price when available.
    #[getter(avg_price)]
    fn py_avg_price(&self) -> Option<f64> {
        self.inner.context.avg_price
    }

    /// Parent venue basket ID for bracket child notifications.
    #[getter(original_basket_id)]
    fn py_original_basket_id(&self) -> Option<&str> {
        self.inner.context.original_basket_id.as_deref()
    }

    /// Linked venue basket IDs for contingent orders.
    #[getter(linked_basket_ids)]
    fn py_linked_basket_ids(&self) -> Vec<String> {
        self.inner.context.linked_basket_ids.clone()
    }

    /// Venue bracket type when available.
    #[getter(bracket_type)]
    fn py_bracket_type(&self) -> Option<&str> {
        self.inner.context.bracket_type.as_deref()
    }

    /// Event timestamp in nanoseconds.
    #[getter(ts_event)]
    fn py_ts_event(&self) -> u64 {
        self.inner.ts_event
    }

    #[pyo3(name = "__repr__")]
    fn py_repr(&self) -> String {
        format!(
            "OrderSubmitted(client_order_id={}, venue_order_id={:?})",
            self.inner.client_order_id, self.inner.venue_order_id,
        )
    }
}

impl From<OrderSubmitted> for PyOrderSubmitted {
    fn from(event: OrderSubmitted) -> Self {
        Self { inner: event }
    }
}

/// Python wrapper for OrderAccepted event.
#[cfg(feature = "python")]
#[pyclass(name = "OrderAccepted", skip_from_py_object)]
#[pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.rithmic")]
#[derive(Clone)]
pub(crate) struct PyOrderAccepted {
    inner: OrderAccepted,
}

#[cfg(feature = "python")]
#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl PyOrderAccepted {
    /// Whether the source notification was a Rithmic snapshot/replay payload.
    #[getter(is_snapshot)]
    fn py_is_snapshot(&self) -> bool {
        self.inner.context.is_snapshot
    }

    /// Client order ID.
    #[getter(client_order_id)]
    fn py_client_order_id(&self) -> &str {
        &self.inner.client_order_id
    }

    /// Venue order ID.
    #[getter(venue_order_id)]
    fn py_venue_order_id(&self) -> &str {
        &self.inner.venue_order_id
    }

    /// Account ID.
    #[getter(account_id)]
    fn py_account_id(&self) -> &str {
        &self.inner.account_id
    }

    /// Instrument symbol when available.
    #[getter(symbol)]
    fn py_symbol(&self) -> Option<&str> {
        self.inner.context.symbol.as_deref()
    }

    /// Exchange code when available.
    #[getter(exchange)]
    fn py_exchange(&self) -> Option<&str> {
        self.inner.context.exchange.as_deref()
    }

    /// Order side when available.
    #[getter(side)]
    fn py_side(&self) -> Option<String> {
        side_text(self.inner.context.side)
    }

    /// Order type when available.
    #[getter(order_type)]
    fn py_order_type(&self) -> Option<String> {
        order_type_text(self.inner.context.order_type)
    }

    /// Time in force when available.
    #[getter(time_in_force)]
    fn py_time_in_force(&self) -> Option<String> {
        time_in_force_text(self.inner.context.time_in_force)
    }

    /// Original quantity when available.
    #[getter(quantity)]
    fn py_quantity(&self) -> Option<f64> {
        self.inner.context.quantity
    }

    /// Cumulative filled quantity when available.
    #[getter(filled_qty)]
    fn py_filled_qty(&self) -> Option<f64> {
        self.inner.context.filled_qty
    }

    /// Remaining quantity when available.
    #[getter(leaves_qty)]
    fn py_leaves_qty(&self) -> Option<f64> {
        self.inner.context.leaves_qty
    }

    /// Order price when available.
    #[getter(price)]
    fn py_price(&self) -> Option<f64> {
        self.inner.context.price
    }

    /// Stop or trigger price when available.
    #[getter(trigger_price)]
    fn py_trigger_price(&self) -> Option<f64> {
        self.inner.context.trigger_price
    }

    /// Average fill price when available.
    #[getter(avg_price)]
    fn py_avg_price(&self) -> Option<f64> {
        self.inner.context.avg_price
    }

    /// Parent venue basket ID for bracket child notifications.
    #[getter(original_basket_id)]
    fn py_original_basket_id(&self) -> Option<&str> {
        self.inner.context.original_basket_id.as_deref()
    }

    /// Linked venue basket IDs for contingent orders.
    #[getter(linked_basket_ids)]
    fn py_linked_basket_ids(&self) -> Vec<String> {
        self.inner.context.linked_basket_ids.clone()
    }

    /// Venue bracket type when available.
    #[getter(bracket_type)]
    fn py_bracket_type(&self) -> Option<&str> {
        self.inner.context.bracket_type.as_deref()
    }

    /// Event timestamp in nanoseconds.
    #[getter(ts_event)]
    fn py_ts_event(&self) -> u64 {
        self.inner.ts_event
    }

    #[pyo3(name = "__repr__")]
    fn py_repr(&self) -> String {
        format!(
            "OrderAccepted(client_order_id={}, venue_order_id={})",
            self.inner.client_order_id, self.inner.venue_order_id,
        )
    }
}

impl From<OrderAccepted> for PyOrderAccepted {
    fn from(event: OrderAccepted) -> Self {
        Self { inner: event }
    }
}

/// Python wrapper for OrderRejected event.
#[cfg(feature = "python")]
#[pyclass(name = "OrderRejected", skip_from_py_object)]
#[pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.rithmic")]
#[derive(Clone)]
pub(crate) struct PyOrderRejected {
    inner: OrderRejected,
}

#[cfg(feature = "python")]
#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl PyOrderRejected {
    /// Whether the source notification was a Rithmic snapshot/replay payload.
    #[getter(is_snapshot)]
    fn py_is_snapshot(&self) -> bool {
        self.inner.context.is_snapshot
    }

    /// Client order ID.
    #[getter(client_order_id)]
    fn py_client_order_id(&self) -> &str {
        &self.inner.client_order_id
    }

    /// Rejection reason.
    #[getter(reason)]
    fn py_reason(&self) -> &str {
        &self.inner.reason
    }

    /// Instrument symbol when available.
    #[getter(symbol)]
    fn py_symbol(&self) -> Option<&str> {
        self.inner.context.symbol.as_deref()
    }

    /// Exchange code when available.
    #[getter(exchange)]
    fn py_exchange(&self) -> Option<&str> {
        self.inner.context.exchange.as_deref()
    }

    /// Parent venue basket ID for bracket child notifications.
    #[getter(original_basket_id)]
    fn py_original_basket_id(&self) -> Option<&str> {
        self.inner.context.original_basket_id.as_deref()
    }

    /// Linked venue basket IDs for contingent orders.
    #[getter(linked_basket_ids)]
    fn py_linked_basket_ids(&self) -> Vec<String> {
        self.inner.context.linked_basket_ids.clone()
    }

    /// Venue bracket type when available.
    #[getter(bracket_type)]
    fn py_bracket_type(&self) -> Option<&str> {
        self.inner.context.bracket_type.as_deref()
    }

    /// Event timestamp in nanoseconds.
    #[getter(ts_event)]
    fn py_ts_event(&self) -> u64 {
        self.inner.ts_event
    }

    #[pyo3(name = "__repr__")]
    fn py_repr(&self) -> String {
        format!(
            "OrderRejected(client_order_id={}, reason={})",
            self.inner.client_order_id, self.inner.reason,
        )
    }
}

impl From<OrderRejected> for PyOrderRejected {
    fn from(event: OrderRejected) -> Self {
        Self { inner: event }
    }
}

/// Python wrapper for OrderFilled event.
#[cfg(feature = "python")]
#[pyclass(name = "OrderFilled", skip_from_py_object)]
#[pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.rithmic")]
#[derive(Clone)]
pub(crate) struct PyOrderFilled {
    inner: OrderFilled,
}

#[cfg(feature = "python")]
#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl PyOrderFilled {
    /// Whether the source notification was a Rithmic snapshot/replay payload.
    #[getter(is_snapshot)]
    fn py_is_snapshot(&self) -> bool {
        self.inner.context.is_snapshot
    }

    /// Client order ID.
    #[getter(client_order_id)]
    fn py_client_order_id(&self) -> &str {
        &self.inner.client_order_id
    }

    /// Venue order ID.
    #[getter(venue_order_id)]
    fn py_venue_order_id(&self) -> &str {
        &self.inner.venue_order_id
    }

    /// Fill price.
    #[getter(fill_price)]
    fn py_fill_price(&self) -> f64 {
        self.inner.fill_price
    }

    /// Fill quantity.
    #[getter(fill_qty)]
    fn py_fill_qty(&self) -> f64 {
        self.inner.fill_qty
    }

    /// Remaining quantity.
    #[getter(leaves_qty)]
    fn py_leaves_qty(&self) -> f64 {
        self.inner.leaves_qty
    }

    /// Commission.
    #[getter(commission)]
    fn py_commission(&self) -> f64 {
        self.inner.commission
    }

    /// Instrument symbol when available.
    #[getter(symbol)]
    fn py_symbol(&self) -> Option<&str> {
        self.inner.context.symbol.as_deref()
    }

    /// Exchange code when available.
    #[getter(exchange)]
    fn py_exchange(&self) -> Option<&str> {
        self.inner.context.exchange.as_deref()
    }

    /// Order side when available.
    #[getter(side)]
    fn py_side(&self) -> Option<String> {
        side_text(self.inner.context.side)
    }

    /// Venue trade identifier when available.
    #[getter(trade_id)]
    fn py_trade_id(&self) -> Option<&str> {
        self.inner.trade_id.as_deref()
    }

    /// Fill currency when available.
    #[getter(currency)]
    fn py_currency(&self) -> Option<&str> {
        self.inner.currency.as_deref()
    }

    /// Parent venue basket ID for bracket child notifications.
    #[getter(original_basket_id)]
    fn py_original_basket_id(&self) -> Option<&str> {
        self.inner.context.original_basket_id.as_deref()
    }

    /// Linked venue basket IDs for contingent orders.
    #[getter(linked_basket_ids)]
    fn py_linked_basket_ids(&self) -> Vec<String> {
        self.inner.context.linked_basket_ids.clone()
    }

    /// Venue bracket type when available.
    #[getter(bracket_type)]
    fn py_bracket_type(&self) -> Option<&str> {
        self.inner.context.bracket_type.as_deref()
    }

    /// Event timestamp in nanoseconds.
    #[getter(ts_event)]
    fn py_ts_event(&self) -> u64 {
        self.inner.ts_event
    }

    #[pyo3(name = "__repr__")]
    fn py_repr(&self) -> String {
        format!(
            "OrderFilled(client_order_id={}, fill_price={:.6}, fill_qty={}, leaves_qty={})",
            self.inner.client_order_id,
            self.inner.fill_price,
            self.inner.fill_qty,
            self.inner.leaves_qty,
        )
    }
}

impl From<OrderFilled> for PyOrderFilled {
    fn from(event: OrderFilled) -> Self {
        Self { inner: event }
    }
}

/// Python wrapper for OrderCancelled event.
#[cfg(feature = "python")]
#[pyclass(name = "OrderCancelled", skip_from_py_object)]
#[pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.rithmic")]
#[derive(Clone)]
pub(crate) struct PyOrderCancelled {
    inner: OrderCancelled,
}

#[cfg(feature = "python")]
#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl PyOrderCancelled {
    /// Whether the source notification was a Rithmic snapshot/replay payload.
    #[getter(is_snapshot)]
    fn py_is_snapshot(&self) -> bool {
        self.inner.context.is_snapshot
    }

    /// Client order ID.
    #[getter(client_order_id)]
    fn py_client_order_id(&self) -> &str {
        &self.inner.client_order_id
    }

    /// Venue order ID.
    #[getter(venue_order_id)]
    fn py_venue_order_id(&self) -> &str {
        &self.inner.venue_order_id
    }

    /// Instrument symbol when available.
    #[getter(symbol)]
    fn py_symbol(&self) -> Option<&str> {
        self.inner.context.symbol.as_deref()
    }

    /// Exchange code when available.
    #[getter(exchange)]
    fn py_exchange(&self) -> Option<&str> {
        self.inner.context.exchange.as_deref()
    }

    /// Order side when available.
    #[getter(side)]
    fn py_side(&self) -> Option<String> {
        side_text(self.inner.context.side)
    }

    /// Order type when available.
    #[getter(order_type)]
    fn py_order_type(&self) -> Option<String> {
        order_type_text(self.inner.context.order_type)
    }

    /// Time in force when available.
    #[getter(time_in_force)]
    fn py_time_in_force(&self) -> Option<String> {
        time_in_force_text(self.inner.context.time_in_force)
    }

    /// Original quantity when available.
    #[getter(quantity)]
    fn py_quantity(&self) -> Option<f64> {
        self.inner.context.quantity
    }

    /// Cumulative filled quantity when available.
    #[getter(filled_qty)]
    fn py_filled_qty(&self) -> Option<f64> {
        self.inner.context.filled_qty
    }

    /// Remaining quantity when available.
    #[getter(leaves_qty)]
    fn py_leaves_qty(&self) -> Option<f64> {
        self.inner.context.leaves_qty
    }

    /// Order price when available.
    #[getter(price)]
    fn py_price(&self) -> Option<f64> {
        self.inner.context.price
    }

    /// Stop or trigger price when available.
    #[getter(trigger_price)]
    fn py_trigger_price(&self) -> Option<f64> {
        self.inner.context.trigger_price
    }

    /// Average fill price when available.
    #[getter(avg_price)]
    fn py_avg_price(&self) -> Option<f64> {
        self.inner.context.avg_price
    }

    /// Parent venue basket ID for bracket child notifications.
    #[getter(original_basket_id)]
    fn py_original_basket_id(&self) -> Option<&str> {
        self.inner.context.original_basket_id.as_deref()
    }

    /// Linked venue basket IDs for contingent orders.
    #[getter(linked_basket_ids)]
    fn py_linked_basket_ids(&self) -> Vec<String> {
        self.inner.context.linked_basket_ids.clone()
    }

    /// Venue bracket type when available.
    #[getter(bracket_type)]
    fn py_bracket_type(&self) -> Option<&str> {
        self.inner.context.bracket_type.as_deref()
    }

    /// Event timestamp in nanoseconds.
    #[getter(ts_event)]
    fn py_ts_event(&self) -> u64 {
        self.inner.ts_event
    }

    #[pyo3(name = "__repr__")]
    fn py_repr(&self) -> String {
        format!(
            "OrderCancelled(client_order_id={}, venue_order_id={})",
            self.inner.client_order_id, self.inner.venue_order_id,
        )
    }
}

impl From<OrderCancelled> for PyOrderCancelled {
    fn from(event: OrderCancelled) -> Self {
        Self { inner: event }
    }
}

/// Python wrapper for OrderModified event.
#[cfg(feature = "python")]
#[pyclass(name = "OrderModified", skip_from_py_object)]
#[pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.rithmic")]
#[derive(Clone)]
pub(crate) struct PyOrderModified {
    inner: OrderModified,
}

#[cfg(feature = "python")]
#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl PyOrderModified {
    /// Whether the source notification was a Rithmic snapshot/replay payload.
    #[getter(is_snapshot)]
    fn py_is_snapshot(&self) -> bool {
        self.inner.context.is_snapshot
    }

    /// Client order ID.
    #[getter(client_order_id)]
    fn py_client_order_id(&self) -> &str {
        &self.inner.client_order_id
    }

    /// Venue order ID.
    #[getter(venue_order_id)]
    fn py_venue_order_id(&self) -> &str {
        &self.inner.venue_order_id
    }

    /// New price (if modified).
    #[getter(new_price)]
    fn py_new_price(&self) -> Option<f64> {
        self.inner.new_price
    }

    /// New quantity (if modified).
    #[getter(new_qty)]
    fn py_new_qty(&self) -> Option<f64> {
        self.inner.new_qty
    }

    /// Instrument symbol when available.
    #[getter(symbol)]
    fn py_symbol(&self) -> Option<&str> {
        self.inner.context.symbol.as_deref()
    }

    /// Exchange code when available.
    #[getter(exchange)]
    fn py_exchange(&self) -> Option<&str> {
        self.inner.context.exchange.as_deref()
    }

    /// Parent venue basket ID for bracket child notifications.
    #[getter(original_basket_id)]
    fn py_original_basket_id(&self) -> Option<&str> {
        self.inner.context.original_basket_id.as_deref()
    }

    /// Linked venue basket IDs for contingent orders.
    #[getter(linked_basket_ids)]
    fn py_linked_basket_ids(&self) -> Vec<String> {
        self.inner.context.linked_basket_ids.clone()
    }

    /// Venue bracket type when available.
    #[getter(bracket_type)]
    fn py_bracket_type(&self) -> Option<&str> {
        self.inner.context.bracket_type.as_deref()
    }

    /// Event timestamp in nanoseconds.
    #[getter(ts_event)]
    fn py_ts_event(&self) -> u64 {
        self.inner.ts_event
    }

    #[pyo3(name = "__repr__")]
    fn py_repr(&self) -> String {
        format!(
            "OrderModified(client_order_id={}, new_price={:?}, new_qty={:?})",
            self.inner.client_order_id, self.inner.new_price, self.inner.new_qty,
        )
    }
}

impl From<OrderModified> for PyOrderModified {
    fn from(event: OrderModified) -> Self {
        Self { inner: event }
    }
}

// Unified event wrapper.

/// Python wrapper for MarketDataEvent (union type).
#[cfg(feature = "python")]
#[pyclass(name = "MarketDataEvent", skip_from_py_object)]
#[pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.rithmic")]
#[derive(Clone)]
pub(crate) struct PyMarketDataEvent {
    inner: MarketDataEvent,
}

#[cfg(feature = "python")]
#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl PyMarketDataEvent {
    /// Returns true if this is a quote event.
    #[pyo3(name = "is_quote")]
    fn py_is_quote(&self) -> bool {
        matches!(self.inner, MarketDataEvent::Quote(_))
    }

    /// Returns true if this is a trade event.
    #[pyo3(name = "is_trade")]
    fn py_is_trade(&self) -> bool {
        matches!(self.inner, MarketDataEvent::Trade(_))
    }

    /// Returns true if this is a live time-bar event.
    #[pyo3(name = "is_bar")]
    fn py_is_bar(&self) -> bool {
        matches!(self.inner, MarketDataEvent::Bar(_))
    }

    /// Returns true if this is an order-book delta event.
    #[pyo3(name = "is_book_delta")]
    fn py_is_book_delta(&self) -> bool {
        matches!(self.inner, MarketDataEvent::BookDelta(_))
    }

    /// Returns true if this is an order-book depth10 event.
    #[pyo3(name = "is_depth10")]
    fn py_is_depth10(&self) -> bool {
        matches!(self.inner, MarketDataEvent::Depth10(_))
    }

    /// Returns true if this is an instrument status event.
    #[pyo3(name = "is_instrument_status")]
    fn py_is_instrument_status(&self) -> bool {
        matches!(self.inner, MarketDataEvent::InstrumentStatus(_))
    }

    /// Returns true if this is a custom market-data event.
    #[pyo3(name = "is_custom")]
    fn py_is_custom(&self) -> bool {
        matches!(self.inner, MarketDataEvent::Custom(_))
    }

    /// Returns true if this is a connection state event.
    #[pyo3(name = "is_connection_state")]
    fn py_is_connection_state(&self) -> bool {
        matches!(self.inner, MarketDataEvent::ConnectionState(_))
    }

    /// Returns true if this is an error event.
    #[pyo3(name = "is_error")]
    fn py_is_error(&self) -> bool {
        matches!(self.inner, MarketDataEvent::Error(_))
    }

    /// Get the quote tick if this is a quote event.
    #[pyo3(name = "as_quote")]
    fn py_as_quote(&self) -> Option<PyQuoteTick> {
        match &self.inner {
            MarketDataEvent::Quote(q) => Some(PyQuoteTick::from(q.clone())),
            _ => None,
        }
    }

    /// Get the trade tick if this is a trade event.
    #[pyo3(name = "as_trade")]
    fn py_as_trade(&self) -> Option<PyTradeTick> {
        match &self.inner {
            MarketDataEvent::Trade(t) => Some(PyTradeTick::from(t.clone())),
            _ => None,
        }
    }

    /// Get the time bar if this is a bar event.
    #[pyo3(name = "as_bar")]
    fn py_as_bar(&self) -> Option<PyTimeBar> {
        match &self.inner {
            MarketDataEvent::Bar(bar) => Some(PyTimeBar::from(bar.clone())),
            _ => None,
        }
    }

    /// Get the order-book delta if this is a book-delta event.
    #[pyo3(name = "as_book_delta")]
    fn py_as_book_delta(&self) -> Option<PyBookDelta> {
        match &self.inner {
            MarketDataEvent::BookDelta(delta) => Some(PyBookDelta::from(delta.clone())),
            _ => None,
        }
    }

    /// Get the order-book depth10 update if this is a depth10 event.
    #[pyo3(name = "as_depth10")]
    fn py_as_depth10(&self) -> Option<OrderBookDepth10> {
        match &self.inner {
            MarketDataEvent::Depth10(depth) => Some(**depth),
            _ => None,
        }
    }

    /// Get the instrument status if this is an instrument-status event.
    #[pyo3(name = "as_instrument_status")]
    fn py_as_instrument_status(&self) -> Option<InstrumentStatus> {
        match &self.inner {
            MarketDataEvent::InstrumentStatus(status) => Some(*status),
            _ => None,
        }
    }

    /// Get the custom data type name if this is a custom market-data event.
    #[pyo3(name = "as_custom_type")]
    fn py_as_custom_type(&self) -> Option<String> {
        match &self.inner {
            MarketDataEvent::Custom(custom) => Some(custom_type_name(custom).to_string()),
            _ => None,
        }
    }

    /// Get the custom market-data payload as JSON if this is a custom event.
    #[pyo3(name = "as_custom_json")]
    fn py_as_custom_json(&self) -> Option<String> {
        match &self.inner {
            MarketDataEvent::Custom(custom) => custom_payload_json(custom),
            _ => None,
        }
    }

    /// Get the connection state as a string if this is a connection state event.
    #[pyo3(name = "as_connection_state")]
    fn py_as_connection_state(&self) -> Option<String> {
        match &self.inner {
            MarketDataEvent::ConnectionState(s) => Some(format!("{s:?}")),
            _ => None,
        }
    }

    /// Get the error message if this is an error event.
    #[pyo3(name = "as_error")]
    fn py_as_error(&self) -> Option<String> {
        match &self.inner {
            MarketDataEvent::Error(e) => Some(e.clone()),
            _ => None,
        }
    }

    #[pyo3(name = "__repr__")]
    fn py_repr(&self) -> String {
        match &self.inner {
            MarketDataEvent::Quote(q) => {
                format!("MarketDataEvent::Quote({q:?})")
            }
            MarketDataEvent::Trade(t) => {
                format!("MarketDataEvent::Trade({t:?})")
            }
            MarketDataEvent::Bar(b) => {
                format!("MarketDataEvent::Bar({b:?})")
            }
            MarketDataEvent::ConnectionState(s) => {
                format!("MarketDataEvent::ConnectionState({s:?})")
            }
            MarketDataEvent::BookDelta(d) => {
                format!("MarketDataEvent::BookDelta({d:?})")
            }
            MarketDataEvent::Depth10(d) => {
                format!("MarketDataEvent::Depth10({d:?})")
            }
            MarketDataEvent::InstrumentStatus(status) => {
                format!("MarketDataEvent::InstrumentStatus({status:?})")
            }
            MarketDataEvent::Custom(custom) => {
                format!("MarketDataEvent::Custom({custom:?})")
            }
            MarketDataEvent::Reconnected => "MarketDataEvent::Reconnected".to_string(),
            MarketDataEvent::Authenticated => "MarketDataEvent::Authenticated".to_string(),
            MarketDataEvent::Error(e) => format!("MarketDataEvent::Error({e})"),
        }
    }
}

impl From<MarketDataEvent> for PyMarketDataEvent {
    fn from(event: MarketDataEvent) -> Self {
        Self { inner: event }
    }
}

fn custom_type_name(custom: &RithmicCustomData) -> &'static str {
    match custom {
        RithmicCustomData::TradeStatistics(_) => TRADE_STATISTICS_TYPE_NAME,
        RithmicCustomData::QuoteStatistics(_) => QUOTE_STATISTICS_TYPE_NAME,
        RithmicCustomData::IndicatorPrices(_) => INDICATOR_PRICES_TYPE_NAME,
        RithmicCustomData::OpenInterest(_) => OPEN_INTEREST_TYPE_NAME,
        RithmicCustomData::EndOfDayPrices(_) => END_OF_DAY_PRICES_TYPE_NAME,
        RithmicCustomData::OrderPriceLimits(_) => ORDER_PRICE_LIMITS_TYPE_NAME,
        RithmicCustomData::SymbolMarginRate(_) => SYMBOL_MARGIN_RATE_TYPE_NAME,
    }
}

fn custom_payload_json(custom: &RithmicCustomData) -> Option<String> {
    let value = match custom {
        RithmicCustomData::TradeStatistics(value) => serde_json::json!({
            "instrument_id": value.instrument_id.to_string(),
            "is_snapshot": value.is_snapshot,
            "open_price": value.open_price,
            "high_price": value.high_price,
            "low_price": value.low_price,
            "ts_event": value.ts_event.as_u64(),
            "ts_init": value.ts_init.as_u64(),
        }),
        RithmicCustomData::QuoteStatistics(value) => serde_json::json!({
            "instrument_id": value.instrument_id.to_string(),
            "is_snapshot": value.is_snapshot,
            "highest_bid_price": value.highest_bid_price,
            "lowest_ask_price": value.lowest_ask_price,
            "ts_event": value.ts_event.as_u64(),
            "ts_init": value.ts_init.as_u64(),
        }),
        RithmicCustomData::IndicatorPrices(value) => serde_json::json!({
            "instrument_id": value.instrument_id.to_string(),
            "is_snapshot": value.is_snapshot,
            "opening_indicator": value.opening_indicator,
            "closing_indicator": value.closing_indicator,
            "ts_event": value.ts_event.as_u64(),
            "ts_init": value.ts_init.as_u64(),
        }),
        RithmicCustomData::OpenInterest(value) => serde_json::json!({
            "instrument_id": value.instrument_id.to_string(),
            "is_snapshot": value.is_snapshot,
            "should_clear": value.should_clear,
            "open_interest": value.open_interest,
            "ts_event": value.ts_event.as_u64(),
            "ts_init": value.ts_init.as_u64(),
        }),
        RithmicCustomData::EndOfDayPrices(value) => serde_json::json!({
            "instrument_id": value.instrument_id.to_string(),
            "is_snapshot": value.is_snapshot,
            "close_price": value.close_price,
            "close_date": value.close_date,
            "adjusted_close_price": value.adjusted_close_price,
            "settlement_price": value.settlement_price,
            "settlement_date": value.settlement_date,
            "settlement_price_type": value.settlement_price_type,
            "projected_settlement_price": value.projected_settlement_price,
            "ts_event": value.ts_event.as_u64(),
            "ts_init": value.ts_init.as_u64(),
        }),
        RithmicCustomData::OrderPriceLimits(value) => serde_json::json!({
            "instrument_id": value.instrument_id.to_string(),
            "is_snapshot": value.is_snapshot,
            "high_price_limit": value.high_price_limit,
            "low_price_limit": value.low_price_limit,
            "ts_event": value.ts_event.as_u64(),
            "ts_init": value.ts_init.as_u64(),
        }),
        RithmicCustomData::SymbolMarginRate(value) => serde_json::json!({
            "instrument_id": value.instrument_id.to_string(),
            "is_snapshot": value.is_snapshot,
            "margin_rate": value.margin_rate,
            "ts_event": value.ts_event.as_u64(),
            "ts_init": value.ts_init.as_u64(),
        }),
    };

    serde_json::to_string(&value).ok()
}

/// Python wrapper for ExecutionEvent (union type).
#[cfg(feature = "python")]
#[pyclass(name = "ExecutionEvent", skip_from_py_object)]
#[pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.rithmic")]
#[derive(Clone)]
pub(crate) struct PyExecutionEvent {
    inner: ExecutionEvent,
}

#[cfg(feature = "python")]
#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl PyExecutionEvent {
    /// Returns true if this is a submitted event.
    #[pyo3(name = "is_submitted")]
    fn py_is_submitted(&self) -> bool {
        matches!(self.inner, ExecutionEvent::Submitted(_))
    }

    /// Returns true if this is an accepted event.
    #[pyo3(name = "is_accepted")]
    fn py_is_accepted(&self) -> bool {
        matches!(self.inner, ExecutionEvent::Accepted(_))
    }

    /// Returns true if this is a rejected event.
    #[pyo3(name = "is_rejected")]
    fn py_is_rejected(&self) -> bool {
        matches!(self.inner, ExecutionEvent::Rejected(_))
    }

    /// Returns true if this is a filled event.
    #[pyo3(name = "is_filled")]
    fn py_is_filled(&self) -> bool {
        matches!(self.inner, ExecutionEvent::Filled(_))
    }

    /// Returns true if this is a cancelled event.
    #[pyo3(name = "is_cancelled")]
    fn py_is_cancelled(&self) -> bool {
        matches!(self.inner, ExecutionEvent::Cancelled(_))
    }

    /// Returns true if this is a modified event.
    #[pyo3(name = "is_modified")]
    fn py_is_modified(&self) -> bool {
        matches!(self.inner, ExecutionEvent::Modified(_))
    }

    /// Returns true if this is a connection state event.
    #[pyo3(name = "is_connection_state")]
    fn py_is_connection_state(&self) -> bool {
        matches!(self.inner, ExecutionEvent::ConnectionState(_))
    }

    /// Returns true if this is an error event.
    #[pyo3(name = "is_error")]
    fn py_is_error(&self) -> bool {
        matches!(self.inner, ExecutionEvent::Error(_))
    }

    /// Get as submitted event.
    #[pyo3(name = "as_submitted")]
    fn py_as_submitted(&self) -> Option<PyOrderSubmitted> {
        match &self.inner {
            ExecutionEvent::Submitted(e) => Some(PyOrderSubmitted::from(e.clone())),
            _ => None,
        }
    }

    /// Get as accepted event.
    #[pyo3(name = "as_accepted")]
    fn py_as_accepted(&self) -> Option<PyOrderAccepted> {
        match &self.inner {
            ExecutionEvent::Accepted(e) => Some(PyOrderAccepted::from(e.clone())),
            _ => None,
        }
    }

    /// Get as rejected event.
    #[pyo3(name = "as_rejected")]
    fn py_as_rejected(&self) -> Option<PyOrderRejected> {
        match &self.inner {
            ExecutionEvent::Rejected(e) => Some(PyOrderRejected::from(e.clone())),
            _ => None,
        }
    }

    /// Get as filled event.
    #[pyo3(name = "as_filled")]
    fn py_as_filled(&self) -> Option<PyOrderFilled> {
        match &self.inner {
            ExecutionEvent::Filled(e) => Some(PyOrderFilled::from(e.clone())),
            _ => None,
        }
    }

    /// Get as cancelled event.
    #[pyo3(name = "as_cancelled")]
    fn py_as_cancelled(&self) -> Option<PyOrderCancelled> {
        match &self.inner {
            ExecutionEvent::Cancelled(e) => Some(PyOrderCancelled::from(e.clone())),
            _ => None,
        }
    }

    /// Get as modified event.
    #[pyo3(name = "as_modified")]
    fn py_as_modified(&self) -> Option<PyOrderModified> {
        match &self.inner {
            ExecutionEvent::Modified(e) => Some(PyOrderModified::from(e.clone())),
            _ => None,
        }
    }

    /// Get the connection state as a string if this is a connection state event.
    #[pyo3(name = "as_connection_state")]
    fn py_as_connection_state(&self) -> Option<String> {
        match &self.inner {
            ExecutionEvent::ConnectionState(s) => Some(format!("{s:?}")),
            _ => None,
        }
    }

    /// Get the error message if this is an error event.
    #[pyo3(name = "as_error")]
    fn py_as_error(&self) -> Option<String> {
        match &self.inner {
            ExecutionEvent::Error(e) => Some(e.clone()),
            _ => None,
        }
    }

    #[pyo3(name = "__repr__")]
    fn py_repr(&self) -> String {
        match &self.inner {
            ExecutionEvent::Submitted(e) => {
                format!("ExecutionEvent::Submitted({})", e.client_order_id)
            }
            ExecutionEvent::Accepted(e) => {
                format!("ExecutionEvent::Accepted({})", e.client_order_id)
            }
            ExecutionEvent::Rejected(e) => {
                format!("ExecutionEvent::Rejected({})", e.client_order_id)
            }
            ExecutionEvent::Filled(e) => format!("ExecutionEvent::Filled({})", e.client_order_id),
            ExecutionEvent::Cancelled(e) => {
                format!("ExecutionEvent::Cancelled({})", e.client_order_id)
            }
            ExecutionEvent::Modified(e) => {
                format!("ExecutionEvent::Modified({})", e.client_order_id)
            }
            ExecutionEvent::ConnectionState(s) => {
                format!("ExecutionEvent::ConnectionState({s:?})")
            }
            ExecutionEvent::Reconnected => "ExecutionEvent::Reconnected".to_string(),
            ExecutionEvent::Authenticated => "ExecutionEvent::Authenticated".to_string(),
            ExecutionEvent::Error(e) => format!("ExecutionEvent::Error({e})"),
        }
    }
}

impl From<ExecutionEvent> for PyExecutionEvent {
    fn from(event: ExecutionEvent) -> Self {
        Self { inner: event }
    }
}

// PnL / position events.

/// Python wrapper for AccountEvent.
#[cfg(feature = "python")]
#[pyclass(name = "AccountEvent", skip_from_py_object)]
#[pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.rithmic")]
#[derive(Clone)]
pub(crate) struct PyAccountEvent {
    inner: AccountEvent,
}

#[cfg(feature = "python")]
#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl PyAccountEvent {
    #[getter(is_snapshot)]
    fn py_is_snapshot(&self) -> bool {
        match &self.inner {
            AccountEvent::BalanceUpdate(balance) => balance.is_snapshot,
            AccountEvent::MarginWarning { .. } => false,
            AccountEvent::Error(_) => false,
        }
    }

    #[getter(account_id)]
    fn py_account_id(&self) -> &str {
        match &self.inner {
            AccountEvent::BalanceUpdate(b) => &b.account_id,
            AccountEvent::MarginWarning { account_id, .. } => account_id,
            AccountEvent::Error(_) => "",
        }
    }

    #[getter(currency)]
    fn py_currency(&self) -> &str {
        match &self.inner {
            AccountEvent::BalanceUpdate(b) => &b.currency,
            AccountEvent::MarginWarning { .. } => "",
            AccountEvent::Error(_) => "",
        }
    }

    #[getter(total)]
    fn py_total(&self) -> f64 {
        match &self.inner {
            AccountEvent::BalanceUpdate(b) => b.total,
            AccountEvent::MarginWarning { .. } => 0.0,
            AccountEvent::Error(_) => 0.0,
        }
    }

    #[getter(available)]
    fn py_available(&self) -> f64 {
        match &self.inner {
            AccountEvent::BalanceUpdate(b) => b.available,
            AccountEvent::MarginWarning { .. } => 0.0,
            AccountEvent::Error(_) => 0.0,
        }
    }

    #[getter(locked)]
    fn py_locked(&self) -> f64 {
        match &self.inner {
            AccountEvent::BalanceUpdate(b) => b.locked,
            AccountEvent::MarginWarning { .. } => 0.0,
            AccountEvent::Error(_) => 0.0,
        }
    }

    #[getter(unrealized_pnl)]
    fn py_unrealized_pnl(&self) -> f64 {
        match &self.inner {
            AccountEvent::BalanceUpdate(b) => b.unrealized_pnl,
            AccountEvent::MarginWarning { .. } => 0.0,
            AccountEvent::Error(_) => 0.0,
        }
    }

    #[getter(realized_pnl)]
    fn py_realized_pnl(&self) -> f64 {
        match &self.inner {
            AccountEvent::BalanceUpdate(b) => b.realized_pnl,
            AccountEvent::MarginWarning { .. } => 0.0,
            AccountEvent::Error(_) => 0.0,
        }
    }

    #[pyo3(name = "__repr__")]
    fn py_repr(&self) -> String {
        match &self.inner {
            AccountEvent::BalanceUpdate(b) => format!(
                "AccountEvent::BalanceUpdate(account={}, total={}, available={}, locked={})",
                b.account_id, b.total, b.available, b.locked
            ),
            AccountEvent::MarginWarning {
                account_id,
                message,
            } => {
                format!("AccountEvent::MarginWarning(account={account_id}, message={message})")
            }
            AccountEvent::Error(e) => format!("AccountEvent::Error({e})"),
        }
    }
}

impl From<AccountEvent> for PyAccountEvent {
    fn from(event: AccountEvent) -> Self {
        Self { inner: event }
    }
}

/// Python wrapper for PositionEvent.
#[cfg(feature = "python")]
#[pyclass(name = "PositionEvent", skip_from_py_object)]
#[pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.rithmic")]
#[derive(Clone)]
pub(crate) struct PyPositionEvent {
    inner: PositionEvent,
}

#[cfg(feature = "python")]
#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl PyPositionEvent {
    #[getter(is_snapshot)]
    fn py_is_snapshot(&self) -> bool {
        match &self.inner {
            PositionEvent::Updated(position) | PositionEvent::Opened(position) => {
                position.is_snapshot
            }
            PositionEvent::Closed { .. } => false,
            PositionEvent::Error(_) => false,
        }
    }

    #[getter(account_id)]
    fn py_account_id(&self) -> &str {
        match &self.inner {
            PositionEvent::Updated(p) | PositionEvent::Opened(p) => &p.account_id,
            PositionEvent::Closed { account_id, .. } => account_id,
            PositionEvent::Error(_) => "",
        }
    }

    #[getter(symbol)]
    fn py_symbol(&self) -> &str {
        match &self.inner {
            PositionEvent::Updated(p) | PositionEvent::Opened(p) => &p.symbol,
            PositionEvent::Closed { symbol, .. } => symbol,
            PositionEvent::Error(_) => "",
        }
    }

    #[getter(exchange)]
    fn py_exchange(&self) -> &str {
        match &self.inner {
            PositionEvent::Updated(p) | PositionEvent::Opened(p) => &p.exchange,
            PositionEvent::Closed { exchange, .. } => exchange,
            PositionEvent::Error(_) => "",
        }
    }

    #[getter(quantity)]
    fn py_quantity(&self) -> f64 {
        match &self.inner {
            PositionEvent::Updated(p) | PositionEvent::Opened(p) => p.quantity,
            PositionEvent::Closed { .. } => 0.0,
            PositionEvent::Error(_) => 0.0,
        }
    }

    #[getter(avg_price)]
    fn py_avg_price(&self) -> f64 {
        match &self.inner {
            PositionEvent::Updated(p) | PositionEvent::Opened(p) => p.avg_price,
            PositionEvent::Closed { .. } => 0.0,
            PositionEvent::Error(_) => 0.0,
        }
    }

    #[getter(unrealized_pnl)]
    fn py_unrealized_pnl(&self) -> f64 {
        match &self.inner {
            PositionEvent::Updated(p) | PositionEvent::Opened(p) => p.unrealized_pnl,
            PositionEvent::Closed { .. } => 0.0,
            PositionEvent::Error(_) => 0.0,
        }
    }

    #[getter(realized_pnl)]
    fn py_realized_pnl(&self) -> f64 {
        match &self.inner {
            PositionEvent::Updated(p) | PositionEvent::Opened(p) => p.realized_pnl,
            PositionEvent::Closed { realized_pnl, .. } => *realized_pnl,
            PositionEvent::Error(_) => 0.0,
        }
    }

    #[getter(ts_event)]
    fn py_ts_event(&self) -> u64 {
        match &self.inner {
            PositionEvent::Updated(p) | PositionEvent::Opened(p) => p.ts_event,
            PositionEvent::Closed { .. } => 0,
            PositionEvent::Error(_) => 0,
        }
    }

    #[pyo3(name = "__repr__")]
    fn py_repr(&self) -> String {
        match &self.inner {
            PositionEvent::Updated(p) | PositionEvent::Opened(p) => format!(
                "PositionEvent::Updated(account={}, symbol={}.{}, qty={}, avg_price={})",
                p.account_id, p.symbol, p.exchange, p.quantity, p.avg_price
            ),
            PositionEvent::Closed {
                account_id,
                symbol,
                exchange,
                ..
            } => format!("PositionEvent::Closed(account={account_id}, symbol={symbol}.{exchange})"),
            PositionEvent::Error(e) => format!("PositionEvent::Error({e})"),
        }
    }
}

impl From<PositionEvent> for PyPositionEvent {
    fn from(event: PositionEvent) -> Self {
        Self { inner: event }
    }
}

// Time bar data.

/// Python wrapper for Rithmic time bar data from history requests and live updates.
#[cfg(feature = "python")]
#[pyclass(name = "TimeBar", skip_from_py_object)]
#[pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.rithmic")]
#[derive(Clone)]
pub(crate) struct PyTimeBar {
    /// Instrument symbol.
    pub symbol: String,
    /// Exchange.
    pub exchange: String,
    /// Open price.
    pub open_price: f64,
    /// High price.
    pub high_price: f64,
    /// Low price.
    pub low_price: f64,
    /// Close price.
    pub close_price: f64,
    /// Volume.
    pub volume: i64,
    /// Raw Rithmic period field.
    pub period: String,
    /// Parsed Rithmic bar type name.
    pub bar_kind: String,
    /// Parsed Rithmic bar period/step.
    pub bar_period: i32,
    /// Price precision (number of decimal places).
    pub price_precision: u8,
    /// Size precision (number of decimal places).
    pub size_precision: u8,
    /// Raw Rithmic bar marker.
    pub marker: Option<i64>,
    /// Event timestamp in nanoseconds.
    pub ts_event: u64,
    /// Initialization timestamp in nanoseconds.
    pub ts_init: u64,
}

#[cfg(feature = "python")]
#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl PyTimeBar {
    #[getter(symbol)]
    fn py_symbol(&self) -> &str {
        &self.symbol
    }

    #[getter(exchange)]
    fn py_exchange(&self) -> &str {
        &self.exchange
    }

    #[getter(open_price)]
    fn py_open_price(&self) -> f64 {
        self.open_price
    }

    #[getter(high_price)]
    fn py_high_price(&self) -> f64 {
        self.high_price
    }

    #[getter(low_price)]
    fn py_low_price(&self) -> f64 {
        self.low_price
    }

    #[getter(close_price)]
    fn py_close_price(&self) -> f64 {
        self.close_price
    }

    #[getter(volume)]
    fn py_volume(&self) -> i64 {
        self.volume
    }

    #[getter(period)]
    fn py_period(&self) -> &str {
        &self.period
    }

    #[getter(bar_kind)]
    fn py_bar_kind(&self) -> &str {
        &self.bar_kind
    }

    #[getter(bar_period)]
    fn py_bar_period(&self) -> i32 {
        self.bar_period
    }

    #[getter(price_precision)]
    fn py_price_precision(&self) -> u8 {
        self.price_precision
    }

    #[getter(size_precision)]
    fn py_size_precision(&self) -> u8 {
        self.size_precision
    }

    #[getter(marker)]
    fn py_marker(&self) -> Option<i64> {
        self.marker
    }

    #[getter(ts_event)]
    fn py_ts_event(&self) -> u64 {
        self.ts_event
    }

    #[getter(ts_init)]
    fn py_ts_init(&self) -> u64 {
        self.ts_init
    }

    /// Converts this Rithmic `TimeBar` into a NautilusTrader `Bar`.
    ///
    /// Parameters
    /// ----------
    /// bar_type : str
    ///     The full NautilusTrader bar type string
    ///     (e.g. ``"ESH5.RITHMIC-1-MINUTE-LAST-EXTERNAL"``).
    /// price_precision : int, optional
    ///     Number of decimal places for prices. If not provided, uses stored value.
    /// size_precision : int, optional
    ///     Number of decimal places for volume. If not provided, uses stored value.
    ///
    /// Returns
    /// -------
    /// nautilus_trader.model.data.Bar
    #[pyo3(signature = (bar_type, price_precision = None, size_precision = None))]
    #[pyo3(name = "to_nautilus_bar")]
    fn py_to_nautilus_bar(
        &self,
        bar_type: &str,
        price_precision: Option<u8>,
        size_precision: Option<u8>,
    ) -> PyResult<NautilusBar> {
        let bar_type: BarType = bar_type
            .parse()
            .map_err(|e: BarTypeParseError| to_pyvalue_err(e.to_string()))?;
        let price_prec = price_precision.unwrap_or(self.price_precision);
        let size_prec = size_precision.unwrap_or(self.size_precision);
        Ok(NautilusBar::new(
            bar_type,
            Price::new(self.open_price, price_prec),
            Price::new(self.high_price, price_prec),
            Price::new(self.low_price, price_prec),
            Price::new(self.close_price, price_prec),
            Quantity::new(self.volume as f64, size_prec),
            self.ts_event.into(),
            self.ts_init.into(),
        ))
    }

    #[pyo3(name = "__repr__")]
    fn py_repr(&self) -> String {
        format!(
            "TimeBar({}:{} type={}, period={}, marker={:?}, o={:.2}, h={:.2}, l={:.2}, c={:.2}, v={})",
            self.exchange,
            self.symbol,
            self.bar_kind,
            self.bar_period,
            self.marker,
            self.open_price,
            self.high_price,
            self.low_price,
            self.close_price,
            self.volume
        )
    }
}

impl PyTimeBar {
    /// Creates a PyTimeBar from a Rithmic ResponseTimeBarReplay message.
    pub(crate) fn from_time_response(bar: &rithmic_rs::rti::ResponseTimeBarReplay) -> Self {
        let marker = bar.marker.map(i64::from);
        let ts_event = marker
            .filter(|value| *value > 0)
            .map(|value| value as u64 * 1_000_000_000)
            .or_else(|| {
                bar.period
                    .as_deref()
                    .and_then(|value| value.parse::<u64>().ok())
                    .map(|value| value * 1_000_000_000)
            })
            .unwrap_or(0);

        Self {
            symbol: bar.symbol.clone().unwrap_or_default(),
            exchange: bar.exchange.clone().unwrap_or_default(),
            open_price: bar.open_price.unwrap_or(0.0),
            high_price: bar.high_price.unwrap_or(0.0),
            low_price: bar.low_price.unwrap_or(0.0),
            close_price: bar.close_price.unwrap_or(0.0),
            volume: volume_to_i64(bar.volume.unwrap_or(0)),
            period: bar.period.clone().unwrap_or_default(),
            bar_kind: bar
                .r#type
                .and_then(time_replay_bar_type)
                .unwrap_or(RithmicBarType::MinuteBar)
                .as_str()
                .to_string(),
            bar_period: bar
                .period
                .as_deref()
                .and_then(|value| value.parse::<i32>().ok())
                .unwrap_or_default(),
            price_precision: 2,
            size_precision: 0,
            marker,
            ts_event,
            ts_init: ts_event,
        }
    }

    /// Creates a PyTimeBar from a Rithmic ResponseTickBarReplay message.
    pub(crate) fn from_tick_response(bar: &rithmic_rs::rti::ResponseTickBarReplay) -> Self {
        let marker = bar.data_bar_ssboe.last().copied().map(i64::from);
        let ts_event = tick_timestamp_nanos(&bar.data_bar_ssboe, &bar.data_bar_usecs);

        Self {
            symbol: bar.symbol.clone().unwrap_or_default(),
            exchange: bar.exchange.clone().unwrap_or_default(),
            open_price: bar.open_price.unwrap_or(0.0),
            high_price: bar.high_price.unwrap_or(0.0),
            low_price: bar.low_price.unwrap_or(0.0),
            close_price: bar.close_price.unwrap_or(0.0),
            volume: volume_to_i64(bar.volume.unwrap_or(0)),
            period: bar
                .type_specifier
                .clone()
                .unwrap_or_else(|| "1".to_string()),
            bar_kind: "TickBar".to_string(),
            bar_period: bar
                .type_specifier
                .as_deref()
                .and_then(|value| value.parse::<i32>().ok())
                .unwrap_or(1),
            price_precision: 2,
            size_precision: 0,
            marker,
            ts_event,
            ts_init: ts_event,
        }
    }

    /// Creates a PyTimeBar from a Rithmic live TimeBar update.
    pub(crate) fn from_live_time_update(bar: &rithmic_rs::rti::TimeBar) -> Option<Self> {
        let marker = bar.marker.map(i64::from);
        let ts_event = marker
            .filter(|value| *value > 0)
            .map(|value| value as u64 * 1_000_000_000)?;

        Some(Self {
            symbol: bar.symbol.clone().unwrap_or_default(),
            exchange: bar.exchange.clone().unwrap_or_default(),
            open_price: bar.open_price.unwrap_or(0.0),
            high_price: bar.high_price.unwrap_or(0.0),
            low_price: bar.low_price.unwrap_or(0.0),
            close_price: bar.close_price.unwrap_or(0.0),
            volume: volume_to_i64(bar.volume.unwrap_or(0)),
            period: bar.period.clone().unwrap_or_default(),
            bar_kind: bar
                .r#type
                .and_then(time_replay_bar_type)
                .unwrap_or(RithmicBarType::MinuteBar)
                .as_str()
                .to_string(),
            bar_period: bar
                .period
                .as_deref()
                .and_then(|value| value.parse::<i32>().ok())
                .unwrap_or_default(),
            price_precision: 2,
            size_precision: 0,
            marker,
            ts_event,
            ts_init: ts_event,
        })
    }
}

impl From<LiveTimeBar> for PyTimeBar {
    fn from(bar: LiveTimeBar) -> Self {
        Self {
            symbol: bar.symbol,
            exchange: bar.exchange,
            open_price: bar.open_price,
            high_price: bar.high_price,
            low_price: bar.low_price,
            close_price: bar.close_price,
            volume: bar.volume as i64,
            period: bar.bar_period.to_string(),
            bar_kind: bar.bar_type.as_str().to_string(),
            bar_period: bar.bar_period,
            price_precision: bar.price_precision,
            size_precision: bar.size_precision,
            marker: bar.marker,
            ts_event: bar.ts_event,
            ts_init: bar.ts_init,
        }
    }
}

/// Python wrapper for order-book deltas (depth-by-order updates).
#[cfg(feature = "python")]
#[pyclass(name = "BookDelta", skip_from_py_object)]
#[pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.rithmic")]
#[derive(Clone)]
pub(crate) struct PyBookDelta {
    inner: BookDelta,
}

#[cfg(feature = "python")]
#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl PyBookDelta {
    #[getter(symbol)]
    fn py_symbol(&self) -> &str {
        &self.inner.symbol
    }

    #[getter(exchange)]
    fn py_exchange(&self) -> &str {
        &self.inner.exchange
    }

    #[getter(action)]
    fn py_action(&self) -> &str {
        &self.inner.action
    }

    #[getter(side)]
    fn py_side(&self) -> &str {
        &self.inner.side
    }

    #[getter(price)]
    fn py_price(&self) -> f64 {
        self.inner.price
    }

    #[getter(size)]
    fn py_size(&self) -> f64 {
        self.inner.size
    }

    #[getter(order_id)]
    fn py_order_id(&self) -> u64 {
        self.inner.order_id
    }

    #[getter(sequence)]
    fn py_sequence(&self) -> u64 {
        self.inner.sequence
    }

    #[getter(flags)]
    fn py_flags(&self) -> u8 {
        self.inner.flags
    }

    #[getter(price_precision)]
    fn py_price_precision(&self) -> u8 {
        self.inner.price_precision
    }

    #[getter(size_precision)]
    fn py_size_precision(&self) -> u8 {
        self.inner.size_precision
    }

    #[getter(ts_event)]
    fn py_ts_event(&self) -> u64 {
        self.inner.ts_event
    }

    #[getter(ts_init)]
    fn py_ts_init(&self) -> u64 {
        self.inner.ts_init
    }

    #[pyo3(signature = (instrument_id, price_precision = None, size_precision = None))]
    #[pyo3(name = "to_nautilus_order_book_delta")]
    fn py_to_nautilus_order_book_delta(
        &self,
        instrument_id: &str,
        price_precision: Option<u8>,
        size_precision: Option<u8>,
    ) -> PyResult<NautilusOrderBookDelta> {
        let instrument_id = NautilusInstrumentId::from(instrument_id);
        let price_precision = price_precision.unwrap_or(self.inner.price_precision.max(2));
        let size_precision = size_precision.unwrap_or(self.inner.size_precision);

        let action = match self.inner.action.as_str() {
            "ADD" => BookAction::Add,
            "UPDATE" => BookAction::Update,
            "REMOVE" => BookAction::Delete,
            other => {
                return Err(to_pyvalue_err(format!(
                    "Unsupported Rithmic book action: {other}"
                )));
            }
        };

        let side = match self.inner.side.as_str() {
            "BUY" => NautilusOrderSide::Buy,
            "SELL" => NautilusOrderSide::Sell,
            other => {
                return Err(to_pyvalue_err(format!(
                    "Unsupported Rithmic book side: {other}"
                )));
            }
        };

        let order = BookOrder::new(
            side,
            Price::new(self.inner.price, price_precision),
            Quantity::new(self.inner.size, size_precision),
            self.inner.order_id,
        );

        NautilusOrderBookDelta::new_checked(
            instrument_id,
            action,
            order,
            self.inner.flags,
            self.inner.sequence,
            self.inner.ts_event.into(),
            self.inner.ts_init.into(),
        )
        .map_err(to_pyvalue_err)
    }

    #[pyo3(name = "__repr__")]
    fn py_repr(&self) -> String {
        format!(
            "BookDelta(symbol={}, exchange={}, action={}, side={}, price={}, size={}, order_id={}, sequence={}, flags={})",
            self.inner.symbol,
            self.inner.exchange,
            self.inner.action,
            self.inner.side,
            self.inner.price,
            self.inner.size,
            self.inner.order_id,
            self.inner.sequence,
            self.inner.flags,
        )
    }
}

impl From<BookDelta> for PyBookDelta {
    fn from(delta: BookDelta) -> Self {
        Self { inner: delta }
    }
}

fn volume_to_i64(value: u64) -> i64 {
    value.min(i64::MAX as u64) as i64
}

fn tick_timestamp_nanos(ssboe: &[i32], usecs: &[i32]) -> u64 {
    let secs = ssboe.last().copied().unwrap_or_default() as u64;
    let micros = usecs.last().copied().unwrap_or_default() as u64;
    secs * 1_000_000_000 + micros * 1_000
}

fn time_replay_bar_type(value: i32) -> Option<RithmicBarType> {
    match crate::TimeBarType::try_from(value).ok()? {
        crate::TimeBarType::SecondBar => Some(RithmicBarType::SecondBar),
        crate::TimeBarType::MinuteBar => Some(RithmicBarType::MinuteBar),
        crate::TimeBarType::DailyBar => Some(RithmicBarType::DailyBar),
        crate::TimeBarType::WeeklyBar => Some(RithmicBarType::WeeklyBar),
    }
}

/// Registers event types with the Python module.
#[cfg(feature = "python")]
pub(crate) fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Market data events
    m.add_class::<PyQuoteTick>()?;
    m.add_class::<PyTradeTick>()?;
    m.add_class::<PyMarketDataEvent>()?;
    m.add_class::<PyTimeBar>()?;
    m.add_class::<PyBookDelta>()?;

    // Execution events
    m.add_class::<PyOrderSubmitted>()?;
    m.add_class::<PyOrderAccepted>()?;
    m.add_class::<PyOrderRejected>()?;
    m.add_class::<PyOrderFilled>()?;
    m.add_class::<PyOrderCancelled>()?;
    m.add_class::<PyOrderModified>()?;
    m.add_class::<PyExecutionEvent>()?;

    // PnL / Position events
    m.add_class::<PyAccountEvent>()?;
    m.add_class::<PyPositionEvent>()?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use rithmic_rs::rti::ResponseTickBarReplay;
    use rstest::rstest;

    use super::PyTradeTick;

    #[rstest]
    fn replay_tick_uses_exchange_time_for_both_timestamps() {
        let tick = ResponseTickBarReplay {
            symbol: Some("MNQM6".to_string()),
            exchange: Some("CME".to_string()),
            close_price: Some(20_000.25),
            volume: Some(4),
            data_bar_ssboe: vec![1_700_000_123],
            data_bar_usecs: vec![456_789],
            ..Default::default()
        };

        let converted = PyTradeTick::from_tick_replay(&tick, 7).expect("valid replay tick");

        assert_eq!(converted.py_ts_event(), 1_700_000_123_456_789_000);
        assert_eq!(converted.py_ts_init(), 1_700_000_123_456_789_000);
    }

    #[rstest]
    fn replay_tick_drops_zero_timestamp_rows() {
        let tick = ResponseTickBarReplay {
            symbol: Some(String::new()),
            exchange: Some(String::new()),
            close_price: Some(0.0),
            volume: Some(0),
            data_bar_ssboe: vec![],
            data_bar_usecs: vec![],
            ..Default::default()
        };

        assert!(PyTradeTick::from_tick_replay(&tick, 99).is_none());
    }
}
