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

//! Python bindings for data client.

#![allow(
    clippy::needless_pass_by_value,
    reason = "PyO3 data-client APIs accept owned Python values at the FFI boundary"
)]

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use ahash::{AHashMap, AHashSet};
use nautilus_common::live::get_runtime;
use nautilus_core::{
    python::{to_pyruntime_err, to_pyvalue_err},
    time::get_atomic_clock_realtime,
};
use nautilus_model::identifiers::InstrumentId;
#[cfg(feature = "python")]
use pyo3::prelude::*;
#[cfg(feature = "python")]
use pyo3_async_runtimes::tokio::future_into_py;
use rithmic_rs::rti::{messages::RithmicMessage, request_tick_bar_update};
use tokio::task::JoinHandle;

use super::{
    events::{PyMarketDataEvent, PyTimeBar, PyTradeTick},
    gateway::PyRithmicGateway,
};
use crate::{
    TimeBarType,
    common::parse::tick_size_to_precision,
    data::{
        MarketDataEvent,
        live::{depth10_from_order_book, order_book_from_snapshot},
    },
    gateway::RithmicGateway,
};

/// Python wrapper for RithmicDataClient.
///
/// The data client manages market data subscriptions and receives
/// quotes/trades from the ticker plant plus live time bars from the history plant.
///
/// Example
/// -------
/// ```python
/// gateway = RithmicGateway.from_env()
/// await gateway.connect()
///
/// client = RithmicDataClient(gateway)
/// client.set_data_callback(on_market_data)
/// await client.subscribe_quotes("ESH5", "CME")
/// ```
#[cfg(feature = "python")]
#[pyclass(name = "RithmicDataClient")]
#[pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.rithmic")]
pub(crate) struct PyRithmicDataClient {
    /// Reference to the gateway for async operations.
    gateway: Arc<tokio::sync::RwLock<RithmicGateway>>,
    /// Local subscription tracking (mirrors the Rust client).
    /// Uses Arc so it can be shared with async futures.
    subscriptions: Arc<parking_lot::RwLock<AHashSet<String>>>,
    /// Local live bar subscription tracking.
    bar_subscriptions: Arc<parking_lot::RwLock<AHashSet<String>>>,
    /// Local instrument-status subscription tracking.
    status_subscriptions: Arc<parking_lot::RwLock<AHashSet<String>>>,
    /// Local order-book subscription tracking.
    book_subscriptions: Arc<parking_lot::RwLock<AHashMap<String, BookSubscription>>>,
    /// Local tracking for Rithmic-specific custom market-data feeds.
    extra_market_data_subscriptions:
        Arc<parking_lot::RwLock<AHashMap<String, ExtraMarketDataSubscription>>>,
    /// Python callback for market data events.
    data_callback: Arc<parking_lot::Mutex<Option<Py<PyAny>>>>,
    event_task: Arc<parking_lot::Mutex<Option<JoinHandle<()>>>>,
    event_running: Arc<AtomicBool>,
    shutdown_tx: Arc<parking_lot::Mutex<Option<tokio::sync::oneshot::Sender<()>>>>,
}

#[derive(Clone, Copy, Default)]
struct BookSubscription {
    deltas: bool,
    depth10: bool,
}

#[derive(Clone, Copy, Default)]
struct ExtraMarketDataSubscription {
    trade_statistics: bool,
    quote_statistics: bool,
    indicator_prices: bool,
    open_interest: bool,
    end_of_day_prices: bool,
    order_price_limits: bool,
    symbol_margin_rate: bool,
}

#[derive(Clone, Copy)]
enum ExtraMarketDataKind {
    TradeStatistics,
    QuoteStatistics,
    IndicatorPrices,
    OpenInterest,
    EndOfDayPrices,
    OrderPriceLimits,
    SymbolMarginRate,
}

#[derive(Clone, Copy)]
enum ParsedBarType {
    Time(TimeBarType),
    Tick,
}

impl ParsedBarType {
    fn as_str(self) -> &'static str {
        match self {
            Self::Time(TimeBarType::SecondBar) => "SecondBar",
            Self::Time(TimeBarType::MinuteBar) => "MinuteBar",
            Self::Time(TimeBarType::DailyBar) => "DailyBar",
            Self::Time(TimeBarType::WeeklyBar) => "WeeklyBar",
            Self::Tick => "TickBar",
        }
    }
}

#[cfg(feature = "python")]
#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl PyRithmicDataClient {
    /// Creates a new data client from a connected gateway.
    ///
    /// Parameters
    /// ----------
    /// gateway : RithmicGateway
    ///     The connected gateway instance.
    #[new]
    fn py_new(gateway: &PyRithmicGateway) -> Self {
        Self {
            gateway: Arc::clone(&gateway.inner),
            subscriptions: Arc::new(parking_lot::RwLock::new(AHashSet::new())),
            bar_subscriptions: Arc::new(parking_lot::RwLock::new(AHashSet::new())),
            status_subscriptions: Arc::new(parking_lot::RwLock::new(AHashSet::new())),
            book_subscriptions: Arc::new(parking_lot::RwLock::new(AHashMap::new())),
            extra_market_data_subscriptions: Arc::new(parking_lot::RwLock::new(AHashMap::new())),
            data_callback: Arc::new(parking_lot::Mutex::new(None)),
            event_task: Arc::new(parking_lot::Mutex::new(None)),
            event_running: Arc::new(AtomicBool::new(false)),
            shutdown_tx: Arc::new(parking_lot::Mutex::new(None)),
        }
    }

    /// Returns true if the gateway is connected.
    #[getter(is_connected)]
    fn py_is_connected(&self) -> bool {
        self.gateway.try_read().is_ok_and(|g| g.is_connected())
    }

    /// Returns the number of active subscriptions.
    #[getter(subscription_count)]
    fn py_subscription_count(&self) -> usize {
        self.subscriptions.read().len()
    }

    /// Returns all active subscription keys in "EXCHANGE:SYMBOL" format.
    #[pyo3(name = "subscriptions")]
    fn py_subscriptions(&self) -> Vec<String> {
        self.subscriptions.read().iter().cloned().collect()
    }

    /// Returns the number of active live bar subscriptions.
    #[getter(bar_subscription_count)]
    fn py_bar_subscription_count(&self) -> usize {
        self.bar_subscriptions.read().len()
    }

    /// Returns all active live bar subscription keys.
    #[pyo3(name = "bar_subscriptions")]
    fn py_bar_subscriptions(&self) -> Vec<String> {
        self.bar_subscriptions.read().iter().cloned().collect()
    }

    /// Returns the number of active order-book delta subscriptions.
    #[getter(book_subscription_count)]
    fn py_book_subscription_count(&self) -> usize {
        self.book_subscriptions.read().len()
    }

    /// Returns all active order-book delta subscription keys.
    #[pyo3(name = "book_subscriptions")]
    fn py_book_subscriptions(&self) -> Vec<String> {
        self.book_subscriptions.read().keys().cloned().collect()
    }

    /// Returns true if subscribed to quotes for the given instrument.
    #[pyo3(name = "is_subscribed")]
    fn py_is_subscribed(&self, symbol: &str, exchange: &str) -> bool {
        let key = format!("{exchange}:{symbol}");
        self.subscriptions.read().contains(&key)
    }

    /// Returns true if subscribed to the given live time-bar stream.
    #[pyo3(name = "is_subscribed_bars")]
    fn py_is_subscribed_bars(
        &self,
        symbol: &str,
        exchange: &str,
        bar_type: String,
        bar_period: i32,
    ) -> bool {
        let key = Self::bar_subscription_key(symbol, exchange, &bar_type, bar_period);
        self.bar_subscriptions.read().contains(&key)
    }

    /// Returns true if subscribed to order-book deltas for the given instrument.
    #[pyo3(name = "is_subscribed_book_deltas")]
    fn py_is_subscribed_book_deltas(&self, symbol: &str, exchange: &str) -> bool {
        let key = format!("{exchange}:{symbol}");
        self.book_subscriptions
            .read()
            .get(&key)
            .is_some_and(|sub| sub.deltas)
    }

    /// Returns true if subscribed to order-book depth10 for the given instrument.
    #[pyo3(name = "is_subscribed_book_depth10")]
    fn py_is_subscribed_book_depth10(&self, symbol: &str, exchange: &str) -> bool {
        let key = format!("{exchange}:{symbol}");
        self.book_subscriptions
            .read()
            .get(&key)
            .is_some_and(|sub| sub.depth10)
    }

    /// Sets the callback for market data events.
    ///
    /// The callback will be called with each market data event (quotes, trades,
    /// live bars, connection state changes, etc.).
    ///
    /// Parameters
    /// ----------
    /// callback : callable
    ///     A Python callable that accepts a single argument (the event).
    ///     The event can be a QuoteTick, TradeTick, or MarketDataEvent.
    ///
    /// Example
    /// -------
    /// ```python
    /// def on_data(event):
    ///     if event.is_quote():
    ///         quote = event.as_quote()
    ///         print(f"Quote: {quote.symbol} bid={quote.bid_price}")
    ///
    /// client.set_data_callback(on_data)
    /// ```
    #[pyo3(name = "set_data_callback")]
    fn py_set_data_callback(&self, callback: Py<PyAny>) {
        *self.data_callback.lock() = Some(callback);
    }

    /// Clears the data callback.
    #[pyo3(name = "clear_data_callback")]
    fn py_clear_data_callback(&self) {
        *self.data_callback.lock() = None;
    }

    /// Starts the background event loop for market data.
    ///
    /// This takes ownership of the gateway's market data receiver and dispatches
    /// events to the Python callback set via `set_data_callback`.
    ///
    /// This is an async method - use `await client.start_event_loop()`.
    #[pyo3(name = "start_event_loop")]
    fn py_start_event_loop<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let gateway = Arc::clone(&self.gateway);
        let callback = Arc::clone(&self.data_callback);
        let event_task = Arc::clone(&self.event_task);
        let shutdown_tx = Arc::clone(&self.shutdown_tx);
        let event_running = Arc::clone(&self.event_running);

        future_into_py(py, async move {
            if event_running.swap(true, Ordering::SeqCst) {
                return Err(to_pyruntime_err("Market data event loop already running"));
            }

            let rx = {
                let gw = gateway.read().await;
                gw.subscribe_market_data_events()
            };

            let (tx, rx_shutdown) = tokio::sync::oneshot::channel();
            *shutdown_tx.lock() = Some(tx);

            let handle = get_runtime().spawn(Self::event_loop(rx, rx_shutdown, callback));

            *event_task.lock() = Some(handle);

            Ok(())
        })
    }

    /// Stops the background event loop for market data.
    #[pyo3(name = "stop_event_loop")]
    fn py_stop_event_loop(&self) {
        self.event_running.store(false, Ordering::SeqCst);

        if let Some(tx) = self.shutdown_tx.lock().take() {
            let _ = tx.send(());
        }

        if let Some(handle) = self.event_task.lock().take() {
            handle.abort();
        }
    }

    /// Subscribes to quotes (best bid/offer) for an instrument.
    ///
    /// This is an async method - use `await client.subscribe_quotes(symbol, exchange)`.
    ///
    /// Parameters
    /// ----------
    /// symbol : str
    ///     The instrument symbol (e.g., "ESH5").
    /// exchange : str
    ///     The exchange code (e.g., "CME").
    ///
    /// Returns
    /// -------
    /// None
    ///     On successful subscription.
    ///
    /// Note
    /// ----
    /// The underlying gateway helper requests both `BBO` and `LAST_TRADE`,
    /// so this shares one combined subscription with `subscribe_trades()`.
    ///
    /// Raises
    /// ------
    /// RuntimeError
    ///     If subscription fails.
    /// ValueError
    ///     If symbol or exchange is empty.
    #[pyo3(name = "subscribe_quotes")]
    fn py_subscribe_quotes<'py>(
        &self,
        py: Python<'py>,
        symbol: String,
        exchange: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        // Validate inputs
        Self::validate_symbol_exchange(&symbol, &exchange)?;

        let gateway = Arc::clone(&self.gateway);
        let subscriptions = Arc::clone(&self.subscriptions);

        future_into_py(py, async move {
            Self::subscribe_market_data_alias(&gateway, &subscriptions, symbol, exchange).await
        })
    }

    /// Subscribes to trades (last trade) for an instrument.
    ///
    /// This is an async method.
    ///
    /// Note: the current gateway market-data helper requests both `BBO`
    /// and `LAST_TRADE`, so this is equivalent to `subscribe_quotes()`.
    #[pyo3(name = "subscribe_trades")]
    fn py_subscribe_trades<'py>(
        &self,
        py: Python<'py>,
        symbol: String,
        exchange: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        // Same as subscribe_quotes - the current gateway helper requests both
        self.py_subscribe_quotes(py, symbol, exchange)
    }

    /// Subscribes to both quotes and trades for an instrument.
    ///
    /// This is an async method.
    #[pyo3(name = "subscribe")]
    fn py_subscribe<'py>(
        &self,
        py: Python<'py>,
        symbol: String,
        exchange: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.py_subscribe_quotes(py, symbol, exchange)
    }

    /// Subscribes to venue instrument-status updates for an instrument.
    #[pyo3(name = "subscribe_instrument_status")]
    fn py_subscribe_instrument_status<'py>(
        &self,
        py: Python<'py>,
        symbol: String,
        exchange: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        Self::validate_symbol_exchange(&symbol, &exchange)?;

        let gateway = Arc::clone(&self.gateway);
        let status_subscriptions = Arc::clone(&self.status_subscriptions);
        let key = format!("{exchange}:{symbol}");

        future_into_py(py, async move {
            if !status_subscriptions.write().insert(key) {
                return Ok(());
            }

            let gw = gateway.read().await;
            gw.subscribe_instrument_status(&symbol, &exchange)
                .await
                .map_err(|e| {
                    to_pyruntime_err(format!("Instrument status subscription failed: {e}"))
                })
        })
    }

    /// Subscribes to trade statistics updates for an instrument.
    #[pyo3(name = "subscribe_trade_statistics")]
    fn py_subscribe_trade_statistics<'py>(
        &self,
        py: Python<'py>,
        symbol: String,
        exchange: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        Self::subscribe_extra_market_data(
            &self.gateway,
            &self.extra_market_data_subscriptions,
            py,
            ExtraMarketDataSubscriptionCmd {
                symbol,
                exchange,
                kind: ExtraMarketDataKind::TradeStatistics,
                enabled: true,
                error_prefix: "Trade statistics subscription failed",
            },
        )
    }

    /// Subscribes to quote statistics updates for an instrument.
    #[pyo3(name = "subscribe_quote_statistics")]
    fn py_subscribe_quote_statistics<'py>(
        &self,
        py: Python<'py>,
        symbol: String,
        exchange: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        Self::subscribe_extra_market_data(
            &self.gateway,
            &self.extra_market_data_subscriptions,
            py,
            ExtraMarketDataSubscriptionCmd {
                symbol,
                exchange,
                kind: ExtraMarketDataKind::QuoteStatistics,
                enabled: true,
                error_prefix: "Quote statistics subscription failed",
            },
        )
    }

    /// Subscribes to indicator price updates for an instrument.
    #[pyo3(name = "subscribe_indicator_prices")]
    fn py_subscribe_indicator_prices<'py>(
        &self,
        py: Python<'py>,
        symbol: String,
        exchange: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        Self::subscribe_extra_market_data(
            &self.gateway,
            &self.extra_market_data_subscriptions,
            py,
            ExtraMarketDataSubscriptionCmd {
                symbol,
                exchange,
                kind: ExtraMarketDataKind::IndicatorPrices,
                enabled: true,
                error_prefix: "Indicator prices subscription failed",
            },
        )
    }

    /// Subscribes to open-interest updates for an instrument.
    #[pyo3(name = "subscribe_open_interest")]
    fn py_subscribe_open_interest<'py>(
        &self,
        py: Python<'py>,
        symbol: String,
        exchange: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        Self::subscribe_extra_market_data(
            &self.gateway,
            &self.extra_market_data_subscriptions,
            py,
            ExtraMarketDataSubscriptionCmd {
                symbol,
                exchange,
                kind: ExtraMarketDataKind::OpenInterest,
                enabled: true,
                error_prefix: "Open interest subscription failed",
            },
        )
    }

    /// Subscribes to end-of-day price updates for an instrument.
    #[pyo3(name = "subscribe_end_of_day_prices")]
    fn py_subscribe_end_of_day_prices<'py>(
        &self,
        py: Python<'py>,
        symbol: String,
        exchange: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        Self::subscribe_extra_market_data(
            &self.gateway,
            &self.extra_market_data_subscriptions,
            py,
            ExtraMarketDataSubscriptionCmd {
                symbol,
                exchange,
                kind: ExtraMarketDataKind::EndOfDayPrices,
                enabled: true,
                error_prefix: "End-of-day prices subscription failed",
            },
        )
    }

    /// Subscribes to order price limit updates for an instrument.
    #[pyo3(name = "subscribe_order_price_limits")]
    fn py_subscribe_order_price_limits<'py>(
        &self,
        py: Python<'py>,
        symbol: String,
        exchange: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        Self::subscribe_extra_market_data(
            &self.gateway,
            &self.extra_market_data_subscriptions,
            py,
            ExtraMarketDataSubscriptionCmd {
                symbol,
                exchange,
                kind: ExtraMarketDataKind::OrderPriceLimits,
                enabled: true,
                error_prefix: "Order price limits subscription failed",
            },
        )
    }

    /// Subscribes to symbol margin-rate updates for an instrument.
    #[pyo3(name = "subscribe_symbol_margin_rate")]
    fn py_subscribe_symbol_margin_rate<'py>(
        &self,
        py: Python<'py>,
        symbol: String,
        exchange: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        Self::subscribe_extra_market_data(
            &self.gateway,
            &self.extra_market_data_subscriptions,
            py,
            ExtraMarketDataSubscriptionCmd {
                symbol,
                exchange,
                kind: ExtraMarketDataKind::SymbolMarginRate,
                enabled: true,
                error_prefix: "Symbol margin rate subscription failed",
            },
        )
    }

    /// Subscribes to live bars on the history plant.
    ///
    /// This is an async method.
    #[pyo3(name = "subscribe_bars")]
    fn py_subscribe_bars<'py>(
        &self,
        py: Python<'py>,
        symbol: String,
        exchange: String,
        bar_type: String,
        bar_period: i32,
    ) -> PyResult<Bound<'py, PyAny>> {
        Self::validate_symbol_exchange(&symbol, &exchange)?;
        let bar_type = Self::parse_bar_type(&bar_type)?;

        let gateway = Arc::clone(&self.gateway);
        let bar_subscriptions = Arc::clone(&self.bar_subscriptions);
        let key = Self::bar_subscription_key(&symbol, &exchange, bar_type.as_str(), bar_period);

        future_into_py(py, async move {
            let gw = gateway.read().await;

            match bar_type {
                ParsedBarType::Time(bar_type) => gw
                    .subscribe_time_bars(&symbol, &exchange, bar_type, bar_period)
                    .await
                    .map_err(|e| to_pyruntime_err(format!("Live bar subscription failed: {e}")))?,
                ParsedBarType::Tick => {
                    let handle = gw.history_handle().ok_or_else(|| {
                        to_pyruntime_err("History plant not connected".to_string())
                    })?;

                    let response = handle
                        .subscribe_tick_bar_updates(
                            &symbol,
                            &exchange,
                            request_tick_bar_update::BarType::TickBar,
                            request_tick_bar_update::BarSubType::Regular,
                            &bar_period.to_string(),
                            request_tick_bar_update::Request::Subscribe,
                        )
                        .await
                        .map_err(|e| {
                            to_pyruntime_err(format!("Live bar subscription failed: {e}"))
                        })?;

                    if let Some(e) = response.error {
                        return Err(to_pyruntime_err(format!(
                            "Live bar subscription failed: {e}"
                        )));
                    }
                }
            }

            bar_subscriptions.write().insert(key);
            Ok(())
        })
    }

    /// Subscribes to order-book deltas (depth-by-order updates) for an instrument.
    #[pyo3(name = "subscribe_book_deltas")]
    fn py_subscribe_book_deltas<'py>(
        &self,
        py: Python<'py>,
        symbol: String,
        exchange: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        Self::validate_symbol_exchange(&symbol, &exchange)?;

        let gateway = Arc::clone(&self.gateway);
        let book_subscriptions = Arc::clone(&self.book_subscriptions);
        let key = format!("{exchange}:{symbol}");

        future_into_py(py, async move {
            let already_subscribed = {
                let mut subscriptions = book_subscriptions.write();
                let entry = subscriptions.entry(key.clone()).or_default();

                if entry.deltas {
                    return Ok(());
                }
                entry.deltas = true;
                false
            };

            if already_subscribed {
                return Ok(());
            }

            let gw = gateway.read().await;
            gw.subscribe_order_book_bootstrapped(&symbol, &exchange)
                .await
                .map_err(|e| to_pyruntime_err(format!("Book delta subscription failed: {e}")))?;
            Ok(())
        })
    }

    /// Subscribes to top-10 order-book depth updates for an instrument.
    #[pyo3(name = "subscribe_book_depth10")]
    fn py_subscribe_book_depth10<'py>(
        &self,
        py: Python<'py>,
        symbol: String,
        exchange: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        Self::validate_symbol_exchange(&symbol, &exchange)?;

        let gateway = Arc::clone(&self.gateway);
        let book_subscriptions = Arc::clone(&self.book_subscriptions);
        let key = format!("{exchange}:{symbol}");

        future_into_py(py, async move {
            {
                let mut subscriptions = book_subscriptions.write();
                let entry = subscriptions.entry(key).or_default();

                if entry.depth10 {
                    return Ok(());
                }
                entry.depth10 = true;
            }

            let gw = gateway.read().await;
            gw.subscribe_order_book_depth(&symbol, &exchange)
                .await
                .map_err(|e| to_pyruntime_err(format!("Book depth10 subscription failed: {e}")))?;
            Ok(())
        })
    }

    /// Unsubscribes from market data for an instrument.
    ///
    /// This is an async method.
    #[pyo3(name = "unsubscribe")]
    fn py_unsubscribe<'py>(
        &self,
        py: Python<'py>,
        symbol: String,
        exchange: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        // Validate inputs
        Self::validate_symbol_exchange(&symbol, &exchange)?;

        let gateway = Arc::clone(&self.gateway);
        let subscriptions = Arc::clone(&self.subscriptions);
        let key = format!("{exchange}:{symbol}");

        future_into_py(py, async move {
            let gw = gateway.read().await;
            gw.unsubscribe_market_data(&symbol, &exchange)
                .await
                .map(|()| {
                    // Only remove from tracking on success
                    subscriptions.write().remove(&key);
                })
                .map_err(|e| to_pyruntime_err(format!("Unsubscribe failed: {e}")))
        })
    }

    /// Unsubscribes from venue instrument-status updates for an instrument.
    #[pyo3(name = "unsubscribe_instrument_status")]
    fn py_unsubscribe_instrument_status<'py>(
        &self,
        py: Python<'py>,
        symbol: String,
        exchange: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        Self::validate_symbol_exchange(&symbol, &exchange)?;

        let gateway = Arc::clone(&self.gateway);
        let status_subscriptions = Arc::clone(&self.status_subscriptions);
        let key = format!("{exchange}:{symbol}");

        future_into_py(py, async move {
            if !status_subscriptions.write().remove(&key) {
                return Ok(());
            }

            let gw = gateway.read().await;
            gw.unsubscribe_instrument_status(&symbol, &exchange)
                .await
                .map_err(|e| to_pyruntime_err(format!("Instrument status unsubscribe failed: {e}")))
        })
    }

    /// Unsubscribes from trade statistics updates for an instrument.
    #[pyo3(name = "unsubscribe_trade_statistics")]
    fn py_unsubscribe_trade_statistics<'py>(
        &self,
        py: Python<'py>,
        symbol: String,
        exchange: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        Self::subscribe_extra_market_data(
            &self.gateway,
            &self.extra_market_data_subscriptions,
            py,
            ExtraMarketDataSubscriptionCmd {
                symbol,
                exchange,
                kind: ExtraMarketDataKind::TradeStatistics,
                enabled: false,
                error_prefix: "Trade statistics unsubscribe failed",
            },
        )
    }

    /// Unsubscribes from quote statistics updates for an instrument.
    #[pyo3(name = "unsubscribe_quote_statistics")]
    fn py_unsubscribe_quote_statistics<'py>(
        &self,
        py: Python<'py>,
        symbol: String,
        exchange: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        Self::subscribe_extra_market_data(
            &self.gateway,
            &self.extra_market_data_subscriptions,
            py,
            ExtraMarketDataSubscriptionCmd {
                symbol,
                exchange,
                kind: ExtraMarketDataKind::QuoteStatistics,
                enabled: false,
                error_prefix: "Quote statistics unsubscribe failed",
            },
        )
    }

    /// Unsubscribes from indicator price updates for an instrument.
    #[pyo3(name = "unsubscribe_indicator_prices")]
    fn py_unsubscribe_indicator_prices<'py>(
        &self,
        py: Python<'py>,
        symbol: String,
        exchange: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        Self::subscribe_extra_market_data(
            &self.gateway,
            &self.extra_market_data_subscriptions,
            py,
            ExtraMarketDataSubscriptionCmd {
                symbol,
                exchange,
                kind: ExtraMarketDataKind::IndicatorPrices,
                enabled: false,
                error_prefix: "Indicator prices unsubscribe failed",
            },
        )
    }

    /// Unsubscribes from open-interest updates for an instrument.
    #[pyo3(name = "unsubscribe_open_interest")]
    fn py_unsubscribe_open_interest<'py>(
        &self,
        py: Python<'py>,
        symbol: String,
        exchange: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        Self::subscribe_extra_market_data(
            &self.gateway,
            &self.extra_market_data_subscriptions,
            py,
            ExtraMarketDataSubscriptionCmd {
                symbol,
                exchange,
                kind: ExtraMarketDataKind::OpenInterest,
                enabled: false,
                error_prefix: "Open interest unsubscribe failed",
            },
        )
    }

    /// Unsubscribes from end-of-day price updates for an instrument.
    #[pyo3(name = "unsubscribe_end_of_day_prices")]
    fn py_unsubscribe_end_of_day_prices<'py>(
        &self,
        py: Python<'py>,
        symbol: String,
        exchange: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        Self::subscribe_extra_market_data(
            &self.gateway,
            &self.extra_market_data_subscriptions,
            py,
            ExtraMarketDataSubscriptionCmd {
                symbol,
                exchange,
                kind: ExtraMarketDataKind::EndOfDayPrices,
                enabled: false,
                error_prefix: "End-of-day prices unsubscribe failed",
            },
        )
    }

    /// Unsubscribes from order price limit updates for an instrument.
    #[pyo3(name = "unsubscribe_order_price_limits")]
    fn py_unsubscribe_order_price_limits<'py>(
        &self,
        py: Python<'py>,
        symbol: String,
        exchange: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        Self::subscribe_extra_market_data(
            &self.gateway,
            &self.extra_market_data_subscriptions,
            py,
            ExtraMarketDataSubscriptionCmd {
                symbol,
                exchange,
                kind: ExtraMarketDataKind::OrderPriceLimits,
                enabled: false,
                error_prefix: "Order price limits unsubscribe failed",
            },
        )
    }

    /// Unsubscribes from symbol margin-rate updates for an instrument.
    #[pyo3(name = "unsubscribe_symbol_margin_rate")]
    fn py_unsubscribe_symbol_margin_rate<'py>(
        &self,
        py: Python<'py>,
        symbol: String,
        exchange: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        Self::subscribe_extra_market_data(
            &self.gateway,
            &self.extra_market_data_subscriptions,
            py,
            ExtraMarketDataSubscriptionCmd {
                symbol,
                exchange,
                kind: ExtraMarketDataKind::SymbolMarginRate,
                enabled: false,
                error_prefix: "Symbol margin rate unsubscribe failed",
            },
        )
    }

    /// Unsubscribes from order-book deltas for an instrument.
    #[pyo3(name = "unsubscribe_book_deltas")]
    fn py_unsubscribe_book_deltas<'py>(
        &self,
        py: Python<'py>,
        symbol: String,
        exchange: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        Self::validate_symbol_exchange(&symbol, &exchange)?;

        let gateway = Arc::clone(&self.gateway);
        let book_subscriptions = Arc::clone(&self.book_subscriptions);
        let key = format!("{exchange}:{symbol}");

        future_into_py(py, async move {
            let should_unsubscribe = {
                let mut subscriptions = book_subscriptions.write();

                if let Some(entry) = subscriptions.get_mut(&key) {
                    entry.deltas = false;

                    if !entry.depth10 {
                        subscriptions.remove(&key);
                    }
                    true
                } else {
                    false
                }
            };

            if !should_unsubscribe {
                return Ok(());
            }

            let gw = gateway.read().await;
            gw.unsubscribe_order_book(&symbol, &exchange)
                .await
                .map_err(|e| to_pyruntime_err(format!("Book delta unsubscribe failed: {e}")))?;
            Ok(())
        })
    }

    /// Unsubscribes from top-10 order-book depth updates for an instrument.
    #[pyo3(name = "unsubscribe_book_depth10")]
    fn py_unsubscribe_book_depth10<'py>(
        &self,
        py: Python<'py>,
        symbol: String,
        exchange: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        Self::validate_symbol_exchange(&symbol, &exchange)?;

        let gateway = Arc::clone(&self.gateway);
        let book_subscriptions = Arc::clone(&self.book_subscriptions);
        let key = format!("{exchange}:{symbol}");

        future_into_py(py, async move {
            let should_unsubscribe = {
                let mut subscriptions = book_subscriptions.write();

                if let Some(entry) = subscriptions.get_mut(&key) {
                    entry.depth10 = false;
                    let should = !entry.deltas;

                    if should {
                        subscriptions.remove(&key);
                    }
                    should
                } else {
                    false
                }
            };

            if !should_unsubscribe {
                return Ok(());
            }

            let gw = gateway.read().await;
            gw.unsubscribe_order_book_depth(&symbol, &exchange)
                .await
                .map_err(|e| to_pyruntime_err(format!("Book depth10 unsubscribe failed: {e}")))?;
            Ok(())
        })
    }

    /// Unsubscribes from live bars on the history plant.
    #[pyo3(name = "unsubscribe_bars")]
    fn py_unsubscribe_bars<'py>(
        &self,
        py: Python<'py>,
        symbol: String,
        exchange: String,
        bar_type: String,
        bar_period: i32,
    ) -> PyResult<Bound<'py, PyAny>> {
        Self::validate_symbol_exchange(&symbol, &exchange)?;
        let bar_type = Self::parse_bar_type(&bar_type)?;

        let gateway = Arc::clone(&self.gateway);
        let bar_subscriptions = Arc::clone(&self.bar_subscriptions);
        let key = Self::bar_subscription_key(&symbol, &exchange, bar_type.as_str(), bar_period);

        future_into_py(py, async move {
            let gw = gateway.read().await;

            match bar_type {
                ParsedBarType::Time(bar_type) => gw
                    .unsubscribe_time_bars(&symbol, &exchange, bar_type, bar_period)
                    .await
                    .map_err(|e| to_pyruntime_err(format!("Live bar unsubscribe failed: {e}")))?,
                ParsedBarType::Tick => {
                    let handle = gw.history_handle().ok_or_else(|| {
                        to_pyruntime_err("History plant not connected".to_string())
                    })?;

                    let response = handle
                        .subscribe_tick_bar_updates(
                            &symbol,
                            &exchange,
                            request_tick_bar_update::BarType::TickBar,
                            request_tick_bar_update::BarSubType::Regular,
                            &bar_period.to_string(),
                            request_tick_bar_update::Request::Unsubscribe,
                        )
                        .await
                        .map_err(|e| {
                            to_pyruntime_err(format!("Live bar unsubscribe failed: {e}"))
                        })?;

                    if let Some(e) = response.error {
                        return Err(to_pyruntime_err(format!(
                            "Live bar unsubscribe failed: {e}"
                        )));
                    }
                }
            }

            bar_subscriptions.write().remove(&key);
            Ok(())
        })
    }

    /// Replays all locally tracked subscriptions after a gateway reconnect.
    #[pyo3(name = "resubscribe_all")]
    fn py_resubscribe_all<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let gateway = Arc::clone(&self.gateway);
        let subscriptions = self.py_subscriptions();
        let bar_subscriptions = self.py_bar_subscriptions();
        let status_subscriptions: Vec<String> =
            self.status_subscriptions.read().iter().cloned().collect();
        let book_subscriptions = self.book_subscriptions.read().clone();
        let extra_market_data_subscriptions = self.extra_market_data_subscriptions.read().clone();

        future_into_py(py, async move {
            for key in subscriptions {
                let (exchange, symbol) = Self::parse_market_subscription_key(&key)?;
                let gw = gateway.read().await;
                gw.subscribe_market_data(&symbol, &exchange)
                    .await
                    .map_err(|e| to_pyruntime_err(format!("Resubscribe failed: {e}")))?;
            }

            for key in bar_subscriptions {
                let (exchange, symbol, bar_type, bar_period) =
                    Self::parse_bar_subscription_key(&key)?;
                let gw = gateway.read().await;

                match bar_type {
                    ParsedBarType::Time(bar_type) => gw
                        .subscribe_time_bars(&symbol, &exchange, bar_type, bar_period)
                        .await
                        .map_err(|e| to_pyruntime_err(format!("Bar resubscribe failed: {e}")))?,
                    ParsedBarType::Tick => {
                        let handle = gw.history_handle().ok_or_else(|| {
                            to_pyruntime_err("History plant not connected".to_string())
                        })?;

                        let response = handle
                            .subscribe_tick_bar_updates(
                                &symbol,
                                &exchange,
                                request_tick_bar_update::BarType::TickBar,
                                request_tick_bar_update::BarSubType::Regular,
                                &bar_period.to_string(),
                                request_tick_bar_update::Request::Subscribe,
                            )
                            .await
                            .map_err(|e| {
                                to_pyruntime_err(format!("Bar resubscribe failed: {e}"))
                            })?;

                        if let Some(e) = response.error {
                            return Err(to_pyruntime_err(format!("Bar resubscribe failed: {e}")));
                        }
                    }
                }
            }

            for key in status_subscriptions {
                let (exchange, symbol) = Self::parse_market_subscription_key(&key)?;
                let gw = gateway.read().await;
                gw.subscribe_instrument_status(&symbol, &exchange)
                    .await
                    .map_err(|e| {
                        to_pyruntime_err(format!("Instrument status resubscribe failed: {e}"))
                    })?;
            }

            for (key, subscription) in book_subscriptions {
                let (exchange, symbol) = Self::parse_market_subscription_key(&key)?;
                let gw = gateway.read().await;

                if subscription.deltas {
                    gw.subscribe_order_book_bootstrapped(&symbol, &exchange)
                        .await
                        .map_err(|e| {
                            to_pyruntime_err(format!("Book delta resubscribe failed: {e}"))
                        })?;
                }

                if subscription.depth10 {
                    gw.subscribe_order_book_depth(&symbol, &exchange)
                        .await
                        .map_err(|e| {
                            to_pyruntime_err(format!("Book depth10 resubscribe failed: {e}"))
                        })?;
                }
            }

            for (key, subscription) in extra_market_data_subscriptions {
                let (exchange, symbol) = Self::parse_market_subscription_key(&key)?;

                for kind in extra_market_data_kinds(&subscription) {
                    Self::apply_extra_market_data_subscription(
                        &gateway, &symbol, &exchange, kind, true,
                    )
                    .await
                    .map_err(|e| {
                        to_pyruntime_err(format!("Custom market-data resubscribe failed: {e}"))
                    })?;
                }
            }

            Ok(())
        })
    }

    /// Unsubscribes from all market data (local tracking only).
    #[pyo3(name = "unsubscribe_all")]
    fn py_unsubscribe_all(&self) {
        self.subscriptions.write().clear();
        self.bar_subscriptions.write().clear();
        self.status_subscriptions.write().clear();
        self.book_subscriptions.write().clear();
        self.extra_market_data_subscriptions.write().clear();
    }

    /// Requests historical trade ticks via 1-tick bar replay on the history plant.
    ///
    /// This is an async method - use `await client.request_trade_ticks(...)`.
    #[pyo3(signature = (symbol, exchange, start_time_sec, end_time_sec))]
    #[pyo3(name = "request_trade_ticks")]
    fn py_request_trade_ticks<'py>(
        &self,
        py: Python<'py>,
        symbol: String,
        exchange: String,
        start_time_sec: i32,
        end_time_sec: i32,
    ) -> PyResult<Bound<'py, PyAny>> {
        Self::validate_symbol_exchange(&symbol, &exchange)?;

        let gateway = Arc::clone(&self.gateway);

        future_into_py(py, async move {
            let gw = gateway.read().await;
            let handle = gw
                .history_handle()
                .ok_or_else(|| to_pyruntime_err("History plant not connected".to_string()))?;

            let responses = handle
                .load_ticks(symbol, exchange, start_time_sec, end_time_sec)
                .await
                .map_err(|e| to_pyruntime_err(e.to_string()))?;

            let mut ticks = Vec::with_capacity(responses.len());

            for (sequence, response) in responses.into_iter().enumerate() {
                if let Some(e) = &response.error {
                    return Err(to_pyruntime_err(e.clone()));
                }

                let RithmicMessage::ResponseTickBarReplay(tick) = response.message else {
                    continue;
                };

                if let Some(tick) = PyTradeTick::from_tick_replay(&tick, sequence) {
                    ticks.push(tick);
                }
            }

            Ok(ticks)
        })
    }

    /// Requests historical bars via the history plant.
    ///
    /// This is an async method - use `await client.request_bars(...)`.
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (symbol, exchange, bar_type, bar_period, start_time_sec, end_time_sec))]
    #[pyo3(name = "request_bars")]
    fn py_request_bars<'py>(
        &self,
        py: Python<'py>,
        symbol: String,
        exchange: String,
        bar_type: String,
        bar_period: i32,
        start_time_sec: i32,
        end_time_sec: i32,
    ) -> PyResult<Bound<'py, PyAny>> {
        // Validate inputs
        Self::validate_symbol_exchange(&symbol, &exchange)?;

        let gateway = Arc::clone(&self.gateway);

        future_into_py(py, async move {
            let bar_type = Self::parse_bar_type(&bar_type)?;
            let gw = gateway.read().await;
            let responses = match bar_type {
                ParsedBarType::Time(bar_type) => gw
                    .request_bars(
                        &symbol,
                        &exchange,
                        bar_type,
                        bar_period,
                        start_time_sec,
                        end_time_sec,
                    )
                    .await
                    .map_err(|e| to_pyruntime_err(e.to_string()))?,
                ParsedBarType::Tick => {
                    let handle = gw.history_handle().ok_or_else(|| {
                        to_pyruntime_err("History plant not connected".to_string())
                    })?;

                    handle
                        .load_tick_bars(
                            symbol.clone(),
                            exchange.clone(),
                            bar_period as u32,
                            start_time_sec,
                            end_time_sec,
                        )
                        .await
                        .map_err(|e| to_pyruntime_err(e.to_string()))?
                }
            };

            let mut bars = Vec::with_capacity(responses.len());

            for response in responses {
                if let Some(e) = &response.error {
                    return Err(to_pyruntime_err(e.clone()));
                }

                match response.message {
                    RithmicMessage::ResponseTimeBarReplay(bar) => {
                        bars.push(PyTimeBar::from_time_response(&bar));
                    }
                    RithmicMessage::TimeBar(bar) => {
                        if let Some(bar) = PyTimeBar::from_live_time_update(&bar) {
                            bars.push(bar);
                        }
                    }
                    RithmicMessage::ResponseTickBarReplay(bar) => {
                        let tick_period = bar
                            .type_specifier
                            .as_deref()
                            .and_then(|value| value.parse::<i32>().ok())
                            .unwrap_or(1);

                        if tick_period != bar_period {
                            continue;
                        }
                        bars.push(PyTimeBar::from_tick_response(&bar));
                    }
                    _ => {}
                }
            }

            Ok(bars)
        })
    }

    /// Requests an order-book depth10 snapshot for an instrument.
    #[pyo3(name = "request_book_snapshot")]
    fn py_request_book_snapshot<'py>(
        &self,
        py: Python<'py>,
        symbol: String,
        exchange: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        Self::validate_symbol_exchange(&symbol, &exchange)?;

        let gateway = Arc::clone(&self.gateway);

        future_into_py(py, async move {
            let now = get_atomic_clock_realtime().get_time_ns();
            let gw = gateway.read().await;
            let responses = gw
                .request_order_book_snapshot(&symbol, &exchange)
                .await
                .map_err(|e| to_pyruntime_err(format!("Book snapshot request failed: {e}")))?;

            let (price_precision, size_precision) = {
                let key = format!("{exchange}:{symbol}");
                gw.instruments()
                    .try_read()
                    .ok()
                    .and_then(|map| map.get(&key).cloned())
                    .map_or((2, 0), |info| {
                        (info.tick_size.map_or(2, tick_size_to_precision), 0)
                    })
            };

            let _ = exchange;
            let instrument_id = InstrumentId::from(format!("{symbol}.RITHMIC").as_str());

            let book = order_book_from_snapshot(
                instrument_id,
                &responses,
                price_precision,
                size_precision,
                now,
            );

            Ok(depth10_from_order_book(&book, now, now))
        })
    }

    #[pyo3(name = "__repr__")]
    fn py_repr(&self) -> String {
        format!(
            "RithmicDataClient(connected={}, subscriptions={}, bar_subscriptions={}, book_subscriptions={})",
            self.py_is_connected(),
            self.py_subscription_count(),
            self.py_bar_subscription_count(),
            self.py_book_subscription_count(),
        )
    }
}

struct ExtraMarketDataSubscriptionCmd {
    symbol: String,
    exchange: String,
    kind: ExtraMarketDataKind,
    enabled: bool,
    error_prefix: &'static str,
}

impl PyRithmicDataClient {
    async fn subscribe_market_data_alias(
        gateway: &Arc<tokio::sync::RwLock<RithmicGateway>>,
        subscriptions: &Arc<parking_lot::RwLock<AHashSet<String>>>,
        symbol: String,
        exchange: String,
    ) -> PyResult<()> {
        let key = format!("{exchange}:{symbol}");

        {
            let mut subscriptions = subscriptions.write();
            if !subscriptions.insert(key.clone()) {
                return Ok(());
            }
        }

        let gateway = gateway.read().await;
        if let Err(e) = gateway.subscribe_market_data(&symbol, &exchange).await {
            subscriptions.write().remove(&key);
            return Err(to_pyruntime_err(format!("Subscription failed: {e}")));
        }

        Ok(())
    }

    fn subscribe_extra_market_data<'py>(
        gateway: &Arc<tokio::sync::RwLock<RithmicGateway>>,
        subscriptions: &Arc<parking_lot::RwLock<AHashMap<String, ExtraMarketDataSubscription>>>,
        py: Python<'py>,
        cmd: ExtraMarketDataSubscriptionCmd,
    ) -> PyResult<Bound<'py, PyAny>> {
        Self::validate_symbol_exchange(&cmd.symbol, &cmd.exchange)?;

        let gateway = Arc::clone(gateway);
        let subscriptions = Arc::clone(subscriptions);
        let key = format!("{}:{}", cmd.exchange, cmd.symbol);
        let symbol = cmd.symbol;
        let exchange = cmd.exchange;
        let kind = cmd.kind;
        let enabled = cmd.enabled;
        let error_prefix = cmd.error_prefix;

        future_into_py(py, async move {
            {
                let mut subscriptions = subscriptions.write();
                let changed = Self::update_extra_market_data_subscription_state(
                    &mut subscriptions,
                    &key,
                    kind,
                    enabled,
                );

                if !changed {
                    return Ok(());
                }
            }

            Self::apply_extra_market_data_subscription(&gateway, &symbol, &exchange, kind, enabled)
                .await
                .map_err(|e| to_pyruntime_err(format!("{error_prefix}: {e}")))
        })
    }

    fn update_extra_market_data_subscription_state(
        subscriptions: &mut AHashMap<String, ExtraMarketDataSubscription>,
        key: &str,
        kind: ExtraMarketDataKind,
        enabled: bool,
    ) -> bool {
        match subscriptions.entry(key.to_string()) {
            std::collections::hash_map::Entry::Occupied(mut entry) => {
                if get_extra_market_data_flag(entry.get(), kind) == enabled {
                    return false;
                }

                set_extra_market_data_flag(entry.get_mut(), kind, enabled);

                if extra_market_data_kinds(entry.get()).is_empty() {
                    entry.remove();
                }
            }
            std::collections::hash_map::Entry::Vacant(entry) => {
                if !enabled {
                    return false;
                }

                let mut subscription = ExtraMarketDataSubscription::default();
                set_extra_market_data_flag(&mut subscription, kind, true);
                entry.insert(subscription);
            }
        }

        true
    }

    async fn apply_extra_market_data_subscription(
        gateway: &Arc<tokio::sync::RwLock<RithmicGateway>>,
        symbol: &str,
        exchange: &str,
        kind: ExtraMarketDataKind,
        enabled: bool,
    ) -> crate::Result<()> {
        let gateway = gateway.read().await;

        match (kind, enabled) {
            (ExtraMarketDataKind::TradeStatistics, true) => {
                gateway.subscribe_trade_statistics(symbol, exchange).await
            }
            (ExtraMarketDataKind::TradeStatistics, false) => {
                gateway.unsubscribe_trade_statistics(symbol, exchange).await
            }
            (ExtraMarketDataKind::QuoteStatistics, true) => {
                gateway.subscribe_quote_statistics(symbol, exchange).await
            }
            (ExtraMarketDataKind::QuoteStatistics, false) => {
                gateway.unsubscribe_quote_statistics(symbol, exchange).await
            }
            (ExtraMarketDataKind::IndicatorPrices, true) => {
                gateway.subscribe_indicator_prices(symbol, exchange).await
            }
            (ExtraMarketDataKind::IndicatorPrices, false) => {
                gateway.unsubscribe_indicator_prices(symbol, exchange).await
            }
            (ExtraMarketDataKind::OpenInterest, true) => {
                gateway.subscribe_open_interest(symbol, exchange).await
            }
            (ExtraMarketDataKind::OpenInterest, false) => {
                gateway.unsubscribe_open_interest(symbol, exchange).await
            }
            (ExtraMarketDataKind::EndOfDayPrices, true) => {
                gateway.subscribe_end_of_day_prices(symbol, exchange).await
            }
            (ExtraMarketDataKind::EndOfDayPrices, false) => {
                gateway
                    .unsubscribe_end_of_day_prices(symbol, exchange)
                    .await
            }
            (ExtraMarketDataKind::OrderPriceLimits, true) => {
                gateway.subscribe_order_price_limits(symbol, exchange).await
            }
            (ExtraMarketDataKind::OrderPriceLimits, false) => {
                gateway
                    .unsubscribe_order_price_limits(symbol, exchange)
                    .await
            }
            (ExtraMarketDataKind::SymbolMarginRate, true) => {
                gateway.subscribe_symbol_margin_rate(symbol, exchange).await
            }
            (ExtraMarketDataKind::SymbolMarginRate, false) => {
                gateway
                    .unsubscribe_symbol_margin_rate(symbol, exchange)
                    .await
            }
        }
    }

    /// Validates symbol and exchange are non-empty.
    fn validate_symbol_exchange(symbol: &str, exchange: &str) -> PyResult<()> {
        if symbol.trim().is_empty() {
            return Err(to_pyvalue_err("symbol cannot be empty"));
        }

        if exchange.trim().is_empty() {
            return Err(to_pyvalue_err("exchange cannot be empty"));
        }
        Ok(())
    }

    fn parse_bar_type(bar_type: &str) -> PyResult<ParsedBarType> {
        match bar_type {
            "SecondBar" => Ok(ParsedBarType::Time(TimeBarType::SecondBar)),
            "MinuteBar" => Ok(ParsedBarType::Time(TimeBarType::MinuteBar)),
            "DailyBar" => Ok(ParsedBarType::Time(TimeBarType::DailyBar)),
            "WeeklyBar" => Ok(ParsedBarType::Time(TimeBarType::WeeklyBar)),
            "TickBar" => Ok(ParsedBarType::Tick),
            _ => Err(to_pyvalue_err(
                "Unsupported bar type. Valid values: SecondBar, MinuteBar, DailyBar, WeeklyBar, TickBar",
            )),
        }
    }

    fn bar_subscription_key(
        symbol: &str,
        exchange: &str,
        bar_type: &str,
        bar_period: i32,
    ) -> String {
        format!("{exchange}:{symbol}:{bar_type}:{bar_period}")
    }

    fn parse_market_subscription_key(key: &str) -> PyResult<(String, String)> {
        let mut parts = key.splitn(2, ':');
        let exchange = parts
            .next()
            .ok_or_else(|| to_pyvalue_err(format!("Invalid subscription key: {key}")))?;
        let symbol = parts
            .next()
            .ok_or_else(|| to_pyvalue_err(format!("Invalid subscription key: {key}")))?;
        Ok((exchange.to_string(), symbol.to_string()))
    }

    fn parse_bar_subscription_key(key: &str) -> PyResult<(String, String, ParsedBarType, i32)> {
        let mut parts = key.splitn(4, ':');
        let exchange = parts
            .next()
            .ok_or_else(|| to_pyvalue_err(format!("Invalid bar subscription key: {key}")))?;
        let symbol = parts
            .next()
            .ok_or_else(|| to_pyvalue_err(format!("Invalid bar subscription key: {key}")))?;
        let bar_type = parts
            .next()
            .ok_or_else(|| to_pyvalue_err(format!("Invalid bar subscription key: {key}")))?;
        let bar_period = parts
            .next()
            .ok_or_else(|| to_pyvalue_err(format!("Invalid bar subscription key: {key}")))?
            .parse::<i32>()
            .map_err(|_| to_pyvalue_err(format!("Invalid bar period in key: {key}")))?;
        let bar_type = Self::parse_bar_type(bar_type)?;
        Ok((
            exchange.to_string(),
            symbol.to_string(),
            bar_type,
            bar_period,
        ))
    }

    /// Event processing loop that runs in a spawned task.
    ///
    /// This is separated out to make the async flow clearer and testable.
    async fn event_loop(
        mut rx: tokio::sync::broadcast::Receiver<MarketDataEvent>,
        mut rx_shutdown: tokio::sync::oneshot::Receiver<()>,
        callback: Arc<parking_lot::Mutex<Option<Py<PyAny>>>>,
    ) {
        loop {
            tokio::select! {
                _ = &mut rx_shutdown => {
                    tracing::debug!("Market data event loop received shutdown signal");
                    break;
                }
                event = rx.recv() => {
                    match event {
                        Ok(event) => {
                            // Acquire GIL and dispatch event
                            // Note: Python::attach is blocking but safe here since
                            // we don't hold any Rust locks while waiting for GIL
                            pyo3::Python::attach(|py| {
                                let cb = {
                                    let guard = callback.lock();
                                    guard.as_ref().map(|cb| cb.clone_ref(py))
                                };

                                if let Some(cb) = cb {
                                    let py_event = PyMarketDataEvent::from(event);

                                    if let Err(e) = cb.call1(py, (py_event,)) {
                                        tracing::error!("Error in Python data callback: {e}");
                                    }
                                }
                            });
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                            tracing::warn!("Market data subscriber lagged by {skipped} events");
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                            tracing::debug!("Market data channel closed");
                            break;
                        }
                    }
                }
            }
        }
    }
}

fn set_extra_market_data_flag(
    subscription: &mut ExtraMarketDataSubscription,
    kind: ExtraMarketDataKind,
    value: bool,
) {
    match kind {
        ExtraMarketDataKind::TradeStatistics => subscription.trade_statistics = value,
        ExtraMarketDataKind::QuoteStatistics => subscription.quote_statistics = value,
        ExtraMarketDataKind::IndicatorPrices => subscription.indicator_prices = value,
        ExtraMarketDataKind::OpenInterest => subscription.open_interest = value,
        ExtraMarketDataKind::EndOfDayPrices => subscription.end_of_day_prices = value,
        ExtraMarketDataKind::OrderPriceLimits => subscription.order_price_limits = value,
        ExtraMarketDataKind::SymbolMarginRate => subscription.symbol_margin_rate = value,
    }
}

fn get_extra_market_data_flag(
    subscription: &ExtraMarketDataSubscription,
    kind: ExtraMarketDataKind,
) -> bool {
    match kind {
        ExtraMarketDataKind::TradeStatistics => subscription.trade_statistics,
        ExtraMarketDataKind::QuoteStatistics => subscription.quote_statistics,
        ExtraMarketDataKind::IndicatorPrices => subscription.indicator_prices,
        ExtraMarketDataKind::OpenInterest => subscription.open_interest,
        ExtraMarketDataKind::EndOfDayPrices => subscription.end_of_day_prices,
        ExtraMarketDataKind::OrderPriceLimits => subscription.order_price_limits,
        ExtraMarketDataKind::SymbolMarginRate => subscription.symbol_margin_rate,
    }
}

fn extra_market_data_kinds(subscription: &ExtraMarketDataSubscription) -> Vec<ExtraMarketDataKind> {
    let mut kinds = Vec::new();

    if subscription.trade_statistics {
        kinds.push(ExtraMarketDataKind::TradeStatistics);
    }

    if subscription.quote_statistics {
        kinds.push(ExtraMarketDataKind::QuoteStatistics);
    }

    if subscription.indicator_prices {
        kinds.push(ExtraMarketDataKind::IndicatorPrices);
    }

    if subscription.open_interest {
        kinds.push(ExtraMarketDataKind::OpenInterest);
    }

    if subscription.end_of_day_prices {
        kinds.push(ExtraMarketDataKind::EndOfDayPrices);
    }

    if subscription.order_price_limits {
        kinds.push(ExtraMarketDataKind::OrderPriceLimits);
    }

    if subscription.symbol_margin_rate {
        kinds.push(ExtraMarketDataKind::SymbolMarginRate);
    }

    kinds
}

#[cfg(feature = "python")]
impl Drop for PyRithmicDataClient {
    fn drop(&mut self) {
        self.event_running.store(false, Ordering::SeqCst);

        if let Some(tx) = self.shutdown_tx.lock().take() {
            let _ = tx.send(());
        }

        if let Some(handle) = self.event_task.lock().take() {
            handle.abort();
        }
    }
}

/// Registers data client types with the Python module.
#[cfg(feature = "python")]
pub(crate) fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyRithmicDataClient>()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{GatewayConfig, RithmicEnv};

    fn test_gateway() -> Arc<tokio::sync::RwLock<RithmicGateway>> {
        Arc::new(tokio::sync::RwLock::new(RithmicGateway::new(
            GatewayConfig::new(
                RithmicEnv::Demo,
                "user",
                "pass",
                "system",
                "fcm",
                "ib",
                "account",
            )
            .with_app_name("TestApp"),
        )))
    }

    #[tokio::test]
    async fn subscribe_market_data_alias_duplicate_is_idempotent() {
        Python::initialize();

        let gateway = test_gateway();
        let subscriptions = Arc::new(parking_lot::RwLock::new(AHashSet::from_iter([
            "CME:ESM6".to_string()
        ])));

        PyRithmicDataClient::subscribe_market_data_alias(
            &gateway,
            &subscriptions,
            "ESM6".to_string(),
            "CME".to_string(),
        )
        .await
        .unwrap();

        let subscriptions = subscriptions.read();
        assert_eq!(subscriptions.len(), 1);
        assert!(subscriptions.contains("CME:ESM6"));
    }

    #[tokio::test]
    async fn subscribe_market_data_alias_rolls_back_tracking_on_error() {
        Python::initialize();

        let gateway = test_gateway();
        let subscriptions = Arc::new(parking_lot::RwLock::new(AHashSet::new()));

        let err = PyRithmicDataClient::subscribe_market_data_alias(
            &gateway,
            &subscriptions,
            "ESM6".to_string(),
            "CME".to_string(),
        )
        .await
        .unwrap_err();

        assert!(err.to_string().contains("Subscription failed"));
        assert!(subscriptions.read().is_empty());
    }
}
