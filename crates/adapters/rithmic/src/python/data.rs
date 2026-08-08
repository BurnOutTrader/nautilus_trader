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
    common::{converters::rithmic_instrument_id, parse::tick_size_to_precision},
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
    /// Serializes subscription state transitions across async gateway calls.
    subscription_update_lock: Arc<tokio::sync::Mutex<()>>,
    /// Python callback for market data events.
    data_callback: Arc<parking_lot::Mutex<Option<Py<PyAny>>>>,
    event_task: Arc<parking_lot::Mutex<Option<JoinHandle<()>>>>,
    event_running: Arc<AtomicBool>,
    shutdown_tx: Arc<parking_lot::Mutex<Option<tokio::sync::oneshot::Sender<()>>>>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct BookSubscription {
    deltas: bool,
    depth10: bool,
}

#[derive(Clone, Copy)]
enum BookSubscriptionKind {
    Deltas,
    Depth10,
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
            subscription_update_lock: Arc::new(tokio::sync::Mutex::new(())),
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

            let handle =
                get_runtime().spawn(Self::event_loop(rx, rx_shutdown, callback, event_running));

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
        let subscription_update_lock = Arc::clone(&self.subscription_update_lock);

        future_into_py(py, async move {
            Self::update_instrument_status_subscription(
                &gateway,
                &status_subscriptions,
                &subscription_update_lock,
                &symbol,
                &exchange,
                true,
            )
            .await
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
        Self::checked_bar_period(bar_period)?;

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
        let subscription_update_lock = Arc::clone(&self.subscription_update_lock);

        future_into_py(py, async move {
            Self::update_book_subscription(
                &gateway,
                &book_subscriptions,
                &subscription_update_lock,
                &symbol,
                &exchange,
                BookSubscriptionKind::Deltas,
                true,
            )
            .await
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
        let subscription_update_lock = Arc::clone(&self.subscription_update_lock);

        future_into_py(py, async move {
            Self::update_book_subscription(
                &gateway,
                &book_subscriptions,
                &subscription_update_lock,
                &symbol,
                &exchange,
                BookSubscriptionKind::Depth10,
                true,
            )
            .await
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
        let subscription_update_lock = Arc::clone(&self.subscription_update_lock);

        future_into_py(py, async move {
            Self::update_instrument_status_subscription(
                &gateway,
                &status_subscriptions,
                &subscription_update_lock,
                &symbol,
                &exchange,
                false,
            )
            .await
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
        let subscription_update_lock = Arc::clone(&self.subscription_update_lock);

        future_into_py(py, async move {
            Self::update_book_subscription(
                &gateway,
                &book_subscriptions,
                &subscription_update_lock,
                &symbol,
                &exchange,
                BookSubscriptionKind::Deltas,
                false,
            )
            .await
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
        let subscription_update_lock = Arc::clone(&self.subscription_update_lock);

        future_into_py(py, async move {
            Self::update_book_subscription(
                &gateway,
                &book_subscriptions,
                &subscription_update_lock,
                &symbol,
                &exchange,
                BookSubscriptionKind::Depth10,
                false,
            )
            .await
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
        Self::checked_bar_period(bar_period)?;

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
                if subscription.deltas || subscription.depth10 {
                    let gw = gateway.read().await;
                    gw.subscribe_order_book_bootstrapped(&symbol, &exchange)
                        .await
                        .map_err(|e| {
                            to_pyruntime_err(format!("Order-book resubscribe failed: {e}"))
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
        Self::validate_history_window(start_time_sec, end_time_sec)?;

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
        Self::validate_symbol_exchange(&symbol, &exchange)?;
        Self::validate_history_window(start_time_sec, end_time_sec)?;
        let bar_type = Self::parse_bar_type(&bar_type)?;
        let tick_period = Self::checked_bar_period(bar_period)?;

        let gateway = Arc::clone(&self.gateway);

        future_into_py(py, async move {
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
                            tick_period,
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
                        if let Some(bar) = PyTimeBar::from_time_response(&bar) {
                            bars.push(bar);
                        }
                    }
                    RithmicMessage::TimeBar(bar) => {
                        if let Some(bar) = PyTimeBar::from_live_time_update(&bar) {
                            bars.push(bar);
                        }
                    }
                    RithmicMessage::ResponseTickBarReplay(bar) => {
                        let Some(tick_period) = bar
                            .type_specifier
                            .as_deref()
                            .and_then(|value| value.parse::<i32>().ok())
                            .filter(|value| *value > 0)
                        else {
                            continue;
                        };

                        if tick_period != bar_period {
                            continue;
                        }
                        if let Some(bar) = PyTimeBar::from_tick_response(&bar) {
                            bars.push(bar);
                        }
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
                let instruments = gw.instruments().try_read().map_err(|e| {
                    to_pyruntime_err(format!("Instrument cache is unavailable: {e}"))
                })?;
                let tick_size = instruments
                    .get(&key)
                    .and_then(|info| info.tick_size)
                    .ok_or_else(|| {
                        to_pyruntime_err(format!("Instrument precision is not available for {key}"))
                    })?;
                let price_precision = tick_size_to_precision(tick_size)
                    .map_err(|e| to_pyruntime_err(e.to_string()))?;
                (price_precision, 0)
            };

            let instrument_id = rithmic_instrument_id(&symbol, &exchange)
                .map_err(|e| to_pyvalue_err(e.to_string()))?;

            let book = order_book_from_snapshot(
                instrument_id,
                &responses,
                price_precision,
                size_precision,
                now,
            )
            .map_err(|e| to_pyruntime_err(format!("Invalid order-book snapshot: {e}")))?;

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
    async fn update_instrument_status_subscription(
        gateway: &Arc<tokio::sync::RwLock<RithmicGateway>>,
        subscriptions: &Arc<parking_lot::RwLock<AHashSet<String>>>,
        update_lock: &Arc<tokio::sync::Mutex<()>>,
        symbol: &str,
        exchange: &str,
        enabled: bool,
    ) -> PyResult<()> {
        let _update = update_lock.lock().await;
        let key = format!("{exchange}:{symbol}");
        if subscriptions.read().contains(&key) == enabled {
            return Ok(());
        }

        let gateway = gateway.read().await;
        let result = if enabled {
            gateway.subscribe_instrument_status(symbol, exchange).await
        } else {
            gateway
                .unsubscribe_instrument_status(symbol, exchange)
                .await
        };
        result.map_err(|e| {
            let operation = if enabled {
                "subscription"
            } else {
                "unsubscribe"
            };
            to_pyruntime_err(format!("Instrument status {operation} failed: {e}"))
        })?;

        if enabled {
            subscriptions.write().insert(key);
        } else {
            subscriptions.write().remove(&key);
        }
        Ok(())
    }

    async fn update_book_subscription(
        gateway: &Arc<tokio::sync::RwLock<RithmicGateway>>,
        subscriptions: &Arc<parking_lot::RwLock<AHashMap<String, BookSubscription>>>,
        update_lock: &Arc<tokio::sync::Mutex<()>>,
        symbol: &str,
        exchange: &str,
        kind: BookSubscriptionKind,
        enabled: bool,
    ) -> PyResult<()> {
        let _update = update_lock.lock().await;
        let key = format!("{exchange}:{symbol}");
        let current = subscriptions.read().get(&key).copied().unwrap_or_default();
        if get_book_subscription_flag(current, kind) == enabled {
            return Ok(());
        }

        let mut next = current;
        set_book_subscription_flag(&mut next, kind, enabled);
        let venue_was_subscribed = current.deltas || current.depth10;
        let venue_is_subscribed = next.deltas || next.depth10;

        if venue_was_subscribed != venue_is_subscribed {
            let gateway = gateway.read().await;
            let result = if venue_is_subscribed {
                gateway
                    .subscribe_order_book_bootstrapped(symbol, exchange)
                    .await
            } else {
                gateway.unsubscribe_order_book(symbol, exchange).await
            };
            result.map_err(|e| {
                let kind = match kind {
                    BookSubscriptionKind::Deltas => "delta",
                    BookSubscriptionKind::Depth10 => "depth10",
                };
                let operation = if enabled {
                    "subscription"
                } else {
                    "unsubscribe"
                };
                to_pyruntime_err(format!("Book {kind} {operation} failed: {e}"))
            })?;
        }

        if venue_is_subscribed {
            subscriptions.write().insert(key, next);
        } else {
            subscriptions.write().remove(&key);
        }
        Ok(())
    }

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
            let previous = {
                let mut subscriptions = subscriptions.write();
                let previous = subscriptions.get(&key).copied();
                let changed = Self::update_extra_market_data_subscription_state(
                    &mut subscriptions,
                    &key,
                    kind,
                    enabled,
                );

                if !changed {
                    return Ok(());
                }
                previous
            };

            if let Err(e) = Self::apply_extra_market_data_subscription(
                &gateway, &symbol, &exchange, kind, enabled,
            )
            .await
            {
                Self::restore_extra_market_data_subscription_state(
                    &mut subscriptions.write(),
                    &key,
                    previous,
                );
                return Err(to_pyruntime_err(format!("{error_prefix}: {e}")));
            }

            Ok(())
        })
    }

    fn restore_extra_market_data_subscription_state(
        subscriptions: &mut AHashMap<String, ExtraMarketDataSubscription>,
        key: &str,
        previous: Option<ExtraMarketDataSubscription>,
    ) {
        if let Some(previous) = previous {
            subscriptions.insert(key.to_string(), previous);
        } else {
            subscriptions.remove(key);
        }
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

    fn checked_bar_period(bar_period: i32) -> PyResult<u32> {
        if bar_period <= 0 {
            return Err(to_pyvalue_err("bar_period must be positive"));
        }

        u32::try_from(bar_period)
            .map_err(|e| to_pyvalue_err(format!("bar_period is out of range: {e}")))
    }

    fn validate_history_window(start_time_sec: i32, end_time_sec: i32) -> PyResult<()> {
        if start_time_sec < 0 || end_time_sec < 0 {
            return Err(to_pyvalue_err(
                "start_time_sec and end_time_sec cannot be negative",
            ));
        }

        if end_time_sec != 0 && end_time_sec < start_time_sec {
            return Err(to_pyvalue_err(
                "end_time_sec must be zero or greater than or equal to start_time_sec",
            ));
        }
        Ok(())
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
        Self::checked_bar_period(bar_period)?;
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
        event_running: Arc<AtomicBool>,
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
        event_running.store(false, Ordering::SeqCst);
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

fn get_book_subscription_flag(subscription: BookSubscription, kind: BookSubscriptionKind) -> bool {
    match kind {
        BookSubscriptionKind::Deltas => subscription.deltas,
        BookSubscriptionKind::Depth10 => subscription.depth10,
    }
}

fn set_book_subscription_flag(
    subscription: &mut BookSubscription,
    kind: BookSubscriptionKind,
    value: bool,
) {
    match kind {
        BookSubscriptionKind::Deltas => subscription.deltas = value,
        BookSubscriptionKind::Depth10 => subscription.depth10 = value,
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
                "TestApp",
                "fcm",
                "ib",
                "account",
            )
            .expect("valid test gateway configuration"),
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

    #[test]
    fn book_snapshot_instrument_id_is_exchange_qualified() {
        let instrument_id = rithmic_instrument_id("MNQM6", "CME").unwrap();
        assert_eq!(instrument_id.to_string(), "MNQM6.CME.RITHMIC");
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

    #[tokio::test]
    async fn instrument_status_failure_does_not_commit_subscription_state() {
        Python::initialize();

        let gateway = test_gateway();
        let subscriptions = Arc::new(parking_lot::RwLock::new(AHashSet::new()));
        let update_lock = Arc::new(tokio::sync::Mutex::new(()));

        let result = PyRithmicDataClient::update_instrument_status_subscription(
            &gateway,
            &subscriptions,
            &update_lock,
            "ESM6",
            "CME",
            true,
        )
        .await;

        assert!(result.is_err());
        assert!(subscriptions.read().is_empty());
    }

    #[tokio::test]
    async fn instrument_status_unsubscribe_failure_preserves_subscription_state() {
        Python::initialize();

        let gateway = test_gateway();
        let subscriptions = Arc::new(parking_lot::RwLock::new(AHashSet::from_iter([
            "CME:ESM6".to_string()
        ])));
        let update_lock = Arc::new(tokio::sync::Mutex::new(()));

        let result = PyRithmicDataClient::update_instrument_status_subscription(
            &gateway,
            &subscriptions,
            &update_lock,
            "ESM6",
            "CME",
            false,
        )
        .await;

        assert!(result.is_err());
        assert!(subscriptions.read().contains("CME:ESM6"));
    }

    #[tokio::test]
    async fn book_subscribe_failure_does_not_commit_subscription_state() {
        Python::initialize();

        let gateway = test_gateway();
        let subscriptions = Arc::new(parking_lot::RwLock::new(AHashMap::new()));
        let update_lock = Arc::new(tokio::sync::Mutex::new(()));

        let result = PyRithmicDataClient::update_book_subscription(
            &gateway,
            &subscriptions,
            &update_lock,
            "ESM6",
            "CME",
            BookSubscriptionKind::Deltas,
            true,
        )
        .await;

        assert!(result.is_err());
        assert!(subscriptions.read().is_empty());
    }

    #[tokio::test]
    async fn book_dual_intent_only_unsubscribes_upstream_after_last_intent() {
        Python::initialize();

        let gateway = test_gateway();
        let subscriptions = Arc::new(parking_lot::RwLock::new(AHashMap::from_iter([(
            "CME:ESM6".to_string(),
            BookSubscription {
                deltas: true,
                depth10: true,
            },
        )])));
        let update_lock = Arc::new(tokio::sync::Mutex::new(()));

        PyRithmicDataClient::update_book_subscription(
            &gateway,
            &subscriptions,
            &update_lock,
            "ESM6",
            "CME",
            BookSubscriptionKind::Deltas,
            false,
        )
        .await
        .expect("remaining depth10 intent must not touch the disconnected gateway");
        assert_eq!(
            subscriptions.read().get("CME:ESM6").copied(),
            Some(BookSubscription {
                deltas: false,
                depth10: true,
            })
        );

        let result = PyRithmicDataClient::update_book_subscription(
            &gateway,
            &subscriptions,
            &update_lock,
            "ESM6",
            "CME",
            BookSubscriptionKind::Depth10,
            false,
        )
        .await;
        assert!(result.is_err());
        assert_eq!(
            subscriptions.read().get("CME:ESM6").copied(),
            Some(BookSubscription {
                deltas: false,
                depth10: true,
            })
        );
    }

    #[rstest::rstest]
    #[case(0)]
    #[case(-1)]
    fn bar_period_must_be_positive(#[case] bar_period: i32) {
        Python::initialize();
        assert!(PyRithmicDataClient::checked_bar_period(bar_period).is_err());
    }

    #[rstest::rstest]
    #[case(-1, 0)]
    #[case(0, -1)]
    #[case(10, 9)]
    fn history_window_rejects_invalid_ranges(#[case] start: i32, #[case] end: i32) {
        Python::initialize();
        assert!(PyRithmicDataClient::validate_history_window(start, end).is_err());
    }

    #[rstest::rstest]
    fn extra_market_data_state_restores_previous_value_after_failure() {
        let key = "CME:ESM6";
        let previous = ExtraMarketDataSubscription {
            quote_statistics: true,
            ..Default::default()
        };
        let mut subscriptions = AHashMap::from_iter([(key.to_string(), previous)]);

        assert!(
            PyRithmicDataClient::update_extra_market_data_subscription_state(
                &mut subscriptions,
                key,
                ExtraMarketDataKind::TradeStatistics,
                true,
            )
        );
        PyRithmicDataClient::restore_extra_market_data_subscription_state(
            &mut subscriptions,
            key,
            Some(previous),
        );

        let restored = subscriptions.get(key).unwrap();
        assert!(!restored.trade_statistics);
        assert!(restored.quote_statistics);
    }

    #[rstest::rstest]
    fn extra_market_data_state_removes_new_value_after_failure() {
        let key = "CME:ESM6";
        let mut subscriptions = AHashMap::new();

        assert!(
            PyRithmicDataClient::update_extra_market_data_subscription_state(
                &mut subscriptions,
                key,
                ExtraMarketDataKind::TradeStatistics,
                true,
            )
        );
        PyRithmicDataClient::restore_extra_market_data_subscription_state(
            &mut subscriptions,
            key,
            None,
        );

        assert!(!subscriptions.contains_key(key));
    }
}
