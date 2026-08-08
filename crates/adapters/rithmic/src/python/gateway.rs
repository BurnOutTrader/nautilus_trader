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

//! Python bindings for the Rithmic gateway.

#![allow(
    clippy::needless_pass_by_value,
    reason = "PyO3 gateway APIs accept owned Python values at the FFI boundary"
)]

use std::sync::Arc;

use ahash::AHashMap;
use nautilus_common::live::get_runtime;
use nautilus_core::python::{to_pyruntime_err, to_pyvalue_err};
#[cfg(feature = "python")]
use pyo3::prelude::*;
#[cfg(feature = "python")]
use pyo3_async_runtimes::tokio::future_into_py;
use tokio::task::JoinHandle;

use super::config::PyRithmicEnv;
use crate::{
    gateway::{GatewayConfig, PnlEvent, RithmicGateway},
    providers::{
        AccountBalance, AccountEvent as ProviderAccountEvent, Position,
        PositionEvent as ProviderPositionEvent,
    },
    python::events::{PyAccountEvent, PyPositionEvent},
};

/// Python wrapper for RithmicGateway.
///
/// The gateway manages all Rithmic plant connections and provides handles
/// for data and execution clients.
#[cfg(feature = "python")]
#[pyclass(name = "RithmicGateway")]
#[pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.rithmic")]
pub(crate) struct PyRithmicGateway {
    /// The inner gateway wrapped in tokio::sync::RwLock for safe async access.
    /// We use tokio::sync::RwLock because connect/disconnect need &mut self.
    pub(crate) inner: Arc<tokio::sync::RwLock<RithmicGateway>>,
    pnl_task: Arc<parking_lot::Mutex<Option<JoinHandle<()>>>>,
    pnl_shutdown: Arc<parking_lot::Mutex<Option<tokio::sync::oneshot::Sender<()>>>>,
    balances: Arc<parking_lot::RwLock<AHashMap<String, AccountBalance>>>,
    positions: Arc<parking_lot::RwLock<AHashMap<String, Position>>>,
}

#[cfg(feature = "python")]
#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl PyRithmicGateway {
    /// Creates a new gateway with the given configuration.
    #[new]
    #[pyo3(signature = (
        environment,
        username,
        password,
        system_name,
        fcm_id,
        ib_id,
        account_id,
        server=None,
        alt_server=None,
        app_name="",
        app_version="1.0",
        enable_ticker=true,
        enable_order=true,
        enable_pnl=true,
        enable_history=false
    ))]
    #[allow(clippy::too_many_arguments)]
    fn py_new(
        environment: PyRithmicEnv,
        username: String,
        password: String,
        system_name: String,
        fcm_id: String,
        ib_id: String,
        account_id: String,
        server: Option<String>,
        alt_server: Option<String>,
        app_name: &str,
        app_version: &str,
        enable_ticker: bool,
        enable_order: bool,
        enable_pnl: bool,
        enable_history: bool,
    ) -> Self {
        let mut config = GatewayConfig::new(
            environment.into(),
            username,
            password,
            system_name,
            fcm_id,
            ib_id,
            account_id,
        )
        .with_app_name(app_name)
        .with_app_version(app_version)
        .with_ticker(enable_ticker)
        .with_order(enable_order)
        .with_pnl(enable_pnl)
        .with_history(enable_history);

        if let Some(server) = server {
            config = config.with_server(server);
        }

        if let Some(alt_server) = alt_server {
            config = config.with_alt_server(alt_server);
        }

        Self {
            inner: Arc::new(tokio::sync::RwLock::new(RithmicGateway::new(config))),
            pnl_task: Arc::new(parking_lot::Mutex::new(None)),
            pnl_shutdown: Arc::new(parking_lot::Mutex::new(None)),
            balances: Arc::new(parking_lot::RwLock::new(AHashMap::new())),
            positions: Arc::new(parking_lot::RwLock::new(AHashMap::new())),
        }
    }

    /// Creates a gateway from environment variables.
    #[staticmethod]
    #[pyo3(signature = (profile=None))]
    #[pyo3(name = "from_env")]
    fn py_from_env(profile: Option<String>) -> PyResult<Self> {
        let config = GatewayConfig::from_env_with_profile(profile.as_deref())
            .map_err(|e| to_pyvalue_err(e.to_string()))?;
        Ok(Self {
            inner: Arc::new(tokio::sync::RwLock::new(RithmicGateway::new(config))),
            pnl_task: Arc::new(parking_lot::Mutex::new(None)),
            pnl_shutdown: Arc::new(parking_lot::Mutex::new(None)),
            balances: Arc::new(parking_lot::RwLock::new(AHashMap::new())),
            positions: Arc::new(parking_lot::RwLock::new(AHashMap::new())),
        })
    }

    /// Returns true if the gateway is connected.
    #[pyo3(name = "is_connected")]
    fn py_is_connected(&self) -> bool {
        // Use try_read to avoid blocking - if locked, assume not connected
        self.inner.try_read().is_ok_and(|g| g.is_connected())
    }

    /// Returns the current connection state as a string.
    #[pyo3(name = "connection_state")]
    fn py_connection_state(&self) -> String {
        self.inner.try_read().map_or_else(
            |_| "Unknown".to_string(),
            |g| format!("{:?}", g.connection_state()),
        )
    }

    /// Returns the account ID from the configuration.
    #[pyo3(name = "account_id")]
    fn py_account_id(&self) -> Option<String> {
        self.inner
            .try_read()
            .ok()
            .map(|g| g.config().account_id.clone())
    }

    /// Returns the accessible trading accounts for the current order session.
    #[pyo3(name = "list_accounts")]
    fn py_list_accounts<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        future_into_py(py, async move {
            let gateway = inner.read().await;
            gateway
                .list_accounts()
                .await
                .map_err(|e| to_pyruntime_err(format!("Account list request failed: {e}")))
        })
    }

    /// Gets the current front month contract symbol for a product.
    ///
    /// This resolves the active contract symbol (e.g., "ESH6" for March 2025)
    /// so you don't subscribe to an expired contract.
    ///
    /// Parameters
    /// ----------
    /// product : str
    ///     The product code (e.g., "ES" for E-mini S&P 500)
    /// exchange : str
    ///     The exchange code (e.g., "CME")
    ///
    /// Returns
    /// -------
    /// str
    ///     The front month contract symbol.
    ///
    /// Raises
    /// ------
    /// RuntimeError
    ///     If not connected or the request fails.
    #[pyo3(name = "get_front_month_symbol")]
    fn py_get_front_month_symbol<'py>(
        &self,
        py: Python<'py>,
        product: String,
        exchange: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        future_into_py(py, async move {
            let gateway = inner.read().await;
            gateway
                .get_front_month_symbol(&product, &exchange)
                .await
                .map_err(|e| to_pyruntime_err(format!("Front month request failed: {e}")))
        })
    }

    /// Requests a PnL snapshot for the configured account.
    ///
    /// Snapshot updates are delivered through the running PnL callback loop.
    #[pyo3(name = "request_pnl_snapshot")]
    fn py_request_pnl_snapshot<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        future_into_py(py, async move {
            let mut gateway = inner.write().await;
            gateway
                .request_pnl_snapshot()
                .await
                .map_err(|e| to_pyruntime_err(format!("PnL snapshot request failed: {e}")))
        })
    }

    /// Returns the current live position snapshots tracked by the gateway PnL loop.
    #[pyo3(signature = (account_id=None))]
    #[pyo3(name = "positions")]
    fn py_positions(&self, account_id: Option<String>) -> Vec<PyPositionEvent> {
        self.positions
            .read()
            .values()
            .filter(|position| {
                account_id
                    .as_ref()
                    .is_none_or(|expected| &position.account_id == expected)
            })
            .cloned()
            .map(ProviderPositionEvent::Updated)
            .map(PyPositionEvent::from)
            .collect()
    }

    #[pyo3(name = "__repr__")]
    fn py_repr(&self) -> String {
        let connected = self.py_is_connected();
        let state = self.py_connection_state();
        format!("RithmicGateway(connected={connected}, state={state})")
    }

    /// Connects to all enabled Rithmic plants.
    ///
    /// This is an async method - use `await gateway.connect()` in Python.
    ///
    /// Returns
    /// -------
    /// None
    ///     On successful connection.
    ///
    /// Raises
    /// ------
    /// RuntimeError
    ///     If connection fails.
    #[pyo3(name = "connect")]
    fn py_connect<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        future_into_py(py, async move {
            let mut gateway = inner.write().await;
            gateway
                .connect()
                .await
                .map_err(|e| to_pyruntime_err(format!("Connection failed: {e}")))
        })
    }

    /// Reconnects all enabled Rithmic plants after a forced logout or transport failure.
    ///
    /// This is an async method - use `await gateway.reconnect()` in Python.
    #[pyo3(name = "reconnect")]
    fn py_reconnect<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        future_into_py(py, async move {
            let mut gateway = inner.write().await;
            gateway
                .reconnect()
                .await
                .map_err(|e| to_pyruntime_err(format!("Reconnect failed: {e}")))
        })
    }

    /// Disconnects from all Rithmic plants.
    ///
    /// This is an async method - use `await gateway.disconnect()` in Python.
    ///
    /// Returns
    /// -------
    /// None
    ///     On successful disconnection.
    ///
    /// Raises
    /// ------
    /// RuntimeError
    ///     If disconnection fails.
    #[pyo3(name = "disconnect")]
    fn py_disconnect<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        future_into_py(py, async move {
            let mut gateway = inner.write().await;
            gateway
                .disconnect()
                .await
                .map_err(|e| to_pyruntime_err(format!("Disconnection failed: {e}")))
        })
    }

    /// Starts a background PnL/position event loop that dispatches events to a Python callback.
    ///
    /// This can be called once after connecting. The callback receives `PyAccountEvent` or
    /// `PyPositionEvent` instances.
    #[pyo3(name = "start_pnl_loop")]
    fn py_start_pnl_loop(&self, _py: Python<'_>, callback: Py<PyAny>) -> PyResult<()> {
        // Prevent double-start

        if self.pnl_task.lock().is_some() {
            return Err(to_pyruntime_err("PnL loop already running"));
        }

        let inner = Arc::clone(&self.inner);
        let callback = callback;
        let shutdown = Arc::clone(&self.pnl_shutdown);
        let task_slot = Arc::clone(&self.pnl_task);
        let balances = Arc::clone(&self.balances);
        let positions = Arc::clone(&self.positions);

        // Spawn async task

        let handle = get_runtime().spawn(async move {
            let mut rx = {
                let gw = inner.read().await;
                gw.subscribe_pnl_events()
            };

            let (tx, mut rx_shutdown) = tokio::sync::oneshot::channel();
            *shutdown.lock() = Some(tx);

            loop {
                tokio::select! {
                    _ = &mut rx_shutdown => {
                        break;
                    }
                    maybe_event = rx.recv() => {
                        match maybe_event {
                            Ok(event) => {
                                Self::sync_pnl_state(&balances, &positions, &event);
                                Python::attach(|py| {
                                    match event {
                                        PnlEvent::Account(ae) => {
                                            let py_event = PyAccountEvent::from(ae);
                                            let _ = callback.call1(py, (py_event,));
                                        }
                                        PnlEvent::Position(pe) => {
                                            let py_event = PyPositionEvent::from(pe);
                                            let _ = callback.call1(py, (py_event,));
                                        }
                                    }
                                });
                            }
                            Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                                tracing::warn!("PnL subscriber lagged by {skipped} events");
                            }
                            Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                        }
                    }
                }
            }
            *shutdown.lock() = None;
            *task_slot.lock() = None;
        });

        *self.pnl_task.lock() = Some(handle);
        Ok(())
    }

    /// Stops the background PnL loop if running.
    #[pyo3(name = "stop_pnl_loop")]
    fn py_stop_pnl_loop(&self) {
        if let Some(tx) = self.pnl_shutdown.lock().take() {
            let _ = tx.send(());
        }

        if let Some(handle) = self.pnl_task.lock().take() {
            handle.abort();
        }
    }

    /// Subscribes to market data for an instrument.
    ///
    /// This is an async method - use `await gateway.subscribe_market_data(symbol, exchange)`.
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
    #[pyo3(name = "subscribe_market_data")]
    fn py_subscribe_market_data<'py>(
        &self,
        py: Python<'py>,
        symbol: String,
        exchange: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        future_into_py(py, async move {
            let gateway = inner.read().await;
            gateway
                .subscribe_market_data(&symbol, &exchange)
                .await
                .map_err(|e| to_pyruntime_err(format!("Subscription failed: {e}")))
        })
    }

    /// Unsubscribes from market data for an instrument.
    ///
    /// This is an async method.
    ///
    /// Parameters
    /// ----------
    /// symbol : str
    ///     The instrument symbol.
    /// exchange : str
    ///     The exchange code.
    #[pyo3(name = "unsubscribe_market_data")]
    fn py_unsubscribe_market_data<'py>(
        &self,
        py: Python<'py>,
        symbol: String,
        exchange: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        future_into_py(py, async move {
            let gateway = inner.read().await;
            gateway
                .unsubscribe_market_data(&symbol, &exchange)
                .await
                .map_err(|e| to_pyruntime_err(format!("Unsubscribe failed: {e}")))
        })
    }
}

#[cfg(feature = "python")]
impl PyRithmicGateway {
    fn sync_pnl_state(
        balances: &Arc<parking_lot::RwLock<AHashMap<String, AccountBalance>>>,
        positions: &Arc<parking_lot::RwLock<AHashMap<String, Position>>>,
        event: &PnlEvent,
    ) {
        match event {
            PnlEvent::Account(ProviderAccountEvent::BalanceUpdate(balance)) => {
                balances
                    .write()
                    .insert(balance.account_id.clone(), balance.clone());
            }
            PnlEvent::Account(_) => {}
            PnlEvent::Position(
                ProviderPositionEvent::Opened(position) | ProviderPositionEvent::Updated(position),
            ) => {
                let key = format!(
                    "{}:{}:{}",
                    position.account_id, position.exchange, position.symbol
                );

                if position.quantity == 0.0 {
                    positions.write().remove(&key);
                } else {
                    positions.write().insert(key, position.clone());
                }
            }
            PnlEvent::Position(ProviderPositionEvent::Closed {
                account_id,
                symbol,
                exchange,
                ..
            }) => {
                let key = format!("{account_id}:{exchange}:{symbol}");
                positions.write().remove(&key);
            }
            PnlEvent::Position(ProviderPositionEvent::Error(_)) => {}
        }
    }
}

/// Registers gateway types with the Python module.
#[cfg(feature = "python")]
pub(crate) fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyRithmicGateway>()?;
    Ok(())
}
