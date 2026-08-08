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

//! Python bindings for the Rithmic instrument provider.

use std::sync::Arc;

use dashmap::DashMap;
use nautilus_core::{UnixNanos, python::to_pyruntime_err};
use nautilus_model::{
    instruments::{Instrument, InstrumentAny},
    python::instruments::instrument_any_to_pyobject,
};
#[cfg(feature = "python")]
use pyo3::prelude::*;
#[cfg(feature = "python")]
use pyo3::types::PyList;
#[cfg(feature = "python")]
use pyo3_async_runtimes::tokio::future_into_py;

use super::gateway::PyRithmicGateway;
use crate::{
    common::consts::exchanges::KNOWN_EXCHANGES,
    error::RithmicError,
    gateway::RithmicGateway,
    instruments::{
        discovery::{
            RithmicInstrumentSymbol, discover_all_symbols_with_handle,
            discover_exchange_symbols_with_handle, discover_product_symbols_with_handle,
            enabled_exchange_names,
        },
        front_month::{
            fetch_instrument_with_handle, instrument_is_tradeable,
            load_front_month_instrument_with_handle, load_supported_front_months_with_handle,
        },
    },
};

fn now_nanos() -> UnixNanos {
    UnixNanos::from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos() as u64),
    )
}

fn cache_key(symbol: &str, exchange: &str) -> String {
    format!("{exchange}:{symbol}")
}

/// Python wrapper for a raw supported-root contract listing.
#[cfg(feature = "python")]
#[pyclass(name = "RithmicInstrumentSymbol", skip_from_py_object)]
#[pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.rithmic")]
#[derive(Clone)]
pub(crate) struct PyRithmicInstrumentSymbol {
    inner: RithmicInstrumentSymbol,
}

#[cfg(feature = "python")]
#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl PyRithmicInstrumentSymbol {
    #[getter]
    fn symbol(&self) -> String {
        self.inner.symbol.clone()
    }

    #[getter]
    fn exchange(&self) -> String {
        self.inner.exchange.clone()
    }

    #[getter]
    fn product_code(&self) -> String {
        self.inner.product_code.clone()
    }

    #[getter]
    fn description(&self) -> Option<String> {
        self.inner.description.clone()
    }

    #[getter]
    fn instrument_type(&self) -> Option<String> {
        self.inner.instrument_type.clone()
    }

    #[getter]
    fn expiration_date(&self) -> Option<String> {
        self.inner.expiration_date.clone()
    }

    #[getter]
    fn instrument_id(&self) -> String {
        self.inner.instrument_id()
    }

    #[pyo3(name = "__repr__")]
    fn py_repr(&self) -> String {
        format!(
            "RithmicInstrumentSymbol(symbol={:?}, exchange={:?}, product_code={:?})",
            self.inner.symbol, self.inner.exchange, self.inner.product_code
        )
    }
}

/// Python wrapper for Rithmic instrument loading.
///
/// Holds `Arc<RwLock<RithmicGateway>>` (Python v1 lifecycle pattern) and implements
/// instrument queries by cloning the ticker handle before every await — no lock is
/// held across async operations.
#[cfg(feature = "python")]
#[pyclass(name = "RithmicInstrumentProvider")]
#[pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.rithmic")]
pub(crate) struct PyRithmicInstrumentProvider {
    gateway: Arc<tokio::sync::RwLock<RithmicGateway>>,
    instruments: Arc<DashMap<String, InstrumentAny>>,
}

#[cfg(feature = "python")]
#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl PyRithmicInstrumentProvider {
    #[new]
    fn py_new(gateway: &PyRithmicGateway) -> Self {
        Self {
            gateway: Arc::clone(&gateway.inner),
            instruments: Arc::new(DashMap::new()),
        }
    }

    /// Loads all instruments across known exchanges.
    #[pyo3(name = "load_all_async")]
    fn py_load_all_async<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.py_load_all_with_filter_async(py, false)
    }

    /// Loads tradeable instruments across known exchanges.
    #[pyo3(name = "load_all_tradeable_async")]
    fn py_load_all_tradeable_async<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.py_load_all_with_filter_async(py, true)
    }

    fn py_load_all_with_filter_async<'py>(
        &self,
        py: Python<'py>,
        tradeable_only: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let gateway = Arc::clone(&self.gateway);
        let instruments = Arc::clone(&self.instruments);
        future_into_py(py, async move {
            let gateway_read = gateway.read().await;
            if !gateway_read.is_connected() {
                return Err(to_pyruntime_err("Gateway is not connected"));
            }
            let ticker = gateway_read
                .ticker_handle()
                .cloned()
                .ok_or_else(|| to_pyruntime_err("Ticker plant not connected"))?;
            let username = gateway_read.config().username.clone();
            drop(gateway_read);

            let exchanges = match enabled_exchanges_with_handle(&ticker, &username).await {
                Ok(enabled) if !enabled.is_empty() => enabled,
                Ok(_) => KNOWN_EXCHANGES
                    .iter()
                    .map(|exchange| exchange.to_string())
                    .collect(),
                Err(e) => {
                    tracing::warn!(
                        "Failed to load enabled Rithmic exchanges, falling back to known set: {e}"
                    );
                    KNOWN_EXCHANGES
                        .iter()
                        .map(|exchange| exchange.to_string())
                        .collect()
                }
            };

            let mut total = 0usize;

            for exchange in exchanges {
                match load_exchange_with_filter_handle(
                    &ticker,
                    &exchange,
                    &instruments,
                    tradeable_only,
                )
                .await
                {
                    Ok(loaded) => total += loaded.len(),
                    Err(e) => tracing::warn!("Failed to load instruments from {exchange}: {e}"),
                }
            }
            Ok(total)
        })
    }

    /// Loads instruments for a specific exchange.
    #[pyo3(name = "load_exchange_async")]
    fn py_load_exchange_async<'py>(
        &self,
        py: Python<'py>,
        exchange: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.py_load_exchange_with_filter_async(py, exchange, false)
    }

    /// Loads tradeable instruments for a specific exchange.
    #[pyo3(name = "load_exchange_tradeable_async")]
    fn py_load_exchange_tradeable_async<'py>(
        &self,
        py: Python<'py>,
        exchange: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.py_load_exchange_with_filter_async(py, exchange, true)
    }

    fn py_load_exchange_with_filter_async<'py>(
        &self,
        py: Python<'py>,
        exchange: String,
        tradeable_only: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let gateway = Arc::clone(&self.gateway);
        let instruments = Arc::clone(&self.instruments);
        future_into_py(py, async move {
            if !gateway.read().await.is_connected() {
                return Err(to_pyruntime_err("Gateway is not connected"));
            }
            let ticker = gateway
                .read()
                .await
                .ticker_handle()
                .cloned()
                .ok_or_else(|| to_pyruntime_err("Ticker plant not connected"))?;

            let loaded =
                load_exchange_with_filter_handle(&ticker, &exchange, &instruments, tradeable_only)
                    .await
                    .map_err(|e| to_pyruntime_err(e.to_string()))?;

            Python::attach(|py| {
                let py_list: PyResult<Vec<_>> = loaded
                    .into_iter()
                    .map(|instrument| instrument_any_to_pyobject(py, instrument))
                    .collect();
                Ok(PyList::new(py, py_list?)?.into_any().unbind())
            })
        })
    }

    /// Discovers raw contract listings for a supported exchange.
    #[pyo3(name = "discover_exchange_symbols_async")]
    fn py_discover_exchange_symbols_async<'py>(
        &self,
        py: Python<'py>,
        exchange: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let gateway = Arc::clone(&self.gateway);
        future_into_py(py, async move {
            if !gateway.read().await.is_connected() {
                return Err(to_pyruntime_err("Gateway is not connected"));
            }
            let ticker = gateway
                .read()
                .await
                .ticker_handle()
                .cloned()
                .ok_or_else(|| to_pyruntime_err("Ticker plant not connected"))?;

            let discovered = discover_exchange_symbols_with_handle(&ticker, &exchange)
                .await
                .map_err(|e| to_pyruntime_err(e.to_string()))?;

            Python::attach(|py| raw_symbols_to_py_list(py, discovered))
        })
    }

    /// Discovers raw contract listings for a supported root product.
    #[pyo3(name = "discover_product_symbols_async")]
    fn py_discover_product_symbols_async<'py>(
        &self,
        py: Python<'py>,
        product: String,
        exchange: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let gateway = Arc::clone(&self.gateway);
        future_into_py(py, async move {
            if !gateway.read().await.is_connected() {
                return Err(to_pyruntime_err("Gateway is not connected"));
            }
            let ticker = gateway
                .read()
                .await
                .ticker_handle()
                .cloned()
                .ok_or_else(|| to_pyruntime_err("Ticker plant not connected"))?;

            let discovered = discover_product_symbols_with_handle(&ticker, &exchange, &product)
                .await
                .map_err(|e| to_pyruntime_err(e.to_string()))?;

            Python::attach(|py| raw_symbols_to_py_list(py, discovered))
        })
    }

    /// Discovers raw contract listings across all enabled supported exchanges.
    #[pyo3(name = "discover_all_symbols_async")]
    fn py_discover_all_symbols_async<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let gateway = Arc::clone(&self.gateway);
        future_into_py(py, async move {
            let gateway_read = gateway.read().await;
            if !gateway_read.is_connected() {
                return Err(to_pyruntime_err("Gateway is not connected"));
            }
            let ticker = gateway_read
                .ticker_handle()
                .cloned()
                .ok_or_else(|| to_pyruntime_err("Ticker plant not connected"))?;
            let username = gateway_read.config().username.clone();
            drop(gateway_read);

            let exchanges = match enabled_exchanges_with_handle(&ticker, &username).await {
                Ok(enabled) if !enabled.is_empty() => enabled,
                Ok(_) => KNOWN_EXCHANGES
                    .iter()
                    .map(|exchange| exchange.to_string())
                    .collect(),
                Err(e) => {
                    tracing::warn!(
                        "Failed to load enabled Rithmic exchanges, falling back to known set: {e}"
                    );
                    KNOWN_EXCHANGES
                        .iter()
                        .map(|exchange| exchange.to_string())
                        .collect()
                }
            };
            let discovered = discover_all_symbols_with_handle(&ticker, &exchanges)
                .await
                .map_err(|e| to_pyruntime_err(e.to_string()))?;

            Python::attach(|py| raw_symbols_to_py_list(py, discovered))
        })
    }

    /// Loads a single instrument by symbol and exchange.
    #[pyo3(name = "load_instrument_async")]
    fn py_load_instrument_async<'py>(
        &self,
        py: Python<'py>,
        symbol: String,
        exchange: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let gateway = Arc::clone(&self.gateway);
        let instruments = Arc::clone(&self.instruments);
        future_into_py(py, async move {
            if !gateway.read().await.is_connected() {
                return Err(to_pyruntime_err("Gateway is not connected"));
            }
            let ticker = gateway
                .read()
                .await
                .ticker_handle()
                .cloned()
                .ok_or_else(|| to_pyruntime_err("Ticker plant not connected"))?;

            let instrument =
                load_instrument_with_handle(&ticker, &symbol, &exchange, &instruments, true)
                    .await
                    .map_err(|e| to_pyruntime_err(e.to_string()))?;

            Python::attach(|py| instrument_any_to_pyobject(py, instrument))
        })
    }

    /// Loads the current front month contract for a product root and exchange.
    #[pyo3(name = "load_front_month_async")]
    fn py_load_front_month_async<'py>(
        &self,
        py: Python<'py>,
        product: String,
        exchange: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let gateway = Arc::clone(&self.gateway);
        let instruments = Arc::clone(&self.instruments);
        future_into_py(py, async move {
            if !gateway.read().await.is_connected() {
                return Err(to_pyruntime_err("Gateway is not connected"));
            }
            let ticker = gateway
                .read()
                .await
                .ticker_handle()
                .cloned()
                .ok_or_else(|| to_pyruntime_err("Ticker plant not connected"))?;

            let instrument =
                load_front_month_instrument_with_handle(&ticker, &product, &exchange, now_nanos())
                    .await
                    .map_err(|e| to_pyruntime_err(e.to_string()))?;
            cache_instrument(&instruments, &instrument);

            Python::attach(|py| instrument_any_to_pyobject(py, instrument))
        })
    }

    /// Returns all cached instruments.
    #[pyo3(name = "instruments")]
    fn py_instruments(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let py_instruments: PyResult<Vec<_>> = self
            .instruments
            .iter()
            .filter(|e| e.key().contains(':'))
            .map(|e| instrument_any_to_pyobject(py, e.value().clone()))
            .collect();
        Ok(PyList::new(py, py_instruments?)?.into_any().unbind())
    }

    #[pyo3(name = "__repr__")]
    fn py_repr(&self) -> String {
        format!(
            "RithmicInstrumentProvider(count={})",
            self.instruments
                .iter()
                .filter(|e| e.key().contains(':'))
                .count()
        )
    }
}

use rithmic_rs::plants::ticker_plant::RithmicTickerPlantHandle;

async fn enabled_exchanges_with_handle(
    ticker: &RithmicTickerPlantHandle,
    username: &str,
) -> Result<Vec<String>, RithmicError> {
    let responses = ticker
        .list_exchanges(username)
        .await
        .map_err(|e| RithmicError::Api(format!("Exchange permissions request failed: {e}")))?;

    Ok(enabled_exchange_names(&responses).into_iter().collect())
}

/// Loads all instruments for one exchange using a pre-cloned ticker handle.
async fn load_exchange_with_filter_handle(
    ticker: &RithmicTickerPlantHandle,
    exchange: &str,
    cache: &DashMap<String, InstrumentAny>,
    tradeable_only: bool,
) -> Result<Vec<InstrumentAny>, RithmicError> {
    let loaded =
        load_supported_front_months_with_handle(ticker, exchange, now_nanos(), tradeable_only)
            .await?;

    for instrument in &loaded {
        cache_instrument(cache, instrument);
    }

    Ok(loaded)
}

/// Loads a single instrument using a pre-cloned ticker handle.
async fn load_instrument_with_handle(
    ticker: &RithmicTickerPlantHandle,
    symbol: &str,
    exchange: &str,
    cache: &DashMap<String, InstrumentAny>,
    cache_non_tradeable: bool,
) -> Result<InstrumentAny, RithmicError> {
    let key = cache_key(symbol, exchange);

    if let Some(cached) = cache.get(&key) {
        return Ok(cached.value().clone());
    }

    let instrument = fetch_instrument_with_handle(ticker, symbol, exchange, now_nanos()).await?;

    if cache_non_tradeable || instrument_is_tradeable(&instrument) {
        cache_instrument(cache, &instrument);
    }

    Ok(instrument)
}

fn cache_instrument(cache: &DashMap<String, InstrumentAny>, instrument: &InstrumentAny) {
    let symbol = instrument.raw_symbol().to_string();
    let exchange = instrument.exchange().map(|value| value.to_string());

    if let Some(exchange) = exchange.as_deref() {
        cache.insert(cache_key(&symbol, exchange), instrument.clone());
    }
    cache.insert(symbol, instrument.clone());
}

#[cfg(feature = "python")]
fn raw_symbols_to_py_list(
    py: Python<'_>,
    symbols: Vec<RithmicInstrumentSymbol>,
) -> PyResult<Py<PyAny>> {
    let py_symbols = symbols
        .into_iter()
        .map(|symbol| Py::new(py, PyRithmicInstrumentSymbol { inner: symbol }))
        .collect::<PyResult<Vec<_>>>()?;
    Ok(PyList::new(py, py_symbols)?.into_any().unbind())
}

/// Registers instrument provider types with the Python module.
#[cfg(feature = "python")]
pub(crate) fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyRithmicInstrumentSymbol>()?;
    m.add_class::<PyRithmicInstrumentProvider>()?;
    Ok(())
}
