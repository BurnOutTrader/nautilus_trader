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

//! PyO3 bindings for Python interoperability.
//!
//! This module exposes Rust functionality to Python via PyO3,
//! enabling integration with NautilusTrader's Python layer.
//!
//! The NautilusTrader v2 integration is Rust-owned: Python supplies typed configuration and
//! factories, while [`crate::data::live::RithmicLiveDataClient`] and
//! [`crate::execution::live::RithmicLiveExecClient`] own runtime behavior. The low-level gateway,
//! data, execution, and venue-event bindings remain available for backwards compatibility and
//! diagnostics, but are not a second high-level Nautilus client implementation.

#[cfg(feature = "python")]
mod config;
#[cfg(feature = "python")]
mod custom;
#[cfg(feature = "python")]
mod data;
#[cfg(feature = "python")]
mod enums;
#[cfg(feature = "python")]
mod events;
#[cfg(feature = "python")]
mod execution;
#[cfg(feature = "python")]
mod gateway;
#[cfg(feature = "python")]
mod instruments;
#[cfg(feature = "python")]
mod symbols;

#[cfg(feature = "python")]
use nautilus_common::factories::{ClientConfig, DataClientFactory, ExecutionClientFactory};
#[cfg(feature = "python")]
use pyo3::prelude::*;

// ---- v2 PyO3 factory registration helpers -------------------------------------------------------

#[cfg(feature = "python")]
#[allow(clippy::needless_pass_by_value)]
fn extract_rithmic_data_factory(
    py: Python<'_>,
    factory: pyo3::Py<PyAny>,
) -> PyResult<Box<dyn DataClientFactory>> {
    match factory.extract::<crate::factories::RithmicDataClientFactory>(py) {
        Ok(f) => Ok(Box::new(f)),
        Err(e) => Err(nautilus_core::python::to_pyvalue_err(format!(
            "Failed to extract RithmicDataClientFactory: {e}"
        ))),
    }
}

#[cfg(feature = "python")]
#[allow(clippy::needless_pass_by_value)]
fn extract_rithmic_exec_factory(
    py: Python<'_>,
    factory: pyo3::Py<PyAny>,
) -> PyResult<Box<dyn ExecutionClientFactory>> {
    match factory.extract::<crate::factories::RithmicExecClientFactory>(py) {
        Ok(f) => Ok(Box::new(f)),
        Err(e) => Err(nautilus_core::python::to_pyvalue_err(format!(
            "Failed to extract RithmicExecClientFactory: {e}"
        ))),
    }
}

#[cfg(feature = "python")]
#[allow(clippy::needless_pass_by_value)]
fn extract_rithmic_data_config(
    py: Python<'_>,
    config: pyo3::Py<PyAny>,
) -> PyResult<Box<dyn ClientConfig>> {
    match config.extract::<pyo3::PyRef<'_, config::PyRithmicDataClientConfig>>(py) {
        Ok(c) => Ok(Box::new(c.inner.clone())),
        Err(e) => Err(nautilus_core::python::to_pyvalue_err(format!(
            "Failed to extract RithmicDataClientConfig: {e}"
        ))),
    }
}

#[cfg(feature = "python")]
#[allow(clippy::needless_pass_by_value)]
fn extract_rithmic_exec_config(
    py: Python<'_>,
    config: pyo3::Py<PyAny>,
) -> PyResult<Box<dyn ClientConfig>> {
    match config.extract::<pyo3::PyRef<'_, config::PyRithmicExecClientConfig>>(py) {
        Ok(c) => Ok(Box::new(c.inner.clone())),
        Err(e) => Err(nautilus_core::python::to_pyvalue_err(format!(
            "Failed to extract RithmicExecClientConfig: {e}"
        ))),
    }
}

// ---- pymodule -----------------------------------------------------------------------------------

/// Registers the Rithmic submodule for `nautilus_trader.core.nautilus_pyo3`.
#[cfg(feature = "python")]
#[pymodule]
pub fn rithmic(m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Register low-level compatibility and diagnostic bindings.
    config::register(m)?;
    enums::register(m)?;
    events::register(m)?;
    gateway::register(m)?;
    data::register(m)?;
    execution::register(m)?;
    instruments::register(m)?;
    symbols::register(m)?;

    // Register every Rust custom-data payload which can cross into Python.
    m.add_class::<crate::data::RithmicTradeStatistics>()?;
    m.add_class::<crate::data::RithmicQuoteStatistics>()?;
    m.add_class::<crate::data::RithmicIndicatorPrices>()?;
    m.add_class::<crate::data::RithmicOpenInterest>()?;
    m.add_class::<crate::data::RithmicEndOfDayPrices>()?;
    m.add_class::<crate::data::RithmicOrderPriceLimits>()?;
    m.add_class::<crate::data::RithmicSymbolMarginRate>()?;
    m.add_class::<crate::data::RithmicVolumeAtPrice>()?;
    m.add_class::<crate::data::RithmicMinuteVolumeProfileBar>()?;

    // Register v2 factory classes
    m.add_class::<crate::factories::RithmicDataClientFactory>()?;
    m.add_class::<crate::factories::RithmicExecClientFactory>()?;

    // Register with get_global_pyo3_registry so LiveNode can discover this adapter
    let registry = nautilus_system::get_global_pyo3_registry();

    if let Err(e) =
        registry.register_factory_extractor("RITHMIC".to_string(), extract_rithmic_data_factory)
    {
        return Err(nautilus_core::python::to_pyruntime_err(format!(
            "Failed to register Rithmic data factory extractor: {e}"
        )));
    }

    if let Err(e) = registry
        .register_exec_factory_extractor("RITHMIC".to_string(), extract_rithmic_exec_factory)
    {
        return Err(nautilus_core::python::to_pyruntime_err(format!(
            "Failed to register Rithmic exec factory extractor: {e}"
        )));
    }

    if let Err(e) = registry.register_config_extractor(
        "RithmicDataClientConfig".to_string(),
        extract_rithmic_data_config,
    ) {
        return Err(nautilus_core::python::to_pyruntime_err(format!(
            "Failed to register RithmicDataClientConfig extractor: {e}"
        )));
    }

    if let Err(e) = registry.register_config_extractor(
        "RithmicExecClientConfig".to_string(),
        extract_rithmic_exec_config,
    ) {
        return Err(nautilus_core::python::to_pyruntime_err(format!(
            "Failed to register RithmicExecClientConfig extractor: {e}"
        )));
    }

    Ok(())
}
