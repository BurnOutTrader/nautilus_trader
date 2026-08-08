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

#![allow(
    clippy::missing_errors_doc,
    reason = "errors documented on underlying Rust methods"
)]

pub mod config;
pub mod factories;
pub mod http;
pub mod symbols;

use nautilus_common::factories::{ClientConfig, DataClientFactory, ExecutionClientFactory};
use nautilus_core::python::{to_pyruntime_err, to_pyvalue_err};
use nautilus_system::get_global_pyo3_registry;
use pyo3::prelude::*;

use crate::{
    config::{ProjectXConfig, ProjectXDataClientConfig, ProjectXExecClientConfig},
    factories::{ProjectXDataClientFactory, ProjectXExecutionClientFactory},
    http::client::ProjectXHttpClient,
};

#[allow(clippy::needless_pass_by_value)]
fn extract_projectx_data_factory(
    py: Python<'_>,
    factory: Py<PyAny>,
) -> PyResult<Box<dyn DataClientFactory>> {
    match factory.extract::<ProjectXDataClientFactory>(py) {
        Ok(factory) => Ok(Box::new(factory)),
        Err(e) => Err(to_pyvalue_err(format!(
            "Failed to extract ProjectXDataClientFactory: {e}"
        ))),
    }
}

#[allow(clippy::needless_pass_by_value)]
fn extract_projectx_exec_factory(
    py: Python<'_>,
    factory: Py<PyAny>,
) -> PyResult<Box<dyn ExecutionClientFactory>> {
    match factory.extract::<ProjectXExecutionClientFactory>(py) {
        Ok(factory) => Ok(Box::new(factory)),
        Err(e) => Err(to_pyvalue_err(format!(
            "Failed to extract ProjectXExecutionClientFactory: {e}"
        ))),
    }
}

#[allow(clippy::needless_pass_by_value)]
fn extract_projectx_data_config(
    py: Python<'_>,
    config: Py<PyAny>,
) -> PyResult<Box<dyn ClientConfig>> {
    match config.extract::<ProjectXDataClientConfig>(py) {
        Ok(config) => Ok(Box::new(config)),
        Err(e) => Err(to_pyvalue_err(format!(
            "Failed to extract ProjectXDataClientConfig: {e}"
        ))),
    }
}

#[allow(clippy::needless_pass_by_value)]
fn extract_projectx_exec_config(
    py: Python<'_>,
    config: Py<PyAny>,
) -> PyResult<Box<dyn ClientConfig>> {
    match config.extract::<ProjectXExecClientConfig>(py) {
        Ok(config) => Ok(Box::new(config)),
        Err(e) => Err(to_pyvalue_err(format!(
            "Failed to extract ProjectXExecClientConfig: {e}"
        ))),
    }
}

/// Loaded as `nautilus_pyo3.projectx`.
///
/// # Errors
///
/// Returns an error if any bindings fail to register with the Python module.
#[pymodule]
pub fn projectx(_: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    use crate::common::consts::{PROJECT_X, PROJECTX_CLIENT_ID, PROJECTX_VENUE};

    m.add(stringify!(PROJECTX), PROJECT_X)?;
    m.add(stringify!(PROJECTX_CLIENT_ID), *PROJECTX_CLIENT_ID)?;
    m.add(stringify!(PROJECTX_VENUE), *PROJECTX_VENUE)?;
    m.add_class::<ProjectXConfig>()?;
    m.add_class::<ProjectXDataClientConfig>()?;
    m.add_class::<ProjectXExecClientConfig>()?;
    m.add_class::<ProjectXDataClientFactory>()?;
    m.add_class::<ProjectXExecutionClientFactory>()?;
    m.add_class::<ProjectXHttpClient>()?;
    m.add_function(wrap_pyfunction!(config::load_projectx_env, m)?)?;
    m.add_function(wrap_pyfunction!(
        symbols::databento_to_projectx_adapter_symbol,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        symbols::databento_to_projectx_contract_id,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(symbols::projectx_to_databento_symbol, m)?)?;
    m.add_function(wrap_pyfunction!(
        symbols::projectx_to_databento_symbol_with_year,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(symbols::projectx_to_rithmic_symbol, m)?)?;
    m.add_function(wrap_pyfunction!(symbols::rithmic_to_projectx_symbol, m)?)?;
    m.add_function(wrap_pyfunction!(
        symbols::rithmic_to_projectx_symbol_with_year,
        m
    )?)?;

    let registry = get_global_pyo3_registry();

    if let Err(e) =
        registry.register_factory_extractor("PROJECTX".to_string(), extract_projectx_data_factory)
    {
        return Err(to_pyruntime_err(format!(
            "Failed to register ProjectX data factory extractor: {e}"
        )));
    }

    if let Err(e) = registry
        .register_exec_factory_extractor("PROJECTX".to_string(), extract_projectx_exec_factory)
    {
        return Err(to_pyruntime_err(format!(
            "Failed to register ProjectX exec factory extractor: {e}"
        )));
    }

    if let Err(e) = registry.register_config_extractor(
        "ProjectXDataClientConfig".to_string(),
        extract_projectx_data_config,
    ) {
        return Err(to_pyruntime_err(format!(
            "Failed to register ProjectX data config extractor: {e}"
        )));
    }

    if let Err(e) = registry.register_config_extractor(
        "ProjectXExecClientConfig".to_string(),
        extract_projectx_exec_config,
    ) {
        return Err(to_pyruntime_err(format!(
            "Failed to register ProjectX exec config extractor: {e}"
        )));
    }

    Ok(())
}
