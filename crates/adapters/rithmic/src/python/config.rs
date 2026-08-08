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

//! Python bindings for configuration types.

#![allow(
    clippy::needless_pass_by_value,
    reason = "PyO3 configuration APIs accept owned Python values at the FFI boundary"
)]
#![allow(
    clippy::too_many_arguments,
    reason = "PyO3 constructors mirror the Python-visible configuration signatures"
)]

use nautilus_core::python::to_pyvalue_err;
#[cfg(feature = "python")]
use pyo3::prelude::*;

use crate::config::{
    RithmicDataClientConfig, RithmicEnv, RithmicExecClientConfig, adapter_account_id,
    configured_env_profiles, data_client_id, exec_client_id, load_rithmic_env_file,
    normalize_rithmic_client_component, parse_rithmic_env, parse_rithmic_trader_id,
};

/// Rithmic trading environment exposed to Python.
#[cfg(feature = "python")]
#[pyo3_stub_gen::derive::gen_stub_pyclass_enum(module = "nautilus_trader.adapters.rithmic")]
#[pyclass(
    name = "RithmicEnv",
    module = "nautilus_trader.adapters.rithmic",
    frozen,
    eq,
    eq_int,
    from_py_object,
    rename_all = "SCREAMING_SNAKE_CASE"
)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PyRithmicEnv {
    /// Demo/paper trading environment.
    Demo,
    /// Live trading environment.
    Live,
    /// Test environment.
    Test,
}

#[cfg(feature = "python")]
impl From<PyRithmicEnv> for RithmicEnv {
    fn from(py_env: PyRithmicEnv) -> Self {
        match py_env {
            PyRithmicEnv::Demo => Self::Demo,
            PyRithmicEnv::Live => Self::Live,
            PyRithmicEnv::Test => Self::Test,
        }
    }
}

#[cfg(feature = "python")]
impl From<RithmicEnv> for PyRithmicEnv {
    fn from(environment: RithmicEnv) -> Self {
        match environment {
            RithmicEnv::Demo => Self::Demo,
            RithmicEnv::Live => Self::Live,
            RithmicEnv::Test => Self::Test,
        }
    }
}

#[cfg(feature = "python")]
#[pyfunction(name = "normalize_rithmic_client_component")]
#[pyo3_stub_gen::derive::gen_stub_pyfunction(module = "nautilus_trader.adapters.rithmic")]
fn py_normalize_rithmic_client_component(value: &str) -> PyResult<String> {
    normalize_rithmic_client_component(value).map_err(|e| to_pyvalue_err(e.to_string()))
}

#[cfg(feature = "python")]
#[pyfunction(name = "get_rithmic_data_client_id")]
#[pyo3_stub_gen::derive::gen_stub_pyfunction(module = "nautilus_trader.adapters.rithmic")]
fn py_get_rithmic_data_client_id(system_name: &str) -> PyResult<String> {
    data_client_id(system_name)
        .map(|value| value.to_string())
        .map_err(|e| to_pyvalue_err(e.to_string()))
}

#[cfg(feature = "python")]
#[pyfunction(name = "get_rithmic_exec_client_id")]
#[pyo3_stub_gen::derive::gen_stub_pyfunction(module = "nautilus_trader.adapters.rithmic")]
fn py_get_rithmic_exec_client_id(system_name: &str, account_id: &str) -> PyResult<String> {
    exec_client_id(system_name, account_id)
        .map(|value| value.to_string())
        .map_err(|e| to_pyvalue_err(e.to_string()))
}

#[cfg(feature = "python")]
#[pyfunction(name = "get_rithmic_adapter_account_id")]
#[pyo3_stub_gen::derive::gen_stub_pyfunction(module = "nautilus_trader.adapters.rithmic")]
fn py_get_rithmic_adapter_account_id(system_name: &str, account_id: &str) -> PyResult<String> {
    let client_id =
        exec_client_id(system_name, account_id).map_err(|e| to_pyvalue_err(e.to_string()))?;
    adapter_account_id(client_id, account_id)
        .map(|value| value.to_string())
        .map_err(|e| to_pyvalue_err(e.to_string()))
}

#[cfg(feature = "python")]
#[pyfunction(name = "get_rithmic_profiles_from_env")]
#[pyo3_stub_gen::derive::gen_stub_pyfunction(module = "nautilus_trader.adapters.rithmic")]
fn py_get_rithmic_profiles_from_env() -> PyResult<Vec<String>> {
    configured_env_profiles().map_err(|e| to_pyvalue_err(e.to_string()))
}

#[cfg(feature = "python")]
#[pyfunction(name = "parse_rithmic_env")]
#[pyo3(signature = (value=None))]
#[pyo3_stub_gen::derive::gen_stub_pyfunction(module = "nautilus_trader.adapters.rithmic")]
fn py_parse_rithmic_env(value: Option<&str>) -> PyResult<PyRithmicEnv> {
    value
        .map_or(Ok(RithmicEnv::Demo), parse_rithmic_env)
        .map(Into::into)
        .map_err(|e| to_pyvalue_err(e.to_string()))
}

/// Python wrapper for RithmicDataClientConfig.
#[cfg(feature = "python")]
#[pyclass(name = "RithmicDataClientConfig", skip_from_py_object)]
#[pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.rithmic")]
#[derive(Clone)]
pub(crate) struct PyRithmicDataClientConfig {
    pub(crate) inner: RithmicDataClientConfig,
}

#[cfg(feature = "python")]
#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl PyRithmicDataClientConfig {
    /// Creates a new data client configuration.
    #[new]
    #[pyo3(signature = (environment, username, password, system_name, app_name, app_version=None, fcm_id=None, ib_id=None, server=None, alt_server=None, enable_history=false))]
    fn py_new(
        environment: PyRithmicEnv,
        username: String,
        password: String,
        system_name: String,
        app_name: &str,
        app_version: Option<String>,
        fcm_id: Option<String>,
        ib_id: Option<String>,
        server: Option<String>,
        alt_server: Option<String>,
        enable_history: bool,
    ) -> PyResult<Self> {
        let mut inner = RithmicDataClientConfig::new(
            environment.into(),
            username,
            password,
            system_name,
            app_name,
        )
        .map_err(|e| to_pyvalue_err(e.to_string()))?;
        if let Some(app_version) = app_version {
            inner.app_version = app_version;
        }
        inner.fcm_id = fcm_id;
        inner.ib_id = ib_id;
        inner.server = server;
        inner.alt_server = alt_server;
        inner.enable_history = enable_history;
        inner
            .validate()
            .map_err(|e| to_pyvalue_err(e.to_string()))?;
        Ok(Self { inner })
    }

    /// Creates configuration from environment variables.
    #[staticmethod]
    #[pyo3(signature = (profile=None, enable_history=None))]
    #[pyo3(name = "from_env")]
    fn py_from_env(profile: Option<String>, enable_history: Option<bool>) -> PyResult<Self> {
        let mut config = RithmicDataClientConfig::from_env_with_profile(profile.as_deref())
            .map_err(|e| to_pyvalue_err(e.to_string()))?;
        if let Some(enable_history) = enable_history {
            config.enable_history = enable_history;
        }
        Ok(Self { inner: config })
    }

    /// Loads `RITHMIC_*` env vars from a dotenv file.
    ///
    /// Existing environment values are not overwritten.
    #[staticmethod]
    #[pyo3(signature = (path=None))]
    #[pyo3(name = "load_env_file")]
    fn py_load_env_file(path: Option<String>) -> PyResult<usize> {
        load_rithmic_env_file(path.as_deref()).map_err(|e| to_pyvalue_err(e.to_string()))
    }

    #[getter(username)]
    fn py_username(&self) -> &str {
        &self.inner.username
    }

    #[getter(password)]
    fn py_password(&self) -> &str {
        &self.inner.password
    }

    #[getter(environment)]
    fn py_environment(&self) -> PyRithmicEnv {
        self.inner.environment.into()
    }

    #[getter(system_name)]
    fn py_system_name(&self) -> &str {
        &self.inner.system_name
    }

    #[getter(app_name)]
    fn py_app_name(&self) -> &str {
        &self.inner.app_name
    }

    #[getter(app_version)]
    fn py_app_version(&self) -> &str {
        &self.inner.app_version
    }

    #[getter(fcm_id)]
    fn py_fcm_id(&self) -> Option<String> {
        self.inner.fcm_id.clone()
    }

    #[getter(ib_id)]
    fn py_ib_id(&self) -> Option<String> {
        self.inner.ib_id.clone()
    }

    #[getter(server)]
    fn py_server(&self) -> Option<String> {
        self.inner.server.clone()
    }

    #[getter(alt_server)]
    fn py_alt_server(&self) -> Option<String> {
        self.inner.alt_server.clone()
    }

    #[getter(enable_history)]
    fn py_enable_history(&self) -> bool {
        self.inner.enable_history
    }

    #[pyo3(name = "__repr__")]
    fn py_repr(&self) -> String {
        format!(
            "RithmicDataClientConfig(environment={:?}, username='{}', system_name='{}', enable_history={})",
            self.inner.environment,
            self.inner.username,
            self.inner.system_name,
            self.inner.enable_history
        )
    }
}

/// Python wrapper for RithmicExecClientConfig.
#[cfg(feature = "python")]
#[pyclass(name = "RithmicExecClientConfig", skip_from_py_object)]
#[pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.rithmic")]
#[derive(Clone)]
pub(crate) struct PyRithmicExecClientConfig {
    pub(crate) inner: RithmicExecClientConfig,
}

#[cfg(feature = "python")]
#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl PyRithmicExecClientConfig {
    /// Creates a new execution client configuration.
    #[new]
    #[pyo3(signature = (
        environment,
        username,
        password,
        system_name,
        account_id,
        app_name,
        trader_id=None,
        app_version=None,
        fcm_id=None,
        ib_id=None,
        server=None,
        alt_server=None,
        execution_replay_lookback_secs=86_400
    ))]
    fn py_new(
        environment: PyRithmicEnv,
        username: String,
        password: String,
        system_name: String,
        account_id: String,
        app_name: &str,
        trader_id: Option<String>,
        app_version: Option<String>,
        fcm_id: Option<String>,
        ib_id: Option<String>,
        server: Option<String>,
        alt_server: Option<String>,
        execution_replay_lookback_secs: u64,
    ) -> PyResult<Self> {
        let trader_id = trader_id.map_or_else(
            || Ok(Default::default()),
            |value| parse_rithmic_trader_id(&value).map_err(|e| to_pyvalue_err(e.to_string())),
        )?;
        let mut inner = RithmicExecClientConfig::new(
            trader_id,
            environment.into(),
            username,
            password,
            system_name,
            account_id,
            app_name,
        )
        .map_err(|e| to_pyvalue_err(e.to_string()))?;
        if let Some(app_version) = app_version {
            inner.app_version = app_version;
        }
        inner.fcm_id = fcm_id;
        inner.ib_id = ib_id;
        inner.server = server;
        inner.alt_server = alt_server;
        inner.execution_replay_lookback_secs = execution_replay_lookback_secs;
        inner
            .validate()
            .map_err(|e| to_pyvalue_err(e.to_string()))?;
        Ok(Self { inner })
    }

    /// Creates configuration from environment variables.
    #[staticmethod]
    #[pyo3(signature = (profile=None, account_id=None, trader_id=None))]
    #[pyo3(name = "from_env")]
    fn py_from_env(
        profile: Option<String>,
        account_id: Option<String>,
        trader_id: Option<String>,
    ) -> PyResult<Self> {
        let config = RithmicExecClientConfig::from_env_with_profile_and_overrides(
            profile.as_deref(),
            account_id.as_deref(),
            trader_id.as_deref(),
        )
        .map_err(|e| to_pyvalue_err(e.to_string()))?;
        Ok(Self { inner: config })
    }

    /// Loads `RITHMIC_*` env vars from a dotenv file.
    ///
    /// Existing environment values are not overwritten.
    #[staticmethod]
    #[pyo3(signature = (path=None))]
    #[pyo3(name = "load_env_file")]
    fn py_load_env_file(path: Option<String>) -> PyResult<usize> {
        load_rithmic_env_file(path.as_deref()).map_err(|e| to_pyvalue_err(e.to_string()))
    }

    #[getter(username)]
    fn py_username(&self) -> &str {
        &self.inner.username
    }

    #[getter(password)]
    fn py_password(&self) -> &str {
        &self.inner.password
    }

    #[getter(environment)]
    fn py_environment(&self) -> PyRithmicEnv {
        self.inner.environment.into()
    }

    #[getter(account_id)]
    fn py_account_id(&self) -> &str {
        &self.inner.account_id
    }

    #[getter(trader_id)]
    fn py_trader_id(&self) -> String {
        self.inner.trader_id.to_string()
    }

    #[getter(system_name)]
    fn py_system_name(&self) -> &str {
        &self.inner.system_name
    }

    #[getter(app_name)]
    fn py_app_name(&self) -> &str {
        &self.inner.app_name
    }

    #[getter(app_version)]
    fn py_app_version(&self) -> &str {
        &self.inner.app_version
    }

    #[getter(fcm_id)]
    fn py_fcm_id(&self) -> Option<String> {
        self.inner.fcm_id.clone()
    }

    #[getter(ib_id)]
    fn py_ib_id(&self) -> Option<String> {
        self.inner.ib_id.clone()
    }

    #[getter(server)]
    fn py_server(&self) -> Option<String> {
        self.inner.server.clone()
    }

    #[getter(alt_server)]
    fn py_alt_server(&self) -> Option<String> {
        self.inner.alt_server.clone()
    }

    #[getter(execution_replay_lookback_secs)]
    fn py_execution_replay_lookback_secs(&self) -> u64 {
        self.inner.execution_replay_lookback_secs
    }

    #[pyo3(name = "__repr__")]
    fn py_repr(&self) -> String {
        format!(
            "RithmicExecClientConfig(environment={:?}, username='{}', account_id='{}', trader_id='{}', execution_replay_lookback_secs={})",
            self.inner.environment,
            self.inner.username,
            self.inner.account_id,
            self.inner.trader_id,
            self.inner.execution_replay_lookback_secs
        )
    }
}

/// Registers configuration types with the Python module.
#[cfg(feature = "python")]
pub(crate) fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyRithmicEnv>()?;
    // Backwards-compatible alias. Keeping one class preserves equality and constructor extraction.
    m.add("RithmicEnvironment", m.getattr("RithmicEnv")?)?;
    m.add_class::<PyRithmicDataClientConfig>()?;
    m.add_class::<PyRithmicExecClientConfig>()?;
    m.add_function(wrap_pyfunction!(py_normalize_rithmic_client_component, m)?)?;
    m.add_function(wrap_pyfunction!(py_get_rithmic_data_client_id, m)?)?;
    m.add_function(wrap_pyfunction!(py_get_rithmic_exec_client_id, m)?)?;
    m.add_function(wrap_pyfunction!(py_get_rithmic_adapter_account_id, m)?)?;
    m.add_function(wrap_pyfunction!(py_get_rithmic_profiles_from_env, m)?)?;
    m.add_function(wrap_pyfunction!(py_parse_rithmic_env, m)?)?;
    Ok(())
}
