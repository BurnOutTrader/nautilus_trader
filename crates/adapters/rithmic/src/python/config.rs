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
use nautilus_model::identifiers::TraderId;
#[cfg(feature = "python")]
use pyo3::prelude::*;

use crate::config::{
    RithmicDataClientConfig, RithmicEnv, RithmicExecClientConfig, load_rithmic_env_file,
};

/// Python wrapper for RithmicEnv.
#[cfg(feature = "python")]
#[pyclass(
    name = "RithmicEnv",
    module = "nautilus_trader.adapters.rithmic",
    from_py_object
)]
#[pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.rithmic")]
#[derive(Clone)]
pub(crate) struct PyRithmicEnv {
    inner: RithmicEnv,
}

#[cfg(feature = "python")]
#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl PyRithmicEnv {
    /// Demo/paper trading environment.
    #[classattr]
    const DEMO: Self = Self {
        inner: RithmicEnv::Demo,
    };

    /// Live trading environment.
    #[classattr]
    const LIVE: Self = Self {
        inner: RithmicEnv::Live,
    };

    /// Test environment.
    #[classattr]
    const TEST: Self = Self {
        inner: RithmicEnv::Test,
    };

    #[pyo3(name = "__repr__")]
    fn py_repr(&self) -> String {
        format!("RithmicEnv.{}", self.inner.to_string().to_uppercase())
    }
}

#[cfg(feature = "python")]
impl From<PyRithmicEnv> for RithmicEnv {
    fn from(py_env: PyRithmicEnv) -> Self {
        py_env.inner
    }
}

/// Deprecated: Use [`PyRithmicEnv`] instead.
///
/// This class is provided for backwards compatibility and will be removed
/// in a future major version.
#[cfg(feature = "python")]
#[pyclass(name = "RithmicEnvironment", skip_from_py_object)]
#[pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.rithmic")]
#[derive(Clone)]
pub(crate) struct PyRithmicEnvironment {
    inner: RithmicEnv,
}

#[cfg(feature = "python")]
#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl PyRithmicEnvironment {
    /// Demo/paper trading environment.
    #[classattr]
    const DEMO: Self = Self {
        inner: RithmicEnv::Demo,
    };

    /// Live trading environment.
    #[classattr]
    const LIVE: Self = Self {
        inner: RithmicEnv::Live,
    };

    /// Test environment.
    #[classattr]
    const TEST: Self = Self {
        inner: RithmicEnv::Test,
    };

    #[pyo3(name = "__repr__")]
    fn py_repr(&self) -> String {
        format!(
            "RithmicEnvironment.{} (deprecated, use RithmicEnv)",
            self.inner.to_string().to_uppercase()
        )
    }
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
    #[pyo3(signature = (environment, username, password, system_name, app_name="", app_version="1.0", fcm_id=None, ib_id=None, server=None, alt_server=None, enable_history=false))]
    fn py_new(
        environment: PyRithmicEnv,
        username: String,
        password: String,
        system_name: String,
        app_name: &str,
        app_version: &str,
        fcm_id: Option<String>,
        ib_id: Option<String>,
        server: Option<String>,
        alt_server: Option<String>,
        enable_history: bool,
    ) -> Self {
        Self {
            inner: RithmicDataClientConfig {
                environment: environment.inner,
                username,
                password,
                system_name,
                app_name: app_name.to_string(),
                app_version: app_version.to_string(),
                fcm_id,
                ib_id,
                server,
                alt_server,
                enable_history,
            },
        }
    }

    /// Creates configuration from environment variables.
    #[staticmethod]
    #[pyo3(signature = (profile=None))]
    #[pyo3(name = "from_env")]
    fn py_from_env(profile: Option<String>) -> PyResult<Self> {
        let config = RithmicDataClientConfig::from_env_with_profile(profile.as_deref())
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
        PyRithmicEnv {
            inner: self.inner.environment,
        }
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
        trader_id="TRADER-001",
        app_name="",
        app_version="1.0",
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
        trader_id: &str,
        app_name: &str,
        app_version: &str,
        fcm_id: Option<String>,
        ib_id: Option<String>,
        server: Option<String>,
        alt_server: Option<String>,
        execution_replay_lookback_secs: u64,
    ) -> Self {
        Self {
            inner: RithmicExecClientConfig {
                trader_id: TraderId::from(trader_id),
                environment: environment.inner,
                username,
                password,
                system_name,
                app_name: app_name.to_string(),
                app_version: app_version.to_string(),
                fcm_id,
                ib_id,
                account_id,
                server,
                alt_server,
                execution_replay_lookback_secs,
            },
        }
    }

    /// Creates configuration from environment variables.
    #[staticmethod]
    #[pyo3(signature = (profile=None))]
    #[pyo3(name = "from_env")]
    fn py_from_env(profile: Option<String>) -> PyResult<Self> {
        let config = RithmicExecClientConfig::from_env_with_profile(profile.as_deref())
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
        PyRithmicEnv {
            inner: self.inner.environment,
        }
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
    m.add_class::<PyRithmicEnvironment>()?; // Deprecated alias
    m.add_class::<PyRithmicDataClientConfig>()?;
    m.add_class::<PyRithmicExecClientConfig>()?;
    Ok(())
}
