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

use nautilus_core::python::to_pyvalue_err;
use nautilus_model::{enums::AccountType, identifiers::TraderId};
use pyo3::prelude::*;

use crate::{
    common::enums::ProjectXEnvironment,
    config::{
        ProjectXConfig, ProjectXDataClientConfig, ProjectXExecClientConfig,
        load_projectx_credentials_from_dotenv, projectx_account_id_from_raw,
        projectx_cached_credential,
    },
};

fn parse_environment(value: &str) -> PyResult<ProjectXEnvironment> {
    match value.to_ascii_lowercase().as_str() {
        "topstep" | "topstepx" | "live" => Ok(ProjectXEnvironment::TopstepX),
        _ => Err(to_pyvalue_err(format!(
            "Invalid ProjectX environment '{value}'. Supported values: 'topstep', 'topstepx', 'live'"
        ))),
    }
}

fn parse_environment_or_default(value: Option<String>) -> PyResult<ProjectXEnvironment> {
    match value {
        Some(value) => parse_environment(&value),
        None => Ok(ProjectXEnvironment::TopstepX),
    }
}

fn resolve_projectx_credentials(
    user_name: Option<String>,
    api_key: Option<String>,
) -> PyResult<(String, String)> {
    fn non_empty(value: String) -> Option<String> {
        (!value.trim().is_empty()).then_some(value)
    }

    fn pick(value: Option<String>, env_key: &str) -> Option<String> {
        value
            .and_then(non_empty)
            .or_else(|| projectx_cached_credential(env_key).and_then(non_empty))
            .or_else(|| std::env::var(env_key).ok().and_then(non_empty))
    }

    let user_name = pick(user_name, "PROJECTX_USERNAME").ok_or_else(|| {
        to_pyvalue_err("ProjectX username is required (pass `user_name` or set PROJECTX_USERNAME)")
    })?;
    let api_key = pick(api_key, "PROJECTX_API_KEY").ok_or_else(|| {
        to_pyvalue_err("ProjectX API key is required (pass `api_key` or set PROJECTX_API_KEY)")
    })?;
    Ok((user_name, api_key))
}

#[allow(clippy::too_many_arguments)]
fn build_transport_config(
    environment: Option<String>,
    user_name: Option<String>,
    api_key: Option<String>,
    http_timeout_secs: Option<u64>,
    max_retries: Option<u32>,
    retry_delay_initial_ms: Option<u64>,
    retry_delay_max_ms: Option<u64>,
    http_proxy_url: Option<String>,
) -> PyResult<ProjectXConfig> {
    let (user_name, api_key) = resolve_projectx_credentials(user_name, api_key)?;
    Ok(ProjectXConfig::new(
        parse_environment_or_default(environment)?,
        user_name,
        api_key,
    )
    .with_optional_overrides(
        http_timeout_secs,
        max_retries,
        retry_delay_initial_ms,
        retry_delay_max_ms,
        http_proxy_url,
    ))
}

#[pyo3_stub_gen::derive::gen_stub_pyfunction(module = "nautilus_trader.adapters.projectx")]
#[pyfunction]
#[pyo3(signature = (path = None, override_existing = false))]
#[allow(clippy::needless_pass_by_value)]
pub fn load_projectx_env(path: Option<String>, override_existing: bool) -> PyResult<usize> {
    load_projectx_credentials_from_dotenv(path.as_deref(), override_existing)
        .map_err(to_pyvalue_err)
}

#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl ProjectXConfig {
    #[new]
    #[pyo3(signature = (
        environment = None,
        user_name = None,
        api_key = None,
        http_timeout_secs = None,
        max_retries = None,
        retry_delay_initial_ms = None,
        retry_delay_max_ms = None,
        http_proxy_url = None,
    ))]
    #[allow(clippy::needless_pass_by_value, clippy::too_many_arguments)]
    fn py_new(
        environment: Option<String>,
        user_name: Option<String>,
        api_key: Option<String>,
        http_timeout_secs: Option<u64>,
        max_retries: Option<u32>,
        retry_delay_initial_ms: Option<u64>,
        retry_delay_max_ms: Option<u64>,
        http_proxy_url: Option<String>,
    ) -> PyResult<Self> {
        build_transport_config(
            environment,
            user_name,
            api_key,
            http_timeout_secs,
            max_retries,
            retry_delay_initial_ms,
            retry_delay_max_ms,
            http_proxy_url,
        )
    }

    fn __repr__(&self) -> String {
        format!("{self:?}")
    }
}

#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl ProjectXDataClientConfig {
    #[new]
    #[pyo3(signature = (
        environment = None,
        user_name = None,
        api_key = None,
        http_timeout_secs = None,
        max_retries = None,
        retry_delay_initial_ms = None,
        retry_delay_max_ms = None,
        http_proxy_url = None,
        market_data_live = false,
    ))]
    #[allow(clippy::needless_pass_by_value, clippy::too_many_arguments)]
    fn py_new(
        environment: Option<String>,
        user_name: Option<String>,
        api_key: Option<String>,
        http_timeout_secs: Option<u64>,
        max_retries: Option<u32>,
        retry_delay_initial_ms: Option<u64>,
        retry_delay_max_ms: Option<u64>,
        http_proxy_url: Option<String>,
        market_data_live: bool,
    ) -> PyResult<Self> {
        let transport = build_transport_config(
            environment,
            user_name,
            api_key,
            http_timeout_secs,
            max_retries,
            retry_delay_initial_ms,
            retry_delay_max_ms,
            http_proxy_url,
        )?;
        Ok(Self::builder()
            .transport(transport)
            .market_data_live(market_data_live)
            .build())
    }

    fn __repr__(&self) -> String {
        format!("{self:?}")
    }
}

#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl ProjectXExecClientConfig {
    /// Creates an execution configuration from the established string-based Python API.
    ///
    /// Both identifiers are parsed with checked Rust constructors so invalid Python input is
    /// reported as `ValueError` rather than reaching an infallible constructor or panicking.
    #[new]
    #[pyo3(signature = (
        trader_id,
        account_id,
        environment = None,
        user_name = None,
        api_key = None,
        http_timeout_secs = None,
        max_retries = None,
        retry_delay_initial_ms = None,
        retry_delay_max_ms = None,
        http_proxy_url = None,
        account_type = None,
    ))]
    #[allow(clippy::needless_pass_by_value, clippy::too_many_arguments)]
    fn py_new(
        trader_id: String,
        account_id: String,
        environment: Option<String>,
        user_name: Option<String>,
        api_key: Option<String>,
        http_timeout_secs: Option<u64>,
        max_retries: Option<u32>,
        retry_delay_initial_ms: Option<u64>,
        retry_delay_max_ms: Option<u64>,
        http_proxy_url: Option<String>,
        account_type: Option<AccountType>,
    ) -> PyResult<Self> {
        let trader_id = TraderId::new_checked(trader_id).map_err(to_pyvalue_err)?;
        let account_id = projectx_account_id_from_raw(&account_id).map_err(to_pyvalue_err)?;
        let account_type = account_type.unwrap_or(AccountType::Margin);
        if account_type != AccountType::Margin {
            return Err(to_pyvalue_err(format!(
                "ProjectX futures accounts require AccountType::Margin, received {account_type:?}"
            )));
        }
        let transport = build_transport_config(
            environment,
            user_name,
            api_key,
            http_timeout_secs,
            max_retries,
            retry_delay_initial_ms,
            retry_delay_max_ms,
            http_proxy_url,
        )?;

        Ok(Self::builder()
            .trader_id(trader_id)
            .account_id(account_id)
            .account_type(account_type)
            .transport(transport)
            .build())
    }

    fn __repr__(&self) -> String {
        format!("{self:?}")
    }
}

#[cfg(test)]
mod tests {
    use nautilus_model::enums::AccountType;

    use super::*;

    fn exec_config(
        trader_id: &str,
        account_id: &str,
        account_type: Option<AccountType>,
    ) -> PyResult<ProjectXExecClientConfig> {
        ProjectXExecClientConfig::py_new(
            trader_id.to_string(),
            account_id.to_string(),
            None,
            Some("test-user".to_string()),
            Some("test-api-key".to_string()),
            None,
            None,
            None,
            None,
            None,
            account_type,
        )
    }

    #[rstest::rstest]
    #[case("")]
    #[case("bad")]
    #[case("💥")]
    fn invalid_python_trader_id_returns_error(#[case] trader_id: &str) {
        assert!(exec_config(trader_id, "PROJECTX-12345", None).is_err());
    }

    #[rstest::rstest]
    #[case("")]
    #[case("PROJECTX-")]
    #[case("💥")]
    fn invalid_python_account_id_returns_error(#[case] account_id: &str) {
        assert!(exec_config("TRADER-001", account_id, None).is_err());
    }

    #[rstest::rstest]
    fn non_margin_python_account_type_returns_error() {
        assert!(exec_config("TRADER-001", "PROJECTX-12345", Some(AccountType::Cash)).is_err());
    }

    #[rstest::rstest]
    fn valid_python_exec_config_defaults_to_margin() {
        let config =
            exec_config("TRADER-001", "DEMO001", None).expect("valid ProjectX exec config");

        assert_eq!(config.trader_id.as_str(), "TRADER-001");
        assert_eq!(config.account_id.as_str(), "PROJECTX-DEMO001");
        assert_eq!(config.account_type, AccountType::Margin);
    }
}
