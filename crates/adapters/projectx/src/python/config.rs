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

fn parse_account_type(value: &str) -> PyResult<AccountType> {
    match value.to_ascii_lowercase().as_str() {
        "cash" => Ok(AccountType::Cash),
        "margin" => Ok(AccountType::Margin),
        "betting" => Ok(AccountType::Betting),
        "wallet" => Ok(AccountType::Wallet),
        _ => Err(to_pyvalue_err(format!(
            "Invalid ProjectX account_type '{value}'"
        ))),
    }
}

fn resolve_projectx_credentials(
    user_name: Option<String>,
    api_key: Option<String>,
) -> PyResult<(String, String)> {
    fn pick(value: Option<String>, env_key: &str) -> Option<String> {
        value
            .and_then(|v| {
                let trimmed = v.trim().to_string();
                (!trimmed.is_empty()).then_some(trimmed)
            })
            .or_else(|| {
                std::env::var(env_key).ok().and_then(|v| {
                    let trimmed = v.trim().to_string();
                    (!trimmed.is_empty()).then_some(trimmed)
                })
            })
            .or_else(|| projectx_cached_credential(env_key))
    }

    let user_name = pick(user_name, "PROJECTX_USERNAME").ok_or_else(|| {
        to_pyvalue_err("ProjectX username is required (pass `user_name` or set PROJECTX_USERNAME)")
    })?;
    let api_key = pick(api_key, "PROJECTX_API_KEY").ok_or_else(|| {
        to_pyvalue_err("ProjectX API key is required (pass `api_key` or set PROJECTX_API_KEY)")
    })?;
    Ok((user_name, api_key))
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
        let (user_name, api_key) = resolve_projectx_credentials(user_name, api_key)?;
        let mut config = Self::new(
            parse_environment_or_default(environment)?,
            user_name,
            api_key,
        );

        if let Some(value) = http_timeout_secs {
            config = config.with_http_timeout_secs(value);
        }

        if let Some(value) = max_retries {
            config = config.with_max_retries(value);
        }

        if let Some(value) = retry_delay_initial_ms {
            config = config.with_retry_delay_initial_ms(value);
        }

        if let Some(value) = retry_delay_max_ms {
            config = config.with_retry_delay_max_ms(value);
        }

        if let Some(value) = http_proxy_url {
            config = config.with_http_proxy_url(value);
        }
        Ok(config)
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
        let (user_name, api_key) = resolve_projectx_credentials(user_name, api_key)?;
        let mut config = ProjectXConfig::new(
            parse_environment_or_default(environment)?,
            user_name,
            api_key,
        );

        if let Some(value) = http_timeout_secs {
            config = config.with_http_timeout_secs(value);
        }

        if let Some(value) = max_retries {
            config = config.with_max_retries(value);
        }

        if let Some(value) = retry_delay_initial_ms {
            config = config.with_retry_delay_initial_ms(value);
        }

        if let Some(value) = retry_delay_max_ms {
            config = config.with_retry_delay_max_ms(value);
        }

        if let Some(value) = http_proxy_url {
            config = config.with_http_proxy_url(value);
        }
        Ok(Self {
            transport: config,
            market_data_live,
        })
    }

    fn __repr__(&self) -> String {
        format!("{self:?}")
    }

    #[getter]
    fn transport(&self) -> ProjectXConfig {
        self.transport.clone()
    }

    #[getter]
    fn market_data_live(&self) -> bool {
        self.market_data_live
    }
}

#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl ProjectXExecClientConfig {
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
        account_type: Option<&str>,
    ) -> PyResult<Self> {
        let (user_name, api_key) = resolve_projectx_credentials(user_name, api_key)?;
        let mut transport = ProjectXConfig::new(
            parse_environment_or_default(environment)?,
            user_name,
            api_key,
        );

        if let Some(value) = http_timeout_secs {
            transport = transport.with_http_timeout_secs(value);
        }

        if let Some(value) = max_retries {
            transport = transport.with_max_retries(value);
        }

        if let Some(value) = retry_delay_initial_ms {
            transport = transport.with_retry_delay_initial_ms(value);
        }

        if let Some(value) = retry_delay_max_ms {
            transport = transport.with_retry_delay_max_ms(value);
        }

        if let Some(value) = http_proxy_url {
            transport = transport.with_http_proxy_url(value);
        }

        Ok(Self {
            trader_id: TraderId::from(trader_id.as_str()),
            account_id: projectx_account_id_from_raw(account_id.as_str()),
            account_type: parse_account_type(account_type.unwrap_or("margin"))?,
            transport,
        })
    }

    fn __repr__(&self) -> String {
        format!("{self:?}")
    }

    #[getter]
    fn transport(&self) -> ProjectXConfig {
        self.transport.clone()
    }
}
