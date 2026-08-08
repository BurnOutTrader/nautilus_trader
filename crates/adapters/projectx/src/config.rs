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

use std::{
    fmt::Debug,
    fs,
    path::Path,
    sync::{LazyLock, RwLock},
};

use nautilus_model::{
    enums::AccountType,
    identifiers::{AccountId, TraderId},
};

use crate::{common::enums::ProjectXEnvironment, http::credentials::ProjectXCredential};

#[derive(Clone)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.adapters.projectx", from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.projectx")
)]
pub struct ProjectXConfig {
    pub credential: ProjectXCredential,
    pub http_timeout_secs: u64,
    pub max_retries: u32,
    pub retry_delay_initial_ms: u64,
    pub retry_delay_max_ms: u64,
    pub http_proxy_url: Option<String>,
}

impl Debug for ProjectXConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(ProjectXConfig))
            .field("credential", &self.credential)
            .field("http_timeout_secs", &self.http_timeout_secs)
            .field("max_retries", &self.max_retries)
            .field("retry_delay_initial_ms", &self.retry_delay_initial_ms)
            .field("retry_delay_max_ms", &self.retry_delay_max_ms)
            .field("http_proxy_url", &self.http_proxy_url)
            .finish()
    }
}

impl ProjectXConfig {
    #[must_use]
    pub fn new(
        environment: ProjectXEnvironment,
        user_name: impl Into<String>,
        api_key: impl Into<String>,
    ) -> Self {
        Self {
            credential: ProjectXCredential::new(environment, user_name, api_key),
            http_timeout_secs: 60,
            max_retries: 3,
            retry_delay_initial_ms: 1_000,
            retry_delay_max_ms: 10_000,
            http_proxy_url: None,
        }
    }

    #[must_use]
    pub fn with_http_timeout_secs(mut self, value: u64) -> Self {
        self.http_timeout_secs = value;
        self
    }

    #[must_use]
    pub fn with_max_retries(mut self, value: u32) -> Self {
        self.max_retries = value;
        self
    }

    #[must_use]
    pub fn with_retry_delay_initial_ms(mut self, value: u64) -> Self {
        self.retry_delay_initial_ms = value;
        self
    }

    #[must_use]
    pub fn with_retry_delay_max_ms(mut self, value: u64) -> Self {
        self.retry_delay_max_ms = value;
        self
    }

    #[must_use]
    pub fn with_http_proxy_url(mut self, value: impl Into<String>) -> Self {
        self.http_proxy_url = Some(value.into());
        self
    }
}

#[must_use]
pub fn canonicalize_projectx_account_id(account_id: AccountId) -> AccountId {
    if account_id.get_issuer().as_str() == "PROJECTX" {
        account_id
    } else {
        AccountId::new(format!("PROJECTX-{}", account_id.as_str()))
    }
}

#[must_use]
pub fn projectx_account_id_from_raw(raw: &str) -> AccountId {
    AccountId::new_checked(raw).map_or_else(
        |_| AccountId::new(format!("PROJECTX-{raw}")),
        canonicalize_projectx_account_id,
    )
}

#[derive(Clone, Debug, Default)]
struct ProjectXDotenvCredentials {
    user_name: Option<String>,
    api_key: Option<String>,
}

static PROJECTX_DOTENV_CREDENTIALS: LazyLock<RwLock<ProjectXDotenvCredentials>> =
    LazyLock::new(|| RwLock::new(ProjectXDotenvCredentials::default()));

#[must_use]
pub fn projectx_cached_credential(key: &str) -> Option<String> {
    let cache = PROJECTX_DOTENV_CREDENTIALS
        .read()
        .expect("PROJECTX dotenv lock poisoned");

    match key {
        "PROJECTX_USERNAME" => cache.user_name.clone(),
        "PROJECTX_API_KEY" => cache.api_key.clone(),
        _ => None,
    }
}

/// Loads ProjectX credentials from a dotenv-style file into process env vars.
///
/// This loader only imports:
/// - `PROJECTX_USERNAME`
/// - `PROJECTX_API_KEY`
///
/// Returns the number of variables set by this call.
pub fn load_projectx_credentials_from_dotenv(
    path: Option<&str>,
    override_existing: bool,
) -> anyhow::Result<usize> {
    let path = path.unwrap_or(".env");
    let env_path = Path::new(path);

    if !env_path.exists() {
        return Ok(0);
    }

    let content = fs::read_to_string(env_path)?;
    let mut user_name: Option<String> = None;
    let mut api_key: Option<String> = None;

    for raw_line in content.lines() {
        let line = raw_line.trim();

        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        let line = line.strip_prefix("export ").unwrap_or(line);
        let Some((raw_key, raw_value)) = line.split_once('=') else {
            continue;
        };

        let key = raw_key.trim();
        let mut value = raw_value.trim().to_string();

        if value.len() >= 2 {
            let bytes = value.as_bytes();
            let first = bytes[0] as char;
            let last = bytes[value.len() - 1] as char;

            if (first == '"' && last == '"') || (first == '\'' && last == '\'') {
                value = value[1..value.len() - 1].to_string();
            }
        }

        match key {
            "PROJECTX_USERNAME" => user_name = Some(value),
            "PROJECTX_API_KEY" => api_key = Some(value),
            _ => {}
        }
    }

    let mut loaded = 0usize;
    let mut cache = PROJECTX_DOTENV_CREDENTIALS
        .write()
        .expect("PROJECTX dotenv lock poisoned");

    if let Some(value) = user_name
        && (override_existing || cache.user_name.is_none())
    {
        cache.user_name = Some(value);
        loaded += 1;
    }

    if let Some(value) = api_key
        && (override_existing || cache.api_key.is_none())
    {
        cache.api_key = Some(value);
        loaded += 1;
    }

    Ok(loaded)
}

/// Configuration for the ProjectX data client.
#[derive(Clone, Debug)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.adapters.projectx", from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.projectx")
)]
pub struct ProjectXDataClientConfig {
    pub transport: ProjectXConfig,
    pub market_data_live: bool,
}

impl Default for ProjectXDataClientConfig {
    fn default() -> Self {
        Self {
            transport: ProjectXConfig::new(ProjectXEnvironment::TopstepX, "test-user", "test-key"),
            market_data_live: false,
        }
    }
}

impl ProjectXDataClientConfig {
    #[must_use]
    pub fn new(
        environment: ProjectXEnvironment,
        user_name: impl Into<String>,
        api_key: impl Into<String>,
    ) -> Self {
        Self {
            transport: ProjectXConfig::new(environment, user_name, api_key),
            market_data_live: false,
        }
    }
}

/// Configuration for the ProjectX execution client.
#[derive(Clone, Debug)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.adapters.projectx", from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.projectx")
)]
pub struct ProjectXExecClientConfig {
    pub trader_id: TraderId,
    pub account_id: AccountId,
    pub account_type: AccountType,
    pub transport: ProjectXConfig,
}

impl Default for ProjectXExecClientConfig {
    fn default() -> Self {
        Self {
            trader_id: TraderId::from("TRADER-001"),
            account_id: AccountId::from("PROJECTX-001"),
            account_type: AccountType::Margin,
            transport: ProjectXConfig::new(ProjectXEnvironment::TopstepX, "test-user", "test-key"),
        }
    }
}

impl ProjectXExecClientConfig {
    #[must_use]
    pub fn new(
        trader_id: TraderId,
        account_id: AccountId,
        environment: ProjectXEnvironment,
        user_name: impl Into<String>,
        api_key: impl Into<String>,
    ) -> Self {
        Self {
            trader_id,
            account_id: canonicalize_projectx_account_id(account_id),
            account_type: AccountType::Margin,
            transport: ProjectXConfig::new(environment, user_name, api_key),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use nautilus_model::{
        enums::AccountType,
        identifiers::{AccountId, TraderId},
    };

    use super::{
        ProjectXDotenvCredentials, ProjectXEnvironment, ProjectXExecClientConfig,
        canonicalize_projectx_account_id, load_projectx_credentials_from_dotenv,
        projectx_account_id_from_raw, projectx_cached_credential,
    };

    static DOTENV_LOCK: Mutex<()> = Mutex::new(());

    #[rstest::rstest]
    fn canonicalize_projectx_account_id_preserves_projectx_issuer() {
        let account_id = AccountId::from("PROJECTX-PRAC-V2-64413-98419885");
        assert_eq!(canonicalize_projectx_account_id(account_id), account_id);
    }

    #[rstest::rstest]
    fn canonicalize_projectx_account_id_wraps_raw_topstep_label() {
        let account_id = AccountId::from("PRAC-V2-64413-98419885");
        assert_eq!(
            canonicalize_projectx_account_id(account_id),
            AccountId::from("PROJECTX-PRAC-V2-64413-98419885"),
        );
    }

    #[rstest::rstest]
    fn projectx_account_id_from_raw_accepts_non_account_id_labels() {
        assert_eq!(
            projectx_account_id_from_raw("DEMO001"),
            AccountId::from("PROJECTX-DEMO001"),
        );
    }

    #[rstest::rstest]
    fn projectx_exec_config_defaults_to_margin_account_type() {
        let config = ProjectXExecClientConfig::default();
        assert_eq!(config.account_type, AccountType::Margin);
    }

    #[rstest::rstest]
    fn projectx_exec_config_new_defaults_to_margin_account_type() {
        let config = ProjectXExecClientConfig::new(
            TraderId::from("TRADER-001"),
            AccountId::from("PRAC-V2-64413-98419885"),
            ProjectXEnvironment::TopstepX,
            "",
            "",
        );
        assert_eq!(config.account_type, AccountType::Margin);
    }

    #[rstest::rstest]
    fn load_projectx_credentials_from_dotenv_loads_projectx_keys() {
        let _guard = DOTENV_LOCK.lock().unwrap();
        *super::PROJECTX_DOTENV_CREDENTIALS
            .write()
            .expect("PROJECTX dotenv lock poisoned") = ProjectXDotenvCredentials::default();

        let tmp =
            std::env::temp_dir().join(format!("projectx-env-test-{}.env", std::process::id()));
        std::fs::write(
            &tmp,
            "PROJECTX_USERNAME=test_user\nPROJECTX_API_KEY=test_key\nUNRELATED=ignored\n",
        )
        .expect("should write temp env file");

        let loaded = load_projectx_credentials_from_dotenv(tmp.to_str(), false)
            .expect("dotenv load should succeed");
        assert_eq!(loaded, 2);
        assert_eq!(
            projectx_cached_credential("PROJECTX_USERNAME"),
            Some("test_user".to_string())
        );
        assert_eq!(
            projectx_cached_credential("PROJECTX_API_KEY"),
            Some("test_key".to_string())
        );

        let _ = std::fs::remove_file(tmp);
    }

    #[rstest::rstest]
    fn load_projectx_credentials_from_dotenv_respects_override_flag() {
        let _guard = DOTENV_LOCK.lock().unwrap();
        *super::PROJECTX_DOTENV_CREDENTIALS
            .write()
            .expect("PROJECTX dotenv lock poisoned") = ProjectXDotenvCredentials {
            user_name: Some("existing_user".to_string()),
            api_key: Some("existing_key".to_string()),
        };

        let tmp = std::env::temp_dir().join(format!(
            "projectx-env-test-override-{}.env",
            std::process::id()
        ));
        std::fs::write(
            &tmp,
            "PROJECTX_USERNAME=new_user\nPROJECTX_API_KEY=new_key\n",
        )
        .expect("should write temp env file");

        let loaded_no_override = load_projectx_credentials_from_dotenv(tmp.to_str(), false)
            .expect("dotenv load should succeed");
        assert_eq!(loaded_no_override, 0);
        assert_eq!(
            projectx_cached_credential("PROJECTX_USERNAME"),
            Some("existing_user".to_string())
        );
        assert_eq!(
            projectx_cached_credential("PROJECTX_API_KEY"),
            Some("existing_key".to_string())
        );

        let loaded_override = load_projectx_credentials_from_dotenv(tmp.to_str(), true)
            .expect("dotenv load with override should succeed");
        assert_eq!(loaded_override, 2);
        assert_eq!(
            projectx_cached_credential("PROJECTX_USERNAME"),
            Some("new_user".to_string())
        );
        assert_eq!(
            projectx_cached_credential("PROJECTX_API_KEY"),
            Some("new_key".to_string())
        );

        let _ = std::fs::remove_file(tmp);
    }
}
