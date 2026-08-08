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

use std::{fmt::Debug, fs, path::Path, sync::LazyLock};

use nautilus_core::correctness::CorrectnessResult;
use nautilus_model::{
    enums::AccountType,
    identifiers::{AccountId, TraderId},
};
use parking_lot::RwLock;
use zeroize::{Zeroize, ZeroizeOnDrop, Zeroizing};

use crate::{common::enums::ProjectXEnvironment, http::credentials::ProjectXCredential};

const DEFAULT_HTTP_TIMEOUT_SECS: u64 = 60;
const DEFAULT_MAX_RETRIES: u32 = 3;
const DEFAULT_RETRY_DELAY_INITIAL_MS: u64 = 1_000;
const DEFAULT_RETRY_DELAY_MAX_MS: u64 = 10_000;

#[derive(Clone, bon::Builder)]
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
    #[builder(default = DEFAULT_HTTP_TIMEOUT_SECS)]
    pub http_timeout_secs: u64,
    #[builder(default = DEFAULT_MAX_RETRIES)]
    pub max_retries: u32,
    #[builder(default = DEFAULT_RETRY_DELAY_INITIAL_MS)]
    pub retry_delay_initial_ms: u64,
    #[builder(default = DEFAULT_RETRY_DELAY_MAX_MS)]
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
            .field(
                "http_proxy_url",
                &self.http_proxy_url.as_ref().map(|_| "<redacted>"),
            )
            .finish()
    }
}

impl Drop for ProjectXConfig {
    fn drop(&mut self) {
        self.http_proxy_url.zeroize();
    }
}

impl ProjectXConfig {
    #[must_use]
    pub fn new(
        environment: ProjectXEnvironment,
        user_name: impl Into<String>,
        api_key: impl Into<String>,
    ) -> Self {
        Self::builder()
            .credential(ProjectXCredential::new(environment, user_name, api_key))
            .build()
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
        self.http_proxy_url.zeroize();
        self.http_proxy_url = Some(value.into());
        self
    }

    /// Applies optional transport overrides while retaining the canonical defaults.
    #[must_use]
    pub fn with_optional_overrides(
        mut self,
        http_timeout_secs: Option<u64>,
        max_retries: Option<u32>,
        retry_delay_initial_ms: Option<u64>,
        retry_delay_max_ms: Option<u64>,
        http_proxy_url: Option<String>,
    ) -> Self {
        if let Some(value) = http_timeout_secs {
            self.http_timeout_secs = value;
        }
        if let Some(value) = max_retries {
            self.max_retries = value;
        }
        if let Some(value) = retry_delay_initial_ms {
            self.retry_delay_initial_ms = value;
        }
        if let Some(value) = retry_delay_max_ms {
            self.retry_delay_max_ms = value;
        }
        if let Some(value) = http_proxy_url {
            self.http_proxy_url.zeroize();
            self.http_proxy_url = Some(value);
        }
        self
    }
}

#[cfg(feature = "python")]
nautilus_core::impl_pyo3_config_getters!(ProjectXConfig {
    http_timeout_secs: u64,
    max_retries: u32,
    retry_delay_initial_ms: u64,
    retry_delay_max_ms: u64,
    http_proxy_url: Option<String>,
});

/// Adds the ProjectX issuer to an account ID when needed.
///
/// # Errors
///
/// Returns an error when the prefixed value exceeds the [`AccountId`] constraints.
pub fn canonicalize_projectx_account_id(account_id: AccountId) -> CorrectnessResult<AccountId> {
    if account_id.get_issuer().as_str() == "PROJECTX" {
        Ok(account_id)
    } else {
        AccountId::new_checked(format!("PROJECTX-{}", account_id.as_str()))
    }
}

/// Converts either a Nautilus account ID or a raw ProjectX account label without panicking.
///
/// # Errors
///
/// Returns an error when `raw` cannot form a valid ASCII [`AccountId`].
pub fn projectx_account_id_from_raw(raw: &str) -> CorrectnessResult<AccountId> {
    match AccountId::new_checked(raw) {
        Ok(account_id) => canonicalize_projectx_account_id(account_id),
        Err(e) if raw.contains('-') => Err(e),
        Err(_) => AccountId::new_checked(format!("PROJECTX-{raw}")),
    }
}

#[derive(Default, ZeroizeOnDrop)]
struct ProjectXDotenvCredentials {
    user_name: Option<String>,
    api_key: Option<String>,
}

static PROJECTX_DOTENV_CREDENTIALS: LazyLock<RwLock<ProjectXDotenvCredentials>> =
    LazyLock::new(|| RwLock::new(ProjectXDotenvCredentials::default()));

fn projectx_env_credential_is_set(key: &str) -> bool {
    std::env::var(key).is_ok_and(|value| {
        let value = Zeroizing::new(value);
        !value.trim().is_empty()
    })
}

#[must_use]
#[cfg(any(feature = "python", test))]
pub(crate) fn projectx_cached_credential(key: &str) -> Option<String> {
    let cache = PROJECTX_DOTENV_CREDENTIALS.read();

    match key {
        "PROJECTX_USERNAME" => cache.user_name.clone(),
        "PROJECTX_API_KEY" => cache.api_key.clone(),
        _ => None,
    }
}

/// Loads ProjectX credentials from a dotenv-style file into an adapter-local cache.
///
/// This loader only imports:
/// - `PROJECTX_USERNAME`
/// - `PROJECTX_API_KEY`
///
/// Returns the number of cached credential values updated by this call.
pub fn load_projectx_credentials_from_dotenv(
    path: Option<&str>,
    override_existing: bool,
) -> anyhow::Result<usize> {
    let path = path.unwrap_or(".env");
    let env_path = Path::new(path);

    if !env_path.exists() {
        return Ok(0);
    }

    let content = Zeroizing::new(fs::read_to_string(env_path)?);
    let mut user_name: Option<&str> = None;
    let mut api_key: Option<&str> = None;

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
        let value = raw_value.trim();
        let value = value
            .strip_prefix('"')
            .and_then(|value| value.strip_suffix('"'))
            .or_else(|| {
                value
                    .strip_prefix('\'')
                    .and_then(|value| value.strip_suffix('\''))
            })
            .unwrap_or(value);

        match key {
            "PROJECTX_USERNAME" => user_name = Some(value),
            "PROJECTX_API_KEY" => api_key = Some(value),
            _ => {}
        }
    }

    let mut loaded = 0usize;
    let mut cache = PROJECTX_DOTENV_CREDENTIALS.write();

    if let Some(value) = user_name
        && (override_existing
            || (cache.user_name.is_none() && !projectx_env_credential_is_set("PROJECTX_USERNAME")))
    {
        cache.user_name.zeroize();
        cache.user_name = Some(value.to_string());
        loaded += 1;
    }

    if let Some(value) = api_key
        && (override_existing
            || (cache.api_key.is_none() && !projectx_env_credential_is_set("PROJECTX_API_KEY")))
    {
        cache.api_key.zeroize();
        cache.api_key = Some(value.to_string());
        loaded += 1;
    }

    Ok(loaded)
}

/// Configuration for the ProjectX data client.
#[derive(Clone, Debug, bon::Builder)]
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
    #[builder(default)]
    pub market_data_live: bool,
}

impl ProjectXDataClientConfig {
    #[must_use]
    pub fn new(
        environment: ProjectXEnvironment,
        user_name: impl Into<String>,
        api_key: impl Into<String>,
    ) -> Self {
        Self::builder()
            .transport(ProjectXConfig::new(environment, user_name, api_key))
            .build()
    }
}

#[cfg(feature = "python")]
nautilus_core::impl_pyo3_config_getters!(ProjectXDataClientConfig {
    transport: ProjectXConfig,
    market_data_live: bool,
});

/// Configuration for the ProjectX execution client.
#[derive(Clone, Debug, bon::Builder)]
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
    #[builder(default = AccountType::Margin)]
    pub account_type: AccountType,
    pub transport: ProjectXConfig,
}

impl ProjectXExecClientConfig {
    /// Creates a ProjectX execution client configuration.
    ///
    /// # Errors
    ///
    /// Returns an error when `account_id` cannot be canonicalized with the ProjectX issuer.
    pub fn new(
        trader_id: TraderId,
        account_id: AccountId,
        environment: ProjectXEnvironment,
        user_name: impl Into<String>,
        api_key: impl Into<String>,
    ) -> CorrectnessResult<Self> {
        Ok(Self::builder()
            .trader_id(trader_id)
            .account_id(canonicalize_projectx_account_id(account_id)?)
            .transport(ProjectXConfig::new(environment, user_name, api_key))
            .build())
    }
}

#[cfg(feature = "python")]
nautilus_core::impl_pyo3_config_getters!(ProjectXExecClientConfig {
    trader_id: TraderId,
    account_id: AccountId,
    account_type: AccountType,
    transport: ProjectXConfig,
});

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
        assert_eq!(
            canonicalize_projectx_account_id(account_id).unwrap(),
            account_id,
        );
    }

    #[rstest::rstest]
    fn canonicalize_projectx_account_id_wraps_raw_topstep_label() {
        let account_id = AccountId::from("PRAC-V2-64413-98419885");
        assert_eq!(
            canonicalize_projectx_account_id(account_id).unwrap(),
            AccountId::from("PROJECTX-PRAC-V2-64413-98419885"),
        );
    }

    #[rstest::rstest]
    fn projectx_account_id_from_raw_accepts_non_account_id_labels() {
        assert_eq!(
            projectx_account_id_from_raw("DEMO001").unwrap(),
            AccountId::from("PROJECTX-DEMO001"),
        );
    }

    #[rstest::rstest]
    #[case("")]
    #[case("PROJECTX-")]
    #[case("💥")]
    fn projectx_account_id_checked_rejects_invalid_values(#[case] value: &str) {
        assert!(projectx_account_id_from_raw(value).is_err());
    }

    #[rstest::rstest]
    fn projectx_exec_config_new_defaults_to_margin_account_type() {
        let config = ProjectXExecClientConfig::new(
            TraderId::from("TRADER-001"),
            AccountId::from("PRAC-V2-64413-98419885"),
            ProjectXEnvironment::TopstepX,
            "",
            "",
        )
        .unwrap();
        assert_eq!(config.account_type, AccountType::Margin);
    }

    #[rstest::rstest]
    fn load_projectx_credentials_from_dotenv_loads_projectx_keys() {
        let _guard = DOTENV_LOCK.lock().unwrap();
        *super::PROJECTX_DOTENV_CREDENTIALS.write() = ProjectXDotenvCredentials::default();

        let tmp =
            std::env::temp_dir().join(format!("projectx-env-test-{}.env", std::process::id()));
        std::fs::write(
            &tmp,
            "PROJECTX_USERNAME=test_user\nPROJECTX_API_KEY=test_key\nUNRELATED=ignored\n",
        )
        .expect("should write temp env file");

        let loaded = load_projectx_credentials_from_dotenv(tmp.to_str(), true)
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
        *super::PROJECTX_DOTENV_CREDENTIALS.write() = ProjectXDotenvCredentials {
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
