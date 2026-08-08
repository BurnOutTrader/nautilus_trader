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

//! Configuration types for the Rithmic adapter.
//!
//! Configuration can be loaded from environment variables or constructed programmatically.
//!
//! # Environment Variables
//!
//! - `RITHMIC_USERNAME`: Rithmic account username
//! - `RITHMIC_PASSWORD`: Rithmic account password
//! - `RITHMIC_SYSTEM_NAME`: System name for connection
//! - `RITHMIC_APP_NAME`: Application name (required for env loading)
//! - `RITHMIC_APP_VERSION`: Application version
//! - `RITHMIC_FCM_ID`: FCM ID (optional)
//! - `RITHMIC_IB_ID`: IB ID (optional)
//! - `RITHMIC_ACCOUNT_ID`: Trading account ID (for execution)
//! - `RITHMIC_ENV`: Environment (demo, live, test)
//! - `RITHMIC_SERVER`: Named primary server (defaults to Chicago on demo/live, Test on test)
//! - `RITHMIC_ALT_SERVER`: Named alternate server (optional)

use std::{collections::BTreeMap, env, fmt, sync::LazyLock};

use nautilus_model::identifiers::{AccountId, ClientId, TraderId};
use parking_lot::RwLock;
pub use rithmic_rs::RithmicEnv;
use serde::{Deserialize, Serialize};

use crate::{
    common::consts::RITHMIC_VENUE,
    error::{Result, RithmicError},
};

const DEFAULT_APP_NAME: &str = "";
const DEFAULT_APP_VERSION: &str = "1.0";
const RITHMIC_PROFILES_ENV: &str = "RITHMIC_PROFILES";

const fn default_environment() -> RithmicEnv {
    RithmicEnv::Demo
}

fn default_app_version() -> String {
    DEFAULT_APP_VERSION.to_string()
}

const fn default_execution_replay_lookback_secs() -> u64 {
    86_400
}

/// Values loaded from dotenv files are kept inside the adapter instead of mutating the process
/// environment. Process environment variables always take precedence over this cache.
static DOTENV_VALUES: LazyLock<RwLock<BTreeMap<String, String>>> =
    LazyLock::new(|| RwLock::new(BTreeMap::new()));

#[cfg(test)]
pub(crate) static RITHMIC_ENV_TEST_LOCK: LazyLock<std::sync::Mutex<()>> =
    LazyLock::new(|| std::sync::Mutex::new(()));

/// Deprecated: Use [`RithmicEnv`] instead.
///
/// This type alias is provided for backwards compatibility and will be removed
/// in a future major version.
#[deprecated(since = "0.2.0", note = "Use RithmicEnv instead")]
pub type RithmicEnvironment = RithmicEnv;

/// Parses a `RithmicEnv` from a string with flexible aliases.
///
/// Accepts:
/// - "demo" or "paper" → `RithmicEnv::Demo`
/// - "live", "prod", or "production" → `RithmicEnv::Live`
/// - "test" → `RithmicEnv::Test`
pub fn parse_rithmic_env(s: &str) -> Result<RithmicEnv> {
    match s.trim().to_ascii_lowercase().as_str() {
        "demo" | "paper" => Ok(RithmicEnv::Demo),
        "live" | "prod" | "production" => Ok(RithmicEnv::Live),
        "test" => Ok(RithmicEnv::Test),
        _ => Err(RithmicError::Config(format!(
            "Invalid environment: {s}; expected demo, live, or test"
        ))),
    }
}

pub(crate) fn parse_rithmic_trader_id(value: &str) -> Result<TraderId> {
    TraderId::new_checked(value).map_err(|e| {
        RithmicError::Config(format!(
            "Invalid Rithmic trader ID; expected a valid value containing a hyphen: {e}"
        ))
    })
}

fn normalize_env_profile(profile: &str) -> Result<String> {
    if profile.contains(',') {
        return Err(RithmicError::Config(format!(
            "Multiple Rithmic env profiles must be configured via {RITHMIC_PROFILES_ENV}"
        )));
    }

    let normalized: String = profile
        .trim()
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() {
                c.to_ascii_uppercase()
            } else {
                '_'
            }
        })
        .collect();

    let normalized = normalized
        .split('_')
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>()
        .join("_");

    if normalized.is_empty() {
        return Err(RithmicError::Config(
            "Rithmic env profile cannot be empty".to_string(),
        ));
    }

    Ok(normalized)
}

/// Normalizes a component used to construct a Nautilus client identifier.
pub fn normalize_rithmic_client_component(value: &str) -> Result<String> {
    let normalized: String = value
        .trim()
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() {
                c.to_ascii_uppercase()
            } else {
                '_'
            }
        })
        .collect();

    let collapsed = normalized
        .split('_')
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>()
        .join("_");

    if collapsed.is_empty() {
        return Err(RithmicError::Config(
            "Rithmic client component cannot be empty after normalization".to_string(),
        ));
    }

    ClientId::new_checked(&collapsed).map_err(|e| {
        RithmicError::Config(format!("Invalid normalized Rithmic client component: {e}"))
    })?;
    Ok(collapsed)
}

/// Returns the checked Nautilus client ID for a Rithmic data client.
pub fn data_client_id(system_name: &str) -> Result<ClientId> {
    let value = normalize_rithmic_client_component(system_name)?;
    ClientId::new_checked(value)
        .map_err(|e| RithmicError::Config(format!("Invalid Rithmic data client ID: {e}")))
}

/// Returns the checked Nautilus client ID for a Rithmic execution client.
pub fn exec_client_id(system_name: &str, account_id: &str) -> Result<ClientId> {
    let system_key = normalize_rithmic_client_component(system_name)?;
    let account_key = normalize_rithmic_client_component(account_id)?;
    ClientId::new_checked(format!("{system_key}_{account_key}"))
        .map_err(|e| RithmicError::Config(format!("Invalid Rithmic execution client ID: {e}")))
}

/// Returns the checked Nautilus account ID produced by the Rithmic execution factory.
pub fn adapter_account_id(client_id: ClientId, account_id: &str) -> Result<AccountId> {
    AccountId::new_checked(format!("{RITHMIC_VENUE}-{client_id}-{account_id}"))
        .map_err(|e| RithmicError::Config(format!("Invalid Rithmic adapter account ID: {e}")))
}

fn parse_env_profiles_csv(profiles: &str) -> Result<Vec<String>> {
    let mut normalized_seen = std::collections::BTreeSet::new();
    let mut parsed = Vec::new();

    for profile in profiles
        .split(',')
        .map(str::trim)
        .filter(|profile| !profile.is_empty())
    {
        let normalized = normalize_env_profile(profile)?;

        if normalized_seen.insert(normalized) {
            parsed.push(profile.to_string());
        }
    }

    Ok(parsed)
}

/// Returns the configured Rithmic env profiles from `RITHMIC_PROFILES`.
///
/// Entries are parsed from a comma-separated list, trimmed, and de-duplicated
/// by their normalized env token. If the env var is unset or empty, an empty
/// vector is returned.
pub fn configured_env_profiles() -> Result<Vec<String>> {
    match cached_or_process_env_var(RITHMIC_PROFILES_ENV) {
        Some(value) if !value.trim().is_empty() => parse_env_profiles_csv(&value),
        _ => Ok(Vec::new()),
    }
}

/// Loads Rithmic environment variables from a dotenv file.
///
/// Only keys prefixed with `RITHMIC_` are considered. Existing environment
/// variables are left unchanged. Loaded values are stored in an adapter-owned cache and are used
/// by the Rithmic configuration loaders only; the process environment is never mutated.
///
/// Returns the number of keys inserted into the adapter cache.
///
/// If `path` is `None`, this attempts to load `.env` from the current working
/// directory and returns `Ok(0)` when no file is found.
pub fn load_rithmic_env_file(path: Option<&str>) -> Result<usize> {
    let dotenv_iter = match path {
        Some(path) => dotenvy::from_path_iter(path).map_err(|e| {
            RithmicError::Config(format!("Failed to load dotenv file '{path}': {e}"))
        })?,
        None => match dotenvy::dotenv_iter() {
            Ok(iter) => iter,
            Err(e) if e.not_found() => return Ok(0),
            Err(e) => {
                return Err(RithmicError::Config(format!(
                    "Failed to load dotenv file '.env': {e}"
                )));
            }
        },
    };

    let mut loaded = 0;
    let mut dotenv_values = DOTENV_VALUES.write();

    for entry in dotenv_iter {
        let (key, value) =
            entry.map_err(|e| RithmicError::Config(format!("Failed parsing dotenv entry: {e}")))?;

        if !key.starts_with("RITHMIC_")
            || env::var_os(&key).is_some()
            || dotenv_values.contains_key(&key)
        {
            continue;
        }

        dotenv_values.insert(key, value);
        loaded += 1;
    }

    Ok(loaded)
}

fn cached_or_process_env_var(key: &str) -> Option<String> {
    env::var(key)
        .ok()
        .filter(|value| !value.is_empty())
        .or_else(|| DOTENV_VALUES.read().get(key).cloned())
        .filter(|value| !value.is_empty())
}

fn validate_required_config_value(name: &str, value: &str) -> Result<()> {
    if value.trim().is_empty() {
        return Err(RithmicError::Config(format!(
            "Rithmic {name} cannot be empty"
        )));
    }
    Ok(())
}

fn env_candidates(key: &str, profile: Option<&str>) -> Result<Vec<String>> {
    let mut candidates = Vec::new();

    if let Some(profile) = profile {
        candidates.push(format!(
            "RITHMIC_{}_{}",
            normalize_env_profile(profile)?,
            key
        ));
    }
    candidates.push(format!("RITHMIC_{key}"));
    Ok(candidates)
}

pub(crate) fn optional_env_var(key: &str, profile: Option<&str>) -> Result<Option<String>> {
    for candidate in env_candidates(key, profile)? {
        if let Some(value) = cached_or_process_env_var(&candidate) {
            return Ok(Some(value));
        }
    }

    Ok(None)
}

pub(crate) fn required_env_var(key: &str, profile: Option<&str>) -> Result<String> {
    if let Some(value) = optional_env_var(key, profile)? {
        return Ok(value);
    }

    let missing = env_candidates(key, profile)?
        .into_iter()
        .next()
        .unwrap_or_else(|| format!("RITHMIC_{key}"));
    Err(RithmicError::Config(format!("{missing} not set")))
}

/// Configuration for the Rithmic data client.
#[must_use]
#[derive(Clone, Serialize, Deserialize, bon::Builder)]
#[builder(finish_fn(name = build_inner, vis = ""), on(String, into))]
#[serde(default, deny_unknown_fields)]
pub struct RithmicDataClientConfig {
    /// Rithmic environment (Demo, Live, Test).
    #[builder(default = default_environment())]
    pub environment: RithmicEnv,
    /// Rithmic username.
    pub username: String,
    /// Rithmic password.
    pub password: String,
    /// System name for Rithmic connection.
    pub system_name: String,
    /// Application name.
    pub app_name: String,
    /// Application version.
    #[builder(default = default_app_version())]
    pub app_version: String,
    /// FCM ID (Futures Commission Merchant).
    pub fcm_id: Option<String>,
    /// IB ID (Introducing Broker).
    pub ib_id: Option<String>,
    /// Named primary server override.
    pub server: Option<String>,
    /// Named alternate server override.
    pub alt_server: Option<String>,
    /// Whether to connect the history plant for bar replay requests.
    #[builder(default)]
    pub enable_history: bool,
}

impl Default for RithmicDataClientConfig {
    fn default() -> Self {
        Self {
            environment: default_environment(),
            username: String::new(),
            password: String::new(),
            system_name: String::new(),
            app_name: DEFAULT_APP_NAME.to_string(),
            app_version: default_app_version(),
            fcm_id: None,
            ib_id: None,
            server: None,
            alt_server: None,
            enable_history: false,
        }
    }
}

impl<S: rithmic_data_client_config_builder::IsComplete> RithmicDataClientConfigBuilder<S> {
    /// Validates and builds a Rithmic data client configuration.
    pub fn build(self) -> Result<RithmicDataClientConfig> {
        let config = self.build_inner();
        let mut checked = RithmicDataClientConfig::new(
            config.environment,
            config.username,
            config.password,
            config.system_name,
            config.app_name,
        )?;
        checked.app_version = config.app_version;
        checked.fcm_id = config.fcm_id;
        checked.ib_id = config.ib_id;
        checked.server = config.server;
        checked.alt_server = config.alt_server;
        checked.enable_history = config.enable_history;
        checked.validate()?;
        Ok(checked)
    }
}

impl fmt::Debug for RithmicDataClientConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(RithmicDataClientConfig))
            .field("environment", &self.environment)
            .field("username", &self.username)
            .field("password", &"<redacted>")
            .field("system_name", &self.system_name)
            .field("app_name", &self.app_name)
            .field("app_version", &self.app_version)
            .field("fcm_id", &self.fcm_id)
            .field("ib_id", &self.ib_id)
            .field("server", &self.server)
            .field("alt_server", &self.alt_server)
            .field("enable_history", &self.enable_history)
            .finish()
    }
}

impl RithmicDataClientConfig {
    /// Creates a new data client configuration.
    pub fn new(
        environment: RithmicEnv,
        username: impl Into<String>,
        password: impl Into<String>,
        system_name: impl Into<String>,
        app_name: impl Into<String>,
    ) -> Result<Self> {
        let config = Self {
            environment,
            username: username.into(),
            password: password.into(),
            system_name: system_name.into(),
            app_name: app_name.into(),
            ..Self::default()
        };
        config.validate()?;
        Ok(config)
    }

    /// Validates values required before constructing a live Rithmic gateway.
    pub fn validate(&self) -> Result<()> {
        validate_required_config_value("username", &self.username)?;
        validate_required_config_value("password", &self.password)?;
        validate_required_config_value("system_name", &self.system_name)?;
        validate_required_config_value("app_name", &self.app_name)?;
        validate_required_config_value("app_version", &self.app_version)
    }

    /// Creates configuration from environment variables.
    pub fn from_env() -> Result<Self> {
        Self::from_env_with_profile(None)
    }

    /// Creates one or more configurations from environment variables.
    ///
    /// When `RITHMIC_PROFILES` is set to a comma-separated list such as
    /// `Apex,Paper`, one config is loaded per profile in order. Otherwise this
    /// falls back to the canonical `RITHMIC_*` env vars and returns a single
    /// config.
    pub fn from_env_profiles() -> Result<Vec<Self>> {
        let profiles = configured_env_profiles()?;

        if profiles.is_empty() {
            return Ok(vec![Self::from_env()?]);
        }

        profiles
            .iter()
            .map(|profile| Self::from_env_with_profile(Some(profile.as_str())))
            .collect()
    }

    /// Creates configuration from environment variables, optionally scoped by profile.
    pub fn from_env_with_profile(profile: Option<&str>) -> Result<Self> {
        let environment = optional_env_var("ENV", profile)?
            .map_or(Ok(RithmicEnv::Demo), |s| parse_rithmic_env(&s))?;

        let config = Self {
            environment,
            username: required_env_var("USERNAME", profile)?,
            password: required_env_var("PASSWORD", profile)?,
            system_name: required_env_var("SYSTEM_NAME", profile)?,
            app_name: required_env_var("APP_NAME", profile)?,
            app_version: optional_env_var("APP_VERSION", profile)?
                .unwrap_or_else(|| DEFAULT_APP_VERSION.to_string()),
            fcm_id: optional_env_var("FCM_ID", profile)?,
            ib_id: optional_env_var("IB_ID", profile)?,
            server: optional_env_var("SERVER", profile)?,
            alt_server: optional_env_var("ALT_SERVER", profile)?,
            enable_history: optional_env_var("ENABLE_HISTORY", profile)?
                .is_some_and(|v| !matches!(v.to_ascii_lowercase().as_str(), "false" | "0" | "no")),
        };
        config.validate()?;
        Ok(config)
    }

    /// Sets the application name.
    pub fn with_app_name(mut self, app_name: impl Into<String>) -> Self {
        self.app_name = app_name.into();
        self
    }

    /// Sets the application version.
    pub fn with_app_version(mut self, app_version: impl Into<String>) -> Self {
        self.app_version = app_version.into();
        self
    }

    /// Sets the FCM ID.
    pub fn with_fcm_id(mut self, fcm_id: impl Into<String>) -> Self {
        self.fcm_id = Some(fcm_id.into());
        self
    }

    /// Sets the IB ID.
    pub fn with_ib_id(mut self, ib_id: impl Into<String>) -> Self {
        self.ib_id = Some(ib_id.into());
        self
    }
}

/// Configuration for the Rithmic execution client.
#[must_use]
#[derive(Clone, Serialize, Deserialize, bon::Builder)]
#[builder(finish_fn(name = build_inner, vis = ""), on(String, into))]
#[serde(default, deny_unknown_fields)]
pub struct RithmicExecClientConfig {
    /// Trader ID for the client.
    #[builder(default)]
    pub trader_id: TraderId,
    /// Rithmic environment (Demo, Live, Test).
    #[builder(default = default_environment())]
    pub environment: RithmicEnv,
    /// Rithmic username.
    pub username: String,
    /// Rithmic password.
    pub password: String,
    /// System name for Rithmic connection.
    pub system_name: String,
    /// Application name.
    pub app_name: String,
    /// Application version.
    #[builder(default = default_app_version())]
    pub app_version: String,
    /// FCM ID (Futures Commission Merchant).
    pub fcm_id: Option<String>,
    /// IB ID (Introducing Broker).
    pub ib_id: Option<String>,
    /// Trading account ID.
    pub account_id: String,
    /// Named primary server override.
    pub server: Option<String>,
    /// Named alternate server override.
    pub alt_server: Option<String>,
    /// Execution replay lookback window in seconds for connect/reconnect bootstrap.
    #[builder(default = default_execution_replay_lookback_secs())]
    pub execution_replay_lookback_secs: u64,
}

impl Default for RithmicExecClientConfig {
    fn default() -> Self {
        Self {
            trader_id: TraderId::default(),
            environment: default_environment(),
            username: String::new(),
            password: String::new(),
            system_name: String::new(),
            app_name: DEFAULT_APP_NAME.to_string(),
            app_version: default_app_version(),
            fcm_id: None,
            ib_id: None,
            account_id: String::new(),
            server: None,
            alt_server: None,
            execution_replay_lookback_secs: default_execution_replay_lookback_secs(),
        }
    }
}

impl<S: rithmic_exec_client_config_builder::IsComplete> RithmicExecClientConfigBuilder<S> {
    /// Validates and builds a Rithmic execution client configuration.
    pub fn build(self) -> Result<RithmicExecClientConfig> {
        let config = self.build_inner();
        let mut checked = RithmicExecClientConfig::new(
            config.trader_id,
            config.environment,
            config.username,
            config.password,
            config.system_name,
            config.account_id,
            config.app_name,
        )?;
        checked.app_version = config.app_version;
        checked.fcm_id = config.fcm_id;
        checked.ib_id = config.ib_id;
        checked.server = config.server;
        checked.alt_server = config.alt_server;
        checked.execution_replay_lookback_secs = config.execution_replay_lookback_secs;
        checked.validate()?;
        Ok(checked)
    }
}

impl fmt::Debug for RithmicExecClientConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(RithmicExecClientConfig))
            .field("trader_id", &self.trader_id)
            .field("environment", &self.environment)
            .field("username", &self.username)
            .field("password", &"<redacted>")
            .field("system_name", &self.system_name)
            .field("app_name", &self.app_name)
            .field("app_version", &self.app_version)
            .field("fcm_id", &self.fcm_id)
            .field("ib_id", &self.ib_id)
            .field("account_id", &self.account_id)
            .field("server", &self.server)
            .field("alt_server", &self.alt_server)
            .field(
                "execution_replay_lookback_secs",
                &self.execution_replay_lookback_secs,
            )
            .finish()
    }
}

impl RithmicExecClientConfig {
    /// Creates a new execution client configuration.
    pub fn new(
        trader_id: TraderId,
        environment: RithmicEnv,
        username: impl Into<String>,
        password: impl Into<String>,
        system_name: impl Into<String>,
        account_id: impl Into<String>,
        app_name: impl Into<String>,
    ) -> Result<Self> {
        let config = Self {
            trader_id,
            environment,
            username: username.into(),
            password: password.into(),
            system_name: system_name.into(),
            account_id: account_id.into(),
            app_name: app_name.into(),
            ..Self::default()
        };
        config.validate()?;
        Ok(config)
    }

    /// Validates values required before constructing a live Rithmic gateway.
    pub fn validate(&self) -> Result<()> {
        validate_required_config_value("username", &self.username)?;
        validate_required_config_value("password", &self.password)?;
        validate_required_config_value("system_name", &self.system_name)?;
        validate_required_config_value("app_name", &self.app_name)?;
        validate_required_config_value("app_version", &self.app_version)?;
        validate_required_config_value("account_id", &self.account_id)
    }

    /// Creates configuration from environment variables.
    pub fn from_env() -> Result<Self> {
        Self::from_env_with_profile(None)
    }

    /// Creates one or more configurations from environment variables.
    ///
    /// When `RITHMIC_PROFILES` is set to a comma-separated list such as
    /// `Apex,Paper`, one config is loaded per profile in order. Otherwise this
    /// falls back to the canonical `RITHMIC_*` env vars and returns a single
    /// config.
    pub fn from_env_profiles() -> Result<Vec<Self>> {
        let profiles = configured_env_profiles()?;

        if profiles.is_empty() {
            return Ok(vec![Self::from_env()?]);
        }

        profiles
            .iter()
            .map(|profile| Self::from_env_with_profile(Some(profile.as_str())))
            .collect()
    }

    /// Creates configuration from environment variables, optionally scoped by profile.
    pub fn from_env_with_profile(profile: Option<&str>) -> Result<Self> {
        Self::from_env_with_profile_and_overrides(profile, None, None)
    }

    /// Creates configuration from environment variables with optional identity overrides.
    ///
    /// Overrides are resolved before consulting `RITHMIC_*_ACCOUNT_ID` and
    /// `RITHMIC_*_TRADER_ID`, allowing callers to keep node routing identity outside the process
    /// environment while retaining the same checked Rust construction path.
    pub fn from_env_with_profile_and_overrides(
        profile: Option<&str>,
        account_id: Option<&str>,
        trader_id: Option<&str>,
    ) -> Result<Self> {
        let environment = optional_env_var("ENV", profile)?
            .map_or(Ok(RithmicEnv::Demo), |s| parse_rithmic_env(&s))?;

        let trader_id = match trader_id {
            Some(value) => parse_rithmic_trader_id(value),
            None => optional_env_var("TRADER_ID", profile)?.map_or_else(
                || Ok(TraderId::default()),
                |value| parse_rithmic_trader_id(&value),
            ),
        }?;

        let config = Self {
            trader_id,
            environment,
            username: required_env_var("USERNAME", profile)?,
            password: required_env_var("PASSWORD", profile)?,
            system_name: required_env_var("SYSTEM_NAME", profile)?,
            app_name: required_env_var("APP_NAME", profile)?,
            app_version: optional_env_var("APP_VERSION", profile)?
                .unwrap_or_else(|| DEFAULT_APP_VERSION.to_string()),
            fcm_id: optional_env_var("FCM_ID", profile)?,
            ib_id: optional_env_var("IB_ID", profile)?,
            account_id: match account_id {
                Some(value) => value.to_string(),
                None => required_env_var("ACCOUNT_ID", profile)?,
            },
            server: optional_env_var("SERVER", profile)?,
            alt_server: optional_env_var("ALT_SERVER", profile)?,
            execution_replay_lookback_secs: optional_env_var(
                "EXECUTION_REPLAY_LOOKBACK_SECS",
                profile,
            )?
            .map_or(Ok(86_400), |value| {
                value.parse::<u64>().map_err(|e| {
                    RithmicError::Config(format!(
                        "Invalid RITHMIC_EXECUTION_REPLAY_LOOKBACK_SECS value '{value}': {e}"
                    ))
                })
            })?,
        };
        config.validate()?;
        Ok(config)
    }

    /// Sets the application name.
    pub fn with_app_name(mut self, app_name: impl Into<String>) -> Self {
        self.app_name = app_name.into();
        self
    }

    /// Sets the application version.
    pub fn with_app_version(mut self, app_version: impl Into<String>) -> Self {
        self.app_version = app_version.into();
        self
    }

    /// Sets the FCM ID.
    pub fn with_fcm_id(mut self, fcm_id: impl Into<String>) -> Self {
        self.fcm_id = Some(fcm_id.into());
        self
    }

    /// Sets the IB ID.
    pub fn with_ib_id(mut self, ib_id: impl Into<String>) -> Self {
        self.ib_id = Some(ib_id.into());
        self
    }

    /// Sets the execution replay lookback window in seconds.
    pub const fn with_execution_replay_lookback_secs(mut self, value: u64) -> Self {
        self.execution_replay_lookback_secs = value;
        self
    }
}

#[cfg(test)]
#[allow(unsafe_code, reason = "test environment mutation is serialized")]
mod tests {
    use std::{
        fs,
        path::PathBuf,
        time::{SystemTime, UNIX_EPOCH},
    };

    use super::*;

    fn set_env(key: &str, value: Option<&str>) -> Option<String> {
        let previous = std::env::var(key).ok();

        match value {
            Some(value) => unsafe { std::env::set_var(key, value) },
            None => unsafe { std::env::remove_var(key) },
        }
        previous
    }

    fn restore_env(entries: &[(&str, Option<String>)]) {
        for (key, value) in entries {
            match value {
                Some(value) => unsafe { std::env::set_var(key, value) },
                None => unsafe { std::env::remove_var(key) },
            }
        }
    }

    #[rstest::rstest]
    fn test_parse_rithmic_env() {
        assert_eq!(parse_rithmic_env("demo").unwrap(), RithmicEnv::Demo);
        assert_eq!(parse_rithmic_env("live").unwrap(), RithmicEnv::Live);
        assert_eq!(parse_rithmic_env("test").unwrap(), RithmicEnv::Test);
        assert!(parse_rithmic_env("invalid").is_err());
    }

    #[rstest::rstest]
    fn test_data_client_config_new() {
        let config =
            RithmicDataClientConfig::new(RithmicEnv::Demo, "user", "pass", "system", "TestApp")
                .unwrap()
                .with_fcm_id("FCM001");

        assert_eq!(config.app_name, "TestApp");
        assert_eq!(config.fcm_id, Some("FCM001".to_string()));
    }

    #[rstest::rstest]
    fn test_checked_builders_reject_invalid_optional_values() {
        let data = RithmicDataClientConfig::builder()
            .username("user")
            .password("pass")
            .system_name("system")
            .app_name("TestApp")
            .app_version("")
            .build();
        let exec = RithmicExecClientConfig::builder()
            .username("user")
            .password("pass")
            .system_name("system")
            .app_name("TestApp")
            .account_id("account")
            .app_version("")
            .build();

        assert!(data.is_err());
        assert!(exec.is_err());
    }

    #[rstest::rstest]
    fn test_new_rejects_empty_app_name() {
        assert!(
            RithmicDataClientConfig::new(RithmicEnv::Demo, "user", "pass", "system", "").is_err()
        );
        assert!(
            RithmicExecClientConfig::new(
                TraderId::default(),
                RithmicEnv::Demo,
                "user",
                "pass",
                "system",
                "account",
                "",
            )
            .is_err()
        );
    }

    #[rstest::rstest]
    fn test_deserialization_rejects_unknown_fields() {
        let result = serde_json::from_str::<RithmicDataClientConfig>(r#"{"unknown":true}"#);
        assert!(result.is_err());
    }

    #[rstest::rstest]
    fn test_data_client_config_from_profile_env() {
        let _guard = RITHMIC_ENV_TEST_LOCK.lock().unwrap();
        let previous = [
            (RITHMIC_PROFILES_ENV, set_env(RITHMIC_PROFILES_ENV, None)),
            (
                "RITHMIC_APEX_ENV",
                set_env("RITHMIC_APEX_ENV", Some("live")),
            ),
            (
                "RITHMIC_APEX_USERNAME",
                set_env("RITHMIC_APEX_USERNAME", Some("user")),
            ),
            (
                "RITHMIC_APEX_PASSWORD",
                set_env("RITHMIC_APEX_PASSWORD", Some("pass")),
            ),
            (
                "RITHMIC_APEX_SYSTEM_NAME",
                set_env("RITHMIC_APEX_SYSTEM_NAME", Some("Apex")),
            ),
            (
                "RITHMIC_APEX_APP_NAME",
                set_env("RITHMIC_APEX_APP_NAME", Some("MyApp")),
            ),
            (
                "RITHMIC_APEX_FCM_ID",
                set_env("RITHMIC_APEX_FCM_ID", Some("fcm")),
            ),
            (
                "RITHMIC_USERNAME",
                set_env("RITHMIC_USERNAME", Some("legacy-user")),
            ),
        ];

        let config = RithmicDataClientConfig::from_env_with_profile(Some("Apex")).unwrap();

        assert_eq!(config.environment, RithmicEnv::Live);
        assert_eq!(config.username, "user");
        assert_eq!(config.password, "pass");
        assert_eq!(config.system_name, "Apex");
        assert_eq!(config.app_name, "MyApp");
        assert_eq!(config.fcm_id.as_deref(), Some("fcm"));

        restore_env(&previous);
    }

    #[rstest::rstest]
    fn test_exec_client_config_profile_falls_back_to_canonical_env() {
        let _guard = RITHMIC_ENV_TEST_LOCK.lock().unwrap();
        let previous = [
            (RITHMIC_PROFILES_ENV, set_env(RITHMIC_PROFILES_ENV, None)),
            ("RITHMIC_ENV", set_env("RITHMIC_ENV", Some("demo"))),
            (
                "RITHMIC_USERNAME",
                set_env("RITHMIC_USERNAME", Some("user")),
            ),
            (
                "RITHMIC_PASSWORD",
                set_env("RITHMIC_PASSWORD", Some("pass")),
            ),
            (
                "RITHMIC_SYSTEM_NAME",
                set_env("RITHMIC_SYSTEM_NAME", Some("system")),
            ),
            (
                "RITHMIC_APP_NAME",
                set_env("RITHMIC_APP_NAME", Some("OwnApp")),
            ),
            (
                "RITHMIC_ACCOUNT_ID",
                set_env("RITHMIC_ACCOUNT_ID", Some("account")),
            ),
            ("RITHMIC_EMPTY_ENV", set_env("RITHMIC_EMPTY_ENV", None)),
        ];

        let config = RithmicExecClientConfig::from_env_with_profile(Some("empty")).unwrap();

        assert_eq!(config.environment, RithmicEnv::Demo);
        assert_eq!(config.username, "user");
        assert_eq!(config.account_id, "account");
        assert_eq!(config.app_name, "OwnApp");

        restore_env(&previous);
    }

    #[rstest::rstest]
    fn test_exec_env_identity_overrides_do_not_require_identity_env_vars() {
        let _guard = RITHMIC_ENV_TEST_LOCK.lock().unwrap();
        let previous = [
            (
                "RITHMIC_USERNAME",
                set_env("RITHMIC_USERNAME", Some("user")),
            ),
            (
                "RITHMIC_PASSWORD",
                set_env("RITHMIC_PASSWORD", Some("pass")),
            ),
            (
                "RITHMIC_SYSTEM_NAME",
                set_env("RITHMIC_SYSTEM_NAME", Some("system")),
            ),
            (
                "RITHMIC_APP_NAME",
                set_env("RITHMIC_APP_NAME", Some("OwnApp")),
            ),
            ("RITHMIC_ACCOUNT_ID", set_env("RITHMIC_ACCOUNT_ID", None)),
            ("RITHMIC_TRADER_ID", set_env("RITHMIC_TRADER_ID", None)),
        ];

        let config = RithmicExecClientConfig::from_env_with_profile_and_overrides(
            None,
            Some("account"),
            Some("TESTER-001"),
        )
        .unwrap();

        assert_eq!(config.account_id, "account");
        assert_eq!(config.trader_id.as_str(), "TESTER-001");

        restore_env(&previous);
    }

    #[rstest::rstest]
    fn test_configured_env_profiles_parses_csv() {
        let _guard = RITHMIC_ENV_TEST_LOCK.lock().unwrap();
        let previous = [(
            RITHMIC_PROFILES_ENV,
            set_env(RITHMIC_PROFILES_ENV, Some(" apex , paper,APEX ")),
        )];

        let profiles = configured_env_profiles().unwrap();
        assert_eq!(profiles, vec!["apex", "paper"]);

        restore_env(&previous);
    }

    #[rstest::rstest]
    fn test_data_client_config_from_env_profiles() {
        let _guard = RITHMIC_ENV_TEST_LOCK.lock().unwrap();
        let previous = [
            (
                RITHMIC_PROFILES_ENV,
                set_env(RITHMIC_PROFILES_ENV, Some("Apex,Paper")),
            ),
            (
                "RITHMIC_APEX_USERNAME",
                set_env("RITHMIC_APEX_USERNAME", Some("apex-user")),
            ),
            (
                "RITHMIC_APEX_PASSWORD",
                set_env("RITHMIC_APEX_PASSWORD", Some("apex-pass")),
            ),
            (
                "RITHMIC_APEX_SYSTEM_NAME",
                set_env("RITHMIC_APEX_SYSTEM_NAME", Some("Apex")),
            ),
            (
                "RITHMIC_APEX_APP_NAME",
                set_env("RITHMIC_APEX_APP_NAME", Some("ApexApp")),
            ),
            (
                "RITHMIC_PAPER_USERNAME",
                set_env("RITHMIC_PAPER_USERNAME", Some("paper-user")),
            ),
            (
                "RITHMIC_PAPER_PASSWORD",
                set_env("RITHMIC_PAPER_PASSWORD", Some("paper-pass")),
            ),
            (
                "RITHMIC_PAPER_SYSTEM_NAME",
                set_env("RITHMIC_PAPER_SYSTEM_NAME", Some("Paper")),
            ),
            (
                "RITHMIC_PAPER_APP_NAME",
                set_env("RITHMIC_PAPER_APP_NAME", Some("PaperApp")),
            ),
        ];

        let configs = RithmicDataClientConfig::from_env_profiles().unwrap();
        assert_eq!(configs.len(), 2);
        assert_eq!(configs[0].system_name, "Apex");
        assert_eq!(configs[1].system_name, "Paper");

        restore_env(&previous);
    }

    #[rstest::rstest]
    fn test_exec_client_config_from_env_profiles() {
        let _guard = RITHMIC_ENV_TEST_LOCK.lock().unwrap();
        let previous = [
            (
                RITHMIC_PROFILES_ENV,
                set_env(RITHMIC_PROFILES_ENV, Some("Apex,Paper")),
            ),
            (
                "RITHMIC_APEX_USERNAME",
                set_env("RITHMIC_APEX_USERNAME", Some("apex-user")),
            ),
            (
                "RITHMIC_APEX_PASSWORD",
                set_env("RITHMIC_APEX_PASSWORD", Some("apex-pass")),
            ),
            (
                "RITHMIC_APEX_SYSTEM_NAME",
                set_env("RITHMIC_APEX_SYSTEM_NAME", Some("Apex")),
            ),
            (
                "RITHMIC_APEX_ACCOUNT_ID",
                set_env("RITHMIC_APEX_ACCOUNT_ID", Some("APEX-001")),
            ),
            (
                "RITHMIC_APEX_APP_NAME",
                set_env("RITHMIC_APEX_APP_NAME", Some("ApexApp")),
            ),
            (
                "RITHMIC_PAPER_USERNAME",
                set_env("RITHMIC_PAPER_USERNAME", Some("paper-user")),
            ),
            (
                "RITHMIC_PAPER_PASSWORD",
                set_env("RITHMIC_PAPER_PASSWORD", Some("paper-pass")),
            ),
            (
                "RITHMIC_PAPER_SYSTEM_NAME",
                set_env("RITHMIC_PAPER_SYSTEM_NAME", Some("Paper")),
            ),
            (
                "RITHMIC_PAPER_ACCOUNT_ID",
                set_env("RITHMIC_PAPER_ACCOUNT_ID", Some("PAPER-001")),
            ),
            (
                "RITHMIC_PAPER_APP_NAME",
                set_env("RITHMIC_PAPER_APP_NAME", Some("PaperApp")),
            ),
        ];

        let configs = RithmicExecClientConfig::from_env_profiles().unwrap();
        assert_eq!(configs.len(), 2);
        assert_eq!(configs[0].account_id, "APEX-001");
        assert_eq!(configs[1].account_id, "PAPER-001");

        restore_env(&previous);
    }

    #[rstest::rstest]
    fn test_load_rithmic_env_file_caches_only_missing_rithmic_keys() {
        let _guard = RITHMIC_ENV_TEST_LOCK.lock().unwrap();
        DOTENV_VALUES.write().clear();
        let previous = [
            (
                "RITHMIC_USERNAME",
                set_env("RITHMIC_USERNAME", Some("existing-user")),
            ),
            ("RITHMIC_APP_NAME", set_env("RITHMIC_APP_NAME", None)),
            ("IGNORED_KEY", set_env("IGNORED_KEY", None)),
        ];

        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path: PathBuf = std::env::temp_dir().join(format!("rithmic-env-{unique}.env"));
        fs::write(
            &path,
            "RITHMIC_USERNAME=file-user\nRITHMIC_APP_NAME=OwnApp\nIGNORED_KEY=ignored\n",
        )
        .unwrap();

        let loaded = load_rithmic_env_file(Some(path.to_string_lossy().as_ref())).unwrap();
        assert_eq!(loaded, 1);
        assert_eq!(
            std::env::var("RITHMIC_USERNAME").as_deref(),
            Ok("existing-user")
        );
        assert!(std::env::var("RITHMIC_APP_NAME").is_err());
        assert_eq!(
            optional_env_var("APP_NAME", None).unwrap().as_deref(),
            Some("OwnApp")
        );
        assert!(std::env::var("IGNORED_KEY").is_err());

        fs::remove_file(path).unwrap();
        DOTENV_VALUES.write().clear();
        restore_env(&previous);
    }

    #[rstest::rstest]
    fn test_load_rithmic_env_file_missing_path_errors() {
        let _guard = RITHMIC_ENV_TEST_LOCK.lock().unwrap();
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path: PathBuf = std::env::temp_dir().join(format!("missing-rithmic-env-{unique}.env"));

        let result = load_rithmic_env_file(Some(path.to_string_lossy().as_ref()));
        assert!(result.is_err());
    }

    #[rstest::rstest]
    fn test_checked_client_and_account_ids_match_factory_convention() {
        let data_id = data_client_id("Rithmic Paper Trading").unwrap();
        let exec_id = exec_client_id("Apex", "PA-123456").unwrap();
        let account_id = adapter_account_id(exec_id, "PA-123456").unwrap();

        assert_eq!(data_id.as_str(), "RITHMIC_PAPER_TRADING");
        assert_eq!(exec_id.as_str(), "APEX_PA_123456");
        assert_eq!(account_id.as_str(), "RITHMIC-APEX_PA_123456-PA-123456");
    }

    #[rstest::rstest]
    fn test_client_component_normalization_is_ascii_only() {
        assert_eq!(normalize_rithmic_client_component("ÅPEX").unwrap(), "PEX");
        assert!(normalize_rithmic_client_component("東京").is_err());
    }
}
