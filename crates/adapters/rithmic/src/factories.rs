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

//! Factory types for creating Rithmic data and execution clients (v2 PyO3 path).
//!
//! These factories implement the NautilusTrader `DataClientFactory` and
//! `ExecutionClientFactory` traits, enabling use with `LiveNode` and the
//! `get_global_pyo3_registry()` Python registry.
//!
//! # Example (Rust)
//!
//! ```rust,ignore
//! let factory = RithmicDataClientFactory::new();
//! let config  = RithmicDataClientConfig::from_env()?;
//! let node    = LiveNode::builder(trader_id, Environment::Live)?
//!     .add_data_client(None, Box::new(factory), Box::new(config))?
//!     .build()?;
//! ```

use std::{any::Any, cell::RefCell, rc::Rc};

use nautilus_common::{
    clients::{DataClient, ExecutionClient},
    clock::Clock,
    factories::{ClientConfig, DataClientFactory, ExecutionClientFactory},
};
use nautilus_live::ExecutionClientCore;
use nautilus_model::{
    enums::{AccountType, OmsType},
    identifiers::{AccountId, ClientId, Venue},
};

use crate::{
    config::{RithmicDataClientConfig, RithmicExecClientConfig},
    data::live::RithmicLiveDataClient,
    execution::live::RithmicLiveExecClient,
};

const RITHMIC_VENUE: &str = "RITHMIC";

fn normalize_rithmic_client_component(value: &str) -> anyhow::Result<String> {
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
        anyhow::bail!("Rithmic client component cannot be empty after normalization");
    }

    if collapsed.contains('-') {
        anyhow::bail!("Rithmic client component cannot contain '-'");
    }

    Ok(collapsed)
}

fn data_client_id(system_name: &str) -> anyhow::Result<ClientId> {
    Ok(ClientId::from(
        normalize_rithmic_client_component(system_name)?.as_str(),
    ))
}

fn exec_client_id(system_name: &str, account_id: &str) -> anyhow::Result<ClientId> {
    let system_key = normalize_rithmic_client_component(system_name)?;
    let account_key = normalize_rithmic_client_component(account_id)?;
    Ok(ClientId::from(
        format!("{system_key}_{account_key}").as_str(),
    ))
}

fn adapter_account_id(client_id: ClientId, account_id: &str) -> AccountId {
    AccountId::from(format!("{RITHMIC_VENUE}-{client_id}-{account_id}").as_str())
}

// ---- ClientConfig marker impls ------------------------------------------------------------------

impl ClientConfig for RithmicDataClientConfig {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl ClientConfig for RithmicExecClientConfig {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

// ---- RithmicDataClientFactory -------------------------------------------------------------------

/// Factory for creating Rithmic live data clients.
#[derive(Debug, Clone)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.adapters.rithmic", from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.rithmic")
)]
pub struct RithmicDataClientFactory;

impl RithmicDataClientFactory {
    /// Creates a new [`RithmicDataClientFactory`].
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
}

impl Default for RithmicDataClientFactory {
    fn default() -> Self {
        Self::new()
    }
}

impl DataClientFactory for RithmicDataClientFactory {
    /// Creates a new [`RithmicLiveDataClient`].
    ///
    /// # Errors
    ///
    /// Returns an error if `config` is not a [`RithmicDataClientConfig`] or if
    /// the data event sender is unavailable.
    fn create(
        &self,
        _name: &str,
        config: &dyn ClientConfig,
        _cache: nautilus_common::cache::CacheView,
        _clock: Rc<RefCell<dyn Clock>>,
    ) -> anyhow::Result<Box<dyn DataClient>> {
        let rithmic_config = config
            .as_any()
            .downcast_ref::<RithmicDataClientConfig>()
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "Invalid config type for RithmicDataClientFactory. \
                     Expected RithmicDataClientConfig, received {config:?}"
                )
            })?
            .clone();

        let client_id = data_client_id(&rithmic_config.system_name)?;
        let client = RithmicLiveDataClient::new(client_id, rithmic_config);
        Ok(Box::new(client))
    }

    fn name(&self) -> &'static str {
        "RITHMIC"
    }

    fn config_type(&self) -> &'static str {
        "RithmicDataClientConfig"
    }
}

// ---- RithmicExecClientFactory -------------------------------------------------------------------

/// Factory for creating Rithmic live execution clients.
#[derive(Debug, Clone)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.adapters.rithmic", from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.rithmic")
)]
pub struct RithmicExecClientFactory;

impl RithmicExecClientFactory {
    /// Creates a new [`RithmicExecClientFactory`].
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
}

impl Default for RithmicExecClientFactory {
    fn default() -> Self {
        Self::new()
    }
}

impl ExecutionClientFactory for RithmicExecClientFactory {
    /// Creates a new [`RithmicLiveExecClient`].
    ///
    /// # Errors
    ///
    /// Returns an error if `config` is not a [`RithmicExecClientConfig`].
    fn create(
        &self,
        _name: &str,
        config: &dyn ClientConfig,
        cache: nautilus_common::cache::CacheView,
    ) -> anyhow::Result<Box<dyn ExecutionClient>> {
        let rithmic_config = config
            .as_any()
            .downcast_ref::<RithmicExecClientConfig>()
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "Invalid config type for RithmicExecClientFactory. \
                     Expected RithmicExecClientConfig, received {config:?}"
                )
            })?
            .clone();

        let client_id = exec_client_id(&rithmic_config.system_name, &rithmic_config.account_id)?;
        let venue = Venue::from(RITHMIC_VENUE);
        let account_id = adapter_account_id(client_id, &rithmic_config.account_id);

        let core = ExecutionClientCore::new(
            rithmic_config.trader_id,
            client_id,
            venue,
            OmsType::Netting, // Rithmic futures are netting by default
            account_id,
            AccountType::Margin,
            None, // base_currency
            cache,
        );

        let client = RithmicLiveExecClient::new(core, rithmic_config);
        Ok(Box::new(client))
    }

    fn name(&self) -> &'static str {
        "RITHMIC"
    }

    fn config_type(&self) -> &'static str {
        "RithmicExecClientConfig"
    }
}

// ---- Tests --------------------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use nautilus_common::{cache::Cache, clock::TestClock};
    use nautilus_model::identifiers::TraderId;

    use super::*;
    use crate::config::{RithmicDataClientConfig, RithmicEnv, RithmicExecClientConfig};

    fn make_data_config() -> RithmicDataClientConfig {
        RithmicDataClientConfig::new(RithmicEnv::Demo, "user", "pass", "TestSystem")
    }

    fn make_exec_config() -> RithmicExecClientConfig {
        RithmicExecClientConfig::new(
            TraderId::from("TRADER-001"),
            RithmicEnv::Demo,
            "user",
            "pass",
            "TestSystem",
            "ACC-001",
        )
    }

    #[rstest::rstest]
    fn test_data_factory_name_and_config_type() {
        let factory = RithmicDataClientFactory::new();
        assert_eq!(factory.name(), "RITHMIC");
        assert_eq!(factory.config_type(), "RithmicDataClientConfig");
    }

    #[rstest::rstest]
    fn test_exec_factory_name_and_config_type() {
        let factory = RithmicExecClientFactory::new();
        assert_eq!(factory.name(), "RITHMIC");
        assert_eq!(factory.config_type(), "RithmicExecClientConfig");
    }

    #[rstest::rstest]
    fn test_data_client_config_as_any_downcasts() {
        let config = make_data_config();
        let boxed: Box<dyn ClientConfig> = Box::new(config.clone());
        let downcast = boxed.as_any().downcast_ref::<RithmicDataClientConfig>();
        assert!(downcast.is_some());
        assert_eq!(downcast.unwrap().username, config.username);
    }

    #[rstest::rstest]
    fn test_exec_client_config_as_any_downcasts() {
        let config = make_exec_config();
        let boxed: Box<dyn ClientConfig> = Box::new(config.clone());
        let downcast = boxed.as_any().downcast_ref::<RithmicExecClientConfig>();
        assert!(downcast.is_some());
        assert_eq!(downcast.unwrap().account_id, config.account_id);
    }

    #[rstest::rstest]
    fn test_data_client_config_wrong_type_returns_none() {
        let config = make_exec_config();
        let boxed: Box<dyn ClientConfig> = Box::new(config);
        // Trying to downcast exec config as data config should fail
        let downcast = boxed.as_any().downcast_ref::<RithmicDataClientConfig>();
        assert!(downcast.is_none());
    }

    #[rstest::rstest]
    fn test_data_factory_rejects_wrong_config_type() {
        let factory = RithmicDataClientFactory::new();
        let wrong_config = make_exec_config();
        // Pass exec config where data config is expected → error
        let result = factory.create(
            "RITHMIC",
            &wrong_config,
            std::rc::Rc::new(std::cell::RefCell::new(Cache::default())).into(),
            std::rc::Rc::new(std::cell::RefCell::new(TestClock::new())),
        );
        assert!(result.is_err());
        let msg = result.err().unwrap().to_string();
        assert!(msg.contains("RithmicDataClientConfig"), "{msg}");
    }

    #[rstest::rstest]
    fn test_normalize_rithmic_client_component() {
        assert_eq!(normalize_rithmic_client_component("Apex").unwrap(), "APEX");
        assert_eq!(
            normalize_rithmic_client_component("Rithmic Paper Trading").unwrap(),
            "RITHMIC_PAPER_TRADING"
        );
        assert_eq!(
            normalize_rithmic_client_component("PA-123456").unwrap(),
            "PA_123456"
        );
    }

    #[rstest::rstest]
    fn test_factory_uses_system_name_for_data_client_id() {
        let factory = RithmicDataClientFactory::new();
        let config = make_data_config();
        let client = factory
            .create(
                "RITHMIC",
                &config,
                Rc::new(RefCell::new(Cache::default())).into(),
                Rc::new(RefCell::new(TestClock::new())),
            )
            .unwrap();

        assert_eq!(client.client_id(), ClientId::from("TESTSYSTEM"));
        assert_eq!(client.venue(), Some(Venue::from("RITHMIC")));
    }

    #[rstest::rstest]
    fn test_factory_uses_system_name_and_account_for_exec_identity() {
        let factory = RithmicExecClientFactory::new();
        let config = make_exec_config();
        let client = factory
            .create(
                "RITHMIC",
                &config,
                Rc::new(RefCell::new(Cache::default())).into(),
            )
            .unwrap();

        assert_eq!(client.client_id(), ClientId::from("TESTSYSTEM_ACC_001"));
        assert_eq!(
            client.account_id(),
            AccountId::from("RITHMIC-TESTSYSTEM_ACC_001-ACC-001")
        );
    }

    #[rstest::rstest]
    fn test_factory_supports_multiple_exec_clients_for_same_system_name() {
        let factory = RithmicExecClientFactory::new();

        let mut config_a = make_exec_config();
        config_a.system_name = "Apex".to_string();
        config_a.account_id = "PA-123456".to_string();
        let client_a = factory
            .create(
                "RITHMIC_APEX_EXEC_1",
                &config_a,
                Rc::new(RefCell::new(Cache::default())).into(),
            )
            .unwrap();

        let mut config_b = make_exec_config();
        config_b.system_name = "Apex".to_string();
        config_b.account_id = "PA-654321".to_string();
        let client_b = factory
            .create(
                "RITHMIC_APEX_EXEC_2",
                &config_b,
                Rc::new(RefCell::new(Cache::default())).into(),
            )
            .unwrap();

        assert_eq!(client_a.venue(), client_b.venue());
        assert_eq!(client_a.venue(), Venue::from("RITHMIC"));
        assert_eq!(client_a.client_id(), ClientId::from("APEX_PA_123456"));
        assert_eq!(client_b.client_id(), ClientId::from("APEX_PA_654321"));
        assert_eq!(
            client_a.account_id(),
            AccountId::from("RITHMIC-APEX_PA_123456-PA-123456")
        );
        assert_eq!(
            client_b.account_id(),
            AccountId::from("RITHMIC-APEX_PA_654321-PA-654321")
        );
    }
}

// ---- PyO3 pymethods -----------------------------------------------------------------------------

#[cfg(feature = "python")]
#[pyo3::pymethods]
impl RithmicDataClientFactory {
    #[new]
    fn py_new() -> Self {
        Self::new()
    }

    fn name(&self) -> &'static str {
        "RITHMIC"
    }

    fn __repr__(&self) -> &'static str {
        "RithmicDataClientFactory()"
    }
}

#[cfg(feature = "python")]
#[pyo3::pymethods]
impl RithmicExecClientFactory {
    #[new]
    fn py_new() -> Self {
        Self::new()
    }

    fn name(&self) -> &'static str {
        "RITHMIC"
    }

    fn __repr__(&self) -> &'static str {
        "RithmicExecClientFactory()"
    }
}
