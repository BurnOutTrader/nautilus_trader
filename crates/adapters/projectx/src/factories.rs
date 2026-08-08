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

//! Factory helpers and client factories for ProjectX.

use std::{any::Any, cell::RefCell, rc::Rc};

use crate::{
    common::{
        consts::{PROJECT_X, PROJECTX_VENUE},
        symbols::{format_databento_symbol, parse_databento_symbol, projectx_to_databento_symbol},
    },
    config::{ProjectXDataClientConfig, ProjectXExecClientConfig},
    data::ProjectXDataClient,
    execution::ProjectXExecutionClient,
};
use anyhow::Context;
use nautilus_common::{
    cache::CacheView,
    clients::{DataClient, ExecutionClient},
    clock::Clock,
    factories::{ClientConfig, DataClientFactory, ExecutionClientFactory},
};
use nautilus_core::{Params, UnixNanos, time::get_atomic_clock_realtime};
use nautilus_live::ExecutionClientCore;
use nautilus_model::{
    enums::{AccountType, AssetClass, OmsType},
    identifiers::{ClientId, InstrumentId, Symbol},
    instruments::{FuturesContract, InstrumentAny},
    types::{Currency, Price, Quantity},
};
use projectx_client::Contract;
use rust_decimal::Decimal;
use serde_json::Value;
#[must_use]
pub fn normalize_projectx_symbol_key(value: &str) -> String {
    value.trim().to_ascii_uppercase()
}

fn parsed_symbol(value: &str) -> Option<(String, char, u16)> {
    parse_databento_symbol(value).ok()
}

#[must_use]
pub fn canonical_projectx_public_symbol(contract: &Contract) -> String {
    let translated = projectx_to_databento_symbol(contract.id.as_ref()).ok();
    let translated_parts = translated.as_deref().and_then(parsed_symbol);
    let name = normalize_projectx_symbol_key(&contract.name);

    if let Some(name_parts) = parsed_symbol(&name) {
        if let Some((_, translated_month, translated_year)) = translated_parts.as_ref()
            && *translated_month == name_parts.1
        {
            return format_databento_symbol(&name_parts.0, name_parts.1, *translated_year);
        }

        return name;
    }

    let symbol_id = normalize_projectx_symbol_key(contract.symbol_id.as_ref());

    if let Some(symbol_parts) = parsed_symbol(&symbol_id) {
        if let Some((_, translated_month, translated_year)) = translated_parts.as_ref()
            && *translated_month == symbol_parts.1
        {
            return format_databento_symbol(&symbol_parts.0, symbol_parts.1, *translated_year);
        }

        return symbol_id;
    }

    translated.unwrap_or_else(|| contract.id.to_string())
}

#[must_use]
pub fn projectx_contract_matches_product_root(
    contract: &Contract,
    product_root: Option<&str>,
) -> bool {
    let Some(product_root) = product_root else {
        return true;
    };

    let target = product_root.trim().to_ascii_uppercase();

    if target.is_empty() {
        return true;
    }

    projectx_contract_root_and_sort_key(contract).is_some_and(|(root, _, _)| root == target)
}

#[must_use]
pub fn projectx_select_front_month_contract(
    contracts: &[Contract],
    product_root: &str,
) -> Option<Contract> {
    let target_root = product_root.trim().to_ascii_uppercase();
    let select = |active_only: bool| {
        contracts
            .iter()
            .filter(|contract| !active_only || contract.active_contract)
            .filter_map(|contract| {
                let (root, year_two, month_index) = projectx_contract_root_and_sort_key(contract)?;
                (root == target_root).then_some((year_two, month_index, contract.clone()))
            })
            .min_by_key(|(year_two, month_index, _)| (*year_two, *month_index))
            .map(|(_, _, contract)| contract)
    };

    select(true).or_else(|| select(false))
}

/// Converts a ProjectX contract payload into a Nautilus futures instrument.
///
/// The conversion is shared between the Rust runtime path, the PyO3 HTTP helper,
/// and the Python instrument-provider workflow to keep live/bootstrap/catalog
/// behavior consistent across v2 Rust and v2 PyO3 entry points.
///
/// # Errors
///
/// Returns an error if the ProjectX contract symbol cannot be normalized into a
/// Nautilus futures instrument identifier.
pub fn projectx_contract_to_instrument(contract: &Contract) -> anyhow::Result<InstrumentAny> {
    let public_symbol = canonical_projectx_public_symbol(contract);
    let (root, _, _) = parse_databento_symbol(&public_symbol)?;
    let raw_symbol = Symbol::new_checked(public_symbol.as_str())
        .context("invalid ProjectX contract public symbol")?;
    let instrument_id = InstrumentId::new(raw_symbol, *PROJECTX_VENUE);
    let activation_ns = UnixNanos::default();
    // ProjectX does not provide contract lifecycle timestamps. Futures instruments always expose
    // an expiration to Nautilus, so use the non-expiring sentinel rather than marking every
    // contract expired at the Unix epoch.
    let expiration_ns = UnixNanos::from(u64::MAX);
    let ts_init = get_atomic_clock_realtime().get_time_ns();
    anyhow::ensure!(
        contract.tick_size > Decimal::ZERO,
        "ProjectX contract {} tick size must be positive, was {}",
        contract.id,
        contract.tick_size,
    );
    anyhow::ensure!(
        contract.tick_value > Decimal::ZERO,
        "ProjectX contract {} tick value must be positive, was {}",
        contract.id,
        contract.tick_value,
    );
    let price_precision = contract.tick_size.normalize().scale() as u8;
    let price_increment = Price::from_decimal_dp(contract.tick_size, price_precision)
        .context("invalid ProjectX contract tick size")?;
    let multiplier_decimal = contract
        .tick_value
        .checked_div(contract.tick_size)
        .context("ProjectX tick value to tick size ratio overflowed")?;
    let multiplier = Quantity::from_decimal_dp(
        multiplier_decimal,
        multiplier_decimal.normalize().scale() as u8,
    )
    .context("invalid ProjectX contract multiplier")?;
    let lot_size =
        Quantity::from_decimal_dp(Decimal::ONE, 0).context("invalid ProjectX contract lot size")?;

    let mut info = Params::new();
    info.insert(
        "projectx_contract_id".to_string(),
        Value::String(contract.id.to_string()),
    );
    info.insert(
        "projectx_symbol_id".to_string(),
        Value::String(contract.symbol_id.to_string()),
    );
    info.insert(
        "projectx_name".to_string(),
        Value::String(contract.name.clone()),
    );
    info.insert(
        "description".to_string(),
        Value::String(contract.description.clone()),
    );
    info.insert(
        "active_contract".to_string(),
        Value::Bool(contract.active_contract),
    );
    info.insert(
        "activation_source".to_string(),
        Value::String("unavailable_from_projectx".to_string()),
    );
    info.insert(
        "expiration_source".to_string(),
        Value::String("unavailable_from_projectx".to_string()),
    );
    info.insert(
        "tick_size".to_string(),
        Value::String(contract.tick_size.to_string()),
    );
    info.insert(
        "tick_value".to_string(),
        Value::String(contract.tick_value.to_string()),
    );

    let instrument = FuturesContract::new_checked(
        instrument_id,
        raw_symbol,
        infer_asset_class(&root, &contract.description),
        None,
        root.as_str().into(),
        activation_ns,
        expiration_ns,
        Currency::USD(),
        price_precision,
        price_increment,
        multiplier,
        lot_size,
        None,
        Some(lot_size),
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        Some(info),
        ts_init,
        ts_init,
    )
    .context("failed to construct ProjectX futures contract")?;

    Ok(InstrumentAny::FuturesContract(instrument))
}

fn infer_asset_class(root: &str, description: &str) -> AssetClass {
    let root_upper = root.to_ascii_uppercase();
    let description_upper = description.to_ascii_uppercase();

    if matches!(
        root_upper.as_str(),
        "ES" | "MES" | "NQ" | "MNQ" | "RTY" | "M2K" | "YM" | "MYM" | "NKD"
    ) || description_upper.contains("S&P")
        || description_upper.contains("NASDAQ")
        || description_upper.contains("RUSSELL")
        || description_upper.contains("DOW")
        || description_upper.contains("NIKKEI")
    {
        return AssetClass::Index;
    }

    if description_upper.contains("DOLLAR")
        || description_upper.contains("EURO")
        || description_upper.contains("POUND")
        || description_upper.contains("FRANC")
        || description_upper.contains("YEN")
        || description_upper.contains("PESO")
        || description_upper.contains("FX")
        || description_upper.contains("AUD/USD")
        || description_upper.contains("GBP/USD")
        || description_upper.contains("EUR/USD")
    {
        return AssetClass::FX;
    }

    if description_upper.contains("TREASURY")
        || description_upper.contains("T-NOTE")
        || description_upper.contains("T-BOND")
        || description_upper.contains("BOND")
        || description_upper.contains("NOTE")
    {
        return AssetClass::Debt;
    }

    if description_upper.contains("BITCOIN") || description_upper.contains("ETHER") {
        return AssetClass::Cryptocurrency;
    }

    AssetClass::Commodity
}

fn month_code_index(month: char) -> Option<u8> {
    "FGHJKMNQUVXZ"
        .chars()
        .position(|code| code == month)
        .map(|idx| idx as u8)
}

fn projectx_contract_root_and_sort_key(contract: &Contract) -> Option<(String, u16, u8)> {
    let parse_candidate = |candidate: &str| -> Option<(String, u16, u8)> {
        let (root, month, year_two) = parse_databento_symbol(candidate).ok()?;
        let month_index = month_code_index(month)?;
        Some((root, year_two, month_index))
    };

    parse_candidate(&canonical_projectx_public_symbol(contract))
        .or_else(|| parse_candidate(&contract.name))
        .or_else(|| parse_candidate(contract.symbol_id.as_ref()))
        .or_else(|| {
            let translated = projectx_to_databento_symbol(contract.id.as_ref()).ok()?;
            parse_candidate(&translated)
        })
}

impl ClientConfig for ProjectXDataClientConfig {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl ClientConfig for ProjectXExecClientConfig {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Factory for creating ProjectX data clients.
#[derive(Debug, Clone)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.adapters.projectx", from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.projectx")
)]
pub struct ProjectXDataClientFactory;

impl ProjectXDataClientFactory {
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
}

impl Default for ProjectXDataClientFactory {
    fn default() -> Self {
        Self::new()
    }
}

impl DataClientFactory for ProjectXDataClientFactory {
    fn create(
        &self,
        name: &str,
        config: &dyn ClientConfig,
        _cache: CacheView,
        _clock: Rc<RefCell<dyn Clock>>,
    ) -> anyhow::Result<Box<dyn DataClient>> {
        let projectx_config = config
            .as_any()
            .downcast_ref::<ProjectXDataClientConfig>()
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "Invalid config type for ProjectXDataClientFactory. Expected ProjectXDataClientConfig, was {config:?}",
                )
            })?
            .clone();

        let client_id = ClientId::from(name);
        let client = ProjectXDataClient::new(client_id, projectx_config)?;
        Ok(Box::new(client))
    }

    fn name(&self) -> &'static str {
        PROJECT_X
    }

    fn config_type(&self) -> &'static str {
        "ProjectXDataClientConfig"
    }
}

/// Factory for creating ProjectX execution clients.
#[derive(Debug, Clone)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.adapters.projectx", from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.projectx")
)]
pub struct ProjectXExecutionClientFactory;

impl ProjectXExecutionClientFactory {
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
}

impl Default for ProjectXExecutionClientFactory {
    fn default() -> Self {
        Self::new()
    }
}

impl ExecutionClientFactory for ProjectXExecutionClientFactory {
    fn create(
        &self,
        name: &str,
        config: &dyn ClientConfig,
        cache: CacheView,
    ) -> anyhow::Result<Box<dyn ExecutionClient>> {
        let projectx_config = config
            .as_any()
            .downcast_ref::<ProjectXExecClientConfig>()
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "Invalid config type for ProjectXExecutionClientFactory. Expected ProjectXExecClientConfig, was {config:?}",
                )
            })?
            .clone();

        if projectx_config.account_type != AccountType::Margin {
            anyhow::bail!(
                "ProjectX futures accounts require AccountType::Margin, received {:?}",
                projectx_config.account_type
            );
        }

        let core = ExecutionClientCore::new(
            projectx_config.trader_id,
            ClientId::from(name),
            *PROJECTX_VENUE,
            OmsType::Netting,
            projectx_config.account_id,
            projectx_config.account_type,
            None,
            cache,
        );

        let client = ProjectXExecutionClient::new(core, projectx_config)?;
        Ok(Box::new(client))
    }

    fn name(&self) -> &'static str {
        PROJECT_X
    }

    fn config_type(&self) -> &'static str {
        "ProjectXExecClientConfig"
    }
}

#[cfg(test)]
mod tests {
    use std::{cell::RefCell, rc::Rc};

    use super::{
        Contract, ProjectXDataClientFactory, ProjectXExecutionClientFactory,
        canonical_projectx_public_symbol, projectx_contract_to_instrument,
        projectx_select_front_month_contract,
    };
    use crate::{
        common::{consts::PROJECTX_VENUE, enums::ProjectXEnvironment},
        config::{ProjectXDataClientConfig, ProjectXExecClientConfig},
    };
    use nautilus_common::{
        cache::Cache,
        clock::TestClock,
        factories::{DataClientFactory, ExecutionClientFactory},
        live::runner::replace_data_event_sender,
    };
    use nautilus_core::{Params, UnixNanos, time::get_atomic_clock_realtime};
    use nautilus_live::ExecutionClientCore;
    use nautilus_model::{
        enums::{AccountType, OmsType},
        identifiers::{AccountId, ClientId, TraderId},
        instruments::{Instrument, InstrumentAny},
    };
    use rust_decimal::Decimal;

    fn base_contract(id: &str, name: &str, active_contract: bool) -> Contract {
        serde_json::from_value(serde_json::json!({
            "id": id,
            "name": name,
            "description": "Micro E-mini S&P 500",
            "tickSize": 0.25,
            "tickValue": 1.25,
            "activeContract": active_contract,
            "symbolId": name,
        }))
        .expect("contract")
    }

    #[rstest::rstest]
    fn shared_contract_parser_preserves_projectx_metadata() {
        let contract = base_contract("CON.F.US.MES.M26", "MESM26", true);
        let instrument = projectx_contract_to_instrument(&contract).expect("contract should parse");

        let InstrumentAny::FuturesContract(instrument) = instrument else {
            panic!("expected futures contract");
        };

        let info = instrument
            .info
            .as_ref()
            .cloned()
            .unwrap_or_else(Params::new);
        assert_eq!(instrument.id.to_string(), "MESM26.PROJECTX");
        assert_eq!(instrument.raw_symbol().to_string(), "MESM26");
        assert_eq!(canonical_projectx_public_symbol(&contract), "MESM26");
        assert_eq!(
            info.get_str("projectx_contract_id"),
            Some("CON.F.US.MES.M26")
        );
        assert_eq!(info.get_str("projectx_name"), Some("MESM26"));
        assert_eq!(info.get_bool("active_contract"), Some(true));
        assert_eq!(
            info.get_str("activation_source"),
            Some("unavailable_from_projectx")
        );
        assert_eq!(
            info.get_str("expiration_source"),
            Some("unavailable_from_projectx")
        );
        assert_eq!(instrument.activation_ns, UnixNanos::default());
        assert_eq!(instrument.expiration_ns, UnixNanos::from(u64::MAX));
        assert!(get_atomic_clock_realtime().get_time_ns() < instrument.expiration_ns);
    }

    #[rstest::rstest]
    fn shared_contract_parser_preserves_decimal_tick_arithmetic() {
        let contract: Contract = serde_json::from_value(serde_json::json!({
            "id": "CON.F.US.MES.M26",
            "name": "MESM26",
            "description": "Micro E-mini S&P 500",
            "tickSize": "0.00000001",
            "tickValue": "0.00000005",
            "activeContract": true,
            "symbolId": "MESM26",
        }))
        .expect("contract");

        let InstrumentAny::FuturesContract(instrument) =
            projectx_contract_to_instrument(&contract).expect("contract should parse")
        else {
            panic!("expected futures contract");
        };

        assert_eq!(instrument.price_increment.as_decimal(), contract.tick_size);
        assert_eq!(instrument.price_precision, 8);
        assert_eq!(instrument.multiplier.as_decimal(), Decimal::from(5));
    }

    #[rstest::rstest]
    fn shared_contract_parser_rejects_non_positive_tick_values() {
        let mut contract = base_contract("CON.F.US.MES.M26", "MESM26", true);
        contract.tick_size = Decimal::ZERO;

        let error = projectx_contract_to_instrument(&contract).expect_err("zero tick must fail");

        assert!(error.to_string().contains("tick size must be positive"));
    }

    #[rstest::rstest]
    fn canonical_public_symbol_uses_two_digit_year_when_contract_id_confirms_expiry() {
        let contract = base_contract("CON.F.US.MNQ.M26", "MNQM6", true);

        assert_eq!(canonical_projectx_public_symbol(&contract), "MNQM26");
    }

    #[rstest::rstest]
    fn shared_contract_parser_canonicalizes_raw_symbol_but_preserves_vendor_name_metadata() {
        let contract = base_contract("CON.F.US.MNQ.M26", "MNQM6", true);
        let instrument = projectx_contract_to_instrument(&contract).expect("contract should parse");

        let InstrumentAny::FuturesContract(instrument) = instrument else {
            panic!("expected futures contract");
        };

        let info = instrument
            .info
            .as_ref()
            .cloned()
            .unwrap_or_else(Params::new);
        assert_eq!(instrument.id.to_string(), "MNQM26.PROJECTX");
        assert_eq!(instrument.raw_symbol().to_string(), "MNQM26");
        assert_eq!(info.get_str("projectx_name"), Some("MNQM6"));
        assert_eq!(info.get_str("projectx_symbol_id"), Some("MNQM6"));
    }

    #[rstest::rstest]
    fn canonical_public_symbol_combines_vendor_root_with_authoritative_expiry() {
        let contract = base_contract("CON.F.US.EP.U25", "ESU5", true);

        assert_eq!(canonical_projectx_public_symbol(&contract), "ESU25");
    }

    #[rstest::rstest]
    fn front_month_selection_prefers_active_contracts_then_nearest_expiry() {
        let contracts = vec![
            base_contract("CON.F.US.MES.U26", "MESU26", false),
            base_contract("CON.F.US.MES.M26", "MESM26", true),
            base_contract("CON.F.US.MES.Z26", "MESZ26", true),
        ];

        let selected =
            projectx_select_front_month_contract(&contracts, "MES").expect("front month");

        assert_eq!(selected.id.to_string(), "CON.F.US.MES.M26");
    }

    #[rstest::rstest]
    fn front_month_selection_uses_canonical_alias_root_across_decade_boundary() {
        let contracts = vec![
            base_contract("CON.F.US.EP.H30", "ESH0", true),
            base_contract("CON.F.US.EP.Z29", "ESZ9", true),
        ];

        let selected = projectx_select_front_month_contract(&contracts, "ES").expect("front month");

        assert_eq!(selected.id.to_string(), "CON.F.US.EP.Z29");
        assert_eq!(canonical_projectx_public_symbol(&selected), "ESZ29");
    }

    #[rstest::rstest]
    fn projectx_data_client_factory_creates_rust_client() {
        let factory = ProjectXDataClientFactory::new();
        let config =
            ProjectXDataClientConfig::new(ProjectXEnvironment::TopstepX, "test-user", "test-key");
        let cache = Rc::new(RefCell::new(Cache::default()));
        let clock = Rc::new(RefCell::new(TestClock::new()));
        let (sender, _receiver) = tokio::sync::mpsc::unbounded_channel();
        replace_data_event_sender(sender);

        let client = factory
            .create("PROJECTX-TEST", &config, cache.into(), clock)
            .expect("data client factory should create a Rust client");

        assert_eq!(factory.name(), "PROJECTX");
        assert_eq!(factory.config_type(), "ProjectXDataClientConfig");
        assert_eq!(client.client_id(), ClientId::from("PROJECTX-TEST"));
        assert_eq!(client.venue(), Some(*PROJECTX_VENUE));
        assert!(!client.is_connected());
        assert!(client.is_disconnected());
    }

    #[rstest::rstest]
    fn projectx_execution_client_factory_creates_rust_client() {
        let factory = ProjectXExecutionClientFactory::new();
        let config = ProjectXExecClientConfig::new(
            TraderId::from("TRADER-001"),
            AccountId::from("PRAC-V2-64413-98419885"),
            ProjectXEnvironment::TopstepX,
            "test-user",
            "test-key",
        )
        .expect("valid ProjectX execution config");
        let cache = Rc::new(RefCell::new(Cache::default()));

        let client = factory
            .create("PROJECTX-TEST", &config, cache.into())
            .expect("execution client factory should create a Rust client");

        assert_eq!(factory.name(), "PROJECTX");
        assert_eq!(factory.config_type(), "ProjectXExecClientConfig");
        assert_eq!(client.client_id(), ClientId::from("PROJECTX-TEST"));
        assert_eq!(
            client.account_id(),
            AccountId::from("PROJECTX-PRAC-V2-64413-98419885"),
        );
        assert_eq!(client.venue(), *PROJECTX_VENUE);
        assert_eq!(client.oms_type(), OmsType::Netting);
        assert_eq!(client.get_account(), None);
        assert!(!client.is_connected());
    }

    #[rstest::rstest]
    fn execution_client_core_defaults_align_with_projectx_exec_config() {
        let cache = Rc::new(RefCell::new(Cache::default()));
        let config = ProjectXExecClientConfig::new(
            TraderId::from("TRADER-001"),
            AccountId::from("PRAC-V2-64413-98419885"),
            ProjectXEnvironment::TopstepX,
            "test-user",
            "test-key",
        )
        .expect("valid ProjectX execution config");
        let core = ExecutionClientCore::new(
            config.trader_id,
            ClientId::from("PROJECTX"),
            *PROJECTX_VENUE,
            OmsType::Netting,
            config.account_id,
            config.account_type,
            None,
            cache,
        );

        assert_eq!(core.oms_type, OmsType::Netting);
        assert_eq!(core.account_type, AccountType::Margin);
        assert_eq!(
            core.account_id,
            AccountId::from("PROJECTX-PRAC-V2-64413-98419885"),
        );
    }

    #[rstest::rstest]
    fn execution_client_factory_rejects_non_margin_account_type() {
        let factory = ProjectXExecutionClientFactory::new();
        let mut config = ProjectXExecClientConfig::new(
            TraderId::from("TRADER-001"),
            AccountId::from("PRAC-V2-64413-98419885"),
            ProjectXEnvironment::TopstepX,
            "test-user",
            "test-key",
        )
        .expect("valid ProjectX execution config");
        config.account_type = AccountType::Cash;
        let cache = Rc::new(RefCell::new(Cache::default()));

        let result = factory.create("PROJECTX-TEST", &config, cache.into());

        assert!(result.is_err());
        assert!(
            result
                .err()
                .expect("cash account type should be rejected")
                .to_string()
                .contains("require AccountType::Margin")
        );
    }
}
