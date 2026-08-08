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
        symbols::{parse_databento_symbol, projectx_to_databento_symbol},
    },
    config::{ProjectXDataClientConfig, ProjectXExecClientConfig},
    data::ProjectXDataClient,
    execution::ProjectXExecutionClient,
};
use chrono::{NaiveDate, TimeZone, Utc};
use nautilus_common::{
    clients::{DataClient, ExecutionClient},
    clock::Clock,
    factories::{ClientConfig, DataClientFactory, ExecutionClientFactory},
};
use nautilus_core::{Params, UnixNanos, time::get_atomic_clock_realtime};
use nautilus_live::ExecutionClientCore;
use nautilus_model::{
    enums::{AssetClass, OmsType},
    identifiers::{ClientId, InstrumentId, Symbol},
    instruments::{FuturesContract, InstrumentAny},
    types::{Currency, Price, Quantity},
};
use projectx_client::Contract;
use rust_decimal::prelude::ToPrimitive;
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
    let name = normalize_projectx_symbol_key(&contract.name);

    if let Some(name_parts) = parsed_symbol(&name) {
        if let Some(translated_symbol) = translated.as_deref()
            && parsed_symbol(translated_symbol).is_some_and(|translated_parts| {
                translated_parts.0 == name_parts.0 && translated_parts.1 == name_parts.1
            })
        {
            return translated_symbol.to_string();
        }

        return name;
    }

    let symbol_id = normalize_projectx_symbol_key(contract.symbol_id.as_ref());

    if let Some(symbol_parts) = parsed_symbol(&symbol_id) {
        if let Some(translated_symbol) = translated.as_deref()
            && parsed_symbol(translated_symbol).is_some_and(|translated_parts| {
                translated_parts.0 == symbol_parts.0 && translated_parts.1 == symbol_parts.1
            })
        {
            return translated_symbol.to_string();
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
    let instrument_id = InstrumentId::new(Symbol::new(public_symbol.as_str()), *PROJECTX_VENUE);
    let (activation_ns, expiration_ns) =
        projectx_contract_expiry_bounds(contract).unwrap_or_else(|| {
            let now = get_atomic_clock_realtime().get_time_ns();
            (now, now)
        });
    let ts_init = get_atomic_clock_realtime().get_time_ns();
    let tick_size = contract.tick_size.to_f64().unwrap_or_default();
    let tick_value = contract.tick_value.to_f64().unwrap_or_default();
    let price_precision = infer_price_precision(tick_size);
    let multiplier = if tick_size > 0.0 {
        tick_value / tick_size
    } else {
        1.0
    };

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
        "tick_size".to_string(),
        serde_json::Number::from_f64(contract.tick_size.to_f64().unwrap_or_default())
            .map_or(Value::Null, Value::Number),
    );
    info.insert(
        "tick_value".to_string(),
        serde_json::Number::from_f64(contract.tick_value.to_f64().unwrap_or_default())
            .map_or(Value::Null, Value::Number),
    );

    Ok(InstrumentAny::FuturesContract(FuturesContract::new(
        instrument_id,
        Symbol::new(public_symbol.as_str()),
        infer_asset_class(&root, &contract.description),
        None,
        root.as_str().into(),
        activation_ns,
        expiration_ns,
        Currency::USD(),
        price_precision,
        Price::new(
            contract.tick_size.to_f64().unwrap_or_default(),
            price_precision,
        ),
        Quantity::new(multiplier, infer_quantity_precision(multiplier)),
        Quantity::new(1.0, 0),
        None,
        Some(Quantity::new(1.0, 0)),
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
    )))
}

fn infer_price_precision(value: f64) -> u8 {
    let s = format!("{value:.12}");
    let trimmed = s.trim_end_matches('0').trim_end_matches('.');
    trimmed
        .split('.')
        .nth(1)
        .map_or(0, |fraction| fraction.len() as u8)
}

fn infer_quantity_precision(value: f64) -> u8 {
    infer_price_precision(value)
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

fn projectx_contract_expiry_bounds(contract: &Contract) -> Option<(UnixNanos, UnixNanos)> {
    let (_, month_code, year_two) =
        parse_databento_symbol(&canonical_projectx_public_symbol(contract)).ok()?;
    let month = match month_code {
        'F' => 1,
        'G' => 2,
        'H' => 3,
        'J' => 4,
        'K' => 5,
        'M' => 6,
        'N' => 7,
        'Q' => 8,
        'U' => 9,
        'V' => 10,
        'X' => 11,
        'Z' => 12,
        _ => return None,
    };
    let year = 2000 + i32::from(year_two);

    let activation = NaiveDate::from_ymd_opt(year, month, 1)?.and_hms_nano_opt(0, 0, 0, 0)?;
    let (next_year, next_month) = if month == 12 {
        (year + 1, 1)
    } else {
        (year, month + 1)
    };
    let expiration =
        NaiveDate::from_ymd_opt(next_year, next_month, 1)?.and_hms_nano_opt(0, 0, 0, 0)?;

    Some((
        UnixNanos::from(Utc.from_utc_datetime(&activation).timestamp_nanos_opt()? as u64),
        UnixNanos::from(Utc.from_utc_datetime(&expiration).timestamp_nanos_opt()? as u64),
    ))
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

    parse_candidate(&contract.name)
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
        _cache: nautilus_common::cache::CacheView,
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
        cache: nautilus_common::cache::CacheView,
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
    use nautilus_core::Params;
    use nautilus_live::ExecutionClientCore;
    use nautilus_model::{
        enums::{AccountType, OmsType},
        identifiers::{AccountId, ClientId, TraderId},
        instruments::{Instrument, InstrumentAny},
    };

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
    fn canonical_public_symbol_preserves_vendor_root_when_id_translation_differs() {
        let contract = base_contract("CON.F.US.EP.U25", "ESU5", true);

        assert_eq!(canonical_projectx_public_symbol(&contract), "ESU5");
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
        );
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
        );
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
}
