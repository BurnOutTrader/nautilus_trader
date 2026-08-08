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

#![allow(dead_code)]

use std::{env, path::PathBuf};

#[path = "input.rs"]
mod input;

use chrono::{DateTime, Utc};
use nautilus_core::UnixNanos;
use nautilus_model::{
    data::BarType,
    identifiers::{ClientId, InstrumentId, TraderId},
    instruments::Instrument,
    types::Quantity,
};
use nautilus_persistence::backend::catalog::ParquetDataCatalog;
use projectx_client::Contract;
use projectx_nt::{
    ProjectXConfig, ProjectXDataClientConfig, ProjectXEnvironment, ProjectXExecClientConfig,
    ProjectXHttpClient,
    config::projectx_account_id_from_raw,
    factories::{
        canonical_projectx_public_symbol, projectx_contract_matches_product_root,
        projectx_contract_to_instrument, projectx_select_front_month_contract,
    },
};

pub(crate) const DEFAULT_PRODUCT_ROOT: &str = "MES";
pub(crate) const DEFAULT_ACCOUNT_ID: &str = "PRAC-V2-64413-98419885";
pub(crate) const DEFAULT_CAPTURE_SECONDS: u64 = 10;

pub(crate) fn load_env() {
    dotenvy::dotenv().ok();
}

pub(crate) fn env_bool(key: &str, default: bool) -> bool {
    match env::var(key) {
        Ok(value) => match value.trim().to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "y" | "on" => true,
            "0" | "false" | "no" | "n" | "off" => false,
            _ => default,
        },
        Err(_) => default,
    }
}

pub(crate) fn env_u64(key: &str, default: u64) -> u64 {
    env::var(key)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(default)
}

pub(crate) fn projectx_client_id() -> ClientId {
    ClientId::from("PROJECTX")
}

pub(crate) fn trader_id_from_env(key: &str, default: &str) -> anyhow::Result<TraderId> {
    let value = env::var(key).unwrap_or_else(|_| default.to_string());
    input::parse_trader_id(key, &value)
}

fn required_env(key: &str) -> anyhow::Result<String> {
    env::var(key).map_err(|_| anyhow::anyhow!("Missing required environment variable: {key}"))
}

pub(crate) fn env_string(key: &str, default: &str) -> String {
    env::var(key).unwrap_or_else(|_| default.to_string())
}

pub(crate) fn positive_quantity_from_env(key: &str, default: &str) -> anyhow::Result<Quantity> {
    let value = env_string(key, default);
    input::parse_positive_quantity(key, &value)
}

pub(crate) fn projectx_transport_config_from_env() -> anyhow::Result<ProjectXConfig> {
    Ok(ProjectXConfig::new(
        ProjectXEnvironment::TopstepX,
        required_env("PROJECTX_USERNAME")?,
        required_env("PROJECTX_API_KEY")?,
    ))
}

pub(crate) fn data_config_from_env() -> anyhow::Result<ProjectXDataClientConfig> {
    let mut config = ProjectXDataClientConfig::new(
        ProjectXEnvironment::TopstepX,
        required_env("PROJECTX_USERNAME")?,
        required_env("PROJECTX_API_KEY")?,
    );
    config.market_data_live = env_bool("PROJECTX_MARKET_DATA_LIVE", false);
    Ok(config)
}

pub(crate) fn exec_config_from_env(
    trader_id: TraderId,
) -> anyhow::Result<ProjectXExecClientConfig> {
    let account_id = env::var("PROJECTX_EXEC_ACCOUNT_ID")
        .or_else(|_| env::var("PROJECTX_ACCOUNT_ID"))
        .unwrap_or_else(|_| DEFAULT_ACCOUNT_ID.to_string());

    Ok(ProjectXExecClientConfig::new(
        trader_id,
        projectx_account_id_from_raw(&account_id)?,
        ProjectXEnvironment::TopstepX,
        required_env("PROJECTX_USERNAME")?,
        required_env("PROJECTX_API_KEY")?,
    )?)
}

pub(crate) async fn resolve_instrument_id_from_env(live: bool) -> anyhow::Result<InstrumentId> {
    let contract = resolve_contract_from_env(live).await?;
    let instrument = projectx_contract_to_instrument(&contract)?;

    Ok(instrument.id())
}

pub(crate) fn example_root_from_env(adapter_name: &str) -> anyhow::Result<PathBuf> {
    let path = env::var("NAUTILUS_PATH").map_err(|_| {
        anyhow::anyhow!(
            "Set NAUTILUS_PATH to the parent directory for the {adapter_name} example catalog, for example /tmp/nautilus-data/examples/{adapter_name}.",
        )
    })?;
    Ok(PathBuf::from(path))
}

fn candidate_contract_live_values(live: bool) -> [bool; 2] {
    [live, !live]
}

fn normalize_public_symbol(value: &str) -> String {
    value
        .split('.')
        .next()
        .unwrap_or(value)
        .trim()
        .to_ascii_uppercase()
}

fn match_contract_by_symbol(contracts: &[Contract], symbol: &str) -> Option<Contract> {
    contracts.iter().find_map(|contract| {
        let public_symbol = canonical_projectx_public_symbol(contract);
        let symbol_id = contract.symbol_id.to_string().to_ascii_uppercase();
        let contract_name = contract.name.trim().to_ascii_uppercase();

        (public_symbol == symbol || symbol_id == symbol || contract_name == symbol)
            .then_some(contract.clone())
    })
}

pub(crate) async fn resolve_contract_from_env(live: bool) -> anyhow::Result<Contract> {
    let http_client = ProjectXHttpClient::from_config(projectx_transport_config_from_env()?)?;
    http_client.start().await?;

    if let Ok(raw_instrument_id) = env::var("PROJECTX_INSTRUMENT_ID") {
        let target_symbol = normalize_public_symbol(&raw_instrument_id);

        for contract_live in candidate_contract_live_values(live) {
            let contracts = http_client.available_contracts(contract_live).await?;
            if let Some(contract) = match_contract_by_symbol(&contracts, &target_symbol) {
                return Ok(contract);
            }
        }

        anyhow::bail!(
            "No ProjectX contract found for instrument symbol {target_symbol}. Set PROJECTX_INSTRUMENT_ID to a valid public symbol such as MNQM26.PROJECTX."
        );
    }

    let product_root = env_string("PROJECTX_PRODUCT_ROOT", DEFAULT_PRODUCT_ROOT);

    for contract_live in candidate_contract_live_values(live) {
        let contracts = http_client.available_contracts(contract_live).await?;
        let contracts: Vec<_> = contracts
            .into_iter()
            .filter(|contract| {
                projectx_contract_matches_product_root(contract, Some(&product_root))
            })
            .collect();

        if let Some(contract) = projectx_select_front_month_contract(&contracts, &product_root) {
            return Ok(contract);
        }
    }

    anyhow::bail!("No ProjectX contract found for product root {product_root}")
}

pub(crate) fn resolve_catalog_instrument_id(
    catalog: &ParquetDataCatalog,
) -> anyhow::Result<InstrumentId> {
    if let Ok(instrument_id) = env::var("PROJECTX_INSTRUMENT_ID") {
        return input::parse_instrument_id("PROJECTX_INSTRUMENT_ID", &instrument_id);
    }

    let instruments = catalog.instruments(None, None, None)?;
    match instruments.as_slice() {
        [] => anyhow::bail!("The ProjectX catalog does not contain any instruments"),
        [instrument] => Ok(instrument.id()),
        _ => anyhow::bail!(
            "The ProjectX catalog contains multiple instruments. Set PROJECTX_INSTRUMENT_ID explicitly.",
        ),
    }
}

pub(crate) fn resolve_catalog_backtest_window(
    catalog: &mut ParquetDataCatalog,
    instrument_id: InstrumentId,
    bar_type: BarType,
) -> anyhow::Result<(UnixNanos, UnixNanos)> {
    let bars = catalog.bars(Some(vec![instrument_id.to_string()]), None, None)?;
    let mut matching = bars
        .into_iter()
        .filter(|bar| bar.bar_type == bar_type)
        .collect::<Vec<_>>();
    matching.sort_by_key(|bar| bar.ts_event);

    let first = matching
        .first()
        .ok_or_else(|| anyhow::anyhow!("No bars found in the catalog for {bar_type}"))?;
    let last = matching
        .last()
        .ok_or_else(|| anyhow::anyhow!("No bars found in the catalog for {bar_type}"))?;

    Ok((first.ts_event, last.ts_event))
}

pub(crate) fn format_utc_nanos(value: UnixNanos) -> anyhow::Result<String> {
    let nanos = i64::try_from(value.as_u64())?;
    let timestamp = DateTime::<Utc>::from_timestamp_nanos(nanos);
    Ok(timestamp.to_rfc3339())
}
