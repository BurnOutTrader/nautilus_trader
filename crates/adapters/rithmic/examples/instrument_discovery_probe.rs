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

//! Probe the hard-coded Rithmic futures discovery surfaces.
//!
//! This example validates the allowlisted catalog and live discovery flow only.
//! Unsupported exchanges and product roots are intentionally excluded.
//!
//! Run with:
//! `cargo run --example rithmic-instrument-discovery-probe --package rithmic-nt`

mod support;

use std::{collections::BTreeSet, sync::Arc};

use futures_util::stream::{self, StreamExt};
use nautilus_model::instruments::{Instrument, InstrumentAny};
use rithmic_nt::{
    GatewayConfig, RithmicGateway, RithmicInstrumentProvider,
    common::consts::{exchanges, products},
};
use rithmic_rs::rti::{
    messages::RithmicMessage,
    request_search_symbols::{InstrumentType, Pattern},
    response_list_exchange_permissions::EntitlementFlag,
};

use self::support::common::{env_string, env_u64, load_env, profile_from_env};

const DEFAULT_EXCHANGE: &str = "CME";
const DEFAULT_CONCURRENCY: usize = 32;
const DEFAULT_PRODUCT_LIMIT: usize = 50;
const DEFAULT_SAMPLE_LIMIT: usize = 10;

fn env_usize(key: &str, default: usize) -> usize {
    usize::try_from(env_u64(key, default as u64)).unwrap_or(default)
}

fn normalize_symbol(raw_symbol: &str) -> String {
    raw_symbol
        .split('.')
        .next()
        .unwrap_or(raw_symbol)
        .trim()
        .to_string()
}

fn log_section(title: &str) {
    println!();
    println!("=== {title} ===");
}

fn instrument_is_tradeable(instrument: &InstrumentAny) -> bool {
    match instrument {
        InstrumentAny::FuturesContract(contract) => contract
            .info
            .as_ref()
            .and_then(|info| info.get_bool("is_tradeable"))
            .unwrap_or(true),
        _ => true,
    }
}

async fn enabled_exchanges(
    gateway: &RithmicGateway,
    username: &str,
) -> anyhow::Result<Vec<String>> {
    let ticker = gateway
        .ticker_handle()
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("Ticker plant not connected"))?;

    let responses = ticker.list_exchanges(username).await?;
    let mut exchanges = BTreeSet::new();

    for response in responses {
        if let Some(error) = response.error {
            println!("list_exchanges.error={error}");
            continue;
        }

        let RithmicMessage::ResponseListExchangePermissions(permission) = response.message else {
            continue;
        };

        let exchange = match permission.exchange {
            Some(exchange) if !exchange.is_empty() => exchange,
            _ => continue,
        };

        let enabled = permission
            .entitlement_flag
            .and_then(|flag| EntitlementFlag::try_from(flag).ok())
            .is_none_or(|flag| flag == EntitlementFlag::Enabled);

        println!(
            "exchange_permission exchange={exchange} enabled={enabled} raw_flag={:?}",
            permission.entitlement_flag,
        );

        if enabled {
            exchanges.insert(exchange);
        }
    }

    Ok(exchanges.into_iter().collect())
}

fn supported_products_for_exchange(exchange: &str) -> Vec<String> {
    let products = match exchange.to_ascii_uppercase().as_str() {
        exchanges::CME => products::SUPPORTED_CME_ROOTS,
        exchanges::CBOT => products::SUPPORTED_CBOT_ROOTS,
        exchanges::NYMEX => products::SUPPORTED_NYMEX_ROOTS,
        exchanges::COMEX => products::SUPPORTED_COMEX_ROOTS,
        _ => &[],
    };

    products
        .iter()
        .map(|product| (*product).to_string())
        .collect()
}

fn search_exchange_candidates(product_code: &str, preferred_exchange: &str) -> Vec<String> {
    if product_code.eq_ignore_ascii_case(products::MYM) {
        if preferred_exchange.eq_ignore_ascii_case(exchanges::CME) {
            return vec![exchanges::CME.to_string(), exchanges::CBOT.to_string()];
        }

        return vec![exchanges::CBOT.to_string(), exchanges::CME.to_string()];
    }

    vec![preferred_exchange.to_ascii_uppercase()]
}

async fn search_supported_product(
    gateway: &RithmicGateway,
    preferred_exchange: &str,
    product_code: &str,
) -> anyhow::Result<Vec<(String, String)>> {
    let mut last_error = None;

    for exchange in search_exchange_candidates(product_code, preferred_exchange) {
        match search_symbols_with_filter(
            gateway,
            &exchange,
            product_code,
            Some(product_code),
            Pattern::Equals,
        )
        .await
        {
            Ok(symbols) => {
                if !symbols.is_empty() {
                    return Ok(symbols
                        .into_iter()
                        .map(|symbol| (symbol, exchange.clone()))
                        .collect());
                }
            }
            Err(e) => {
                last_error = Some(e);
            }
        }
    }

    if let Some(error) = last_error {
        return Err(error);
    }

    Ok(Vec::new())
}

async fn search_symbols_probe(
    gateway: &RithmicGateway,
    exchange: &str,
    search_text: &str,
) -> anyhow::Result<usize> {
    Ok(
        search_symbols_with_filter(gateway, exchange, search_text, None, Pattern::Contains)
            .await?
            .len(),
    )
}

async fn search_symbols_with_filter(
    gateway: &RithmicGateway,
    exchange: &str,
    search_text: &str,
    product_code: Option<&str>,
    pattern: Pattern,
) -> anyhow::Result<Vec<String>> {
    let ticker = gateway
        .ticker_handle()
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("Ticker plant not connected"))?;

    let responses = ticker
        .search_symbols(
            search_text,
            Some(exchange),
            product_code,
            Some(InstrumentType::Future),
            Some(pattern),
        )
        .await?;

    let mut symbols = BTreeSet::new();

    for response in responses {
        if response.error.is_some() {
            continue;
        }

        let RithmicMessage::ResponseSearchSymbols(search) = response.message else {
            continue;
        };

        if let Some(symbol) = search.symbol {
            symbols.insert(normalize_symbol(&symbol));
        }
    }

    Ok(symbols.into_iter().collect())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    load_env();

    let profile = profile_from_env();
    let requested_exchange = env_string("RITHMIC_DISCOVERY_EXCHANGE", DEFAULT_EXCHANGE);
    let product_limit = env_usize("RITHMIC_DISCOVERY_LIMIT_PRODUCTS", DEFAULT_PRODUCT_LIMIT);
    let sample_limit = env_usize("RITHMIC_DISCOVERY_SAMPLE_LIMIT", DEFAULT_SAMPLE_LIMIT);
    let concurrency = env_usize("RITHMIC_DISCOVERY_CONCURRENCY", DEFAULT_CONCURRENCY);
    let optional_product = std::env::var("RITHMIC_DISCOVERY_PRODUCT")
        .ok()
        .filter(|value| !value.trim().is_empty());

    let config = GatewayConfig::from_env_with_profile(profile.as_deref())?
        .with_order(false)
        .with_pnl(false)
        .with_history(false);

    println!(
        "profile={profile:?} environment={:?} system={} server={:?} alt_server={:?}",
        config.environment, config.system_name, config.server, config.alt_server
    );

    let username = config.username.clone();
    let mut gateway = RithmicGateway::new(config);
    gateway.connect().await?;
    let gateway = Arc::new(gateway);
    let provider = RithmicInstrumentProvider::new(Arc::clone(&gateway));

    let result = async {
        log_section("Exchange Permissions");
        let exchanges = enabled_exchanges(&gateway, &username).await?;
        println!("enabled_exchange_count={}", exchanges.len());
        for exchange in &exchanges {
            println!("enabled_exchange={exchange}");
        }

        let exchange = if exchanges.iter().any(|value| value == &requested_exchange) {
            requested_exchange.clone()
        } else if let Some(first) = exchanges.first() {
            println!(
                "requested_exchange={requested_exchange} not enabled, falling back to {first}"
            );
            first.clone()
        } else {
            requested_exchange.clone()
        };

        log_section("Search Symbols");
        for search_text in ["", "ES", "MNQ"] {
            let count = search_symbols_probe(&gateway, &exchange, search_text).await?;
            println!("search_symbols exchange={exchange} text={search_text:?} count={count}");
        }

        let mut search_discovered_symbols = BTreeSet::new();
        if let Some(product_code) = optional_product.as_deref() {
            log_section("Product Search Variants");
            for (label, search_text, pattern) in [
                ("empty+product-filter", "", Pattern::Contains),
                ("text+product-filter-contains", product_code, Pattern::Contains),
                ("text+product-filter-equals", product_code, Pattern::Equals),
            ] {
                let symbols = search_symbols_with_filter(
                    &gateway,
                    &exchange,
                    search_text,
                    Some(product_code),
                    pattern,
                )
                .await?;
                println!(
                    "product_search label={label} product_code={product_code} count={}",
                    symbols.len()
                );
                for symbol in symbols.iter().take(sample_limit) {
                    println!("product_search_sample label={label} symbol={symbol}");
                }
                if symbols.len() > search_discovered_symbols.len() {
                    search_discovered_symbols = symbols.into_iter().collect();
                }
            }
        }

        log_section("Supported Roots");
        let mut product_codes = supported_products_for_exchange(&exchange);
        println!(
            "supported_root_count exchange={exchange} count={}",
            product_codes.len()
        );
        if let Some(product_code) = optional_product.clone() {
            product_codes.retain(|code| code == &product_code);
        }
        if product_codes.len() > product_limit {
            product_codes.truncate(product_limit);
        }
        for product_code in product_codes.iter().take(sample_limit) {
            println!("supported_root_sample exchange={exchange} product_code={product_code}");
        }

        log_section("Supported Root Search");
        let discoveries = stream::iter(product_codes.iter().cloned())
            .map(|product_code| {
                let gateway = Arc::clone(&gateway);
                let exchange = exchange.clone();
                async move {
                    let result = search_supported_product(&gateway, &exchange, &product_code).await;
                    (product_code, result)
                }
            })
            .buffer_unordered(concurrency)
            .collect::<Vec<_>>()
            .await;

        let mut discovered_contracts = BTreeSet::new();
        let mut product_count_with_symbols = 0usize;

        for (product_code, result) in discoveries {
            match result {
                Ok(discovery) => {
                    if !discovery.is_empty() {
                        product_count_with_symbols += 1;
                        let routed_exchanges = discovery
                            .iter()
                            .map(|(_, resolved_exchange)| resolved_exchange.as_str())
                            .collect::<BTreeSet<_>>();
                        println!(
                            "root_search product_code={product_code} symbols={} resolved_exchanges={:?}",
                            discovery.len(),
                            routed_exchanges,
                        );
                        discovered_contracts.extend(discovery);
                    }
                }
                Err(e) => {
                    println!("root_search.error product_code={product_code} error={e}");
                }
            }
        }

        println!(
            "supported_products_with_symbols exchange={exchange} count={product_count_with_symbols}"
        );
        println!(
            "supported_unique_future_symbols exchange={exchange} count={}",
            discovered_contracts.len()
        );
        for (symbol, resolved_exchange) in discovered_contracts.iter().take(sample_limit) {
            println!(
                "supported_symbol_sample requested_exchange={exchange} resolved_exchange={resolved_exchange} symbol={symbol}"
            );
        }

        log_section("Provider Raw Discovery");
        let provider_discovered = if let Some(product_code) = optional_product.as_deref() {
            provider
                .discover_product_symbols_async(product_code, &exchange)
                .await?
        } else {
            provider.discover_exchange_symbols_async(&exchange).await?
        };
        println!(
            "provider_raw_listing_count exchange={exchange} count={}",
            provider_discovered.len()
        );
        for listing in provider_discovered.iter().take(sample_limit) {
            println!(
                "provider_raw_listing symbol={} exchange={} product_code={} expiration_date={:?}",
                listing.symbol,
                listing.exchange,
                listing.product_code,
                listing.expiration_date
            );
        }

        if discovered_contracts.is_empty() && !search_discovered_symbols.is_empty() {
            println!(
                "discovery_fallback=search_symbols product_search_symbol_count={}",
                search_discovered_symbols.len()
            );
            discovered_contracts = search_discovered_symbols
                .into_iter()
                .map(|symbol| (symbol, exchange.clone()))
                .collect();
        }

        log_section("Reference Data Fan-Out");
        let loaded = stream::iter(discovered_contracts.into_iter())
            .map(|(symbol, resolved_exchange)| {
                let provider = &provider;
                async move {
                    let result = provider
                        .load_instrument_async(&symbol, &resolved_exchange)
                        .await;
                    (symbol, resolved_exchange, result)
                }
            })
            .buffer_unordered(concurrency)
            .collect::<Vec<_>>()
            .await;

        let mut loaded_count = 0usize;
        let mut tradeable_count = 0usize;

        for (symbol, resolved_exchange, result) in loaded {
            match result {
                Ok(instrument) => {
                    loaded_count += 1;
                    let is_tradeable = instrument_is_tradeable(&instrument);
                    if is_tradeable {
                        tradeable_count += 1;
                    }
                    if loaded_count <= sample_limit {
                        println!(
                            "reference_data symbol={symbol} resolved_exchange={resolved_exchange} instrument_id={} tradeable={} underlying={:?} raw_symbol={}",
                            instrument.id(),
                            is_tradeable,
                            instrument.underlying(),
                            instrument.raw_symbol(),
                        );
                    }
                }
                Err(e) => {
                    println!(
                        "reference_data.error symbol={symbol} resolved_exchange={resolved_exchange} error={e}"
                    );
                }
            }
        }

        println!("reference_data_loaded_count={loaded_count}");
        println!("reference_data_tradeable_count={tradeable_count}");

        anyhow::Ok(())
    }
    .await;

    drop(provider);
    let gateway = Arc::try_unwrap(gateway)
        .map_err(|_| anyhow::anyhow!("Rithmic gateway still has active references"))?;
    let mut gateway = gateway;
    gateway.disconnect().await?;

    result
}
