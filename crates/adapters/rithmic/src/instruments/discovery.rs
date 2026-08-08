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

//! Raw supported-root symbol discovery helpers.

use std::collections::{BTreeMap, BTreeSet};

use futures_util::stream::{self, StreamExt};
use rithmic_rs::{
    RithmicResponse,
    plants::ticker_plant::RithmicTickerPlantHandle,
    rti::{
        ResponseSearchSymbols,
        messages::RithmicMessage,
        request_search_symbols::{InstrumentType, Pattern},
        response_list_exchange_permissions::EntitlementFlag,
    },
};

use super::parse::{SupportedProduct, search_exchange_candidates, supported_products_for_exchange};
use crate::error::{Result, RithmicError};

const PRODUCT_SEARCH_CONCURRENCY: usize = 16;

/// A raw Rithmic contract listing discovered via `search_symbols(...)`.
///
/// This is intentionally lighter-weight than a fully parsed Nautilus
/// `FuturesContract`. It is meant for users who want to enumerate supported
/// concrete contracts first, then selectively resolve reference data only for
/// the contracts they actually intend to trade or backfill.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct RithmicInstrumentSymbol {
    pub symbol: String,
    pub exchange: String,
    pub product_code: String,
    pub description: Option<String>,
    pub instrument_type: Option<String>,
    pub expiration_date: Option<String>,
}

impl RithmicInstrumentSymbol {
    pub fn instrument_id(&self) -> Result<String> {
        crate::common::converters::rithmic_instrument_id(&self.symbol, &self.exchange)
            .map(|instrument_id| instrument_id.to_string())
    }
}

pub(crate) fn normalize_discovered_symbol(raw_symbol: &str) -> Option<String> {
    let symbol = raw_symbol.split('.').next().unwrap_or(raw_symbol).trim();

    if symbol.is_empty() {
        None
    } else {
        Some(symbol.to_string())
    }
}

fn instrument_symbol_from_search_result(
    search: &ResponseSearchSymbols,
    default_product_code: &str,
    default_exchange: &str,
) -> Option<RithmicInstrumentSymbol> {
    let symbol = normalize_discovered_symbol(search.symbol.as_deref()?)?;
    let exchange = search
        .exchange
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or(default_exchange)
        .to_string();

    let product_code = search
        .product_code
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or(default_product_code)
        .to_string();

    let instrument_type = search
        .instrument_type
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string);

    if let Some(instrument_type) = instrument_type.as_deref()
        && !instrument_type.eq_ignore_ascii_case("future")
    {
        return None;
    }

    Some(RithmicInstrumentSymbol {
        symbol,
        exchange,
        product_code,
        description: search
            .symbol_name
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToString::to_string),
        instrument_type,
        expiration_date: search
            .expiration_date
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToString::to_string),
    })
}

fn listings_from_search_responses(
    responses: &[RithmicResponse],
    default_product_code: &str,
    default_exchange: &str,
) -> BTreeMap<(String, String), RithmicInstrumentSymbol> {
    responses
        .iter()
        .filter(|response| response.error.is_none())
        .filter_map(|response| {
            let RithmicMessage::ResponseSearchSymbols(search) = &response.message else {
                return None;
            };

            instrument_symbol_from_search_result(search, default_product_code, default_exchange)
        })
        .map(|listing| ((listing.exchange.clone(), listing.symbol.clone()), listing))
        .collect()
}

pub(crate) fn enabled_exchange_names(responses: &[RithmicResponse]) -> BTreeSet<String> {
    responses
        .iter()
        .filter(|response| response.error.is_none())
        .filter_map(|response| {
            let RithmicMessage::ResponseListExchangePermissions(permission) = &response.message
            else {
                return None;
            };

            let exchange = permission.exchange.as_deref()?.trim();
            if exchange.is_empty() {
                return None;
            }

            let enabled = permission
                .entitlement_flag
                .and_then(|flag| EntitlementFlag::try_from(flag).ok())
                .is_none_or(|flag| flag == EntitlementFlag::Enabled);

            enabled.then(|| exchange.to_string())
        })
        .collect()
}

async fn search_symbols_for_supported_product_with_handle(
    ticker: &RithmicTickerPlantHandle,
    preferred_exchange: &str,
    product: SupportedProduct,
) -> Result<Vec<RithmicInstrumentSymbol>> {
    let mut last_error = None;

    for exchange in search_exchange_candidates(product, preferred_exchange) {
        match ticker
            .search_symbols(
                product.code,
                Some(exchange),
                Some(product.code),
                Some(InstrumentType::Future),
                Some(Pattern::Equals),
            )
            .await
        {
            Ok(responses) => {
                let listings = listings_from_search_responses(&responses, product.code, exchange)
                    .into_values()
                    .collect::<Vec<_>>();

                if !listings.is_empty() {
                    return Ok(listings);
                }
            }
            Err(e) => {
                last_error = Some(format!(
                    "Product symbol search failed for {} on {}: {e}",
                    product.code, exchange
                ));
            }
        }
    }

    if let Some(e) = last_error {
        return Err(RithmicError::Api(e));
    }

    Ok(Vec::new())
}

pub(crate) async fn discover_product_symbols_with_handle(
    ticker: &RithmicTickerPlantHandle,
    exchange: &str,
    product_code: &str,
) -> Result<Vec<RithmicInstrumentSymbol>> {
    let supported_product = supported_products_for_exchange(exchange)
        .into_iter()
        .find(|product| product.code.eq_ignore_ascii_case(product_code))
        .ok_or_else(|| {
            RithmicError::Instrument(format!(
                "Unsupported Rithmic product '{product_code}' for discovery on {exchange}"
            ))
        })?;

    search_symbols_for_supported_product_with_handle(ticker, exchange, supported_product).await
}

pub(crate) async fn discover_exchange_symbols_with_handle(
    ticker: &RithmicTickerPlantHandle,
    exchange: &str,
) -> Result<Vec<RithmicInstrumentSymbol>> {
    let supported_products = supported_products_for_exchange(exchange);
    if supported_products.is_empty() {
        return Ok(Vec::new());
    }

    let discovery_results = stream::iter(supported_products)
        .map(|product| async move {
            let result =
                search_symbols_for_supported_product_with_handle(ticker, exchange, product).await;
            (product.code, result)
        })
        .buffer_unordered(PRODUCT_SEARCH_CONCURRENCY)
        .collect::<Vec<_>>()
        .await;

    let mut listings = BTreeMap::new();
    for (product_code, result) in discovery_results {
        match result {
            Ok(found) => {
                listings.extend(
                    found.into_iter().map(|listing| {
                        ((listing.exchange.clone(), listing.symbol.clone()), listing)
                    }),
                );
            }
            Err(e) => {
                tracing::debug!(
                    "Failed to discover raw contract listings for product {} on {}: {}",
                    product_code,
                    exchange,
                    e
                );
            }
        }
    }

    Ok(listings.into_values().collect())
}

pub(crate) async fn discover_all_symbols_with_handle(
    ticker: &RithmicTickerPlantHandle,
    exchanges: &[String],
) -> Result<Vec<RithmicInstrumentSymbol>> {
    let mut listings = BTreeMap::new();
    let mut last_error = None;

    for exchange in exchanges {
        match discover_exchange_symbols_with_handle(ticker, exchange).await {
            Ok(found) => {
                listings.extend(
                    found.into_iter().map(|listing| {
                        ((listing.exchange.clone(), listing.symbol.clone()), listing)
                    }),
                );
            }
            Err(e) => {
                tracing::warn!(
                    "Failed to discover raw contract listings from {}: {}",
                    exchange,
                    e
                );
                last_error = Some(format!("{exchange}: {e}"));
            }
        }
    }

    if listings.is_empty()
        && let Some(e) = last_error
    {
        return Err(RithmicError::Instrument(format!(
            "Failed to discover any supported Rithmic contract listings: {e}"
        )));
    }

    Ok(listings.into_values().collect())
}

#[cfg(test)]
mod tests {
    use rithmic_rs::rti::ResponseSearchSymbols;
    use rstest::rstest;

    use super::*;

    #[rstest]
    fn test_normalize_discovered_symbol_strips_exchange_suffix() {
        assert_eq!(
            normalize_discovered_symbol("ESM6.CME"),
            Some("ESM6".to_string())
        );
        assert_eq!(
            normalize_discovered_symbol("ESM6"),
            Some("ESM6".to_string())
        );
        assert_eq!(normalize_discovered_symbol("   "), None);
    }

    #[rstest]
    fn test_instrument_symbol_from_search_result_uses_defaults_and_strips_suffix() {
        let search = ResponseSearchSymbols {
            template_id: 0,
            user_msg: Vec::new(),
            rq_handler_rp_code: Vec::new(),
            rp_code: Vec::new(),
            symbol: Some("MNQM6.CME".to_string()),
            exchange: None,
            symbol_name: Some("Micro E-mini Nasdaq-100".to_string()),
            product_code: None,
            instrument_type: Some("FUTURE".to_string()),
            expiration_date: Some("20260619".to_string()),
        };

        let listing = instrument_symbol_from_search_result(&search, "MNQ", "CME").expect("listing");

        assert_eq!(listing.symbol, "MNQM6");
        assert_eq!(listing.exchange, "CME");
        assert_eq!(listing.product_code, "MNQ");
        assert_eq!(
            listing.description.as_deref(),
            Some("Micro E-mini Nasdaq-100")
        );
        assert_eq!(listing.expiration_date.as_deref(), Some("20260619"));
        assert_eq!(listing.instrument_id().unwrap(), "MNQM6.CME.RITHMIC");
    }

    #[rstest]
    fn test_instrument_symbol_from_search_result_skips_non_futures() {
        let search = ResponseSearchSymbols {
            template_id: 0,
            user_msg: Vec::new(),
            rq_handler_rp_code: Vec::new(),
            rp_code: Vec::new(),
            symbol: Some("ESM6".to_string()),
            exchange: Some("CME".to_string()),
            symbol_name: None,
            product_code: Some("ES".to_string()),
            instrument_type: Some("OPTION".to_string()),
            expiration_date: None,
        };

        assert!(instrument_symbol_from_search_result(&search, "ES", "CME").is_none());
    }
}
