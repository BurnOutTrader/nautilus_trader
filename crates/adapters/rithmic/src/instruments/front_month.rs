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

//! Shared front-month bootstrap helpers for the Rithmic adapter.

use std::time::Duration;

use nautilus_core::UnixNanos;
use nautilus_model::instruments::InstrumentAny;
use rithmic_rs::{
    error::RithmicError as RsRithmicError,
    plants::ticker_plant::RithmicTickerPlantHandle,
    rti::{
        ResponseFrontMonthContract, ResponseGetInstrumentByUnderlying, messages::RithmicMessage,
    },
};
use tokio::time::sleep;

use super::parse::{
    apply_auxiliary_reference_data, parse_expiration_date, response_to_instrument,
    supported_products_for_exchange,
};
use crate::error::{Result, RithmicError};

const FRONT_MONTH_REQUEST_RETRIES: usize = 3;
const FRONT_MONTH_RETRY_DELAY_MS: u64 = 250;
const FRONT_MONTH_BULK_PAUSE_MS: u64 = 25;

#[derive(Debug, Clone, Eq, PartialEq)]
struct FrontMonthCandidate {
    symbol: String,
    exchange: String,
    expiration_ns: u64,
}

fn is_retriable_front_month_error(error: &RsRithmicError) -> bool {
    matches!(
        error,
        RsRithmicError::ConnectionClosed
            | RsRithmicError::SendFailed
            | RsRithmicError::ConnectionFailed(_)
    )
}

fn finalize_supported_front_months(
    exchange: &str,
    requested_products: &[&str],
    instruments: Vec<InstrumentAny>,
    errors: &[String],
) -> Result<Vec<InstrumentAny>> {
    if !instruments.is_empty() || errors.is_empty() {
        return Ok(instruments);
    }

    let products = requested_products.join(", ");
    let failures = errors.join("; ");
    Err(RithmicError::Instrument(format!(
        "Failed to load any supported front months on {exchange} for roots [{products}]: {failures}"
    )))
}

pub(crate) fn instrument_is_tradeable(instrument: &InstrumentAny) -> bool {
    match instrument {
        InstrumentAny::FuturesContract(contract) => contract
            .info
            .as_ref()
            .and_then(|info| info.get_bool("is_tradeable"))
            .unwrap_or(true),
        _ => true,
    }
}

pub(crate) fn front_month_contract(
    response: &ResponseFrontMonthContract,
) -> Result<(String, String)> {
    let symbol = response
        .trading_symbol
        .as_deref()
        .or(response.symbol.as_deref())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            RithmicError::Instrument(
                "Front month response did not contain a contract symbol".to_string(),
            )
        })?;

    let exchange = response
        .trading_exchange
        .as_deref()
        .or(response.exchange.as_deref())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            RithmicError::Instrument(format!(
                "Front month response for {symbol} did not contain an exchange"
            ))
        })?;

    Ok((symbol.to_string(), exchange.to_string()))
}

fn front_month_candidate_from_underlying_response(
    response: &ResponseGetInstrumentByUnderlying,
    requested_product: &str,
    requested_exchange: &str,
) -> Option<FrontMonthCandidate> {
    let symbol = response.symbol.as_deref()?.trim();
    let exchange = response.exchange.as_deref()?.trim();

    if symbol.is_empty() || exchange.is_empty() {
        return None;
    }

    let instrument_type = response
        .instrument_type
        .as_deref()
        .unwrap_or_default()
        .trim();
    if !instrument_type.is_empty() && !instrument_type.eq_ignore_ascii_case("future") {
        return None;
    }

    let product_code = response
        .product_code
        .as_deref()
        .or(response.underlying_symbol.as_deref())
        .unwrap_or_default()
        .trim();

    if !product_code.is_empty() {
        if !product_code.eq_ignore_ascii_case(requested_product) {
            return None;
        }
    } else if !symbol
        .to_ascii_uppercase()
        .starts_with(&requested_product.to_ascii_uppercase())
    {
        return None;
    }

    if !exchange.eq_ignore_ascii_case(requested_exchange) {
        return None;
    }

    let expiration_ns = parse_expiration_date(response.expiration_date.as_deref()?).ok()?;

    Some(FrontMonthCandidate {
        symbol: symbol.to_string(),
        exchange: exchange.to_string(),
        expiration_ns,
    })
}

fn select_front_month_candidate(candidates: &[FrontMonthCandidate]) -> Option<FrontMonthCandidate> {
    let now_ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos() as u64);

    candidates
        .iter()
        .filter(|candidate| candidate.expiration_ns >= now_ns)
        .min_by_key(|candidate| candidate.expiration_ns)
        .cloned()
        .or_else(|| {
            candidates
                .iter()
                .min_by_key(|candidate| candidate.expiration_ns)
                .cloned()
        })
}

async fn resolve_front_month_contract_by_underlying_with_handle(
    ticker: &RithmicTickerPlantHandle,
    product: &str,
    exchange: &str,
) -> Result<(String, String)> {
    let responses = ticker
        .get_instrument_by_underlying(product, exchange, None)
        .await
        .map_err(|e| {
            RithmicError::Api(format!(
                "Underlying instrument request failed for {product} on {exchange}: {e}"
            ))
        })?;

    let mut candidates = Vec::new();
    let mut errors = Vec::new();
    let mut samples = Vec::new();

    for response in responses {
        if let Some(e) = response.error {
            errors.push(e);
            continue;
        }

        let RithmicMessage::ResponseGetInstrumentByUnderlying(ref_data) = response.message else {
            continue;
        };

        if samples.len() < 5 {
            samples.push(format!(
                "symbol={:?} exchange={:?} product_code={:?} underlying={:?} instrument_type={:?} expiration_date={:?}",
                ref_data.symbol,
                ref_data.exchange,
                ref_data.product_code,
                ref_data.underlying_symbol,
                ref_data.instrument_type,
                ref_data.expiration_date
            ));
        }

        if let Some(candidate) =
            front_month_candidate_from_underlying_response(&ref_data, product, exchange)
        {
            candidates.push(candidate);
        }
    }

    if let Some(candidate) = select_front_month_candidate(&candidates) {
        return Ok((candidate.symbol, candidate.exchange));
    }

    let error_suffix = if errors.is_empty() {
        String::new()
    } else {
        format!(
            ": {}",
            errors
                .iter()
                .map(|e| e.to_string())
                .collect::<Vec<_>>()
                .join("; ")
        )
    };
    let sample_suffix = if samples.is_empty() {
        String::new()
    } else {
        format!("; samples=[{}]", samples.join(" | "))
    };

    Err(RithmicError::Instrument(format!(
        "No front-month candidate found from underlying instruments for {product} on {exchange}{error_suffix}{sample_suffix}"
    )))
}

async fn request_front_month_contract_with_handle(
    ticker: &RithmicTickerPlantHandle,
    product: &str,
    exchange: &str,
) -> Result<(String, String)> {
    for attempt in 1..=FRONT_MONTH_REQUEST_RETRIES {
        match ticker
            .get_front_month_contract(product, exchange, false)
            .await
        {
            Ok(response) => {
                if let Some(e) = &response.error {
                    return Err(RithmicError::Instrument(format!(
                        "Front month error for {product} on {exchange}: {e}"
                    )));
                }

                let RithmicMessage::ResponseFrontMonthContract(front_month) = &response.message
                else {
                    return Err(RithmicError::Instrument(format!(
                        "Unexpected response type for front month request {product} on {exchange}: {:?}",
                        response.message
                    )));
                };

                return front_month_contract(front_month);
            }
            Err(e)
                if attempt < FRONT_MONTH_REQUEST_RETRIES && is_retriable_front_month_error(&e) =>
            {
                tracing::warn!(
                    "Front month request attempt {attempt}/{FRONT_MONTH_REQUEST_RETRIES} failed for {product} on {exchange}: {e}"
                );
                sleep(Duration::from_millis(FRONT_MONTH_RETRY_DELAY_MS)).await;
            }
            Err(e) => {
                return Err(RithmicError::Api(format!(
                    "Front month request failed for {product} on {exchange}: {e}"
                )));
            }
        }
    }

    Err(RithmicError::Api(format!(
        "Front month request retry loop exhausted for {product} on {exchange}"
    )))
}

pub(crate) async fn resolve_front_month_contract_with_handle(
    ticker: &RithmicTickerPlantHandle,
    product: &str,
    exchange: &str,
) -> Result<(String, String)> {
    match resolve_front_month_contract_by_underlying_with_handle(ticker, product, exchange).await {
        Ok(contract) => Ok(contract),
        Err(underlying_error) => {
            tracing::warn!(
                "Underlying front-month resolution failed for {} on {}: {}. Falling back to direct front-month request.",
                product,
                exchange,
                underlying_error
            );
            match request_front_month_contract_with_handle(ticker, product, exchange).await {
                Ok(contract) => Ok(contract),
                Err(direct_error) => Err(RithmicError::Instrument(format!(
                    "Underlying resolution failed for {product} on {exchange}: {underlying_error}; direct front-month request also failed: {direct_error}"
                ))),
            }
        }
    }
}

pub(crate) async fn fetch_instrument_with_handle(
    ticker: &RithmicTickerPlantHandle,
    symbol: &str,
    exchange: &str,
    ts_init: UnixNanos,
) -> Result<InstrumentAny> {
    let response = ticker
        .get_reference_data(symbol, exchange)
        .await
        .map_err(|e| {
            RithmicError::Api(format!(
                "Reference data request failed for {symbol} on {exchange}: {e}"
            ))
        })?;

    if let Some(e) = &response.error {
        return Err(RithmicError::Instrument(format!(
            "Reference data error for {symbol} on {exchange}: {e}"
        )));
    }

    let RithmicMessage::ResponseReferenceData(ref_data) = &response.message else {
        return Err(RithmicError::Instrument(format!(
            "Unexpected response type for reference data {symbol} on {exchange}: {:?}",
            response.message
        )));
    };

    let mut instrument = response_to_instrument(ref_data, ts_init)?;

    match ticker.get_auxilliary_reference_data(symbol, exchange).await {
        Ok(aux_response) => {
            if aux_response.error.is_none() {
                if let RithmicMessage::ResponseAuxilliaryReferenceData(aux_data) =
                    &aux_response.message
                {
                    apply_auxiliary_reference_data(&mut instrument, aux_data);
                }
            } else if let Some(e) = aux_response.error {
                tracing::debug!(
                    "Ignoring auxiliary reference data error for {symbol} on {exchange}: {e}"
                );
            }
        }
        Err(e) => {
            tracing::debug!(
                "Ignoring auxiliary reference data request failure for {symbol} on {exchange}: {e}"
            );
        }
    }

    Ok(InstrumentAny::FuturesContract(instrument))
}

pub(crate) async fn load_front_month_instrument_with_handle(
    ticker: &RithmicTickerPlantHandle,
    product: &str,
    exchange: &str,
    ts_init: UnixNanos,
) -> Result<InstrumentAny> {
    let (contract_symbol, contract_exchange) =
        resolve_front_month_contract_with_handle(ticker, product, exchange).await?;
    fetch_instrument_with_handle(ticker, &contract_symbol, &contract_exchange, ts_init).await
}

pub(crate) async fn load_supported_front_months_with_handle(
    ticker: &RithmicTickerPlantHandle,
    exchange: &str,
    ts_init: UnixNanos,
    tradeable_only: bool,
) -> Result<Vec<InstrumentAny>> {
    let products = supported_products_for_exchange(exchange);
    let requested_products = products
        .iter()
        .map(|product| product.code)
        .collect::<Vec<_>>();

    let mut instruments = Vec::new();
    let mut errors = Vec::new();

    for (index, product) in products.into_iter().enumerate() {
        if index > 0 {
            sleep(Duration::from_millis(FRONT_MONTH_BULK_PAUSE_MS)).await;
        }

        match load_front_month_instrument_with_handle(ticker, product.code, exchange, ts_init).await
        {
            Ok(instrument) => {
                if tradeable_only && !instrument_is_tradeable(&instrument) {
                    continue;
                }

                instruments.push(instrument);
            }
            Err(e) => {
                tracing::warn!(
                    "Supported front-month bootstrap failed for {} on {}: {}",
                    product.code,
                    exchange,
                    e
                );
                errors.push(format!("{}: {}", product.code, e));
            }
        }
    }

    finalize_supported_front_months(exchange, &requested_products, instruments, &errors)
}

#[cfg(test)]
mod tests {
    use nautilus_model::{
        enums::AssetClass,
        identifiers::{InstrumentId, Symbol},
        instruments::{FuturesContract, Instrument},
        types::{Currency, Price, Quantity},
    };
    use rstest::rstest;
    use ustr::Ustr;

    use super::*;

    fn test_instrument(symbol: &str, exchange: &str, tradeable: bool) -> InstrumentAny {
        let mut info = nautilus_core::Params::new();
        info.insert(
            "is_tradeable".to_string(),
            serde_json::Value::Bool(tradeable),
        );

        InstrumentAny::FuturesContract(FuturesContract::new(
            InstrumentId::from(format!("{symbol}.RITHMIC")),
            Symbol::new(symbol),
            AssetClass::Index,
            Some(Ustr::from(exchange)),
            Ustr::from("ES"),
            UnixNanos::from(1),
            UnixNanos::from(2),
            Currency::USD(),
            2,
            Price::new(0.25, 2),
            Quantity::from("50"),
            Quantity::from(1),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            Some(info),
            UnixNanos::from(3),
            UnixNanos::from(3),
        ))
    }

    #[rstest]
    fn test_front_month_contract_prefers_trading_symbol_and_exchange() {
        let response = ResponseFrontMonthContract {
            template_id: 0,
            user_msg: Vec::new(),
            rp_code: Vec::new(),
            symbol: Some("MNQ".to_string()),
            exchange: Some("CME".to_string()),
            is_front_month_symbol: Some(true),
            symbol_name: None,
            trading_symbol: Some("MNQM26".to_string()),
            trading_exchange: Some("CME".to_string()),
        };

        let (symbol, exchange) = front_month_contract(&response).expect("front month should parse");
        assert_eq!(symbol, "MNQM26");
        assert_eq!(exchange, "CME");
    }

    #[rstest]
    fn test_instrument_is_tradeable_reads_info_flag() {
        assert!(instrument_is_tradeable(&test_instrument(
            "ESM6", "CME", true
        )));
        assert!(!instrument_is_tradeable(&test_instrument(
            "ESM6", "CME", false
        )));
    }

    #[rstest]
    fn test_finalize_supported_front_months_keeps_partial_success() {
        let instruments = vec![test_instrument("ESM6", "CME", true)];
        let errors = vec!["NQ: connection closed".to_string()];
        let result = finalize_supported_front_months("CME", &["ES", "NQ"], instruments, &errors)
            .expect("partial success should not be discarded");

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].raw_symbol().as_str(), "ESM6");
    }

    #[rstest]
    fn test_finalize_supported_front_months_errors_when_everything_fails() {
        let errors = vec![
            "ES: connection closed".to_string(),
            "NQ: connection closed".to_string(),
        ];
        let error = finalize_supported_front_months("CME", &["ES", "NQ"], Vec::new(), &errors)
            .expect_err("all failures should bubble up");

        assert!(
            error
                .to_string()
                .contains("Failed to load any supported front months on CME")
        );
        assert!(error.to_string().contains("ES: connection closed"));
    }
}
