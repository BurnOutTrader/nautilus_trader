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

//! Parsing utilities for Rithmic instrument data.

use std::str::FromStr;

use chrono::NaiveDate;
use nautilus_core::{Params, UnixNanos};
use nautilus_model::{
    enums::AssetClass,
    identifiers::{InstrumentId, Symbol},
    instruments::FuturesContract,
    types::{Currency, Price, Quantity},
};
use rithmic_rs::rti::{ResponseAuxilliaryReferenceData, ResponseReferenceData};
use serde_json::json;
use ustr::Ustr;

use crate::{
    common::{
        consts::{exchanges, products},
        parse::tick_size_to_precision,
    },
    error::{Result, RithmicError},
};

const CME_ONLY: &[&str] = &[exchanges::CME];
const CBOT_ONLY: &[&str] = &[exchanges::CBOT];
const NYMEX_ONLY: &[&str] = &[exchanges::NYMEX];
const COMEX_ONLY: &[&str] = &[exchanges::COMEX];
const CBOT_OR_CME: &[&str] = &[exchanges::CBOT, exchanges::CME];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct SupportedProduct {
    pub code: &'static str,
    pub exchange_candidates: &'static [&'static str],
    pub asset_class: AssetClass,
}

const SUPPORTED_PRODUCTS: &[SupportedProduct] = &[
    SupportedProduct {
        code: products::ES,
        exchange_candidates: CME_ONLY,
        asset_class: AssetClass::Index,
    },
    SupportedProduct {
        code: products::MES,
        exchange_candidates: CME_ONLY,
        asset_class: AssetClass::Index,
    },
    SupportedProduct {
        code: products::NQ,
        exchange_candidates: CME_ONLY,
        asset_class: AssetClass::Index,
    },
    SupportedProduct {
        code: products::MNQ,
        exchange_candidates: CME_ONLY,
        asset_class: AssetClass::Index,
    },
    SupportedProduct {
        code: products::RTY,
        exchange_candidates: CME_ONLY,
        asset_class: AssetClass::Index,
    },
    SupportedProduct {
        code: products::M2K,
        exchange_candidates: CME_ONLY,
        asset_class: AssetClass::Index,
    },
    SupportedProduct {
        code: products::NKD,
        exchange_candidates: CME_ONLY,
        asset_class: AssetClass::Index,
    },
    SupportedProduct {
        code: products::YM,
        exchange_candidates: CBOT_ONLY,
        asset_class: AssetClass::Index,
    },
    SupportedProduct {
        code: products::EMD,
        exchange_candidates: CME_ONLY,
        asset_class: AssetClass::Index,
    },
    SupportedProduct {
        code: products::MYM,
        exchange_candidates: CBOT_OR_CME,
        asset_class: AssetClass::Index,
    },
    SupportedProduct {
        code: products::MBT,
        exchange_candidates: CME_ONLY,
        asset_class: AssetClass::Cryptocurrency,
    },
    SupportedProduct {
        code: products::MET,
        exchange_candidates: CME_ONLY,
        asset_class: AssetClass::Cryptocurrency,
    },
    SupportedProduct {
        code: products::FX_6A,
        exchange_candidates: CME_ONLY,
        asset_class: AssetClass::FX,
    },
    SupportedProduct {
        code: products::FX_6B,
        exchange_candidates: CME_ONLY,
        asset_class: AssetClass::FX,
    },
    SupportedProduct {
        code: products::FX_6C,
        exchange_candidates: CME_ONLY,
        asset_class: AssetClass::FX,
    },
    SupportedProduct {
        code: products::FX_6E,
        exchange_candidates: CME_ONLY,
        asset_class: AssetClass::FX,
    },
    SupportedProduct {
        code: products::FX_6J,
        exchange_candidates: CME_ONLY,
        asset_class: AssetClass::FX,
    },
    SupportedProduct {
        code: products::FX_6S,
        exchange_candidates: CME_ONLY,
        asset_class: AssetClass::FX,
    },
    SupportedProduct {
        code: products::FX_E7,
        exchange_candidates: CME_ONLY,
        asset_class: AssetClass::FX,
    },
    SupportedProduct {
        code: products::FX_M6E,
        exchange_candidates: CME_ONLY,
        asset_class: AssetClass::FX,
    },
    SupportedProduct {
        code: products::FX_M6A,
        exchange_candidates: CME_ONLY,
        asset_class: AssetClass::FX,
    },
    SupportedProduct {
        code: products::FX_6M,
        exchange_candidates: CME_ONLY,
        asset_class: AssetClass::FX,
    },
    SupportedProduct {
        code: products::FX_6N,
        exchange_candidates: CME_ONLY,
        asset_class: AssetClass::FX,
    },
    SupportedProduct {
        code: products::FX_M6B,
        exchange_candidates: CME_ONLY,
        asset_class: AssetClass::FX,
    },
    SupportedProduct {
        code: products::HE,
        exchange_candidates: CME_ONLY,
        asset_class: AssetClass::Commodity,
    },
    SupportedProduct {
        code: products::LE,
        exchange_candidates: CME_ONLY,
        asset_class: AssetClass::Commodity,
    },
    SupportedProduct {
        code: products::GF,
        exchange_candidates: CME_ONLY,
        asset_class: AssetClass::Commodity,
    },
    SupportedProduct {
        code: products::CL,
        exchange_candidates: NYMEX_ONLY,
        asset_class: AssetClass::Commodity,
    },
    SupportedProduct {
        code: products::QM,
        exchange_candidates: NYMEX_ONLY,
        asset_class: AssetClass::Commodity,
    },
    SupportedProduct {
        code: products::NG,
        exchange_candidates: NYMEX_ONLY,
        asset_class: AssetClass::Commodity,
    },
    SupportedProduct {
        code: products::QG,
        exchange_candidates: NYMEX_ONLY,
        asset_class: AssetClass::Commodity,
    },
    SupportedProduct {
        code: products::MCL,
        exchange_candidates: NYMEX_ONLY,
        asset_class: AssetClass::Commodity,
    },
    SupportedProduct {
        code: products::RB,
        exchange_candidates: NYMEX_ONLY,
        asset_class: AssetClass::Commodity,
    },
    SupportedProduct {
        code: products::HO,
        exchange_candidates: NYMEX_ONLY,
        asset_class: AssetClass::Commodity,
    },
    SupportedProduct {
        code: products::PL,
        exchange_candidates: NYMEX_ONLY,
        asset_class: AssetClass::Commodity,
    },
    SupportedProduct {
        code: products::MNG,
        exchange_candidates: NYMEX_ONLY,
        asset_class: AssetClass::Commodity,
    },
    SupportedProduct {
        code: products::ZC,
        exchange_candidates: CBOT_ONLY,
        asset_class: AssetClass::Commodity,
    },
    SupportedProduct {
        code: products::ZW,
        exchange_candidates: CBOT_ONLY,
        asset_class: AssetClass::Commodity,
    },
    SupportedProduct {
        code: products::ZS,
        exchange_candidates: CBOT_ONLY,
        asset_class: AssetClass::Commodity,
    },
    SupportedProduct {
        code: products::ZM,
        exchange_candidates: CBOT_ONLY,
        asset_class: AssetClass::Commodity,
    },
    SupportedProduct {
        code: products::ZL,
        exchange_candidates: CBOT_ONLY,
        asset_class: AssetClass::Commodity,
    },
    SupportedProduct {
        code: products::ZT,
        exchange_candidates: CBOT_ONLY,
        asset_class: AssetClass::Debt,
    },
    SupportedProduct {
        code: products::ZF,
        exchange_candidates: CBOT_ONLY,
        asset_class: AssetClass::Debt,
    },
    SupportedProduct {
        code: products::ZN,
        exchange_candidates: CBOT_ONLY,
        asset_class: AssetClass::Debt,
    },
    SupportedProduct {
        code: products::TN,
        exchange_candidates: CBOT_ONLY,
        asset_class: AssetClass::Debt,
    },
    SupportedProduct {
        code: products::ZB,
        exchange_candidates: CBOT_ONLY,
        asset_class: AssetClass::Debt,
    },
    SupportedProduct {
        code: products::UB,
        exchange_candidates: CBOT_ONLY,
        asset_class: AssetClass::Debt,
    },
    SupportedProduct {
        code: products::GC,
        exchange_candidates: COMEX_ONLY,
        asset_class: AssetClass::Commodity,
    },
    SupportedProduct {
        code: products::SI,
        exchange_candidates: COMEX_ONLY,
        asset_class: AssetClass::Commodity,
    },
    SupportedProduct {
        code: products::HG,
        exchange_candidates: COMEX_ONLY,
        asset_class: AssetClass::Commodity,
    },
    SupportedProduct {
        code: products::MGC,
        exchange_candidates: COMEX_ONLY,
        asset_class: AssetClass::Commodity,
    },
    SupportedProduct {
        code: products::SIL,
        exchange_candidates: COMEX_ONLY,
        asset_class: AssetClass::Commodity,
    },
    SupportedProduct {
        code: products::MHG,
        exchange_candidates: COMEX_ONLY,
        asset_class: AssetClass::Commodity,
    },
];

#[cfg(test)]
pub(crate) fn supported_products() -> &'static [SupportedProduct] {
    SUPPORTED_PRODUCTS
}

pub(crate) fn supported_products_for_exchange(exchange: &str) -> Vec<SupportedProduct> {
    SUPPORTED_PRODUCTS
        .iter()
        .copied()
        .filter(|product| {
            product
                .exchange_candidates
                .iter()
                .any(|candidate| candidate.eq_ignore_ascii_case(exchange))
        })
        .collect()
}

pub(crate) fn search_exchange_candidates(
    product: SupportedProduct,
    preferred_exchange: &str,
) -> Vec<&'static str> {
    product
        .exchange_candidates
        .iter()
        .copied()
        .find(|candidate| candidate.eq_ignore_ascii_case(preferred_exchange))
        .into_iter()
        .collect()
}

pub(crate) fn supported_product_for_symbol(symbol: &str) -> Option<SupportedProduct> {
    let symbol_upper = symbol.trim().to_ascii_uppercase();

    SUPPORTED_PRODUCTS
        .iter()
        .copied()
        .filter(|product| symbol_upper.starts_with(product.code))
        .max_by_key(|product| product.code.len())
}

pub(crate) fn candidate_exchanges_for_symbol(
    symbol: &str,
    preferred_exchange: Option<&str>,
) -> Vec<&'static str> {
    let Some(product) = supported_product_for_symbol(symbol) else {
        return preferred_exchange
            .map(str::to_ascii_uppercase)
            .and_then(|exchange| {
                exchanges::KNOWN_EXCHANGES
                    .iter()
                    .copied()
                    .find(|candidate| *candidate == exchange)
            })
            .into_iter()
            .collect();
    };

    preferred_exchange.map_or_else(
        || product.exchange_candidates.to_vec(),
        |preferred_exchange| search_exchange_candidates(product, preferred_exchange),
    )
}

fn supported_product_for_reference(
    exchange: &str,
    product_code: &str,
    symbol: &str,
) -> Option<&'static SupportedProduct> {
    let product_code = product_code.trim();

    if !product_code.is_empty()
        && let Some(product) = SUPPORTED_PRODUCTS.iter().find(|product| {
            product.code.eq_ignore_ascii_case(product_code)
                && product
                    .exchange_candidates
                    .iter()
                    .any(|candidate| candidate.eq_ignore_ascii_case(exchange))
        })
    {
        return Some(product);
    }

    supported_product_for_symbol(symbol).and_then(|product| {
        product
            .exchange_candidates
            .iter()
            .any(|candidate| candidate.eq_ignore_ascii_case(exchange))
            .then_some(
                SUPPORTED_PRODUCTS
                    .iter()
                    .find(|candidate| candidate.code == product.code)?,
            )
    })
}

fn build_instrument_id(symbol: &str, exchange: &str) -> Result<InstrumentId> {
    crate::common::converters::rithmic_instrument_id(symbol, exchange)
}

fn build_info(exchange: &str, product_code: &str, description: &str, is_tradeable: bool) -> Params {
    let mut info = Params::new();
    info.insert("exchange".to_string(), json!(exchange));
    info.insert("product_code".to_string(), json!(product_code));
    info.insert("description".to_string(), json!(description));
    info.insert("is_tradeable".to_string(), json!(is_tradeable));
    info
}

/// Converts a Rithmic `ResponseReferenceData` to a Nautilus `FuturesContract`.
pub fn response_to_instrument(
    ref_data: &ResponseReferenceData,
    ts_init: UnixNanos,
) -> Result<FuturesContract> {
    let symbol = ref_data
        .symbol
        .as_ref()
        .ok_or_else(|| RithmicError::Instrument("Missing symbol in reference data".to_string()))?
        .clone();

    let exchange = ref_data
        .exchange
        .as_ref()
        .ok_or_else(|| RithmicError::Instrument("Missing exchange in reference data".to_string()))?
        .clone();

    let product_code = ref_data.product_code.clone().unwrap_or_default();
    let description = ref_data.symbol_name.clone().unwrap_or_default();
    let supported_product = supported_product_for_reference(&exchange, &product_code, &symbol)
        .ok_or_else(|| {
            let product_label = if product_code.trim().is_empty() {
                symbol.clone()
            } else {
                product_code.clone()
            };

            RithmicError::Instrument(format!(
                "Unsupported Rithmic product '{product_label}' on {exchange}. \
Only the hard-coded supported product list is available."
            ))
        })?;

    let tick_size = ref_data.min_qprice_change.ok_or_else(|| {
        RithmicError::Instrument(format!("Missing tick size in reference data for {symbol}"))
    })?;
    if !tick_size.is_finite() || tick_size <= 0.0 {
        return Err(RithmicError::Instrument(format!(
            "Invalid tick size {tick_size} in reference data for {symbol}"
        )));
    }

    let point_value = ref_data.single_point_value.ok_or_else(|| {
        RithmicError::Instrument(format!(
            "Missing point value in reference data for {symbol}"
        ))
    })?;
    if !point_value.is_finite() || point_value <= 0.0 {
        return Err(RithmicError::Instrument(format!(
            "Invalid point value {point_value} in reference data for {symbol}"
        )));
    }

    let currency_code = ref_data.currency.as_deref().ok_or_else(|| {
        RithmicError::Instrument(format!("Missing currency in reference data for {symbol}"))
    })?;
    let currency_code = currency_code.trim();
    if currency_code.is_empty() {
        return Err(RithmicError::Instrument(format!(
            "Empty currency in reference data for {symbol}"
        )));
    }
    if !currency_code.eq_ignore_ascii_case("USD") {
        return Err(RithmicError::Instrument(format!(
            "Unsupported settlement currency '{currency_code}' for {symbol}; the supported Rithmic catalog is USD-margined"
        )));
    }
    let currency = Currency::from_str("USD").map_err(|e| {
        RithmicError::Instrument(format!(
            "Invalid currency '{currency_code}' in reference data for {symbol}: {e}"
        ))
    })?;

    let price_precision = tick_size_to_precision(tick_size)?;
    let price_increment = Price::new_checked(tick_size, price_precision).map_err(|e| {
        RithmicError::Instrument(format!(
            "Invalid tick size {tick_size} in reference data for {symbol}: {e}"
        ))
    })?;
    let multiplier = point_value.to_string().parse::<Quantity>().map_err(|e| {
        RithmicError::Instrument(format!(
            "Invalid point value {point_value} in reference data for {symbol}: {e}"
        ))
    })?;
    let lot_size = Quantity::from_mantissa_exponent_checked(1, 0, 0)
        .map_err(|e| RithmicError::Instrument(format!("Invalid lot size for {symbol}: {e}")))?;
    let asset_class = supported_product.asset_class;
    let underlying = supported_product.code;

    let expiration_date = ref_data.expiration_date.as_deref().ok_or_else(|| {
        RithmicError::Instrument(format!(
            "Missing expiration date in reference data for {symbol}"
        ))
    })?;
    let expiration_ns = parse_expiration_date(expiration_date).map_err(|e| {
        RithmicError::Instrument(format!(
            "Invalid expiration date '{expiration_date}' in reference data for {symbol}: {e}"
        ))
    })?;

    let is_tradeable = match ref_data.is_tradable.as_deref().map(str::trim) {
        Some(value) if value.eq_ignore_ascii_case("true") || value == "1" => true,
        Some(value) if value.eq_ignore_ascii_case("false") || value == "0" => false,
        Some(value) => {
            return Err(RithmicError::Instrument(format!(
                "Invalid is_tradable value '{value}' in reference data for {symbol}"
            )));
        }
        None => {
            return Err(RithmicError::Instrument(format!(
                "Missing is_tradable value in reference data for {symbol}"
            )));
        }
    };

    let instrument_id = build_instrument_id(&symbol, &exchange)?;
    let raw_symbol = Symbol::new_checked(&symbol).map_err(|e| {
        RithmicError::Instrument(format!("Invalid Rithmic raw symbol {symbol:?}: {e}"))
    })?;

    FuturesContract::new_checked(
        instrument_id,
        raw_symbol,
        asset_class,
        Some(Ustr::from(exchange.as_str())),
        Ustr::from(underlying),
        UnixNanos::from(0),
        UnixNanos::from(expiration_ns),
        currency,
        price_precision,
        price_increment,
        multiplier,
        lot_size,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        Some(build_info(
            &exchange,
            supported_product.code,
            &description,
            is_tradeable,
        )),
        ts_init,
        ts_init,
    )
    .map_err(|e| RithmicError::Instrument(format!("Invalid futures contract for {symbol}: {e}")))
}

/// Applies auxiliary reference data to an already parsed instrument.
pub fn apply_auxiliary_reference_data(
    instrument: &mut FuturesContract,
    aux_data: &ResponseAuxilliaryReferenceData,
) -> Result<()> {
    if let Some(date) = aux_data.first_trading_date.as_deref() {
        instrument.activation_ns = UnixNanos::from(parse_expiration_date(date)?);
    }
    Ok(())
}

/// Parses a Rithmic expiration date string to Unix timestamp (nanoseconds).
pub fn parse_expiration_date(date_str: &str) -> Result<u64> {
    let date = NaiveDate::parse_from_str(date_str, "%Y%m%d").map_err(|e| {
        RithmicError::Parse(format!("Invalid expiration date format '{date_str}': {e}"))
    })?;

    let timestamp = date
        .and_hms_opt(0, 0, 0)
        .and_then(|datetime| datetime.and_utc().timestamp_nanos_opt())
        .ok_or_else(|| {
            RithmicError::Parse(format!("Unable to convert expiration date '{date_str}'"))
        })?;

    u64::try_from(timestamp).map_err(|_| {
        RithmicError::Parse(format!(
            "Expiration date '{date_str}' produced a negative timestamp"
        ))
    })
}

#[cfg(test)]
mod tests {
    use nautilus_model::{enums::AssetClass, identifiers::InstrumentId, types::Price};
    use rstest::rstest;
    use ustr::Ustr;

    use super::*;
    use crate::common::consts::{exchanges, products};

    fn reference_data(
        product_code: Option<&str>,
        description: Option<&str>,
    ) -> ResponseReferenceData {
        ResponseReferenceData {
            template_id: 15,
            user_msg: vec![],
            rp_code: vec![],
            presence_bits: None,
            clear_bits: None,
            symbol: Some("ESH5".to_string()),
            exchange: Some("CME".to_string()),
            exchange_symbol: None,
            symbol_name: description.map(str::to_string),
            trading_symbol: None,
            trading_exchange: None,
            product_code: product_code.map(str::to_string),
            instrument_type: Some("Future".to_string()),
            underlying_symbol: None,
            expiration_date: Some("20250321".to_string()),
            currency: Some("USD".to_string()),
            put_call_indicator: None,
            tick_size_type: None,
            price_display_format: None,
            is_tradable: Some("true".to_string()),
            is_underlying_for_binary_contrats: None,
            strike_price: None,
            ftoq_price: None,
            qtof_price: None,
            min_qprice_change: Some(0.25),
            min_fprice_change: None,
            single_point_value: Some(50.0),
        }
    }

    #[rstest]
    fn test_response_to_instrument() {
        let ts_init = UnixNanos::from(42);
        let instrument = response_to_instrument(
            &reference_data(Some("ES"), Some("E-mini S&P 500 Mar25")),
            ts_init,
        )
        .unwrap();

        assert_eq!(instrument.id, InstrumentId::from("ESH5.CME.RITHMIC"));
        assert_eq!(instrument.raw_symbol, Symbol::new("ESH5"));
        assert_eq!(instrument.asset_class, AssetClass::Index);
        assert_eq!(instrument.exchange, Some(Ustr::from("CME")));
        assert_eq!(instrument.underlying, Ustr::from("ES"));
        assert_eq!(instrument.price_increment, Price::new(0.25, 2));
        assert_eq!(instrument.multiplier, Quantity::from("50"));
        assert_eq!(instrument.lot_size, Quantity::from(1));
        assert_eq!(
            instrument.expiration_ns.as_u64(),
            parse_expiration_date("20250321").unwrap()
        );
        assert_eq!(instrument.activation_ns.as_u64(), 0);
        assert_eq!(instrument.ts_event, ts_init);
        assert_eq!(instrument.ts_init, ts_init);
        assert_eq!(
            instrument
                .info
                .as_ref()
                .and_then(|info| info.get_bool("is_tradeable")),
            Some(true)
        );
    }

    #[rstest]
    fn test_response_to_instrument_missing_symbol() {
        let mut ref_data = reference_data(Some("ES"), Some("E-mini S&P 500 Mar25"));
        ref_data.symbol = None;

        let result = response_to_instrument(&ref_data, UnixNanos::default());
        assert!(result.is_err());
    }

    #[rstest]
    fn test_response_to_instrument_rejects_incomplete_definition() {
        let ref_data = ResponseReferenceData {
            template_id: 15,
            user_msg: vec![],
            rp_code: vec![],
            presence_bits: None,
            clear_bits: None,
            symbol: Some("NQZ4".to_string()),
            exchange: Some("CME".to_string()),
            exchange_symbol: None,
            symbol_name: None,
            trading_symbol: None,
            trading_exchange: None,
            product_code: None,
            instrument_type: None,
            underlying_symbol: None,
            expiration_date: None,
            currency: None,
            put_call_indicator: None,
            tick_size_type: None,
            price_display_format: None,
            is_tradable: None,
            is_underlying_for_binary_contrats: None,
            strike_price: None,
            ftoq_price: None,
            qtof_price: None,
            min_qprice_change: None,
            min_fprice_change: None,
            single_point_value: None,
        };

        assert!(response_to_instrument(&ref_data, UnixNanos::default()).is_err());
    }

    #[rstest]
    fn test_response_to_instrument_rejects_each_missing_required_field() {
        let complete = reference_data(Some("ES"), Some("E-mini S&P 500 Mar25"));
        let incomplete = [
            {
                let mut value = complete.clone();
                value.min_qprice_change = None;
                value
            },
            {
                let mut value = complete.clone();
                value.single_point_value = None;
                value
            },
            {
                let mut value = complete.clone();
                value.currency = None;
                value
            },
            {
                let mut value = complete.clone();
                value.expiration_date = None;
                value
            },
            {
                let mut value = complete;
                value.is_tradable = None;
                value
            },
        ];

        for ref_data in incomplete {
            assert!(response_to_instrument(&ref_data, UnixNanos::default()).is_err());
        }
    }

    #[rstest]
    fn test_response_to_instrument_rejects_malformed_required_fields() {
        let complete = reference_data(Some("ES"), Some("E-mini S&P 500 Mar25"));
        let malformed = [
            {
                let mut value = complete.clone();
                value.min_qprice_change = Some(f64::NAN);
                value
            },
            {
                let mut value = complete.clone();
                value.single_point_value = Some(f64::INFINITY);
                value
            },
            {
                let mut value = complete.clone();
                value.currency = Some("NOT-A-CURRENCY".to_string());
                value
            },
            {
                let mut value = complete.clone();
                value.currency = Some("EUR".to_string());
                value
            },
            {
                let mut value = complete.clone();
                value.expiration_date = Some("20241399".to_string());
                value
            },
            {
                let mut value = complete;
                value.is_tradable = Some("yes".to_string());
                value
            },
        ];

        for ref_data in malformed {
            assert!(response_to_instrument(&ref_data, UnixNanos::default()).is_err());
        }
    }

    #[rstest]
    #[case("true", true)]
    #[case("false", false)]
    #[case("1", true)]
    #[case("0", false)]
    fn test_response_to_instrument_accepts_documented_tradable_tokens(
        #[case] token: &str,
        #[case] expected: bool,
    ) {
        let mut ref_data = reference_data(Some("ES"), Some("E-mini S&P 500 Mar25"));
        ref_data.is_tradable = Some(token.to_string());

        let instrument = response_to_instrument(&ref_data, UnixNanos::default()).unwrap();
        assert_eq!(
            instrument
                .info
                .as_ref()
                .and_then(|info| info.get_bool("is_tradeable")),
            Some(expected)
        );
    }

    #[rstest]
    #[case("CME", "ES", "ESH5", AssetClass::Index)]
    #[case("CME", "M6E", "M6EM5", AssetClass::FX)]
    #[case("CBOT", "ZN", "ZNM5", AssetClass::Debt)]
    #[case("CBOT", "TN", "TNM5", AssetClass::Debt)]
    #[case("NYMEX", "CL", "CLM5", AssetClass::Commodity)]
    #[case("COMEX", "GC", "GCM5", AssetClass::Commodity)]
    #[case("NYMEX", "PL", "PLN5", AssetClass::Commodity)]
    #[case("CME", "MBT", "MBTM5", AssetClass::Cryptocurrency)]
    #[case("CME", "MET", "METM5", AssetClass::Cryptocurrency)]
    #[case("CME", "", "MNQM5", AssetClass::Index)]
    #[case("CME", "E7", "E7M5", AssetClass::FX)]
    #[case("CBOT", "MYM", "MYMM5", AssetClass::Index)]
    #[case("CME", "MYM", "MYMM5", AssetClass::Index)]
    fn test_supported_product_for_reference(
        #[case] exchange: &str,
        #[case] product_code: &str,
        #[case] symbol: &str,
        #[case] expected: AssetClass,
    ) {
        let supported = supported_product_for_reference(exchange, product_code, symbol).unwrap();
        assert_eq!(supported.asset_class, expected);
    }

    #[rstest]
    fn test_response_to_instrument_rejects_unsupported_product() {
        let mut ref_data = reference_data(Some("FESX"), Some("Euro Stoxx 50"));
        ref_data.symbol = Some("FESXM5".to_string());
        let result = response_to_instrument(&ref_data, UnixNanos::default());
        assert!(result.is_err());
    }

    #[rstest]
    fn test_supported_products_for_exchange_returns_only_hard_coded_scope() {
        let cme_products = supported_products_for_exchange("CME");

        assert!(
            cme_products
                .iter()
                .any(|product| product.code == products::ES)
        );
        assert!(
            cme_products
                .iter()
                .any(|product| product.code == products::MYM)
        );
        assert!(!cme_products.iter().any(|product| product.code == "FESX"));
    }

    #[rstest]
    fn test_search_exchange_candidates_treats_requested_exchange_as_authoritative() {
        let mym = supported_products()
            .iter()
            .find(|product| product.code == products::MYM)
            .copied()
            .unwrap();

        assert_eq!(
            search_exchange_candidates(mym, exchanges::CBOT),
            vec![exchanges::CBOT]
        );
        assert_eq!(
            search_exchange_candidates(mym, exchanges::CME),
            vec![exchanges::CME]
        );
        assert!(search_exchange_candidates(mym, exchanges::NYMEX).is_empty());
        assert!(candidate_exchanges_for_symbol("MYMZ6", Some("NYMEX")).is_empty());
    }

    #[rstest]
    fn test_apply_auxiliary_reference_data_sets_activation_ts() {
        let mut instrument = response_to_instrument(
            &reference_data(Some("ES"), Some("E-mini S&P 500 Mar25")),
            UnixNanos::default(),
        )
        .unwrap();
        let aux_data = ResponseAuxilliaryReferenceData {
            template_id: 19,
            user_msg: vec![],
            rp_code: vec![],
            presence_bits: None,
            clear_bits: None,
            symbol: Some("ESH5".to_string()),
            exchange: Some("CME".to_string()),
            settlement_method: None,
            first_notice_date: None,
            last_notice_date: None,
            first_trading_date: Some("20240616".to_string()),
            last_trading_date: None,
            first_delivery_date: None,
            last_delivery_date: None,
            first_position_date: None,
            last_position_date: None,
            unit_of_measure: None,
            unit_of_measure_qty: None,
        };

        apply_auxiliary_reference_data(&mut instrument, &aux_data).unwrap();

        assert_eq!(
            instrument.activation_ns.as_u64(),
            parse_expiration_date("20240616").unwrap()
        );
    }

    #[rstest]
    fn test_apply_auxiliary_reference_data_rejects_malformed_activation_date() {
        let mut instrument = response_to_instrument(
            &reference_data(Some("ES"), Some("E-mini S&P 500 Mar25")),
            UnixNanos::default(),
        )
        .unwrap();
        let aux_data = ResponseAuxilliaryReferenceData {
            first_trading_date: Some("not-a-date".to_string()),
            ..Default::default()
        };

        assert!(apply_auxiliary_reference_data(&mut instrument, &aux_data).is_err());
        assert_eq!(instrument.activation_ns, UnixNanos::default());
    }

    #[rstest]
    fn test_parse_expiration_date() {
        let ts = parse_expiration_date("20240315").unwrap();
        assert!(ts > 0);

        let ts2 = parse_expiration_date("20241231").unwrap();
        assert!(ts2 > ts);
    }

    #[rstest]
    fn test_parse_expiration_date_invalid() {
        assert!(parse_expiration_date("2024").is_err());
        assert!(parse_expiration_date("2024XX15").is_err());
        assert!(parse_expiration_date("20241315").is_err());
    }
}
