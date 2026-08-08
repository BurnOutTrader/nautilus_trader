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

use std::str::FromStr;

use nautilus_model::{
    data::BarSpecification,
    enums::{BarAggregation, PriceType},
    identifiers::{InstrumentId, TraderId},
    types::Quantity,
};

pub(crate) fn parse_trader_id(key: &str, value: &str) -> anyhow::Result<TraderId> {
    TraderId::new_checked(value)
        .map_err(|error| anyhow::anyhow!("Invalid {key} value '{value}': {error}"))
}

pub(crate) fn parse_instrument_id(key: &str, value: &str) -> anyhow::Result<InstrumentId> {
    value
        .parse::<InstrumentId>()
        .map_err(|error| anyhow::anyhow!("Invalid {key} value '{value}': {error}"))
}

pub(crate) fn parse_positive_quantity(key: &str, value: &str) -> anyhow::Result<Quantity> {
    let quantity = value
        .parse::<Quantity>()
        .map_err(|error| anyhow::anyhow!("Invalid {key} value '{value}': {error}"))?;
    anyhow::ensure!(quantity.is_positive(), "{key} must be greater than zero");
    Ok(quantity)
}

pub(crate) fn parse_bar_specification(key: &str, value: &str) -> anyhow::Result<BarSpecification> {
    let mut parts = value.split('-');
    let (Some(step), Some(aggregation), Some(price_type), None) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        anyhow::bail!("Invalid {key} value '{value}', expected STEP-AGGREGATION-PRICE_TYPE");
    };

    let step = step
        .parse::<usize>()
        .map_err(|error| anyhow::anyhow!("Invalid bar step in {key} value '{value}': {error}"))?;
    let aggregation = BarAggregation::from_str(aggregation).map_err(|error| {
        anyhow::anyhow!("Invalid aggregation in {key} value '{value}': {error}")
    })?;
    let price_type = PriceType::from_str(price_type)
        .map_err(|error| anyhow::anyhow!("Invalid price type in {key} value '{value}': {error}"))?;

    BarSpecification::new_checked(step, aggregation, price_type)
        .map_err(|error| anyhow::anyhow!("Invalid {key} value '{value}': {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[rstest::rstest]
    fn rejects_malformed_trader_id() {
        let result = parse_trader_id("PROJECTX_TRADER_ID", "missing-separator-");

        assert!(result.is_err());
    }

    #[rstest::rstest]
    fn rejects_malformed_instrument_id() {
        let result = parse_instrument_id("PROJECTX_INSTRUMENT_ID", "MESM26");

        assert!(result.is_err());
    }

    #[rstest::rstest]
    #[case("not-a-number")]
    #[case("0")]
    fn rejects_invalid_trade_size(#[case] value: &str) {
        let result = parse_positive_quantity("PROJECTX_TRADE_SIZE", value);

        assert!(result.is_err());
    }

    #[rstest::rstest]
    #[case("not-a-spec")]
    #[case("0-MINUTE-LAST")]
    #[case("60-SECOND-LAST")]
    fn rejects_invalid_bar_specification(#[case] value: &str) {
        let result = parse_bar_specification("PROJECTX_BAR_SPEC", value);

        assert!(result.is_err());
    }

    #[rstest::rstest]
    fn parses_valid_example_inputs() {
        assert_eq!(
            parse_trader_id("PROJECTX_TRADER_ID", "RUST-PROJECTX-001")
                .expect("valid trader ID")
                .as_str(),
            "RUST-PROJECTX-001"
        );
        assert_eq!(
            parse_instrument_id("PROJECTX_INSTRUMENT_ID", "MESM26.PROJECTX")
                .expect("valid instrument ID")
                .to_string(),
            "MESM26.PROJECTX"
        );
        assert_eq!(
            parse_positive_quantity("PROJECTX_TRADE_SIZE", "1.25")
                .expect("valid positive quantity")
                .to_string(),
            "1.25"
        );
        assert_eq!(
            parse_bar_specification("PROJECTX_BAR_SPEC", "1-MINUTE-LAST")
                .expect("valid bar specification")
                .to_string(),
            "1-MINUTE-LAST"
        );
    }
}
