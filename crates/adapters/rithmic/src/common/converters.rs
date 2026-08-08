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

//! Symbol parsing utilities.

use chrono::{Datelike, Utc};
use nautilus_core::UnixNanos;
use nautilus_model::identifiers::{InstrumentId, Symbol};

use crate::{
    common::consts::RITHMIC_VENUE_ID,
    error::{Result, RithmicError},
};

const VALID_MONTH_CODES: &[char] = &['F', 'G', 'H', 'J', 'K', 'M', 'N', 'Q', 'U', 'V', 'X', 'Z'];

/// Returns the current wall-clock time in Unix nanoseconds.
#[must_use]
pub fn now_unix_nanos() -> UnixNanos {
    UnixNanos::from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |duration| {
                u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
            }),
    )
}

/// Builds a Rithmic `InstrumentId` from a venue symbol and exchange.
///
/// Rithmic symbols are not globally unique: the same symbol can be listed by
/// more than one exchange. Encoding the exchange into the Nautilus symbol
/// component makes the mapping reversible while retaining `RITHMIC` as the
/// venue (`{symbol}.{exchange}.RITHMIC`).
pub fn rithmic_instrument_id(symbol: &str, exchange: &str) -> Result<InstrumentId> {
    let symbol = symbol.trim();
    let exchange = exchange.trim();
    if symbol.is_empty() || exchange.is_empty() {
        return Err(RithmicError::Instrument(
            "Rithmic instrument symbol and exchange must be non-empty".to_string(),
        ));
    }
    if symbol.contains('.') || exchange.contains('.') {
        return Err(RithmicError::Instrument(format!(
            "Rithmic instrument components cannot contain '.': symbol={symbol:?}, exchange={exchange:?}"
        )));
    }

    let canonical = format!(
        "{}.{}",
        symbol.to_ascii_uppercase(),
        exchange.to_ascii_uppercase()
    );
    let symbol = Symbol::new_checked(&canonical).map_err(|e| {
        RithmicError::Instrument(format!(
            "Invalid Rithmic instrument identity {canonical:?}: {e}"
        ))
    })?;
    Ok(InstrumentId::new(symbol, *RITHMIC_VENUE_ID))
}

fn clean_input(value: &str) -> String {
    value.trim().to_ascii_uppercase()
}

fn reference_year_two_digit() -> u16 {
    let year = Utc::now().year().rem_euclid(100);
    match u16::try_from(year) {
        Ok(year) => year,
        Err(e) => {
            log::error!("Failed to convert current UTC year to two digits: {e}");
            0
        }
    }
}

fn extract_year(value: &str, context: &str) -> Result<(u16, usize)> {
    if value.is_empty() {
        return Err(RithmicError::Parse(format!(
            "Symbol '{context}' is missing year designator"
        )));
    }

    if !value.chars().all(|c| c.is_ascii_digit()) {
        return Err(RithmicError::Parse(format!(
            "Invalid year '{value}' in symbol '{context}'"
        )));
    }

    let year = value.parse::<u16>().map_err(|e| {
        RithmicError::Parse(format!("Invalid year '{value}' in symbol '{context}': {e}"))
    })?;

    Ok((year % 100, value.len()))
}

fn resolve_year_two_digit(year: u16, digits: usize, reference_year: u16) -> u16 {
    if digits >= 2 {
        return year % 100;
    }

    let last_digit = year % 10;
    let reference_year = reference_year % 100;
    let reference_decade = reference_year / 10;
    let mut best = last_digit;
    let mut best_distance = u16::MAX;
    let mut best_is_future = false;

    for decade in [
        reference_decade.saturating_sub(1),
        reference_decade,
        reference_decade.saturating_add(1),
    ] {
        let candidate = decade.saturating_mul(10).saturating_add(last_digit);
        let distance = candidate.abs_diff(reference_year);
        let is_future = candidate >= reference_year;

        if distance < best_distance || (distance == best_distance && is_future && !best_is_future) {
            best = candidate;
            best_distance = distance;
            best_is_future = is_future;
        }
    }

    best % 100
}

fn parse_contract_symbol_parts(symbol: &str) -> Result<(String, char, u16, usize)> {
    if !symbol.trim().is_ascii() {
        return Err(RithmicError::Parse(format!(
            "Symbol must contain only ASCII characters: {symbol}"
        )));
    }
    let upper = clean_input(symbol);

    if upper.is_empty() {
        return Err(RithmicError::Parse("symbol cannot be empty".to_string()));
    }

    if !upper.chars().all(|c| c.is_ascii_alphanumeric()) {
        return Err(RithmicError::Parse(format!(
            "Symbol '{symbol}' contains unsupported characters"
        )));
    }
    let cleaned = upper;

    let trailing_digit_count = cleaned
        .chars()
        .rev()
        .take_while(|c| c.is_ascii_digit())
        .count();

    if trailing_digit_count == 0 {
        return Err(RithmicError::Parse(format!(
            "Symbol '{symbol}' is missing year designator"
        )));
    }

    if trailing_digit_count > 2 {
        let invalid_year = cleaned
            .len()
            .checked_sub(trailing_digit_count)
            .and_then(|start| cleaned.get(start..))
            .unwrap_or("<invalid>");
        return Err(RithmicError::Parse(format!(
            "Invalid year '{invalid_year}' in symbol '{symbol}'"
        )));
    }

    let month_idx = cleaned
        .len()
        .checked_sub(trailing_digit_count + 1)
        .ok_or_else(|| RithmicError::Parse(format!("Unable to locate month code in '{symbol}'")))?;

    let root = cleaned.get(..month_idx).ok_or_else(|| {
        RithmicError::Parse(format!("Unable to locate product root in '{symbol}'"))
    })?;
    if root.is_empty() {
        return Err(RithmicError::Parse(format!(
            "Symbol '{symbol}' is missing product root"
        )));
    }

    let month = cleaned
        .as_bytes()
        .get(month_idx)
        .copied()
        .map(char::from)
        .ok_or_else(|| RithmicError::Parse(format!("Unable to locate month code in '{symbol}'")))?;

    if !VALID_MONTH_CODES.contains(&month) {
        return Err(RithmicError::Parse(format!(
            "Unable to locate month code in '{symbol}'"
        )));
    }

    let year = month_idx
        .checked_add(1)
        .and_then(|start| cleaned.get(start..))
        .ok_or_else(|| {
            RithmicError::Parse(format!("Unable to locate year designator in '{symbol}'"))
        })?;
    let (year_two, year_digits) = extract_year(year, symbol)?;
    Ok((root.to_string(), month, year_two, year_digits))
}

fn parse_projectx_symbol_with_year(
    symbol: &str,
    reference_year: u16,
) -> Result<(String, char, u16)> {
    if !symbol.trim().is_ascii() {
        return Err(RithmicError::Parse(format!(
            "ProjectX symbol must contain only ASCII characters: {symbol}"
        )));
    }
    let upper = clean_input(symbol);
    if upper.is_empty() {
        return Err(RithmicError::Parse(
            "projectx symbol cannot be empty".to_string(),
        ));
    }

    let parts: Vec<&str> = upper.split('.').filter(|part| !part.is_empty()).collect();

    if parts.len() >= 5 {
        let root = parts[3];
        let expiry = parts[4];
        let mut chars = expiry.chars();
        let month = chars.next().ok_or_else(|| {
            RithmicError::Parse(format!("ProjectX contract '{symbol}' missing month code"))
        })?;

        if !VALID_MONTH_CODES.contains(&month) {
            return Err(RithmicError::Parse(format!(
                "ProjectX contract '{symbol}' has invalid month designator '{month}'"
            )));
        }

        let (year, digits) = extract_year(&chars.collect::<String>(), symbol)?;
        return Ok((
            root.to_string(),
            month,
            resolve_year_two_digit(year, digits, reference_year),
        ));
    }

    let (root, month, year, digits) = parse_contract_symbol_parts(symbol)?;
    Ok((
        root,
        month,
        resolve_year_two_digit(year, digits, reference_year),
    ))
}

fn format_databento_symbol(root: &str, month: char, year_two_digit: u16) -> String {
    format!("{root}{month}{year_two_digit:02}")
}

fn format_rithmic_symbol(root: &str, month: char, year_two_digit: u16) -> String {
    format!("{root}{month}{}", year_two_digit % 10)
}

/// Parses a futures symbol into (product, expiry) components.
///
/// # Examples
///
/// - `"ESZ4"` → `("ES", "Z4")`
/// - `"MESZ4"` → `("MES", "Z4")`
///
/// # Errors
///
/// Returns [`RithmicError::Parse`] if the symbol is too short or the last two
/// characters are not a valid expiry code (month letter + digit).
pub fn parse_symbol(symbol: &str) -> Result<(&str, &str)> {
    if !symbol.is_ascii() {
        return Err(RithmicError::Parse(format!(
            "Symbol must contain only ASCII characters: {symbol}"
        )));
    }
    if !symbol.chars().all(|c| c.is_ascii_alphanumeric()) {
        return Err(RithmicError::Parse(format!(
            "Symbol '{symbol}' contains unsupported characters"
        )));
    }

    let mut chars = symbol.char_indices().rev();
    let Some((_, year_char)) = chars.next() else {
        return Err(RithmicError::Parse(format!("Invalid symbol: {symbol}")));
    };
    let Some((split_idx, month_char)) = chars.next() else {
        return Err(RithmicError::Parse(format!("Invalid symbol: {symbol}")));
    };
    if split_idx == 0 {
        return Err(RithmicError::Parse(format!("Invalid symbol: {symbol}")));
    }
    let month_char = month_char.to_ascii_uppercase();

    if !VALID_MONTH_CODES.contains(&month_char) || !year_char.is_ascii_digit() {
        return Err(RithmicError::Parse(format!(
            "Invalid expiry in symbol: {symbol}"
        )));
    }
    let product = symbol.get(..split_idx).ok_or_else(|| {
        RithmicError::Parse(format!("Invalid product component in symbol: {symbol}"))
    })?;
    let expiry = symbol.get(split_idx..).ok_or_else(|| {
        RithmicError::Parse(format!("Invalid expiry component in symbol: {symbol}"))
    })?;
    Ok((product, expiry))
}

/// Converts futures month code to month number (F=1, G=2, ..., Z=12).
pub fn month_code_to_number(code: char) -> Result<u32> {
    match code.to_ascii_uppercase() {
        'F' => Ok(1),
        'G' => Ok(2),
        'H' => Ok(3),
        'J' => Ok(4),
        'K' => Ok(5),
        'M' => Ok(6),
        'N' => Ok(7),
        'Q' => Ok(8),
        'U' => Ok(9),
        'V' => Ok(10),
        'X' => Ok(11),
        'Z' => Ok(12),
        _ => Err(RithmicError::Parse(format!("Invalid month code: {code}"))),
    }
}

/// Converts a Databento or canonical two-digit contract symbol into a Rithmic symbol.
pub fn databento_to_rithmic_symbol(symbol: &str) -> Result<String> {
    let (root, month, year_two, _) = parse_contract_symbol_parts(symbol)?;
    Ok(format_rithmic_symbol(&root, month, year_two))
}

/// Converts a ProjectX symbol or contract id into a Rithmic symbol.
pub fn projectx_to_rithmic_symbol(symbol: &str) -> Result<String> {
    let (root, month, year_two) =
        parse_projectx_symbol_with_year(symbol, reference_year_two_digit())?;
    Ok(format_rithmic_symbol(&root, month, year_two))
}

/// Converts a Rithmic symbol into a Databento-style two-digit contract symbol.
pub fn rithmic_to_databento_symbol(symbol: &str) -> Result<String> {
    rithmic_to_databento_symbol_with_year(symbol, reference_year_two_digit())
}

/// Converts a Rithmic symbol into a Databento-style two-digit contract symbol using an explicit
/// reference year to resolve one-digit expiries deterministically.
pub fn rithmic_to_databento_symbol_with_year(symbol: &str, reference_year: u16) -> Result<String> {
    let (root, month, year_two, digits) = parse_contract_symbol_parts(symbol)?;
    let resolved_year = resolve_year_two_digit(year_two, digits, reference_year);
    Ok(format_databento_symbol(&root, month, resolved_year))
}

/// Converts a Rithmic symbol into the canonical ProjectX adapter symbol.
pub fn rithmic_to_projectx_symbol(symbol: &str) -> Result<String> {
    rithmic_to_projectx_symbol_with_year(symbol, reference_year_two_digit())
}

/// Converts a Rithmic symbol into the canonical ProjectX adapter symbol using an explicit
/// reference year to resolve one-digit expiries deterministically.
pub fn rithmic_to_projectx_symbol_with_year(symbol: &str, reference_year: u16) -> Result<String> {
    rithmic_to_databento_symbol_with_year(symbol, reference_year)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[rstest::rstest]
    fn test_parse_symbol() {
        let (product, expiry) = parse_symbol("ESZ4").unwrap();
        assert_eq!(product, "ES");
        assert_eq!(expiry, "Z4");

        let (product, expiry) = parse_symbol("MESZ4").unwrap();
        assert_eq!(product, "MES");
        assert_eq!(expiry, "Z4");

        assert!(parse_symbol("ES").is_err()); // too short
        assert!(parse_symbol("ESA4").is_err()); // 'A' is not a valid month code
        assert!(parse_symbol("ESZ-").is_err()); // '-' is not a digit
    }

    #[rstest::rstest]
    fn test_checked_rithmic_instrument_identity() {
        assert_eq!(
            rithmic_instrument_id("mnqm6", "cme").unwrap(),
            InstrumentId::from("MNQM6.CME.RITHMIC")
        );
        assert!(rithmic_instrument_id("", "CME").is_err());
        assert!(rithmic_instrument_id("MNQM6", "").is_err());
        assert!(rithmic_instrument_id("MNQM6.CME", "CME").is_err());
    }

    #[rstest::rstest]
    fn test_parse_symbol_rejects_invalid_expiry() {
        // Non-month-code letter
        assert!(parse_symbol("ESA4").is_err());
        assert!(parse_symbol("ESB5").is_err());
        // Non-digit year
        assert!(parse_symbol("ESZx").is_err());
        assert!(parse_symbol("ESZ-").is_err());
        // Valid codes still pass
        assert!(parse_symbol("ESZ4").is_ok());
        assert!(parse_symbol("ESH5").is_ok());
        assert!(parse_symbol("MESM4").is_ok());
    }

    #[rstest::rstest]
    fn test_symbol_parsers_reject_unexpected_ascii_punctuation() {
        assert!(parse_symbol("$ES-Z4!").is_err());
        assert!(databento_to_rithmic_symbol("$ES-Z4!").is_err());
        assert!(rithmic_to_databento_symbol("$ES-Z4!").is_err());
    }

    #[rstest::rstest]
    #[case("ES💥")]
    #[case("ESZ４")]
    #[case("ÉSZ4")]
    fn test_parse_symbol_rejects_unicode_without_panicking(#[case] symbol: &str) {
        assert!(parse_symbol(symbol).is_err());
        assert!(databento_to_rithmic_symbol(symbol).is_err());
        assert!(projectx_to_rithmic_symbol(symbol).is_err());
    }

    #[rstest::rstest]
    fn test_month_code_to_number() {
        assert_eq!(month_code_to_number('F').unwrap(), 1);
        assert_eq!(month_code_to_number('Z').unwrap(), 12);
        assert!(month_code_to_number('A').is_err());
    }

    #[rstest::rstest]
    fn test_databento_to_rithmic_symbol() {
        assert_eq!(databento_to_rithmic_symbol("MNQM26").unwrap(), "MNQM6");
        assert_eq!(databento_to_rithmic_symbol("MESZ30").unwrap(), "MESZ0");
    }

    #[rstest::rstest]
    fn test_projectx_to_rithmic_symbol() {
        assert_eq!(
            projectx_to_rithmic_symbol("CON.F.US.MNQ.M26").unwrap(),
            "MNQM6",
        );
        assert_eq!(projectx_to_rithmic_symbol("MNQM26").unwrap(), "MNQM6");
    }

    #[rstest::rstest]
    fn test_rithmic_to_databento_symbol_with_year() {
        assert_eq!(
            rithmic_to_databento_symbol_with_year("MNQM6", 26).unwrap(),
            "MNQM26",
        );
        assert_eq!(
            rithmic_to_databento_symbol_with_year("MNQZ0", 29).unwrap(),
            "MNQZ30",
        );
        assert_eq!(
            rithmic_to_databento_symbol_with_year("MNQZ9", 20).unwrap(),
            "MNQZ19",
        );
    }

    #[rstest::rstest]
    fn test_rithmic_to_projectx_symbol_with_year() {
        assert_eq!(
            rithmic_to_projectx_symbol_with_year("MNQM6", 26).unwrap(),
            "MNQM26",
        );
        assert_eq!(
            rithmic_to_projectx_symbol_with_year("MNQZ0", 29).unwrap(),
            "MNQZ30",
        );
    }
}
