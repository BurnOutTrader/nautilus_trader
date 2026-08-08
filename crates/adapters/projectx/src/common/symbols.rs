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

use chrono::{Datelike, Utc};

const MONTH_CODES: [char; 12] = ['F', 'G', 'H', 'J', 'K', 'M', 'N', 'Q', 'U', 'V', 'X', 'Z'];

fn clean_input(value: &str) -> String {
    value.trim().to_ascii_uppercase()
}

fn reference_year_two_digit() -> u16 {
    let year = Utc::now().year().rem_euclid(100);
    u16::try_from(year).expect("UTC year modulo 100 should fit in u16")
}

fn extract_year(value: &str, context: &str) -> anyhow::Result<(u16, usize)> {
    if value.is_empty() {
        anyhow::bail!("Symbol '{context}' is missing year designator");
    }

    if !value.chars().all(|c| c.is_ascii_digit()) {
        anyhow::bail!("Invalid year '{value}' in symbol '{context}'");
    }

    Ok((value.parse::<u16>()? % 100, value.len()))
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

fn parse_contract_symbol_parts(raw_symbol: &str) -> anyhow::Result<(String, char, u16, usize)> {
    let upper = clean_input(raw_symbol);

    if upper.is_empty() {
        anyhow::bail!("raw_symbol cannot be empty");
    }

    if upper.contains(".C.") {
        anyhow::bail!(
            "Continuous contracts ('{raw_symbol}') are not supported for ProjectX routing"
        );
    }

    let cleaned: String = upper
        .chars()
        .filter(|c| matches!(c, 'A'..='Z' | '0'..='9'))
        .collect();

    if cleaned.is_empty() {
        anyhow::bail!("Symbol '{raw_symbol}' did not contain alphanumeric characters");
    }

    let trailing_digit_count = cleaned
        .chars()
        .rev()
        .take_while(|c| c.is_ascii_digit())
        .count();

    if trailing_digit_count == 0 {
        anyhow::bail!("Symbol '{raw_symbol}' is missing year designator");
    }

    if trailing_digit_count > 2 {
        anyhow::bail!(
            "Invalid year '{}' in symbol '{raw_symbol}'",
            &cleaned[cleaned.len() - trailing_digit_count..]
        );
    }

    let month_idx = cleaned
        .len()
        .checked_sub(trailing_digit_count + 1)
        .ok_or_else(|| anyhow::anyhow!("Unable to locate month code in '{raw_symbol}'"))?;

    let root = &cleaned[..month_idx];

    if root.is_empty() {
        anyhow::bail!("Symbol '{raw_symbol}' is missing product root");
    }

    let month = cleaned
        .chars()
        .nth(month_idx)
        .expect("month index should be in bounds");

    if !MONTH_CODES.contains(&month) {
        anyhow::bail!("Unable to locate month code in '{raw_symbol}'");
    }

    let (year_two, year_digits) = extract_year(&cleaned[month_idx + 1..], raw_symbol)?;
    Ok((root.to_string(), month, year_two, year_digits))
}

fn parse_projectx_symbol_with_year(
    raw_symbol: &str,
    reference_year: u16,
) -> anyhow::Result<(String, char, u16)> {
    let upper = clean_input(raw_symbol);

    if upper.is_empty() {
        anyhow::bail!("projectx symbol cannot be empty");
    }

    let parts: Vec<&str> = upper.split('.').filter(|part| !part.is_empty()).collect();

    if parts.len() >= 5 {
        let root = parts[3];
        let expiry = parts[4];
        let mut chars = expiry.chars();
        let month = chars.next().ok_or_else(|| {
            anyhow::anyhow!("ProjectX contract '{raw_symbol}' missing month code")
        })?;

        if !MONTH_CODES.contains(&month) {
            anyhow::bail!(
                "ProjectX contract '{raw_symbol}' has invalid month designator '{month}'"
            );
        }

        let (year, digits) = extract_year(&chars.collect::<String>(), raw_symbol)?;
        return Ok((
            root.to_string(),
            month,
            resolve_year_two_digit(year, digits, reference_year),
        ));
    }

    let (root, month, year, digits) = parse_contract_symbol_parts(raw_symbol)?;
    Ok((
        root,
        month,
        resolve_year_two_digit(year, digits, reference_year),
    ))
}

pub fn parse_databento_symbol(raw_symbol: &str) -> anyhow::Result<(String, char, u16)> {
    let (root, month, year_two, _) = parse_contract_symbol_parts(raw_symbol)?;
    Ok((root, month, year_two))
}

#[must_use]
pub fn databento_root(raw_symbol: &str) -> String {
    if let Ok((root, _, _)) = parse_databento_symbol(raw_symbol) {
        return root;
    }

    let upper = clean_input(raw_symbol);

    for (idx, ch) in upper.char_indices() {
        if MONTH_CODES.contains(&ch) {
            return upper[..idx].to_string();
        }
    }

    upper
}

#[must_use]
pub fn format_databento_symbol(root: &str, month: char, year_two_digit: u16) -> String {
    format!("{root}{month}{year_two_digit:02}")
}

#[must_use]
pub fn format_rithmic_symbol(root: &str, month: char, year_two_digit: u16) -> String {
    format!("{root}{month}{}", year_two_digit % 10)
}

pub fn databento_to_projectx_contract_id(raw_symbol: &str) -> anyhow::Result<String> {
    let (root, month, year_two) = parse_databento_symbol(raw_symbol)?;
    Ok(format!("CON.F.US.{root}.{month}{year_two:02}"))
}

pub fn databento_to_projectx_symbol(raw_symbol: &str) -> anyhow::Result<String> {
    databento_to_projectx_contract_id(raw_symbol)
}

pub fn databento_to_projectx_adapter_symbol(raw_symbol: &str) -> anyhow::Result<String> {
    let (root, month, year_two) = parse_databento_symbol(raw_symbol)?;
    Ok(format_databento_symbol(&root, month, year_two))
}

#[must_use]
pub fn projectx_contract_root(contract_id: &str) -> String {
    let upper = clean_input(contract_id);
    let parts: Vec<&str> = upper.split('.').filter(|part| !part.is_empty()).collect();

    if parts.len() >= 4 {
        parts[3].to_string()
    } else {
        upper
    }
}

pub fn projectx_to_databento_symbol(contract_id: &str) -> anyhow::Result<String> {
    projectx_to_databento_symbol_with_year(contract_id, reference_year_two_digit())
}

pub fn projectx_to_databento_symbol_with_year(
    contract_id: &str,
    reference_year: u16,
) -> anyhow::Result<String> {
    let (root, month, year_two) = parse_projectx_symbol_with_year(contract_id, reference_year)?;
    Ok(format_databento_symbol(&root, month, year_two))
}

pub fn projectx_to_rithmic_symbol(projectx_symbol: &str) -> anyhow::Result<String> {
    let (root, month, year_two) =
        parse_projectx_symbol_with_year(projectx_symbol, reference_year_two_digit())?;
    Ok(format_rithmic_symbol(&root, month, year_two))
}

pub fn rithmic_to_projectx_symbol(rithmic_symbol: &str) -> anyhow::Result<String> {
    rithmic_to_projectx_symbol_with_year(rithmic_symbol, reference_year_two_digit())
}

pub fn rithmic_to_projectx_symbol_with_year(
    rithmic_symbol: &str,
    reference_year: u16,
) -> anyhow::Result<String> {
    let (root, month, year_two, digits) = parse_contract_symbol_parts(rithmic_symbol)?;
    let resolved_year = resolve_year_two_digit(year_two, digits, reference_year);
    Ok(format_databento_symbol(&root, month, resolved_year))
}

#[cfg(test)]
mod tests {
    use super::{
        databento_to_projectx_adapter_symbol, databento_to_projectx_contract_id,
        databento_to_projectx_symbol, parse_databento_symbol, projectx_to_databento_symbol,
        projectx_to_databento_symbol_with_year, projectx_to_rithmic_symbol,
        rithmic_to_projectx_symbol_with_year,
    };

    #[rstest::rstest]
    fn parse_symbol_uses_expiry_suffix() {
        let cases = [
            ("MESM26", ("MES".to_string(), 'M', 26)),
            ("YMM26", ("YM".to_string(), 'M', 26)),
            ("EU6M26", ("EU6".to_string(), 'M', 26)),
            ("TNAM26", ("TNA".to_string(), 'M', 26)),
        ];

        for (raw_symbol, expected) in cases {
            assert_eq!(parse_databento_symbol(raw_symbol).unwrap(), expected);
        }
    }

    #[rstest::rstest]
    fn translate_databento_to_projectx_contract_id() {
        assert_eq!(
            databento_to_projectx_symbol("MESM26").unwrap(),
            "CON.F.US.MES.M26",
        );
        assert_eq!(
            databento_to_projectx_contract_id("MESM26").unwrap(),
            "CON.F.US.MES.M26",
        );
    }

    #[rstest::rstest]
    fn translate_projectx_contract_id_to_databento_symbol() {
        assert_eq!(
            projectx_to_databento_symbol("CON.F.US.MES.M26").unwrap(),
            "MESM26",
        );
    }

    #[rstest::rstest]
    fn translate_databento_to_projectx_adapter_symbol() {
        assert_eq!(
            databento_to_projectx_adapter_symbol("MNQM26").unwrap(),
            "MNQM26",
        );
    }

    #[rstest::rstest]
    fn translate_projectx_symbol_to_rithmic_symbol() {
        assert_eq!(
            projectx_to_rithmic_symbol("CON.F.US.MNQ.M26").unwrap(),
            "MNQM6",
        );
        assert_eq!(projectx_to_rithmic_symbol("MNQM26").unwrap(), "MNQM6");
    }

    #[rstest::rstest]
    fn translate_rithmic_symbol_to_projectx_symbol() {
        assert_eq!(
            rithmic_to_projectx_symbol_with_year("MNQM6", 26).unwrap(),
            "MNQM26",
        );
        assert_eq!(
            rithmic_to_projectx_symbol_with_year("MNQZ0", 29).unwrap(),
            "MNQZ30",
        );
    }

    #[rstest::rstest]
    fn translate_projectx_vendor_alias_to_databento_symbol_uses_reference_year() {
        assert_eq!(
            projectx_to_databento_symbol_with_year("MNQM6", 26).unwrap(),
            "MNQM26",
        );
        assert_eq!(
            projectx_to_databento_symbol_with_year("MNQZ0", 29).unwrap(),
            "MNQZ30",
        );
    }
}
