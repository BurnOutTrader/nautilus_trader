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

//! Parsing helpers for Rithmic messages.
//!
//! This module provides utilities for parsing Rithmic protocol buffer
//! messages into Nautilus domain types.

use ahash::RandomState;
use rust_decimal::{
    Decimal,
    prelude::{FromPrimitive, ToPrimitive},
};

use crate::error::{Result, RithmicError};

/// Parses a price string from Rithmic to f64.
pub fn parse_price(price_str: &str) -> Result<f64> {
    let price = price_str
        .parse::<f64>()
        .map_err(|e| RithmicError::Parse(format!("Invalid price '{price_str}': {e}")))?;
    if !price.is_finite() {
        return Err(RithmicError::Parse(format!(
            "Price must be finite: '{price_str}'"
        )));
    }
    Ok(price)
}

/// Parses a quantity string from Rithmic to f64.
pub fn parse_quantity(qty_str: &str) -> Result<f64> {
    let quantity = qty_str
        .parse::<f64>()
        .map_err(|e| RithmicError::Parse(format!("Invalid quantity '{qty_str}': {e}")))?;
    if !quantity.is_finite() || quantity <= 0.0 {
        return Err(RithmicError::Parse(format!(
            "Quantity must be finite and positive: '{qty_str}'"
        )));
    }
    Ok(quantity)
}

/// Parses a Rithmic timestamp to Unix nanoseconds.
///
/// Rithmic timestamps are typically in seconds with fractional part.
pub fn parse_timestamp_nanos(secs: f64) -> Result<u64> {
    if !secs.is_finite() || secs < 0.0 {
        return Err(RithmicError::Parse(format!(
            "Timestamp seconds must be finite and non-negative: {secs}"
        )));
    }
    Decimal::from_f64(secs)
        .and_then(|value| value.checked_mul(Decimal::from(1_000_000_000_u64)))
        .and_then(|value| value.round().to_u64())
        .ok_or_else(|| {
            RithmicError::Parse(format!(
                "Timestamp seconds exceed the Unix nanosecond range: {secs}"
            ))
        })
}

/// Parses Unix timestamp in seconds to nanoseconds.
pub fn secs_to_nanos(secs: i64) -> Result<u64> {
    u64::try_from(secs)
        .ok()
        .and_then(|value| value.checked_mul(1_000_000_000))
        .ok_or_else(|| RithmicError::Parse(format!("Invalid Unix timestamp seconds: {secs}")))
}

/// Parses Unix timestamp in milliseconds to nanoseconds.
pub fn millis_to_nanos(millis: i64) -> Result<u64> {
    u64::try_from(millis)
        .ok()
        .and_then(|value| value.checked_mul(1_000_000))
        .ok_or_else(|| {
            RithmicError::Parse(format!("Invalid Unix timestamp milliseconds: {millis}"))
        })
}

/// Parses a tick size from display string (e.g., "0.25" -> 0.25).
pub fn parse_tick_size(tick_str: &str) -> Result<f64> {
    let tick_size = tick_str
        .parse::<f64>()
        .map_err(|e| RithmicError::Parse(format!("Invalid tick size '{tick_str}': {e}")))?;
    if !tick_size.is_finite() || tick_size <= 0.0 {
        return Err(RithmicError::Parse(format!(
            "Tick size must be finite and positive: '{tick_str}'"
        )));
    }
    Ok(tick_size)
}

/// Calculates price precision from tick size.
///
/// Example: tick_size=0.25 -> precision=2
pub fn tick_size_to_precision(tick_size: f64) -> Result<u8> {
    if !tick_size.is_finite() || tick_size <= 0.0 {
        return Err(RithmicError::Parse(format!(
            "Tick size must be finite and positive: {tick_size}"
        )));
    }
    if tick_size >= 1.0 {
        return Ok(0);
    }

    let tick_str = format!("{tick_size}");

    if let Some(dot_pos) = tick_str.find('.') {
        let decimal_part = &tick_str[dot_pos + 1..];
        // Count significant decimal places
        u8::try_from(decimal_part.trim_end_matches('0').len()).map_err(|_| {
            RithmicError::Parse(format!("Tick size precision exceeds u8 range: {tick_size}"))
        })
    } else {
        Ok(0)
    }
}

/// Normalizes a symbol by removing exchange prefix if present.
///
/// Example: "CME:ES" -> "ES"
pub fn normalize_symbol(symbol: &str) -> &str {
    if let Some(colon_pos) = symbol.find(':') {
        &symbol[colon_pos + 1..]
    } else {
        symbol
    }
}

/// Extracts exchange from a qualified symbol.
///
/// Example: "CME:ES" -> Some("CME")
pub fn extract_exchange(symbol: &str) -> Option<&str> {
    symbol.find(':').map(|pos| &symbol[..pos])
}

/// Resolves a stable Nautilus order ID for Rithmic depth-by-order rows.
///
/// Rithmic exposes both `exchange_order_id` and `depth_order_priority`. The latter is queue
/// priority metadata, not a stable unique order identity, so we prefer the exchange order ID when
/// it is available and only fall back to the priority value when it is absent.
pub fn rithmic_depth_order_id(exchange_order_id: Option<&str>, depth_order_priority: u64) -> u64 {
    exchange_order_id
        .filter(|value| !value.is_empty())
        .map_or(depth_order_priority, |value| {
            RandomState::with_seeds(0, 0, 0, 0).hash_one(value)
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[rstest::rstest]
    fn test_parse_price() {
        assert_eq!(parse_price("1234.50").unwrap(), 1234.50);
        assert!(parse_price("invalid").is_err());
        assert!(parse_price("NaN").is_err());
        assert!(parse_price("inf").is_err());
    }

    #[rstest::rstest]
    fn test_parse_quantity_and_tick_size_require_finite_positive_values() {
        assert_eq!(parse_quantity("2").unwrap(), 2.0);
        assert_eq!(parse_tick_size("0.25").unwrap(), 0.25);
        for value in ["0", "-1", "NaN", "inf"] {
            assert!(parse_quantity(value).is_err());
            assert!(parse_tick_size(value).is_err());
        }
    }

    #[rstest::rstest]
    fn test_parse_timestamp() {
        let timestamp = 1_234_567_890.123_456_7;
        let nanos = parse_timestamp_nanos(timestamp).unwrap();
        // The source is an f64, so allow one relative machine-epsilon at this magnitude.
        let expected = 1234567890123456789_u64;
        let diff = nanos.abs_diff(expected);
        let tolerance = (f64::EPSILON * timestamp * 1_000_000_000.0).ceil() as u64;

        assert!(
            diff <= tolerance,
            "Timestamp diff {diff} exceeds f64 tolerance {tolerance}"
        );
        assert!(parse_timestamp_nanos(f64::NAN).is_err());
        assert!(parse_timestamp_nanos(-1.0).is_err());
        assert!(parse_timestamp_nanos(f64::MAX).is_err());
        assert!(secs_to_nanos(-1).is_err());
        assert!(secs_to_nanos(i64::MAX).is_err());
        assert!(millis_to_nanos(-1).is_err());
        assert!(millis_to_nanos(i64::MAX).is_err());
    }

    #[rstest::rstest]
    fn test_tick_size_to_precision() {
        assert_eq!(tick_size_to_precision(0.25).unwrap(), 2);
        assert_eq!(tick_size_to_precision(0.01).unwrap(), 2);
        assert_eq!(tick_size_to_precision(0.0001).unwrap(), 4);
        assert_eq!(tick_size_to_precision(1.0).unwrap(), 0);
        assert!(tick_size_to_precision(0.0).is_err());
        assert!(tick_size_to_precision(-0.25).is_err());
        assert!(tick_size_to_precision(f64::NAN).is_err());
        assert!(tick_size_to_precision(f64::INFINITY).is_err());
    }

    #[rstest::rstest]
    fn test_normalize_symbol() {
        assert_eq!(normalize_symbol("CME:ES"), "ES");
        assert_eq!(normalize_symbol("ES"), "ES");
    }

    #[rstest::rstest]
    fn test_extract_exchange() {
        assert_eq!(extract_exchange("CME:ES"), Some("CME"));
        assert_eq!(extract_exchange("ES"), None);
    }

    #[rstest::rstest]
    fn test_rithmic_depth_order_id_prefers_exchange_order_id() {
        let order_id = rithmic_depth_order_id(Some("bid-11"), 11);
        assert_ne!(order_id, 11);
        assert_eq!(order_id, rithmic_depth_order_id(Some("bid-11"), 99));
    }

    #[rstest::rstest]
    fn test_rithmic_depth_order_id_falls_back_to_priority() {
        assert_eq!(rithmic_depth_order_id(None, 11), 11);
        assert_eq!(rithmic_depth_order_id(Some(""), 11), 11);
    }
}
