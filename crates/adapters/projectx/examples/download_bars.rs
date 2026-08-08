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

//! Download historical ProjectX bars into a Nautilus catalog for Rust backtests.
//!
//! Run with:
//! `cargo run --example projectx-rust-download-bars -p projectx-nt`
//!
//! Required environment variables:
//! - `PROJECTX_USERNAME`
//! - `PROJECTX_API_KEY`
//! - `NAUTILUS_PATH`
//!
//! Optional environment variables:
//! - `PROJECTX_INSTRUMENT_ID` (for example `MNQM26.PROJECTX`)
//! - `PROJECTX_PRODUCT_ROOT` (used when `PROJECTX_INSTRUMENT_ID` is unset, default `MES`)
//! - `PROJECTX_BAR_SPEC` (default `1-MINUTE-LAST`)
//! - `PROJECTX_REQUEST_START`
//! - `PROJECTX_REQUEST_END`
//! - `PROJECTX_REQUEST_LIMIT` (default `500`)
//! - `PROJECTX_MARKET_DATA_LIVE` (`true` or `false`, default `false`)
//! - `PROJECTX_ALLOW_LIVE_HISTORY_FALLBACK` (`true` or `false`, default `false`)

#[path = "support/mod.rs"]
mod support;

use std::num::NonZeroUsize;

use chrono::{Datelike, Duration, Utc};

use crate::support::{
    common::{env_bool, env_string, example_root_from_env, load_env, resolve_contract_from_env},
    downloader::download_bars_to_catalog,
};

fn default_request_window() -> (chrono::DateTime<Utc>, chrono::DateTime<Utc>) {
    let now = Utc::now();
    let current_week_start = (now - Duration::days(now.weekday().num_days_from_monday() as i64))
        .date_naive()
        .and_hms_opt(0, 0, 0)
        .expect("valid week start")
        .and_utc();
    let start = current_week_start - Duration::days(7);
    let end = start + Duration::days(4) + Duration::hours(23) + Duration::minutes(59);
    (start, end)
}

fn parse_time_env(
    key: &str,
    default: chrono::DateTime<Utc>,
) -> anyhow::Result<chrono::DateTime<Utc>> {
    let value = match std::env::var(key) {
        Ok(value) => value,
        Err(_) => return Ok(default),
    };
    Ok(chrono::DateTime::parse_from_rfc3339(&value)?.with_timezone(&Utc))
}

fn parse_limit() -> anyhow::Result<NonZeroUsize> {
    let raw = env_string("PROJECTX_REQUEST_LIMIT", "500");
    let parsed = raw.parse::<usize>()?;
    NonZeroUsize::new(parsed.max(1))
        .ok_or_else(|| anyhow::anyhow!("PROJECTX_REQUEST_LIMIT must be positive"))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    load_env();

    let example_root = example_root_from_env("projectx")?;
    let catalog_path = example_root.join("catalog");
    let market_data_live = env_bool("PROJECTX_MARKET_DATA_LIVE", false);
    let allow_live_history_fallback = env_bool("PROJECTX_ALLOW_LIVE_HISTORY_FALLBACK", false);
    let bar_spec = env_string("PROJECTX_BAR_SPEC", "1-MINUTE-LAST");
    let (default_start, default_end) = default_request_window();
    let request_start = parse_time_env("PROJECTX_REQUEST_START", default_start)?;
    let request_end = parse_time_env("PROJECTX_REQUEST_END", default_end)?;
    let request_limit = parse_limit()?;
    let contract = resolve_contract_from_env(market_data_live).await?;

    let result = download_bars_to_catalog(
        &catalog_path,
        contract,
        &bar_spec,
        request_start,
        request_end,
        request_limit,
        market_data_live,
        allow_live_history_fallback,
    )
    .await?;

    println!("Catalog path: {}", result.catalog_path.display());
    println!("Instrument ID: {}", result.instrument_id);
    println!("Bar type: {}", result.bar_type);
    println!("Market data live: {market_data_live}");
    println!("Allow live history fallback: {allow_live_history_fallback}");
    println!(
        "Download window: {} -> {}",
        result.start_time, result.end_time
    );
    println!("Instruments stored: {}", result.instrument_count);
    println!("Bars stored: {}", result.bar_count);
    Ok(())
}
