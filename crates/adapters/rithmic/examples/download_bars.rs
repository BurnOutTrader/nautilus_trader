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

//! Download historical Rithmic bars for one concrete contract into a Nautilus
//! catalog for Rust backtests.
//!
//! Use `rithmic-write-instruments-catalog` first if you need to pre-populate
//! supported current-contract instrument definitions before requesting bars.
//! The catalog writer only covers the adapter's hard-coded Rithmic futures
//! roots and writes the current contract for each root.
//!
//! Run with:
//! `cargo run --example rithmic-rust-download-bars --package rithmic-nt`
//!
//! Required environment variables:
//! - `RITHMIC_USERNAME`
//! - `RITHMIC_PASSWORD`
//! - `RITHMIC_SYSTEM_NAME`
//! - `RITHMIC_ACCOUNT_ID`
//! - `RITHMIC_APP_NAME`
//! - `NAUTILUS_PATH`
//!
//! Optional environment variables:
//! - `RITHMIC_PROFILE`
//! - `RITHMIC_INSTRUMENT_ID`
//! - `RITHMIC_PRODUCT_CODE` (default `MNQ`)
//! - `RITHMIC_EXCHANGE` (default `CME`)
//! - `RITHMIC_BAR_SPEC` (default `1-MINUTE-LAST`)
//! - `RITHMIC_REQUEST_START`
//! - `RITHMIC_REQUEST_END`

#[path = "support/mod.rs"]
mod support;

use chrono::{Datelike, Duration, Utc};
use rithmic_nt::RithmicInstrumentProvider;

use crate::support::{
    common::{
        connect_history_gateway, disconnect_history_gateway, env_string, example_root_from_env,
        load_env, profile_from_env, resolve_instrument_from_env,
    },
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

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    load_env();

    let example_root = example_root_from_env("rithmic")?;
    let catalog_path = example_root.join("catalog");
    let profile = profile_from_env();
    let bar_spec = env_string("RITHMIC_BAR_SPEC", "1-MINUTE-LAST");
    let (default_start, default_end) = default_request_window();
    let request_start = parse_time_env("RITHMIC_REQUEST_START", default_start)?;
    let request_end = parse_time_env("RITHMIC_REQUEST_END", default_end)?;

    let gateway = connect_history_gateway(profile.as_deref()).await?;
    let provider = RithmicInstrumentProvider::new(std::sync::Arc::clone(&gateway));
    let instrument = resolve_instrument_from_env(&provider).await?;
    drop(provider);

    let result = download_bars_to_catalog(
        &gateway,
        &catalog_path,
        &instrument,
        &bar_spec,
        request_start,
        request_end,
    )
    .await?;

    disconnect_history_gateway(gateway).await?;

    println!("Catalog path: {}", result.catalog_path.display());
    println!("Instrument ID: {}", result.instrument_id);
    println!("Bar type: {}", result.bar_type);
    println!(
        "Download window: {} -> {}",
        result.start_time, result.end_time
    );
    println!("Instruments stored: {}", result.instrument_count);
    println!("Bars stored: {}", result.bar_count);
    Ok(())
}
