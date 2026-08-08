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

//! High-level ProjectX Rust backtest example using catalog bars and a custom bar strategy.
//!
//! Run with:
//! `cargo run --example projectx-rust-backtest-ema-cross -p projectx-nt`
//!
//! Required environment variables:
//! - `NAUTILUS_PATH`
//!
//! Optional environment variables:
//! - `PROJECTX_INSTRUMENT_ID`
//! - `PROJECTX_BAR_SPEC` (default `1-MINUTE-LAST`)
//! - `PROJECTX_TRADE_SIZE` (default `1`)
//! - `PROJECTX_FAST_EMA` (default `10`)
//! - `PROJECTX_SLOW_EMA` (default `20`)

#[path = "support/mod.rs"]
mod support;

use nautilus_backtest::{
    config::{BacktestDataConfig, BacktestRunConfig, BacktestVenueConfig, NautilusDataType},
    node::BacktestNode,
};
use nautilus_model::{
    enums::{AccountType, BookType, OmsType},
    identifiers::StrategyId,
    types::{Currency, Quantity},
};
use nautilus_persistence::backend::catalog::ParquetDataCatalog;
use ustr::Ustr;

use crate::support::{
    bar_ema_cross::{ProjectXBarEmaCrossConfig, ProjectXBarEmaCrossStrategy},
    common::{
        env_string, example_root_from_env, format_utc_nanos, load_env,
        resolve_catalog_backtest_window, resolve_catalog_instrument_id,
    },
    downloader::build_external_bar_type,
};

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(default)
}

fn env_quantity(key: &str, default: &str) -> Quantity {
    Quantity::from(env_string(key, default).as_str())
}

fn main() -> anyhow::Result<()> {
    load_env();

    let example_root = example_root_from_env("projectx")?;
    let catalog_path = example_root.join("catalog");
    let catalog_path_str = catalog_path
        .to_str()
        .ok_or_else(|| anyhow::anyhow!("Invalid catalog path"))?
        .to_string();
    let bar_spec = env_string("PROJECTX_BAR_SPEC", "1-MINUTE-LAST");
    let fast_ema = env_usize("PROJECTX_FAST_EMA", 10);
    let slow_ema = env_usize("PROJECTX_SLOW_EMA", 20);
    let trade_size = env_quantity("PROJECTX_TRADE_SIZE", "1");

    let mut catalog = ParquetDataCatalog::new(&catalog_path, None, None, None, None);
    let instrument_id = resolve_catalog_instrument_id(&catalog)?;
    let bar_type = build_external_bar_type(instrument_id, &bar_spec)?;
    let (start_time, end_time) =
        resolve_catalog_backtest_window(&mut catalog, instrument_id, bar_type)?;

    let venue_config = BacktestVenueConfig::builder()
        .name(Ustr::from("PROJECTX"))
        .oms_type(OmsType::Netting)
        .account_type(AccountType::Cash)
        .book_type(BookType::L1_MBP)
        .starting_balances(vec!["100_000 USD".to_string()])
        .base_currency(Currency::USD())
        .build()?;

    let data_config = BacktestDataConfig::builder()
        .data_type(NautilusDataType::Bar)
        .catalog_path(catalog_path_str)
        .instrument_id(instrument_id)
        .bar_spec(bar_type.spec())
        .start_time(start_time)
        .end_time(end_time)
        .build()?;

    let run_id = "projectx-rust-backtest-ema-cross".to_string();
    let run_config = BacktestRunConfig::builder()
        .id(run_id.clone())
        .venues(vec![venue_config])
        .data(vec![data_config])
        .build()?;

    let strategy = ProjectXBarEmaCrossStrategy::new(ProjectXBarEmaCrossConfig {
        strategy_id: StrategyId::from("PROJECTX-EMA-001"),
        instrument_id,
        live_bar_type: bar_type,
        history_bar_type: bar_type,
        trade_size,
        fast_period: fast_ema,
        slow_period: slow_ema,
        warmup_minutes: 0,
        request_bars_on_start: false,
        unsubscribe_on_stop: false,
        cleanup_on_stop: false,
    });

    let mut node = BacktestNode::new(vec![run_config])?;
    node.build()?;
    node.get_engine_mut(&run_id)
        .ok_or_else(|| anyhow::anyhow!("Missing backtest engine"))?
        .add_strategy(strategy)?;

    let results = node.run()?;

    println!("Catalog path: {}", catalog_path.display());
    println!("Instrument ID: {instrument_id}");
    println!("Bar type: {bar_type}");
    println!(
        "Backtest range: {} -> {}",
        format_utc_nanos(start_time)?,
        format_utc_nanos(end_time)?,
    );
    println!("{results:#?}");
    Ok(())
}
