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

//! Self-contained pure Rust ProjectX backtest example.
//!
//! This example writes a synthetic ProjectX futures instrument plus external bars to a Nautilus
//! catalog under the ProjectX example root, then runs a Rust strategy through `BacktestNode`.
//!
//! Run with:
//! `cargo run --example projectx-rust-backtest-bars -p projectx-nt`
//!
//! Optional environment variables:
//! - `PROJECTX_BAR_SPEC` (default `1-MINUTE-LAST`)
//! - `PROJECTX_BACKTEST_BARS` (default `24`)
//! - `NAUTILUS_PATH` ProjectX example root (for example `/tmp/nautilus-data/examples/projectx`)

use std::{
    path::PathBuf,
    str::FromStr,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use nautilus_backtest::{
    config::{BacktestDataConfig, BacktestRunConfig, BacktestVenueConfig, NautilusDataType},
    node::BacktestNode,
};
use nautilus_common::actor::DataActor;
use nautilus_core::UnixNanos;
use nautilus_model::{
    data::{Bar, BarSpecification, BarType},
    enums::{AccountType, AggregationSource, BarAggregation, BookType, OmsType, PriceType},
    identifiers::{InstrumentId, StrategyId},
    instruments::{Instrument, InstrumentAny},
    types::{Price, Quantity},
};
use nautilus_persistence::backend::catalog::ParquetDataCatalog;
use nautilus_trading::{StrategyConfig, StrategyCore, nautilus_strategy};
use projectx_client::Contract;
use projectx_nt::factories::projectx_contract_to_instrument;
use ustr::Ustr;

fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(default)
}

fn parse_bar_spec_env() -> anyhow::Result<BarSpecification> {
    let raw = std::env::var("PROJECTX_BAR_SPEC").unwrap_or_else(|_| "1-MINUTE-LAST".to_string());
    let parts: Vec<_> = raw.split('-').collect();

    if parts.len() != 3 {
        anyhow::bail!("Invalid PROJECTX_BAR_SPEC '{raw}', expected STEP-AGGREGATION-PRICE_TYPE");
    }

    let step = parts[0]
        .parse::<usize>()
        .map_err(|_| anyhow::anyhow!("Invalid bar step in PROJECTX_BAR_SPEC: {}", parts[0]))?;
    let aggregation = BarAggregation::from_str(parts[1])?;
    let price_type = PriceType::from_str(parts[2])?;
    Ok(BarSpecification::new(step, aggregation, price_type))
}

fn example_root_from_env() -> anyhow::Result<PathBuf> {
    let path = std::env::var("NAUTILUS_PATH").map_err(|_| {
        anyhow::anyhow!(
            "Set NAUTILUS_PATH to the ProjectX example root, for example /tmp/nautilus-data/examples/projectx"
        )
    })?;
    Ok(PathBuf::from(path))
}

fn sample_projectx_instrument() -> anyhow::Result<InstrumentAny> {
    let contract: Contract = serde_json::from_value(serde_json::json!({
        "id": "CON.F.US.MNQ.M26",
        "name": "MNQM26",
        "description": "Micro E-mini Nasdaq-100",
        "tickSize": 0.25,
        "tickValue": 0.50,
        "activeContract": true,
        "symbolId": "MNQM26",
    }))
    .expect("contract");
    projectx_contract_to_instrument(&contract)
}

fn synthetic_bars(bar_type: BarType, count: usize) -> Vec<Bar> {
    let base_ts = 1_775_304_000_000_000_000_u64; // 2026-04-01T00:00:00Z
    let interval_ns = 60_000_000_000_u64;

    (0..count)
        .map(|index| {
            let open = 20_000.0 + index as f64;
            let close = open + 0.5;
            let high = close + 0.25;
            let low = open - 0.25;
            let ts = UnixNanos::from(base_ts + index as u64 * interval_ns);
            Bar::new(
                bar_type,
                Price::from(format!("{open:.2}").as_str()),
                Price::from(format!("{high:.2}").as_str()),
                Price::from(format!("{low:.2}").as_str()),
                Price::from(format!("{close:.2}").as_str()),
                Quantity::from("10"),
                ts,
                ts,
            )
        })
        .collect()
}

#[derive(Debug)]
struct ProjectXBarStrategy {
    core: StrategyCore,
    bar_type: BarType,
    seen_bars: Arc<AtomicUsize>,
}

impl ProjectXBarStrategy {
    fn new(bar_type: BarType, seen_bars: Arc<AtomicUsize>) -> Self {
        let config = StrategyConfig {
            strategy_id: Some(StrategyId::from("PROJECTX-BARS-001")),
            order_id_tag: Some("RUST".to_string()),
            ..Default::default()
        };
        Self {
            core: StrategyCore::new(config),
            bar_type,
            seen_bars,
        }
    }
}

nautilus_strategy!(ProjectXBarStrategy);

impl DataActor for ProjectXBarStrategy {
    fn on_start(&mut self) -> anyhow::Result<()> {
        self.subscribe_bars(self.bar_type, None, None);
        Ok(())
    }

    fn on_bar(&mut self, bar: &Bar) -> anyhow::Result<()> {
        let count = self.seen_bars.fetch_add(1, Ordering::Relaxed) + 1;

        if count <= 3 {
            println!("Bar #{count}: {bar:?}");
        }
        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    let instrument = sample_projectx_instrument()?;
    let instrument_id: InstrumentId = instrument.id();
    let bar_spec = parse_bar_spec_env()?;
    let bar_type = BarType::new(instrument_id, bar_spec, AggregationSource::External);
    let bars = synthetic_bars(bar_type, env_u64("PROJECTX_BACKTEST_BARS", 24) as usize);

    let example_root = example_root_from_env()?;
    let catalog_root = example_root.join("catalog");
    std::fs::create_dir_all(&catalog_root)?;
    let catalog_path = catalog_root
        .to_str()
        .expect("valid catalog path")
        .to_string();
    let catalog = ParquetDataCatalog::new(&catalog_root, None, None, None, None);
    catalog.write_instruments(vec![instrument])?;
    catalog.write_to_parquet(&bars, None, None, None)?;

    let venue_config = BacktestVenueConfig::builder()
        .name(Ustr::from("PROJECTX"))
        .oms_type(OmsType::Netting)
        .account_type(AccountType::Margin)
        .book_type(BookType::L1_MBP)
        .starting_balances(vec!["100_000 USD".to_string()])
        .build()?;

    let data_config = BacktestDataConfig::builder()
        .data_type(NautilusDataType::Bar)
        .catalog_path(catalog_path.clone())
        .instrument_id(instrument_id)
        .bar_spec(bar_spec)
        .build()?;

    let run_id = "projectx-rust-backtest".to_string();
    let run_config = BacktestRunConfig::builder()
        .id(run_id.clone())
        .venues(vec![venue_config])
        .data(vec![data_config])
        .build()?;

    let seen_bars = Arc::new(AtomicUsize::new(0));
    let mut node = BacktestNode::new(vec![run_config])?;
    node.build()?;
    node.get_engine_mut(&run_id)
        .expect("backtest engine should exist")
        .add_strategy(ProjectXBarStrategy::new(bar_type, Arc::clone(&seen_bars)))?;
    node.run()?;

    println!(
        "Processed {} ProjectX bars from synthetic catalog at {}",
        seen_bars.load(Ordering::Relaxed),
        catalog_path,
    );
    Ok(())
}
