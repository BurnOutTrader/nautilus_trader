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

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use nautilus_backtest::{
    config::{BacktestDataConfig, BacktestRunConfig, BacktestVenueConfig, NautilusDataType},
    node::BacktestNode,
};
use nautilus_common::{actor::DataActor, enums::Environment};
use nautilus_live::node::LiveNode;
use nautilus_model::{
    data::{Bar, BarSpecification, BarType},
    enums::{AccountType, AggregationSource, BarAggregation, BookType, OmsType, PriceType},
    identifiers::{AccountId, ClientId, InstrumentId, StrategyId, TraderId},
    instruments::{Instrument, InstrumentAny},
    types::{Price, Quantity},
};
use nautilus_persistence::backend::catalog::ParquetDataCatalog;
use nautilus_testkit::testers::{ExecTester, ExecTesterConfig};
use nautilus_trading::{StrategyConfig, StrategyCore, nautilus_strategy};
use projectx_nt::{
    ProjectXDataClientConfig, ProjectXDataClientFactory, ProjectXEnvironment,
    ProjectXExecClientConfig, ProjectXExecutionClientFactory,
    factories::projectx_contract_to_instrument,
};
use serde_json::json;
use tempfile::TempDir;
use ustr::Ustr;

fn sample_projectx_instrument() -> anyhow::Result<InstrumentAny> {
    projectx_contract_to_instrument(
        &serde_json::from_value(json!({
            "id": "CON.F.US.MES.M26",
            "name": "MESM26",
            "description": "Micro E-mini S&P 500",
            "tickSize": 0.25,
            "tickValue": 1.25,
            "activeContract": true,
            "symbolId": "MESM26",
        }))
        .expect("contract"),
    )
}

#[rstest::rstest]
fn projectx_catalog_round_trip_keeps_canonical_symbols() -> anyhow::Result<()> {
    let instrument = projectx_contract_to_instrument(
        &serde_json::from_value(json!({
            "id": "CON.F.US.MNQ.M26",
            "name": "MNQM6",
            "description": "Micro Nasdaq",
            "tickSize": 0.25,
            "tickValue": 0.5,
            "activeContract": true,
            "symbolId": "F.US.MNQ",
        }))
        .expect("contract"),
    )?;

    let temp_dir = TempDir::new()?;
    let catalog = ParquetDataCatalog::new(temp_dir.path(), None, None, None, None);
    catalog.write_instruments(vec![instrument])?;

    let loaded = catalog.instruments(Some(&["MNQM26.PROJECTX".to_string()]), None, None)?;
    anyhow::ensure!(
        loaded.len() == 1,
        "expected 1 catalog instrument, found {}",
        loaded.len()
    );
    anyhow::ensure!(
        loaded[0].id().to_string() == "MNQM26.PROJECTX",
        "expected canonical instrument id, found {}",
        loaded[0].id()
    );
    anyhow::ensure!(
        loaded[0].raw_symbol().to_string() == "MNQM26",
        "expected canonical raw symbol, found {}",
        loaded[0].raw_symbol()
    );

    Ok(())
}

fn synthetic_bars(bar_type: BarType, count: usize) -> Vec<Bar> {
    let base_ts = 1_775_304_000_000_000_000_u64; // 2026-04-01T00:00:00Z
    let interval_ns = 60_000_000_000_u64;

    (0..count)
        .map(|index| {
            let open = 5_000.0 + index as f64;
            let close = open + 0.25;
            let high = close + 0.25;
            let low = open - 0.25;
            let ts = nautilus_core::UnixNanos::from(base_ts + index as u64 * interval_ns);
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
struct ProjectXRustDataStrategy {
    core: StrategyCore,
    instrument_id: InstrumentId,
    client_id: Option<ClientId>,
}

impl ProjectXRustDataStrategy {
    fn new(instrument_id: InstrumentId, client_id: Option<ClientId>) -> Self {
        let config = StrategyConfig {
            strategy_id: Some(StrategyId::from("PROJECTX-DATA-RUST-001")),
            order_id_tag: Some("RUST".to_string()),
            ..Default::default()
        };
        Self {
            core: StrategyCore::new(config),
            instrument_id,
            client_id,
        }
    }
}

nautilus_strategy!(ProjectXRustDataStrategy);

impl DataActor for ProjectXRustDataStrategy {
    fn on_start(&mut self) -> anyhow::Result<()> {
        let _ = self.request_instrument(self.instrument_id, None, None, self.client_id, None)?;
        self.subscribe_instrument(self.instrument_id, self.client_id, None);
        self.subscribe_quotes(self.instrument_id, self.client_id, None);
        self.subscribe_trades(self.instrument_id, self.client_id, None);
        Ok(())
    }
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
            strategy_id: Some(StrategyId::from("PROJECTX-BAR-RUST-001")),
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

    fn on_bar(&mut self, _bar: &Bar) -> anyhow::Result<()> {
        self.seen_bars.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

#[rstest::rstest]
fn projectx_live_node_builds_with_pure_rust_data_strategy() -> anyhow::Result<()> {
    let instrument_id = sample_projectx_instrument()?.id();
    let trader_id = TraderId::from("TRADER-001");
    let mut node = LiveNode::builder(trader_id, Environment::Live)?
        .with_name("PROJECTX-RUST-DATA-TEST".to_string())
        .add_data_client(
            None,
            Box::new(ProjectXDataClientFactory::new()),
            Box::new(ProjectXDataClientConfig::new(
                ProjectXEnvironment::TopstepX,
                "test-user",
                "test-key",
            )),
        )?
        .build()?;

    node.add_strategy(ProjectXRustDataStrategy::new(
        instrument_id,
        Some(ClientId::from("PROJECTX")),
    ))?;
    Ok(())
}

#[rstest::rstest]
fn projectx_live_node_builds_with_pure_rust_exec_strategy() -> anyhow::Result<()> {
    let trader_id = TraderId::from("TRADER-001");
    let instrument_id = sample_projectx_instrument()?.id();
    let order_qty = Quantity::from(1);
    let tester = ExecTester::new(
        ExecTesterConfig::builder()
            .base(StrategyConfig {
                strategy_id: Some(StrategyId::from("PROJECTX-EXEC-RUST-001")),
                external_order_claims: Some(vec![instrument_id]),
                ..Default::default()
            })
            .instrument_id(instrument_id)
            .client_id(ClientId::from("PROJECTX"))
            .order_qty(order_qty)
            .open_position_on_start_qty(order_qty.as_decimal())
            .dry_run(true)
            .log_data(false)
            .build()?,
    );

    let mut node = LiveNode::builder(trader_id, Environment::Live)?
        .with_name("PROJECTX-RUST-EXEC-TEST".to_string())
        .add_data_client(
            None,
            Box::new(ProjectXDataClientFactory::new()),
            Box::new(ProjectXDataClientConfig::new(
                ProjectXEnvironment::TopstepX,
                "test-user",
                "test-key",
            )),
        )?
        .add_exec_client(
            None,
            Box::new(ProjectXExecutionClientFactory::new()),
            Box::new(ProjectXExecClientConfig::new(
                trader_id,
                AccountId::from("PRAC-V2-64413-98419885"),
                ProjectXEnvironment::TopstepX,
                "test-user",
                "test-key",
            )),
        )?
        .build()?;

    node.add_strategy(tester)?;
    Ok(())
}

#[rstest::rstest]
#[allow(clippy::panic_in_result_fn)]
fn projectx_backtest_node_runs_pure_rust_bar_strategy() -> anyhow::Result<()> {
    let instrument = sample_projectx_instrument()?;
    let instrument_id = instrument.id();
    let bar_spec = BarSpecification::new(1, BarAggregation::Minute, PriceType::Last);
    let bar_type = BarType::new(instrument_id, bar_spec, AggregationSource::External);
    let bars = synthetic_bars(bar_type, 12);

    let temp_dir = TempDir::new()?;
    let catalog_path = temp_dir
        .path()
        .to_str()
        .expect("valid temp path")
        .to_string();
    let catalog = ParquetDataCatalog::new(temp_dir.path(), None, None, None, None);
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
        .catalog_path(catalog_path)
        .instrument_id(instrument_id)
        .bar_spec(bar_spec)
        .build()?;
    let run_id = "projectx-rust-backtest-test".to_string();
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

    assert_eq!(seen_bars.load(Ordering::Relaxed), bars.len());
    Ok(())
}
