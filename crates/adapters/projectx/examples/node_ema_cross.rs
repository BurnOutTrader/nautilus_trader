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

//! Live ProjectX Rust strategy example with internal bars and external warmup history.
//!
//! Run with:
//! `cargo run --example projectx-rust-live-ema-cross -p projectx-nt`
//!
//! Required environment variables:
//! - `PROJECTX_USERNAME`
//! - `PROJECTX_API_KEY`
//!
//! Optional environment variables:
//! - `PROJECTX_INSTRUMENT_ID`
//! - `PROJECTX_PRODUCT_ROOT`
//! - `PROJECTX_LIVE_BAR_SPEC` (default `15-SECOND-LAST`)
//! - `PROJECTX_TRADE_SIZE` (default `1`)
//! - `PROJECTX_FAST_EMA` (default `10`)
//! - `PROJECTX_SLOW_EMA` (default `20`)
//! - `PROJECTX_WARMUP_MINUTES` (default `30`)
//! - `PROJECTX_RUN_SECONDS` (default `0`, disabled)
//! - `PROJECTX_MARKET_DATA_LIVE` (`true` or `false`, default `false`)
//! - `PROJECTX_TRADER_ID`

#[path = "support/mod.rs"]
mod support;

use std::str::FromStr;

use nautilus_common::{enums::Environment, live::get_runtime};
use nautilus_live::node::LiveNode;
use nautilus_model::{data::BarType, identifiers::StrategyId, types::Quantity};
use projectx_nt::{ProjectXDataClientFactory, ProjectXExecutionClientFactory};

use crate::support::{
    bar_ema_cross::{ProjectXBarEmaCrossConfig, ProjectXBarEmaCrossStrategy},
    common::{
        data_config_from_env, env_bool, env_string, env_u64, exec_config_from_env, load_env,
        resolve_instrument_id_from_env, trader_id_from_env,
    },
};

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(default)
}

fn bar_type_from_spec(
    instrument_id: nautilus_model::identifiers::InstrumentId,
    bar_spec: &str,
    source: &str,
) -> anyhow::Result<BarType> {
    BarType::from_str(&format!("{instrument_id}-{bar_spec}-{source}")).map_err(Into::into)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    load_env();

    let market_data_live = env_bool("PROJECTX_MARKET_DATA_LIVE", false);
    let trader_id = trader_id_from_env("PROJECTX_TRADER_ID", "RUST-PROJECTX-EMA-001");
    let instrument_id = resolve_instrument_id_from_env(market_data_live).await?;
    let bar_spec = env_string("PROJECTX_LIVE_BAR_SPEC", "15-SECOND-LAST");
    let live_bar_type = bar_type_from_spec(instrument_id, &bar_spec, "INTERNAL")?;
    let history_bar_type = bar_type_from_spec(instrument_id, &bar_spec, "EXTERNAL")?;
    let fast_ema = env_usize("PROJECTX_FAST_EMA", 10);
    let slow_ema = env_usize("PROJECTX_SLOW_EMA", 20);
    let trade_size = Quantity::from(env_string("PROJECTX_TRADE_SIZE", "1").as_str());
    let warmup_minutes = env_usize("PROJECTX_WARMUP_MINUTES", 30);
    let run_seconds = env_u64("PROJECTX_RUN_SECONDS", 0);

    let strategy = ProjectXBarEmaCrossStrategy::new(ProjectXBarEmaCrossConfig {
        strategy_id: StrategyId::from("PROJECTX-EMA-001"),
        instrument_id,
        live_bar_type,
        history_bar_type,
        trade_size,
        fast_period: fast_ema,
        slow_period: slow_ema,
        warmup_minutes,
        request_bars_on_start: warmup_minutes > 0,
        unsubscribe_on_stop: true,
        cleanup_on_stop: true,
    });

    let mut node = LiveNode::builder(trader_id, Environment::Live)?
        .with_name("PROJECTX-RUST-LIVE-EMA-CROSS".to_string())
        .with_reconciliation(true)
        .with_delay_post_stop_secs(5)
        .add_data_client(
            None,
            Box::new(ProjectXDataClientFactory::new()),
            Box::new(data_config_from_env()?),
        )?
        .add_exec_client(
            None,
            Box::new(ProjectXExecutionClientFactory::new()),
            Box::new(exec_config_from_env(trader_id)?),
        )?
        .build()?;

    node.add_strategy(strategy)?;

    println!("ProjectX Live EMA Cross");
    println!("Instrument ID: {instrument_id}");
    println!("Live bar type: {live_bar_type}");
    println!("Warmup bar type: {history_bar_type}");
    println!("Warmup minutes: {warmup_minutes}");
    println!("Market data live: {market_data_live}");
    println!("WARNING: this example can submit live orders to the configured ProjectX account.");

    if run_seconds > 0 {
        let handle = node.handle();
        get_runtime().spawn(async move {
            tokio::time::sleep(std::time::Duration::from_secs(run_seconds)).await;
            handle.stop();
        });
    }

    node.run().await?;
    Ok(())
}
