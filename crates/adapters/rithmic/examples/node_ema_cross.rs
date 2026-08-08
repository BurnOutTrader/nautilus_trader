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

//! Live Rithmic Rust strategy example using external bars and historical warmup.
//!
//! Run with:
//! `cargo run --example rithmic-rust-live-ema-cross --package rithmic-nt`
//!
//! Required environment variables:
//! - `RITHMIC_USERNAME`
//! - `RITHMIC_PASSWORD`
//! - `RITHMIC_SYSTEM_NAME`
//! - `RITHMIC_ACCOUNT_ID`
//! - `RITHMIC_APP_NAME`
//!
//! Optional environment variables:
//! - `RITHMIC_PROFILE`
//! - `RITHMIC_INSTRUMENT_ID`
//! - `RITHMIC_PRODUCT_CODE`
//! - `RITHMIC_EXCHANGE`
//! - `RITHMIC_BAR_SPEC` (default `15-SECOND-LAST`)
//! - `RITHMIC_TRADE_SIZE` (default `1`)
//! - `RITHMIC_FAST_EMA` (default `10`)
//! - `RITHMIC_SLOW_EMA` (default `20`)
//! - `RITHMIC_WARMUP_MINUTES` (default `30`)
//! - `RITHMIC_RUN_SECONDS` (default `0`, disabled)

#[path = "support/mod.rs"]
mod support;

use nautilus_common::{enums::Environment, live::get_runtime};
use nautilus_live::node::LiveNode;
use nautilus_model::{identifiers::StrategyId, instruments::Instrument};
use rithmic_nt::{RithmicDataClientFactory, RithmicExecClientFactory, RithmicInstrumentProvider};

use crate::support::{
    bar_ema_cross::{RithmicBarEmaCrossConfig, RithmicBarEmaCrossStrategy},
    common::{
        build_external_bar_type, connect_history_gateway, data_config_from_env,
        disconnect_history_gateway, env_string, env_u64, exec_config_from_env, load_env,
        profile_from_env, resolve_instrument_from_env,
    },
};

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(default)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    load_env();

    let profile = profile_from_env();
    let bar_spec = env_string("RITHMIC_BAR_SPEC", "15-SECOND-LAST");
    let warmup_minutes = env_usize("RITHMIC_WARMUP_MINUTES", 30);
    let fast_ema = env_usize("RITHMIC_FAST_EMA", 10);
    let slow_ema = env_usize("RITHMIC_SLOW_EMA", 20);
    let trade_size =
        nautilus_model::types::Quantity::from(env_string("RITHMIC_TRADE_SIZE", "1").as_str());
    let run_seconds = env_u64("RITHMIC_RUN_SECONDS", 0);

    let gateway = connect_history_gateway(profile.as_deref()).await?;
    let provider = RithmicInstrumentProvider::new(std::sync::Arc::clone(&gateway));
    let instrument = resolve_instrument_from_env(&provider).await?;
    drop(provider);
    disconnect_history_gateway(gateway).await?;

    let instrument_id = instrument.id();
    let bar_type = build_external_bar_type(instrument_id, &bar_spec)?;
    let data_config = data_config_from_env(profile.as_deref(), true)?;
    let exec_config = exec_config_from_env(profile.as_deref())?;
    let trader_id = exec_config.trader_id;

    let strategy = RithmicBarEmaCrossStrategy::new(RithmicBarEmaCrossConfig {
        strategy_id: StrategyId::from("RITHMIC-EMA-001"),
        instrument_id,
        live_bar_type: bar_type,
        history_bar_type: bar_type,
        trade_size,
        fast_period: fast_ema,
        slow_period: slow_ema,
        warmup_minutes,
        request_bars_on_start: warmup_minutes > 0,
        unsubscribe_on_stop: true,
        cleanup_on_stop: true,
    });

    let mut node = LiveNode::builder(trader_id, Environment::Live)?
        .with_name("RITHMIC-RUST-LIVE-EMA-CROSS".to_string())
        .with_reconciliation(true)
        .with_delay_post_stop_secs(5)
        .add_data_client(
            None,
            Box::new(RithmicDataClientFactory::new()),
            Box::new(data_config),
        )?
        .add_exec_client(
            None,
            Box::new(RithmicExecClientFactory::new()),
            Box::new(exec_config),
        )?
        .build()?;

    node.add_strategy(strategy)?;

    println!("Rithmic Live EMA Cross");
    println!("Instrument ID: {instrument_id}");
    println!("Bar type: {bar_type}");
    println!("Warmup minutes: {warmup_minutes}");
    println!("Profile: {}", profile.as_deref().unwrap_or("<default>"));
    println!("WARNING: this example can submit live orders to the configured Rithmic account.");

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
