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

//! Pure Rust `LiveNode` execution tester for the ProjectX adapter.
//!
//! Run with:
//! `cargo run --example projectx-rust-exec-tester -p projectx-nt`
//!
//! Required environment variables:
//! - `PROJECTX_USERNAME`
//! - `PROJECTX_API_KEY`
//!
//! Optional environment variables:
//! - `PROJECTX_INSTRUMENT_ID` (for example `MESM26.PROJECTX`)
//! - `PROJECTX_PRODUCT_ROOT` (used when `PROJECTX_INSTRUMENT_ID` is unset)
//! - `PROJECTX_MARKET_DATA_LIVE` (`true` or `false`, default `false`)
//! - `PROJECTX_ACCOUNT_ID` or `PROJECTX_EXEC_ACCOUNT_ID`
//! - `PROJECTX_EXEC_SECONDS` (default `10`)
//! - `PROJECTX_EXEC_DRY_RUN` (`true` by default)

#[path = "support/common.rs"]
mod common;

use nautilus_common::{enums::Environment, live::get_runtime};
use nautilus_live::node::LiveNode;
use nautilus_model::{identifiers::StrategyId, types::Quantity};
use nautilus_testkit::testers::{ExecTester, ExecTesterConfig};
use nautilus_trading::strategy::StrategyConfig;
use projectx_nt::{ProjectXDataClientFactory, ProjectXExecutionClientFactory};

use crate::common::{
    DEFAULT_CAPTURE_SECONDS, data_config_from_env, env_bool, env_u64, exec_config_from_env,
    load_env, projectx_client_id, resolve_instrument_id_from_env, trader_id_from_env,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    load_env();

    let live = env_bool("PROJECTX_MARKET_DATA_LIVE", false);
    let run_secs = env_u64("PROJECTX_EXEC_SECONDS", DEFAULT_CAPTURE_SECONDS);
    let dry_run = env_bool("PROJECTX_EXEC_DRY_RUN", true);
    let trader_id = trader_id_from_env("PROJECTX_TRADER_ID", "RUST-PROJECTX-EXEC-001");
    let instrument_id = resolve_instrument_id_from_env(live).await?;
    let client_id = projectx_client_id();
    let order_qty = Quantity::from(1);

    let tester_config = ExecTesterConfig::builder()
        .base(StrategyConfig {
            strategy_id: Some(StrategyId::from("PROJECTX-EXEC-TESTER-001")),
            external_order_claims: Some(vec![instrument_id]),
            ..Default::default()
        })
        .instrument_id(instrument_id)
        .client_id(client_id)
        .order_qty(order_qty)
        .open_position_on_start_qty(order_qty.as_decimal())
        .log_data(false)
        .dry_run(dry_run)
        .build()?;
    let tester = ExecTester::new(tester_config);

    println!(
        "Running ProjectX Rust exec tester for {run_secs}s on {instrument_id} (market_data_live={live}, dry_run={dry_run})",
    );

    let mut node = LiveNode::builder(trader_id, Environment::Live)?
        .with_name("PROJECTX-RUST-EXEC-TESTER".to_string())
        .with_reconciliation(true)
        .with_delay_post_stop_secs(2)
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

    node.add_strategy(tester)?;

    let handle = node.handle();

    get_runtime().spawn(async move {
        tokio::time::sleep(std::time::Duration::from_secs(run_secs)).await;
        handle.stop();
    });

    node.run().await?;
    Ok(())
}
