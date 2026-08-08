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

//! Example demonstrating live execution testing with the Rithmic adapter.
//!
//! Reads credentials from environment variables (or a `.env` file):
//! - `RITHMIC_USERNAME`
//! - `RITHMIC_PASSWORD`
//! - `RITHMIC_SYSTEM_NAME`
//! - `RITHMIC_ACCOUNT_ID`
//! - `RITHMIC_ENV` (`Demo`, `Live`, or `Test` — defaults to `Demo`)
//!
//! WARNING: This example submits real orders. Use only with a demo/paper account.
//!
//! Run with: `cargo run --example rithmic-exec-tester --package rithmic-nt`

use nautilus_common::enums::Environment;
use nautilus_live::node::LiveNode;
use nautilus_model::{
    identifiers::{ClientId, InstrumentId, TraderId},
    types::Quantity,
};
use nautilus_testkit::testers::{ExecTester, ExecTesterConfig};
use rithmic_nt::{
    config::{RithmicDataClientConfig, RithmicExecClientConfig},
    factories::{RithmicDataClientFactory, RithmicExecClientFactory},
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenvy::dotenv().ok();

    let environment = Environment::Live;
    let trader_id = TraderId::from("RITHMIC-TESTER-001");
    let node_name = "RITHMIC-EXEC-TESTER-001".to_string();
    let client_id = ClientId::new("RITHMIC");

    // Adjust to a contract active at the time of testing.
    let instrument_id = InstrumentId::from("ESM5.RITHMIC");

    // Data client for market data subscriptions (quotes needed by ExecTester).
    let data_config = RithmicDataClientConfig::from_env()?;

    // Execution client for order management.
    let exec_config = RithmicExecClientConfig::from_env()?;

    let data_factory = RithmicDataClientFactory::new();
    let exec_factory = RithmicExecClientFactory::new();

    let mut node = LiveNode::builder(trader_id, environment)?
        .with_name(node_name)
        .add_data_client(None, Box::new(data_factory), Box::new(data_config))?
        .add_exec_client(None, Box::new(exec_factory), Box::new(exec_config))?
        .with_reconciliation(true)
        .with_delay_post_stop_secs(5)
        .build()?;

    let tester_config = ExecTesterConfig::builder()
        .instrument_id(instrument_id)
        .client_id(client_id)
        .order_qty(Quantity::from("1")) // 1 contract
        .log_data(false)
        .cancel_orders_on_stop(true)
        .close_positions_on_stop(true)
        .build()?;

    let tester = ExecTester::new(tester_config);

    node.add_strategy(tester)?;
    node.run().await?;

    Ok(())
}
