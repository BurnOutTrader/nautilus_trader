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

//! Example demonstrating live data testing with the Rithmic adapter.
//!
//! Reads credentials from environment variables (or a `.env` file):
//! - `RITHMIC_USERNAME`
//! - `RITHMIC_PASSWORD`
//! - `RITHMIC_SYSTEM_NAME`
//! - `RITHMIC_ENV` (`Demo`, `Live`, or `Test` — defaults to `Demo`)
//!
//! Run with: `cargo run --example rithmic-data-tester --package rithmic-nt`

use nautilus_common::enums::Environment;
use nautilus_live::node::LiveNode;
use nautilus_model::{
    identifiers::{ClientId, InstrumentId, TraderId},
    stubs::TestDefault,
};
use nautilus_testkit::testers::{DataTester, DataTesterConfig};
use rithmic_nt::{config::RithmicDataClientConfig, factories::RithmicDataClientFactory};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenvy::dotenv().ok();

    let environment = Environment::Live;
    let trader_id = TraderId::test_default();
    let node_name = "RITHMIC-DATA-TESTER-001".to_string();

    // Instrument IDs use the format `{symbol}.RITHMIC`.
    // Adjust to an exact contract active at the time of testing.
    let instrument_ids = vec![
        InstrumentId::from("ESM5.RITHMIC"),
        // InstrumentId::from("NQM5.RITHMIC"),
    ];

    // Reads credentials from env vars: RITHMIC_USERNAME, RITHMIC_PASSWORD,
    // RITHMIC_SYSTEM_NAME, RITHMIC_ENV, RITHMIC_APP_NAME, RITHMIC_APP_VERSION.
    let rithmic_config = RithmicDataClientConfig::from_env()?;

    let client_factory = RithmicDataClientFactory::new();
    let client_id = ClientId::new("RITHMIC");

    let mut node = LiveNode::builder(trader_id, environment)?
        .with_name(node_name)
        .with_delay_post_stop_secs(2)
        .add_data_client(None, Box::new(client_factory), Box::new(rithmic_config))?
        .build()?;

    let tester_config = DataTesterConfig::builder()
        .client_id(client_id)
        .instrument_ids(instrument_ids)
        .subscribe_quotes(true)
        .subscribe_trades(true)
        .subscribe_bars(true)
        .build()?;
    let tester = DataTester::new(tester_config);

    node.add_actor(tester)?;
    node.run().await?;

    Ok(())
}
