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

//! Pure Rust `LiveNode` example for the ProjectX data path.
//!
//! Run with:
//! `cargo run --example projectx-rust-quote-probe -p projectx-nt`
//!
//! Required environment variables:
//! - `PROJECTX_USERNAME`
//! - `PROJECTX_API_KEY`
//!
//! Optional environment variables:
//! - `PROJECTX_INSTRUMENT_ID` (for example `MESM26.PROJECTX`)
//! - `PROJECTX_PRODUCT_ROOT` (used when `PROJECTX_INSTRUMENT_ID` is unset)
//! - `PROJECTX_MARKET_DATA_LIVE` (`true` or `false`, default `false`)
//! - `PROJECTX_CAPTURE_SECONDS` (default `10`)

#[path = "support/common.rs"]
mod common;

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use nautilus_common::{actor::DataActor, enums::Environment, live::get_runtime};
use nautilus_live::node::LiveNode;
use nautilus_model::{
    data::{OrderBookDeltas, QuoteTick, TradeTick},
    enums::BookType,
    identifiers::{ClientId, InstrumentId, StrategyId},
    instruments::{Instrument, InstrumentAny},
};
use nautilus_trading::{StrategyConfig, StrategyCore, nautilus_strategy};
use projectx_nt::ProjectXDataClientFactory;

use crate::common::{
    DEFAULT_CAPTURE_SECONDS, data_config_from_env, env_bool, env_u64, load_env, projectx_client_id,
    resolve_instrument_id_from_env, trader_id_from_env,
};

#[derive(Debug)]
struct ProjectXQuoteProbe {
    core: StrategyCore,
    instrument_id: InstrumentId,
    client_id: Option<ClientId>,
    quote_count: Arc<AtomicUsize>,
    trade_count: Arc<AtomicUsize>,
    depth_count: Arc<AtomicUsize>,
}

impl ProjectXQuoteProbe {
    fn new(instrument_id: InstrumentId, client_id: Option<ClientId>) -> Self {
        let config = StrategyConfig {
            strategy_id: Some(StrategyId::from("PROJECTX-PROBE-001")),
            order_id_tag: Some("RUST".to_string()),
            ..Default::default()
        };

        Self {
            core: StrategyCore::new(config),
            instrument_id,
            client_id,
            quote_count: Arc::new(AtomicUsize::new(0)),
            trade_count: Arc::new(AtomicUsize::new(0)),
            depth_count: Arc::new(AtomicUsize::new(0)),
        }
    }
}

nautilus_strategy!(ProjectXQuoteProbe);

impl DataActor for ProjectXQuoteProbe {
    fn on_start(&mut self) -> anyhow::Result<()> {
        println!(
            "Starting ProjectX Rust quote probe for {}",
            self.instrument_id
        );

        let _ = self.request_instrument(self.instrument_id, None, None, self.client_id, None)?;
        self.subscribe_instrument(self.instrument_id, self.client_id, None);
        self.subscribe_quotes(self.instrument_id, self.client_id, None);
        self.subscribe_trades(self.instrument_id, self.client_id, None);
        self.subscribe_book_deltas(
            self.instrument_id,
            BookType::L2_MBP,
            None,
            self.client_id,
            false,
            None,
        );
        Ok(())
    }

    fn on_stop(&mut self) -> anyhow::Result<()> {
        self.unsubscribe_quotes(self.instrument_id, self.client_id, None);
        self.unsubscribe_trades(self.instrument_id, self.client_id, None);
        self.unsubscribe_book_deltas(self.instrument_id, self.client_id, None);
        self.unsubscribe_instrument(self.instrument_id, self.client_id, None);

        println!(
            "Stopped ProjectX Rust quote probe: quotes={}, trades={}, depth_updates={}",
            self.quote_count.load(Ordering::Relaxed),
            self.trade_count.load(Ordering::Relaxed),
            self.depth_count.load(Ordering::Relaxed),
        );
        Ok(())
    }

    fn on_instrument(&mut self, instrument: &InstrumentAny) -> anyhow::Result<()> {
        if instrument.id() == self.instrument_id {
            println!("Resolved ProjectX instrument {}", instrument.id());
        }
        Ok(())
    }

    fn on_quote(&mut self, quote: &QuoteTick) -> anyhow::Result<()> {
        let count = self.quote_count.fetch_add(1, Ordering::Relaxed) + 1;

        if count <= 3 {
            println!("Quote #{count}: {quote:?}");
        }
        Ok(())
    }

    fn on_trade(&mut self, trade: &TradeTick) -> anyhow::Result<()> {
        let count = self.trade_count.fetch_add(1, Ordering::Relaxed) + 1;

        if count <= 3 {
            println!("Trade #{count}: {trade:?}");
        }
        Ok(())
    }

    fn on_book_deltas(&mut self, deltas: &OrderBookDeltas) -> anyhow::Result<()> {
        let count = self.depth_count.fetch_add(1, Ordering::Relaxed) + 1;

        if count <= 3 {
            println!(
                "Depth #{count}: instrument={}, deltas={}, sequence={}",
                deltas.instrument_id,
                deltas.deltas.len(),
                deltas.sequence,
            );
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    load_env();

    let live = env_bool("PROJECTX_MARKET_DATA_LIVE", false);
    let run_secs = env_u64("PROJECTX_CAPTURE_SECONDS", DEFAULT_CAPTURE_SECONDS);
    let trader_id = trader_id_from_env("PROJECTX_TRADER_ID", "RUST-PROJECTX-001");
    let instrument_id = resolve_instrument_id_from_env(live).await?;
    let client_id = projectx_client_id();
    let data_config = data_config_from_env()?;
    let strategy = ProjectXQuoteProbe::new(instrument_id, Some(client_id));

    println!(
        "Running ProjectX Rust quote probe for {run_secs}s on {instrument_id} (market_data_live={live})",
    );

    let mut node = LiveNode::builder(trader_id, Environment::Live)?
        .with_name("PROJECTX-RUST-QUOTE-PROBE".to_string())
        .with_delay_post_stop_secs(1)
        .add_data_client(
            None,
            Box::new(ProjectXDataClientFactory::new()),
            Box::new(data_config),
        )?
        .build()?;

    node.add_strategy(strategy)?;

    let handle = node.handle();

    get_runtime().spawn(async move {
        tokio::time::sleep(std::time::Duration::from_secs(run_secs)).await;
        handle.stop();
    });

    node.run().await?;
    Ok(())
}
