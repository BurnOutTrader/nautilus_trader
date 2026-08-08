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

//! Resolve supported Rithmic front-month contracts and write them to a Nautilus
//! Parquet catalog.
//!
//! Run with:
//! `cargo run --example rithmic-write-instruments-catalog --package rithmic-nt`
//!
//! Required environment variables:
//! - `NAUTILUS_PATH`
//! - standard `RITHMIC_*` credentials (or `RITHMIC_PROFILE` + scoped values)
//!
//! Optional environment variables:
//! - `RITHMIC_EXCHANGE` to restrict the supported-root bootstrap to one exchange
//! - `RITHMIC_TRADEABLE_ONLY` (`true` by default to keep only currently tradeable contracts)
//!
//! This writer only supports the adapter's hard-coded Rithmic futures roots.
//! It writes one current contract per supported root and does not fall back to
//! the old full-chain discovery flow.

#[path = "support/mod.rs"]
mod support;

use std::sync::Arc;

use nautilus_model::instruments::Instrument;
use nautilus_persistence::backend::catalog::ParquetDataCatalog;
use rithmic_nt::{GatewayConfig, RithmicGateway, RithmicInstrumentProvider};

use crate::support::common::{
    env_bool, env_string, example_root_from_env, load_env, profile_from_env,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    load_env();

    let profile = profile_from_env();
    let exchange_filter = std::env::var("RITHMIC_EXCHANGE")
        .ok()
        .filter(|value| !value.trim().is_empty());
    let tradeable_only = env_bool("RITHMIC_TRADEABLE_ONLY", true);
    let example_root = example_root_from_env("rithmic")?;
    let catalog_path = example_root.join("catalog");

    let config = GatewayConfig::from_env_with_profile(profile.as_deref())?
        .with_order(false)
        .with_pnl(false)
        .with_history(false);
    let mut gateway = RithmicGateway::new(config);
    gateway.connect().await?;
    let gateway = Arc::new(gateway);
    let provider = RithmicInstrumentProvider::new(Arc::clone(&gateway));

    let instruments = if let Some(exchange) = exchange_filter.as_deref() {
        if tradeable_only {
            provider.load_exchange_tradeable_async(exchange).await?
        } else {
            provider.load_exchange_async(exchange).await?
        }
    } else {
        if tradeable_only {
            provider.load_all_tradeable_async().await?;
        } else {
            provider.load_all_async().await?;
        }
        provider.instruments()
    };

    let catalog_path_for_write = catalog_path.clone();
    let instruments_for_write = instruments.clone();
    let written_paths =
        tokio::task::spawn_blocking(move || -> anyhow::Result<Vec<std::path::PathBuf>> {
            std::fs::create_dir_all(&catalog_path_for_write)?;
            let catalog = ParquetDataCatalog::new(&catalog_path_for_write, None, None, None, None);
            catalog.write_instruments(instruments_for_write)
        })
        .await??;

    drop(provider);
    let gateway = Arc::try_unwrap(gateway)
        .map_err(|_| anyhow::anyhow!("Rithmic gateway still has active references"))?;
    let mut gateway = gateway;
    gateway.disconnect().await?;

    println!("Catalog path: {}", catalog_path.display());
    println!(
        "Exchange scope: {}",
        exchange_filter
            .as_deref()
            .unwrap_or(&env_string("RITHMIC_EXCHANGE", "ALL_ENABLED"))
    );
    println!(
        "Instrument mode: {}",
        if tradeable_only {
            "tradeable-front-months"
        } else {
            "supported-front-months"
        }
    );
    println!("Support scope: hard-coded supported Rithmic futures roots only");
    println!("Instruments written: {}", instruments.len());
    if let Some(first) = instruments.first() {
        println!("First instrument ID: {}", first.id());
    }
    if let Some(path) = written_paths.first() {
        println!("First parquet path: {}", path.display());
    }

    Ok(())
}
