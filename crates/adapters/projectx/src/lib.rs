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

//! ProjectX adapter implementation for NautilusTrader.
//!
//! The crate provides:
//! - REST authentication and gateway operations.
//! - SignalR-over-WebSocket connection management.
//! - Rust-native Nautilus data and execution clients for live trading.
//! - Thin PyO3 bindings and factory/config exports for the v2 Python-on-Rust path.
//!
//! Current adapter planning and verification notes live in `devplan.md`.
//!
//! # Feature Flags
//!
//! - `high-precision`: Enables 128-bit fixed-point Nautilus value types and is enabled by default.
//! - `python`: Enables the thin Python projection through [PyO3](https://pyo3.rs).
//! - `extension-module`: Builds the Python extension module and enables `python`.

#![warn(rustc::all)]
#![deny(unsafe_code)]
#![deny(nonstandard_style)]
#![deny(missing_debug_implementations)]
#![deny(rustdoc::broken_intra_doc_links)]

pub mod api;
pub mod common;
pub mod config;
pub mod data;
pub mod execution;
pub mod factories;
pub mod http;
pub mod websocket;

#[cfg(feature = "python")]
pub mod python;

pub use crate::{
    common::{
        enums::{ProjectXEnvironment, ProjectXHub},
        symbols::{
            databento_root, databento_to_projectx_adapter_symbol,
            databento_to_projectx_contract_id, databento_to_projectx_symbol,
            parse_databento_symbol, projectx_contract_root, projectx_to_databento_symbol,
            projectx_to_databento_symbol_with_year, projectx_to_rithmic_symbol,
            rithmic_to_projectx_symbol, rithmic_to_projectx_symbol_with_year,
        },
        urls::ProjectXUrls,
    },
    config::{ProjectXConfig, ProjectXDataClientConfig, ProjectXExecClientConfig},
    data::ProjectXDataClient,
    execution::ProjectXExecutionClient,
    factories::{ProjectXDataClientFactory, ProjectXExecutionClientFactory},
    http::{client::ProjectXHttpClient, credentials::ProjectXCredential, error::ProjectXHttpError},
    websocket::{
        client::{ProjectXSubscription, ProjectXWsClient, ProjectXWsEvent},
        error::ProjectXWsError,
    },
};
