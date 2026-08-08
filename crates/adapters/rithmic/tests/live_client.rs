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

//! Trait compliance tests for `RithmicLiveDataClient` and `RithmicLiveExecClient`.
//!
//! These tests verify the `DataClient` and `ExecutionClient` trait interfaces without
//! requiring a live Rithmic connection. They cover:
//! - Initial state (not connected, correct IDs/venue)
//! - Error paths when invoking subscribe methods before connect
//! - start/stop/reset/dispose lifecycle without panics

use std::{cell::RefCell, rc::Rc};

use nautilus_common::{
    cache::Cache,
    clients::{DataClient, ExecutionClient},
    messages::data::{
        SubscribeBars, SubscribeQuotes, SubscribeTrades, UnsubscribeBars, UnsubscribeQuotes,
        UnsubscribeTrades,
    },
};
use nautilus_core::{UUID4, UnixNanos};
use nautilus_live::ExecutionClientCore;
use nautilus_model::{
    data::{BarType, bar::BarSpecification},
    enums::{AccountType, AggregationSource, BarAggregation, OmsType, PriceType},
    identifiers::{AccountId, ClientId, InstrumentId, TraderId, Venue},
};
use rithmic_nt::{
    config::{RithmicDataClientConfig, RithmicEnv, RithmicExecClientConfig},
    data::live::RithmicLiveDataClient,
    execution::live::RithmicLiveExecClient,
};

// ---- Helpers ------------------------------------------------------------------------------------

fn make_cache() -> Rc<RefCell<Cache>> {
    Rc::new(RefCell::new(Cache::default()))
}

fn make_data_client() -> RithmicLiveDataClient {
    let config = RithmicDataClientConfig::new(RithmicEnv::Demo, "user", "pass", "TestSystem");
    RithmicLiveDataClient::new(ClientId::new("TESTSYSTEM"), config)
}

fn make_exec_client() -> RithmicLiveExecClient {
    let config = RithmicExecClientConfig::new(
        TraderId::from("TEST-001"),
        RithmicEnv::Demo,
        "user",
        "pass",
        "TestSystem",
        "ACC-001",
    );
    let cache = make_cache();
    let core = ExecutionClientCore::new(
        TraderId::from("TEST-001"),
        ClientId::new("TESTSYSTEM_ACC_001"),
        Venue::from("RITHMIC"),
        OmsType::Netting,
        AccountId::from("RITHMIC-TESTSYSTEM_ACC_001-ACC-001"),
        AccountType::Margin,
        None,
        Rc::clone(&cache),
    );
    RithmicLiveExecClient::new(core, config)
}

fn instrument_id() -> InstrumentId {
    InstrumentId::from("ESM5.RITHMIC")
}

fn minute_bar_type() -> BarType {
    BarType::Standard {
        instrument_id: instrument_id(),
        spec: BarSpecification {
            step: std::num::NonZero::new(1).unwrap(),
            aggregation: BarAggregation::Minute,
            price_type: PriceType::Last,
        },
        aggregation_source: AggregationSource::External,
    }
}

fn rithmic_venue() -> Venue {
    Venue::from("RITHMIC")
}

fn sub_quotes() -> SubscribeQuotes {
    SubscribeQuotes::new(
        instrument_id(),
        None,
        Some(rithmic_venue()),
        UUID4::new(),
        UnixNanos::default(),
        None,
        None,
    )
}

fn sub_trades() -> SubscribeTrades {
    SubscribeTrades::new(
        instrument_id(),
        None,
        Some(rithmic_venue()),
        UUID4::new(),
        UnixNanos::default(),
        None,
        None,
    )
}

fn sub_bars() -> SubscribeBars {
    SubscribeBars::new(
        minute_bar_type(),
        None,
        Some(rithmic_venue()),
        UUID4::new(),
        UnixNanos::default(),
        None,
        None,
    )
}

fn unsub_quotes() -> UnsubscribeQuotes {
    UnsubscribeQuotes::new(
        instrument_id(),
        None,
        Some(rithmic_venue()),
        UUID4::new(),
        UnixNanos::default(),
        None,
        None,
    )
}

fn unsub_trades() -> UnsubscribeTrades {
    UnsubscribeTrades::new(
        instrument_id(),
        None,
        Some(rithmic_venue()),
        UUID4::new(),
        UnixNanos::default(),
        None,
        None,
    )
}

fn unsub_bars() -> UnsubscribeBars {
    UnsubscribeBars::new(
        minute_bar_type(),
        None,
        Some(rithmic_venue()),
        UUID4::new(),
        UnixNanos::default(),
        None,
        None,
    )
}

// ---- DataClient trait compliance ----------------------------------------------------------------

#[rstest::rstest]
fn data_client_id_is_rithmic() {
    let client = make_data_client();
    assert_eq!(client.client_id(), ClientId::new("TESTSYSTEM"));
}

#[rstest::rstest]
fn data_client_venue_is_rithmic() {
    let client = make_data_client();
    assert_eq!(client.venue(), Some(Venue::from("RITHMIC")));
}

#[rstest::rstest]
fn data_client_initially_disconnected() {
    let client = make_data_client();
    assert!(!client.is_connected());
    assert!(client.is_disconnected());
}

#[rstest::rstest]
fn data_client_start_does_not_panic() {
    let mut client = make_data_client();
    assert!(client.start().is_ok());
}

#[rstest::rstest]
fn data_client_stop_does_not_panic() {
    let mut client = make_data_client();
    assert!(client.stop().is_ok());
    assert!(!client.is_connected());
}

#[rstest::rstest]
fn data_client_reset_clears_state() {
    let mut client = make_data_client();
    assert!(client.reset().is_ok());
    assert!(!client.is_connected());
}

#[rstest::rstest]
fn data_client_dispose_does_not_panic() {
    let mut client = make_data_client();
    assert!(client.dispose().is_ok());
}

#[rstest::rstest]
fn subscribe_quotes_errors_when_not_connected() {
    let mut client = make_data_client();
    let result = client.subscribe_quotes(sub_quotes());
    assert!(result.is_err());
    let msg = result.err().unwrap().to_string();
    assert!(
        msg.to_lowercase().contains("not connected"),
        "Expected 'not connected' in error: {msg}"
    );
}

#[rstest::rstest]
fn subscribe_trades_errors_when_not_connected() {
    let mut client = make_data_client();
    assert!(client.subscribe_trades(sub_trades()).is_err());
}

#[rstest::rstest]
fn subscribe_bars_errors_when_not_connected() {
    let mut client = make_data_client();
    // subscribe_bars now requires a connected gateway (v2 full implementation)
    assert!(client.subscribe_bars(sub_bars()).is_err());
}

#[rstest::rstest]
fn unsubscribe_quotes_is_noop_when_not_connected() {
    let mut client = make_data_client();
    // No inner gateway, so nothing to unsubscribe — should succeed silently
    assert!(client.unsubscribe_quotes(&unsub_quotes()).is_ok());
}

#[rstest::rstest]
fn unsubscribe_trades_is_noop_when_not_connected() {
    let mut client = make_data_client();
    assert!(client.unsubscribe_trades(&unsub_trades()).is_ok());
}

#[rstest::rstest]
fn unsubscribe_bars_is_stub_succeeds() {
    let mut client = make_data_client();
    assert!(client.unsubscribe_bars(&unsub_bars()).is_ok());
}

// ---- ExecutionClient trait compliance -----------------------------------------------------------

#[rstest::rstest]
fn exec_client_id_is_rithmic() {
    let client = make_exec_client();
    assert_eq!(client.client_id(), ClientId::new("TESTSYSTEM_ACC_001"));
}

#[rstest::rstest]
fn exec_client_venue_is_rithmic() {
    let client = make_exec_client();
    assert_eq!(client.venue(), Venue::from("RITHMIC"));
}

#[rstest::rstest]
fn exec_client_account_id() {
    let client = make_exec_client();
    assert_eq!(
        client.account_id(),
        AccountId::from("RITHMIC-TESTSYSTEM_ACC_001-ACC-001")
    );
}

#[rstest::rstest]
fn exec_client_oms_type_netting() {
    let client = make_exec_client();
    assert_eq!(client.oms_type(), OmsType::Netting);
}

#[rstest::rstest]
fn exec_client_initially_disconnected() {
    let client = make_exec_client();
    assert!(!client.is_connected());
}

#[rstest::rstest]
fn exec_client_start_does_not_panic() {
    let mut client = make_exec_client();
    assert!(client.start().is_ok());
}

#[rstest::rstest]
fn exec_client_stop_does_not_panic() {
    let mut client = make_exec_client();
    assert!(client.stop().is_ok());
    assert!(!client.is_connected());
}

#[rstest::rstest]
fn exec_client_get_account_returns_none_before_connect() {
    let client = make_exec_client();
    assert!(client.get_account().is_none());
}
