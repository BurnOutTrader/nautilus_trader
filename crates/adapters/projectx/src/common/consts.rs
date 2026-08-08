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

use std::sync::LazyLock;

use nautilus_model::identifiers::{ClientId, Venue};

pub const PROJECT_X: &str = "PROJECTX";
pub static PROJECTX_CLIENT_ID: LazyLock<ClientId> = LazyLock::new(|| ClientId::from(PROJECT_X));
pub static PROJECTX_VENUE: LazyLock<Venue> = LazyLock::new(|| Venue::from(PROJECT_X));
pub const PROJECT_X_USER_AGENT: &str = "NautilusTrader/ProjectX";
pub const PROJECT_X_SIGNALR_TERMINATOR: char = '\u{001e}';
pub const PROJECT_X_GLOBAL_RATE_KEY: &str = "projectx:global";
pub const PROJECT_X_BARS_RATE_KEY: &str = "projectx:bars";
pub const PROJECT_X_REST_QUOTA_PER_MINUTE: u32 = 200;
pub const PROJECT_X_BARS_QUOTA_PER_30S: u32 = 50;
pub const PROJECT_X_TOKEN_VALIDATE_INTERVAL_SECS: u64 = 12 * 60 * 60;
pub const PROJECT_X_HEARTBEAT_SECS: u64 = 30;
pub const PROJECT_X_STALE_AFTER_MS: u64 = 90_000;
pub const PROJECT_X_WATCHDOG_INTERVAL_SECS: u64 = 5;
