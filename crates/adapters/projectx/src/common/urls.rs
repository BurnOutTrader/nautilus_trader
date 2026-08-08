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

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProjectXUrls {
    pub api_base: String,
    pub rtc_base: String,
}

impl ProjectXUrls {
    #[must_use]
    pub fn topstep() -> Self {
        Self {
            api_base: "https://api.topstepx.com".to_string(),
            rtc_base: "https://rtc.topstepx.com".to_string(),
        }
    }

    #[must_use]
    pub fn new(api_base: impl Into<String>, rtc_base: impl Into<String>) -> Self {
        Self {
            api_base: api_base.into(),
            rtc_base: rtc_base.into(),
        }
    }

    #[must_use]
    pub fn user_hub_ws_url(&self, token: Option<&str>) -> String {
        self.hub_ws_url("user", token)
    }

    #[must_use]
    pub fn market_hub_ws_url(&self, token: Option<&str>) -> String {
        self.hub_ws_url("market", token)
    }

    fn hub_ws_url(&self, hub: &str, token: Option<&str>) -> String {
        let mut base = self.rtc_base.trim_end_matches('/').to_string();

        if base.starts_with("https://") {
            base = base.replacen("https://", "wss://", 1);
        } else if base.starts_with("http://") {
            base = base.replacen("http://", "ws://", 1);
        }

        match token {
            Some(token) if !token.is_empty() => format!("{base}/hubs/{hub}?access_token={token}"),
            _ => format!("{base}/hubs/{hub}"),
        }
    }
}
