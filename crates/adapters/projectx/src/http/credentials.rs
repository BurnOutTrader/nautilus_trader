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

use std::fmt::Debug;

use crate::common::enums::ProjectXEnvironment;

#[derive(Clone)]
pub struct ProjectXCredential {
    pub environment: ProjectXEnvironment,
    pub user_name: String,
    pub api_key: String,
}

impl ProjectXCredential {
    #[must_use]
    pub fn new(
        environment: ProjectXEnvironment,
        user_name: impl Into<String>,
        api_key: impl Into<String>,
    ) -> Self {
        Self {
            environment,
            user_name: user_name.into(),
            api_key: api_key.into(),
        }
    }
}

impl Debug for ProjectXCredential {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(ProjectXCredential))
            .field("environment", &self.environment)
            .field("user_name", &"<redacted>")
            .field("api_key", &"<redacted>")
            .finish()
    }
}
