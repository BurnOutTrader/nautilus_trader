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

//! Credential handling for Rithmic authentication.

use std::fmt::Debug;

use crate::{
    config::RithmicDataClientConfig,
    error::{Result, RithmicError},
};

const DEFAULT_APP_VERSION: &str = "1.0";

/// Rithmic API credentials.
#[derive(Clone)]
pub struct RithmicCredentials {
    /// Rithmic username.
    pub username: String,
    /// Rithmic password.
    pub password: String,
    /// System name.
    pub system_name: String,
    /// Application name.
    pub app_name: String,
    /// Application version.
    pub app_version: String,
    /// FCM ID (optional).
    pub fcm_id: Option<String>,
    /// IB ID (optional).
    pub ib_id: Option<String>,
}

impl Debug for RithmicCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(RithmicCredentials))
            .field("username", &self.username)
            .field("password", &"[REDACTED]")
            .field("system_name", &self.system_name)
            .field("app_name", &self.app_name)
            .field("app_version", &self.app_version)
            .field("fcm_id", &self.fcm_id)
            .field("ib_id", &self.ib_id)
            .finish()
    }
}

impl RithmicCredentials {
    /// Creates new credentials.
    pub fn new(
        username: impl Into<String>,
        password: impl Into<String>,
        system_name: impl Into<String>,
        app_name: impl Into<String>,
    ) -> Result<Self> {
        let credentials = Self {
            username: username.into(),
            password: password.into(),
            system_name: system_name.into(),
            app_name: app_name.into(),
            app_version: DEFAULT_APP_VERSION.to_string(),
            fcm_id: None,
            ib_id: None,
        };
        credentials.validate()?;
        Ok(credentials)
    }

    /// Loads credentials from environment variables.
    ///
    /// Required variables:
    /// - `RITHMIC_USERNAME`
    /// - `RITHMIC_PASSWORD`
    /// - `RITHMIC_SYSTEM_NAME`
    ///
    /// Required variables:
    /// - `RITHMIC_APP_NAME`
    ///
    /// Optional variables:
    /// - `RITHMIC_APP_VERSION`
    /// - `RITHMIC_FCM_ID`
    /// - `RITHMIC_IB_ID`
    pub fn from_env() -> Result<Self> {
        Self::from_env_with_profile(None)
    }

    /// Loads credentials from canonical or profile-scoped environment variables.
    pub fn from_env_with_profile(profile: Option<&str>) -> Result<Self> {
        let config = RithmicDataClientConfig::from_env_with_profile(profile)?;
        Ok(Self {
            username: config.username,
            password: config.password,
            system_name: config.system_name,
            app_name: config.app_name,
            app_version: config.app_version,
            fcm_id: config.fcm_id,
            ib_id: config.ib_id,
        })
    }

    /// Validates that all required credentials are present and non-empty.
    pub fn validate(&self) -> Result<()> {
        if self.username.is_empty() {
            return Err(RithmicError::Config("Username cannot be empty".to_string()));
        }

        if self.password.is_empty() {
            return Err(RithmicError::Config("Password cannot be empty".to_string()));
        }

        if self.system_name.is_empty() {
            return Err(RithmicError::Config(
                "System name cannot be empty".to_string(),
            ));
        }

        if self.app_name.is_empty() {
            return Err(RithmicError::Config(
                "Application name cannot be empty".to_string(),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[rstest::rstest]
    fn test_credentials_validation() {
        let creds = RithmicCredentials::new("user", "pass", "system", "OwnApp").unwrap();
        assert!(creds.validate().is_ok());

        assert!(RithmicCredentials::new("", "pass", "system", "OwnApp").is_err());
        assert!(RithmicCredentials::new("user", "pass", "system", "").is_err());
    }

    #[rstest::rstest]
    fn test_credentials_debug_redacts_password() {
        let credentials =
            RithmicCredentials::new("user", "super-secret", "system", "OwnApp").unwrap();
        let output = format!("{credentials:?}");

        assert!(output.contains("[REDACTED]"));
        assert!(!output.contains("super-secret"));
    }
}
