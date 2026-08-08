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

use thiserror::Error;

/// Errors surfaced by the ProjectX HTTP wrapper.
///
/// The wrapper is a thin facade over `projectx-client`, so transport,
/// authentication, provider, rate-limit, and decode failures all arrive as
/// [`projectx_client::Error`].
#[derive(Debug, Error)]
pub enum ProjectXHttpError {
    #[error(transparent)]
    Client(#[from] projectx_client::Error),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}
