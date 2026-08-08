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

use crate::common::urls::ProjectXUrls;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ProjectXEnvironment {
    TopstepX,
    Custom(ProjectXUrls),
}

impl ProjectXEnvironment {
    #[must_use]
    pub fn urls(&self) -> ProjectXUrls {
        match self {
            Self::TopstepX => ProjectXUrls::topstep(),
            Self::Custom(urls) => urls.clone(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(
        module = "nautilus_trader.adapters.projectx",
        eq,
        eq_int,
        from_py_object
    )
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass_enum(module = "nautilus_trader.adapters.projectx")
)]
pub enum ProjectXHub {
    User,
    Market,
}

impl ProjectXHub {
    #[must_use]
    pub const fn path(self) -> &'static str {
        match self {
            Self::User => "user",
            Self::Market => "market",
        }
    }

    #[must_use]
    pub const fn requires_authentication(self) -> bool {
        matches!(self, Self::User)
    }
}
