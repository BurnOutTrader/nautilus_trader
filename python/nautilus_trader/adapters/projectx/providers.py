# -------------------------------------------------------------------------------------------------
#  Copyright (C) 2026 Kevin Monaghan. All rights reserved.
#
#  Licensed under the GNU Lesser General Public License Version 3.0 or later.
#  You may not use this file except in compliance with the License.
#  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
# -------------------------------------------------------------------------------------------------
"""
ProjectX instrument provider helper.

A thin Python wrapper over the PyO3 ``ProjectXHttpClient.available_instruments``
surface for standalone instrument-discovery workflows.
"""

from __future__ import annotations

from typing import Any

from nautilus_trader._libnautilus.projectx import ProjectXHttpClient


class ProjectXInstrumentProvider:
    """
    Provides Nautilus instrument definitions from ProjectX available contracts.

    Parameters
    ----------
    client : ProjectXHttpClient
        The ProjectX HTTP client.
    live : bool, default False
        Whether to request live (`true`) or simulated (`false`) contract availability.

    """

    def __init__(
        self,
        client: ProjectXHttpClient,
        live: bool = False,
    ) -> None:
        self._client = client
        self._live = live
        self._instruments: list[Any] = []

    async def load_all_async(self, filters: dict | None = None) -> None:
        """
        Load all available instruments matching the optional filters.

        Parameters
        ----------
        filters : dict, optional
            Optional ``active_only`` and ``product_root`` filters.

        """
        filters = filters or {}
        active_only = bool(filters.get("active_only", False))
        product_root = filters.get("product_root")

        self._instruments = list(
            await self._client.available_instruments(
                live=self._live,
                active_only=active_only,
                product_root=product_root,
            ),
        )

    def get_all(self) -> list[Any]:
        """
        Return the loaded instruments.
        """
        return list(self._instruments)
