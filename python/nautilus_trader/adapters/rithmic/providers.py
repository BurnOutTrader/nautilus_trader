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
Pure-Python symbol and product helpers for the Rithmic adapter.

The live instrument provider lives in the Rust `rithmic_nt` crate; this module
only carries the exchange/product lookup tables used by the download helpers.
"""

from __future__ import annotations

from nautilus_trader.model import Venue


RITHMIC_VENUE = Venue("RITHMIC")
KNOWN_EXCHANGES = (
    "CME",
    "CBOT",
    "NYMEX",
    "COMEX",
)

SUPPORTED_FRONT_MONTH_PRODUCTS_BY_EXCHANGE = {
    "CME": (
        "ES",
        "MES",
        "NQ",
        "MNQ",
        "RTY",
        "M2K",
        "NKD",
        "EMD",
        "MYM",
        "MBT",
        "MET",
        "6A",
        "6B",
        "6C",
        "6E",
        "6J",
        "6S",
        "E7",
        "M6E",
        "M6A",
        "6M",
        "6N",
        "M6B",
        "HE",
        "LE",
        "GF",
    ),
    "CBOT": ("YM", "MYM", "ZC", "ZW", "ZS", "ZM", "ZL", "ZT", "ZF", "ZN", "TN", "ZB", "UB"),
    "NYMEX": ("CL", "QM", "NG", "QG", "MCL", "RB", "HO", "PL", "MNG"),
    "COMEX": ("GC", "SI", "HG", "MGC", "SIL", "MHG"),
}

SUPPORTED_PRODUCT_EXCHANGE_CANDIDATES = {
    product: tuple(
        exchange
        for exchange, products in SUPPORTED_FRONT_MONTH_PRODUCTS_BY_EXCHANGE.items()
        if product in products
    )
    for product in {
        product
        for products in SUPPORTED_FRONT_MONTH_PRODUCTS_BY_EXCHANGE.values()
        for product in products
    }
}

SUPPORTED_PRODUCTS_BY_LENGTH = tuple(
    sorted(SUPPORTED_PRODUCT_EXCHANGE_CANDIDATES, key=len, reverse=True),
)


def split_exchange_from_symbol(symbol: str) -> tuple[str, str | None]:
    """
    Split an exchange suffix from a symbol when encoded as `SYMBOL.EXCHANGE` or
    `SYMBOL:EXCHANGE`.
    """
    normalized = symbol.strip().upper()

    if not normalized:
        raise ValueError("Rithmic symbol cannot be empty")

    separator_count = normalized.count(".") + normalized.count(":")

    if separator_count == 0:
        return normalized, None

    if separator_count != 1:
        raise ValueError(f"Malformed exchange-qualified Rithmic symbol {symbol!r}")

    separator = "." if "." in normalized else ":"
    base, suffix = normalized.split(separator, maxsplit=1)

    if not base or suffix not in KNOWN_EXCHANGES:
        raise ValueError(f"Malformed exchange-qualified Rithmic symbol {symbol!r}")

    return base, suffix


def normalize_rithmic_symbol(symbol: str) -> str:
    """
    Return the bare venue symbol without any encoded exchange suffix.
    """
    base, _ = split_exchange_from_symbol(symbol)
    return base


def supported_product_for_symbol(symbol: str) -> str | None:
    """
    Return the supported product root matched by the given exact contract or root symbol.
    """
    normalized = normalize_rithmic_symbol(symbol).upper()

    for product in SUPPORTED_PRODUCTS_BY_LENGTH:
        if normalized.startswith(product):
            return product

    return None


def candidate_exchanges_for_symbol(
    symbol: str,
    preferred_exchange: str | None = None,
) -> tuple[str, ...]:
    """
    Return the supported exchange candidates for a contract or root symbol.
    """
    product = supported_product_for_symbol(symbol)

    if product is None:
        if preferred_exchange is None:
            return ()
        preferred_exchange = preferred_exchange.strip().upper()
        return (preferred_exchange,) if preferred_exchange in KNOWN_EXCHANGES else ()

    exchanges = SUPPORTED_PRODUCT_EXCHANGE_CANDIDATES[product]

    if preferred_exchange is None:
        return exchanges

    preferred_exchange = preferred_exchange.strip().upper()

    if preferred_exchange not in exchanges:
        return ()

    return (preferred_exchange,)


def resolve_exchange_hint(symbol: str, filters: dict | None = None) -> str | None:
    """
    Resolve an exchange from request filters first, then from a symbol suffix.
    """
    if filters:
        exchange = filters.get("exchange")

        if exchange:
            return str(exchange).strip().upper()

        exchanges = filters.get("exchanges")

        if isinstance(exchanges, (list, tuple)) and exchanges:
            return str(exchanges[0]).strip().upper()

    _, exchange = split_exchange_from_symbol(symbol)
    return exchange


def front_month_products_for_exchange(exchange: str) -> tuple[str, ...]:
    """
    Return the supported hard-coded front-month roots for an exchange.
    """
    return tuple(SUPPORTED_FRONT_MONTH_PRODUCTS_BY_EXCHANGE.get(exchange.upper(), ()))
