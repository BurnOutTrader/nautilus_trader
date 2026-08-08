"""
Tests for the v2 ProjectX instrument provider helper.
"""

from unittest.mock import MagicMock

import pytest

from nautilus_trader.adapters.projectx import ProjectXInstrumentProvider


class _FakeHttpClient:
    def __init__(self) -> None:
        self.instruments = ["inst-1", "inst-2"]

    async def available_instruments(self, live, active_only, product_root):
        return self.instruments


@pytest.mark.asyncio
async def test_load_all_async_populates_instruments():
    client = _FakeHttpClient()
    provider = ProjectXInstrumentProvider(client=client, live=True)

    await provider.load_all_async(filters={"active_only": True, "product_root": "MES"})

    assert provider.get_all() == ["inst-1", "inst-2"]


@pytest.mark.asyncio
async def test_get_all_empty_before_load():
    provider = ProjectXInstrumentProvider(client=MagicMock())

    assert provider.get_all() == []
