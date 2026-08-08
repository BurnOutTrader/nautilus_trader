"""
Tests for the thin PyO3 ProjectX HTTP client binding.
"""

import pytest

import nautilus_trader.adapters.projectx as projectx
from nautilus_trader.adapters.projectx import ProjectXConfig
from nautilus_trader.adapters.projectx import ProjectXHttpClient
from nautilus_trader.model import BarType


def _test_config() -> ProjectXConfig:
    return ProjectXConfig(
        environment="TopstepX",
        user_name="test-user",
        api_key="test-api-key",
    )


class TestProjectXHttpClient:
    def test_instantiation_from_config(self):
        client = ProjectXHttpClient.from_config(_test_config())
        assert client is not None

    def test_urls_expose_environment_endpoints(self):
        client = ProjectXHttpClient.from_config(_test_config())
        assert "topstepx" in client.api_base_url
        assert "topstepx" in client.rtc_base_url

    @pytest.mark.parametrize(
        ("user_name", "api_key"),
        [(" test-user", "test-api-key"), ("test-user", "test-api-key ")],
    )
    def test_padded_credentials_are_rejected_not_silently_trimmed(self, user_name, api_key):
        config = ProjectXConfig(
            environment="TopstepX",
            user_name=user_name,
            api_key=api_key,
        )

        with pytest.raises(ValueError, match="surrounding whitespace"):
            ProjectXHttpClient.from_config(config)

    def test_low_level_ws_projection_is_not_exposed(self):
        client = ProjectXHttpClient.from_config(_test_config())
        assert not hasattr(client, "create_ws_client")
        assert not hasattr(projectx, "ProjectXHub")
        assert not hasattr(projectx, "ProjectXSubscription")
        assert not hasattr(projectx, "ProjectXWsClient")

    def test_stop_is_idempotent(self):
        client = ProjectXHttpClient.from_config(_test_config())
        client.stop()
        client.stop()

    @pytest.mark.asyncio
    async def test_validate_checks_existing_session_without_authenticating(self):
        client = ProjectXHttpClient.from_config(_test_config())

        with pytest.raises(RuntimeError, match="not authenticated"):
            await client.validate()

    @pytest.mark.asyncio
    async def test_retrieve_bars_rejects_unit_mismatched_with_bar_type(self):
        client = ProjectXHttpClient.from_config(_test_config())
        bar_type = BarType.from_str("MNQM26.PROJECTX-1-MINUTE-LAST-EXTERNAL")

        with pytest.raises(ValueError, match="does not match BarType aggregation"):
            await client.retrieve_bars(
                contract_id="CON.F.US.MNQ.M26",
                bar_type=bar_type,
                start_time="2026-04-07T00:00:00Z",
                end_time="2026-04-07T01:00:00Z",
                unit=3,
                unit_number=1,
            )

    @pytest.mark.asyncio
    async def test_retrieve_bars_rejects_step_mismatched_with_bar_type(self):
        client = ProjectXHttpClient.from_config(_test_config())
        bar_type = BarType.from_str("MNQM26.PROJECTX-5-MINUTE-LAST-EXTERNAL")

        with pytest.raises(ValueError, match="does not match BarType step"):
            await client.retrieve_bars(
                contract_id="CON.F.US.MNQ.M26",
                bar_type=bar_type,
                start_time="2026-04-07T00:00:00Z",
                end_time="2026-04-07T01:00:00Z",
                unit=2,
                unit_number=1,
            )

    @pytest.mark.asyncio
    async def test_retrieve_bars_derives_units_and_rejects_internal_bar_type(self):
        client = ProjectXHttpClient.from_config(_test_config())
        bar_type = BarType.from_str("MNQM26.PROJECTX-1-MINUTE-LAST-INTERNAL")

        with pytest.raises(ValueError, match="standard externally aggregated"):
            await client.retrieve_bars(
                contract_id="CON.F.US.MNQ.M26",
                bar_type=bar_type,
                start_time="2026-04-07T00:00:00Z",
                end_time="2026-04-07T01:00:00Z",
            )

    def test_contract_by_id_maps_invalid_input_to_value_error(self):
        client = ProjectXHttpClient.from_config(_test_config())

        with pytest.raises(ValueError, match="contract"):
            client.contract_by_id("")
