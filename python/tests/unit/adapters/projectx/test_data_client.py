"""
Tests for the PyO3 ProjectX HTTP and websocket client bindings.
"""


from nautilus_trader.adapters.projectx import ProjectXConfig
from nautilus_trader.adapters.projectx import ProjectXHttpClient
from nautilus_trader.adapters.projectx import ProjectXHub
from nautilus_trader.adapters.projectx import ProjectXSubscription
from nautilus_trader.adapters.projectx import ProjectXWsClient


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

    def test_create_ws_client_returns_ws_client(self):
        client = ProjectXHttpClient.from_config(_test_config())
        ws_client = client.create_ws_client(ProjectXHub.Market)
        assert isinstance(ws_client, ProjectXWsClient)

    def test_stop_is_idempotent(self):
        client = ProjectXHttpClient.from_config(_test_config())
        client.stop()
        client.stop()


class TestProjectXSubscription:
    def test_constructs_with_target_and_arguments(self):
        subscription = ProjectXSubscription("SubscribeContractQuotes", ['"CON.F.US.MNQ.M26"'])
        assert "SubscribeContractQuotes" in repr(subscription)
        assert "CON.F.US.MNQ.M26" in repr(subscription)

    def test_repr_round_trip(self):
        subscription = ProjectXSubscription("SubscribeContractQuotes", ['"CON.F.US.MNQ.M26"'])
        assert "SubscribeContractQuotes" in repr(subscription)
