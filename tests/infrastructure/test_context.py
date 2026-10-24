import pytest

from config.context import Context, ServiceType, configuration_profiles
from tests.infrastructure.conftest import TEST_CONFIG


def config_with_profiles(**blocks: list[str]) -> dict:
    """ A worker config whose blocks each define the given profiles (with throwaway contents). """
    return {"main": {"default_config": "debug"}, "worker": {block: dict.fromkeys(profiles, {}) for block, profiles in blocks.items()}}


@pytest.fixture(autouse=True)
def reset_context():
    Context._instance = None
    yield
    Context._instance = None


class TestConfigurationProfiles:
    def test_lists_profiles_in_order(self):
        config = config_with_profiles(sunbeamdb=["debug", "production", "staging"], telemetrydb=["debug", "production", "staging"])

        assert configuration_profiles(ServiceType.Worker, config) == ["debug", "production", "staging"]

    def test_only_profiles_every_block_defines(self):
        config = config_with_profiles(sunbeamdb=["debug", "production", "staging"], telemetrydb=["debug", "production"])

        assert configuration_profiles(ServiceType.Worker, config) == ["debug", "production"]


class TestContextProfile:
    def test_remembers_the_loaded_profile(self):
        assert Context.from_config(TEST_CONFIG, ServiceType.Worker, "test").configuration_profile == "test"

    def test_defaults_to_main_default_config(self):
        assert Context.from_config(TEST_CONFIG, ServiceType.Worker).configuration_profile == "test"

    def test_unknown_profile_names_the_known_ones(self):
        with pytest.raises(ValueError, match=r"Unknown configuration profile 'nope'.*\['test'\]"):
            Context.from_config(TEST_CONFIG, ServiceType.Worker, "nope")
