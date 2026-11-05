import pytest

import stage.stage_library as stage_library
from stage.stage_library import StageLibrary
from stage.v3_0.ingress import Ingress as V3Ingress
from stage.v3_0.offline_ingress import OfflineIngress as V3OfflineIngress
from stage.v3_0.pack_power import PackPower as V3PackPower


def use_registry(monkeypatch, registry: dict) -> None:
    monkeypatch.setattr(stage_library, "_load_registry", lambda: registry)


PACK_POWER = {"module": "stage.v3_0.pack_power", "class": "PackPower"}


class TestRegistry:
    def test_v4_0_reuses_v3_0_offline_ingress(self):
        assert StageLibrary("v4_0").get_stage_by_name("Offline_Ingress") is V3OfflineIngress

    def test_v4_0_ingress_extends_v3_0_with_a_longer_grace(self):
        ingress = StageLibrary("v4_0").get_stage_by_name("Ingress")

        assert issubclass(ingress, V3Ingress)
        assert (V3Ingress.data_grace_s, ingress.data_grace_s) == (1.0, 1000.0)

    def test_every_stage_in_every_edition_resolves(self):
        for edition in StageLibrary().pipeline_editions:
            library = StageLibrary(edition)
            for name, meta in library._edition.items():
                if isinstance(meta, dict):
                    library.get_stage_by_name(name)

    def test_unknown_edition_is_rejected(self):
        with pytest.raises(ValueError):
            StageLibrary("v0_0")


class TestFrom:
    def test_from_reuses_the_named_editions_stage(self, monkeypatch):
        use_registry(monkeypatch, {"a": {"PackPower": PACK_POWER}, "b": {"PackPower": {"from": "a"}}})

        assert StageLibrary("b").get_stage_by_name("PackPower") is V3PackPower

    def test_from_follows_a_chain(self, monkeypatch):
        use_registry(monkeypatch, {
            "a": {"PackPower": PACK_POWER},
            "b": {"PackPower": {"from": "a"}},
            "c": {"PackPower": {"from": "b"}},
        })

        assert StageLibrary("c").get_stage_by_name("PackPower") is V3PackPower

    def test_from_an_edition_without_the_stage_is_an_error(self, monkeypatch):
        use_registry(monkeypatch, {"a": {}, "b": {"PackPower": {"from": "a"}}})

        with pytest.raises(ValueError, match="has no such stage"):
            StageLibrary("b").get_stage_by_name("PackPower")

    def test_a_loop_is_an_error(self, monkeypatch):
        use_registry(monkeypatch, {"a": {"PackPower": {"from": "b"}}, "b": {"PackPower": {"from": "a"}}})

        with pytest.raises(ValueError, match="loop"):
            StageLibrary("a").get_stage_by_name("PackPower")
