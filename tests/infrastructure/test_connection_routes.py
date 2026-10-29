from datetime import datetime

import pytest

pytest.importorskip("fastapi")

from fastapi.testclient import TestClient

from db.sunbeamdb.models import Event
from server.deps import get_db
from server.routes.connections import get_connection_service
from server.services.connection_service import ConnectionService
from tests.infrastructure.conftest import TEST_CONNECTION, FakeChecker

NEW_CONNECTION = {
    "name": "staging",
    "url": "http://staging:8086",
    "organization": "staging-org",
    "bucket": "CAN_staging",
    "token": "staging-token",
}


@pytest.fixture
def checker():
    return FakeChecker()


@pytest.fixture
def client(session_factory, checker):
    from server.main import create_app

    app = create_app(lifespan=None)

    def override_get_db():
        db = session_factory()
        try:
            yield db
        finally:
            db.close()

    service = ConnectionService(checker=checker)
    app.dependency_overrides[get_db] = override_get_db
    app.dependency_overrides[get_connection_service] = lambda: service

    with TestClient(app) as test_client:
        yield test_client


def defaults(client) -> list[str]:
    return [c["name"] for c in client.get("/connections").json() if c["is_default"]]


class TestCrud:
    def test_lists_the_seeded_connection_with_its_token(self, client, seeded_event):
        [connection] = client.get("/connections").json()

        assert connection["name"] == TEST_CONNECTION["name"]
        assert connection["token"] == TEST_CONNECTION["token"]
        assert connection["kind"] == "influxdb"
        assert connection["is_default"] is True

    def test_first_connection_becomes_the_default(self, client):
        response = client.post("/connections", json=NEW_CONNECTION)

        assert response.status_code == 201
        assert response.json()["is_default"] is True

    def test_new_default_replaces_the_old(self, client, seeded_event):
        client.post("/connections", json={**NEW_CONNECTION, "is_default": True})

        assert defaults(client) == ["staging"]

    def test_non_default_leaves_the_default_alone(self, client, seeded_event):
        client.post("/connections", json=NEW_CONNECTION)

        assert defaults(client) == [TEST_CONNECTION["name"]]

    def test_duplicate_name_is_409(self, client, seeded_event):
        response = client.post("/connections", json={**NEW_CONNECTION, "name": TEST_CONNECTION["name"]})

        assert response.status_code == 409

    def test_update_changes_only_given_fields_and_clears_the_last_test(self, client, seeded_event):
        client.post(f"/connections/{seeded_event.connection_id}/test")

        updated = client.patch(f"/connections/{seeded_event.connection_id}", json={"token": "new-token"}).json()

        assert updated["token"] == "new-token"
        assert updated["bucket"] == TEST_CONNECTION["bucket"]
        assert updated["last_test_ok"] is None

    def test_rename_keeps_the_last_test(self, client, seeded_event):
        client.post(f"/connections/{seeded_event.connection_id}/test")

        updated = client.patch(f"/connections/{seeded_event.connection_id}", json={"name": "renamed"}).json()

        assert (updated["name"], updated["last_test_ok"]) == ("renamed", True)

    def test_make_default(self, client, seeded_event):
        created = client.post("/connections", json=NEW_CONNECTION).json()

        client.post(f"/connections/{created['id']}/default")

        assert defaults(client) == ["staging"]

    def test_deleting_the_default_promotes_another(self, client, seeded_event):
        client.post("/connections", json=NEW_CONNECTION)

        assert client.delete(f"/connections/{seeded_event.connection_id}").status_code == 204
        assert defaults(client) == ["staging"]

    def test_unknown_connection_is_404(self, client):
        assert client.patch("/connections/999", json={"name": "x"}).status_code == 404
        assert client.delete("/connections/999").status_code == 404
        assert client.post("/connections/999/default").status_code == 404
        assert client.post("/connections/999/test").status_code == 404


class TestTesting:
    def test_unsaved_settings_can_be_tested(self, client, checker):
        result = client.post("/connections/test", json=NEW_CONNECTION | {"name": "ignored"}).json()

        assert result["ok"] is True
        assert [step["name"] for step in result["steps"]] == ["connect", "bucket"]
        assert checker.calls[0]["bucket"] == "CAN_staging"
        assert checker.calls[0]["window"] is None

    def test_saved_test_is_recorded(self, client, checker, seeded_event):
        checker.fail_with = "no data"

        result = client.post(f"/connections/{seeded_event.connection_id}/test").json()

        assert result["ok"] is False
        assert result["detail"] == "data: no data"
        [connection] = client.get("/connections").json()
        assert connection["last_test_ok"] is False
        assert connection["last_test_detail"] == "data: no data"
        assert connection["last_tested_at"] is not None

    def test_against_an_event_checks_its_window(self, client, checker, session_factory, seeded_event):
        with session_factory() as session:
            session.get(Event, seeded_event.event_id).ends_at = datetime(2026, 7, 1, 13, 0, 0)
            session.commit()

        result = client.post(
            f"/connections/{seeded_event.connection_id}/test", params={"event_id": seeded_event.event_id},
        ).json()

        start, stop = checker.calls[0]["window"]
        assert (start.hour, stop.hour) == (12, 13)
        assert result["window_start"].startswith("2026-07-01T12:00:00")

    def test_against_an_unknown_event_is_404(self, client, seeded_event):
        response = client.post(f"/connections/{seeded_event.connection_id}/test", params={"event_id": 999})

        assert response.status_code == 404
