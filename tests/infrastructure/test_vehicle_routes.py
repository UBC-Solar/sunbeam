import pytest

pytest.importorskip("fastapi")
pytest.importorskip("docker")

from sqlalchemy import func, select

from db.sunbeamdb.models import Vehicle


def count_vehicles(session_factory) -> int:
    with session_factory() as session:
        return session.scalar(select(func.count()).select_from(Vehicle))


class TestList:
    def test_lists_vehicles_with_their_event_counts(self, api_client, seeded_event):
        vehicles = api_client.get("/vehicles").json()

        assert [(v["id"], v["name"], v["event_count"]) for v in vehicles] == [
            (seeded_event.vehicle_id, "TestVehicle", 1),
        ]


class TestCreate:
    def test_creates_a_vehicle(self, api_client):
        response = api_client.post("/vehicles", json={"name": " Cascade ", "description": "4th generation"})

        assert response.status_code == 201
        assert (response.json()["name"], response.json()["event_count"]) == ("Cascade", 0)

    def test_a_taken_name_is_409(self, api_client, seeded_event):
        assert api_client.post("/vehicles", json={"name": "TestVehicle"}).status_code == 409

    def test_a_blank_name_is_400(self, api_client):
        response = api_client.post("/vehicles", json={"name": "  "})

        assert response.status_code == 400
        assert "needs a name" in response.json()["detail"]


class TestUpdate:
    def test_changes_only_the_fields_sent(self, api_client, seeded_event):
        response = api_client.patch(f"/vehicles/{seeded_event.vehicle_id}", json={"description": "renamed"})

        assert (response.json()["name"], response.json()["description"]) == ("TestVehicle", "renamed")

    def test_the_name_cant_be_cleared(self, api_client, seeded_event):
        assert api_client.patch(f"/vehicles/{seeded_event.vehicle_id}", json={"name": None}).status_code == 400

    def test_renaming_to_a_taken_name_is_409(self, api_client, seeded_event):
        api_client.post("/vehicles", json={"name": "Cascade"})

        response = api_client.patch(f"/vehicles/{seeded_event.vehicle_id}", json={"name": "Cascade"})

        assert response.status_code == 409

    def test_unknown_vehicle_is_404(self, api_client):
        assert api_client.patch("/vehicles/999", json={"description": "x"}).status_code == 404


class TestDelete:
    def test_deletes_a_vehicle_without_events(self, api_client, session_factory):
        vehicle_id = api_client.post("/vehicles", json={"name": "Cascade"}).json()["id"]

        assert api_client.delete(f"/vehicles/{vehicle_id}").status_code == 204
        assert count_vehicles(session_factory) == 0

    def test_a_vehicle_with_events_is_409(self, api_client, session_factory, seeded_event):
        response = api_client.delete(f"/vehicles/{seeded_event.vehicle_id}")

        assert response.status_code == 409
        assert "has 1 event;" in response.json()["detail"]
        assert count_vehicles(session_factory) == 1
