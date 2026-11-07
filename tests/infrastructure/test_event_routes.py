from datetime import datetime

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("docker")

from sqlalchemy import func, select

from config.events import load_event
from db.sunbeamdb.models import Event, Sample, Signal, WorkerRun, WorkerStatus


@pytest.fixture
def new_event(seeded_event) -> dict:
    return {
        "name": "FSGP_2024_Day_2",
        "vehicle_id": seeded_event.vehicle_id,
        "description": "Second day",
        "starts_at": "2024-07-17T15:00:00Z",
        "ends_at": "2024-07-18T04:00:00Z",
        "pipeline_edition": "v3_0",
        "stages": ["MotorPower", "Efficiency"],
    }


def count(session_factory, model, **where) -> int:
    with session_factory() as session:
        query = select(func.count()).select_from(model)
        for column, value in where.items():
            query = query.where(getattr(model, column) == value)
        return session.scalar(query)


def add_worker(session_factory, event_id: int, status: WorkerStatus) -> None:
    with session_factory() as session:
        session.add(WorkerRun(event_id=event_id, pipeline_edition="v3_0", status=status))
        session.commit()


class TestCreate:
    def test_creates_an_unrun_event_with_its_signals(self, api_client, session_factory, new_event):
        response = api_client.post("/events", json=new_event)

        assert response.status_code == 201
        event = response.json()
        assert (event["name"], event["status"], event["running"], event["sample_count"]) == (
            "FSGP_2024_Day_2", "unprocessed", False, 0,
        )
        assert event["stages"] == ["MotorPower", "Efficiency"]
        assert event["is_replay"] is True
        assert count(session_factory, Signal, event_id=event["id"]) > 0

    def test_the_worker_can_load_it(self, api_client, engine, new_event):
        api_client.post("/events", json=new_event)

        event = load_event(engine, "FSGP_2024_Day_2")

        assert event.stages == ["MotorPower", "Efficiency"]
        assert event.starts_at == datetime.fromisoformat("2024-07-17T15:00:00Z")

    def test_without_an_end_it_runs_live(self, api_client, new_event):
        del new_event["ends_at"]

        assert api_client.post("/events", json=new_event).json()["is_replay"] is False

    def test_a_taken_name_is_409(self, api_client, seeded_event, new_event):
        new_event["name"] = seeded_event.event_name

        assert api_client.post("/events", json=new_event).status_code == 409

    @pytest.mark.parametrize("change, message", [
        ({"name": "  "}, "needs a name"),
        ({"stages": []}, "at least one stage"),
        ({"stages": ["  "]}, "at least one stage"),
        ({"stages": ["Ingress"]}, "adds ingress itself"),
        ({"pipeline_edition": "v0_0"}, "Unknown pipeline edition"),
        ({"vehicle_id": 999}, "No vehicle"),
        ({"ends_at": "2024-07-17T14:00:00Z"}, "end after it starts"),
    ])
    def test_invalid_events_are_400(self, api_client, new_event, change, message):
        response = api_client.post("/events", json={**new_event, **change})

        assert response.status_code == 400
        assert message in response.json()["detail"]

    def test_a_stage_the_server_lacks_is_kept_for_a_git_ref(self, api_client, new_event):
        new_event["stages"] = ["MotorPower", " TryingThisOut "]

        event = api_client.post("/events", json=new_event).json()

        assert event["stages"] == ["MotorPower", "TryingThisOut"]
        assert event["unregistered_stages"] == ["TryingThisOut"]

    def test_duplicate_stages_are_kept_once(self, api_client, new_event):
        new_event["stages"] = ["MotorPower", "MotorPower"]

        assert api_client.post("/events", json=new_event).json()["stages"] == ["MotorPower"]


class TestUpdate:
    def test_changes_only_the_fields_sent(self, api_client, seeded_event):
        response = api_client.patch(f"/events/{seeded_event.event_id}", json={"description": "edited"})

        assert response.status_code == 200
        assert response.json()["description"] == "edited"
        assert response.json()["name"] == seeded_event.event_name

    def test_null_end_makes_it_ongoing(self, api_client, seeded_event):
        api_client.patch(f"/events/{seeded_event.event_id}", json={"ends_at": "2026-07-01T13:00:00Z"})

        response = api_client.patch(f"/events/{seeded_event.event_id}", json={"ends_at": None})

        assert response.json()["ends_at"] is None

    def test_a_required_field_cant_be_cleared(self, api_client, seeded_event):
        response = api_client.patch(f"/events/{seeded_event.event_id}", json={"name": None})

        assert response.status_code == 400

    def test_renaming_to_a_taken_name_is_409(self, api_client, seeded_event, new_event):
        api_client.post("/events", json=new_event)

        response = api_client.patch(f"/events/{seeded_event.event_id}", json={"name": new_event["name"]})

        assert response.status_code == 409

    def test_changing_edition_reports_stages_it_lacks(self, api_client, seeded_event):
        response = api_client.patch(f"/events/{seeded_event.event_id}", json={"pipeline_edition": "v4_0"})

        assert response.status_code == 200
        assert response.json()["unregistered_stages"] == ["MotorPower"]

    def test_running_event_cant_be_edited(self, api_client, session_factory, seeded_event):
        add_worker(session_factory, seeded_event.event_id, WorkerStatus.RUNNING)

        response = api_client.patch(f"/events/{seeded_event.event_id}", json={"description": "edited"})

        assert response.status_code == 409

    def test_unknown_event_is_404(self, api_client):
        assert api_client.patch("/events/999", json={"description": "x"}).status_code == 404


class TestDelete:
    def test_deletes_the_event_and_everything_of_it(self, api_client, session_factory, seeded_event):
        add_worker(session_factory, seeded_event.event_id, WorkerStatus.COMPLETED)
        with session_factory() as session:
            session.add(Sample(
                event_id=seeded_event.event_id, signal_id=seeded_event.signal_ids["speed"],
                ts=datetime(2026, 7, 1, 12), value_f64=1.0,
            ))
            session.commit()

        assert api_client.delete(f"/events/{seeded_event.event_id}").status_code == 204

        assert api_client.get(f"/events/{seeded_event.event_id}").status_code == 404
        for model in (Event, Sample, Signal, WorkerRun):
            assert count(session_factory, model) == 0

    def test_running_event_cant_be_deleted(self, api_client, session_factory, seeded_event):
        add_worker(session_factory, seeded_event.event_id, WorkerStatus.RUNNING)

        assert api_client.delete(f"/events/{seeded_event.event_id}").status_code == 409
        assert count(session_factory, Event) == 1


def test_lists_vehicles(api_client, seeded_event):
    vehicles = api_client.get("/vehicles").json()

    assert [vehicle["id"] for vehicle in vehicles] == [seeded_event.vehicle_id]
