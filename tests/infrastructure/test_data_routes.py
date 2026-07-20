import json
from datetime import datetime, timedelta, timezone

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("docker")

from tests.infrastructure.test_data_service import BASE, seed_samples


class TestSignalsListing:
    def test_lists_signals_with_metadata(self, api_client, seeded_event):
        response = api_client.get(f"/events/{seeded_event.event_name}/signals")

        assert response.status_code == 200
        signals = {signal["name"]: signal for signal in response.json()}
        assert set(signals) == {"speed", "power"}
        assert signals["speed"]["frequency"] == 10.0
        assert signals["speed"]["unit"] == "unit"

    def test_unknown_event_is_404(self, api_client):
        assert api_client.get("/events/nope/signals").status_code == 404


class TestQueryEndpoint:
    def url(self, seeded_event, signal="speed"):
        return f"/events/{seeded_event.event_name}/signals/{signal}/data"

    def test_between_mode_with_metadata(self, api_client, session_factory, seeded_event):
        seed_samples(session_factory, seeded_event)

        response = api_client.get(
            self.url(seeded_event),
            params={
                "start": BASE.isoformat(),
                "end": (BASE + timedelta(seconds=3)).isoformat(),
            },
        )

        assert response.status_code == 200
        body = response.json()
        assert body["values"] == [0.0, 1.0, 2.0]
        assert body["count"] == 3
        assert body["truncated"] is False
        assert body["signal"] == "speed"
        assert body["unit"] == "unit"
        assert body["frequency"] == 10.0
        assert body["timestamps"] == sorted(body["timestamps"])

    def test_since_mode(self, api_client, session_factory, seeded_event):
        # "Since" runs until real now, so seed relative to real now.
        base = datetime.now(timezone.utc) - timedelta(seconds=20)
        seed_samples(session_factory, seeded_event, start=base)

        response = api_client.get(
            self.url(seeded_event),
            params={"start": (base + timedelta(seconds=7)).isoformat()},
        )

        assert response.status_code == 200
        assert response.json()["values"] == [7.0, 8.0, 9.0]

    def test_last_seconds_mode(self, api_client, session_factory, seeded_event):
        now = datetime.now(timezone.utc)
        seed_samples(
            session_factory, seeded_event, count=5, start=now - timedelta(seconds=4)
        )

        response = api_client.get(
            self.url(seeded_event), params={"last_seconds": 3600}
        )

        assert response.status_code == 200
        assert response.json()["count"] == 5

    def test_limit_truncates(self, api_client, session_factory, seeded_event):
        seed_samples(session_factory, seeded_event, count=10)

        response = api_client.get(
            self.url(seeded_event),
            params={
                "start": BASE.isoformat(),
                "end": (BASE + timedelta(seconds=60)).isoformat(),
                "limit": 4,
            },
        )

        body = response.json()
        assert body["truncated"] is True
        assert body["values"] == [6.0, 7.0, 8.0, 9.0]

    def test_both_modes_is_422(self, api_client, seeded_event):
        response = api_client.get(
            self.url(seeded_event),
            params={"start": BASE.isoformat(), "last_seconds": 10},
        )

        assert response.status_code == 422
        assert "cannot be combined" in response.json()["detail"]

    def test_no_mode_is_422(self, api_client, seeded_event):
        assert api_client.get(self.url(seeded_event)).status_code == 422

    def test_naive_start_is_422(self, api_client, seeded_event):
        response = api_client.get(
            self.url(seeded_event), params={"start": "2026-07-20T11:00:00"}
        )

        assert response.status_code == 422
        assert "timezone-aware" in response.json()["detail"]

    def test_unknown_signal_is_404(self, api_client, seeded_event):
        response = api_client.get(
            self.url(seeded_event, signal="nope"), params={"last_seconds": 10}
        )

        assert response.status_code == 404


def parse_sse(messages: list[str]) -> list[tuple[str, str | None, dict]]:
    """Parse (event, id, payload) triples from formatted SSE messages."""
    events = []
    for message in messages:
        current_event, current_id, current_data = None, None, None
        for line in message.splitlines():
            if line.startswith("event:"):
                current_event = line.split(":", 1)[1].strip()
            elif line.startswith("id:"):
                current_id = line.split(":", 1)[1].strip()
            elif line.startswith("data:"):
                current_data = json.loads(line.split(":", 1)[1])
        if current_event is not None:
            events.append((current_event, current_id, current_data))
    return events


class TestStreamEndpoint:
    def test_sse_framing_meta_then_data_with_cursor(self, session_factory, seeded_event):
        # The TestClient buffers a response to completion, so an infinite SSE
        # stream cannot be consumed through it; exercise the exact protocol
        # (service batches -> wire format) directly instead. The 404/422
        # behavior below still goes through the full HTTP stack.
        from server.routes.data import _format_sse
        from server.services.data_service import stream_batches

        seed_samples(session_factory, seeded_event, count=3)
        since_us = int(BASE.timestamp() * 1_000_000) - 1

        messages = [
            _format_sse(kind, payload, cursor)
            for kind, payload, cursor in stream_batches(
                session_factory,
                event_name=seeded_event.event_name,
                signal_names=["speed", "power"],
                since_us=since_us,
                max_batches=1,
                sleep=lambda s: None,
            )
        ]

        (meta_kind, meta_id, meta), (data_kind, cursor, payload) = parse_sse(messages)
        assert meta_kind == "meta"
        assert meta["speed"]["frequency"] == 10.0
        assert data_kind == "data"
        assert payload["speed"]["values"] == [0.0, 1.0, 2.0]
        assert payload["power"]["values"] == []
        assert int(cursor) > since_us

    def test_idle_and_keepalive_framing(self):
        from server.routes.data import _format_sse

        assert _format_sse("idle", None, None) == ""
        assert _format_sse("keepalive", None, None) == ": keepalive\n\n"

    def test_stream_route_headers(self, api_client, session_factory, seeded_event):
        # The route itself is exercised up to (and including) response
        # construction via the 404/422 paths; verify content type through a
        # HEAD-of-stream request is impossible with the buffering TestClient,
        # so assert the error paths carry normal JSON instead.
        response = api_client.get(
            f"/events/{seeded_event.event_name}/data/stream",
            params={"signals": "nope"},
        )
        assert response.headers["content-type"].startswith("application/json")

    def test_unknown_signal_is_404_not_mid_stream_error(self, api_client, seeded_event):
        response = api_client.get(
            f"/events/{seeded_event.event_name}/data/stream",
            params={"signals": "nope"},
        )

        assert response.status_code == 404

    def test_empty_signal_list_is_422(self, api_client, seeded_event):
        response = api_client.get(
            f"/events/{seeded_event.event_name}/data/stream",
            params={"signals": " , "},
        )

        assert response.status_code == 422


class TestViewerPage:
    def test_viewer_serves_reference_client(self, api_client):
        response = api_client.get("/debug/viewer")

        assert response.status_code == 200
        assert "EventSource" in response.text
        assert "/data/stream" in response.text
