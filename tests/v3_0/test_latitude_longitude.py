from data_tools.localization import CanonicalName
from state.frame import FrameView
from stage.v3_0.latitude_longitude import LatitudeLongitude
import pytest


class TestLatitudeLongitudeStage:
    def test_no_event_bounds_are_none(self, latitude_longitude_stage):
        assert latitude_longitude_stage.min_lat is None
        assert latitude_longitude_stage.max_lat is None
        assert latitude_longitude_stage.min_lon is None
        assert latitude_longitude_stage.max_lon is None

    def test_no_event_all_coords_pass(self, latitude_longitude_stage, make_frame_view):
        frame = make_frame_view({
            CanonicalName.LatitudeRaw: 49.0,
            CanonicalName.LongitudeRaw: -123.0,
        })
        result = latitude_longitude_stage.run(frame)
        assert result.read(CanonicalName.LatitudeFiltered) == pytest.approx(49.0)
        assert result.read(CanonicalName.LongitudeFiltered) == pytest.approx(-123.0)

    def test_in_bounds_no_bounds_configured(self, latitude_longitude_stage):
        assert latitude_longitude_stage.in_bounds(-999, -999) is True
        assert latitude_longitude_stage.in_bounds(0, 0) is True
        assert latitude_longitude_stage.in_bounds(999, 999) is True

    def test_in_bounds_within_range(self, latitude_longitude_stage):
        latitude_longitude_stage.min_lat = 48.0
        latitude_longitude_stage.max_lat = 50.0
        latitude_longitude_stage.min_lon = -124.0
        latitude_longitude_stage.max_lon = -122.0
        assert latitude_longitude_stage.in_bounds(49.0, -123.0) is True

    def test_in_bounds_on_boundary(self, latitude_longitude_stage):
        latitude_longitude_stage.min_lat = 48.0
        latitude_longitude_stage.max_lat = 50.0
        latitude_longitude_stage.min_lon = -124.0
        latitude_longitude_stage.max_lon = -122.0
        assert latitude_longitude_stage.in_bounds(48.0, -124.0) is True
        assert latitude_longitude_stage.in_bounds(50.0, -122.0) is True

    def test_in_bounds_lat_below_min(self, latitude_longitude_stage):
        latitude_longitude_stage.min_lat = 48.0
        latitude_longitude_stage.max_lat = 50.0
        latitude_longitude_stage.min_lon = -124.0
        latitude_longitude_stage.max_lon = -122.0
        assert latitude_longitude_stage.in_bounds(47.9, -123.0) is False

    def test_in_bounds_lat_above_max(self, latitude_longitude_stage):
        latitude_longitude_stage.min_lat = 48.0
        latitude_longitude_stage.max_lat = 50.0
        latitude_longitude_stage.min_lon = -124.0
        latitude_longitude_stage.max_lon = -122.0
        assert latitude_longitude_stage.in_bounds(50.1, -123.0) is False

    def test_in_bounds_lon_below_min(self, latitude_longitude_stage):
        latitude_longitude_stage.min_lat = 48.0
        latitude_longitude_stage.max_lat = 50.0
        latitude_longitude_stage.min_lon = -124.0
        latitude_longitude_stage.max_lon = -122.0
        assert latitude_longitude_stage.in_bounds(49.0, -124.1) is False

    def test_in_bounds_lon_above_max(self, latitude_longitude_stage):
        latitude_longitude_stage.min_lat = 48.0
        latitude_longitude_stage.max_lat = 50.0
        latitude_longitude_stage.min_lon = -124.0
        latitude_longitude_stage.max_lon = -122.0
        assert latitude_longitude_stage.in_bounds(49.0, -121.9) is False

    def test_run_coords_in_bounds_written(self, latitude_longitude_stage, make_frame_view):
        latitude_longitude_stage.min_lat = 48.0
        latitude_longitude_stage.max_lat = 50.0
        latitude_longitude_stage.min_lon = -124.0
        latitude_longitude_stage.max_lon = -122.0

        frame = make_frame_view({
            CanonicalName.LatitudeRaw: 49.0,
            CanonicalName.LongitudeRaw: -123.0,
        })
        result = latitude_longitude_stage.run(frame)
        assert result.read(CanonicalName.LatitudeFiltered) == pytest.approx(49.0)
        assert result.read(CanonicalName.LongitudeFiltered) == pytest.approx(-123.0)

    def test_negative_lat_lon_bounds(self, make_frame_view):
        stage = LatitudeLongitude(event_name=None)
        stage.min_lat = -34.0
        stage.max_lat = -33.0
        stage.min_lon = 18.0
        stage.max_lon = 19.0

        frame = make_frame_view({
            CanonicalName.LatitudeRaw: -33.5,
            CanonicalName.LongitudeRaw: 18.5,
        })
        result = stage.run(frame)
        assert result.read(CanonicalName.LatitudeFiltered) == pytest.approx(-33.5)
        assert result.read(CanonicalName.LongitudeFiltered) == pytest.approx(18.5)

    def test_negative_lat_lon_out_of_bounds(self, make_frame_view):
        stage = LatitudeLongitude(event_name=None)
        stage.min_lat = -34.0
        stage.max_lat = -33.0
        stage.min_lon = 18.0
        stage.max_lon = 19.0

        frame = make_frame_view({
            CanonicalName.LatitudeRaw: -35.0,
            CanonicalName.LongitudeRaw: 18.5,
        })
        result = stage.run(frame)
        signals = [s for s, _ in result]
        assert CanonicalName.LatitudeFiltered not in signals
        assert CanonicalName.LongitudeFiltered not in signals

    def test_run_coords_out_of_bounds_skipped(self, latitude_longitude_stage, make_frame_view):
        latitude_longitude_stage.min_lat = 48.0
        latitude_longitude_stage.max_lat = 50.0
        latitude_longitude_stage.min_lon = -124.0
        latitude_longitude_stage.max_lon = -122.0

        frame = make_frame_view({
            CanonicalName.LatitudeRaw: 47.0,
            CanonicalName.LongitudeRaw: -123.0,
        })
        result = latitude_longitude_stage.run(frame)
        signals = [s for s, _ in result]
        assert CanonicalName.LatitudeFiltered not in signals
        assert CanonicalName.LongitudeFiltered not in signals

    def test_run_lat_out_of_bounds_skipped(self, latitude_longitude_stage, make_frame_view):
        latitude_longitude_stage.min_lat = 48.0
        latitude_longitude_stage.max_lat = 50.0
        latitude_longitude_stage.min_lon = -124.0
        latitude_longitude_stage.max_lon = -122.0

        frame = make_frame_view({
            CanonicalName.LatitudeRaw: 51.0,
            CanonicalName.LongitudeRaw: -123.0,
        })
        result = latitude_longitude_stage.run(frame)
        signals = [s for s, _ in result]
        assert CanonicalName.LatitudeFiltered not in signals
        assert CanonicalName.LongitudeFiltered not in signals

    def test_run_lon_out_of_bounds_skipped(self, latitude_longitude_stage, make_frame_view):
        latitude_longitude_stage.min_lat = 48.0
        latitude_longitude_stage.max_lat = 50.0
        latitude_longitude_stage.min_lon = -124.0
        latitude_longitude_stage.max_lon = -122.0

        frame = make_frame_view({
            CanonicalName.LatitudeRaw: 49.0,
            CanonicalName.LongitudeRaw: -121.0,
        })
        result = latitude_longitude_stage.run(frame)
        signals = [s for s, _ in result]
        assert CanonicalName.LatitudeFiltered not in signals
        assert CanonicalName.LongitudeFiltered not in signals

    def test_run_with_event_name_and_temp_file(self, tmp_path, monkeypatch):
        event_name = "test_event"
        coords_dir = tmp_path / "localization" / event_name
        coords_dir.mkdir(parents=True)
        coords_file = coords_dir / "coords.toml"
        coords_file.write_text("coordinates = [[48.5, 123.5], [49.5, 124.5]]")

        monkeypatch.setattr(
            "stage.v3_0.latitude_longitude.COORDINATES_DIR", tmp_path / "localization"
        )

        stage = LatitudeLongitude(event_name=event_name)

        assert stage.min_lat == pytest.approx(48.49)
        assert stage.max_lat == pytest.approx(49.51)
        assert stage.min_lon == pytest.approx(123.49)
        assert stage.max_lon == pytest.approx(124.51)

        assert stage.in_bounds(49.0, 124.0)
        assert not stage.in_bounds(48.3, 124.0)
        assert not stage.in_bounds(49.0, 123.3)

    def test_event_name_file_not_found_falls_back(self, tmp_path, monkeypatch):
        monkeypatch.setattr(
            "stage.v3_0.latitude_longitude.COORDINATES_DIR", tmp_path / "localization"
        )

        stage = LatitudeLongitude(event_name="nonexistent_event")

        assert stage.min_lat is None
        assert stage.max_lat is None
        assert stage.min_lon is None
        assert stage.max_lon is None
        assert stage.in_bounds(0, 0) is True

    def test_missing_gps_lat_raises_key_error(self, latitude_longitude_stage, make_frame_view):
        frame = make_frame_view({CanonicalName.LongitudeRaw: -123.0})
        with pytest.raises(KeyError):
            latitude_longitude_stage.run(frame)

    def test_missing_gps_lon_raises_key_error(self, latitude_longitude_stage, make_frame_view):
        frame = make_frame_view({CanonicalName.LatitudeRaw: 49.0})
        with pytest.raises(KeyError):
            latitude_longitude_stage.run(frame)

    def test_both_signals_missing_raises_key_error(
        self, latitude_longitude_stage, make_frame_view
    ):
        frame = make_frame_view({})
        with pytest.raises(KeyError):
            latitude_longitude_stage.run(frame)

    def test_output_frame_preserves_timestamp(
        self, latitude_longitude_stage, make_frame_view, sample_timestamp
    ):
        frame = make_frame_view(
            values={
                CanonicalName.LatitudeRaw: 49.0,
                CanonicalName.LongitudeRaw: -123.0,
            },
            timestamp=sample_timestamp,
        )
        result = latitude_longitude_stage.run(frame)
        assert result.timestamp == sample_timestamp

    def test_output_contains_both_signals(self, latitude_longitude_stage, make_frame_view):
        frame = make_frame_view({
            CanonicalName.LatitudeRaw: 49.0,
            CanonicalName.LongitudeRaw: -123.0,
        })
        result = latitude_longitude_stage.run(frame)
        signals = [s for s, _ in result]
        assert CanonicalName.LatitudeFiltered in signals
        assert CanonicalName.LongitudeFiltered in signals