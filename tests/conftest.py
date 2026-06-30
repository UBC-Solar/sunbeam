from datetime import datetime
from data_tools.localization import CanonicalName
from state.frame import FrameView
import pytest

@pytest.fixture
def sample_timestamp():
    return datetime(2026, 4, 2, 20, 5)

# workaround so each test can have unique values
@pytest.fixture
def make_frame_view(sample_timestamp):
    def _make(values: dict[CanonicalName, float], timestamp=None):
        return FrameView(
            timestamp=timestamp or sample_timestamp,
            values=values,
        )
    return _make

@pytest.fixture
def array_stage():
    from stage.v3_0.array import Array
    return Array()

@pytest.fixture
def motor_power_stage():
    from stage.v3_0.motor_power import MotorPower
    return MotorPower()


@pytest.fixture
def pack_power_stage():
    from stage.v3_0.pack_power import PackPower
    return PackPower()


@pytest.fixture
def integrated_pack_power_stage():
    from stage.v3_0.integrated_pack_power import IntegratedPackPower
    return IntegratedPackPower()


@pytest.fixture
def energy_from_integrated_power_stage():
    from stage.v3_0.energy_from_integrated_power import EnergyFromIntegratedPower
    return EnergyFromIntegratedPower()


@pytest.fixture
def energy_vol_extrapolated_stage():
    from stage.v3_0.energy_vol_extrapolated import EnergyVOLExtrapolated
    return EnergyVOLExtrapolated()


@pytest.fixture
def latitude_longitude_stage():
    from stage.v3_0.latitude_longitude import LatitudeLongitude
    return LatitudeLongitude() # no arguments passed means no event name