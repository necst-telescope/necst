from types import SimpleNamespace

import pytest
from astropy import units as u

from necst.core import commander as commander_module
from necst.core.commander import Commander, MountPointingInterruptedError
from necst.ctrl.antenna.horizontal_coord import HorizontalCoord
from necst.procedures.observations import optical_pointing as optical_pointing_module
from necst.procedures.observations.optical_pointing import OpticalPointing
from necst.rx.ccd import CCDController


class DummyLogger:
    def debug(self, *args, **kwargs):
        pass

    def info(self, *args, **kwargs):
        pass

    def warning(self, *args, **kwargs):
        pass

    def error(self, *args, **kwargs):
        pass


def test_mount_wait_raises_when_manual_stop_was_seen():
    commander = object.__new__(Commander)
    commander._manual_antenna_stop_seen = True
    commander._manual_antenna_stop_time = 123.0
    commander._raise_if_abort_requested = lambda: None
    commander.logger = DummyLogger()

    with pytest.raises(MountPointingInterruptedError, match="manual antenna stop"):
        commander.wait_mount_point(10.0, 20.0, command_id="cmd-1")


def test_mount_wait_receives_id_returned_by_coordinate_service(monkeypatch):
    waited = {}
    fake = SimpleNamespace(
        _resolve_commander_az_target_mode=lambda **kwargs: "mount",
        _send_request=lambda request, client: SimpleNamespace(id="mount-command-7"),
        client={"raw_coord": object()},
        logger=DummyLogger(),
        _manual_antenna_stop_seen=True,
        _manual_antenna_stop_time=1.0,
    )

    def wait_mount_point(az, el, **kwargs):
        waited.update(az=az, el=el, **kwargs)

    fake.wait_mount_point = wait_mount_point
    monkeypatch.setattr(
        commander_module,
        "assert_mount_az_allowed_when_unwrap_disabled",
        lambda *args, **kwargs: None,
    )

    command_id = Commander.antenna.__wrapped__(
        fake,
        "point",
        target=(10.0, 20.0, "altaz"),
        unit="deg",
        direct_mode=True,
        az_target_mode="mount",
        wait=True,
    )

    assert command_id == "mount-command-7"
    assert waited["command_id"] == "mount-command-7"
    assert fake._manual_antenna_stop_seen is False


def test_ccd_callback_contains_driver_exception(tmp_path):
    class FailingCamera:
        def capture(self, savepath):
            raise RuntimeError("USB failure")

    controller = SimpleNamespace(ccd=FailingCamera(), logger=DummyLogger())
    request = SimpleNamespace(capture=True, savepath=str(tmp_path / "image.jpg"))
    response = SimpleNamespace(captured=True)

    result = CCDController.capture(controller, request, response)

    assert result is response
    assert response.captured is False


def test_direct_mode_weather_update_keeps_latest_measurement():
    finder = SimpleNamespace(
        temperature=0.0,
        pressure=0.0,
        relative_humidity=0.0,
    )
    controller = SimpleNamespace(last_status=None, direct_mode=True, finder=finder)
    weather = SimpleNamespace(temperature=280.0, pressure=900.0, humidity=0.35)

    HorizontalCoord._update_weather(controller, weather)

    assert finder.temperature == 280.0
    assert finder.pressure == 900.0
    assert finder.relative_humidity == 0.35


def test_live_tracking_check_rejects_large_unwrap(monkeypatch):
    class FakeSpec:
        def __init__(self, *args, **kwargs):
            pass

        def to_altaz(self, *args, **kwargs):
            return SimpleNamespace(az=10.0 * u.deg)

    class FakeChecker:
        def __init__(self, *args, **kwargs):
            pass

        def optimize(self, *args, **kwargs):
            return 350.0 * u.deg

    monkeypatch.setattr(optical_pointing_module, "OpticalPointingSpec", FakeSpec)
    monkeypatch.setattr(optical_pointing_module, "DriveLimitChecker", FakeChecker)

    observation = object.__new__(OpticalPointing)
    observation.com = SimpleNamespace(
        antenna=lambda command: SimpleNamespace(lon=0.0, lat=45.0)
    )
    observation.logger = DummyLogger()
    stars = {"ra": [10.0], "dec": [20.0]}

    with pytest.raises(RuntimeError, match="Unsafe optical-pointing Az unwrap"):
        observation._validate_live_tracking_slew(stars, 0)
