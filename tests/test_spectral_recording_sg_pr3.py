"""PR3 tests for SG apply/verify helpers."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RX = ROOT / "necst" / "rx"
sys.path.insert(0, str(RX))
MODULE_PATH = RX / "spectral_recording_sg.py"
SPEC = importlib.util.spec_from_file_location("spectral_recording_sg", MODULE_PATH)
assert SPEC and SPEC.loader
sg = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(sg)


def _profile():
    return {
        "sg_devices": {
            "sg_lsb": {
                "control_adapter": "necst_commander_signal_generator_set_v1",
                "sg_set_frequency_hz": 4.0e9,
                "sg_set_power_dbm": 15.0,
                "frequency_tolerance_hz": 10.0,
                "power_tolerance_db": 0.5,
                "output_required": True,
            },
            "sg_off": {
                "control_adapter": "necst_commander_signal_generator_set_v1",
                "sg_set_frequency_hz": 1.0e9,
                "sg_set_power_dbm": 0.0,
                "frequency_tolerance_hz": 10.0,
                "power_tolerance_db": 0.5,
                "output_required": False,
                "output_policy": "require_off",
            },
        }
    }


class FakeCommander:
    def __init__(self):
        self.calls = []

    def signal_generator(self, cmd, **kwargs):
        self.calls.append((cmd, kwargs))


def test_apply_plan_set_and_stop():
    plan = sg.build_sg_apply_plan(_profile())
    com = FakeCommander()
    sg.apply_sg_plan_entry(com, plan["sg_lsb"])
    sg.apply_sg_plan_entry(com, plan["sg_off"])
    assert com.calls[0][0] == "set"
    assert com.calls[0][1]["GHz"] == 4.0
    assert com.calls[0][1]["dBm"] == 15.0
    assert com.calls[0][1]["id"] == "sg_lsb"
    assert com.calls[1] == ("stop", {"id": "sg_off"})


def test_verify_readback_accepts_fresh_device_readback():
    entry = sg.build_sg_apply_plan(_profile(), "sg_lsb")["sg_lsb"]
    result = sg.verify_sg_readback(
        sg_id="sg_lsb",
        entry=entry,
        readback={
            "id": "sg_lsb",
            "freq": 4.000000005,  # GHz
            "power": 15.1,
            "time": 200.0,
            "output_status": True,
            "readback_source": "device_readback",
        },
        verify_started_at=100.0,
    )
    assert result["success"]
    assert abs(result["frequency_error_hz"] - 5.0) < 1.0e-6


def test_verify_rejects_id_mismatch_and_stale_readback():
    entry = sg.build_sg_apply_plan(_profile(), "sg_lsb")["sg_lsb"]
    try:
        sg.verify_sg_readback(
            sg_id="sg_lsb",
            entry=entry,
            readback={
                "id": "other",
                "frequency_hz": 4.0e9,
                "power_dbm": 15.0,
                "time": 50.0,
                "output_status": True,
            },
            verify_started_at=100.0,
        )
    except sg.SpectralRecordingSGValidationError as exc:
        text = str(exc)
        assert "id mismatch" in text
        assert "stale readback" in text
    else:  # pragma: no cover
        raise AssertionError("Expected stale/id mismatch failure")


def test_verify_rejects_tolerance_and_command_echo_by_default():
    entry = sg.build_sg_apply_plan(_profile(), "sg_lsb")["sg_lsb"]
    try:
        sg.verify_sg_readback(
            sg_id="sg_lsb",
            entry=entry,
            readback={
                "id": "sg_lsb",
                "frequency_hz": 4.0e9 + 1000.0,
                "power_dbm": 15.0,
                "time": 200.0,
                "output_status": True,
                "readback_source": "command_echo_detected",
            },
            verify_started_at=100.0,
        )
    except sg.SpectralRecordingSGValidationError as exc:
        text = str(exc)
        assert "frequency out of tolerance" in text
        assert "strict verify requires device_readback" in text
    else:  # pragma: no cover
        raise AssertionError("Expected tolerance/echo failure")


def test_output_required_false_require_off():
    entry = sg.build_sg_apply_plan(_profile(), "sg_off")["sg_off"]
    try:
        sg.verify_sg_readback(
            sg_id="sg_off",
            entry=entry,
            readback={
                "id": "sg_off",
                "frequency_hz": 1.0e9,
                "power_dbm": 0.0,
                "time": 200.0,
                "output_status": True,
            },
            verify_started_at=100.0,
        )
    except sg.SpectralRecordingSGValidationError as exc:
        assert "require_off" in str(exc)
    else:  # pragma: no cover
        raise AssertionError("Expected output-off failure")


class _PollingCommander:
    def __init__(self, readbacks):
        self.readbacks = list(readbacks)
        self.calls = 0
    def signal_generator(self, cmd, **kwargs):
        assert cmd == "?"
        self.calls += 1
        if self.readbacks:
            return self.readbacks.pop(0)
        return {"id": kwargs.get("id", ""), "frequency_hz": 0.0, "time": 0.0}


def test_poll_sg_readback_waits_for_fresh_matching_message():
    entry = sg.build_sg_apply_plan(_profile(), "sg_lsb")["sg_lsb"]
    started = sg.pytime.time()
    commander = _PollingCommander([
        {"id": "sg_lsb", "frequency_hz": 4.0e9, "power_dbm": 15.0, "time": started - 1.0, "output_status": True},
        {"id": "other", "frequency_hz": 4.0e9, "power_dbm": 15.0, "time": started + 0.1, "output_status": True},
        {"id": "sg_lsb", "frequency_hz": 4.0e9, "power_dbm": 15.0, "time": started + 0.1, "output_status": True},
    ])
    result = sg.poll_sg_readback(
        commander,
        sg_id="sg_lsb",
        entry=entry,
        verify_started_at=started,
        timeout_sec=1.0,
        poll_interval_sec=0.0,
    )
    assert result["success"] is True
    assert result["attempts"] == 3
    assert commander.calls == 3


if __name__ == "__main__":
    test_apply_plan_set_and_stop()
    test_verify_readback_accepts_fresh_device_readback()
    test_verify_rejects_id_mismatch_and_stale_readback()
    test_verify_rejects_tolerance_and_command_echo_by_default()
    test_output_required_false_require_off()
    test_poll_sg_readback_waits_for_fresh_matching_message()
    print("all spectral_recording_sg PR3 tests passed")
