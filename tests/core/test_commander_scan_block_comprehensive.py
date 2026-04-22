from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from tests._helpers.scan_block_stub_runtime import FakeLogger, load_commander_module, q


REPO_ROOT = Path(__file__).resolve().parents[3]
MODULE = load_commander_module(REPO_ROOT)


class DummyPublisher:
    def __init__(self):
        self.messages = []
    def publish(self, msg):
        self.messages.append(msg)


class DummySection(SimpleNamespace):
    pass


def test_commander_scan_block_wait_false_skips_control_wait_and_forwards_offset_obsfreq_cos(monkeypatch):
    com = MODULE.Commander.__new__(MODULE.Commander)
    com.logger = FakeLogger()
    publisher = DummyPublisher()
    com.publisher = {"cmd_trans": publisher}
    com.client = {"scan_block": object()}

    call_order = []

    def fake_send_request(req, client):
        call_order.append(("scan_block", req))
        return SimpleNamespace(id="blk-42")

    def fake_wait(target, mode="error", id=None):
        call_order.append(("wait", target, mode, id))

    monkeypatch.setattr(com, "_send_request", fake_send_request, raising=False)
    monkeypatch.setattr(com, "wait", fake_wait)

    section = DummySection(
        kind="line",
        tight=True,
        label="SCI",
        line_index=4,
        start=(q(0.0), q(0.0)),
        stop=(q(1.0), q(0.0)),
        speed=q(0.5, "deg/s"),
        margin=q(0.1),
        duration=None,
        turn_radius_hint=None,
    )

    out = MODULE.Commander.scan_block(
        com,
        sections=[section],
        scan_frame="altaz",
        reference=(30.0, 45.0, "fk5"),
        offset=(0.1, -0.2, "altaz"),
        unit="deg",
        wait=False,
        direct_mode=True,
        cos_correction=True,
        obsfreq=230.5,
    )

    assert out == "blk-42"
    assert call_order == [
        ("scan_block", call_order[0][1]),
        ("wait", "antenna", "error", None),
    ]
    assert len(publisher.messages) == 1
    req = call_order[0][1]
    assert req.direct_mode is True
    assert req.cos_correction is True
    assert req.obsfreq == 230.5
    assert req.offset_lon == [0.1]
    assert req.offset_lat == [-0.2]
    assert req.offset_frame == "altaz"
    assert req.lon == [30.0]
    assert req.lat == [45.0]
    assert req.frame == "fk5"
