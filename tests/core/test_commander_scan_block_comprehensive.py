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


def test_commander_scan_block_prewait_false_skips_antenna_wait(monkeypatch):
    com = MODULE.Commander.__new__(MODULE.Commander)
    com.logger = FakeLogger()
    publisher = DummyPublisher()
    com.publisher = {"cmd_trans": publisher}
    com.client = {"scan_block": object()}

    call_order = []

    def fake_send_request(req, client):
        call_order.append(("scan_block", req))
        return SimpleNamespace(id="blk-99")

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
        unit="deg",
        wait=True,
        prewait=False,
        direct_mode=True,
    )

    assert out == "blk-99"
    assert call_order == [
        ("scan_block", call_order[0][1]),
        ("wait", "antenna", "control", "blk-99"),
    ]
    assert len(publisher.messages) == 1


def test_commander_scan_block_accepts_handoff_turn_and_handoff_standby(monkeypatch):
    com = MODULE.Commander.__new__(MODULE.Commander)
    com.logger = FakeLogger()
    publisher = DummyPublisher()
    com.publisher = {"cmd_trans": publisher}
    com.client = {"scan_block": object()}

    seen = {}

    def fake_send_request(req, client):
        seen["req"] = req
        return SimpleNamespace(id="blk-handoff")

    monkeypatch.setattr(com, "_send_request", fake_send_request, raising=False)
    monkeypatch.setattr(com, "wait", lambda *a, **k: None)

    sections = [
        DummySection(
            kind="handoff_turn", tight=False, label="handoff:0->1", line_index=1,
            start=(q(1.1), q(0.0)), stop=(q(-0.1), q(0.2)),
            speed=q(0.5, "deg/s"), margin=None, duration=None, turn_radius_hint=q(0.1),
        ),
        DummySection(
            kind="handoff_standby", tight=False, label="L1:handoff_standby", line_index=1,
            start=(q(1.0), q(0.2)), stop=(q(0.0), q(0.2)),
            speed=q(0.5, "deg/s"), margin=q(0.1), duration=None, turn_radius_hint=None,
        ),
    ]

    MODULE.Commander.scan_block(
        com,
        sections=sections,
        scan_frame="altaz",
        reference=(30.0, 45.0, "fk5"),
        unit="deg",
        wait=False,
        prewait=False,
    )

    req_sections = seen["req"].sections
    assert req_sections[0].kind == MODULE.ScanBlockSection.HANDOFF_TURN
    assert req_sections[1].kind == MODULE.ScanBlockSection.HANDOFF_STANDBY


def test_commander_scan_block_kind_map_covers_all_supported_wire_kinds(monkeypatch):
    com = MODULE.Commander.__new__(MODULE.Commander)
    com.logger = FakeLogger()
    com.publisher = {"cmd_trans": DummyPublisher()}
    com.client = {"scan_block": object()}

    seen = {}

    def fake_send_request(req, client):
        seen["req"] = req
        return SimpleNamespace(id="blk-kind-map")

    monkeypatch.setattr(com, "_send_request", fake_send_request, raising=False)
    monkeypatch.setattr(com, "wait", lambda *a, **k: None)

    sections = [
        DummySection(
            kind="initial_standby", tight=False, label="init", line_index=-1,
            start=(q(0.0), q(0.0)), stop=(q(0.0), q(0.0)),
            speed=q(0.0, "deg/s"), margin=q(0.0), duration=q(1.0, "s"), turn_radius_hint=None,
        ),
        DummySection(
            kind="accelerate", tight=False, label="acc", line_index=0,
            start=(q(0.0), q(0.0)), stop=(q(1.0), q(0.0)),
            speed=q(0.5, "deg/s"), margin=q(0.1), duration=None, turn_radius_hint=None,
        ),
        DummySection(
            kind="line", tight=True, label="line", line_index=0,
            start=(q(0.0), q(0.0)), stop=(q(1.0), q(0.0)),
            speed=q(0.5, "deg/s"), margin=q(0.1), duration=None, turn_radius_hint=None,
        ),
        DummySection(
            kind="turn", tight=False, label="turn", line_index=1,
            start=(q(1.1), q(0.0)), stop=(q(-0.1), q(0.2)),
            speed=q(0.5, "deg/s"), margin=None, duration=None, turn_radius_hint=q(0.2),
        ),
        DummySection(
            kind="handoff_turn", tight=False, label="handoff", line_index=1,
            start=(q(1.1), q(0.0)), stop=(q(-0.1), q(0.2)),
            speed=q(0.5, "deg/s"), margin=None, duration=None, turn_radius_hint=q(0.2),
        ),
        DummySection(
            kind="decelerate", tight=False, label="dec", line_index=0,
            start=(q(0.0), q(0.0)), stop=(q(1.0), q(0.0)),
            speed=q(0.5, "deg/s"), margin=q(0.1), duration=None, turn_radius_hint=None,
        ),
        DummySection(
            kind="final_decelerate", tight=False, label="fdec", line_index=0,
            start=(q(0.0), q(0.0)), stop=(q(1.0), q(0.0)),
            speed=q(0.5, "deg/s"), margin=q(0.1), duration=None, turn_radius_hint=None,
        ),
        DummySection(
            kind="final_standby", tight=False, label="final", line_index=0,
            start=(q(1.1), q(0.0)), stop=(q(1.1), q(0.0)),
            speed=q(0.0, "deg/s"), margin=q(0.0), duration=q(2.5, "s"), turn_radius_hint=None,
        ),
        DummySection(
            kind="handoff_standby", tight=False, label="handoff-standby", line_index=1,
            start=(q(1.0), q(0.2)), stop=(q(0.0), q(0.2)),
            speed=q(0.5, "deg/s"), margin=q(0.1), duration=None, turn_radius_hint=None,
        ),
    ]

    MODULE.Commander.scan_block(
        com,
        sections=sections,
        scan_frame="altaz",
        reference=(30.0, 45.0, "fk5"),
        unit="deg",
        wait=False,
        prewait=False,
    )

    assert [section.kind for section in seen["req"].sections] == [
        MODULE.ScanBlockSection.FIRST_STANDBY,
        MODULE.ScanBlockSection.ACCELERATE,
        MODULE.ScanBlockSection.LINE,
        MODULE.ScanBlockSection.TURN,
        MODULE.ScanBlockSection.HANDOFF_TURN,
        MODULE.ScanBlockSection.DECELERATE,
        MODULE.ScanBlockSection.DECELERATE,
        MODULE.ScanBlockSection.FINAL_STANDBY,
        MODULE.ScanBlockSection.HANDOFF_STANDBY,
    ]
