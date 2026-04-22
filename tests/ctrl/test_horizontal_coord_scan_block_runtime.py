from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from tests._helpers.scan_block_stub_runtime import load_horizontal_coord_module

REPO_ROOT = Path(__file__).resolve().parents[3]
MODULE = load_horizontal_coord_module(REPO_ROOT)


class DummyPublisher:
    def __init__(self):
        self.messages = []

    def publish(self, msg):
        self.messages.append(msg)


class DummyStatus:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


def test_horizontal_coord_telemetry_idle_status(monkeypatch):
    monkeypatch.setattr(MODULE, "ControlStatus", DummyStatus)
    monkeypatch.setattr(MODULE.config, "antenna_command_offset_sec", 0.0, raising=False)
    pub = DummyPublisher()
    hc = MODULE.HorizontalCoord.__new__(MODULE.HorizontalCoord)
    hc._idle_exec_id = -1
    hc._current_exec_id = 42
    hc.status_publisher = pub
    hc.last_status = None

    MODULE.HorizontalCoord.telemetry(hc, None)

    msg = pub.messages[-1]
    assert msg.controlled is False
    assert msg.tight is False
    assert msg.id == -1
    assert msg.section_kind == ""
    assert msg.section_label == ""
    assert msg.line_index == -1


def test_horizontal_coord_telemetry_forwards_scan_block_section_fields(monkeypatch):
    monkeypatch.setattr(MODULE, "ControlStatus", DummyStatus)
    pub = DummyPublisher()
    hc = MODULE.HorizontalCoord.__new__(MODULE.HorizontalCoord)
    hc._idle_exec_id = -1
    hc._current_exec_id = 314
    hc.status_publisher = pub
    hc.last_status = None

    status = SimpleNamespace(
        tight=True,
        infinite=False,
        waypoint=False,
        start=123.5,
        kind="line" * 30,
        label="L" * 80,
        line_index=7,
    )
    MODULE.HorizontalCoord.telemetry(hc, status)

    msg = pub.messages[-1]
    assert msg.controlled is True
    assert msg.tight is True
    assert msg.id == 314
    assert msg.interrupt_ok is False
    assert msg.time == 123.5
    assert msg.section_kind == ("line" * 30)[:64]
    assert msg.section_label == ("L" * 80)[:64]
    assert msg.line_index == 7
