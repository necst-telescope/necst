from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from tests._helpers.scan_block_stub_runtime import (
    FakeLogger,
    load_commander_module,
    load_horizontal_coord_module,
    q,
)


REPO_ROOT = Path(__file__).resolve().parents[3]


class DummyPublisher:
    def __init__(self):
        self.messages = []

    def publish(self, msg):
        self.messages.append(msg)


class DummySection(SimpleNamespace):
    pass


class DummyLock:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class DummyGeneratorManager:
    def __init__(self):
        self.attached = None

    def attach(self, generator):
        self.attached = generator


class DummyFinder:
    def __init__(self):
        self.calls = []
        self.direct_mode = False
        self.obsfreq = None

    def scan_block(self, *args, **kwargs):
        self.calls.append((args, kwargs))
        return "GEN"


def test_commander_to_horizontal_roundtrip_preserves_handoff_section_kinds(monkeypatch):
    commander_module = load_commander_module(REPO_ROOT)
    com = commander_module.Commander.__new__(commander_module.Commander)
    com.logger = FakeLogger()
    com.publisher = {"cmd_trans": DummyPublisher()}
    com.client = {"scan_block": object()}

    seen = {}

    def fake_send_request(req, client):
        seen["req"] = req
        return SimpleNamespace(id="blk-roundtrip")

    monkeypatch.setattr(com, "_send_request", fake_send_request, raising=False)
    monkeypatch.setattr(com, "wait", lambda *a, **k: None)

    commander_module.Commander.scan_block(
        com,
        sections=[
            DummySection(
                kind="handoff_turn",
                tight=False,
                label="handoff:0->1",
                line_index=1,
                start=(q(1.1), q(0.0)),
                stop=(q(-0.1), q(0.2)),
                speed=q(0.5, "deg/s"),
                margin=None,
                duration=None,
                turn_radius_hint=q(0.1),
            ),
            DummySection(
                kind="handoff_standby",
                tight=False,
                label="L1:handoff_standby",
                line_index=1,
                start=(q(1.0), q(0.2)),
                stop=(q(0.0), q(0.2)),
                speed=q(0.5, "deg/s"),
                margin=q(0.1),
                duration=None,
                turn_radius_hint=None,
            ),
        ],
        scan_frame="altaz",
        reference=(30.0, 45.0, "fk5"),
        unit="deg",
        wait=False,
        prewait=False,
    )

    req = seen["req"]
    assert [section.kind for section in req.sections] == [
        commander_module.ScanBlockSection.HANDOFF_TURN,
        commander_module.ScanBlockSection.HANDOFF_STANDBY,
    ]

    horizontal_module = load_horizontal_coord_module(REPO_ROOT)
    hc = horizontal_module.HorizontalCoord.__new__(horizontal_module.HorizontalCoord)
    hc.finder = DummyFinder()
    hc.executing_generator = DummyGeneratorManager()
    hc._gen_lock = DummyLock()
    hc.direct_mode = False

    req.direct_mode = False
    req.obsfreq = 0.0
    req.name = ""
    req.offset_lon = []
    req.offset_lat = []
    req.offset_frame = ""
    req.cos_correction = False
    horizontal_module.HorizontalCoord._parse_scan_block_cmd(hc, req)

    args, kwargs = hc.finder.calls[0]
    assert args == (30.0, 45.0, "fk5")
    assert [section.kind for section in kwargs["sections"]] == [
        "handoff_turn",
        "handoff_standby",
    ]
