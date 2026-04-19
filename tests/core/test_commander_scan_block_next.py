from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from tests._helpers.scan_block_stub_runtime import FakeLogger, load_commander_module, q


REPO_ROOT = Path(__file__).resolve().parents[3]
MODULE = load_commander_module(REPO_ROOT)


class DummySection(SimpleNamespace):
    pass


def test_commander_scan_block_multiple_sections_and_all_kinds():
    commander = MODULE.Commander.__new__(MODULE.Commander)
    sent = {}
    waits = []
    published = []
    commander.client = {"scan_block": object()}
    commander.logger = FakeLogger()
    commander.publisher = {
        "cmd_trans": SimpleNamespace(publish=lambda msg: published.append(msg))
    }

    def fake_send_request(req, client):
        sent["req"] = req
        sent["client"] = client
        return SimpleNamespace(id="scanblock-001")

    commander._send_request = fake_send_request
    commander.wait = lambda *args, **kwargs: waits.append((args, kwargs))

    sections = [
        DummySection(
            kind="initial_standby",
            start=(q(0.0), q(0.0)),
            stop=(q(1.0), q(0.0)),
            speed=q(0.5, "deg/s"),
            margin=q(0.1),
            label="A" * 80,
            line_index=0,
            tight=False,
        ),
        DummySection(
            kind="accelerate",
            start=(q(0.0), q(0.0)),
            stop=(q(1.0), q(0.0)),
            speed=q(0.5, "deg/s"),
            margin=q(0.1),
            label="acc",
            line_index=0,
            tight=False,
        ),
        DummySection(
            kind="line",
            start=(q(0.0), q(0.0)),
            stop=(q(1.0), q(0.0)),
            speed=q(0.5, "deg/s"),
            margin=q(0.1),
            label="line",
            line_index=0,
            tight=True,
        ),
        DummySection(
            kind="turn",
            start=(q(1.1), q(0.0)),
            stop=(q(-0.1), q(0.2)),
            speed=q(0.4, "deg/s"),
            label="turn",
            line_index=1,
            tight=False,
            turn_radius_hint=q(0.05),
        ),
        DummySection(
            kind="final_decelerate",
            start=(q(0.0), q(0.2)),
            stop=(q(1.0), q(0.2)),
            speed=q(0.5, "deg/s"),
            margin=q(0.1),
            label="fin-dec",
            line_index=1,
            tight=False,
        ),
        DummySection(
            kind="final_standby",
            start=(q(1.1), q(0.2)),
            duration=q(2.5, "s"),
            speed=q(0.5, "deg/s"),
            label="final",
            line_index=1,
            tight=False,
        ),
    ]

    out = MODULE.Commander.scan_block(
        commander,
        sections=sections,
        scan_frame="altaz",
        target=(30.0, 45.0, "altaz"),
        offset=(0.1, -0.2, "altaz"),
        unit="deg",
        wait=True,
        direct_mode=True,
        cos_correction=True,
        obsfreq=230.5,
    )

    assert out == "scanblock-001"
    req = sent["req"]
    assert req.unit == "deg"
    assert req.frame == "altaz"
    assert req.lon == [30.0]
    assert req.lat == [45.0]
    assert req.offset_lon == [0.1]
    assert req.offset_lat == [-0.2]
    assert req.offset_frame == "altaz"
    assert req.direct_mode is True
    assert req.cos_correction is True
    assert req.obsfreq == 230.5
    assert [s.kind for s in req.sections] == [
        MODULE.ScanBlockSection.FIRST_STANDBY,
        MODULE.ScanBlockSection.ACCELERATE,
        MODULE.ScanBlockSection.LINE,
        MODULE.ScanBlockSection.TURN,
        MODULE.ScanBlockSection.DECELERATE,
        MODULE.ScanBlockSection.FINAL_STANDBY,
    ]
    assert len(req.sections[0].label) == 64
    assert req.sections[3].turn_radius_hint == 0.05
    assert req.sections[5].duration_hint == 2.5
    assert published and getattr(published[0], "data", None) is True
    assert waits[0] == (("antenna",), {})
    assert waits[1][0] == ("antenna",)
    assert waits[1][1]["mode"] == "control"
    assert waits[1][1]["id"] == "scanblock-001"
