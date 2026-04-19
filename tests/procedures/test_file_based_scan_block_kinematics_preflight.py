from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from tests._helpers.scan_block_stub_runtime import FakeLogger, load_file_based_module, q

REPO_ROOT = Path(__file__).resolve().parents[3]
MODULE = load_file_based_module(REPO_ROOT)
ObservationMode = MODULE.ObservationMode


class DummyCommander:
    def __init__(self):
        self.calls = []

    def antenna(self, mode, **kwargs):
        self.calls.append(("antenna", mode, kwargs))

    def metadata(self, action, **kwargs):
        self.calls.append(("metadata", action, kwargs))

    def scan_block(self, **kwargs):
        self.calls.append(("scan_block", kwargs))


class Waypoint:
    def __init__(self, wid: str):
        self.id = wid
        self.mode = ObservationMode.ON
        self.is_scan = True
        self.scan_frame = "altaz"
        self.start = (q(0.0), q(0.0))
        self.stop = (q(1.0), q(0.0))
        self.speed = q(0.5, "deg/s")
        self.reference = (q(30.0), q(45.0), "altaz")
        self.target = None
        self.name_query = False
        self.integration = SimpleNamespace(to_value=lambda unit: 1.0)
        self.with_offset = False


def _make_obs():
    obs = MODULE.OTF.__new__(MODULE.OTF)
    obs.com = DummyCommander()
    obs.logger = FakeLogger()
    obs.observation_type = "OTF"
    return obs


def test_preflight_logs_single_line_edge_slowdown_and_allows_execution(monkeypatch):
    obs = _make_obs()
    line = SimpleNamespace(
        start=(q(0.0), q(0.0)),
        stop=(q(1.0), q(0.0)),
        speed=q(0.7, "deg/s"),
        margin=q(0.1),
        label="L0",
        line_index=0,
    )
    monkeypatch.setattr(
        MODULE.OTF,
        "_make_scan_block_line",
        lambda self, wp, frag, margin_deg, line_index: line,
    )
    monkeypatch.setattr(
        MODULE, "build_scan_block_sections", lambda lines, **kwargs: ["SEC"]
    )
    monkeypatch.setattr(
        MODULE.OTF, "_move_to_scan_block_entry", lambda self, wp, line, cos_scan: None
    )
    monkeypatch.setattr(
        MODULE,
        "plan_scan_block_kinematics",
        lambda lines: {
            "limits": SimpleNamespace(max_acceleration=q(1.6, "deg/s^2")),
            "lines": [
                {
                    "line_index": 0,
                    "label": "L0",
                    "required_acceleration": q(2.45, "deg/s^2"),
                    "peak_acceleration": q(1.60, "deg/s^2"),
                    "peak_jerk": q(4.2, "deg/s^3"),
                    "duration_scale": q(1.5, ""),
                    "within_limits": True,
                }
            ],
            "turns": [],
        },
    )
    obs._run_on_scan_block(
        [Waypoint("L0")],
        scan_frags=[1],
        cos_scan=False,
        margin_deg=0.1,
        include_final_standby=False,
        final_standby_duration_sec=1.0,
    )
    assert any(
        "line-edge slowed for kinematic limits" in msg for msg in obs.logger.infos
    )
    assert [c[0] for c in obs.com.calls] == ["metadata", "scan_block", "metadata"]


def test_preflight_logs_turn_slowdown_and_allows_execution(monkeypatch):
    obs = _make_obs()
    line0 = SimpleNamespace(
        start=(q(0.0), q(0.0)),
        stop=(q(1.0), q(0.0)),
        speed=q(0.5, "deg/s"),
        margin=q(0.1),
        label="L0",
        line_index=0,
    )
    line1 = SimpleNamespace(
        start=(q(1.0), q(0.2)),
        stop=(q(0.0), q(0.2)),
        speed=q(0.5, "deg/s"),
        margin=q(0.1),
        label="L1",
        line_index=1,
    )
    monkeypatch.setattr(
        MODULE.OTF,
        "_make_scan_block_line",
        lambda self, wp, frag, margin_deg, line_index: [line0, line1][line_index],
    )
    monkeypatch.setattr(
        MODULE, "build_scan_block_sections", lambda lines, **kwargs: ["SEC"]
    )
    monkeypatch.setattr(
        MODULE.OTF, "_move_to_scan_block_entry", lambda self, wp, line, cos_scan: None
    )
    monkeypatch.setattr(
        MODULE,
        "plan_scan_block_kinematics",
        lambda lines: {
            "limits": SimpleNamespace(max_acceleration=q(1.6, "deg/s^2")),
            "lines": [
                {
                    "line_index": 0,
                    "label": "L0",
                    "required_acceleration": q(2.34, "deg/s^2"),
                    "peak_acceleration": q(1.5, "deg/s^2"),
                    "duration_scale": q(1.25, ""),
                    "within_limits": True,
                },
                {
                    "line_index": 1,
                    "label": "L1",
                    "required_acceleration": q(2.34, "deg/s^2"),
                    "peak_acceleration": q(1.5, "deg/s^2"),
                    "duration_scale": q(1.25, ""),
                    "within_limits": True,
                },
            ],
            "turns": [
                {
                    "from_line_index": 0,
                    "to_line_index": 1,
                    "duration_scale": q(1.8, ""),
                    "peak_speed": q(1.5, "deg/s"),
                    "peak_acceleration": q(1.55, "deg/s^2"),
                }
            ],
        },
    )
    obs._run_on_scan_block(
        [Waypoint("L0"), Waypoint("L1")],
        scan_frags=[1, -1],
        cos_scan=False,
        margin_deg=0.1,
        include_final_standby=False,
        final_standby_duration_sec=1.0,
    )
    assert any(
        "line-edge slowed for kinematic limits" in msg for msg in obs.logger.infos
    )
    assert any("turn slowed for kinematic limits" in msg for msg in obs.logger.infos)
    assert [c[0] for c in obs.com.calls] == ["metadata", "scan_block", "metadata"]
