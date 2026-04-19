from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from tests._helpers.scan_block_stub_runtime import FakeLogger, FakeUnit, load_file_based_module, q


REPO_ROOT = Path(__file__).resolve().parents[3]
MODULE = load_file_based_module(REPO_ROOT)
ObservationMode = MODULE.ObservationMode


def qdeg(value: float):
    out = q(value)
    out.unit = FakeUnit("deg")
    return out


class DummyCommander:
    def __init__(self):
        self.calls = []
    def record(self, *args, **kwargs):
        self.calls.append(("record", args, kwargs))
    def antenna(self, mode, **kwargs):
        self.calls.append(("antenna", mode, kwargs))
    def metadata(self, action, **kwargs):
        self.calls.append(("metadata", action, kwargs))
    def scan_block(self, **kwargs):
        self.calls.append(("scan_block", kwargs))


class DummySpec(list):
    def __init__(self, waypoints, **params):
        super().__init__(waypoints)
        self.parameters = params
        self.bydirectional = 0
        self.reset_scan = 0
        self.scan_direction = "x"
        self.relative = True
        self.delta_lambda = SimpleNamespace(value=1.0)
        self.delta_beta = SimpleNamespace(value=1.0)
        self.lambda_off = SimpleNamespace(value=1.0)
        self.beta_off = SimpleNamespace(value=1.0)
        self.lambda_on = SimpleNamespace(value=0.0)
        self.beta_on = SimpleNamespace(value=0.0)


class ScanWaypoint:
    def __init__(self, wid: str, *, start=(0.0, 0.0), stop=(1.0, 0.0)):
        self.id = wid
        self.mode = ObservationMode.ON
        self.is_scan = True
        self.scan_frame = "altaz"
        self.start = (qdeg(start[0]), qdeg(start[1]))
        self.stop = (qdeg(stop[0]), qdeg(stop[1]))
        self.speed = q(0.5, "deg/s")
        self.reference = (qdeg(30.0), qdeg(45.0), "altaz")
        self.target = None
        self.name_query = False
        self.integration = SimpleNamespace(to_value=lambda unit: 1.0)
        self.with_offset = True
        self.offset = (qdeg(0.25), qdeg(-0.125), "altaz")


class PointWaypoint:
    def __init__(self, wid: str):
        self.id = wid
        self.mode = ObservationMode.ON
        self.is_scan = False
        self.scan_frame = "altaz"
        self.reference = (qdeg(30.0), qdeg(45.0), "altaz")
        self.target = None
        self.name_query = False
        self.integration = SimpleNamespace(to_value=lambda unit: 2.0)
        self.with_offset = True
        self.offset = (qdeg(0.1), qdeg(0.2), "altaz")


def _make_obs(spec):
    obs = MODULE.RadioPointing.__new__(MODULE.RadioPointing)
    obs.obsspec = spec
    obs.com = DummyCommander()
    obs.logger = FakeLogger()
    obs.observation_type = "RadioPointing"
    obs.hot = lambda *a, **k: None
    obs.off = lambda *a, **k: None
    obs.on = lambda *a, **k: None
    return obs


def test_radio_pointing_scan_mode_uses_scan_block_when_requested(monkeypatch):
    spec = DummySpec([ScanWaypoint("RP0")], use_scan_block=True, merge_scan_blocks=False)
    obs = _make_obs(spec)

    preflight_lines = []

    def fake_preflight(self, lines):
        preflight_lines.extend(lines)

    monkeypatch.setattr(MODULE.RadioPointing, "_preflight_scan_block_kinematics", fake_preflight)
    obs.run("dummy.obs")

    antenna_calls = [call for call in obs.com.calls if call[0] == "antenna"]
    assert [call[1] for call in antenna_calls] == ["point"]
    assert len(preflight_lines) == 1
    assert len([call for call in obs.com.calls if call[0] == "scan_block"]) == 1
    assert not any(call[0] == "antenna" and call[1] == "scan" for call in obs.com.calls)
    metadata_sets = [call for call in obs.com.calls if call[0] == "metadata"]
    assert metadata_sets[0] == ("metadata", "set", {"position": "ON", "id": "RP0"})
    assert metadata_sets[-1] == ("metadata", "set", {"position": "", "id": ""})
    assert obs.logger.warnings == []


def test_radio_pointing_scan_mode_can_merge_xy_pair(monkeypatch):
    spec = DummySpec(
        [
            ScanWaypoint("RP-X", start=(-1.0, 0.0), stop=(1.0, 0.0)),
            ScanWaypoint("RP-Y", start=(0.0, 1.0), stop=(0.0, -1.0)),
        ],
        use_scan_block=True,
        merge_scan_blocks=True,
        scan_block_final_standby=True,
        scan_block_final_standby_duration_sec=1.5,
    )
    obs = _make_obs(spec)

    captured = {}

    def fake_preflight(self, lines):
        captured["lines"] = list(lines)

    def fake_build_scan_block_sections(lines, **kwargs):
        captured["build_kwargs"] = dict(kwargs)
        return [
            SimpleNamespace(kind="initial_standby", line_index=-1),
            SimpleNamespace(kind="line", line_index=0),
            SimpleNamespace(kind="turn", line_index=-1),
            SimpleNamespace(kind="line", line_index=1),
            SimpleNamespace(kind="final_standby", line_index=-1),
        ]

    monkeypatch.setattr(MODULE.RadioPointing, "_preflight_scan_block_kinematics", fake_preflight)
    monkeypatch.setattr(MODULE, "build_scan_block_sections", fake_build_scan_block_sections)
    obs.run("dummy.obs")

    assert len(captured["lines"]) == 2
    assert captured["build_kwargs"]["include_final_standby"] is True
    assert captured["build_kwargs"]["final_standby_duration"] == 1.5
    scan_block_calls = [call for call in obs.com.calls if call[0] == "scan_block"]
    assert len(scan_block_calls) == 1
    kwargs = scan_block_calls[0][1]
    assert kwargs["scan_frame"] == "altaz"
    sections = kwargs["sections"]
    section_kinds = [section.kind for section in sections]
    assert section_kinds[0] == "initial_standby"
    assert section_kinds[-1] == "final_standby"
    line_indices = [section.line_index for section in sections if section.kind == "line"]
    assert line_indices == [0, 1]


def test_radio_pointing_point_mode_stays_on_legacy_point_path(monkeypatch):
    spec = DummySpec([PointWaypoint("RP-point")], use_scan_block=True, merge_scan_blocks=True)
    obs = _make_obs(spec)

    def fail_if_called(*args, **kwargs):
        raise AssertionError("Point mode must not enter scan_block path")

    monkeypatch.setattr(MODULE.RadioPointing, "_run_on_scan_block", fail_if_called)
    obs.run("dummy.obs")

    antenna_calls = [call for call in obs.com.calls if call[0] == "antenna"]
    assert [call[1] for call in antenna_calls] == ["point"]
    assert not any(call[0] == "scan_block" for call in obs.com.calls)


