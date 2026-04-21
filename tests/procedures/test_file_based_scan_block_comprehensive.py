from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from tests._helpers.scan_block_stub_runtime import FakeLogger, load_file_based_module, q


REPO_ROOT = Path(__file__).resolve().parents[3]
MODULE = load_file_based_module(REPO_ROOT)
ObservationMode = MODULE.ObservationMode


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
        self.bydirectional = int(params.get("bydirectional", 0))
        self.reset_scan = int(params.get("reset_scan", 0))
        self.scan_direction = params.get("scan_direction", "x")
        self.relative = True
        self.delta_lambda = SimpleNamespace(value=1.0)
        self.delta_beta = SimpleNamespace(value=1.0)
        self.lambda_off = SimpleNamespace(value=1.0)
        self.beta_off = SimpleNamespace(value=1.0)
        self.lambda_on = SimpleNamespace(value=0.0)
        self.beta_on = SimpleNamespace(value=0.0)


class Waypoint:
    def __init__(self, wid: str, *, name_query=False, target=None, reference=(30.0, 45.0, "altaz"), offset=None, mode=None, is_scan=True):
        self.id = wid
        self.mode = ObservationMode.ON if mode is None else mode
        self.is_scan = is_scan
        self.scan_frame = "altaz"
        self.start = (q(0.0), q(0.0))
        self.stop = (q(1.0), q(0.0))
        self.speed = q(0.5, "deg/s")
        self.reference = (q(reference[0]), q(reference[1]), reference[2]) if reference else None
        self.target = target
        self.name_query = name_query
        self.integration = SimpleNamespace(to_value=lambda unit: 1.0)
        self.with_offset = offset is not None
        self.offset = offset


def _make_obs(spec, *, observation_type="OTF"):
    obs = MODULE.OTF.__new__(MODULE.OTF)
    obs.obsspec = spec
    obs.com = DummyCommander()
    obs.logger = FakeLogger()
    obs.observation_type = observation_type
    obs.hot = lambda *a, **k: None
    obs.off = lambda *a, **k: None
    obs.on = lambda *a, **k: None
    return obs


def test_run_on_scan_block_order_and_final_standby_forwarding(monkeypatch):
    spec = DummySpec([Waypoint("L0")])
    obs = _make_obs(spec)
    calls = []

    line = SimpleNamespace(start=(q(-0.1), q(0.0)), stop=(q(1.0), q(0.0)), speed=q(0.5, "deg/s"), margin=q(0.1), label="L0", line_index=0)
    monkeypatch.setattr(MODULE.OTF, "_make_scan_block_line", lambda self, wp, frag, margin_deg, line_index: line)
    monkeypatch.setattr(MODULE, "build_scan_block_sections", lambda lines, **kwargs: calls.append(("build", kwargs)) or ["SEC"])
    monkeypatch.setattr(MODULE.OTF, "_move_to_scan_block_entry", lambda self, wp, line, cos_scan: calls.append(("move", wp.id, cos_scan, line.label)))

    obs._run_on_scan_block(
        [spec[0]],
        scan_frags=[1],
        cos_scan=True,
        margin_deg=0.1,
        include_final_standby=True,
        final_standby_duration_sec=2.5,
    )

    assert calls[0] == ("move", "L0", True, "L0")
    assert calls[1][0] == "build"
    assert calls[1][1]["include_initial_standby"] is False
    assert calls[1][1]["include_final_standby"] is True
    assert calls[1][1]["final_standby_duration"] == 2.5
    assert [c[0] for c in obs.com.calls] == ["metadata", "scan_block", "metadata"]
    assert obs.com.calls[0][2] == {"position": "ON", "id": "L0"}
    assert obs.com.calls[1][1]["sections"] == ["SEC"]
    assert obs.com.calls[1][1]["prewait"] is False
    assert obs.com.calls[2][2] == {"position": "", "id": ""}


def test_non_merge_scan_blocks_bydirectional_runs_individual_blocks(monkeypatch):
    spec = DummySpec([Waypoint("L0"), Waypoint("L1"), Waypoint("L2")], use_scan_block=True, merge_scan_blocks=False, bydirectional=1)
    obs = _make_obs(spec)
    seen = []
    monkeypatch.setattr(
        MODULE.OTF,
        "_make_scan_block_line",
        lambda self, wp, frag, margin_deg, line_index: SimpleNamespace(
            start=(q(0.0), q(0.0)), stop=(q(1.0), q(0.0)), speed=q(0.5, "deg/s"), margin=q(0.1), label=wp.id, line_index=line_index
        ),
    )
    monkeypatch.setattr(MODULE.OTF, "_run_on_scan_block", lambda self, waypoints, *, scan_frags, **kwargs: seen.append(([wp.id for wp in waypoints], list(scan_frags))))
    obs.run("dummy.obs")
    assert seen == [(["L0"], [1]), (["L1"], [-1]), (["L2"], [1])]


def test_scan_block_supported_rejects_non_on_non_scan_and_bad_offset():
    obs = _make_obs(DummySpec([]))
    wp_non_scan = Waypoint("P0", is_scan=False)
    wp_off = Waypoint("OFF0", mode=ObservationMode.OFF)
    wp_bad_offset = Waypoint("B0", offset=(q(0.1), q(0.2)))
    assert obs._scan_block_supported_for_waypoint(wp_non_scan, use_scan_block=True) is False
    assert obs._scan_block_supported_for_waypoint(wp_off, use_scan_block=True) is False
    assert obs._scan_block_supported_for_waypoint(wp_bad_offset, use_scan_block=True) is False


def test_move_to_scan_block_entry_named_query_uses_name_and_combined_offset(monkeypatch):
    wp = Waypoint("LQ", name_query=True, target="Orion", reference=None, offset=(q(0.3), q(-0.2), "altaz"))
    obs = _make_obs(DummySpec([wp]))
    line = SimpleNamespace(start=(q(0.1), q(0.4)), stop=(q(1.0), q(0.4)), speed=q(0.5, "deg/s"), margin=q(0.1), label="LQ", line_index=0)
    monkeypatch.setattr(MODULE, "margin_start_of", lambda line: (q(0.1), q(0.4)))
    obs._move_to_scan_block_entry(wp, line=line, cos_scan=False)
    assert obs.com.calls == [(
        "antenna",
        "point",
        {
            "unit": "deg",
            "cos_correction": False,
            "name": "Orion",
            "offset": (0.4, 0.2, "altaz"),
        },
    )]


def test_hot_off_pair_moves_to_off_once_then_runs_hot_and_off():
    hot_wp = Waypoint("CAL0", mode=ObservationMode.HOT, is_scan=False)
    off_wp = Waypoint("CAL0", mode=ObservationMode.OFF, is_scan=False)
    obs = _make_obs(DummySpec([hot_wp, off_wp]))

    executed = []
    obs.hot = lambda integ, wid, **kwargs: executed.append(("hot", integ, wid, kwargs))
    obs.off = lambda integ, wid: executed.append(("off", integ, wid))

    obs.run("dummy.obs")

    antenna_calls = [call for call in obs.com.calls if call[0] == "antenna"]
    assert antenna_calls == [
        (
            "antenna",
            "point",
            {
                "unit": "deg",
                "cos_correction": False,
                "reference": (30.0, 45.0, "altaz"),
            },
        )
    ]
    assert executed == [
        ("hot", 1.0, "CAL0", {"preserve_tracking": True}),
        ("off", 1.0, "CAL0"),
    ]


@pytest.mark.parametrize("observation_type", ["Grid", "PSW", "RadioPointing", "OTF"])
def test_hot_off_pair_execution_is_shared_across_observation_modes(observation_type):
    hot_wp = Waypoint("CAL0", mode=ObservationMode.HOT, is_scan=False)
    off_wp = Waypoint("CAL0", mode=ObservationMode.OFF, is_scan=False)
    on_wp = Waypoint("ON0", is_scan=False)
    obs = _make_obs(DummySpec([hot_wp, off_wp, on_wp]), observation_type=observation_type)

    executed = []
    obs.hot = lambda integ, wid, **kwargs: executed.append(("hot", integ, wid, kwargs))
    obs.off = lambda integ, wid: executed.append(("off", integ, wid))
    obs.on = lambda integ, wid: executed.append(("on", integ, wid))

    obs.run("dummy.obs")

    antenna_calls = [call for call in obs.com.calls if call[0] == "antenna"]
    assert antenna_calls[0] == (
        "antenna",
        "point",
        {
            "unit": "deg",
            "cos_correction": False,
            "reference": (30.0, 45.0, "altaz"),
        },
    )
    assert executed[:2] == [
        ("hot", 1.0, "CAL0", {"preserve_tracking": True}),
        ("off", 1.0, "CAL0"),
    ]
    assert executed[2] == ("on", 1.0, "ON0")


def test_run_on_scan_block_can_skip_move_when_entry_is_already_ready(monkeypatch):
    spec = DummySpec([Waypoint("L0")])
    obs = _make_obs(spec)
    calls = []

    line = SimpleNamespace(start=(q(-0.1), q(0.0)), stop=(q(1.0), q(0.0)), speed=q(0.5, "deg/s"), margin=q(0.1), label="L0", line_index=0)
    next_line = SimpleNamespace(start=(q(1.0), q(0.2)), stop=(q(0.0), q(0.2)), speed=q(0.5, "deg/s"), margin=q(0.1), label="L1", line_index=0)
    monkeypatch.setattr(MODULE.OTF, "_make_scan_block_line", lambda self, wp, frag, margin_deg, line_index: line)
    monkeypatch.setattr(MODULE, "build_scan_block_sections", lambda lines, **kwargs: calls.append(("build", kwargs)) or ["SEC"])
    monkeypatch.setattr(MODULE.OTF, "_move_to_scan_block_entry", lambda self, wp, line, cos_scan: calls.append(("move", wp.id)))

    obs._run_on_scan_block(
        [spec[0]],
        scan_frags=[1],
        cos_scan=True,
        margin_deg=0.1,
        include_final_standby=False,
        final_standby_duration_sec=1.0,
        next_entry_line=next_line,
        entry_already_ready=True,
    )

    assert calls == [("build", {
        "include_initial_standby": False,
        "include_final_decelerate": True,
        "include_final_standby": False,
        "final_standby_duration": 1.0,
        "next_entry_line": next_line,
    })]


def test_non_merge_scan_blocks_prepare_next_entry_and_skip_second_move(monkeypatch):
    spec = DummySpec([Waypoint("L0"), Waypoint("L1")], use_scan_block=True, merge_scan_blocks=False, bydirectional=0)
    obs = _make_obs(spec)
    seen = []

    def _fake_make(self, wp, frag, margin_deg, line_index):
        y = 0.0 if wp.id == "L0" else 0.2
        return SimpleNamespace(
            start=(q(0.0), q(y)), stop=(q(1.0), q(y)), speed=q(0.5, "deg/s"), margin=q(0.1), label=wp.id, line_index=line_index
        )

    monkeypatch.setattr(MODULE.OTF, "_make_scan_block_line", _fake_make)
    monkeypatch.setattr(
        MODULE.OTF,
        "_run_on_scan_block",
        lambda self, waypoints, **kwargs: seen.append((waypoints[0].id, kwargs.get("entry_already_ready"), kwargs.get("next_entry_line").label if kwargs.get("next_entry_line") is not None else None)),
    )

    obs.run("dummy.obs")
    assert seen == [("L0", False, "L1"), ("L1", True, None)]
