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
    def __init__(
        self,
        wid: str,
        *,
        scan_frame: str = "altaz",
        reference=(30.0, 45.0, "altaz"),
        offset=None,
    ):
        self.id = wid
        self.mode = ObservationMode.ON
        self.is_scan = True
        self.scan_frame = scan_frame
        self.start = (q(0.0), q(0.0))
        self.stop = (q(1.0), q(0.0))
        self.speed = q(0.5, "deg/s")
        self.reference = (
            (q(reference[0]), q(reference[1]), reference[2]) if reference else None
        )
        self.target = None
        self.name_query = False
        self.integration = SimpleNamespace(to_value=lambda unit: 1.0)
        self.with_offset = offset is not None
        self.offset = offset


def _make_obs(spec) -> MODULE.OTF:
    obs = MODULE.OTF.__new__(MODULE.OTF)
    obs.obsspec = spec
    obs.com = DummyCommander()
    obs.logger = FakeLogger()
    obs.observation_type = "OTF"
    obs.hot = lambda *a, **k: None
    obs.off = lambda *a, **k: None
    obs.on = lambda *a, **k: None
    return obs


def test_file_based_scan_block_grouping_merge_bydirectional(monkeypatch):
    spec = DummySpec(
        [Waypoint("L0"), Waypoint("L1"), Waypoint("L2")],
        use_scan_block=True,
        merge_scan_blocks=True,
        bydirectional=1,
    )
    obs = _make_obs(spec)
    seen = []

    def fake_run_on_scan_block(self, waypoints, *, scan_frags, **kwargs):
        seen.append(([wp.id for wp in waypoints], list(scan_frags), kwargs))

    monkeypatch.setattr(MODULE.OTF, "_run_on_scan_block", fake_run_on_scan_block)
    obs.run("dummy.obs")

    assert len(seen) == 1
    line_ids, scan_frags, kwargs = seen[0]
    assert line_ids == ["L0", "L1", "L2"]
    assert scan_frags == [1, -1, 1]
    assert kwargs["include_final_standby"] is False


def test_file_based_scan_block_split_on_signature_change(monkeypatch):
    spec = DummySpec(
        [
            Waypoint("L0", reference=(30.0, 45.0, "altaz")),
            Waypoint("L1", reference=(30.0, 45.0, "altaz")),
            Waypoint("L2", reference=(31.0, 45.0, "altaz")),
        ],
        use_scan_block=True,
        merge_scan_blocks=True,
        bydirectional=0,
    )
    obs = _make_obs(spec)
    seen = []

    def fake_run_on_scan_block(self, waypoints, *, scan_frags, **kwargs):
        seen.append(([wp.id for wp in waypoints], list(scan_frags)))

    monkeypatch.setattr(MODULE.OTF, "_run_on_scan_block", fake_run_on_scan_block)
    obs.run("dummy.obs")

    assert seen == [(["L0", "L1"], [1, 1]), (["L2"], [1])]


def test_file_based_scan_block_fallback_on_offset_frame_mismatch(monkeypatch):
    bad_offset = (q(0.2), q(0.1), "fk5")
    spec = DummySpec(
        [Waypoint("L0", scan_frame="altaz", offset=bad_offset)],
        use_scan_block=True,
        merge_scan_blocks=True,
        bydirectional=0,
    )
    obs = _make_obs(spec)

    def fail_if_called(*args, **kwargs):
        raise AssertionError(
            "scan_block path should not be used on offset frame mismatch"
        )

    monkeypatch.setattr(MODULE.OTF, "_run_on_scan_block", fail_if_called)
    obs.run("dummy.obs")

    antenna_calls = [call for call in obs.com.calls if call[0] == "antenna"]
    assert [call[1] for call in antenna_calls] == ["point", "scan"]
    assert any("falling back to legacy scan" in msg for msg in obs.logger.warnings)
