from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from tests._helpers.scan_block_stub_runtime import load_spectrometer_module


REPO_ROOT = Path(__file__).resolve().parents[3]
MODULE = load_spectrometer_module(REPO_ROOT)


class DummyPublisher:
    def __init__(self):
        self.messages = []

    def publish(self, msg):
        self.messages.append(msg)


class DummyCondition:
    def __init__(self, values):
        self.values = list(values)

    def check(self, arg):
        if self.values:
            return self.values.pop(0)
        return True


class DummyRecorder:
    def __init__(self):
        self.is_recording = True
        self.appended = []

    def append(self, path, chunk):
        self.appended.append((path, chunk))


class DummyIO(dict):
    def __init__(self):
        super().__init__({"xffts": SimpleNamespace(calc_tp=lambda data, rng: data)})


def _mode(position="ON", id="OBS-0123456789ABCDEFGHIJ"):
    return MODULE.ObservingModeManager.ObservingMode(time=0.0, position=position, id=id)


def _section(kind="line", label="L" * 70, line_index=7):
    return MODULE.ControlSectionManager.ControlSection(
        time=0.0, kind=kind, label=label, line_index=line_index
    )


def _spec_node():
    spec = MODULE.SpectralData.__new__(MODULE.SpectralData)
    spec.publisher = {"xffts_board0": DummyPublisher()}
    spec.metadata = SimpleNamespace(get=lambda time: _mode())
    spec.control_section = SimpleNamespace(get=lambda time: _section())
    spec.qlook_ch_range = (0, 4)
    spec.resizers = {
        "xffts": {
            0: SimpleNamespace(keep_duration=1.5, get=lambda *a, **k: [1.0, 2.0, 3.0])
        }
    }
    spec.data_queue = {}
    spec.last_data = {}
    spec.io = DummyIO()
    spec.record_condition = DummyCondition([True, False])
    spec.recorder = DummyRecorder()
    spec.tp_mode = False
    spec.tp_range = None
    spec.get_data = lambda: {"xffts": (123.0, 120.0, {0: [4.0, 5.0]})}
    spec.logger = SimpleNamespace(warning=lambda *a, **k: None)
    return spec


def test_observing_and_control_section_managers_return_latest_past_entries(monkeypatch):
    mgr = MODULE.ObservingModeManager()
    sec = MODULE.ControlSectionManager()
    monkeypatch.setattr(MODULE.pytime, "time", lambda: 100.0)

    mgr.enable(80.0)
    mgr.set(90.0, position="ON", id="A")
    mgr.set(95.0, position="OFF", id="B")
    assert mgr.get(92.0).position == "ON"
    assert mgr.get(97.0).position == "OFF"

    sec.set(90.0, kind="line", label="L0", line_index=0)
    sec.set(95.0, kind="turn", label="T01", line_index=1)
    assert sec.get(92.0).kind == "line"
    assert sec.get(97.0).kind == "turn"


def test_update_control_status_enables_disables_and_sets_section():
    enabled = []
    disabled = []
    set_calls = []
    spec = MODULE.SpectralData.__new__(MODULE.SpectralData)
    spec.metadata = SimpleNamespace(
        enable=lambda start: enabled.append(start),
        disable=lambda start: disabled.append(start),
    )
    spec.control_section = SimpleNamespace(
        set=lambda *args, **kwargs: set_calls.append((args, kwargs))
    )

    MODULE.SpectralData.update_control_status(
        spec,
        SimpleNamespace(
            time=10.0,
            tight=True,
            section_kind="line",
            section_label="SCI",
            line_index=3,
        ),
    )
    MODULE.SpectralData.update_control_status(
        spec,
        SimpleNamespace(
            time=11.0, tight=False, section_kind="turn", section_label="T", line_index=4
        ),
    )

    assert enabled == [10.0]
    assert disabled == [11.0]
    assert set_calls[0][0] == (10.0,)
    assert set_calls[0][1] == {"kind": "line", "label": "SCI", "line_index": 3}
    assert set_calls[1][1] == {"kind": "turn", "label": "T", "line_index": 4}


def test_stream_and_record_gate_line_metadata_by_on_line_only(monkeypatch):
    spec = _spec_node()
    MODULE.namespace.data = "data"
    monkeypatch.setattr(MODULE.pytime, "time", lambda: 200.0)

    MODULE.SpectralData.stream(spec)
    msg = spec.publisher["xffts_board0"].messages[-1]
    assert msg.position == "ON"
    assert msg.id == "OBS-0123456789AB"  # 16 chars
    assert msg.line_index == 7
    assert len(msg.line_label) == 64

    MODULE.SpectralData.record(spec)
    path, chunk = spec.recorder.appended[-1]
    chunk_dict = {entry["key"]: entry["value"] for entry in chunk}
    assert path.endswith("/spectral/xffts/board0")
    assert chunk_dict["position"].strip() == "ON"
    assert chunk_dict["line_index"] == 7
    assert chunk_dict["line_label"].strip() == "L" * 64

    spec.metadata = SimpleNamespace(
        get=lambda time: _mode(position="OFF", id="SHORTID")
    )
    spec.control_section = SimpleNamespace(
        get=lambda time: _section(kind="turn", label="TURNLABEL", line_index=9)
    )
    MODULE.SpectralData.stream(spec)
    msg2 = spec.publisher["xffts_board0"].messages[-1]
    assert msg2.line_index == -1
    assert msg2.line_label == ""
