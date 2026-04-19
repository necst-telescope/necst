from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from tests._helpers.scan_block_stub_runtime import load_spectrometer_module


REPO_ROOT = Path(__file__).resolve().parents[3]
MODULE = load_spectrometer_module(REPO_ROOT)


class DummyResizer:
    def __init__(self, keep_duration=1.5):
        self.keep_duration = keep_duration
    def get(self, qlook_ch_range, n_samples=100):
        return [1.0, 2.0, 3.0]


class DummyPublisher:
    def __init__(self):
        self.messages = []
    def publish(self, msg):
        self.messages.append(msg)


def test_spectrometer_line_label_truncation_boundary(monkeypatch):
    spec = MODULE.SpectralData.__new__(MODULE.SpectralData)
    spec.resizers = {"xffts": {0: DummyResizer()}}
    spec.qlook_ch_range = (0, 100)
    spec.publisher = {"xffts_board0": DummyPublisher()}
    spec.metadata = MODULE.ObservingModeManager()
    spec.control_section = MODULE.ControlSectionManager()

    monkeypatch.setattr(MODULE.pytime, "time", lambda: 1.0)

    label64 = "A" * 64
    label65 = "B" * 65
    assert MODULE._fit_string(label64, 64) == label64
    assert MODULE._fit_string(label65, 64) == "B" * 64

    spec.metadata.enable(0.0)
    spec.metadata.set(0.0, position="ON", id="obs")
    spec.control_section.set(0.0, kind="line", label="X" * 80, line_index=12)

    MODULE.SpectralData.stream(spec)
    msg = spec.publisher["xffts_board0"].messages[-1]
    assert msg.position == "ON"
    assert msg.line_index == 12
    assert msg.line_label == "X" * 64
    assert len(msg.line_label) == 64
