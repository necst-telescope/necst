"""PR8a writer/schema hardening tests for spectral recording chunks.

These tests intentionally use the real ``neclib.recorders.necstdb_writer``
implementation but load it through minimal stubs, so the test does not require
the full telescope-control dependency stack (for example astropy).
"""

from __future__ import annotations

import importlib.util
import json
import struct
import sys
import tempfile
import types
from pathlib import Path
from typing import Protocol, runtime_checkable


ROOT = Path(__file__).resolve().parents[1]
RX = ROOT / "necst" / "rx"
WORK_ROOT = ROOT.parent
NECLIB_ROOT = WORK_ROOT / "neclib-second_OTF_neclib"
NECSTDB_ROOT = WORK_ROOT / "necstdb-master"

sys.path.insert(0, str(RX))
sys.path.insert(0, str(NECSTDB_ROOT))

SETUP_SPEC = importlib.util.spec_from_file_location(
    "spectral_recording_setup", RX / "spectral_recording_setup.py"
)
assert SETUP_SPEC and SETUP_SPEC.loader
srs = importlib.util.module_from_spec(SETUP_SPEC)
sys.modules["spectral_recording_setup"] = srs
SETUP_SPEC.loader.exec_module(srs)

RUNTIME_SPEC = importlib.util.spec_from_file_location(
    "spectral_recording_runtime", RX / "spectral_recording_runtime.py"
)
assert RUNTIME_SPEC and RUNTIME_SPEC.loader
rt = importlib.util.module_from_spec(RUNTIME_SPEC)
sys.modules["spectral_recording_runtime"] = rt
RUNTIME_SPEC.loader.exec_module(rt)


class _Logger:
    def warning(self, *args, **kwargs):
        pass

    def info(self, *args, **kwargs):
        pass

    def error(self, *args, **kwargs):
        raise AssertionError("NECSTDBWriter logged an error: " + " ".join(map(str, args)))


def _load_necstdb_writer_class():
    # Minimal package stubs needed by neclib/recorders/necstdb_writer.py.
    neclib_pkg = types.ModuleType("neclib")
    neclib_pkg.__path__ = [str(NECLIB_ROOT / "neclib")]
    sys.modules["neclib"] = neclib_pkg

    recorders_pkg = types.ModuleType("neclib.recorders")
    recorders_pkg.__path__ = [str(NECLIB_ROOT / "neclib" / "recorders")]
    sys.modules["neclib.recorders"] = recorders_pkg

    core_mod = types.ModuleType("neclib.core")
    core_mod.get_logger = lambda name: _Logger()
    sys.modules["neclib.core"] = core_mod

    @runtime_checkable
    class TextLike(Protocol):
        def upper(self) -> "TextLike": ...

        def lower(self) -> "TextLike": ...

        def find(self, *args, **kwargs) -> int: ...

        def replace(self, *args, **kwargs) -> "TextLike": ...

        def __len__(self) -> int: ...

    core_types_mod = types.ModuleType("neclib.core.types")
    core_types_mod.TextLike = TextLike
    sys.modules["neclib.core.types"] = core_types_mod

    writer_base_spec = importlib.util.spec_from_file_location(
        "neclib.recorders.writer_base",
        NECLIB_ROOT / "neclib" / "recorders" / "writer_base.py",
    )
    assert writer_base_spec and writer_base_spec.loader
    writer_base_mod = importlib.util.module_from_spec(writer_base_spec)
    sys.modules["neclib.recorders.writer_base"] = writer_base_mod
    writer_base_spec.loader.exec_module(writer_base_mod)

    writer_spec = importlib.util.spec_from_file_location(
        "neclib.recorders.necstdb_writer",
        NECLIB_ROOT / "neclib" / "recorders" / "necstdb_writer.py",
    )
    assert writer_spec and writer_spec.loader
    writer_mod = importlib.util.module_from_spec(writer_spec)
    sys.modules["neclib.recorders.necstdb_writer"] = writer_mod
    writer_spec.loader.exec_module(writer_mod)
    return writer_mod.NECSTDBWriter


def _stream(computed_windows_json: str = ""):
    return {
        "stream_id": "xffts_board4",
        "recording_mode": "tp",
        "spectrometer_key": "xffts",
        "board_id": 4,
        "fdnum": 0,
        "ifnum": 1,
        "plnum": 0,
        "polariza": "XX",
        "beam_id": "B00",
        "full_nchan": 16,
        "saved_ch_start": 8,
        "saved_ch_stop": 12,
        "saved_nchan": 4,
        "computed_windows_json": computed_windows_json,
        "tp_stat": "sum_mean",
    }


def _setup(stream):
    return rt.ActiveSpectralRecordingSetup(
        snapshot={"streams": {stream["stream_id"]: stream}},
        setup_id="writer_pr8a",
        setup_hash="a" * 64,
    )


def _chunk(stream, setup, line_label="scan-line", spectral_data=None):
    if spectral_data is None:
        spectral_data = list(range(16))
    return rt.tp_chunk_for_stream(
        stream,
        setup,
        time=123.5,
        time_spectrometer="xffts-time",
        position="ON",
        obs_id="orion",
        line_index=7,
        line_label=line_label,
        spectral_data=spectral_data,
    )


def _field_map(chunk):
    return {entry["key"]: entry for entry in chunk}


def test_pr8a_chunks_emit_fixed_length_bytes_before_writer():
    stream = _stream()
    setup = _setup(stream)
    fields = _field_map(_chunk(stream, setup))

    assert fields["setup_id"]["type"] == "string"
    assert isinstance(fields["setup_id"]["value"], bytes)
    assert len(fields["setup_id"]["value"]) == 64
    assert rt.decode_fixed_string_value(fields["setup_id"]["value"]) == "writer_pr8a"

    assert fields["tp_windows_json"]["type"] == "string"
    assert isinstance(fields["tp_windows_json"]["value"], bytes)
    assert len(fields["tp_windows_json"]["value"]) == 2048
    assert "computed_ch_start" in rt.decode_fixed_string_value(fields["tp_windows_json"]["value"])

    assert fields["tp_sum"]["type"] == "float64"
    assert fields["tp_valid"]["type"] == "bool"


def test_pr8a_writer_dry_run_uses_fixed_string_and_float64_formats():
    NECSTDBWriter = _load_necstdb_writer_class()
    stream = _stream()
    setup = _setup(stream)

    chunk1 = _chunk(stream, setup, line_label="short")
    chunk2 = _chunk(stream, setup, line_label="longer_line_label")
    topic = "data/tp/xffts/board4"

    with tempfile.TemporaryDirectory() as tmp:
        db_dir = Path(tmp) / "writer_pr8a.necstdb"
        writer = NECSTDBWriter()
        writer.start_recording(db_dir)
        assert writer.append(topic, chunk1) is True
        assert writer.append(topic, chunk2) is True
        writer.stop_recording()

        header_path = db_dir / "data-tp-xffts-board4.header"
        data_path = db_dir / "data-tp-xffts-board4.data"
        assert header_path.exists()
        assert data_path.exists()

        header = json.loads(header_path.read_text())
        cols = {entry["key"]: entry for entry in header["data"]}

        assert cols["setup_id"]["format"] == "64s"
        assert cols["setup_id"]["size"] == 64
        assert cols["setup_hash"]["format"] == "64s"
        assert cols["tp_windows_json"]["format"] == "2048s"
        assert cols["tp_windows_json"]["size"] == 2048
        assert cols["tp_stat"]["format"] == "16s"

        assert cols["tp_sum"]["format"] == "d"
        assert cols["tp_sum"]["size"] == 8
        assert cols["tp_mean"]["format"] == "d"
        assert cols["tp_valid"]["format"] == "?"

        record_size = struct.calcsize("<" + "".join(entry["format"] for entry in header["data"]))
        assert data_path.stat().st_size == 2 * record_size


def test_pr8a_rejects_oversized_tp_windows_json_instead_of_truncating():
    too_long_json = "[" + ("x" * 2048) + "]"
    stream = _stream(computed_windows_json=too_long_json)
    setup = _setup(stream)

    try:
        _chunk(stream, setup)
    except rt.SpectralRecordingRuntimeError as exc:
        assert "tp_windows_json" in str(exc)
        assert "too long" in str(exc)
    else:  # pragma: no cover
        raise AssertionError("Expected oversized fixed-string error")


if __name__ == "__main__":
    test_pr8a_chunks_emit_fixed_length_bytes_before_writer()
    test_pr8a_writer_dry_run_uses_fixed_string_and_float64_formats()
    test_pr8a_rejects_oversized_tp_windows_json_instead_of_truncating()
    print("all spectral_recording_writer PR8a tests passed")
