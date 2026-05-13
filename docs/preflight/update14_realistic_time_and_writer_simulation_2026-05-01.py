#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Realistic update14 timing/writer-path simulation.

This script is a lightweight preflight diagnostic, not a production tool.
It exercises these hot paths with the patched update14 tree:

1. xfftspy.data_consumer packet parsing, including received_time capture.
2. NECSTDBWriter ndarray/raw append_packed path for spectral rows.
3. NECSTDBWriter tuple/list fallback path, to quantify the cost if the
   zero-copy numpy receive path is unavailable.
"""

from __future__ import annotations

import importlib.util
import json
import os
import shutil
import struct
import sys
import tempfile
import time
import types
from pathlib import Path
from typing import Protocol, runtime_checkable

import numpy as np

ROOT = Path(__file__).resolve().parents[2]
XFFTSPY_ROOT = ROOT / "xfftspy-master"
NECLIB_ROOT = ROOT / "neclib-second_OTF_neclib"
NECSTDB_ROOT = ROOT / "necstdb-master"
OUT_JSON = ROOT / "docs" / "preflight" / "update14_realistic_time_and_writer_simulation_2026-05-01.json"


def _load_xfftspy_data_consumer():
    sys.path.insert(0, str(XFFTSPY_ROOT))
    from xfftspy.data_consumer import data_consumer

    return data_consumer


class _FakeSocket:
    def __init__(self, payload: bytes):
        self.payload = memoryview(payload)
        self.pos = 0

    def recv(self, nbytes: int) -> bytes:
        if self.pos >= len(self.payload):
            return b""
        stop = min(self.pos + int(nbytes), len(self.payload))
        out = self.payload[self.pos : stop].tobytes()
        self.pos = stop
        return out


def _timestamp28() -> bytes:
    raw = b"2026-05-01T00:00:00.0GPS"
    return raw[:28].ljust(28, b"\x00")


def _xffts_packet(nboard: int = 8, nchan: int = 32768) -> bytes:
    spectrum = np.linspace(-1.0, 1.0, nchan, dtype="<f4")
    spec_bytes = spectrum.tobytes(order="C")
    raw_parts = []
    for board in range(1, nboard + 1):
        raw_parts.append(struct.pack("<II", board, nchan))
        raw_parts.append(spec_bytes)
    raw = b"".join(raw_parts)
    package_length = 64 + len(raw)
    header = struct.pack(
        "<4s4sI8s28sIIII",
        b"IEEE",
        b"F32 ",
        package_length,
        b"XFFTS\x00\x00\x00",
        _timestamp28(),
        100000,
        0,
        nboard,
        0,
    )
    assert len(header) == 64
    return header + raw


def _measure_receive_path(return_numpy: bool, n_iter: int, nboard: int = 8, nchan: int = 32768):
    data_consumer = _load_xfftspy_data_consumer()
    packet = _xffts_packet(nboard=nboard, nchan=nchan)
    consumer = data_consumer.__new__(data_consumer)
    consumer.return_numpy = bool(return_numpy)
    times = []
    sample_type = None
    received_time_ok = False
    for _ in range(n_iter):
        consumer.sock = _FakeSocket(packet)
        t0 = time.perf_counter()
        result = consumer.receive_once()
        times.append(time.perf_counter() - t0)
        sample = result["data"][1]
        sample_type = type(sample).__name__
        received_time_ok = isinstance(result["header"].get("received_time"), float)
        if return_numpy:
            assert isinstance(sample, np.ndarray)
            assert sample.dtype == np.dtype("<f4")
            assert sample.shape == (nchan,)
        else:
            assert isinstance(sample, tuple)
            assert len(sample) == nchan
    arr = np.asarray(times, dtype=float)
    mb_per_packet = len(packet) / 1.0e6
    return {
        "return_numpy": bool(return_numpy),
        "n_iter": int(n_iter),
        "nboard": int(nboard),
        "nchan": int(nchan),
        "packet_MB": float(mb_per_packet),
        "sample_type": sample_type,
        "received_time_header_present": bool(received_time_ok),
        "median_ms_per_packet": float(np.median(arr) * 1000.0),
        "p84_ms_per_packet": float(np.percentile(arr, 84.0) * 1000.0),
        "throughput_MB_s_median": float(mb_per_packet / np.median(arr)),
    }


class _Logger:
    def warning(self, *args, **kwargs):
        pass

    def info(self, *args, **kwargs):
        pass

    def error(self, *args, **kwargs):
        raise RuntimeError("NECSTDBWriter logged an error: " + " ".join(map(str, args)))


def _load_necstdb_writer_class():
    sys.path.insert(0, str(NECSTDB_ROOT))

    # necstdb imports pandas for table read helpers, but this diagnostic only
    # writes append_packed records.  Keep the simulation runnable on lean
    # telescope-control environments where pandas is not installed.
    if "pandas" not in sys.modules:
        pandas = types.ModuleType("pandas")

        class _DataFrame:
            def __init__(self, *args, **kwargs):
                pass

            @classmethod
            def from_records(cls, *args, **kwargs):
                return cls()

        pandas.DataFrame = _DataFrame
        pandas.to_datetime = lambda *args, **kwargs: np.asarray(args[0] if args else [])
        pandas.merge_asof = lambda left, right, *args, **kwargs: left
        sys.modules["pandas"] = pandas

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
    writer_base_mod = importlib.util.module_from_spec(writer_base_spec)
    sys.modules["neclib.recorders.writer_base"] = writer_base_mod
    writer_base_spec.loader.exec_module(writer_base_mod)

    writer_spec = importlib.util.spec_from_file_location(
        "neclib.recorders.necstdb_writer",
        NECLIB_ROOT / "neclib" / "recorders" / "necstdb_writer.py",
    )
    writer_mod = importlib.util.module_from_spec(writer_spec)
    sys.modules["neclib.recorders.necstdb_writer"] = writer_mod
    writer_spec.loader.exec_module(writer_mod)
    return writer_mod.NECSTDBWriter


def _chunk(spectral_data, *, time_value: float, timestamp_text: str):
    return [
        {"key": "data", "type": "float32", "value": spectral_data},
        {"key": "position", "type": "string<=8", "value": "ON".ljust(8)},
        {"key": "id", "type": "string<=16", "value": "sim".ljust(16)},
        {"key": "line_index", "type": "int32", "value": 7},
        {"key": "line_label", "type": "string<=64", "value": "line".ljust(64)},
        {"key": "time", "type": "float64", "value": float(time_value)},
        {"key": "time_spectrometer", "type": "string<=32", "value": timestamp_text[:32].ljust(32)},
        {"key": "ch", "type": "int32", "value": []},
        {"key": "rfreq", "type": "float64", "value": []},
        {"key": "ifreq", "type": "float64", "value": []},
        {"key": "vlsr", "type": "float64", "value": []},
        {"key": "integ", "type": "float64", "value": 0.1},
    ]


def _close_writer(writer):
    for table in list(writer.tables.values()):
        table.close()
    writer.tables.clear()
    writer._table_last_update.clear()
    writer.db = None


def _measure_writer_direct(use_numpy: bool, n_packet: int, nboard: int = 8, nchan: int = 32768):
    NECSTDBWriter = _load_necstdb_writer_class()
    # Reset singleton state for repeatable diagnostics in one process.
    NECSTDBWriter._instance.pop(NECSTDBWriter, None)
    NECSTDBWriter._initialized.pop(NECSTDBWriter, None)
    import necstdb

    tmp = Path(tempfile.mkdtemp(prefix="u14_writer_direct_", dir="/mnt/data"))
    db_dir = tmp / "sim.necstdb"
    writer = NECSTDBWriter()
    writer.db = necstdb.opendb(db_dir, mode="w")
    spec_np = np.linspace(-1.0, 1.0, nchan, dtype="<f4")
    spec_value = spec_np if use_numpy else tuple(float(x) for x in spec_np)
    timestamp_text = "2026-05-01T00:00:00.000000GPS"
    nrow = 0
    t0 = time.perf_counter()
    for ipacket in range(n_packet):
        for board in range(1, nboard + 1):
            topic = f"/necst/data/spectral/xffts/board{board}"
            writer._write(
                topic,
                _chunk(spec_value, time_value=1777593600.0 + ipacket * 0.1, timestamp_text=timestamp_text),
            )
            nrow += 1
    elapsed = time.perf_counter() - t0
    total_bytes = 0
    for path in db_dir.glob("*.data"):
        total_bytes += path.stat().st_size
    used_append_packed = all(hasattr(table, "append_packed") for table in writer.tables.values())
    _close_writer(writer)
    shutil.rmtree(tmp, ignore_errors=True)
    return {
        "use_numpy": bool(use_numpy),
        "n_packet": int(n_packet),
        "nboard": int(nboard),
        "nrow": int(nrow),
        "nchan": int(nchan),
        "used_append_packed": bool(used_append_packed),
        "elapsed_sec": float(elapsed),
        "rows_per_sec": float(nrow / elapsed),
        "data_MB_written": float(total_bytes / 1.0e6),
        "write_MB_s": float((total_bytes / 1.0e6) / elapsed),
    }


def _measure_writer_queue(n_packet: int, nboard: int = 8, nchan: int = 32768):
    NECSTDBWriter = _load_necstdb_writer_class()
    NECSTDBWriter._instance.pop(NECSTDBWriter, None)
    NECSTDBWriter._initialized.pop(NECSTDBWriter, None)

    tmp = Path(tempfile.mkdtemp(prefix="u14_writer_queue_", dir="/mnt/data"))
    db_dir = tmp / "sim.necstdb"
    writer = NECSTDBWriter()
    spec_np = np.linspace(-1.0, 1.0, nchan, dtype="<f4")
    timestamp_text = "2026-05-01T00:00:00.000000GPS"
    nrow = 0
    writer.start_recording(db_dir)
    t0 = time.perf_counter()
    for ipacket in range(n_packet):
        for board in range(1, nboard + 1):
            topic = f"/necst/data/spectral/xffts/board{board}"
            writer.append(
                topic,
                _chunk(spec_np, time_value=1777593600.0 + ipacket * 0.1, timestamp_text=timestamp_text),
            )
            nrow += 1
    enqueue_elapsed = time.perf_counter() - t0
    writer.stop_recording()
    total_elapsed = time.perf_counter() - t0
    total_bytes = sum(path.stat().st_size for path in db_dir.glob("*.data"))
    shutil.rmtree(tmp, ignore_errors=True)
    return {
        "n_packet": int(n_packet),
        "nboard": int(nboard),
        "nrow": int(nrow),
        "nchan": int(nchan),
        "enqueue_elapsed_sec": float(enqueue_elapsed),
        "total_elapsed_sec": float(total_elapsed),
        "total_rows_per_sec": float(nrow / total_elapsed),
        "data_MB_written": float(total_bytes / 1.0e6),
        "total_write_MB_s": float((total_bytes / 1.0e6) / total_elapsed),
    }


def main():
    results = {
        "definitions": {
            "nboard": "number of XFFTS boards per packet",
            "nchan": "channels per board",
            "packet": "one XFFTS TCP payload containing all boards",
            "row": "one NECSTDB spectral table row for one board",
        },
        "receive_numpy": _measure_receive_path(True, n_iter=30),
        "receive_tuple_fallback": _measure_receive_path(False, n_iter=3),
        "writer_direct_numpy": _measure_writer_direct(True, n_packet=30),
        "writer_direct_tuple_fallback": _measure_writer_direct(False, n_packet=2),
        "writer_queue_numpy": _measure_writer_queue(n_packet=30),
    }
    OUT_JSON.write_text(json.dumps(results, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps(results, indent=2, sort_keys=True), flush=True)


if __name__ == "__main__":
    try:
        main()
    finally:
        sys.stdout.flush()
        sys.stderr.flush()
        os._exit(0)
