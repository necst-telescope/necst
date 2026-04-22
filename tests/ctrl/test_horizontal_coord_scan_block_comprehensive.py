from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from tests._helpers.scan_block_stub_runtime import load_horizontal_coord_module


REPO_ROOT = Path(__file__).resolve().parents[3]
MODULE = load_horizontal_coord_module(REPO_ROOT)
MSG = MODULE.ScanBlockSectionMsg


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


def _sec(kind, *, frame="altaz", start=(0.0, 0.0), stop=(1.0, 0.0), speed=0.5, margin=0.1, label="L", line_index=0, tight=False, duration_hint=0.0, turn_radius_hint=0.0):
    return SimpleNamespace(
        kind=kind,
        frame=frame,
        start=list(start),
        stop=list(stop),
        speed=speed,
        margin=margin,
        label=label,
        line_index=line_index,
        tight=tight,
        duration_hint=duration_hint,
        turn_radius_hint=turn_radius_hint,
    )


def _hc():
    hc = MODULE.HorizontalCoord.__new__(MODULE.HorizontalCoord)
    hc.finder = DummyFinder()
    hc.executing_generator = DummyGeneratorManager()
    hc._gen_lock = DummyLock()
    hc.direct_mode = False
    return hc


def test_convert_scan_block_section_maps_all_supported_kinds_and_optional_fields():
    hc = _hc()
    section = MODULE.HorizontalCoord._convert_scan_block_section(
        hc,
        _sec(MSG.TURN, start=(1.1, 0.0), stop=(-0.1, 0.2), label="turn:0->1", line_index=1, turn_radius_hint=0.2),
    )
    assert section.kind == "turn"
    assert section.start == (1.1, 0.0)
    assert section.stop == (-0.1, 0.2)
    assert section.turn_radius_hint == 0.2
    assert section.line_index == 1
    assert section.label == "turn:0->1"

    final = MODULE.HorizontalCoord._convert_scan_block_section(
        hc,
        _sec(MSG.FINAL_STANDBY, start=(2.0, 3.0), stop=(2.0, 3.0), speed=0.0, margin=0.0, duration_hint=2.5),
    )
    assert final.kind == "final_standby"
    assert final.duration == 2.5

    handoff_turn = MODULE.HorizontalCoord._convert_scan_block_section(
        hc,
        _sec(MSG.HANDOFF_TURN, start=(1.1, 0.0), stop=(-0.1, 0.2), label="handoff:0->1", line_index=1, turn_radius_hint=0.3),
    )
    assert handoff_turn.kind == "handoff_turn"
    assert handoff_turn.turn_radius_hint == 0.3

    handoff_standby = MODULE.HorizontalCoord._convert_scan_block_section(
        hc,
        _sec(MSG.HANDOFF_STANDBY, start=(1.0, 0.2), stop=(0.0, 0.2), speed=0.5, margin=0.1, label="L1:handoff_standby", line_index=1),
    )
    assert handoff_standby.kind == "handoff_standby"
    assert handoff_standby.speed == 0.5
    assert handoff_standby.margin == 0.1


def test_parse_scan_block_cmd_rejects_mixed_section_frames():
    hc = _hc()
    req = SimpleNamespace(
        direct_mode=False,
        obsfreq=0.0,
        sections=[_sec(MSG.LINE, frame="altaz"), _sec(MSG.LINE, frame="fk5")],
        offset_lon=[], offset_lat=[], offset_frame="",
        lon=[30.0], lat=[45.0], frame="altaz", name="", unit="deg", cos_correction=False,
    )
    with pytest.raises(ValueError, match="exactly one shared section frame"):
        MODULE.HorizontalCoord._parse_scan_block_cmd(hc, req)


def test_parse_scan_block_cmd_rejects_bad_offset_pair_length():
    hc = _hc()
    req = SimpleNamespace(
        direct_mode=False,
        obsfreq=0.0,
        sections=[_sec(MSG.LINE)],
        offset_lon=[1.0, 2.0], offset_lat=[3.0], offset_frame="altaz",
        lon=[30.0], lat=[45.0], frame="altaz", name="", unit="deg", cos_correction=False,
    )
    with pytest.raises(ValueError, match=r"exactly one \(lon, lat\) pair"):
        MODULE.HorizontalCoord._parse_scan_block_cmd(hc, req)


def test_parse_scan_block_cmd_attaches_generator_with_reference_offset_and_obsfreq():
    hc = _hc()
    req = SimpleNamespace(
        direct_mode=True,
        obsfreq=230.5,
        sections=[_sec(MSG.LINE, label="SCI", line_index=4, tight=True)],
        offset_lon=[0.1], offset_lat=[-0.2], offset_frame="altaz",
        lon=[30.0], lat=[45.0], frame="fk5", name="", unit="deg", cos_correction=True,
    )
    MODULE.HorizontalCoord._parse_scan_block_cmd(hc, req)
    assert hc.direct_mode is True
    assert hc.finder.direct_mode is True
    assert hc.finder.obsfreq.to_value("GHz") == 230.5
    assert hc.executing_generator.attached == "GEN"
    args, kwargs = hc.finder.calls[0]
    assert args == (30.0, 45.0, "fk5")
    assert kwargs["scan_frame"] == "altaz"
    assert kwargs["offset"] == (0.1, -0.2, "altaz")
    assert kwargs["cos_correction"] is True
    assert kwargs["sections"][0].kind == "line"


def test_horizontal_coord_wire_kind_map_covers_all_decodeable_message_kinds():
    expected = {
        int(MSG.FIRST_STANDBY): "initial_standby",
        int(MSG.ACCELERATE): "accelerate",
        int(MSG.LINE): "line",
        int(MSG.TURN): "turn",
        int(MSG.HANDOFF_TURN): "handoff_turn",
        int(MSG.DECELERATE): "decelerate",
        int(MSG.FINAL_STANDBY): "final_standby",
        int(MSG.HANDOFF_STANDBY): "handoff_standby",
    }
    assert MODULE.HorizontalCoord._SCAN_BLOCK_KIND_MAP == expected


def test_convert_scan_block_section_rejects_move_to_entry_wire_kind():
    hc = _hc()
    with pytest.raises(ValueError, match="MOVE_TO_ENTRY"):
        MODULE.HorizontalCoord._convert_scan_block_section(hc, _sec(MSG.MOVE_TO_ENTRY))
