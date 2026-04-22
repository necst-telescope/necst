from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from tests._helpers.scan_block_stub_runtime import load_horizontal_coord_module


REPO_ROOT = Path(__file__).resolve().parents[3]
MODULE = load_horizontal_coord_module(REPO_ROOT)


def test_horizontal_coord_accepts_move_to_entry_in_scan_block():
    hc = MODULE.HorizontalCoord.__new__(MODULE.HorizontalCoord)
    msg = SimpleNamespace(
        kind=MODULE.ScanBlockSectionMsg.MOVE_TO_ENTRY,
        start=[-0.1, 0.0],
        stop=[-0.1, 0.0],
        speed=0.0,
        margin=0.0,
        label="L0:move_to_entry",
        line_index=0,
        tight=False,
        duration_hint=1.0,
        turn_radius_hint=0.0,
    )
    section = hc._convert_scan_block_section(msg)
    assert section.kind == "move_to_entry"
    assert section.start == (-0.1, 0.0)
    assert section.stop == (-0.1, 0.0)
    assert section.duration == 1.0
    assert section.tight is False
