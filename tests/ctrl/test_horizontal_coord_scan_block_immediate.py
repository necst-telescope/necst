from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from tests._helpers.scan_block_stub_runtime import load_horizontal_coord_module


REPO_ROOT = Path(__file__).resolve().parents[3]
MODULE = load_horizontal_coord_module(REPO_ROOT)


def test_horizontal_coord_rejects_move_to_entry_in_scan_block():
    hc = MODULE.HorizontalCoord.__new__(MODULE.HorizontalCoord)
    msg = SimpleNamespace(kind=MODULE.ScanBlockSectionMsg.MOVE_TO_ENTRY)
    with pytest.raises(ValueError, match="MOVE_TO_ENTRY"):
        hc._convert_scan_block_section(msg)
