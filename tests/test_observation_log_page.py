import importlib.util
from pathlib import Path
import re


_MODULE_PATH = Path(__file__).parents[1] / "necst" / "web" / "observation_log_page.py"
_SPEC = importlib.util.spec_from_file_location("necst_test_observation_log_page", _MODULE_PATH)
assert _SPEC is not None and _SPEC.loader is not None
_MODULE = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(_MODULE)


def test_detail_page_contains_observation_log_workflow():
    html = _MODULE.render_observation_log_page()

    assert "Observation Log" in html
    assert "Target row: none" in html
    assert "/api/observation-log-rows?limit=5000" in html
    assert "/api/observation-log-files?limit=500" in html
    assert "obslog_comment" in html
    assert "UTC: newest first" in html
    assert "Event: all" in html
    assert "Copy selection" in html
    assert "row-number or column header" in html
    assert "selected row × column matrix" in html
    assert "comment cell to edit it directly" in html


def test_detail_page_has_sticky_scrollable_table_and_markdown_copy():
    html = _MODULE.render_observation_log_page()

    assert "max-height:calc(100vh - 205px)" in html
    assert "overflow:auto" in html
    assert "position:sticky" in html
    assert "| '+picked.cols.map(markdown).join(' | ')+' |'" in html
    assert re.search(r"replace\(/\\\|/g,'\\\\\\|'", html)
    assert 'data-row-select="${esc(rid)}"' in html
    assert "function selectColumn(col,index,event)" in html
    assert "state.selectedRows" in html and "state.selectedCols" in html
