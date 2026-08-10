import csv
import importlib.util
import os
from pathlib import Path
import sys
import time

import pytest

_MODULE_PATH = Path(__file__).parents[1] / "necst" / "web" / "observation_log.py"
_SPEC = importlib.util.spec_from_file_location("necst_test_observation_log", _MODULE_PATH)
assert _SPEC is not None and _SPEC.loader is not None
_MODULE = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = _MODULE
_SPEC.loader.exec_module(_MODULE)

CSV_HEADER = _MODULE.CSV_HEADER
LEGACY_CSV_HEADER = _MODULE.LEGACY_CSV_HEADER
ObservationLogManager = _MODULE.ObservationLogManager
resolve_log_dir = _MODULE.resolve_log_dir


def test_comment_is_appended_to_existing_row(tmp_path):
    manager = ObservationLogManager.create(configured_dir=tmp_path)
    try:
        assert manager.write_event({}, mode="HOT", event="hot", result="success")
        assert manager.append_comment(1, "noise")
        assert manager.append_comment(1, "weather degraded")
        assert manager.update_comment(1, "edited comment")

        with manager.csv_path.open("r", encoding="utf-8", newline="") as fh:
            rows = list(csv.DictReader(fh))
        hot_row = next(row for row in rows if row["event"] == "hot")
        assert hot_row["row_id"] == "2"
        console_row = next(row for row in rows if row["event"] == "console_start")
        assert console_row["comment"] == "edited comment"
        assert all(row["event"] != "comment_amendment" for row in rows)
        assert manager.read_rows(limit=10)[-1]["comment"] == "edited comment"
        assert manager.has_row_id(1)
        assert not manager.has_row_id(99)
    finally:
        manager.close()


def test_legacy_csv_keeps_append_only_shape_and_tracks_amendment(tmp_path):
    path = tmp_path / "legacy.csv"
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(LEGACY_CSV_HEADER)
        writer.writerow(
            [
                "2026-08-10T00:00:00.000Z",
                "",
                "",
                "",
                "HOT",
                "hot",
                "",
                "success",
                "User",
                "",
                "old-session",
                "7",
                "",
                "",
                "",
                "",
            ]
        )

    manager = ObservationLogManager.create(configured_dir=tmp_path)
    try:
        manager.open_existing(path)
        assert manager.row_id >= 7
        assert manager.has_row_id(7)
        assert manager.append_comment(7, "weather degraded")
        with path.open("r", encoding="utf-8", newline="") as fh:
            rows = list(csv.DictReader(fh))
        assert rows[0]["comment"] == "weather degraded"
        assert len(rows[-1]) == len(LEGACY_CSV_HEADER)
        assert manager.read_rows(limit=10)[-1]["comment"] == "weather degraded"
    finally:
        manager.close()


def test_target_row_id_validation(tmp_path):
    manager = ObservationLogManager.create(configured_dir=tmp_path)
    try:
        assert not manager.write_event({}, target_row_id="not-an-id")
        assert "target_row_id" in manager.last_error
        assert manager.status()["columns"] == CSV_HEADER
    finally:
        manager.close()


def test_log_directory_precedence(tmp_path, monkeypatch):
    explicit = tmp_path / "explicit"
    from_env = tmp_path / "env"
    from_site = tmp_path / "site"
    for name in (
        "NECST_OBSLOG_DIR",
        "NECST_CONSOLE_PC_RECORD_ROOT",
        "NECST_CONSOLE_RECORD_ROOT",
        "NECST_RECORD_ROOT",
        "NECST_DATA_ROOT",
        "NECST_CONSOLE_DATA_ROOT",
    ):
        monkeypatch.delenv(name, raising=False)

    monkeypatch.setenv("NECST_OBSLOG_DIR", str(from_env))
    selected, source, _, _ = resolve_log_dir(
        explicit, site_configured_dir=from_site
    )
    assert selected == explicit
    assert source == "configured_dir"

    selected, source, _, _ = resolve_log_dir(site_configured_dir=from_site)
    assert selected == from_env
    assert source == "NECST_OBSLOG_DIR"

    monkeypatch.delenv("NECST_OBSLOG_DIR")
    selected, source, _, _ = resolve_log_dir(site_configured_dir=from_site)
    assert selected == from_site
    assert source == "site_config_observation_log_dir"


def test_csv_file_listing_is_limited_to_log_directory(tmp_path):
    manager = ObservationLogManager.create(configured_dir=tmp_path)
    try:
        outside = tmp_path.parent / "outside.csv"
        outside.write_text("not selectable\n", encoding="utf-8")
        inside = tmp_path / "night1.csv"
        inside.write_text(",".join(CSV_HEADER) + "\n", encoding="utf-8")

        files = manager.list_csv_files()
        names = {item["name"] for item in files}
        assert manager.csv_path.name in names
        assert "night1.csv" in names
        assert "outside.csv" not in names
        assert all(Path(item["path"]).parent == tmp_path for item in files)
    finally:
        manager.close()


def test_recent_csv_is_resumed_when_console_restarts_within_one_hour(tmp_path):
    first = ObservationLogManager.create(configured_dir=tmp_path)
    first_path = first.csv_path
    first.close()

    resumed = ObservationLogManager.create(configured_dir=tmp_path)
    try:
        assert resumed.csv_path == first_path
        assert resumed.read_rows(limit=1)[0]["event"] == "console_start"
    finally:
        resumed.close()


def test_csv_older_than_one_hour_starts_a_new_file(tmp_path):
    first = ObservationLogManager.create(configured_dir=tmp_path)
    old_path = first.csv_path
    first.close()
    old_timestamp = time.time() - (60 * 60 + 1)
    os.utime(old_path, (old_timestamp, old_timestamp))

    resumed = ObservationLogManager.create(configured_dir=tmp_path)
    try:
        assert resumed.csv_path != old_path
    finally:
        resumed.close()
