#!/usr/bin/env python3
"""Validate NECST observation-progress sidecar files.

This command intentionally has no ROS imports.  It validates the structured
progress files produced by ObservationProgressReporter so the same checker can
be run on an observing machine, copied record sidecars, or a development tree.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

FINAL_STATES = {"finished", "error", "aborted"}
VALID_LIFECYCLE_STATES = {
    "initializing",
    "recording_starting",
    "running",
    "cleanup",
    "finished",
    "error",
    "aborted",
}
VALID_DATA_STATES = {"integrating", "not_integrating", "waiting_spectrum", None, ""}


def safe_name(value: Any) -> str:
    original = str(value or "unknown")
    sanitized = re.sub(r"[^A-Za-z0-9_.-]+", "_", original)
    sanitized = re.sub(r"_+", "_", sanitized).strip("_") or "record"
    if sanitized != original:
        digest = hashlib.sha1(
            original.encode("utf-8", errors="surrogatepass")
        ).hexdigest()[:8]
        sanitized = f"{sanitized}_{digest}"
    return sanitized


def progress_root(value: Optional[str] = None) -> Path:
    return Path(
        value or os.environ.get("NECST_PROGRESS_ROOT", "/tmp/necst_progress")
    ).expanduser()


def strip_record_file_header(text: str) -> str:
    """Remove NECST FileWriter comment headers from structured sidecars.

    Files saved through ``com.record('file', ...)`` are prepended with comment
    lines such as ``# Original file: ...`` and ``# Recorded time: ...``.  JSON
    and JSONL progress sidecars must still be checkable directly from record
    directories, so the checker tolerates leading comment/blank lines.
    """
    lines = text.splitlines()
    start = 0
    while start < len(lines) and (
        not lines[start].strip() or lines[start].lstrip().startswith("#")
    ):
        start += 1
    return "\n".join(lines[start:])


def structured_lines(text: str) -> List[Tuple[int, str]]:
    result: List[Tuple[int, str]] = []
    for lineno, line in enumerate(text.splitlines(), start=1):
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        result.append((lineno, line))
    return result


@dataclass
class Finding:
    severity: str
    code: str
    message: str
    file: Optional[str] = None

    def as_dict(self) -> Dict[str, Any]:
        payload = {
            "severity": self.severity,
            "code": self.code,
            "message": self.message,
        }
        if self.file:
            payload["file"] = self.file
        return payload


class ProgressCheck:
    def __init__(self, *, strict: bool = False, require_final: bool = False) -> None:
        self.strict = strict
        self.require_final = require_final
        self.findings: List[Finding] = []

    def error(self, code: str, message: str, file: Optional[Path] = None) -> None:
        self.findings.append(
            Finding("error", code, message, str(file) if file else None)
        )

    def warn(self, code: str, message: str, file: Optional[Path] = None) -> None:
        severity = "error" if self.strict else "warning"
        self.findings.append(
            Finding(severity, code, message, str(file) if file else None)
        )

    def info(self, code: str, message: str, file: Optional[Path] = None) -> None:
        self.findings.append(
            Finding("info", code, message, str(file) if file else None)
        )

    @property
    def ok(self) -> bool:
        return not any(item.severity == "error" for item in self.findings)

    def load_json(
        self, path: Path, *, required: bool = True
    ) -> Optional[Dict[str, Any]]:
        if not path.exists():
            if required:
                self.error(
                    "missing_json", f"Required JSON file is missing: {path.name}", path
                )
            return None
        if path.stat().st_size <= 0:
            self.error("empty_json", f"JSON file is empty: {path.name}", path)
            return None
        try:
            payload = json.loads(
                strip_record_file_header(path.read_text(encoding="utf-8"))
            )
        except json.JSONDecodeError as exc:
            self.error("invalid_json", f"Invalid JSON: {exc}", path)
            return None
        except Exception as exc:
            self.error("read_failed", f"Could not read JSON: {exc}", path)
            return None
        if not isinstance(payload, dict):
            self.error(
                "json_not_object",
                f"Top-level JSON must be an object, got {type(payload).__name__}",
                path,
            )
            return None
        return payload

    def load_events(self, path: Path, *, required: bool = True) -> List[Dict[str, Any]]:
        if not path.exists():
            if required:
                self.error(
                    "missing_events",
                    "Required event log is missing: observation_events.jsonl",
                    path,
                )
            return []
        if path.stat().st_size <= 0:
            self.warn("empty_events", "Event log is empty", path)
            return []
        events: List[Dict[str, Any]] = []
        for lineno, line in structured_lines(path.read_text(encoding="utf-8")):
            try:
                event = json.loads(line)
            except json.JSONDecodeError as exc:
                self.error(
                    "invalid_event_json", f"Invalid JSONL at line {lineno}: {exc}", path
                )
                continue
            if not isinstance(event, dict):
                self.error(
                    "event_not_object", f"Event at line {lineno} is not an object", path
                )
                continue
            event["_lineno"] = lineno
            events.append(event)
        return events

    def check_schema_version(self, payload: Mapping[str, Any], path: Path) -> None:
        if payload.get("schema_version") != 1:
            self.error(
                "schema_version",
                f"schema_version must be 1, got {payload.get('schema_version')!r}",
                path,
            )

    def check_snapshot(self, snapshot: Optional[Mapping[str, Any]], path: Path) -> None:
        if snapshot is None:
            return
        self.check_schema_version(snapshot, path)
        for section in (
            "observation",
            "lifecycle",
            "plan",
            "activity",
            "geometry",
            "data",
            "time",
        ):
            if section not in snapshot:
                self.error(
                    "missing_section", f"Snapshot is missing section: {section}", path
                )
            elif not isinstance(snapshot.get(section), dict):
                self.error(
                    "section_not_object",
                    f"Snapshot section {section!r} must be an object",
                    path,
                )

        obs = snapshot.get("observation") or {}
        lifecycle = snapshot.get("lifecycle") or {}
        plan = snapshot.get("plan") or {}
        activity = snapshot.get("activity") or {}
        geometry = snapshot.get("geometry") or {}
        data = snapshot.get("data") or {}
        timing = snapshot.get("time") or {}

        state = lifecycle.get("state")
        if state not in VALID_LIFECYCLE_STATES:
            self.warn(
                "unknown_lifecycle_state", f"Unknown lifecycle.state: {state!r}", path
            )
        if self.require_final and state not in FINAL_STATES:
            self.error(
                "not_final",
                f"Final state required but lifecycle.state is {state!r}",
                path,
            )

        if not obs.get("type"):
            self.warn("missing_observation_type", "observation.type is missing", path)
        if not obs.get("record_name"):
            self.warn("missing_record_name", "observation.record_name is missing", path)

        index0 = plan.get("index0")
        total = plan.get("total")
        if index0 is not None:
            if not isinstance(index0, int):
                self.error(
                    "plan_index_type",
                    f"plan.index0 must be int when present, got {type(index0).__name__}",
                    path,
                )
            elif isinstance(total, int) and total > 0 and not (0 <= index0 < total):
                self.error(
                    "plan_index_range",
                    f"plan.index0={index0} is outside total={total}",
                    path,
                )
        if total is not None and (not isinstance(total, int) or total < 0):
            self.error(
                "plan_total",
                f"plan.total must be a non-negative int, got {total!r}",
                path,
            )
        if plan.get("index0_end") is not None:
            end = plan.get("index0_end")
            if not isinstance(end, int):
                self.error(
                    "plan_index_end_type",
                    f"plan.index0_end must be int, got {type(end).__name__}",
                    path,
                )
            elif isinstance(index0, int) and end < index0:
                self.error(
                    "plan_index_end_order",
                    f"plan.index0_end={end} is less than index0={index0}",
                    path,
                )

        data_state = activity.get("data_state")
        if data_state not in VALID_DATA_STATES:
            self.warn(
                "unknown_data_state",
                f"Unknown activity.data_state: {data_state!r}",
                path,
            )
        if data_state == "integrating":
            if not data.get("expected_metadata_position"):
                self.error(
                    "integrating_without_metadata",
                    "data_state=integrating but expected_metadata_position is empty",
                    path,
                )
            if data.get("expected_metadata_id") in (None, ""):
                self.warn(
                    "integrating_without_metadata_id",
                    "data_state=integrating but expected_metadata_id is empty",
                    path,
                )
        else:
            if data.get("expected_metadata_position"):
                self.warn(
                    "stale_expected_metadata",
                    "expected_metadata_position remains set although data_state is not integrating",
                    path,
                )
            if data.get("expected_metadata_id"):
                self.warn(
                    "stale_expected_metadata_id",
                    "expected_metadata_id remains set although data_state is not integrating",
                    path,
                )

        start = timing.get("started_at_unix")
        updated = timing.get("updated_at_unix")
        elapsed = timing.get("elapsed_sec")
        for key, value in (
            ("started_at_unix", start),
            ("updated_at_unix", updated),
            ("elapsed_sec", elapsed),
            ("estimated_remaining_sec", timing.get("estimated_remaining_sec")),
            ("estimated_total_sec", timing.get("estimated_total_sec")),
            ("estimated_completion_unix", timing.get("estimated_completion_unix")),
        ):
            if value is not None and not isinstance(value, (int, float)):
                self.error(
                    "time_type",
                    f"time.{key} must be numeric when present, got {type(value).__name__}",
                    path,
                )
            if (
                key.startswith("estimated_")
                and isinstance(value, (int, float))
                and value < 0
            ):
                self.error(
                    "time_negative",
                    f"time.{key} must be non-negative when present, got {value!r}",
                    path,
                )
        sample_count = timing.get("remaining_sample_count")
        if sample_count is not None and (
            not isinstance(sample_count, int) or sample_count < 0
        ):
            self.error(
                "remaining_sample_count",
                f"time.remaining_sample_count must be non-negative int, got {sample_count!r}",
                path,
            )
        confidence = timing.get("remaining_confidence")
        if confidence not in (None, "unknown", "low", "medium", "high"):
            self.warn(
                "remaining_confidence_unknown",
                f"Unknown time.remaining_confidence: {confidence!r}",
                path,
            )
        if (
            isinstance(start, (int, float))
            and isinstance(updated, (int, float))
            and updated + 1e-6 < start
        ):
            self.error(
                "time_order",
                "time.updated_at_unix is earlier than started_at_unix",
                path,
            )

        self.check_geometry(geometry, path, context="snapshot.geometry")

    def check_plan(self, plan_doc: Optional[Mapping[str, Any]], path: Path) -> None:
        if plan_doc is None:
            return
        self.check_schema_version(plan_doc, path)
        items = plan_doc.get("items")
        if not isinstance(items, list):
            self.error(
                "plan_items_missing",
                "observation_plan.json must contain an items list",
                path,
            )
            return
        item_count = plan_doc.get("item_count")
        if item_count is not None and item_count != len(items):
            self.error(
                "plan_item_count",
                f"item_count={item_count} but len(items)={len(items)}",
                path,
            )
        seen_uid = set()
        previous_index = -1
        for idx, item in enumerate(items):
            if not isinstance(item, dict):
                self.error(
                    "plan_item_not_object", f"Plan item {idx} is not an object", path
                )
                continue
            uid = item.get("item_uid")
            if not uid:
                self.warn("missing_item_uid", f"Plan item {idx} has no item_uid", path)
            elif uid in seen_uid:
                self.warn(
                    "duplicate_item_uid", f"Duplicate plan item_uid {uid!r}", path
                )
            seen_uid.add(uid)
            index0 = item.get("index0")
            if index0 is not None:
                if not isinstance(index0, int):
                    self.error(
                        "plan_item_index_type",
                        f"Plan item {idx} index0 must be int",
                        path,
                    )
                elif index0 < previous_index:
                    self.warn(
                        "plan_item_index_order",
                        f"Plan item {idx} index0={index0} decreased from {previous_index}",
                        path,
                    )
                elif index0 == previous_index:
                    self.warn(
                        "plan_item_index_duplicate",
                        f"Plan item {idx} index0={index0} duplicates previous item",
                        path,
                    )
                previous_index = index0 if isinstance(index0, int) else previous_index
            total = item.get("total")
            if (
                isinstance(index0, int)
                and isinstance(total, int)
                and total > 0
                and not (0 <= index0 < total)
            ):
                self.error(
                    "plan_item_index_range",
                    f"Plan item {idx} index0={index0} is outside total={total}",
                    path,
                )
            if item.get("index0_end") is not None:
                end = item.get("index0_end")
                if (
                    not isinstance(end, int)
                    or not isinstance(index0, int)
                    or end < index0
                ):
                    self.error(
                        "plan_item_index_end",
                        f"Plan item {idx} has invalid index0_end={end!r}",
                        path,
                    )
            geom = item.get("geometry")
            if isinstance(geom, dict):
                self.check_geometry(geom, path, context=f"plan.items[{idx}].geometry")
            elif geom is not None:
                self.error(
                    "plan_item_geometry_type",
                    f"Plan item {idx} geometry must be an object when present",
                    path,
                )

    def check_geometry(
        self, geometry: Mapping[str, Any], path: Path, *, context: str
    ) -> None:
        if not geometry:
            return
        kind = geometry.get("kind")
        if not kind:
            self.warn("geometry_kind_missing", f"{context}.kind is missing", path)
            return
        if kind in {"point", "grid_point", "point_target"}:
            if not any(
                geometry.get(key) is not None
                for key in ("target", "reference", "target_name", "offset")
            ):
                self.warn(
                    "point_geometry_missing_target",
                    f"{context} kind={kind!r} has no target/reference/target_name/offset",
                    path,
                )
        elif kind in {"scan_line", "scan_block_line"}:
            if geometry.get("start") is None:
                self.error(
                    "scan_geometry_missing_start",
                    f"{context} kind={kind!r} has no start",
                    path,
                )
            if geometry.get("stop") is None:
                self.error(
                    "scan_geometry_missing_stop",
                    f"{context} kind={kind!r} has no stop",
                    path,
                )
            self.check_line_index_fields(
                geometry,
                path,
                context=context,
                line_required=(kind == "scan_block_line"),
            )
        elif kind == "scan_block":
            lines = geometry.get("lines")
            if not isinstance(lines, list) or not lines:
                self.error(
                    "scan_block_missing_lines",
                    f"{context} kind='scan_block' requires non-empty lines",
                    path,
                )
            line_total = geometry.get("line_total")
            if (
                line_total is not None
                and isinstance(lines, list)
                and line_total != len(lines)
            ):
                self.error(
                    "scan_block_line_total",
                    f"{context}.line_total={line_total} but len(lines)={len(lines)}",
                    path,
                )
            if isinstance(lines, list):
                for idx, line in enumerate(lines):
                    if not isinstance(line, dict):
                        self.error(
                            "scan_block_line_not_object",
                            f"{context}.lines[{idx}] is not an object",
                            path,
                        )
                        continue
                    self.check_line_index_fields(
                        line,
                        path,
                        context=f"{context}.lines[{idx}]",
                        line_required=True,
                    )
        elif kind == "skydip_elevation":
            if (
                geometry.get("el_deg") is None
                and geometry.get("target_el_deg") is None
                and geometry.get("target") is None
            ):
                self.warn(
                    "skydip_geometry_missing_el",
                    f"{context} has no el_deg/target_el_deg/target",
                    path,
                )
        elif kind in {
            "current_position",
            "current_sky_position",
            "calibration_at_current_point",
            "calibration_at_skydip_start",
            "calibration_at_skydip_end",
            "none",
        }:
            return
        else:
            self.warn(
                "unknown_geometry_kind", f"Unknown {context}.kind: {kind!r}", path
            )

    def check_line_index_fields(
        self, obj: Mapping[str, Any], path: Path, *, context: str, line_required: bool
    ) -> None:
        line_total = obj.get("line_total")
        line_index = obj.get("line_index0")
        if line_required and line_index is None:
            self.error("line_index_missing", f"{context}.line_index0 is required", path)
        if line_total is not None:
            if not isinstance(line_total, int) or line_total <= 0:
                self.error(
                    "line_total_invalid",
                    f"{context}.line_total must be positive int, got {line_total!r}",
                    path,
                )
            if line_index is not None:
                if not isinstance(line_index, int):
                    self.error(
                        "line_index_type", f"{context}.line_index0 must be int", path
                    )
                elif isinstance(line_total, int) and not (0 <= line_index < line_total):
                    self.error(
                        "line_index_range",
                        f"{context}.line_index0={line_index} outside line_total={line_total}",
                        path,
                    )

    def check_events(self, events: Sequence[Mapping[str, Any]], path: Path) -> None:
        if not events:
            return
        prev_seq: Optional[int] = None
        prev_time: Optional[float] = None
        started = False
        finished = False
        active_items = 0
        active_integrations = 0
        for event in events:
            lineno = event.get("_lineno", "?")
            seq = event.get("seq")
            if not isinstance(seq, int):
                self.error(
                    "event_seq_type",
                    f"Event line {lineno} has non-int seq={seq!r}",
                    path,
                )
            elif prev_seq is not None and seq <= prev_seq:
                self.error(
                    "event_seq_order",
                    f"Event seq is not increasing at line {lineno}: {seq} after {prev_seq}",
                    path,
                )
            if isinstance(seq, int):
                prev_seq = seq
            ts = event.get("time_unix")
            if not isinstance(ts, (int, float)):
                self.error(
                    "event_time_type",
                    f"Event line {lineno} has non-numeric time_unix={ts!r}",
                    path,
                )
            elif prev_time is not None and float(ts) + 1e-6 < prev_time:
                self.error(
                    "event_time_order", f"Event time decreases at line {lineno}", path
                )
            if isinstance(ts, (int, float)):
                prev_time = float(ts)
            name = event.get("event")
            if not name:
                self.error(
                    "event_name_missing", f"Event line {lineno} has no event name", path
                )
                continue
            if name == "observation_running" or name == "observation_started":
                started = True
            if name in {
                "observation_finished",
                "observation_error",
                "observation_aborted",
            }:
                finished = True
            if name == "plan_item_started":
                active_items += 1
            elif name == "plan_item_finished":
                active_items = max(0, active_items - 1)
            elif name == "plan_item_failed":
                active_items = max(0, active_items - 1)
            elif name == "integration_started":
                active_integrations += 1
                if not event.get("metadata_position"):
                    self.error(
                        "integration_event_no_metadata",
                        f"integration_started at line {lineno} has no metadata_position",
                        path,
                    )
            elif name == "integration_finished":
                active_integrations = max(0, active_integrations - 1)
            elif name == "integration_failed":
                active_integrations = max(0, active_integrations - 1)
        if self.require_final and not finished:
            self.error(
                "events_not_final",
                "require-final was set but final observation event was not found",
                path,
            )
        if active_integrations > 0:
            self.warn(
                "unclosed_integration",
                f"Event log has {active_integrations} integration_started without matching finish/fail",
                path,
            )
        if active_items > 0:
            self.warn(
                "unclosed_plan_item",
                f"Event log has {active_items} plan_item_started without matching finish/fail",
                path,
            )
        if not any(event.get("event") == "plan_defined" for event in events):
            self.warn("plan_defined_event_missing", "No plan_defined event found", path)
        if not started:
            self.warn(
                "observation_running_event_missing",
                "No observation_running/observation_started event found",
                path,
            )

    def cross_check(
        self,
        snapshot: Optional[Mapping[str, Any]],
        plan_doc: Optional[Mapping[str, Any]],
        events: Sequence[Mapping[str, Any]],
        record_dir: Path,
    ) -> None:
        if snapshot is None:
            return
        obs = snapshot.get("observation") or {}
        record_name = obs.get("record_name")
        if record_name:
            # The live progress directory is sanitized, while final sidecars can
            # also be copied directly into the real NECST record directory whose
            # name is the raw record_name.  Accept both forms so --strict works
            # for copied record sidecars with spaces or non-ASCII target names.
            accepted_names = {safe_name(record_name), str(record_name)}
            if record_dir.name not in accepted_names:
                self.warn(
                    "record_dir_name_mismatch",
                    f"record directory {record_dir.name!r} matches neither raw record_name {str(record_name)!r} nor safe path {safe_name(record_name)!r}",
                    record_dir,
                )
        plan = snapshot.get("plan") or {}
        if plan_doc and isinstance(plan_doc.get("items"), list):
            items = plan_doc["items"]
            index0 = plan.get("index0")
            if isinstance(index0, int) and 0 <= index0 < len(items):
                item = items[index0]
                if isinstance(item, dict):
                    if (
                        plan.get("item_uid")
                        and item.get("item_uid")
                        and plan.get("item_uid") != item.get("item_uid")
                    ):
                        # Merged scan blocks intentionally use a synthetic block
                        # item_uid in the live snapshot while observation_plan
                        # keeps the original waypoint items for map/line display.
                        # This is expected and must not make --strict fail.
                        if (
                            plan.get("drive_kind") == "scan_block"
                            or plan.get("index0_end") is not None
                        ):
                            self.info(
                                "snapshot_plan_uid_differs_for_scan_block",
                                f"snapshot scan_block item_uid={plan.get('item_uid')!r} covers plan.items[index0].item_uid={item.get('item_uid')!r}",
                                record_dir,
                            )
                        else:
                            self.warn(
                                "snapshot_plan_uid_mismatch",
                                f"snapshot plan.item_uid={plan.get('item_uid')!r} does not match plan.items[index0].item_uid={item.get('item_uid')!r}",
                                record_dir,
                            )
            total = plan.get("total")
            if isinstance(total, int) and total != len(items):
                has_merged_item = bool(plan.get("index0_end") is not None) or any(
                    isinstance(item, dict) and item.get("index0_end") is not None
                    for item in items
                )
                # Merged scan blocks intentionally compress multiple source
                # waypoints into one plan item.  In that case snapshot.total may
                # represent the original waypoint total while observation_plan
                # contains block-level items.  Treat it as informational so
                # --strict still accepts valid scan-block progress.
                if has_merged_item:
                    self.info(
                        "snapshot_total_differs_for_merged_block",
                        f"snapshot plan.total={total} while plan item count={len(items)}",
                        record_dir,
                    )
                else:
                    self.warn(
                        "snapshot_total_mismatch",
                        f"snapshot plan.total={total} but plan item count={len(items)}",
                        record_dir,
                    )
        if events:
            last_event = events[-1].get("event")
            state = (snapshot.get("lifecycle") or {}).get("state")
            if state == "finished" and last_event != "observation_finished":
                self.warn(
                    "final_event_mismatch",
                    f"snapshot is finished but last event is {last_event!r}",
                    record_dir,
                )
            if state == "error" and last_event != "observation_error":
                self.warn(
                    "final_event_mismatch",
                    f"snapshot is error but last event is {last_event!r}",
                    record_dir,
                )
            if state == "aborted" and last_event != "observation_aborted":
                self.warn(
                    "final_event_mismatch",
                    f"snapshot is aborted but last event is {last_event!r}",
                    record_dir,
                )

    def check_record_dir(self, record_dir: Path) -> Dict[str, Any]:
        snapshot_path = record_dir / "observation_progress.json"
        plan_path = record_dir / "observation_plan.json"
        events_path = record_dir / "observation_events.jsonl"
        snapshot = self.load_json(snapshot_path, required=True)
        plan = self.load_json(plan_path, required=True)
        events = self.load_events(events_path, required=True)
        self.check_snapshot(snapshot, snapshot_path)
        self.check_plan(plan, plan_path)
        self.check_events(events, events_path)
        self.cross_check(snapshot, plan, events, record_dir)
        return {
            "record_dir": str(record_dir),
            "snapshot": str(snapshot_path),
            "plan": str(plan_path),
            "events": str(events_path),
            "event_count": len(events),
            "ok": self.ok,
        }


def current_record_name(root: Path) -> Optional[str]:
    try:
        text = (
            (root / "current_observation_record.txt")
            .read_text(encoding="utf-8")
            .strip()
        )
        return text or None
    except Exception:
        return None


def latest_record_dir(root: Path) -> Optional[Path]:
    record = current_record_name(root)
    if record:
        candidate = root / safe_name(record)
        if candidate.exists():
            return candidate
    candidates = [p for p in root.iterdir() if p.is_dir()] if root.exists() else []
    candidates.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return candidates[0] if candidates else None


def resolve_record_dir(args: argparse.Namespace) -> Optional[Path]:
    if args.path:
        path = Path(args.path).expanduser()
        if path.is_file():
            return path.parent
        return path
    root = progress_root(args.root)
    return latest_record_dir(root)


def render_text(summary: Mapping[str, Any], findings: Iterable[Finding]) -> str:
    lines = []
    status = "OK" if summary.get("ok") else "NG"
    lines.append(f"NECST progress-check: {status}")
    if summary.get("record_dir"):
        lines.append(f"record_dir : {summary['record_dir']}")
    if summary.get("event_count") is not None:
        lines.append(f"events     : {summary['event_count']}")
    grouped = {"error": [], "warning": [], "info": []}
    for item in findings:
        grouped.setdefault(item.severity, []).append(item)
    lines.append(f"errors     : {len(grouped.get('error', []))}")
    lines.append(f"warnings   : {len(grouped.get('warning', []))}")
    for severity in ("error", "warning", "info"):
        items = grouped.get(severity, [])
        if not items:
            continue
        lines.append("")
        lines.append(severity.upper())
        for item in items:
            loc = f" [{item.file}]" if item.file else ""
            lines.append(f"  - {item.code}: {item.message}{loc}")
    return "\n".join(lines)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Validate NECST observation progress sidecars"
    )
    parser.add_argument(
        "--root",
        default=None,
        help="Progress root directory (default: NECST_PROGRESS_ROOT or /tmp/necst_progress)",
    )
    parser.add_argument(
        "--path", default=None, help="Record progress directory, or a file inside it"
    )
    parser.add_argument(
        "--latest",
        action="store_true",
        help="Validate latest/current record directory (default when --path is omitted)",
    )
    parser.add_argument(
        "--strict", action="store_true", help="Treat warnings as errors"
    )
    parser.add_argument(
        "--require-final",
        action="store_true",
        help="Require final lifecycle/event state: finished, error, or aborted",
    )
    parser.add_argument(
        "--json", action="store_true", help="Print machine-readable result JSON"
    )
    return parser


def main(argv: Optional[List[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    record_dir = resolve_record_dir(args)
    checker = ProgressCheck(strict=args.strict, require_final=args.require_final)
    if record_dir is None:
        checker.error("no_record_dir", "No progress record directory found")
        summary = {"record_dir": None, "event_count": 0, "ok": False}
    elif not record_dir.exists():
        checker.error(
            "record_dir_missing",
            f"Progress record directory does not exist: {record_dir}",
            record_dir,
        )
        summary = {"record_dir": str(record_dir), "event_count": 0, "ok": False}
    elif not record_dir.is_dir():
        checker.error(
            "record_dir_not_directory",
            f"Progress path is not a directory: {record_dir}",
            record_dir,
        )
        summary = {"record_dir": str(record_dir), "event_count": 0, "ok": False}
    else:
        summary = checker.check_record_dir(record_dir)
        summary["ok"] = checker.ok
    payload = {
        "ok": checker.ok,
        "summary": summary,
        "findings": [item.as_dict() for item in checker.findings],
    }
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
    else:
        print(render_text(summary, checker.findings))
    return 0 if checker.ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
