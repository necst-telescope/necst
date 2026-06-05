"""Shared read-only status model for NECST operator/progress UIs.

This module intentionally contains no telescope command path.  It reads the
progress sidecar files and optional live telemetry snapshots, then returns a
canonical dictionary that can be consumed by both the diagnostic progress
monitor and the future operator console.
"""

from __future__ import annotations

import copy
import hashlib
import json
import math
import os
import re
import time
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional


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


def live_status_max_age_sec() -> float:
    """Maximum age for live ROS status before it is treated as stale."""
    try:
        return max(
            0.2, float(os.environ.get("NECST_PROGRESS_LIVE_STATUS_MAX_AGE_SEC", "2.0"))
        )
    except Exception:
        return 2.0


def strip_record_file_header(text: str) -> str:
    """Remove leading NECST FileWriter comment headers if present."""
    lines = text.splitlines()
    start = 0
    while start < len(lines) and (
        not lines[start].strip() or lines[start].lstrip().startswith("#")
    ):
        start += 1
    return "\n".join(lines[start:])


def read_json(path: Optional[Path]) -> Optional[Dict[str, Any]]:
    if path is None:
        return None
    try:
        return json.loads(strip_record_file_header(path.read_text(encoding="utf-8")))
    except FileNotFoundError:
        return None
    except json.JSONDecodeError as exc:
        return {"_error": f"invalid JSON in {path}: {exc}"}
    except Exception as exc:
        return {"_error": f"failed to read {path}: {exc}"}


def read_jsonl(path: Optional[Path], limit: Optional[int] = None) -> List[Dict[str, Any]]:
    if path is None:
        return []
    try:
        raw_lines = path.read_text(encoding="utf-8").splitlines()
    except Exception:
        return []
    entries: List[Dict[str, Any]] = []
    for line in raw_lines:
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        try:
            payload = json.loads(line)
        except Exception:
            continue
        if isinstance(payload, dict):
            entries.append(payload)
    if limit is not None and limit >= 0:
        return entries[-limit:]
    return entries


def current_snapshot_path(root: Path) -> Path:
    return root / "current_observation_progress.json"


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


def progress_record_dir(root: Path, record_name: Optional[Any]) -> Optional[Path]:
    """Return the progress sidecar directory for a record name, if known."""
    record = str(record_name or "").strip()
    if not record:
        record = current_record_name(root) or ""
    if not record:
        return None
    return root / safe_name(record)


def default_record_root() -> Path:
    """Return the NECST FileWriter root used by recorder.py.

    The recorder defaults to ``$NECST_RECORD_ROOT`` or ``~/data``.  This helper
    mirrors that logic for display only; it does not create directories.
    """
    return Path(os.environ.get("NECST_RECORD_ROOT", str(Path.home() / "data"))).expanduser()


def recording_data_dir(record_name: Optional[Any]) -> Optional[Path]:
    """Return the expected observation data directory for a record name.

    NECST RecorderController calls ``Recorder(record_root).start_recording(name)``.
    When ``name`` is relative, files are written under the record root; if ``name``
    is already absolute, pathlib keeps it absolute.
    """
    record = str(record_name or "").strip()
    if not record or record == "-":
        return None
    path = Path(record).expanduser()
    if path.is_absolute():
        return path
    return default_record_root() / path


def latest_events_path(
    root: Path, snapshot: Optional[Mapping[str, Any]] = None
) -> Optional[Path]:
    record = None
    if snapshot:
        record = (snapshot.get("observation") or {}).get("record_name")
    record = record or current_record_name(root)
    if record:
        safe = safe_name(record)
        path = root / safe / "observation_events.jsonl"
        if path.exists():
            return path
    candidates = sorted(
        root.glob("*/observation_events.jsonl"),
        key=lambda p: p.stat().st_mtime if p.exists() else 0,
        reverse=True,
    )
    return candidates[0] if candidates else None


def latest_plan_path(
    root: Path, snapshot: Optional[Mapping[str, Any]] = None
) -> Optional[Path]:
    record = None
    if snapshot:
        record = (snapshot.get("observation") or {}).get("record_name")
    record = record or current_record_name(root)
    if record:
        safe = safe_name(record)
        path = root / safe / "observation_plan.json"
        if path.exists():
            return path
    candidates = sorted(
        root.glob("*/observation_plan.json"),
        key=lambda p: p.stat().st_mtime if p.exists() else 0,
        reverse=True,
    )
    return candidates[0] if candidates else None


def read_recent_events(path: Optional[Path], limit: int) -> List[Dict[str, Any]]:
    if limit <= 0:
        return []
    return read_jsonl(path, limit=limit)


def read_all_events(path: Optional[Path]) -> List[Dict[str, Any]]:
    return read_jsonl(path, limit=None)


def _finite_float(value: Any) -> Optional[float]:
    try:
        out = float(value)
    except Exception:
        return None
    return out if math.isfinite(out) else None


def _as_mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


FINAL_LIFECYCLE_STATES = {"finished", "error", "aborted"}


def apply_dynamic_remaining(
    snapshot: Optional[Dict[str, Any]], *, now_unix: Optional[float] = None
) -> Optional[Dict[str, Any]]:
    """Return a display/API snapshot with ETA counted down from completion time."""
    if not isinstance(snapshot, dict):
        return snapshot
    timing = snapshot.get("time")
    lifecycle = snapshot.get("lifecycle") if isinstance(snapshot.get("lifecycle"), dict) else {}
    if not isinstance(timing, dict):
        return snapshot
    state = str(lifecycle.get("state") or "").lower()
    if state in FINAL_LIFECYCLE_STATES:
        if timing.get("estimated_remaining_sec") is not None:
            timing["estimated_remaining_sec"] = 0.0
        return snapshot
    completion = _finite_float(timing.get("estimated_completion_unix"))
    if completion is not None:
        timing["estimated_remaining_sec"] = max(
            0.0, completion - float(now_unix if now_unix is not None else time.time())
        )
    return snapshot

def _live_payload_has_position(live_payload: Mapping[str, Any]) -> bool:
    """Return True when live ROS telemetry has enough antenna data to display.

    Observation progress files may legitimately be absent while the telescope is
    idle.  In that case the dashboard should still show current encoder Az/El
    when ROS is running, but it must not fabricate an observation merely because
    unrelated topics such as weather or queue status are available.
    """
    if not isinstance(live_payload, Mapping):
        return False
    pointing = (
        live_payload.get("pointing_status")
        if isinstance(live_payload.get("pointing_status"), Mapping)
        else {}
    )
    encoder = (
        live_payload.get("encoder")
        if isinstance(live_payload.get("encoder"), Mapping)
        else {}
    )

    # An idle snapshot must represent the actual telescope position.  A live
    # command-only sample may be stale or merely the last target, so it must not
    # fabricate an "idle antenna position" when no encoder/pointing encoder value
    # is available.
    for payload, keys in (
        (pointing, ("enc_az_deg", "enc_el_deg")),
        (encoder, ("encoder_lon_deg", "encoder_lat_deg")),
    ):
        if any(_finite_float(payload.get(key)) is not None for key in keys):
            return True
    return False


def make_idle_live_snapshot(
    live_payload: Mapping[str, Any], *, now_unix: Optional[float] = None
) -> Dict[str, Any]:
    """Build a minimal display-only snapshot for idle live telemetry.

    This snapshot is used only by progress.py.  It is not written back to
    ``current_observation_progress.json`` and therefore does not affect the
    observation runtime.
    """
    now = float(now_unix if now_unix is not None else time.time())
    return {
        "observation": {
            "type": "idle",
            "record_name": "-",
            "obs_file": "-",
            "target": "-",
        },
        "lifecycle": {
            "state": "idle",
            "status": "idle",
        },
        "plan": {
            "label": "no active observation",
            "mode": "-",
            "role": "idle",
            "total": 0,
        },
        "activity": {
            "phase": "IDLE",
            "drive_kind": "idle",
            "motion_stage": "idle",
            "data_state": "no active observation",
        },
        "geometry": {
            "kind": "idle",
            "frame": "altaz",
            "unit": "deg",
        },
        "data": {
            "expected_metadata_position": "-",
            "expected_metadata_id": "-",
        },
        "time": {
            "updated_at_unix": now,
            "elapsed_sec": None,
            "estimated_remaining_sec": None,
            "remaining_method": "idle/live-telemetry",
            "remaining_confidence": "n/a",
        },
        "display": {
            "idle_live_telemetry": True,
            "idle_message": "No active observation; showing live antenna telemetry.",
        },
    }


def merge_live_telemetry(
    snapshot: Optional[Dict[str, Any]], live: Optional[Mapping[str, Any]]
) -> Optional[Dict[str, Any]]:
    """Return a display-only snapshot augmented with live ROS telemetry.

    The progress JSON files remain the source of truth while an observation is
    active.  Live ROS values are merged only in the CLI/web process so the
    observation runtime stays free of high-frequency subscriptions.

    If no progress snapshot exists but live antenna position telemetry is
    available, return a minimal ``idle`` snapshot so observers can still see
    current encoder Az/El before starting an observation.  This idle snapshot is
    display-only and is never written to disk.
    """
    if not live:
        return snapshot
    live_payload = dict(live)
    if not isinstance(snapshot, dict):
        if not _live_payload_has_position(live_payload):
            return snapshot
        snapshot = make_idle_live_snapshot(live_payload)
    out = copy.deepcopy(snapshot)
    out["live"] = live_payload

    antenna_update: Dict[str, Any] = {}
    cmd = (
        live_payload.get("command")
        if isinstance(live_payload.get("command"), dict)
        else {}
    )
    enc = (
        live_payload.get("encoder")
        if isinstance(live_payload.get("encoder"), dict)
        else {}
    )
    pointing = (
        live_payload.get("pointing_status")
        if isinstance(live_payload.get("pointing_status"), dict)
        else {}
    )
    az_unwrap = (
        live_payload.get("az_unwrap_status")
        if isinstance(live_payload.get("az_unwrap_status"), dict)
        else {}
    )
    # Always start from direct encoder telemetry.  Command display is stricter:
    # it must represent a real, fresh command status topic (or a valid
    # pointing_status command), not an observation plan, old progress snapshot,
    # or interpolated section.  Otherwise Command Az/El can keep moving after
    # abort even when no command topic is being published.
    command_valid = False
    command_source = "none"
    live_ages = (
        live_payload.get("last_message_age_sec")
        if isinstance(live_payload.get("last_message_age_sec"), dict)
        else {}
    )
    command_topic_age = _topic_receipt_age_sec(live_payload, "command")
    command_topic_fresh = _topic_received_fresh(live_payload, "command")
    direct_cmd_az = _finite_float(
        _first_present(cmd, "command_lon_deg", "cmd_az_deg", "az_cmd_deg")
    )
    direct_cmd_el = _finite_float(
        _first_present(cmd, "command_lat_deg", "cmd_el_deg", "el_cmd_deg")
    )
    if direct_cmd_az is not None and direct_cmd_el is not None and command_topic_fresh:
        antenna_update.update(cmd)
        command_valid = True
        command_source = "command_topic"
    if enc:
        antenna_update.update(enc)

    if pointing:
        antenna_update["pointing_status_available"] = True
        pointing_valid = bool(pointing.get("valid", False))
        antenna_update["pointing_status_valid"] = pointing_valid
        cmd_az = _finite_float(pointing.get("cmd_az_deg"))
        cmd_el = _finite_float(pointing.get("cmd_el_deg"))
        enc_az = _finite_float(pointing.get("enc_az_deg"))
        enc_el = _finite_float(pointing.get("enc_el_deg"))
        pointing_cmd_age = _finite_float(pointing.get("cmd_age_sec"))
        pointing_cmd_fresh = (
            pointing_cmd_age is not None
            and pointing_cmd_age <= live_status_max_age_sec()
        )
        pointing_command_stale = bool(pointing.get("command_stale", False))
        if (
            not command_valid
            and pointing_valid
            and not pointing_command_stale
            and pointing_cmd_fresh
            and cmd_az is not None
            and cmd_el is not None
        ):
            antenna_update["command_lon_deg"] = cmd_az
            antenna_update["command_lat_deg"] = cmd_el
            antenna_update["command_frame"] = "altaz"
            antenna_update["command_unit"] = "deg"
            antenna_update["command_time_unix"] = pointing.get("command_time_unix")
            command_valid = True
            command_source = "pointing_status"
        if enc_az is not None and enc_el is not None:
            antenna_update["encoder_lon_deg"] = enc_az
            antenna_update["encoder_lat_deg"] = enc_el
            antenna_update["encoder_frame"] = "altaz"
            antenna_update["encoder_unit"] = "deg"
            antenna_update["encoder_time_unix"] = pointing.get("encoder_time_unix")
        antenna_update["tracking_status_available"] = True
        antenna_update["tracking_ok"] = bool(pointing.get("tracking_ok", False))
        antenna_update["tracking_error_deg"] = pointing.get("tracking_error_deg")
        antenna_update["tracking_status_time_unix"] = pointing.get("publish_time_unix")
        antenna_update["tracking_status_age_sec"] = (
            max(0.0, time.time() - pointing.get("publish_time_unix", time.time()))
            if pointing.get("publish_time_unix") is not None
            else None
        )
        antenna_update["tracking_threshold_deg"] = pointing.get(
            "tracking_threshold_deg"
        )
        antenna_update["command_stale"] = bool(pointing.get("command_stale", False))
        antenna_update["encoder_stale"] = bool(pointing.get("encoder_stale", False))
        antenna_update["command_age_sec"] = pointing.get("cmd_age_sec")
        antenna_update["encoder_age_sec"] = pointing.get("enc_age_sec")
        antenna_update["delta_az_deg"] = pointing.get("delta_az_deg")
        antenna_update["delta_el_deg"] = pointing.get("delta_el_deg")
        antenna_update["delta_az_cos_el_deg"] = pointing.get("delta_az_cos_el_deg")
        antenna_update["pointing_basis"] = pointing.get("basis")
    if az_unwrap and az_unwrap.get("enabled"):
        antenna_update["az_unwrap"] = dict(az_unwrap)
        antenna_update["az_unwrap_enabled"] = True
        antenna_update["az_unwrap_valid"] = bool(az_unwrap.get("valid", False))
        antenna_update["az_unwrap_branch"] = az_unwrap.get("branch")
        antenna_update["az_unwrap_raw_az_deg"] = az_unwrap.get("raw_az_deg")
        antenna_update["az_unwrap_state"] = az_unwrap.get("state")
        antenna_update["az_unwrap_reason"] = az_unwrap.get("reason")
    tracking = (
        live_payload.get("tracking")
        if isinstance(live_payload.get("tracking"), dict)
        else {}
    )
    if tracking:
        terr = _finite_float(tracking.get("error_deg"))
        tstat = _finite_float(tracking.get("time_unix"))
        antenna_update["tracking_status_available"] = True
        antenna_update["tracking_ok"] = bool(tracking.get("ok", False))
        if terr is not None:
            antenna_update["tracking_error_deg"] = terr
        if tstat is not None:
            antenna_update["tracking_status_time_unix"] = tstat
            antenna_update["tracking_status_age_sec"] = max(0.0, time.time() - tstat)
    else:
        # Do not compute a tracking error from the latest command and latest
        # encoder samples here.  altaz_cmd is a time-tagged command stream and
        # its newest sample is not necessarily the command at the encoder time.
        # NECST's antenna_tracking topic already performs the correct comparison.
        antenna_update.setdefault("tracking_status_available", False)
    if antenna_update:
        antenna_section = out.setdefault("antenna", {})
        if not command_valid:
            _clear_command_fields(antenna_section)
            _clear_command_fields(antenna_update)
        antenna_update["command_valid"] = bool(command_valid)
        antenna_update["command_source"] = command_source
        if command_topic_age is not None:
            antenna_update["command_topic_age_sec"] = command_topic_age
        out.setdefault("antenna", {}).update(antenna_update)
    weather_payload = (
        live_payload.get("weather")
        if isinstance(live_payload.get("weather"), dict)
        else {}
    )
    if weather_payload:
        out.setdefault("weather", {}).update(weather_payload)
    queue_payload = (
        live_payload.get("queue_status")
        if isinstance(live_payload.get("queue_status"), dict)
        else {}
    )
    if queue_payload:
        out.setdefault("antenna_command_queue", {}).update(queue_payload)
    spec_payload = (
        live_payload.get("spectrometer_status")
        if isinstance(live_payload.get("spectrometer_status"), dict)
        else {}
    )
    if spec_payload:
        out.setdefault("spectrometer", {}).update(spec_payload)
    chopper_payload = (
        live_payload.get("chopper_status")
        if isinstance(live_payload.get("chopper_status"), dict)
        else {}
    )
    if chopper_payload:
        out.setdefault("chopper", {}).update(chopper_payload)

    lifecycle = out.get("lifecycle") if isinstance(out.get("lifecycle"), dict) else {}
    final_state = str(lifecycle.get("state") or "").lower() in {
        "finished",
        "error",
        "aborted",
    }
    section_status = (
        live_payload.get("section_status")
        if isinstance(live_payload.get("section_status"), dict)
        else {}
    )
    ctrl = (
        live_payload.get("control")
        if isinstance(live_payload.get("control"), dict)
        else {}
    )
    if section_status and not final_state:
        activity = out.setdefault("activity", {})
        geometry = out.setdefault("geometry", {})
        data = out.setdefault("data", {})
        # Preserve the observation-progress line index before live antenna
        # section status may overwrite geometry.current_line_index0.  The
        # former answers "which planned OTF item is being processed", while
        # the latter answers "which antenna scan-line section is active now".
        # Mixing them makes Plan View jump to the next line during OFF,
        # standby, or accelerate intervals.
        progress_line_index = geometry.get("current_line_index0")
        if isinstance(progress_line_index, int) and progress_line_index >= 0:
            geometry.setdefault("progress_line_index0", progress_line_index)
        section_kind = section_status.get("section_kind")
        section_label = section_status.get("section_label")
        line_index = section_status.get("line_index")
        if section_kind not in (None, ""):
            activity["motion_stage"] = section_kind
            activity["control_section_kind"] = section_kind
        if section_label not in (None, ""):
            activity["control_section_label"] = section_label
            geometry["current_line_label"] = section_label
        if section_status.get("control_id") not in (None, ""):
            activity["control_id"] = section_status.get("control_id")
        activity["control_tight"] = bool(section_status.get("tight", False))
        activity["control_section_uid"] = section_status.get("section_uid")
        activity["control_section_plan_index"] = section_status.get(
            "section_plan_index"
        )
        activity["control_section_sequence_index"] = section_status.get(
            "section_sequence_index"
        )
        activity["control_section_line_index"] = line_index
        activity["control_status_basis"] = section_status.get("status_basis")
        activity["control_section_active"] = bool(section_status.get("active", False))
        activity["control_section_science_line"] = bool(
            section_status.get("science_line", False)
        )
        activity["control_section_interrupt_ok"] = bool(
            section_status.get("interrupt_ok", False)
        )
        activity["control_section_start_unix"] = section_status.get(
            "section_start_unix"
        )
        activity["control_section_stop_unix"] = section_status.get("section_stop_unix")
        activity["control_section_duration_sec"] = section_status.get(
            "section_duration_sec"
        )
        activity["control_section_command_time_unix"] = section_status.get(
            "command_time_unix"
        )
        activity["control_section_query_time_unix"] = section_status.get(
            "query_time_unix"
        )
        activity["control_section_publish_time_unix"] = section_status.get(
            "publish_time_unix"
        )
        pub_time = section_status.get("publish_time_unix")
        try:
            section_age = max(0.0, time.time() - float(pub_time))
        except Exception:
            section_age = None
        activity["control_section_age_sec"] = section_age
        activity["control_section_fresh"] = bool(
            section_age is not None and section_age <= live_status_max_age_sec()
        )
        if section_status.get("fraction_valid"):
            activity["control_section_fraction"] = section_status.get(
                "nominal_fraction"
            )
        if isinstance(line_index, int) and line_index >= 0:
            geometry["current_line_index0"] = line_index
            if data.get("latest_control_line_index") is None:
                data["latest_control_line_index"] = line_index
        if section_status.get("geometry_valid"):
            geometry["live_section_geometry"] = {
                "frame": section_status.get("section_frame"),
                "unit": section_status.get("section_unit"),
                "start": [
                    section_status.get("section_start_lon_deg"),
                    section_status.get("section_start_lat_deg"),
                ],
                "stop": [
                    section_status.get("section_stop_lon_deg"),
                    section_status.get("section_stop_lat_deg"),
                ],
                "speed_deg_per_sec": section_status.get("section_speed_deg_per_sec"),
            }
    elif ctrl and not final_state:
        activity = out.setdefault("activity", {})
        geometry = out.setdefault("geometry", {})
        section_kind = ctrl.get("section_kind")
        section_label = ctrl.get("section_label")
        ctrl_id = ctrl.get("id")
        line_index = ctrl.get("line_index")
        if section_kind not in (None, ""):
            activity["motion_stage"] = section_kind
            activity["control_section_kind"] = section_kind
        if section_label not in (None, ""):
            activity["control_section_label"] = section_label
            geometry["current_line_label"] = section_label
        if ctrl_id not in (None, ""):
            activity["control_id"] = ctrl_id
        if ctrl.get("tight") is not None:
            activity["control_tight"] = bool(ctrl.get("tight"))
        activity["control_section_line_index"] = line_index
        if isinstance(line_index, int) and line_index >= 0:
            geometry["current_line_index0"] = line_index
            data = out.setdefault("data", {})
            # Do not overwrite expected_metadata_line_index for non-scan-block
            # integrations; only fill a display helper when no explicit value is
            # present.
            if data.get("latest_control_line_index") is None:
                data["latest_control_line_index"] = line_index
    _live_truth_scrub_stale_motion(out, live_payload, command_valid=command_valid)
    return apply_dynamic_remaining(out)





def _topic_age_sec(
    live_payload: Mapping[str, Any],
    key: str,
    payload: Optional[Mapping[str, Any]] = None,
    *,
    now_unix: Optional[float] = None,
) -> Optional[float]:
    """Return the age of a live ROS topic sample, if known.

    Console live telemetry records ``last_message_age_sec`` explicitly.  The
    standalone progress monitor may also carry a payload timestamp.  Treat
    missing age as unknown, not fresh.
    """
    ages = live_payload.get("last_message_age_sec")
    if isinstance(ages, Mapping):
        age = _finite_float(ages.get(key))
        if age is not None:
            return age
    if payload is not None:
        now = float(now_unix if now_unix is not None else time.time())
        for ts_key in (
            "publish_time_unix",
            "time_unix",
            "command_time_unix",
            "encoder_time_unix",
            "query_time_unix",
        ):
            ts = _finite_float(payload.get(ts_key))
            if ts is not None:
                return max(0.0, now - ts)
    return None


def _topic_receipt_age_sec(
    live_payload: Mapping[str, Any],
    key: str,
) -> Optional[float]:
    """Return age since the local subscriber actually received this topic.

    This intentionally does *not* use timestamps carried inside the message.
    For antenna command and control topics, message timestamps may describe the
    planned command time rather than the wall-clock arrival time.  Using those
    timestamps made old or interpolated command data look live after abort.
    """
    ages = live_payload.get("last_message_age_sec")
    if isinstance(ages, Mapping):
        age = _finite_float(ages.get(key))
        if age is not None:
            return age
    return None


def _topic_received_fresh(
    live_payload: Mapping[str, Any],
    key: str,
) -> bool:
    age = _topic_receipt_age_sec(live_payload, key)
    return bool(age is not None and age <= live_status_max_age_sec())


def _topic_is_fresh(
    live_payload: Mapping[str, Any],
    key: str,
    payload: Optional[Mapping[str, Any]] = None,
) -> bool:
    age = _topic_age_sec(live_payload, key, payload)
    return bool(age is not None and age <= live_status_max_age_sec())


def _clear_command_fields(mapping: Dict[str, Any]) -> None:
    for key in (
        "command_lon_deg",
        "command_lat_deg",
        "cmd_az_deg",
        "cmd_el_deg",
        "az_cmd_deg",
        "el_cmd_deg",
        "command_frame",
        "command_unit",
        "command_time_unix",
        "command_name",
    ):
        mapping.pop(key, None)


def _live_truth_scrub_stale_motion(
    out: Dict[str, Any],
    live_payload: Mapping[str, Any],
    *,
    command_valid: bool,
) -> None:
    """Remove stale planned/sidecar motion fields when live ROS says idle.

    The progress sidecar can legitimately lag after ``necst abort`` or a local
    launcher failure.  In operator-facing displays, command and motion must be
    shown only when backed by fresh live command / section / queue status.
    """
    if not isinstance(out, dict) or not isinstance(live_payload, Mapping):
        return

    antenna = out.setdefault("antenna", {})
    if isinstance(antenna, dict) and not command_valid:
        _clear_command_fields(antenna)
        antenna["command_valid"] = False
        antenna.setdefault("command_source", "none")

    queue = live_payload.get("queue_status") if isinstance(live_payload.get("queue_status"), Mapping) else {}
    section = live_payload.get("section_status") if isinstance(live_payload.get("section_status"), Mapping) else {}
    control = live_payload.get("control") if isinstance(live_payload.get("control"), Mapping) else {}

    queue_fresh = _topic_received_fresh(live_payload, "queue_status")
    try:
        queue_depth = int(queue.get("queue_depth", 0)) if isinstance(queue, Mapping) else 0
    except Exception:
        queue_depth = 0
    queue_active = bool(queue_fresh and (queue.get("active") or queue_depth > 0))

    section_fresh = _topic_received_fresh(live_payload, "section_status")
    section_active = bool(section_fresh and section.get("active"))

    control_fresh = _topic_received_fresh(live_payload, "control")
    control_active = bool(control_fresh and (control.get("controlled") or control.get("tight")))

    live_motion_active = bool(command_valid or queue_active or section_active or control_active)
    activity = out.setdefault("activity", {})
    if not isinstance(activity, dict):
        return
    activity["live_motion_active"] = live_motion_active
    activity["live_motion_source"] = (
        "command" if command_valid else
        "queue_status" if queue_active else
        "section_status" if section_active else
        "control_status" if control_active else
        "none"
    )

    # If fresh live status does not support motion, old sidecar/planned values
    # such as ON/line/moving must not remain visible as current state.
    if live_motion_active:
        return

    for key in (
        "motion_stage",
        "drive_kind",
        "active_task",
        "control_section_kind",
        "control_section_label",
        "control_id",
        "control_section_uid",
        "control_status_basis",
        "control_section_start_unix",
        "control_section_stop_unix",
        "control_section_command_time_unix",
        "control_section_publish_time_unix",
        "control_section_query_time_unix",
        "control_section_duration_sec",
        "control_section_fraction",
        "phase",
    ):
        activity.pop(key, None)
    activity["active_task"] = "idle"
    activity["phase"] = "IDLE"
    activity["motion_stage"] = "idle"
    activity["drive_kind"] = "idle"
    activity["control_section_active"] = False
    activity["control_section_fresh"] = False
    activity["control_section_age_sec"] = _topic_age_sec(live_payload, "section_status", section)

    geometry = out.get("geometry")
    if isinstance(geometry, dict):
        for key in (
            "current_line_label",
            "live_section_geometry",
        ):
            geometry.pop(key, None)

def _first_present(mapping: Mapping[str, Any], *keys: str) -> Any:
    for key in keys:
        value = mapping.get(key)
        if value not in (None, ""):
            return value
    return None


def _age_sec(timestamp_unix: Any, *, now_unix: float) -> Optional[float]:
    ts = _finite_float(timestamp_unix)
    if ts is None:
        return None
    return max(0.0, now_unix - ts)


def _progress_percent(snapshot: Mapping[str, Any]) -> Optional[float]:
    plan = _as_mapping(snapshot.get("plan"))
    for key in ("percent", "completion_percent", "progress_percent"):
        value = _finite_float(plan.get(key))
        if value is not None:
            return max(0.0, min(100.0, value))
    total = _finite_float(plan.get("total"))
    completed = _finite_float(
        _first_present(plan, "completed", "done", "finished", "current_index")
    )
    if total is not None and total > 0 and completed is not None:
        return max(0.0, min(100.0, 100.0 * completed / total))
    return None


def build_operator_status(
    snapshot: Optional[Mapping[str, Any]],
    *,
    plan: Optional[Mapping[str, Any]] = None,
    events: Optional[List[Mapping[str, Any]]] = None,
    paths: Optional[Mapping[str, Any]] = None,
    server_time_unix: Optional[float] = None,
) -> Dict[str, Any]:
    """Build a compact, read-only status summary for operator-facing UIs.

    The output is deliberately conservative: missing fields are represented as
    ``None`` or ``unknown`` rather than guessed.  It must be safe to call while
    the telescope is idle and while ROS/neclib are unavailable.
    """
    now = float(server_time_unix if server_time_unix is not None else time.time())
    snap = snapshot if isinstance(snapshot, Mapping) else {}
    obs = _as_mapping(snap.get("observation"))
    lifecycle = _as_mapping(snap.get("lifecycle"))
    activity = _as_mapping(snap.get("activity"))
    snap_plan = _as_mapping(snap.get("plan"))
    plan_payload = plan if isinstance(plan, Mapping) else {}
    antenna = _as_mapping(snap.get("antenna"))
    chopper = _as_mapping(snap.get("chopper"))
    spectrometer = _as_mapping(snap.get("spectrometer"))
    time_sync = _as_mapping(snap.get("time_sync"))
    timing = _as_mapping(snap.get("time"))

    lifecycle_state = str(
        _first_present(lifecycle, "state", "status") or "unknown"
    ).lower()
    observation_type = _first_present(obs, "type", "mode")
    observation_record = obs.get("record_name")
    observation_target = obs.get("target")
    progress_record_directory = (
        str(progress_record_dir(Path(str(paths.get("root") or "")) if isinstance(paths, Mapping) and paths.get("root") else progress_root(), observation_record))
        if observation_record
        else None
    )
    recording_directory = str(recording_data_dir(observation_record)) if observation_record else None

    if activity.get("live_motion_active") is False:
        active_task = "idle"
    else:
        active_task = _first_present(
            activity,
            "active_task",
            "phase",
            "drive_kind",
            "motion_stage",
        )
        if active_task in (None, ""):
            active_task = "observation" if lifecycle_state not in {"idle", "unknown"} else "idle"

    command_fields_valid = antenna.get("command_valid") is not False
    command_lon = (
        _finite_float(_first_present(antenna, "command_lon_deg", "cmd_az_deg", "az_cmd_deg"))
        if command_fields_valid
        else None
    )
    command_lat = (
        _finite_float(_first_present(antenna, "command_lat_deg", "cmd_el_deg", "el_cmd_deg"))
        if command_fields_valid
        else None
    )
    encoder_lon = _finite_float(
        _first_present(antenna, "encoder_lon_deg", "enc_az_deg", "az_deg")
    )
    encoder_lat = _finite_float(
        _first_present(antenna, "encoder_lat_deg", "enc_el_deg", "el_deg")
    )
    tracking_error = _finite_float(antenna.get("tracking_error_deg"))
    tracking_ok_raw = antenna.get("tracking_ok")
    tracking_ok = bool(tracking_ok_raw) if tracking_ok_raw is not None else None

    chopper_time = _first_present(
        chopper, "publish_time_unix", "time_unix", "updated_at_unix"
    )
    chopper_position = _finite_float(
        _first_present(chopper, "position", "position_count", "pos")
    )
    time_sync_status = str(
        _first_present(time_sync, "status", "label") or "unknown"
    ).lower()

    warnings: List[str] = []
    if antenna.get("command_stale"):
        warnings.append("antenna command status is stale")
    if antenna.get("encoder_stale"):
        warnings.append("antenna encoder status is stale")
    if time_sync_status in {"bad", "nosync", "warn"}:
        warnings.append(f"time sync is {time_sync_status}")
    if chopper.get("alarm") or str(chopper.get("state") or "").lower() == "alarm":
        warnings.append("chopper alarm")

    compact_plan_total = _first_present(snap_plan, "total", "n_total")
    if compact_plan_total is None:
        compact_plan_total = _first_present(plan_payload, "total", "n_total")

    return {
        "schema": "necst.operator_status.v1",
        "server_time_unix": now,
        "system": {
            "state": lifecycle_state,
            "lifecycle_status": lifecycle.get("status"),
            "ok": lifecycle_state not in {"error", "failed"},
        },
        "observation": {
            "type": observation_type,
            "record_name": observation_record,
            "obs_file": obs.get("obs_file"),
            "target": observation_target,
            "recording_dir": recording_directory,
            "progress_record_dir": progress_record_directory,
            "elapsed_sec": _finite_float(timing.get("elapsed_sec")),
            "remaining_sec": _finite_float(timing.get("estimated_remaining_sec")),
            "remaining_method": timing.get("remaining_method"),
        },
        "progress": {
            "percent": _progress_percent(snap),
            "total": compact_plan_total,
            "current": _first_present(snap_plan, "current", "current_index", "current_id"),
            "label": _first_present(snap_plan, "label", "mode", "role"),
        },
        "motion": {
            "active_task": active_task,
            "stage": ("idle" if activity.get("live_motion_active") is False else _first_present(activity, "motion_stage", "phase")),
            "tracking_ok": tracking_ok,
            "tracking_error_deg": tracking_error,
        },
        "antenna": {
            "command_az_deg": command_lon,
            "command_el_deg": command_lat,
            "current_az_deg": encoder_lon,
            "current_el_deg": encoder_lat,
            "command_frame": antenna.get("command_frame"),
            "encoder_frame": antenna.get("encoder_frame"),
            "tracking_status_age_sec": _finite_float(
                antenna.get("tracking_status_age_sec")
            ),
            "az_unwrap": antenna.get("az_unwrap") if isinstance(antenna.get("az_unwrap"), Mapping) else None,
        },
        "chopper": {
            "state": _first_present(chopper, "state", "position_state", "status") or "unknown",
            "position": chopper_position,
            "age_sec": _age_sec(chopper_time, now_unix=now),
            "raw": dict(chopper) if chopper else {},
        },
        "spectrometer": {
            "status": _first_present(spectrometer, "status", "state") or "unknown",
            "irigb": _first_present(
                spectrometer, "irigb", "irig_b", "time_source", "time_src"
            ),
            "raw": dict(spectrometer) if spectrometer else {},
        },
        "time_sync": dict(time_sync) if time_sync else {},
        "paths": dict(paths) if isinstance(paths, Mapping) else {},
        "event_count": len(events or []),
        "warnings": warnings,
    }


def build_progress_status_state(
    root: Path,
    *,
    events_limit: int = 12,
    live_payload: Optional[Mapping[str, Any]] = None,
    time_sync_payload: Optional[Mapping[str, Any]] = None,
    server_time_unix: Optional[float] = None,
) -> Dict[str, Any]:
    """Read progress sidecars and return raw inputs plus compact status.

    This is the shared read-only boundary between progress.py and the future
    operator console.  It does not render HTML and it does not send commands.
    """
    now = float(server_time_unix if server_time_unix is not None else time.time())
    snapshot_path = current_snapshot_path(root)
    raw_snapshot = read_json(snapshot_path)
    snapshot = merge_live_telemetry(
        raw_snapshot if isinstance(raw_snapshot, dict) else None,
        live_payload if isinstance(live_payload, Mapping) else None,
    )
    snapshot = apply_dynamic_remaining(snapshot, now_unix=now)

    if isinstance(snapshot, dict) and isinstance(time_sync_payload, Mapping):
        if time_sync_payload.get("enabled"):
            snapshot.setdefault("time_sync", {}).update(dict(time_sync_payload))

    raw_snapshot_for_paths = raw_snapshot if isinstance(raw_snapshot, Mapping) else None
    events_path = latest_events_path(root, raw_snapshot_for_paths)
    plan_path = latest_plan_path(root, raw_snapshot_for_paths)
    plan = read_json(plan_path) if plan_path else None
    all_events = read_all_events(events_path)
    events = all_events[-events_limit:] if events_limit > 0 else []
    record_name_for_paths = None
    if isinstance(snapshot, Mapping):
        record_name_for_paths = (_as_mapping(snapshot.get("observation"))).get("record_name")
    record_name_for_paths = record_name_for_paths or current_record_name(root)
    prog_record_dir = progress_record_dir(root, record_name_for_paths)
    data_record_dir = recording_data_dir(record_name_for_paths)
    paths = {
        "root": str(root),
        "snapshot": str(snapshot_path) if snapshot_path else None,
        "events": str(events_path) if events_path else None,
        "plan": str(plan_path) if plan_path else None,
        "progress_record_dir": str(prog_record_dir) if prog_record_dir else None,
        "recording_dir": str(data_record_dir) if data_record_dir else None,
    }
    compact_status = build_operator_status(
        snapshot if isinstance(snapshot, Mapping) else None,
        plan=plan if isinstance(plan, Mapping) else None,
        events=events,
        paths=paths,
        server_time_unix=now,
    )
    return {
        "ok": isinstance(snapshot, dict) and bool(snapshot) and not snapshot.get("_error"),
        "root": str(root),
        "paths": paths,
        "raw_snapshot": raw_snapshot or {},
        "snapshot": snapshot or {},
        "events": events,
        "status_events": all_events,
        "plan": plan or {},
        "server_time_unix": now,
        "operator_status": compact_status,
    }
