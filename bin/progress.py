#!/usr/bin/env python3
"""Show NECST observation progress.

Default data source is the lightweight progress files written by
ObservationProgressReporter.  This keeps the command useful even when started
after the observation process has already published its latest state.
"""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
import math
import os
import re
import sys
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional
from urllib.parse import parse_qs, urlparse




def safe_name(value: Any) -> str:
    original = str(value or "unknown")
    sanitized = re.sub(r"[^A-Za-z0-9_.-]+", "_", original)
    sanitized = re.sub(r"_+", "_", sanitized).strip("_") or "record"
    if sanitized != original:
        digest = hashlib.sha1(original.encode("utf-8", errors="surrogatepass")).hexdigest()[:8]
        sanitized = f"{sanitized}_{digest}"
    return sanitized

def progress_root(value: Optional[str] = None) -> Path:
    return Path(value or os.environ.get("NECST_PROGRESS_ROOT", "/tmp/necst_progress")).expanduser()


def strip_record_file_header(text: str) -> str:
    """Remove leading NECST FileWriter comment headers if present."""
    lines = text.splitlines()
    start = 0
    while start < len(lines) and (not lines[start].strip() or lines[start].lstrip().startswith("#")):
        start += 1
    return "\n".join(lines[start:])


def read_json(path: Path) -> Optional[Dict[str, Any]]:
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
        text = (root / "current_observation_record.txt").read_text(encoding="utf-8").strip()
        return text or None
    except Exception:
        return None


def latest_events_path(root: Path, snapshot: Optional[Dict[str, Any]] = None) -> Optional[Path]:
    record = None
    if snapshot:
        record = (snapshot.get("observation") or {}).get("record_name")
    record = record or current_record_name(root)
    if record:
        safe = safe_name(record)
        path = root / safe / "observation_events.jsonl"
        if path.exists():
            return path
    candidates = sorted(root.glob("*/observation_events.jsonl"), key=lambda p: p.stat().st_mtime if p.exists() else 0, reverse=True)
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


def angular_delta_deg(measured: float, commanded: float) -> float:
    """Shortest signed angular difference measured - commanded in degrees."""
    try:
        return ((float(measured) - float(commanded) + 180.0) % 360.0) - 180.0
    except Exception:
        return float(measured) - float(commanded)


def _msg_coord(msg: Any, *, prefix: str) -> Dict[str, Any]:
    lon = _finite_float(getattr(msg, "lon", None))
    lat = _finite_float(getattr(msg, "lat", None))
    payload: Dict[str, Any] = {}
    if lon is not None:
        payload[f"{prefix}_lon_deg"] = lon
    if lat is not None:
        payload[f"{prefix}_lat_deg"] = lat
    frame = getattr(msg, "frame", None)
    unit = getattr(msg, "unit", None)
    name = getattr(msg, "name", None)
    ts = _finite_float(getattr(msg, "time", None))
    if frame not in (None, ""):
        payload[f"{prefix}_frame"] = str(frame)
    if unit not in (None, ""):
        payload[f"{prefix}_unit"] = str(unit)
    if name not in (None, ""):
        payload[f"{prefix}_name"] = str(name)
    if ts is not None:
        payload[f"{prefix}_time_unix"] = ts
    return payload



FINAL_LIFECYCLE_STATES = {"finished", "error", "aborted"}


def apply_dynamic_remaining(snapshot: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Return a display/API snapshot with ETA counted down from completion time.

    ObservationProgressReporter writes an estimated_completion_unix whenever it
    has enough event-history data.  The sidecar itself is updated only at state
    transitions, so a watch/web client should derive the current remaining time
    from the completion timestamp while the observation is still active.
    """
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
        timing["estimated_remaining_sec"] = max(0.0, completion - time.time())
    return snapshot

def merge_live_telemetry(snapshot: Optional[Dict[str, Any]], live: Optional[Mapping[str, Any]]) -> Optional[Dict[str, Any]]:
    """Return a display-only snapshot augmented with live ROS telemetry.

    The progress JSON files remain the source of truth.  Live ROS values are
    merged only in the CLI/web process so the observation runtime stays free of
    high-frequency subscriptions.  This is especially important for scan_block:
    the sidecar knows the current block, while the antenna ControlStatus topic
    knows the fine section/line currently being executed.
    """
    if not isinstance(snapshot, dict) or not live:
        return snapshot
    out = copy.deepcopy(snapshot)
    live_payload = dict(live)
    out["live"] = live_payload

    antenna_update: Dict[str, Any] = {}
    cmd = live_payload.get("command") if isinstance(live_payload.get("command"), dict) else {}
    enc = live_payload.get("encoder") if isinstance(live_payload.get("encoder"), dict) else {}
    if cmd:
        antenna_update.update(cmd)
    if enc:
        antenna_update.update(enc)
    cmd_lon = _finite_float(cmd.get("command_lon_deg")) if isinstance(cmd, dict) else None
    cmd_lat = _finite_float(cmd.get("command_lat_deg")) if isinstance(cmd, dict) else None
    enc_lon = _finite_float(enc.get("encoder_lon_deg")) if isinstance(enc, dict) else None
    enc_lat = _finite_float(enc.get("encoder_lat_deg")) if isinstance(enc, dict) else None
    if None not in (cmd_lon, cmd_lat, enc_lon, enc_lat):
        # Azimuth/longitude is circular.  Without wrapping, a perfectly normal
        # 359.9 deg -> 0.1 deg crossing would look like a 359.8 deg tracking
        # error.  Use the shortest signed angular difference for the longitude
        # component; latitude/elevation remains a linear difference.
        dlon = angular_delta_deg(enc_lon, cmd_lon)
        dlat = enc_lat - cmd_lat
        antenna_update["tracking_delta_lon_deg"] = dlon
        antenna_update["tracking_delta_lat_deg"] = dlat
        antenna_update["tracking_error_deg"] = math.hypot(dlon, dlat)
    if antenna_update:
        out.setdefault("antenna", {}).update(antenna_update)

    lifecycle = out.get("lifecycle") if isinstance(out.get("lifecycle"), dict) else {}
    final_state = str(lifecycle.get("state") or "").lower() in {"finished", "error", "aborted"}
    ctrl = live_payload.get("control") if isinstance(live_payload.get("control"), dict) else {}
    if ctrl and not final_state:
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
        if isinstance(line_index, int) and line_index >= 0:
            geometry["current_line_index0"] = line_index
            data = out.setdefault("data", {})
            # Do not overwrite expected_metadata_line_index for non-scan-block
            # integrations; only fill a display helper when no explicit value is
            # present.
            if data.get("latest_control_line_index") is None:
                data["latest_control_line_index"] = line_index
    return apply_dynamic_remaining(out)


class LiveRosCache:
    """Optional best-effort ROS telemetry cache for the progress CLI/web UI."""

    def __init__(self, *, enabled: bool = True) -> None:
        self.requested = bool(enabled)
        self.available = False
        self.error: Optional[str] = None
        self._rclpy = None
        self._node = None
        self._owns_context = False
        self.control: Dict[str, Any] = {}
        self.command: Dict[str, Any] = {}
        self.encoder: Dict[str, Any] = {}
        if self.requested:
            self._start()

    def _start(self) -> None:
        try:
            repo_root = str(Path(__file__).resolve().parents[1])
            if repo_root not in sys.path:
                sys.path.insert(0, repo_root)
            import rclpy  # type: ignore
            from necst.definitions import topic  # type: ignore

            self._rclpy = rclpy
            self._owns_context = not rclpy.ok()
            if self._owns_context:
                rclpy.init(args=None)
            self._node = rclpy.create_node("necst_progress_monitor")

            def control_cb(msg: Any) -> None:
                line_index = getattr(msg, "line_index", -1)
                try:
                    line_index = int(line_index)
                except Exception:
                    line_index = -1
                self.control = {
                    "controlled": bool(getattr(msg, "controlled", False)),
                    "tight": bool(getattr(msg, "tight", False)),
                    "remote": bool(getattr(msg, "remote", False)),
                    "id": str(getattr(msg, "id", "")),
                    "interrupt_ok": bool(getattr(msg, "interrupt_ok", False)),
                    "time_unix": _finite_float(getattr(msg, "time", None)),
                    "section_kind": str(getattr(msg, "section_kind", "")),
                    "section_label": str(getattr(msg, "section_label", "")),
                    "line_index": line_index,
                }

            def command_cb(msg: Any) -> None:
                self.command = _msg_coord(msg, prefix="command")

            def encoder_cb(msg: Any) -> None:
                self.encoder = _msg_coord(msg, prefix="encoder")

            topic.antenna_control_status.subscription(self._node, control_cb)
            topic.altaz_cmd.subscription(self._node, command_cb)
            topic.antenna_encoder.subscription(self._node, encoder_cb)
            self.available = True
        except Exception as exc:
            self.available = False
            self.error = str(exc)
            try:
                if self._node is not None:
                    self._node.destroy_node()
            except Exception:
                pass
            self._node = None

    def spin_once(self, timeout_sec: float = 0.0) -> None:
        if not (self.available and self._rclpy is not None and self._node is not None):
            return
        try:
            self._rclpy.spin_once(self._node, timeout_sec=timeout_sec)
        except Exception as exc:
            self.error = str(exc)
            self.available = False

    def snapshot(self) -> Dict[str, Any]:
        payload: Dict[str, Any] = {"available": self.available}
        if self.error:
            payload["error"] = self.error
        if self.control:
            payload["control"] = dict(self.control)
        if self.command:
            payload["command"] = dict(self.command)
        if self.encoder:
            payload["encoder"] = dict(self.encoder)
        return payload

    def close(self) -> None:
        try:
            if self._node is not None:
                self._node.destroy_node()
        except Exception:
            pass
        try:
            if self._owns_context and self._rclpy is not None and self._rclpy.ok():
                self._rclpy.shutdown()
        except Exception:
            pass


def latest_plan_path(root: Path, snapshot: Optional[Dict[str, Any]] = None) -> Optional[Path]:
    record = None
    if snapshot:
        record = (snapshot.get("observation") or {}).get("record_name")
    record = record or current_record_name(root)
    if record:
        safe = safe_name(record)
        path = root / safe / "observation_plan.json"
        if path.exists():
            return path
    candidates = sorted(root.glob("*/observation_plan.json"), key=lambda p: p.stat().st_mtime if p.exists() else 0, reverse=True)
    return candidates[0] if candidates else None


def fmt_time(seconds: Optional[float], approx: bool = False) -> str:
    if seconds is None:
        return "--:--:--"
    try:
        sec = max(0, int(float(seconds)))
    except Exception:
        return "--:--:--"
    h, r = divmod(sec, 3600)
    m, s = divmod(r, 60)
    prefix = "≈" if approx else ""
    return f"{prefix}{h:02d}:{m:02d}:{s:02d}"


def pct_bar(percent: Optional[float], width: int = 50) -> str:
    try:
        p = max(0.0, min(100.0, float(percent)))
    except Exception:
        p = 0.0
    n = int(round(width * p / 100.0))
    return "[" + "#" * n + "-" * (width - n) + f"] {p:5.1f}%"


def _index_range_from_mapping(payload: Mapping[str, Any]) -> Optional[tuple[int, int]]:
    try:
        start = int(payload.get("index0"))
    except Exception:
        return None
    try:
        end = int(payload.get("index0_end")) if payload.get("index0_end") is not None else start
    except Exception:
        end = start
    if end < start:
        start, end = end, start
    return start, end


def _ranges_overlap(a: tuple[int, int], b: tuple[int, int]) -> bool:
    return a[0] <= b[1] and b[0] <= a[1]


def _snapshot_plan_finished_by_events(plan: Mapping[str, Any], events: Iterable[Mapping[str, Any]]) -> bool:
    """Return True when the current snapshot plan item is already finished.

    observation_progress.json deliberately keeps the last item until the next
    item starts.  Display code must therefore treat plan_item_finished events as
    stronger than the current snapshot during short inter-item gaps.
    """
    uid = plan.get("item_uid")
    uid_text = str(uid) if uid not in (None, "") else None
    plan_range = _index_range_from_mapping(plan)
    for event in events:
        if event.get("event") != "plan_item_finished":
            continue
        ev_uid = event.get("item_uid")
        if uid_text is not None and ev_uid not in (None, "") and str(ev_uid) == uid_text:
            return True
        ev_range = _index_range_from_mapping(event)
        if plan_range is not None and ev_range is not None and _ranges_overlap(plan_range, ev_range):
            return True
    return False


def progress_display_status(
    lifecycle: Mapping[str, Any],
    plan: Mapping[str, Any],
    events: Iterable[Mapping[str, Any]],
) -> str:
    state = str(lifecycle.get("state") or "unknown").lower()
    if state in FINAL_LIFECYCLE_STATES:
        return state
    if _snapshot_plan_finished_by_events(plan, events):
        return "done/between-items"
    return state


def compact_value(value: Any) -> str:
    if value is None:
        return "-"
    if isinstance(value, float):
        return f"{value:.3f}"
    if isinstance(value, list):
        return "(" + ", ".join(compact_value(v) for v in value) + ")"
    if isinstance(value, dict):
        return json.dumps(value, ensure_ascii=False, sort_keys=True)
    return str(value)


def compact_record_name(value: Any, *, max_len: int = 24) -> str:
    text = str(value or "-")
    for prefix in ("necst_otf_", "necst_rsky_", "necst_skydip_", "necst_"):
        if text.startswith(prefix) and len(text) > max_len:
            text = text[len(prefix):]
            break
    if len(text) <= max_len:
        return text
    keep = max(4, (max_len - 1) // 2)
    tail = max(4, max_len - keep - 1)
    return f"{text[:keep]}…{text[-tail:]}"


def render(snapshot: Optional[Dict[str, Any]], events: Iterable[Dict[str, Any]], *, compact: bool = False) -> str:
    if snapshot is None:
        return "No active NECST observation progress found."
    if "_error" in snapshot:
        return snapshot["_error"]

    obs = snapshot.get("observation") or {}
    lifecycle = snapshot.get("lifecycle") or {}
    plan = snapshot.get("plan") or {}
    activity = snapshot.get("activity") or {}
    geom = snapshot.get("geometry") or {}
    antenna = snapshot.get("antenna") or {}
    data = snapshot.get("data") or {}
    timing = snapshot.get("time") or {}

    display_status = progress_display_status(lifecycle, plan, events)
    state = compact_value(lifecycle.get("state")).upper()
    status_label = compact_value(display_status)
    obs_type = compact_value(obs.get("type"))
    record = compact_record_name(obs.get("record_name"))
    phase = compact_value(activity.get("phase") or plan.get("mode"))
    drive = compact_value(activity.get("drive_kind") or plan.get("drive_kind"))
    motion = compact_value(activity.get("motion_stage"))
    data_state = compact_value(activity.get("data_state"))

    index0 = plan.get("index0")
    index0_end = plan.get("index0_end")
    total = plan.get("total")
    index_label = "-"
    percent = plan.get("percent")
    if isinstance(index0, int) and isinstance(total, int) and total > 0:
        if isinstance(index0_end, int) and index0_end >= index0:
            index_label = f"{index0 + 1}-{index0_end + 1}/{total}"
            # A merged scan_block covers a range of plan items.  During the block
            # this represents the scheduled range, not per-line completion; the
            # line-level status is shown separately when available.
            percent = 100.0 * (index0_end + 1) / total
        else:
            index_label = f"{index0 + 1}/{total}"
            percent = 100.0 * (index0 + 1) / total
    elif total:
        index_label = f"-/{total}"

    if compact:
        compact_line_info = ""
        current_line = geom.get("current_line_index0")
        line_total = geom.get("line_total") or plan.get("line_total")
        if current_line is not None and line_total is not None:
            try:
                compact_line_info = f" line={int(current_line) + 1}/{int(line_total)}"
            except Exception:
                compact_line_info = f" line={compact_value(current_line)}/{compact_value(line_total)}"
        elif current_line is not None:
            compact_line_info = f" line={compact_value(current_line)}"
        state_prefix = state if status_label.lower() == str(lifecycle.get("state") or "").lower() else f"{state} status={status_label}"
        line = (
            f"{state_prefix} {obs_type} {index_label} {phase} | "
            f"drive={drive}:{motion}{compact_line_info} data={data_state} | "
            f"metadata={compact_value(data.get('expected_metadata_position'))} "
            f"id={compact_value(data.get('expected_metadata_id'))} | "
            f"elapsed={fmt_time(timing.get('elapsed_sec'))} "
            f"remaining={fmt_time(timing.get('estimated_remaining_sec'), approx=True)} "
            f"conf={compact_value(timing.get('remaining_confidence'))}"
        )
        return line

    lines = []
    lines.append("┌──────────────── NECST Observation Progress ────────────────┐")
    status_suffix = "" if status_label.lower() == str(lifecycle.get("state") or "").lower() else f" status={status_label}"
    lines.append(
        f"│ state={(state + status_suffix)[:21]:<21} type={obs_type:<14} record={record[:20]:<20}│"
    )
    obsfile = compact_value(obs.get("obs_file"))
    lines.append(
        f"│ obsfile={obsfile[:30]:<30} elapsed={fmt_time(timing.get('elapsed_sec')):<8} "
        f"remaining={fmt_time(timing.get('estimated_remaining_sec'), approx=True):<9}│"
    )
    method = compact_value(timing.get('remaining_method'))
    confidence = compact_value(timing.get('remaining_confidence'))
    if method != '-' or confidence != '-':
        lines.append(f"│ ETA method={method[:29]:<29} confidence={confidence:<12}│")
    lines.append("├──────────────────── PLAN ──────────────────────────────────┤")
    label = compact_value(plan.get("label"))
    role = compact_value(plan.get("role"))
    obs_id = compact_value(plan.get("obs_id"))
    lines.append(f"│ item {index_label:<10} phase={phase:<8} id={obs_id:<10} role={role:<12}│")
    if plan.get("index0_end") is not None:
        lines.append(
            f"│ block index={plan.get('index0')}..{plan.get('index0_end')} "
            f"lines={compact_value(plan.get('line_total')):<5} label={label[:21]:<21}│"
        )
    lines.append(f"│ {pct_bar(percent):<58}│")
    lines.append("├──────────────────── CURRENT ACTIVITY ──────────────────────┤")
    line_info = ""
    current_line = geom.get("current_line_index0")
    line_total = geom.get("line_total") or plan.get("line_total")
    if current_line is not None and line_total is not None:
        try:
            line_info = f" line={int(current_line) + 1}/{int(line_total)}"
        except Exception:
            line_info = f" line={compact_value(current_line)}/{compact_value(line_total)}"
    elif current_line is not None:
        line_info = f" line={compact_value(current_line)}"
    elif line_total is not None:
        line_info = f" line_total={line_total}"
    lines.append(
        f"│ drive={drive:<12} motion={motion:<18} data={data_state:<14}{line_info[:16]:<16}│"
    )
    loc = activity.get("location_context")
    if loc:
        lines.append(f"│ location_context={compact_value(loc):<44}│")
    lines.append("├──────────────────── COORDINATE ────────────────────────────┤")
    gkind = compact_value(geom.get("kind"))
    frame = compact_value(geom.get("frame"))
    if geom.get("start") is not None or geom.get("stop") is not None:
        lines.append(f"│ plan : kind={gkind:<12} frame={frame:<8} start={compact_value(geom.get('start'))[:20]:<20}│")
        lines.append(f"│        stop={compact_value(geom.get('stop'))[:47]:<47}│")
    elif geom.get("target") is not None:
        lines.append(f"│ plan : kind={gkind:<12} frame={frame:<8} target={compact_value(geom.get('target'))[:20]:<20}│")
    elif geom:
        lines.append(f"│ plan : kind={gkind:<12} frame={frame:<8} {compact_value(geom)[:31]:<31}│")
    else:
        lines.append("│ plan : -                                                     │")
    if antenna:
        cmd_lon = antenna.get("command_lon_deg")
        cmd_lat = antenna.get("command_lat_deg")
        enc_lon = antenna.get("encoder_lon_deg")
        enc_lat = antenna.get("encoder_lat_deg")
        if cmd_lon is not None or cmd_lat is not None:
            lines.append(f"│ cmd  : lon={compact_value(cmd_lon):<13} lat={compact_value(cmd_lat):<13} frame={compact_value(antenna.get('command_frame')):<8}│")
        if enc_lon is not None or enc_lat is not None:
            lines.append(f"│ enc  : lon={compact_value(enc_lon):<13} lat={compact_value(enc_lat):<13} frame={compact_value(antenna.get('encoder_frame')):<8}│")
        if antenna.get("tracking_error_deg") is not None:
            lines.append(f"│ err  : tracking_error={compact_value(antenna.get('tracking_error_deg')):<37} deg│")
        if not (cmd_lon is not None or enc_lon is not None):
            lines.append(f"│ ant  : {compact_value(antenna)[:56]:<56}│")
    lines.append("├──────────────────── DATA ──────────────────────────────────┤")
    lines.append(
        f"│ expected: {compact_value(data.get('expected_metadata_position')):<5} "
        f"id={compact_value(data.get('expected_metadata_id')):<12} "
        f"line={compact_value(data.get('expected_metadata_line_index')):<8}              │"
    )
    if data.get("latest_spectrum_position") is not None:
        lines.append(
            f"│ latest  : {compact_value(data.get('latest_spectrum_position')):<5} "
            f"id={compact_value(data.get('latest_spectrum_id')):<12} "
            f"age={compact_value(data.get('latest_spectrum_age_sec')):<8}              │"
        )
    lines.append("├──────────────────── RECENT EVENTS ─────────────────────────┤")
    event_lines = list(events)[-8:] or snapshot.get("recent_events", [])[-8:]
    if not event_lines:
        lines.append("│ -                                                          │")
    for event in event_lines[-8:]:
        ts = event.get("time_unix")
        try:
            t = time.strftime("%H:%M:%S", time.localtime(float(ts)))
        except Exception:
            t = "--:--:--"
        body = event.get("event", "event")
        if event.get("phase"):
            body += f" phase={event.get('phase')}"
        if event.get("id") is not None:
            body += f" id={event.get('id')}"
        if event.get("line_index") is not None:
            body += f" line={event.get('line_index')}"
        lines.append(f"│ {t}  {body[:51]:<51}│")
    lines.append("└─────────────────────────────────────────────────────────────┘")
    lines.append("Press Ctrl-C to quit.  Use --follow for scrolling event log.")
    return "\n".join(lines)


def cmd_once(args: argparse.Namespace) -> int:
    root = progress_root(args.root)
    live = LiveRosCache(enabled=not args.no_ros)
    try:
        live.spin_once(0.05)
        snapshot = apply_dynamic_remaining(merge_live_telemetry(read_json(current_snapshot_path(root)), live.snapshot()))
        if args.json:
            print(json.dumps(snapshot or {}, ensure_ascii=False, indent=2, sort_keys=True))
            return 0
        events = read_recent_events(latest_events_path(root, snapshot), args.events)
        print(render(snapshot, events, compact=args.compact))
        return 0
    finally:
        live.close()


def cmd_watch(args: argparse.Namespace) -> int:
    root = progress_root(args.root)
    live = LiveRosCache(enabled=not args.no_ros)
    try:
        while True:
            live.spin_once(0.0)
            snapshot = apply_dynamic_remaining(merge_live_telemetry(read_json(current_snapshot_path(root)), live.snapshot()))
            events = read_recent_events(latest_events_path(root, snapshot), args.events)
            if args.no_clear:
                print(render(snapshot, events, compact=args.compact))
                print()
            else:
                sys.stdout.write("\x1b[H\x1b[2J")
                sys.stdout.write(render(snapshot, events, compact=args.compact))
                sys.stdout.write("\n")
                sys.stdout.flush()
            time.sleep(args.interval)
    except KeyboardInterrupt:
        return 0
    finally:
        live.close()


def cmd_follow(args: argparse.Namespace) -> int:
    root = progress_root(args.root)
    snapshot = read_json(current_snapshot_path(root))
    path = latest_events_path(root, snapshot)
    if path is None:
        print("No observation_events.jsonl found.", file=sys.stderr)
        return 1
    pos = 0
    try:
        if args.from_end:
            pos = path.stat().st_size
        while True:
            try:
                with path.open("r", encoding="utf-8") as fh:
                    fh.seek(pos)
                    while True:
                        line = fh.readline()
                        if not line:
                            pos = fh.tell()
                            break
                        pos = fh.tell()
                        if args.json:
                            print(line.rstrip())
                        else:
                            try:
                                ev = json.loads(line)
                            except Exception:
                                print(line.rstrip())
                                continue
                            ts = ev.get("time_unix")
                            try:
                                t = time.strftime("%H:%M:%S", time.localtime(float(ts)))
                            except Exception:
                                t = "--:--:--"
                            print(f"{t} {ev.get('event', 'event')} " + " ".join(
                                f"{k}={v}" for k, v in ev.items()
                                if k not in {"seq", "event", "time_unix"}
                            ))
            except FileNotFoundError:
                pass
            sys.stdout.flush()
            time.sleep(args.interval)
    except KeyboardInterrupt:
        return 0




def _json_response(handler: BaseHTTPRequestHandler, payload: Any, status: int = 200) -> None:
    body = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True).encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json; charset=utf-8")
    handler.send_header("Cache-Control", "no-store")
    handler.send_header("Content-Length", str(len(body)))
    handler.end_headers()
    handler.wfile.write(body)


def _text_response(handler: BaseHTTPRequestHandler, body: str, *, content_type: str = "text/html; charset=utf-8", status: int = 200) -> None:
    data = body.encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", content_type)
    handler.send_header("Cache-Control", "no-store")
    handler.send_header("Content-Length", str(len(data)))
    handler.end_headers()
    handler.wfile.write(data)


def _int_query(query: Dict[str, List[str]], name: str, default: int, *, minimum: int = 0, maximum: int = 1000) -> int:
    try:
        value = int(query.get(name, [default])[0])
    except Exception:
        value = default
    return max(minimum, min(maximum, value))


def build_state(root: Path, *, events_limit: int = 12, live: Optional[LiveRosCache] = None) -> Dict[str, Any]:
    snapshot_path = current_snapshot_path(root)
    raw_snapshot = read_json(snapshot_path)
    if live is not None:
        live.spin_once(0.0)
    snapshot = apply_dynamic_remaining(merge_live_telemetry(raw_snapshot, live.snapshot() if live is not None else None))
    events_path = latest_events_path(root, raw_snapshot if isinstance(raw_snapshot, dict) else None)
    plan_path = latest_plan_path(root, raw_snapshot if isinstance(raw_snapshot, dict) else None)
    plan = read_json(plan_path) if plan_path else None
    all_events = read_all_events(events_path)
    events = all_events[-events_limit:] if events_limit > 0 else []
    return {
        "ok": isinstance(snapshot, dict) and bool(snapshot) and not snapshot.get("_error"),
        "root": str(root),
        "paths": {
            "snapshot": str(snapshot_path) if snapshot_path else None,
            "events": str(events_path) if events_path else None,
            "plan": str(plan_path) if plan_path else None,
        },
        "snapshot": snapshot or {},
        "events": events,
        # Full observation event status is used only for plan-status
        # classification.  The visible Recent Events table remains bounded by
        # ``events`` so the dashboard stays readable.
        "status_events": all_events,
        "plan": plan or {},
        "rendered": render(snapshot if isinstance(snapshot, dict) else None, all_events, compact=True),
        "server_time_unix": time.time(),
    }


_HTML_TEMPLATE = """<!doctype html>
<html lang=\"en\">
<head>
<meta charset=\"utf-8\">
<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">
<title>NECST Observation Progress</title>
<style>
:root { color-scheme: light dark; --ok:#1a7f37; --warn:#b54708; --err:#c1121f; --muted:#666; --border:#9995; }
body { font-family: -apple-system, BlinkMacSystemFont, \"Segoe UI\", sans-serif; margin: 0; padding: .75rem; line-height: 1.28; font-size:14px; }
header { display:flex; gap:1rem; align-items:baseline; justify-content:space-between; flex-wrap:wrap; }
h1 { font-size:1.12rem; margin:0 0 .35rem; }
.grid { display:grid; grid-template-columns: repeat(auto-fit, minmax(260px, 1fr)); gap: .6rem; }
.card { border:1px solid var(--border); border-radius:12px; padding:.6rem; box-shadow:0 1px 4px #0001; }
.card h2 { font-size:.95rem; margin:.05rem 0 .4rem; }
.kv { display:grid; grid-template-columns: 7.6rem minmax(0,1fr); gap:.15rem .45rem; align-items:baseline; }
.k { color:var(--muted); }
.status { font-weight:700; padding:.1rem .45rem; border-radius:999px; border:1px solid var(--border); }
.status.running, .status.finished { color:var(--ok); } .status.error, .status.aborted { color:var(--err); } .status.cleanup { color:var(--warn); }
.bar { width:100%; height:1rem; border:1px solid var(--border); border-radius:999px; overflow:hidden; background:#9992; }
.fill { height:100%; width:0%; background:currentColor; color:var(--ok); transition:width .2s; }
pre { white-space:pre-wrap; overflow:auto; max-height:16rem; margin:.2rem 0 0; font-size:.82rem; }
table { border-collapse:collapse; width:100%; font-size:.82rem; }
th, td { border-bottom:1px solid var(--border); text-align:left; padding:.25rem; vertical-align:top; }
.plotbox { min-height: 340px; display:flex; align-items:center; justify-content:center; }
.plotbox svg { width:100%; max-height:520px; }
.legend { display:flex; gap:.75rem; flex-wrap:wrap; margin:.25rem 0 .5rem; font-size:.85rem; color:var(--muted); }
.dot { display:inline-block; width:.8rem; height:.8rem; border-radius:999px; vertical-align:-.1rem; margin-right:.25rem; }
.small { font-size:.78rem; color:var(--muted); }
.planview { grid-column: span 2; }
.planview .plotbox { min-height: 390px; }
.planview svg { max-height: 620px; }
.important { font-weight:700; }
.notice { margin:.25rem 0 .55rem; padding:.45rem .6rem; border:1px solid var(--border); border-radius:10px; background:#9991; }
@media (max-width: 760px) { .planview { grid-column: span 1; } }
.err { color:var(--err); font-weight:700; }
</style>
</head>
<body>
<header><h1>NECST Observation Progress</h1><div class=\"small\">Auto refresh: __REFRESH_MS__ ms | <a href=\"/api/state\">JSON</a></div></header>
<div id=\"message\" class=\"small\">Loading...</div>
<div class=\"grid\">
<section class=\"card\"><h2>Observation</h2><div class=\"kv\" id=\"observation\"></div></section>
<section class=\"card\"><h2>Plan</h2><div class=\"kv\" id=\"plan\"></div><div class=\"bar\"><div id=\"bar\" class=\"fill\"></div></div></section>
<section class=\"card\"><h2>Activity</h2><div class=\"kv\" id=\"activity\"></div></section>
<section class=\"card planview\"><h2>Plan View</h2><div class=\"legend\"><span><i class=\"dot\" style=\"background:#2ca02c\"></i>done</span><span><i class=\"dot\" style=\"background:#ff7f0e\"></i>current</span><span><i class=\"dot\" style=\"background:#bdbdbd\"></i>pending</span><span>OFF/ref points are labeled; map scale follows ON scan area when possible.</span></div><div id=\"plotview\" class=\"plotbox small\">-</div></section>
<section class=\"card\"><h2>Geometry</h2><div class=\"kv\" id=\"geometry\"></div></section>
<section class=\"card\"><h2>Data</h2><div class=\"kv\" id=\"data\"></div></section>
<section class=\"card\"><h2>Recent Events</h2><div id=\"events\"></div></section>
<section class=\"card\"><h2>Terminal View</h2><pre id=\"terminal\"></pre></section>
<section class=\"card\"><h2>Files</h2><div class=\"kv\" id=\"paths\"></div></section>
</div>
<script>
const refreshMs = __REFRESH_MS__;
function esc(v) { return String(v ?? '-').replace(/[&<>\"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','\"':'&quot;',"'":'&#39;'}[c])); }
function isTimeLike(label, key) {
  const s = `${label || ''} ${key || ''}`.toLowerCase();
  return s.includes('[s]') || s.includes('_sec') || s.includes('elapsed') || s.includes('remaining') || s.includes('age');
}
function val(v, label='', key='') {
  if (v === null || v === undefined || v === '') return '-';
  if (typeof v === 'number') {
    if (!Number.isFinite(v)) return '-';
    if (Number.isInteger(v)) return String(v);
    return isTimeLike(label, key) ? v.toFixed(1) : v.toFixed(3);
  }
  if (Array.isArray(v)) return '(' + v.map(x => typeof x === 'number' && Number.isFinite(x) ? x.toFixed(3) : String(x)).join(', ') + ')';
  if (typeof v === 'object') return JSON.stringify(v);
  return String(v);
}
function shortRecordName(v, maxLen=26) {
  let s = String(v ?? '-');
  for (const p of ['necst_otf_', 'necst_rsky_', 'necst_skydip_', 'necst_']) {
    if (s.startsWith(p) && s.length > maxLen) { s = s.slice(p.length); break; }
  }
  if (s.length <= maxLen) return s;
  const head = Math.max(5, Math.floor((maxLen-1)/2));
  const tail = Math.max(5, maxLen-head-1);
  return s.slice(0, head) + '…' + s.slice(-tail);
}
function kv(id, obj, keys) {
  const el = document.getElementById(id); el.innerHTML = '';
  for (const [label, key] of keys) {
    const raw = typeof key === 'function' ? key(obj) : obj?.[key];
    const keyName = typeof key === 'string' ? key : '';
    el.insertAdjacentHTML('beforeend', `<div class=\"k\">${esc(label)}</div><div>${esc(val(raw, label, keyName))}</div>`);
  }
}
function stateClass(s) { return ['running','finished','error','aborted','cleanup'].includes(String(s||'').toLowerCase()) ? String(s).toLowerCase() : ''; }
function pct(plan) {
  if (Number.isInteger(plan?.index0) && Number.isInteger(plan?.total) && plan.total > 0) {
    const end = Number.isInteger(plan?.index0_end) && plan.index0_end >= plan.index0 ? plan.index0_end : plan.index0;
    return Math.max(0, Math.min(100, 100*(end+1)/plan.total));
  }
  if (typeof plan?.percent === 'number') return Math.max(0, Math.min(100, plan.percent));
  return 0;
}
function pair(v) { if (!Array.isArray(v) || v.length < 2) return null; const x=Number(v[0]), y=Number(v[1]); return Number.isFinite(x)&&Number.isFinite(y) ? [x,y] : null; }
function geomOf(item) { return item && typeof item.geometry === 'object' && item.geometry ? item.geometry : {}; }
function geomContext(g) {
  const out = {};
  for (const key of ['target_name','target','reference','offset','mode','frame','unit']) if (g && g[key] !== undefined) out[key] = g[key];
  return out;
}
function flattenItems(plan) {
  const raw = Array.isArray(plan?.items) ? plan.items : [];
  const out = [];
  for (const item of raw) {
    if (!item || typeof item !== 'object') continue;
    out.push(item);
    const g = geomOf(item);
    if ((g.kind === 'scan_block' || g.kind === 'scan_block_line') && Array.isArray(g.lines)) {
      for (const line of g.lines) {
        if (!line || typeof line !== 'object') continue;
        const child = {...item, geometry: {...geomContext(g), ...line, kind: line.kind || 'scan_block_line', frame: line.frame || g.frame, unit: line.unit || g.unit}, _parent_item_uid: item.item_uid};
        child.item_uid = `${item.item_uid || 'scan_block'}:line:${line.line_index0 ?? out.length}`;
        if (child.index0 === undefined && item.index0 !== undefined) child.index0 = item.index0;
        out.push(child);
      }
    }
  }
  return out;
}
function idxRange(obj) {
  const p = obj || {};
  const s = Number(p.index0); if (!Number.isFinite(s)) return null;
  const e0 = p.index0_end === undefined || p.index0_end === null ? s : Number(p.index0_end);
  const e = Number.isFinite(e0) ? e0 : s;
  return [Math.min(s,e), Math.max(s,e)];
}
function inRange(item, r) { const ir = idxRange(item); return ir && r && ir[0] <= r[1] && ir[1] >= r[0]; }
function doneRanges(events) { return (events||[]).filter(e=>e.event==='plan_item_finished').map(idxRange).filter(Boolean); }
function statusFor(item, snapshot, events) {
  const cur = snapshot?.plan?.item_uid; const parent = item._parent_item_uid; const uid = item.item_uid; const cr = idxRange(snapshot?.plan);
  const lifecycle = String(snapshot?.lifecycle?.state || '').toLowerCase();
  const finalState = ['finished','error','aborted'].includes(lifecycle);
  const liveLine = Number(snapshot?.geometry?.current_line_index0);
  const itemLine = Number(item?.geometry?.line_index0 ?? item?.line_index0);
  for (const ev of events || []) if (ev.event === 'plan_item_finished' && (ev.item_uid === uid || ev.item_uid === parent)) return 'done';
  for (const r of doneRanges(events)) if (inRange(item, r)) return 'done';
  const inActiveBlock = !finalState && cur && parent === cur && Number.isFinite(liveLine) && Number.isFinite(itemLine);
  if (inActiveBlock) {
    if (itemLine < liveLine) return 'done';
    if (itemLine === liveLine) return 'current';
    return 'pending';
  }
  if (!finalState && ((cur && (uid === cur || parent === cur)) || inRange(item, cr))) return 'current';
  return 'pending';
}
function sx(x, bounds) { return 38 + (x-bounds.xmin) * 520 / Math.max(1e-9, bounds.xmax-bounds.xmin); }
function sy(y, bounds) { return 320 - (y-bounds.ymin) * 270 / Math.max(1e-9, bounds.ymax-bounds.ymin); }
function colorFor(status) { return status === 'done' ? '#2ca02c' : status === 'current' ? '#ff7f0e' : '#bdbdbd'; }
function renderPlanView(snapshot, plan, events) {
  const items = flattenItems(plan);
  const kinds = new Set(items.map(it=>geomOf(it).kind));
  const plot = document.getElementById('plotview');
  if (kinds.has('skydip_elevation')) { plot.innerHTML = renderSkydipSvg(snapshot, items, events); return; }
  if ([...kinds].some(k => ['scan_line','scan_block_line','point','grid_point'].includes(k))) { plot.innerHTML = renderMapSvg(snapshot, items, events); return; }
  plot.innerHTML = '<div>No plottable geometry yet.</div>';
}
function itemMode(item) { return String(item?.mode || geomOf(item).mode || '').toUpperCase(); }
function pointKind(item) {
  const mode = itemMode(item);
  const role = String(item?.role || '').toLowerCase();
  if (mode && mode !== 'ON') return mode;
  if (role === 'calibration') return 'CAL';
  return mode || 'POINT';
}
function clipPoint(p, b) {
  const x = Math.max(b.xmin, Math.min(b.xmax, p[0]));
  const y = Math.max(b.ymin, Math.min(b.ymax, p[1]));
  return {p:[x,y], clipped: Math.abs(x-p[0])>1e-9 || Math.abs(y-p[1])>1e-9};
}
function renderPoint(row, b) {
  const clipped = clipPoint(row.a, b);
  const x = sx(clipped.p[0], b), y = sy(clipped.p[1], b);
  const r = row.status === 'current' ? 6 : 4;
  const label = row.pointLabel;
  const shape = row.mode === 'ON' || row.mode === 'POINT'
    ? `<circle cx="${x}" cy="${y}" r="${r}" fill="${colorFor(row.status)}" stroke="${row.status==='current'?'#111':'none'}"/>`
    : `<path d="M ${x} ${y-r-2} L ${x+r+2} ${y} L ${x} ${y+r+2} L ${x-r-2} ${y} Z" fill="${colorFor(row.status)}" stroke="#1118"/>`;
  const suffix = clipped.clipped ? ' (outside)' : '';
  const text = label ? `<text x="${Math.min(555, x+7)}" y="${Math.max(16, y-6)}" font-size="11">${esc(label + suffix)}</text>` : '';
  return shape + text;
}
function renderMapSvg(snapshot, items, events) {
  const rows=[]; const allPts=[]; const boundsPts=[];
  for (const item of items) {
    const g = geomOf(item); const k=g.kind; const mode = itemMode(item);
    const status = statusFor(item,snapshot,events);
    if (k === 'scan_line' || k === 'scan_block_line') {
      const a=pair(g.start), b=pair(g.stop);
      if (a&&b) {
        rows.push({item,a,b,status,mode:mode || 'ON'});
        allPts.push(a,b);
        if ((mode || 'ON') === 'ON') boundsPts.push(a,b);
      }
    }
    if (k === 'point' || k === 'grid_point') {
      const xy=pair(g.target)||pair(g.reference)||pair(g.offset);
      if (xy) {
        const pointLabel = pointKind(item);
        rows.push({item,a:xy,b:null,status,mode:mode || pointLabel,pointLabel});
        allPts.push(xy);
        if ((mode || '').toUpperCase() === 'ON' || k === 'grid_point') boundsPts.push(xy);
      }
    }
  }
  if (!allPts.length) return '<div>No numeric geometry. This is normal for target-name-only observations.</div>';
  const scalePts = boundsPts.length >= 2 ? boundsPts : allPts;
  let xmin=Math.min(...scalePts.map(p=>p[0])), xmax=Math.max(...scalePts.map(p=>p[0])), ymin=Math.min(...scalePts.map(p=>p[1])), ymax=Math.max(...scalePts.map(p=>p[1]));
  const dx=Math.max(1e-6,(xmax-xmin)*0.08), dy=Math.max(1e-6,(ymax-ymin)*0.08); xmin-=dx; xmax+=dx; ymin-=dy; ymax+=dy; const b={xmin,xmax,ymin,ymax};
  rows.sort((a,b)=>({pending:0,done:1,current:2}[a.status]-{pending:0,done:1,current:2}[b.status]));
  const els = rows.map(r => r.b ? `<line x1="${sx(r.a[0],b)}" y1="${sy(r.a[1],b)}" x2="${sx(r.b[0],b)}" y2="${sy(r.b[1],b)}" stroke="${colorFor(r.status)}" stroke-width="${r.status==='current'?4:2}" opacity="${r.status==='pending'?0.55:0.95}"/>` : renderPoint(r,b)).join('');
  const counts = {done: rows.filter(r=>r.status==='done').length, current: rows.filter(r=>r.status==='current').length, pending: rows.filter(r=>r.status==='pending').length};
  return `<svg viewBox="0 0 600 365" role="img"><rect x="1" y="1" width="598" height="363" fill="none" stroke="#9995"/><line x1="38" y1="320" x2="558" y2="320" stroke="#666"/><line x1="38" y1="50" x2="38" y2="320" stroke="#666"/>${els}<text x="38" y="348" font-size="11">x: ${esc(xmin.toFixed(3))} .. ${esc(xmax.toFixed(3))}</text><text x="335" y="348" font-size="11">done ${counts.done} / current ${counts.current} / pending ${counts.pending}</text></svg>`;
}
function renderSkydipSvg(snapshot, items, events) {
  const rows=[];
  for (let i=0; i<items.length; i++) { const g=geomOf(items[i]); let el = Number(g.el_deg ?? g.target_el_deg ?? g.elevation_deg); const t=pair(g.target); if (!Number.isFinite(el) && t) el=t[1]; if (Number.isFinite(el)) rows.push({x:i+1,y:el,status:statusFor(items[i],snapshot,events)}); }
  if (!rows.length) return '<div>No skydip elevation geometry.</div>';
  const ymin=Math.min(...rows.map(r=>r.y))-5, ymax=Math.max(...rows.map(r=>r.y))+5; const xmin=1, xmax=Math.max(1, rows.length); const b={xmin,xmax,ymin,ymax};
  const poly = rows.map(r=>`${sx(r.x,b)},${sy(r.y,b)}`).join(' ');
  const pts = rows.map(r=>`<circle cx="${sx(r.x,b)}" cy="${sy(r.y,b)}" r="${r.status==='current'?6:4}" fill="${colorFor(r.status)}" stroke="${r.status==='current'?'#111':'none'}"/>`).join('');
  return `<svg viewBox="0 0 600 365" role="img"><rect x="1" y="1" width="598" height="363" fill="none" stroke="#9995"/><line x1="38" y1="320" x2="558" y2="320" stroke="#666"/><line x1="38" y1="50" x2="38" y2="320" stroke="#666"/><polyline points="${poly}" fill="none" stroke="#9467bd" stroke-width="2"/>${pts}<text x="38" y="348" font-size="11">sequence step</text><text x="335" y="348" font-size="11">elevation ${esc(ymin.toFixed(1))}..${esc(ymax.toFixed(1))} deg</text></svg>`;
}
async function update() {
  try {
    const res = await fetch('/api/state?events=12', {cache:'no-store'});
    const s = await res.json();
    const snap = s.snapshot || {};
    const obs = snap.observation || {};
    const life = snap.lifecycle || {};
    const plan = snap.plan || {};
    const activity = snap.activity || {};
    const geom = snap.geometry || {};
    const data = snap.data || {};
    const timing = snap.time || {};
    const finalState = ['finished','error','aborted'].includes(String(life.state || '').toLowerCase());
    let msg = s.ok ? '' : '<span class=\"err\">Progress snapshot has errors or is missing.</span>';
    if (finalState) {
      const updated = typeof timing.updated_at_unix === 'number' ? new Date(timing.updated_at_unix*1000).toLocaleTimeString() : '-';
      msg += `<div class=\"notice\"><span class=\"important\">This observation is ${esc(life.state)}.</span> Last update: ${esc(updated)}. Waiting for the next observation snapshot.</div>`;
    }
    document.getElementById('message').innerHTML = msg;
    kv('observation', {...obs, state: life.state, record_label: shortRecordName(obs.record_name), elapsed_sec: timing.elapsed_sec, remaining_sec: timing.estimated_remaining_sec, remaining_method: timing.remaining_method, remaining_confidence: timing.remaining_confidence}, [['state','state'], ['type','type'], ['record','record_label'], ['obsfile','obs_file'], ['target','target'], ['elapsed [s]','elapsed_sec'], ['remaining≈ [s]','remaining_sec'], ['ETA method','remaining_method'], ['ETA confidence','remaining_confidence']]);
    document.querySelector('#observation div:nth-child(2)').innerHTML = `<span class=\"status ${stateClass(life.state)}\">${esc(life.state ?? '-')}</span>`;
    kv('plan', plan, [['item_uid',(p)=>shortRecordName(p?.item_uid, 34)], ['index0','index0'], ['index0_end','index0_end'], ['total','total'], ['mode','mode'], ['role','role'], ['label','label'], ['obs_id','obs_id']]);
    document.getElementById('bar').style.width = pct(plan).toFixed(1) + '%';
    renderPlanView(snap, s.plan || {}, s.status_events || s.events || []);
    kv('activity', activity, [['phase','phase'], ['drive_kind','drive_kind'], ['motion_stage','motion_stage'], ['data_state','data_state'], ['location_context','location_context'], ['description','description']]);
    kv('geometry', geom, [['kind','kind'], ['frame','frame'], ['unit','unit'], ['target_name','target_name'], ['target','target'], ['reference','reference'], ['offset','offset'], ['start','start'], ['stop','stop'], ['current_line','current_line_index0'], ['line_total','line_total']]);
    kv('data', data, [['expected position','expected_metadata_position'], ['expected id','expected_metadata_id'], ['expected line','expected_metadata_line_index'], ['latest position','latest_spectrum_position'], ['latest id','latest_spectrum_id'], ['latest line','latest_spectrum_line_index'], ['latest age [s]','latest_spectrum_age_sec']]);
    kv('paths', s.paths || {}, [['snapshot','snapshot'], ['events','events'], ['plan','plan']]);
    const rows = (s.events || []).slice(-12).map(ev => `<tr><td>${esc(ev.seq ?? '')}</td><td>${esc(ev.event ?? '')}</td><td>${esc(ev.phase ?? '')}</td><td>${esc(ev.id ?? ev.obs_id ?? '')}</td><td>${esc(ev.line_index ?? ev.line_index0 ?? '')}</td></tr>`).join('');
    document.getElementById('events').innerHTML = `<table><thead><tr><th>seq</th><th>event</th><th>phase</th><th>id</th><th>line</th></tr></thead><tbody>${rows || '<tr><td colspan=\"5\">-</td></tr>'}</tbody></table>`;
    document.getElementById('terminal').textContent = s.rendered || '';
  } catch (err) {
    document.getElementById('message').innerHTML = `<span class=\"err\">${esc(err)}</span>`;
  }
}
update(); setInterval(update, refreshMs);
</script>
</body>
</html>"""


def html_page(refresh_ms: int) -> str:
    return _HTML_TEMPLATE.replace("__REFRESH_MS__", str(int(refresh_ms)))


def cmd_serve(args: argparse.Namespace) -> int:
    root = progress_root(args.root)
    refresh_ms = max(250, int(args.refresh_ms))
    live = LiveRosCache(enabled=not args.no_ros)

    class ProgressHandler(BaseHTTPRequestHandler):
        server_version = "NECSTProgressHTTP/1.0"

        def log_message(self, fmt: str, *values: Any) -> None:
            if not args.quiet:
                super().log_message(fmt, *values)

        def do_GET(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler API
            parsed = urlparse(self.path)
            query = parse_qs(parsed.query)
            if parsed.path in {"/", "/index.html"}:
                _text_response(self, html_page(refresh_ms))
                return
            if parsed.path == "/api/state":
                limit = _int_query(query, "events", args.events, minimum=0, maximum=200)
                _json_response(self, build_state(root, events_limit=limit, live=live))
                return
            if parsed.path == "/api/progress":
                _json_response(self, read_json(current_snapshot_path(root)) or {})
                return
            if parsed.path == "/api/events":
                snapshot = read_json(current_snapshot_path(root))
                path = latest_events_path(root, snapshot if isinstance(snapshot, dict) else None)
                limit = _int_query(query, "limit", args.events, minimum=0, maximum=1000)
                _json_response(self, read_recent_events(path, limit))
                return
            if parsed.path == "/api/plan":
                snapshot = read_json(current_snapshot_path(root))
                path = latest_plan_path(root, snapshot if isinstance(snapshot, dict) else None)
                _json_response(self, (read_json(path) if path else None) or {})
                return
            if parsed.path == "/api/health":
                _json_response(self, {"ok": True, "root": str(root), "server_time_unix": time.time()})
                return
            _json_response(self, {"ok": False, "error": "not found", "path": parsed.path}, status=404)

    try:
        server = ThreadingHTTPServer((args.host, args.port), ProgressHandler)
    except OSError as exc:
        print(f"Could not start progress server on {args.host}:{args.port}: {exc}", file=sys.stderr)
        return 1
    host, port = server.server_address[:2]
    print(f"Serving NECST progress dashboard at http://{host}:{port}/")
    print(f"Progress root: {root}")
    try:
        server.serve_forever(poll_interval=0.5)
    except KeyboardInterrupt:
        print("\nStopping progress dashboard.")
        return 0
    finally:
        server.server_close()
        live.close()
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Show NECST observation progress")
    parser.add_argument("--root", default=None, help="Progress root directory (default: NECST_PROGRESS_ROOT or /tmp/necst_progress)")
    parser.add_argument("--interval", type=float, default=1.0, help="Watch/follow refresh interval [s]")
    parser.add_argument("--events", type=int, default=8, help="Number of recent events to show")
    parser.add_argument("--compact", action="store_true", help="Print compact one-line view")
    parser.add_argument("--json", action="store_true", help="Print raw JSON snapshot or JSONL events")
    parser.add_argument("--watch", action="store_true", help="Continuously redraw a fixed dashboard")
    parser.add_argument("--once", action="store_true", help="Print once and exit (default)")
    parser.add_argument("--follow", action="store_true", help="Follow observation_events.jsonl as a scrolling log")
    parser.add_argument("--serve", action="store_true", help="Serve a lightweight web dashboard")
    parser.add_argument("--host", default="127.0.0.1", help="With --serve, bind address (default: 127.0.0.1)")
    parser.add_argument("--port", type=int, default=8080, help="With --serve, bind port (default: 8080)")
    parser.add_argument("--refresh-ms", type=int, default=1000, help="With --serve, browser refresh interval [ms]")
    parser.add_argument("--quiet", action="store_true", help="With --serve, suppress HTTP request logs")
    parser.add_argument("--no-ros", action="store_true", help="Disable optional live ROS telemetry augmentation")
    parser.add_argument("--from-end", action="store_true", help="With --follow, start at end of current event log")
    parser.add_argument("--no-clear", action="store_true", help="With --watch, do not clear/redraw the terminal")
    return parser


def main(argv: Optional[List[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    selected = sum(bool(x) for x in (args.watch, args.once, args.follow, args.serve))
    if selected > 1:
        print("Choose only one of --watch, --once, --follow, --serve.", file=sys.stderr)
        return 2
    if args.serve:
        return cmd_serve(args)
    if args.follow:
        return cmd_follow(args)
    if args.watch:
        return cmd_watch(args)
    return cmd_once(args)


if __name__ == "__main__":
    raise SystemExit(main())
