#!/usr/bin/env python3
"""Plot NECST observation-progress sidecar files.

This command intentionally has no ROS imports.  It reads the structured files
written by ObservationProgressReporter and creates static figures suitable for
quick checks after or during an observation.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
import sys
import time
import warnings
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple


PLOT_KINDS = {"auto", "timeline", "map", "skydip", "summary", "dashboard"}

# Fixed, high-contrast colors for human-readable progress figures.
# These are intentionally kept in this plotting command rather than in the
# observation runtime, so observation execution remains unaffected by UI choices.
STATUS_COLOR = {
    "done": "#2ca02c",
    "current": "#ff7f0e",
    "pending": "#bdbdbd",
    "snapshot": "#1f77b4",
}
PHASE_COLOR = {
    "HOT": "#d62728",
    "OFF": "#1f77b4",
    "ON": "#2ca02c",
    "SKY": "#9467bd",
    "MOVE": "#8c564b",
}


def phase_color(label: Any) -> str:
    text = str(label or "").upper()
    for key, color in PHASE_COLOR.items():
        if key in text:
            return color
    return "#7f7f7f"


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


def read_json(path: Path, *, required: bool = True) -> Optional[Dict[str, Any]]:
    if not path.exists():
        if required:
            raise FileNotFoundError(f"Required JSON file is missing: {path}")
        return None
    try:
        payload = json.loads(strip_record_file_header(path.read_text(encoding="utf-8")))
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid JSON in {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise ValueError(f"Top-level JSON must be an object: {path}")
    return payload


def read_events(path: Path, *, required: bool = True) -> List[Dict[str, Any]]:
    if not path.exists():
        if required:
            raise FileNotFoundError(f"Required JSONL file is missing: {path}")
        return []
    events: List[Dict[str, Any]] = []
    for lineno, line in structured_lines(path.read_text(encoding="utf-8")):
        try:
            payload = json.loads(line)
        except json.JSONDecodeError as exc:
            raise ValueError(f"Invalid JSONL in {path} line {lineno}: {exc}") from exc
        if isinstance(payload, dict):
            events.append(payload)
    return events


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


def latest_progress_dir(root: Path) -> Optional[Path]:
    record = current_record_name(root)
    if record:
        candidate = root / safe_name(record)
        if candidate.exists():
            return candidate
    candidates = [p for p in root.iterdir() if p.is_dir()] if root.exists() else []
    candidates = sorted(candidates, key=lambda p: p.stat().st_mtime, reverse=True)
    return candidates[0] if candidates else None


def resolve_progress_dir(args: argparse.Namespace) -> Path:
    if args.path:
        return Path(args.path).expanduser()
    root = progress_root(args.root)
    if args.latest:
        latest = latest_progress_dir(root)
        if latest is None:
            raise FileNotFoundError(f"No progress directory found below {root}")
        return latest
    current = root / "current_observation_progress.json"
    if current.exists():
        latest = latest_progress_dir(root)
        if latest is not None:
            return latest
    raise FileNotFoundError(
        "Specify --path <progress_dir> or use --latest with an existing progress root"
    )


def load_progress_tree(
    path: Path,
) -> Tuple[Dict[str, Any], Dict[str, Any], List[Dict[str, Any]]]:
    snapshot = read_json(path / "observation_progress.json") or {}
    plan = read_json(path / "observation_plan.json", required=False) or {"items": []}
    events = read_events(path / "observation_events.jsonl", required=False)
    return snapshot, plan, events


def import_matplotlib():
    try:
        import matplotlib

        matplotlib.use("Agg")
        # Record names and target names can contain Japanese characters.  Some
        # observing environments have Japanese fonts installed and render them
        # correctly; minimal CI/containers often do not.  Suppress matplotlib's
        # repeated missing-glyph warnings so --json output remains clean on stderr.
        warnings.filterwarnings(
            "ignore", message="Glyph .* missing from font.*", category=UserWarning
        )
        import matplotlib.pyplot as plt
        from matplotlib.patches import Rectangle
    except Exception as exc:  # pragma: no cover - depends on optional install
        raise RuntimeError(
            "matplotlib is required for progress-plot. Install it in the NECST environment."
        ) from exc
    return plt, Rectangle


def as_float_pair(value: Any) -> Optional[Tuple[float, float]]:
    if not isinstance(value, (list, tuple)) or len(value) < 2:
        return None
    try:
        x = float(value[0])
        y = float(value[1])
    except Exception:
        return None
    if not (math.isfinite(x) and math.isfinite(y)):
        return None
    return x, y


def item_geometry(item: Mapping[str, Any]) -> Dict[str, Any]:
    geom = item.get("geometry")
    return geom if isinstance(geom, dict) else {}


def flatten_plan_items(plan: Mapping[str, Any]) -> List[Dict[str, Any]]:
    raw_items = plan.get("items") or []
    result: List[Dict[str, Any]] = []
    if not isinstance(raw_items, list):
        return result
    for item in raw_items:
        if not isinstance(item, dict):
            continue
        result.append(item)
        geom = item_geometry(item)
        if geom.get("kind") in {"scan_block", "scan_block_line"}:
            lines = geom.get("lines")
            if isinstance(lines, list):
                for line in lines:
                    if not isinstance(line, dict):
                        continue
                    child = dict(item)
                    child_geom = dict(line)
                    child_geom.setdefault("kind", "scan_block_line")
                    child_geom.setdefault("frame", geom.get("frame"))
                    child_geom.setdefault("unit", geom.get("unit"))
                    child["geometry"] = child_geom
                    parent_uid = str(item.get("item_uid") or "scan_block")
                    child["_parent_item_uid"] = parent_uid
                    child["item_uid"] = (
                        f"{parent_uid}:line:{line.get('line_index0', len(result))}"
                    )
                    child["line_index0"] = line.get("line_index0")
                    if child.get("index0") is None and item.get("index0") is not None:
                        child["index0"] = item.get("index0")
                    result.append(child)
    return result


def event_item_uid(event: Mapping[str, Any]) -> Optional[str]:
    uid = event.get("item_uid")
    return str(uid) if uid not in (None, "") else None


def completed_item_uids(events: Sequence[Mapping[str, Any]]) -> set:
    return {
        uid
        for event in events
        if event.get("event") == "plan_item_finished"
        for uid in [event_item_uid(event)]
        if uid
    }


def _index_range(payload: Mapping[str, Any]) -> Optional[Tuple[int, int]]:
    try:
        start = int(payload.get("index0"))
    except Exception:
        return None
    try:
        end = (
            int(payload.get("index0_end"))
            if payload.get("index0_end") is not None
            else start
        )
    except Exception:
        end = start
    if end < start:
        start, end = end, start
    return start, end


def completed_index_ranges(
    events: Sequence[Mapping[str, Any]]
) -> List[Tuple[int, int]]:
    ranges: List[Tuple[int, int]] = []
    for event in events:
        if event.get("event") != "plan_item_finished":
            continue
        index_range = _index_range(event)
        if index_range is not None:
            ranges.append(index_range)
    return ranges


FINAL_LIFECYCLE_STATES = {"finished", "error", "aborted"}


def _snapshot_is_final(snapshot: Mapping[str, Any]) -> bool:
    lifecycle = (
        snapshot.get("lifecycle") if isinstance(snapshot.get("lifecycle"), dict) else {}
    )
    return str(lifecycle.get("state") or "").lower() in FINAL_LIFECYCLE_STATES


def current_item_uid(snapshot: Mapping[str, Any]) -> Optional[str]:
    if _snapshot_is_final(snapshot):
        return None
    plan = snapshot.get("plan") if isinstance(snapshot.get("plan"), dict) else {}
    uid = plan.get("item_uid")
    return str(uid) if uid not in (None, "") else None


def current_index_range(snapshot: Mapping[str, Any]) -> Optional[Tuple[int, int]]:
    if _snapshot_is_final(snapshot):
        return None
    plan = snapshot.get("plan") if isinstance(snapshot.get("plan"), dict) else {}
    return _index_range(plan)


def _index_in_ranges(
    item: Mapping[str, Any], ranges: Sequence[Tuple[int, int]]
) -> bool:
    try:
        idx = int(item.get("index0"))
    except Exception:
        return False
    return any(start <= idx <= end for start, end in ranges)


def auto_kind(plan: Mapping[str, Any], snapshot: Mapping[str, Any]) -> str:
    items = flatten_plan_items(plan)
    kinds = {item_geometry(item).get("kind") for item in items}
    snap_geom = (
        snapshot.get("geometry") if isinstance(snapshot.get("geometry"), dict) else {}
    )
    kinds.add(snap_geom.get("kind"))
    if kinds & {"scan_line", "scan_block", "scan_block_line", "grid_point", "point"}:
        return "map"
    if kinds & {"skydip_elevation"}:
        return "skydip"
    return "timeline"


def event_label(event: Mapping[str, Any]) -> str:
    parts = [str(event.get("event", "event"))]
    for key in ("phase", "mode", "drive_kind", "motion_stage", "id", "item_uid"):
        value = event.get(key)
        if value not in (None, ""):
            parts.append(f"{key}={value}")
    return " ".join(parts)


def event_time_range(
    events: Sequence[Mapping[str, Any]], snapshot: Mapping[str, Any]
) -> Tuple[float, float]:
    event_times: List[float] = []
    for event in events:
        value = event.get("time_unix")
        if isinstance(value, (int, float)) and math.isfinite(float(value)):
            event_times.append(float(value))
    timing = snapshot.get("time") if isinstance(snapshot.get("time"), dict) else {}
    if event_times:
        times = list(event_times)
        ref = min(event_times)
        # Synthetic tests and manually edited files can contain stale/mocked
        # started_at_unix values. Do not let a far-away start timestamp force
        # matplotlib to display huge epoch offsets on the elapsed-time axis.
        for key in ("started_at_unix", "updated_at_unix"):
            value = timing.get(key)
            if isinstance(value, (int, float)) and math.isfinite(float(value)):
                value = float(value)
                if abs(value - ref) <= 86400.0:
                    times.append(value)
    else:
        times = []
        for key in ("started_at_unix", "updated_at_unix"):
            value = timing.get(key)
            if isinstance(value, (int, float)) and math.isfinite(float(value)):
                times.append(float(value))
    if not times:
        return 0.0, 1.0
    start, end = min(times), max(times)
    if end <= start:
        end = start + 1.0
    return start, end


def build_phase_spans(
    events: Sequence[Mapping[str, Any]], snapshot: Mapping[str, Any]
) -> List[Dict[str, Any]]:
    start_time, end_time = event_time_range(events, snapshot)
    open_by_key: Dict[Tuple[str, str], Dict[str, Any]] = {}
    spans: List[Dict[str, Any]] = []

    def close_matching(
        event: Mapping[str, Any], finish_time: float, suffix: str
    ) -> None:
        phase = str(event.get("phase") or event.get("mode") or "")
        uid = str(event.get("item_uid") or event.get("id") or phase or "unknown")
        candidates = [(suffix, uid), (suffix, phase), (suffix, "*")]
        for key in candidates:
            if key in open_by_key:
                span = open_by_key.pop(key)
                span["end"] = finish_time
                spans.append(span)
                return

    for event in events:
        t = event.get("time_unix")
        if not isinstance(t, (int, float)):
            continue
        t = float(t)
        name = str(event.get("event", ""))
        phase = str(
            event.get("phase")
            or event.get("mode")
            or event.get("expected_metadata_position")
            or ""
        )
        uid = str(event.get("item_uid") or event.get("id") or phase or "unknown")
        if name.endswith("started"):
            if name == "integration_started":
                key = ("integration", uid)
                label = phase or str(event.get("metadata_position") or "integration")
                open_by_key[key] = {
                    "kind": "integration",
                    "label": label,
                    "start": t,
                    "uid": uid,
                }
            elif name in {"plan_item_started", "drive_started"}:
                key = (
                    name.replace("_started", ""),
                    uid if name == "plan_item_started" else "*",
                )
                label = phase or event.get("drive_kind") or name.replace("_started", "")
                open_by_key[key] = {
                    "kind": key[0],
                    "label": str(label),
                    "start": t,
                    "uid": uid,
                }
        elif name.endswith("finished"):
            if name == "integration_finished":
                close_matching(event, t, "integration")
            elif name == "plan_item_finished":
                close_matching(event, t, "plan_item")
            elif name == "drive_finished":
                close_matching(event, t, "drive")
    for span in open_by_key.values():
        span["end"] = end_time
        spans.append(span)
    if not spans and events:
        # Fall back to point markers rendered as short spans.
        dt = max(1.0, (end_time - start_time) / max(20, len(events) * 5))
        for event in events:
            t = event.get("time_unix")
            if isinstance(t, (int, float)):
                spans.append(
                    {
                        "kind": "event",
                        "label": str(event.get("event", "event")),
                        "start": float(t),
                        "end": float(t) + dt,
                    }
                )
    return sorted(
        spans,
        key=lambda item: (float(item.get("start", 0.0)), str(item.get("label", ""))),
    )


def _draw_timeline(
    ax: Any, snapshot: Mapping[str, Any], events: Sequence[Mapping[str, Any]]
) -> None:
    spans = build_phase_spans(events, snapshot)
    start_time, end_time = event_time_range(events, snapshot)
    labels: List[str] = []
    preferred = ["HOT", "OFF", "ON", "SKY"]
    for label in preferred:
        if any(label in str(span.get("label", "")).upper() for span in spans):
            labels.append(label)
    for span in spans:
        label = str(span.get("label") or span.get("kind") or "event")
        compact = next((p for p in preferred if p in label.upper()), label)
        if compact not in labels:
            labels.append(compact)
    if not labels:
        labels = ["events"]
    y_of = {label: idx for idx, label in enumerate(labels)}
    for span in spans:
        raw_label = str(span.get("label") or span.get("kind") or "event")
        label = next((p for p in preferred if p in raw_label.upper()), raw_label)
        y = y_of.get(label, 0)
        left = float(span.get("start", start_time)) - start_time
        width = max(
            0.02,
            float(span.get("end", start_time)) - float(span.get("start", start_time)),
        )
        ax.barh(
            y,
            width,
            left=left,
            height=0.72,
            align="center",
            color=phase_color(raw_label),
            alpha=0.85,
        )
    timing = snapshot.get("time") if isinstance(snapshot.get("time"), dict) else {}
    now = timing.get("updated_at_unix")
    if isinstance(now, (int, float)) and start_time <= float(now) <= end_time:
        ax.axvline(
            float(now) - start_time,
            color="black",
            linestyle="--",
            linewidth=1.0,
            alpha=0.8,
        )
        ax.text(
            float(now) - start_time, len(labels) - 0.2, " now", va="bottom", fontsize=8
        )
    if not spans:
        ax.text(
            0.5,
            0.5,
            "No events to plot",
            ha="center",
            va="center",
            transform=ax.transAxes,
        )
    ax.set_yticks(list(y_of.values()))
    ax.set_yticklabels(labels)
    ax.set_xlabel("Elapsed time since first event [s]")
    ax.grid(True, axis="x", alpha=0.3)


def plot_timeline(
    snapshot: Mapping[str, Any],
    plan: Mapping[str, Any],
    events: Sequence[Mapping[str, Any]],
    out: Path,
    *,
    dpi: int,
) -> None:
    plt, _ = import_matplotlib()
    labels = {
        str(span.get("label") or span.get("kind") or "event")
        for span in build_phase_spans(events, snapshot)
    }
    height = max(3.5, 1.2 + 0.45 * max(1, len(labels)))
    fig, ax = plt.subplots(figsize=(11.0, height))
    _draw_timeline(ax, snapshot, events)
    ax.set_title(progress_title(snapshot, "Timeline"))
    fig.tight_layout()
    fig.savefig(out, dpi=dpi)
    plt.close(fig)


def progress_title(snapshot: Mapping[str, Any], suffix: str) -> str:
    obs = (
        snapshot.get("observation")
        if isinstance(snapshot.get("observation"), dict)
        else {}
    )
    lifecycle = (
        snapshot.get("lifecycle") if isinstance(snapshot.get("lifecycle"), dict) else {}
    )
    obs_type = obs.get("type") or "Observation"
    record = obs.get("record_name") or "unknown record"
    state = lifecycle.get("state") or "unknown"
    return f"{obs_type} {suffix} — {record} ({state})"


def status_for_item(
    item: Mapping[str, Any],
    done: set,
    current_uid: Optional[str],
    *,
    done_ranges: Sequence[Tuple[int, int]] = (),
    current_range: Optional[Tuple[int, int]] = None,
) -> Tuple[str, str]:
    uid = str(item.get("item_uid") or "")
    parent_uid = str(item.get("_parent_item_uid") or "")

    # Event history is stronger than the latest snapshot.  The runner keeps the
    # just-finished plan item in observation_progress.json until the next item
    # starts, so a short inter-item gap can have a stale snapshot current item.
    # In that case, plan_item_finished / done ranges must win and the line/point
    # must be shown as done, not current.
    if (uid and uid in done) or (parent_uid and parent_uid in done):
        return "done", "x"
    if done_ranges and _index_in_ranges(item, done_ranges):
        return "done", "x"
    if current_uid and (uid == current_uid or parent_uid == current_uid):
        return "current", "o"
    if current_range is not None and _index_in_ranges(item, [current_range]):
        return "current", "o"
    return "pending", "."


def geometry_xy(geom: Mapping[str, Any]) -> Optional[Tuple[float, float]]:
    for key in ("target", "reference", "offset"):
        pair = as_float_pair(geom.get(key))
        if pair is not None:
            return pair
    if geom.get("target_name"):
        return None
    return None


def _draw_map(
    ax: Any,
    snapshot: Mapping[str, Any],
    plan: Mapping[str, Any],
    events: Sequence[Mapping[str, Any]],
) -> int:
    items = flatten_plan_items(plan)
    done = completed_item_uids(events)
    done_ranges = completed_index_ranges(events)
    current = current_item_uid(snapshot)
    active_range = current_index_range(snapshot)
    plotted = 0
    label_used: set = set()

    # Draw pending first, then done, then current so the current item is visible.
    staged: List[Tuple[str, str, Mapping[str, Any], Dict[str, Any]]] = []
    for item in items:
        geom = item_geometry(item)
        kind = geom.get("kind")
        if kind not in {"scan_line", "scan_block_line", "point", "grid_point"}:
            continue
        status, marker = status_for_item(
            item, done, current, done_ranges=done_ranges, current_range=active_range
        )
        staged.append((status, marker, item, geom))
    order = {"pending": 0, "done": 1, "current": 2}
    for status, marker, item, geom in sorted(staged, key=lambda x: order.get(x[0], 0)):
        kind = geom.get("kind")
        color = STATUS_COLOR.get(status, "#7f7f7f")
        label = status if status not in label_used else None
        label_used.add(status)
        if kind in {"scan_line", "scan_block_line"}:
            start = as_float_pair(geom.get("start"))
            stop = as_float_pair(geom.get("stop"))
            if start is None or stop is None:
                continue
            lw = 2.8 if status == "current" else 1.3
            alpha = 0.9 if status != "pending" else 0.55
            ax.plot(
                [start[0], stop[0]],
                [start[1], stop[1]],
                color=color,
                linewidth=lw,
                alpha=alpha,
                marker=marker if status == "current" else None,
                label=label,
            )
            plotted += 1
        elif kind in {"point", "grid_point"}:
            xy = geometry_xy(geom)
            if xy is None:
                continue
            size = 70 if status == "current" else 28
            edge = "black" if status == "current" else "none"
            ax.scatter(
                [xy[0]],
                [xy[1]],
                s=size,
                color=color,
                edgecolors=edge,
                linewidths=0.8,
                marker="o",
                label=label,
                alpha=0.95,
            )
            plotted += 1
    snap_geom = (
        snapshot.get("geometry") if isinstance(snapshot.get("geometry"), dict) else {}
    )
    # The blue snapshot overlay is useful only while the snapshot really denotes
    # an active item.  During the inter-item gap after plan_item_finished, the
    # snapshot intentionally still contains the completed item, but drawing it
    # again as a snapshot/current overlay makes the finished line look active.
    snapshot_status = "pending"
    snap_plan = snapshot.get("plan") if isinstance(snapshot.get("plan"), dict) else {}
    if isinstance(snap_plan, dict):
        snapshot_status, _ = status_for_item(
            snap_plan,
            done,
            current,
            done_ranges=done_ranges,
            current_range=active_range,
        )
    if isinstance(snap_geom, dict) and snapshot_status != "done":
        if snap_geom.get("kind") in {"scan_line", "scan_block_line"}:
            start = as_float_pair(snap_geom.get("start"))
            stop = as_float_pair(snap_geom.get("stop"))
            if start is not None and stop is not None:
                ax.plot(
                    [start[0], stop[0]],
                    [start[1], stop[1]],
                    color=STATUS_COLOR["snapshot"],
                    linewidth=3.2,
                    alpha=0.65,
                    label="snapshot",
                )
                plotted += 1
        elif snap_geom.get("kind") in {"point", "grid_point"}:
            xy = geometry_xy(snap_geom)
            if xy is not None:
                ax.scatter(
                    [xy[0]],
                    [xy[1]],
                    s=95,
                    color=STATUS_COLOR["snapshot"],
                    edgecolors="black",
                    linewidths=0.8,
                    label="snapshot",
                )
                plotted += 1
    if plotted == 0:
        ax.text(
            0.5,
            0.5,
            "No numeric point/scan geometry found",
            ha="center",
            va="center",
            transform=ax.transAxes,
        )
    frame = ""
    for item in items:
        frame = str(item_geometry(item).get("frame") or "")
        if frame:
            break
    unit = ""
    for item in items:
        unit = str(item_geometry(item).get("unit") or "")
        if unit:
            break
    suffix = f" [{unit}]" if unit else ""
    ax.set_xlabel(f"{frame} x{suffix}" if frame else f"x{suffix}")
    ax.set_ylabel(f"{frame} y{suffix}" if frame else f"y{suffix}")
    ax.grid(True, alpha=0.3)
    if plotted:
        try:
            ax.set_aspect("equal", adjustable="datalim")
        except Exception:
            pass
    if label_used or plotted:
        ax.legend(loc="best")
    return plotted


def plot_map(
    snapshot: Mapping[str, Any],
    plan: Mapping[str, Any],
    events: Sequence[Mapping[str, Any]],
    out: Path,
    *,
    dpi: int,
) -> None:
    plt, _ = import_matplotlib()
    fig, ax = plt.subplots(figsize=(8.5, 7.0))
    _draw_map(ax, snapshot, plan, events)
    ax.set_title(progress_title(snapshot, "Map"))
    fig.tight_layout()
    fig.savefig(out, dpi=dpi)
    plt.close(fig)


def _skydip_points(
    snapshot: Mapping[str, Any],
    plan: Mapping[str, Any],
    events: Sequence[Mapping[str, Any]],
) -> List[Tuple[int, float, str, str]]:
    items = flatten_plan_items(plan)
    done = completed_item_uids(events)
    done_ranges = completed_index_ranges(events)
    current = current_item_uid(snapshot)
    active_range = current_index_range(snapshot)
    points: List[Tuple[int, float, str, str]] = []
    for idx, item in enumerate(items):
        geom = item_geometry(item)
        el = (
            geom.get("el_deg") or geom.get("target_el_deg") or geom.get("elevation_deg")
        )
        if el is None and geom.get("target") is not None:
            pair = as_float_pair(geom.get("target"))
            if pair is not None:
                el = pair[1]
        try:
            value = float(el)
        except Exception:
            continue
        status, _ = status_for_item(
            item, done, current, done_ranges=done_ranges, current_range=active_range
        )
        label = str(item.get("mode") or item.get("label") or idx + 1)
        points.append((idx + 1, value, label, status))
    return points


def _draw_skydip(
    ax: Any,
    snapshot: Mapping[str, Any],
    plan: Mapping[str, Any],
    events: Sequence[Mapping[str, Any]],
) -> int:
    points = _skydip_points(snapshot, plan, events)
    if points:
        xs = [p[0] for p in points]
        ys = [p[1] for p in points]
        ax.plot(xs, ys, color="#9467bd", linewidth=1.5, alpha=0.75)
        used: set = set()
        for x, y, label, status in points:
            color = STATUS_COLOR.get(status, "#7f7f7f")
            edge = "black" if status == "current" else "none"
            size = 70 if status == "current" else 38
            legend_label = status if status not in used else None
            used.add(status)
            ax.scatter(
                [x],
                [y],
                s=size,
                color=color,
                edgecolors=edge,
                linewidths=0.8,
                label=legend_label,
                zorder=3,
            )
            if status == "current":
                ax.text(x, y, f" {label}", va="center", fontsize=8)
    else:
        ax.text(
            0.5,
            0.5,
            "No skydip elevation geometry found",
            ha="center",
            va="center",
            transform=ax.transAxes,
        )
    ax.set_xlabel("Sequence index")
    ax.set_ylabel("Elevation [deg]")
    ax.grid(True, alpha=0.3)
    if points:
        ax.legend(loc="best")
    return len(points)


def plot_skydip(
    snapshot: Mapping[str, Any],
    plan: Mapping[str, Any],
    events: Sequence[Mapping[str, Any]],
    out: Path,
    *,
    dpi: int,
) -> None:
    plt, _ = import_matplotlib()
    fig, ax = plt.subplots(figsize=(8.0, 5.0))
    _draw_skydip(ax, snapshot, plan, events)
    ax.set_title(progress_title(snapshot, "Skydip"))
    fig.tight_layout()
    fig.savefig(out, dpi=dpi)
    plt.close(fig)


def plot_summary(
    snapshot: Mapping[str, Any],
    plan: Mapping[str, Any],
    events: Sequence[Mapping[str, Any]],
    out: Path,
    *,
    dpi: int,
) -> None:
    plt, _ = import_matplotlib()
    obs = (
        snapshot.get("observation")
        if isinstance(snapshot.get("observation"), dict)
        else {}
    )
    lifecycle = (
        snapshot.get("lifecycle") if isinstance(snapshot.get("lifecycle"), dict) else {}
    )
    current_plan = (
        snapshot.get("plan") if isinstance(snapshot.get("plan"), dict) else {}
    )
    activity = (
        snapshot.get("activity") if isinstance(snapshot.get("activity"), dict) else {}
    )
    timing = snapshot.get("time") if isinstance(snapshot.get("time"), dict) else {}
    items = plan.get("items") if isinstance(plan.get("items"), list) else []
    lines = [
        f"type: {obs.get('type')}",
        f"record: {obs.get('record_name')}",
        f"obs_file: {obs.get('obs_file')}",
        f"state: {lifecycle.get('state')}",
        f"plan item: {current_plan.get('index0')} / {current_plan.get('total')}",
        f"phase: {activity.get('phase')}",
        f"drive: {activity.get('drive_kind')} / {activity.get('motion_stage')}",
        f"data_state: {activity.get('data_state')}",
        f"elapsed_sec: {timing.get('elapsed_sec')}",
        f"plan item count: {len(items)}",
        f"event count: {len(events)}",
    ]
    fig, ax = plt.subplots(figsize=(8.5, 5.0))
    ax.axis("off")
    ax.text(
        0.02,
        0.98,
        "NECST Observation Progress Summary\n\n" + "\n".join(lines),
        va="top",
        family="monospace",
    )
    fig.tight_layout()
    fig.savefig(out, dpi=dpi)
    plt.close(fig)


def format_plan_progress(plan: Mapping[str, Any]) -> str:
    idx = plan.get("index0")
    total = plan.get("total")
    end = plan.get("index0_end")
    if isinstance(idx, int) and isinstance(total, int) and total > 0:
        if isinstance(end, int) and end >= idx:
            return f"{idx + 1}-{end + 1} / {total}"
        return f"{idx + 1} / {total}"
    if total:
        return f"- / {total}"
    return "-"


def _summary_lines(
    snapshot: Mapping[str, Any],
    plan: Mapping[str, Any],
    events: Sequence[Mapping[str, Any]],
) -> List[str]:
    obs = (
        snapshot.get("observation")
        if isinstance(snapshot.get("observation"), dict)
        else {}
    )
    lifecycle = (
        snapshot.get("lifecycle") if isinstance(snapshot.get("lifecycle"), dict) else {}
    )
    current_plan = (
        snapshot.get("plan") if isinstance(snapshot.get("plan"), dict) else {}
    )
    activity = (
        snapshot.get("activity") if isinstance(snapshot.get("activity"), dict) else {}
    )
    data = snapshot.get("data") if isinstance(snapshot.get("data"), dict) else {}
    timing = snapshot.get("time") if isinstance(snapshot.get("time"), dict) else {}
    return [
        f"state: {lifecycle.get('state')}",
        f"type: {obs.get('type')}",
        f"record: {obs.get('record_name')}",
        f"plan: {format_plan_progress(current_plan)}  mode={current_plan.get('mode')}",
        f"activity: phase={activity.get('phase')} drive={activity.get('drive_kind')} stage={activity.get('motion_stage')}",
        f"data: expected={data.get('expected_metadata_position')} latest={data.get('latest_spectrum_position')} age={data.get('latest_spectrum_age_sec')}",
        f"elapsed_sec: {timing.get('elapsed_sec')}  remaining_sec≈{timing.get('estimated_remaining_sec')}",
        f"plan items: {len(flatten_plan_items(plan))}  events: {len(events)}",
    ]


def plot_dashboard(
    snapshot: Mapping[str, Any],
    plan: Mapping[str, Any],
    events: Sequence[Mapping[str, Any]],
    out: Path,
    *,
    dpi: int,
) -> None:
    plt, _ = import_matplotlib()
    fig, axes = plt.subplots(
        2,
        2,
        figsize=(12.5, 8.0),
        constrained_layout=True,
        gridspec_kw={"height_ratios": [0.92, 1.08]},
    )
    ax_summary, ax_timeline = axes[0]
    ax_geom, ax_extra = axes[1]

    ax_summary.axis("off")
    ax_summary.text(
        0.02,
        0.98,
        "Current status\n\n" + "\n".join(_summary_lines(snapshot, plan, events)),
        va="top",
        family="monospace",
        fontsize=9,
    )
    ax_summary.set_title("Status")

    _draw_timeline(ax_timeline, snapshot, events)
    ax_timeline.set_title("Timeline")

    plotted = _draw_map(ax_geom, snapshot, plan, events)
    ax_geom.set_title("Plan map")

    if auto_kind(plan, snapshot) == "skydip":
        _draw_skydip(ax_extra, snapshot, plan, events)
        ax_extra.set_title("Skydip")
    elif plotted == 0:
        _draw_skydip(ax_extra, snapshot, plan, events)
        ax_extra.set_title("Skydip / sequence")
    else:
        ax_extra.axis("off")
        recent = events[-8:] if events else []
        lines = ["Recent events"]
        for ev in recent:
            lines.append(f"{ev.get('seq', '')}: {event_label(ev)}")
        ax_extra.text(
            0.02, 0.98, "\n".join(lines), va="top", family="monospace", fontsize=8
        )
        ax_extra.set_title("Recent events")

    fig.suptitle(progress_title(snapshot, "Dashboard"), fontsize=12)
    fig.savefig(out, dpi=dpi)
    plt.close(fig)


def default_output(path: Path, kind: str) -> Path:
    suffix = "png"
    return path / f"observation_progress_{kind}.{suffix}"


def run(args: argparse.Namespace) -> int:
    progress_dir = resolve_progress_dir(args)
    snapshot, plan, events = load_progress_tree(progress_dir)
    kind = args.kind
    if kind == "auto":
        kind = auto_kind(plan, snapshot)
    out = (
        Path(args.out).expanduser() if args.out else default_output(progress_dir, kind)
    )
    out.parent.mkdir(parents=True, exist_ok=True)
    if kind == "timeline":
        plot_timeline(snapshot, plan, events, out, dpi=args.dpi)
    elif kind == "map":
        plot_map(snapshot, plan, events, out, dpi=args.dpi)
    elif kind == "skydip":
        plot_skydip(snapshot, plan, events, out, dpi=args.dpi)
    elif kind == "summary":
        plot_summary(snapshot, plan, events, out, dpi=args.dpi)
    elif kind == "dashboard":
        plot_dashboard(snapshot, plan, events, out, dpi=args.dpi)
    else:
        raise ValueError(f"Unknown plot kind: {kind}")
    if args.json:
        print(
            json.dumps(
                {"ok": True, "kind": kind, "path": str(progress_dir), "out": str(out)},
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
            )
        )
    else:
        print(f"Wrote {kind} plot: {out}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Plot NECST observation-progress sidecar files."
    )
    source = parser.add_mutually_exclusive_group()
    source.add_argument(
        "--path", help="Progress directory containing observation_progress.json"
    )
    source.add_argument(
        "--latest",
        action="store_true",
        help="Use latest progress directory under --root",
    )
    parser.add_argument(
        "--root",
        help="Progress root directory; default: NECST_PROGRESS_ROOT or /tmp/necst_progress",
    )
    parser.add_argument(
        "--kind",
        choices=sorted(PLOT_KINDS),
        default="auto",
        help="Plot kind; default: auto",
    )
    parser.add_argument(
        "--out",
        help="Output PNG/PDF path. Default is placed in the progress directory.",
    )
    parser.add_argument(
        "--dpi", type=int, default=120, help="Output DPI for raster formats"
    )
    parser.add_argument(
        "--json", action="store_true", help="Print machine-readable result"
    )
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return run(args)
    except Exception as exc:
        if args.json:
            print(
                json.dumps(
                    {"ok": False, "error": str(exc)},
                    ensure_ascii=False,
                    indent=2,
                    sort_keys=True,
                )
            )
        else:
            print(f"progress-plot: error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
