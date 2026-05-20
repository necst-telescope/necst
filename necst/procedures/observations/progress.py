"""Lightweight observation-progress reporting utilities.

This module is intentionally independent of ROS imports.  The observation runner
may attach a Commander instance; if that Commander has an ``observation_progress``
publisher, JSON snapshots are published as ``std_msgs/String``.  File and topic
publish failures are swallowed so progress reporting can never stop an
observation.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import statistics
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Mapping, Optional


def _safe_name(value: Any) -> str:
    """Return a stable ASCII directory name for live progress files.

    Record names may contain spaces, Japanese target names, or other characters
    that cannot safely be used in a simple monitoring path.  A lossy replacement
    alone can collide, e.g. two Japanese-only target names would both become
    ``unknown``.  When sanitization changes the input, append a short stable hash
    so reporter, CLI, and checker agree without losing uniqueness.
    """
    original = str(value or "unknown")
    sanitized = re.sub(r"[^A-Za-z0-9_.-]+", "_", original)
    sanitized = re.sub(r"_+", "_", sanitized).strip("_") or "record"
    if sanitized != original:
        digest = hashlib.sha1(original.encode("utf-8", errors="surrogatepass")).hexdigest()[:8]
        sanitized = f"{sanitized}_{digest}"
    return sanitized


def json_safe(value: Any) -> Any:
    """Convert common observation objects into JSON-serializable values."""
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, Mapping):
        return {str(k): json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_safe(v) for v in value]
    if hasattr(value, "to_value"):
        try:
            return float(value.to_value("deg"))
        except Exception:
            try:
                return float(value.to_value(""))
            except Exception:
                try:
                    return float(value.value)
                except Exception:
                    return str(value)
    if hasattr(value, "value"):
        try:
            return float(value.value)
        except Exception:
            return str(value)
    return str(value)


def deep_update(base: Dict[str, Any], update: Mapping[str, Any]) -> Dict[str, Any]:
    for key, value in update.items():
        if isinstance(value, Mapping) and isinstance(base.get(key), dict):
            deep_update(base[key], value)
        else:
            base[key] = json_safe(value)
    return base


@contextmanager
def _null_context() -> Iterator[None]:
    yield


class NullProgressReporter:
    """No-op reporter used before a real reporter is attached."""

    enabled = False

    def attach_commander(self, commander: Any) -> None:  # pragma: no cover - no-op
        return None

    def set_lifecycle(self, state: str, **fields: Any) -> None:  # pragma: no cover
        return None

    def update(self, **sections: Any) -> None:  # pragma: no cover
        return None

    def event(self, event: str, **fields: Any) -> None:  # pragma: no cover
        return None

    def set_plan(self, items: Iterable[Mapping[str, Any]], **fields: Any) -> None:
        return None

    def item(self, **fields: Any):
        return _null_context()

    def drive(self, **fields: Any):
        return _null_context()

    def integration(self, **fields: Any):
        return _null_context()

    def record_sidecars(self) -> None:  # pragma: no cover
        return None

    def close(self, state: str = "finished", **fields: Any) -> None:  # pragma: no cover
        return None



FINAL_LIFECYCLE_STATES = {"finished", "error", "aborted"}
RUNNING_LIFECYCLE_STATES = {"recording_starting", "running", "cleanup"}


def _finite_float(value: Any) -> Optional[float]:
    try:
        out = float(value)
    except Exception:
        return None
    return out if out == out and out not in (float("inf"), float("-inf")) else None


def _geometry_kind_from(obj: Mapping[str, Any]) -> str:
    geom = obj.get("geometry")
    if isinstance(geom, Mapping):
        kind = geom.get("kind")
        if kind not in (None, ""):
            return str(kind)
    kind = obj.get("geometry_kind")
    if kind not in (None, ""):
        return str(kind)
    return "unknown"


def _geometry_kind_from_event(event: Mapping[str, Any]) -> str:
    return str(event.get("geometry_kind") or _geometry_kind_from(event))


def _duration_key(obj: Mapping[str, Any]) -> str:
    phase = obj.get("phase") or obj.get("mode") or "UNKNOWN"
    drive = obj.get("drive_kind") or "unknown"
    geom = _geometry_kind_from(obj)
    return f"phase={phase}|drive={drive}|geometry={geom}"


def _duration_key_from_event(event: Mapping[str, Any]) -> str:
    phase = event.get("phase") or event.get("mode") or "UNKNOWN"
    drive = event.get("drive_kind") or "unknown"
    geom = event.get("geometry_kind") or _geometry_kind_from(event)
    return f"phase={phase}|drive={drive}|geometry={geom}"


def _phase_key(obj: Mapping[str, Any]) -> str:
    phase = obj.get("phase") or obj.get("mode") or "UNKNOWN"
    return f"phase={phase}"


def _phase_key_from_event(event: Mapping[str, Any]) -> str:
    phase = event.get("phase") or event.get("mode") or "UNKNOWN"
    return f"phase={phase}"


def _has_specific_duration_identity(item: Mapping[str, Any]) -> bool:
    """Return True when phase-only ETA fallback would be too broad.

    HOT/OFF/ON/SKY durations can differ substantially between point, scan,
    scan_block, skydip, and future observation modes.  If an item already has a
    concrete drive_kind and geometry.kind, using a phase-only median from a
    different drive/geometry can overestimate by orders of magnitude, e.g. a
    completed merged scan_block duration being reused for each remaining OTF
    scan_line.  Broad phase fallback is therefore reserved for legacy or
    incomplete items where no specific identity is available.
    """
    drive = str(item.get("drive_kind") or "").strip().lower()
    geom = str(_geometry_kind_from(item) or "").strip().lower()
    vague = {"", "unknown", "none", "null"}
    return drive not in vague and geom not in vague


def _allow_phase_duration_fallback(item: Mapping[str, Any]) -> bool:
    return not _has_specific_duration_identity(item)


def _median_recent(values: List[float], limit: int = 5) -> Optional[float]:
    clean = [float(v) for v in values if _finite_float(v) is not None and float(v) >= 0.0]
    if not clean:
        return None
    return float(statistics.median(clean[-limit:]))


def _index_start_end(item: Mapping[str, Any]) -> tuple[Optional[int], Optional[int]]:
    start = item.get("index0")
    end = item.get("index0_end", start)
    if not isinstance(start, int):
        return None, None
    if not isinstance(end, int) or end < start:
        end = start
    return start, end


class ObservationProgressReporter:
    """Best-effort structured progress reporter.

    The reporter writes three files below ``NECST_PROGRESS_ROOT``
    (default: ``/tmp/necst_progress``):

    - ``current_observation_progress.json``: latest snapshot for CLI/GUI.
    - ``<record_name>/observation_progress.json``: latest snapshot for this run.
    - ``<record_name>/observation_events.jsonl``: state-change event stream.
    - ``<record_name>/observation_plan.json``: static-ish plan geometry.

    None of the reporting operations raise to the caller.
    """

    def __init__(
        self,
        *,
        observation_type: str,
        record_name: str,
        obs_file: Optional[Any] = None,
        target: Optional[str] = None,
        started_at_unix: Optional[float] = None,
        logger: Optional[Any] = None,
        root: Optional[Any] = None,
    ) -> None:
        self.enabled = os.environ.get("NECST_PROGRESS_DISABLE", "").lower() not in {
            "1",
            "true",
            "yes",
            "on",
        }
        self.logger = logger
        self._commander: Optional[Any] = None
        self._seq = 0
        self._recent_events: List[Dict[str, Any]] = []
        self._events_all: List[Dict[str, Any]] = []
        self._plan_items: List[Dict[str, Any]] = []
        self._history_events_cache: Optional[List[Dict[str, Any]]] = None
        self._remaining_min_current_samples = int(os.environ.get("NECST_PROGRESS_REMAINING_MIN_CURRENT_SAMPLES", "2"))
        self._remaining_history_limit = int(os.environ.get("NECST_PROGRESS_REMAINING_HISTORY_LIMIT", "20"))
        self.started_at_unix = float(started_at_unix or time.time())
        root_path = Path(root or os.environ.get("NECST_PROGRESS_ROOT", "/tmp/necst_progress"))
        self.root = root_path.expanduser()
        self.record_name = str(record_name)
        self.record_dir = self.root / _safe_name(record_name)
        self.current_snapshot_path = self.root / "current_observation_progress.json"
        self.current_record_path = self.root / "current_observation_record.txt"
        self.snapshot_path = self.record_dir / "observation_progress.json"
        self.events_path = self.record_dir / "observation_events.jsonl"
        self.plan_path = self.record_dir / "observation_plan.json"
        self.snapshot: Dict[str, Any] = {
            "schema_version": 1,
            "observation": {
                "type": observation_type,
                "record_name": self.record_name,
                "obs_file": str(obs_file) if obs_file is not None else None,
                "target": target,
            },
            "lifecycle": {"state": "initializing"},
            "plan": {},
            "activity": {
                "phase": None,
                "drive_kind": None,
                "motion_stage": None,
                "data_state": "not_integrating",
            },
            "geometry": {},
            "antenna": {},
            "data": {},
            "time": {
                "started_at_unix": self.started_at_unix,
                "updated_at_unix": self.started_at_unix,
                "elapsed_sec": 0.0,
                "estimated_remaining_sec": None,
                "estimated_total_sec": None,
                "estimated_completion_unix": None,
                "remaining_method": "not_enough_data",
                "remaining_confidence": "unknown",
                "remaining_sample_count": 0,
            },
            "recent_events": [],
            "extra": {},
        }
        if self.enabled:
            self._ensure_dirs()
            self._write_snapshot()

    def attach_commander(self, commander: Any) -> None:
        self._commander = commander

    def _log_debug(self, message: str) -> None:
        try:
            if self.logger is not None:
                self.logger.debug(message)
        except Exception:
            pass

    def _ensure_dirs(self) -> None:
        try:
            self.root.mkdir(parents=True, exist_ok=True)
            self.record_dir.mkdir(parents=True, exist_ok=True)
        except Exception as exc:
            self._log_debug(f"Observation progress directory creation failed: {exc}")

    def _atomic_write_text(self, path: Path, text: str) -> None:
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            tmp = path.with_name(path.name + ".tmp")
            tmp.write_text(text, encoding="utf-8")
            tmp.replace(path)
        except Exception as exc:
            self._log_debug(f"Observation progress write failed for {path}: {exc}")

    def _append_jsonl(self, path: Path, payload: Mapping[str, Any]) -> None:
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(json_safe(payload), ensure_ascii=False, sort_keys=True))
                fh.write("\n")
        except Exception as exc:
            self._log_debug(f"Observation progress event write failed for {path}: {exc}")

    def _publish_snapshot(self, text: str) -> None:
        commander = self._commander
        if commander is None:
            return
        try:
            publisher = getattr(commander, "publisher", {}).get("observation_progress")
            if publisher is None:
                return
            from std_msgs.msg import String  # type: ignore

            try:
                msg = String(data=text)
            except TypeError:
                msg = String()
                msg.data = text
            publisher.publish(msg)
        except Exception as exc:
            self._log_debug(f"Observation progress topic publish failed: {exc}")

    def _write_snapshot(self) -> None:
        if not self.enabled:
            return
        now = time.time()
        self.snapshot.setdefault("time", {})["updated_at_unix"] = now
        self.snapshot["time"]["elapsed_sec"] = max(0.0, now - self.started_at_unix)
        self.snapshot["recent_events"] = self._recent_events[-10:]
        text = json.dumps(json_safe(self.snapshot), ensure_ascii=False, indent=2, sort_keys=True)
        self._atomic_write_text(self.snapshot_path, text + "\n")
        self._atomic_write_text(self.current_snapshot_path, text + "\n")
        self._atomic_write_text(self.current_record_path, self.record_name + "\n")
        self._publish_snapshot(text)

    def update(self, _replace_sections: Optional[Iterable[str]] = None, **sections: Any) -> None:
        """Best-effort snapshot update.

        By default, nested dictionaries are merged so callers can update only a
        few fields.  Some sections, however, must be replaced wholesale at
        state-boundaries.  In particular ``plan`` and ``geometry`` may contain
        keys such as ``index0_end``, ``lines``, ``start`` or ``stop`` that are
        meaningful for one item but wrong for the next one.  ``_replace_sections``
        prevents stale keys from leaking across observation items and confusing
        ``necst progress --watch`` or a future GUI.
        """
        if not self.enabled:
            return
        try:
            replace_sections = {str(name) for name in (_replace_sections or ())}
            for key, value in sections.items():
                if key in replace_sections:
                    self.snapshot[key] = json_safe(value)
                else:
                    current = self.snapshot.get(key)
                    if isinstance(value, Mapping) and isinstance(current, dict):
                        deep_update(current, value)
                    else:
                        self.snapshot[key] = json_safe(value)
            self._write_snapshot()
        except Exception as exc:
            self._log_debug(f"Observation progress update failed: {exc}")

    def set_lifecycle(self, state: str, **fields: Any) -> None:
        payload = {"state": state, **fields}
        self.update(lifecycle=payload)
        self.event(f"observation_{state}", **fields)

    def event(self, event: str, **fields: Any) -> None:
        if not self.enabled:
            return
        try:
            self._seq += 1
            payload = {
                "seq": self._seq,
                "event": event,
                "time_unix": time.time(),
                **json_safe(fields),
            }
            self._append_jsonl(self.events_path, payload)
            self._events_all.append(payload)
            self._recent_events.append(payload)
            self._recent_events = self._recent_events[-20:]
            self._refresh_remaining_estimate(now=float(payload["time_unix"]))
            self._write_snapshot()
        except Exception as exc:
            self._log_debug(f"Observation progress event failed: {exc}")

    def set_plan(self, items: Iterable[Mapping[str, Any]], **fields: Any) -> None:
        if not self.enabled:
            return
        items_list = [json_safe(item) for item in items]
        self._plan_items = [dict(item) for item in items_list if isinstance(item, Mapping)]
        self._history_events_cache = None
        doc = {
            "schema_version": 1,
            "observation": self.snapshot.get("observation", {}),
            "created_at_unix": time.time(),
            "item_count": len(items_list),
            "items": items_list,
            **json_safe(fields),
        }
        text = json.dumps(doc, ensure_ascii=False, indent=2, sort_keys=True)
        self._atomic_write_text(self.plan_path, text + "\n")
        self.update(plan={"total": len(items_list), "defined": True})
        self.event("plan_defined", item_count=len(items_list))

    def _load_history_events(self) -> List[Dict[str, Any]]:
        """Load a bounded set of previous event logs as duration priors.

        History is used only after the current observation has produced a small
        number of completed items/integrations.  This avoids showing a confident
        ETA at the very beginning based solely on another observation while still
        letting previous runs fill missing HOT/OFF/ON/move categories later.
        """
        if self._history_events_cache is not None:
            return self._history_events_cache
        events: List[Dict[str, Any]] = []
        if self._remaining_history_limit <= 0:
            self._history_events_cache = []
            return []
        try:
            paths = sorted(
                self.root.glob("*/observation_events.jsonl"),
                key=lambda q: q.stat().st_mtime if q.exists() else 0.0,
                reverse=True,
            )
            used = 0
            for path in paths:
                try:
                    if path.resolve() == self.events_path.resolve():
                        continue
                except Exception:
                    if path == self.events_path:
                        continue
                if used >= self._remaining_history_limit:
                    break
                try:
                    for line in path.read_text(encoding="utf-8").splitlines():
                        if not line.strip() or line.lstrip().startswith("#"):
                            continue
                        payload = json.loads(line)
                        if isinstance(payload, dict):
                            payload["_history"] = True
                            payload["_source_id"] = str(path)
                            events.append(payload)
                    used += 1
                except Exception:
                    continue
        except Exception:
            events = []
        self._history_events_cache = events
        return events

    def _duration_tables(self, events: List[Mapping[str, Any]]) -> Dict[str, Any]:
        item_by_key: Dict[str, List[float]] = {}
        item_by_phase: Dict[str, List[float]] = {}
        integration_by_key: Dict[str, List[float]] = {}
        integration_by_phase: Dict[str, List[float]] = {}
        drive_by_key: Dict[str, List[float]] = {}
        drive_by_drive: Dict[str, List[float]] = {}
        gaps: List[float] = []
        current_item_samples = 0
        last_item_finished_time: Optional[float] = None
        pending_item_starts: Dict[str, Mapping[str, Any]] = {}
        pending_integrations: Dict[str, Mapping[str, Any]] = {}
        pending_drives: Dict[str, Mapping[str, Any]] = {}
        last_source_id: Optional[str] = None

        def add(table: Dict[str, List[float]], key: str, value: Any) -> None:
            duration = _finite_float(value)
            if duration is not None and duration >= 0.0:
                table.setdefault(key, []).append(duration)

        def event_source_id(item: Mapping[str, Any]) -> str:
            return str(item.get("_source_id") or ("history" if item.get("_history") else "current"))

        for ev in sorted(
            events,
            key=lambda item: (
                event_source_id(item),
                float(item.get("time_unix", 0.0) or 0.0),
                int(item.get("seq", 0) or 0),
            ),
        ):
            source_id = event_source_id(ev)
            if last_source_id is None or source_id != last_source_id:
                pending_item_starts = {}
                pending_integrations = {}
                pending_drives = {}
                last_item_finished_time = None
                last_source_id = source_id
            name = ev.get("event")
            ts = _finite_float(ev.get("time_unix"))
            source_is_current = not bool(ev.get("_history"))
            if name == "plan_item_started":
                uid = str(ev.get("item_uid") or ev.get("seq") or len(pending_item_starts))
                pending_item_starts[uid] = ev
                if ts is not None and last_item_finished_time is not None and ts >= last_item_finished_time:
                    gaps.append(ts - last_item_finished_time)
            elif name in {"plan_item_finished", "plan_item_failed"}:
                uid = str(ev.get("item_uid") or ev.get("seq") or len(pending_item_starts))
                start = pending_item_starts.pop(uid, None)
                duration = _finite_float(ev.get("duration_sec"))
                if duration is None and start is not None:
                    start_ts = _finite_float(start.get("time_unix"))
                    if ts is not None and start_ts is not None:
                        duration = ts - start_ts
                if name == "plan_item_finished" and duration is not None and duration >= 0.0:
                    key = _duration_key_from_event(ev)
                    add(item_by_key, key, duration)
                    add(item_by_phase, _phase_key_from_event(ev), duration)
                    if source_is_current:
                        current_item_samples += 1
                if ts is not None:
                    last_item_finished_time = ts
            elif name == "integration_started":
                key = str(ev.get("id") or ev.get("seq") or len(pending_integrations))
                pending_integrations[key] = ev
            elif name in {"integration_finished", "integration_failed"}:
                key_id = str(ev.get("id") or ev.get("seq") or len(pending_integrations))
                start = pending_integrations.pop(key_id, None)
                duration = _finite_float(ev.get("duration_sec"))
                if duration is None and start is not None:
                    start_ts = _finite_float(start.get("time_unix"))
                    if ts is not None and start_ts is not None:
                        duration = ts - start_ts
                if name == "integration_finished" and duration is not None and duration >= 0.0:
                    key = _duration_key_from_event(ev)
                    add(integration_by_key, key, duration)
                    add(integration_by_phase, _phase_key_from_event(ev), duration)
            elif name == "drive_started":
                key = str(ev.get("seq") or len(pending_drives))
                pending_drives[key] = ev
            elif name in {"drive_finished", "drive_failed"}:
                # drive events do not carry a persistent id, so pair in FIFO order.
                start_key = next(iter(pending_drives), None)
                start = pending_drives.pop(start_key, None) if start_key is not None else None
                duration = _finite_float(ev.get("duration_sec"))
                if duration is None and start is not None:
                    start_ts = _finite_float(start.get("time_unix"))
                    if ts is not None and start_ts is not None:
                        duration = ts - start_ts
                if name == "drive_finished" and duration is not None and duration >= 0.0:
                    drive = str(ev.get("drive_kind") or "unknown")
                    stage = str(ev.get("motion_stage") or "unknown")
                    geom = str(ev.get("geometry_kind") or _geometry_kind_from_event(ev))
                    add(drive_by_key, f"drive={drive}|stage={stage}|geometry={geom}", duration)
                    add(drive_by_drive, f"drive={drive}|stage={stage}", duration)

        return {
            "item_by_key": item_by_key,
            "item_by_phase": item_by_phase,
            "integration_by_key": integration_by_key,
            "integration_by_phase": integration_by_phase,
            "drive_by_key": drive_by_key,
            "drive_by_drive": drive_by_drive,
            "gaps": gaps,
            "current_item_samples": current_item_samples,
            "current_open_item": self._latest_open_item([ev for ev in events if not ev.get("_history")]),
        }

    def _latest_open_item(self, events: List[Mapping[str, Any]]) -> Optional[Dict[str, Any]]:
        """Return the currently open plan item event for the current run.

        The snapshot intentionally keeps the last plan item until the next one
        starts, which is useful for display.  ETA estimation, however, must not
        count a just-finished item again during the short gap before the next
        item starts.  This helper therefore tracks actual open plan-item events
        by ``item_uid`` when available, and falls back to stack behavior for
        legacy/artificial events.
        """
        stack: List[Mapping[str, Any]] = []
        keyed: Dict[str, Mapping[str, Any]] = {}
        order: List[str] = []
        for ev in events:
            name = ev.get("event")
            uid = ev.get("item_uid")
            if name == "plan_item_started":
                if uid not in (None, ""):
                    key = str(uid)
                    keyed[key] = ev
                    order.append(key)
                else:
                    stack.append(ev)
            elif name in {"plan_item_finished", "plan_item_failed"}:
                if uid not in (None, ""):
                    key = str(uid)
                    keyed.pop(key, None)
                    order = [item for item in order if item != key]
                elif stack:
                    stack.pop()
        if order:
            last = order[-1]
            item = keyed.get(last)
            return dict(item) if item is not None else None
        if stack:
            return dict(stack[-1])
        return None

    def _estimate_item_duration(self, item: Mapping[str, Any], tables: Mapping[str, Any]) -> tuple[Optional[float], str, str, int]:
        key = _duration_key(item)
        phase_key = _phase_key(item)
        values = tables["item_by_key"].get(key, [])
        value = _median_recent(values)
        if value is not None:
            n = len(values)
            return value, "event_history_item_key", "high" if n >= 5 else "medium", n

        if _allow_phase_duration_fallback(item):
            values = tables["item_by_phase"].get(phase_key, [])
            value = _median_recent(values)
            if value is not None:
                n = len(values)
                return value, "event_history_phase", "medium" if n >= 2 else "low", n

        # Component fallback: integration duration plus observed drive/move and gap overheads.
        component = 0.0
        used = 0
        integration_values = tables["integration_by_key"].get(key, [])
        integration = _median_recent(integration_values)
        if integration is not None:
            component += integration
            used += len(integration_values)
        else:
            integration_values = tables["integration_by_phase"].get(phase_key, []) if _allow_phase_duration_fallback(item) else []
            integration = _median_recent(integration_values)
            if integration is not None:
                component += integration
                used += len(integration_values)
            else:
                planned = _finite_float(item.get("integration_sec"))
                if planned is not None and planned > 0.0:
                    component += planned

        drive = str(item.get("drive_kind") or "unknown")
        geom = _geometry_kind_from(item)
        for stage in ("moving", "tracking", "running", "scanning"):
            vals = tables["drive_by_key"].get(f"drive={drive}|stage={stage}|geometry={geom}", [])
            d = _median_recent(vals)
            if d is None:
                vals = tables["drive_by_drive"].get(f"drive={drive}|stage={stage}", [])
                d = _median_recent(vals)
            if d is not None and d > 0.0:
                # Avoid counting long scan_block/scan drives twice when their drive duration
                # already encloses the integration.  For point observations moving/tracking
                # overheads are genuinely useful.
                if drive in {"scan", "scan_block"} and stage in {"running", "scanning"} and component > 0.0:
                    continue
                component += d
                used += len(vals)
                break

        if component > 0.0:
            return component, "event_history_components", "low", used
        return None, "not_enough_data", "unknown", 0

    def _current_plan_item_for_estimate(self) -> Optional[Dict[str, Any]]:
        plan = self.snapshot.get("plan") or {}
        if not plan.get("mode") and not plan.get("drive_kind"):
            return None
        item = dict(plan)
        if "geometry" not in item:
            item["geometry"] = self.snapshot.get("geometry") or {}
        return item

    def _plan_items_after_current(self) -> List[Dict[str, Any]]:
        plan = self.snapshot.get("plan") or {}
        start, end = _index_start_end(plan)
        items = [dict(item) for item in self._plan_items if isinstance(item, Mapping)]
        if end is None:
            return items
        result = []
        for item in items:
            item_start, _ = _index_start_end(item)
            if item_start is None or item_start > end:
                result.append(item)
        return result

    def _eta_scan_group_key(self, item: Mapping[str, Any]) -> str:
        """Return an execution-context key for scan-like ETA grouping.

        Consecutive ON scan_line waypoints are often merged into one
        ``scan_block`` command, so ETA must not apply one inter-item gap to every
        line.  They should be grouped only when their execution context is the
        same.  Target/reference/offset/frame changes break scan-block merging in
        the actual runner and must therefore also break ETA grouping; otherwise
        the remaining time can be underestimated when two distinct scan blocks
        appear back-to-back.  Start/stop are intentionally ignored because they
        differ for each line within the same block.
        """
        geom = item.get("geometry") if isinstance(item.get("geometry"), Mapping) else {}
        relevant = {
            "mode": str(item.get("mode") or item.get("phase") or "").upper(),
            "drive_kind": str(item.get("drive_kind") or "").lower(),
            "geometry_kind": _geometry_kind_from(item).lower(),
            "frame": geom.get("frame"),
            "unit": geom.get("unit"),
            "target_name": geom.get("target_name"),
            "target": geom.get("target"),
            "reference": geom.get("reference"),
            "offset": geom.get("offset"),
        }
        return json.dumps(json_safe(relevant), ensure_ascii=False, sort_keys=True)

    def _eta_gap_unit_count(self, future_items: List[Dict[str, Any]]) -> int:
        """Estimate how many inter-item gaps remain at execution granularity.

        ``observation_plan.json`` intentionally keeps the original waypoint-level
        plan.  With ``merge_scan_blocks=True``, however, many ON scan_line
        waypoints are executed as one scan_block command.  Applying one learned
        inter-item gap to every remaining line would therefore overestimate ETA.
        Consecutive scan-like ON items are counted as one execution group only
        when their target/reference/offset/frame context also matches.
        """
        units = 0
        previous_group: Optional[str] = None
        for item in future_items:
            mode = str(item.get("mode") or item.get("phase") or "").upper()
            drive = str(item.get("drive_kind") or "").lower()
            geom = _geometry_kind_from(item).lower()
            if mode == "ON" and (drive in {"scan", "scan_block"} or geom in {"scan_line", "scan_block", "scan_block_line"}):
                group = "ON_SCAN_SEQUENCE:" + self._eta_scan_group_key(item)
            else:
                # Include item_uid so non-scan items do not collapse together.
                group = f"ITEM:{item.get('item_uid') or item.get('index0') or units}"
            if group != previous_group:
                units += 1
                previous_group = group
        return units

    def _eta_progress_fraction_remaining(self, *, current_item_open: bool) -> Optional[float]:
        """Low-confidence sanity estimate from elapsed time and plan fraction.

        Event-history ETA can be too optimistic during OTF maps when the first
        few scan blocks happen to be short or when per-item histories do not yet
        represent the full HOT/OFF/ON cadence.  This estimate is deliberately
        used only while a plan item is actually open; in an inter-item gap the
        snapshot may still point to the just-finished item and would otherwise
        overestimate badly.
        """
        if not current_item_open:
            return None
        plan = self.snapshot.get("plan") if isinstance(self.snapshot.get("plan"), Mapping) else {}
        geometry = self.snapshot.get("geometry") if isinstance(self.snapshot.get("geometry"), Mapping) else {}
        timing = self.snapshot.get("time") if isinstance(self.snapshot.get("time"), Mapping) else {}
        total = plan.get("total")
        if not isinstance(total, int) or total <= 1:
            return None
        start, end = _index_start_end(plan)
        if start is None or end is None:
            return None
        width = max(1, end - start + 1)
        frac = 0.5
        line_total = geometry.get("line_total") or plan.get("line_total")
        current_line = geometry.get("current_line_index0")
        if isinstance(line_total, int) and line_total > 0:
            try:
                line = max(0, min(int(current_line), line_total - 1))
                frac = (line + 0.5) / float(line_total)
            except Exception:
                frac = 0.5
        completed = start + width * max(0.0, min(1.0, frac))
        if not (0.0 < completed < float(total)):
            return None
        progress_fraction = completed / float(total)
        if progress_fraction < 0.10:
            return None
        elapsed = _finite_float(timing.get("elapsed_sec"))
        if elapsed is None:
            elapsed = max(0.0, time.time() - self.started_at_unix)
        if elapsed <= 0.0:
            return None
        return max(0.0, elapsed * (1.0 - progress_fraction) / progress_fraction)

    def _refresh_remaining_estimate(self, *, now: Optional[float] = None) -> None:
        """Update ETA fields in the snapshot using observed event history.

        The ETA intentionally remains unknown at the beginning of an observation.
        Once the current run has enough completed item/integration samples, the
        estimator uses current-run durations plus bounded previous-run history.
        The priority is: item median for the same phase/drive/geometry, phase
        median, then a low-confidence component estimate from integration,
        drive/move, and inter-item gap durations.
        """
        if not self.enabled:
            return
        try:
            now = float(now or time.time())
            timing = self.snapshot.setdefault("time", {})
            lifecycle = (self.snapshot.get("lifecycle") or {}).get("state")
            elapsed = max(0.0, now - self.started_at_unix)
            timing["elapsed_sec"] = elapsed
            if lifecycle in FINAL_LIFECYCLE_STATES:
                timing.update(
                    {
                        "estimated_remaining_sec": 0.0,
                        "estimated_total_sec": elapsed,
                        "estimated_completion_unix": now,
                        "remaining_method": "final_state",
                        "remaining_confidence": "high",
                        "remaining_sample_count": 0,
                    }
                )
                return

            current_events = [dict(ev) for ev in self._events_all]
            current_tables = self._duration_tables(current_events)
            current_samples = int(current_tables.get("current_item_samples") or 0)
            if current_samples < self._remaining_min_current_samples:
                timing.update(
                    {
                        "estimated_remaining_sec": None,
                        "estimated_total_sec": None,
                        "estimated_completion_unix": None,
                        "remaining_method": "not_enough_current_data",
                        "remaining_confidence": "unknown",
                        "remaining_sample_count": current_samples,
                    }
                )
                return

            history_events = self._load_history_events()
            tables = self._duration_tables(current_events + history_events)
            current_item = self._current_plan_item_for_estimate()
            future_items = self._plan_items_after_current()
            remaining = 0.0
            sample_count = 0
            methods: List[str] = []
            confidences: List[str] = []

            open_item = tables.get("current_open_item")
            open_start = _finite_float(open_item.get("time_unix")) if isinstance(open_item, Mapping) else None
            if current_item is not None and open_start is not None:
                estimated, method, confidence, n = self._estimate_item_duration(current_item, tables)
                if estimated is not None:
                    partial = max(0.0, now - open_start)
                    remaining += max(0.0, estimated - partial)
                    sample_count += n
                    methods.append(method)
                    confidences.append(confidence)

            for item in future_items:
                estimated, method, confidence, n = self._estimate_item_duration(item, tables)
                if estimated is not None:
                    remaining += estimated
                    sample_count += n
                    methods.append(method)
                    confidences.append(confidence)
                else:
                    planned = _finite_float(item.get("integration_sec"))
                    if planned is not None and planned > 0.0:
                        remaining += planned
                        methods.append("planned_integration_fallback")
                        confidences.append("low")

            gap = _median_recent(list(tables.get("gaps") or []))
            if gap is not None and future_items:
                # ``gaps`` are measured between a finished item and the next
                # started item.  Count them at execution granularity rather than
                # raw plan-line granularity so merged scan blocks do not get one
                # gap per scan line.
                gap_units = self._eta_gap_unit_count(future_items)
                gap_remaining = 0.0
                if open_start is not None:
                    gap_remaining = gap * gap_units
                else:
                    last_finish = None
                    for ev in reversed(self._events_all):
                        if ev.get("event") in {"plan_item_finished", "plan_item_failed"}:
                            last_finish = _finite_float(ev.get("time_unix"))
                            break
                    if last_finish is not None:
                        elapsed_gap = max(0.0, now - last_finish)
                        gap_remaining += max(0.0, gap - elapsed_gap)
                        if gap_units > 1:
                            gap_remaining += gap * (gap_units - 1)
                    else:
                        gap_remaining = gap * gap_units
                if gap_remaining > 0.0:
                    remaining += gap_remaining
                    methods.append("event_history_inter_item_gap")
                    confidences.append("low")

            fraction_remaining = self._eta_progress_fraction_remaining(
                current_item_open=open_start is not None
            )
            if fraction_remaining is not None and remaining < 0.75 * fraction_remaining:
                remaining = fraction_remaining
                methods.append("elapsed_plan_fraction_sanity")
                confidences.append("low")

            if remaining <= 0.0 and future_items:
                timing.update(
                    {
                        "estimated_remaining_sec": None,
                        "estimated_total_sec": None,
                        "estimated_completion_unix": None,
                        "remaining_method": "not_enough_duration_data",
                        "remaining_confidence": "unknown",
                        "remaining_sample_count": sample_count,
                    }
                )
                return

            if not methods and current_item is None and not future_items:
                remaining = 0.0
                methods.append("no_remaining_plan_items")
                confidences.append("high")

            if any(c == "low" for c in confidences):
                confidence = "low"
            elif all(c == "high" for c in confidences) and confidences:
                confidence = "high"
            elif confidences:
                confidence = "medium"
            else:
                confidence = "unknown"
            method = "+".join(sorted(set(methods))) if methods else "not_enough_data"
            timing.update(
                {
                    "estimated_remaining_sec": max(0.0, remaining),
                    "estimated_total_sec": elapsed + max(0.0, remaining),
                    "estimated_completion_unix": now + max(0.0, remaining),
                    "remaining_method": method,
                    "remaining_confidence": confidence,
                    "remaining_sample_count": sample_count,
                }
            )
        except Exception as exc:
            self._log_debug(f"Observation progress ETA update failed: {exc}")

    @contextmanager
    def item(self, **fields: Any) -> Iterator[None]:
        plan = json_safe(fields)
        mode = plan.get("mode")
        drive_kind = plan.get("drive_kind")
        # Reset per-item activity at plan-item boundaries.  Without this, a
        # newly started OFF/SKY/Grid item can inherit the previous item's phase
        # until its first integration starts, which is misleading in --watch and
        # any future GUI.
        activity_reset = {
            "phase": mode,
            "drive_kind": drive_kind,
            "motion_stage": None,
            "data_state": "not_integrating",
            "location_context": None,
        }
        data_reset = {
            "expected_metadata_position": "",
            "expected_metadata_id": "",
            "expected_metadata_line_index": None,
        }
        # Replace plan/geometry atomically at item boundaries.  A scan_block item
        # has fields such as index0_end and geometry.lines that must not survive
        # into the next point or HOT item.
        started_at = time.time()
        self.update(
            _replace_sections=("plan", "geometry"),
            plan=plan,
            geometry=plan.get("geometry") or {},
            activity=activity_reset,
            data=data_reset,
        )
        self.event("plan_item_started", **plan)
        try:
            yield
        except BaseException as exc:
            self.event("plan_item_failed", error=repr(exc), duration_sec=max(0.0, time.time() - started_at), **plan)
            raise
        else:
            self.event("plan_item_finished", duration_sec=max(0.0, time.time() - started_at), **plan)

    @contextmanager
    def drive(self, *, kind: str, stage: str, geometry: Optional[Mapping[str, Any]] = None, **fields: Any) -> Iterator[None]:
        started_at = time.time()
        geometry_kind = _geometry_kind_from({"geometry": geometry or {}}) if geometry is not None else str(fields.get("geometry_kind") or "unknown")
        activity = {"drive_kind": kind, "motion_stage": stage, **json_safe(fields)}
        sections: Dict[str, Any] = {"activity": activity}
        replace_sections = ()
        if geometry is not None:
            sections["geometry"] = geometry
            replace_sections = ("geometry",)
        self.update(_replace_sections=replace_sections, **sections)
        self.event("drive_started", drive_kind=kind, motion_stage=stage, geometry_kind=geometry_kind, **fields)
        try:
            yield
        except BaseException as exc:
            self.event("drive_failed", drive_kind=kind, motion_stage=stage, geometry_kind=geometry_kind, duration_sec=max(0.0, time.time() - started_at), error=repr(exc), **fields)
            raise
        else:
            self.event("drive_finished", drive_kind=kind, motion_stage=stage, geometry_kind=geometry_kind, duration_sec=max(0.0, time.time() - started_at), **fields)

    @contextmanager
    def integration(
        self,
        *,
        phase: str,
        metadata_position: str,
        id: Any,
        line_index: Optional[int] = None,
        location_context: Optional[str] = None,
        geometry: Optional[Mapping[str, Any]] = None,
        **fields: Any,
    ) -> Iterator[None]:
        started_at = time.time()
        geometry_kind = _geometry_kind_from({"geometry": geometry or (self.snapshot.get("geometry") or {})})
        data = {
            "expected_metadata_position": metadata_position,
            "expected_metadata_id": str(id),
        }
        if line_index is not None:
            data["expected_metadata_line_index"] = int(line_index)
        activity = {
            "phase": phase,
            "data_state": "integrating",
            "location_context": location_context,
            **json_safe(fields),
        }
        sections: Dict[str, Any] = {"activity": activity, "data": data}
        replace_sections = ()
        if geometry is not None:
            sections["geometry"] = geometry
            replace_sections = ("geometry",)
        self.update(_replace_sections=replace_sections, **sections)
        self.event(
            "integration_started",
            phase=phase,
            metadata_position=metadata_position,
            id=str(id),
            line_index=line_index,
            location_context=location_context,
            geometry_kind=geometry_kind,
            **fields,
        )
        try:
            yield
        except BaseException as exc:
            self.event(
                "integration_failed",
                phase=phase,
                metadata_position=metadata_position,
                id=str(id),
                line_index=line_index,
                location_context=location_context,
                geometry_kind=geometry_kind,
                error=repr(exc),
                duration_sec=max(0.0, time.time() - started_at),
                **fields,
            )
            raise
        else:
            self.event(
                "integration_finished",
                phase=phase,
                metadata_position=metadata_position,
                id=str(id),
                line_index=line_index,
                location_context=location_context,
                geometry_kind=geometry_kind,
                duration_sec=max(0.0, time.time() - started_at),
                **fields,
            )
        finally:
            # Clear expected metadata fields that may otherwise be stale across
            # transitions, especially after scan_block integrations.
            self.update(
                activity={"data_state": "not_integrating"},
                data={
                    "expected_metadata_position": "",
                    "expected_metadata_id": "",
                    "expected_metadata_line_index": None,
                },
            )

    def record_sidecars(self) -> None:
        """Best-effort save of progress sidecars through the NECST recorder.

        The recorder service can only accept files while the recorder is active,
        and the standard FileWriter prepends a comment header to saved files.
        This method is still useful during cleanup while recording is active, but
        :meth:`copy_sidecars_to_local_record_dir` is used after the final
        lifecycle update to leave parseable JSON/JSONL sidecars in the local
        record directory when that directory is visible from this process.
        """
        if not self.enabled:
            return
        commander = self._commander
        if commander is None:
            return
        for path in (self.plan_path, self.events_path, self.snapshot_path):
            try:
                if path.exists() and path.stat().st_size > 0:
                    commander.record("file", name=str(path))
            except Exception as exc:
                self._log_debug(f"Observation progress sidecar record failed for {path}: {exc}")

    def copy_sidecars_to_local_record_dir(
        self,
        *,
        record_name: Optional[str] = None,
        record_root: Optional[Any] = None,
    ) -> None:
        """Best-effort direct copy of final sidecars into a local record dir.

        ``RecorderController.write_file`` rejects files after ``record('stop')``
        and FileWriter adds comment headers to files saved through the service.
        Therefore the final parseable snapshot is copied directly when the NECST
        record directory is locally visible.  This is deliberately best-effort:
        remote recorder deployments or different ``NECST_RECORD_ROOT`` values
        simply fall back to the live progress directory.
        """
        if not self.enabled:
            return
        try:
            root = Path(record_root or os.environ.get("NECST_RECORD_ROOT", Path.home() / "data")).expanduser()
            name = str(record_name or self.record_name).lstrip("/")
            record_dir = root / name
            if not record_dir.is_dir():
                self._log_debug(f"Observation progress local record dir not found: {record_dir}")
                return
            for path in (self.plan_path, self.events_path, self.snapshot_path):
                if path.exists() and path.stat().st_size > 0:
                    shutil.copy2(path, record_dir / path.name)
        except Exception as exc:
            self._log_debug(f"Observation progress local sidecar copy failed: {exc}")

    def close(self, state: str = "finished", **fields: Any) -> None:
        self.set_lifecycle(state, **fields)
        self.record_sidecars()
        self.copy_sidecars_to_local_record_dir(record_name=self.record_name)
