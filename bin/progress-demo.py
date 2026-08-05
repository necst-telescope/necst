#!/usr/bin/env python3
"""Serve the NECST progress web UI with synthetic observation data.

This tool is intentionally independent of the real observation program and ROS.
It writes a small, changing progress-file tree and then launches ``progress.py
--serve --no-ros`` against that tree.  It is meant for checking the web layout,
Plan View behavior, and v44 compact/free-grid layout, draggable ordering, interactive card sizing, and status-card rendering before an on-sky test.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import re
import shutil
import signal
import subprocess
import sys
import threading
import time
import webbrowser
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


def safe_name(value: Any) -> str:
    text = str(value or "unknown")
    sanitized = re.sub(r"[^A-Za-z0-9_.-]+", "_", text)
    sanitized = re.sub(r"_+", "_", sanitized).strip("_") or "record"
    return sanitized


def atomic_write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.replace(tmp, path)


def atomic_write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    os.replace(tmp, path)


def append_jsonl(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(payload, ensure_ascii=False, sort_keys=True) + "\n")


def otf_lines(total: int) -> List[Dict[str, Any]]:
    if total <= 1:
        ys = [0.0]
    else:
        ys = [-0.50 + i * (0.967 / (total - 1)) for i in range(total)]
    lines: List[Dict[str, Any]] = []
    for i, y in enumerate(ys):
        # Alternate scan directions so the arrow direction looks realistic.
        if i % 2 == 0:
            start, stop = [-0.50, y], [0.50, y]
        else:
            start, stop = [0.50, y], [-0.50, y]
        lines.append(
            {
                "kind": "scan_block_line",
                "line_index0": i,
                "label": f"scan_block {i + 1}",
                "start": start,
                "stop": stop,
                "speed_deg_per_sec": 0.10,
            }
        )
    return lines


def make_plan(total: int, target: str) -> Dict[str, Any]:
    return {
        "schema_version": "necst-progress-demo-v1",
        "item_count": 1,
        "items": [
            {
                "item_uid": "demo:otf:scan_block:0",
                "index0": 0,
                "index0_end": total - 1,
                "total": total,
                "mode": "ON",
                "role": "science",
                "drive_kind": "scan_block",
                "line_total": total,
                "geometry": {
                    "kind": "scan_block",
                    "frame": "J2000",
                    "unit": "deg",
                    "target_name": target,
                    "target": [83.6331, -5.3911],
                    "reference": [83.5600, -5.3000],
                    "offset": [0.0, 0.0],
                    "cos_correction": True,
                    "line_total": total,
                    "lines": otf_lines(total),
                },
            }
        ],
    }


def section_at(elapsed: float, total: int) -> Tuple[int, str, float, float, float]:
    """Return line_index, section_kind, section_fraction, section_start, section_stop."""
    # The numbers are deliberately chosen so the demo visibly shows standby,
    # accelerate, line, decelerate, and turn.  The real telescope timing is not
    # simulated here; this is a UI preview fixture.
    sequence = [
        ("initial_standby", 1.5),
        ("accelerate", 1.6),
        ("line", 8.0),
        ("decelerate", 1.4),
        ("turn", 1.6),
    ]
    cycle = sum(d for _, d in sequence)
    cycle_pos = elapsed % (cycle * total)
    line = int(cycle_pos // cycle) % total
    pos = cycle_pos - line * cycle
    start = elapsed - pos
    for kind, duration in sequence:
        if pos < duration:
            frac = 0.0 if duration <= 0 else max(0.0, min(1.0, pos / duration))
            return line, kind, frac, start, start + duration
        pos -= duration
        start += duration
    return line, "turn", 1.0, elapsed, elapsed


def section_geometry(plan: Dict[str, Any], line_index: int) -> Dict[str, Any]:
    lines = plan["items"][0]["geometry"]["lines"]
    return lines[max(0, min(line_index, len(lines) - 1))]


def make_snapshot(
    root: Path,
    record_name: str,
    plan: Dict[str, Any],
    started_at: float,
    now: float,
    *,
    total: int,
    stale_demo: bool,
) -> Dict[str, Any]:
    elapsed = now - started_at
    line, kind, frac, section_start, section_stop = section_at(elapsed, total)
    geom = section_geometry(plan, line)
    is_line = kind == "line"
    section_publish_time = now - (3.2 if stale_demo else 0.10)
    pointing_publish_time = now - (2.8 if stale_demo else 0.12)
    queue_publish_time = now - (2.5 if stale_demo else 0.09)
    spec_publish_time = now - (2.2 if stale_demo else 0.20)

    direction = 1.0 if geom["stop"][0] >= geom["start"][0] else -1.0
    along = (
        frac if is_line else (0.0 if kind in {"initial_standby", "accelerate"} else 1.0)
    )
    cmd_az = 146.0 + 0.8 * line / max(1, total - 1) + direction * 0.03 * along
    cmd_el = 53.0 + 0.2 * math.sin(0.2 * line) + 0.015 * along
    # Tracking is intentionally sometimes marginal so the warning styling can be checked.
    track_error_arcsec = 4.0 + 14.0 * max(0.0, math.sin(elapsed / 7.0))
    track_error_deg = track_error_arcsec / 3600.0
    enc_az = cmd_az + track_error_deg / max(0.2, math.cos(math.radians(cmd_el)))
    enc_el = cmd_el + 0.0002 * math.sin(elapsed)
    tracking_ok = track_error_arcsec < 12.0

    line_completed = line if kind != "turn" else min(total, line + 1)
    remaining = max(0, total - line_completed - (1 if is_line else 0))
    duration_per_line = 14.1
    eta = max(0.0, total * duration_per_line - elapsed)
    record_dir = root / safe_name(record_name)
    # Demo-only: alternate every 30 s so the UTC sync/nosync styling can be checked.
    time_sync_ok = int(elapsed // 30) % 2 == 0

    return {
        "schema_version": "necst-progress-demo-v1",
        "observation": {
            "type": "OTF",
            "record_name": record_name,
            "obs_file": "demo_otf_scan_block.obs",
            "target_name": "Orion-KL",
        },
        "lifecycle": {
            "state": "running",
            "started_at_unix": started_at,
        },
        "plan": {
            "item_uid": "demo:otf:scan_block:0",
            "index0": line,
            "index0_end": line,
            "total": total,
            "mode": "ON",
            "role": "science",
            "drive_kind": "scan_block",
            "completed": line_completed,
            "remaining": remaining,
        },
        "activity": {
            "phase": "ON",
            "drive_kind": "scan_block",
            "motion_stage": kind,
            "data_state": (
                "integrating"
                if kind in {"initial_standby", "accelerate", "line", "decelerate"}
                else "not_integrating"
            ),
            "control_id": "demo-section-control",
            "control_line": "1/1",
            "expected_metadata_position": "ON",
            "expected_metadata_id": str(line),
            "control_section_active": True,
            "control_status_basis": "current-time",
            "control_section_kind": kind,
            "control_section_label": f"{kind}:{line + 1}",
            "control_section_uid": f"demo:{line:04d}:{kind}",
            "control_section_plan_index": line * 5
            + ["initial_standby", "accelerate", "line", "decelerate", "turn"].index(
                kind
            ),
            "control_section_sequence_index": line * 5
            + ["initial_standby", "accelerate", "line", "decelerate", "turn"].index(
                kind
            ),
            "control_section_line_index": line,
            "control_section_science_line": is_line,
            "control_section_interrupt_ok": kind in {"initial_standby", "turn"},
            "control_section_start_unix": section_start,
            "control_section_stop_unix": section_stop,
            "control_section_duration_sec": max(0.001, section_stop - section_start),
            "control_section_command_time_unix": now,
            "control_section_query_time_unix": now,
            "control_section_publish_time_unix": section_publish_time,
            "control_section_age_sec": max(0.0, now - section_publish_time),
            "control_section_fresh": (now - section_publish_time) <= 2.0,
            "control_section_fraction": frac,
        },
        "geometry": {
            "kind": "scan_block",
            "frame": "J2000",
            "unit": "deg",
            "target_name": "Orion-KL",
            "target": [83.6331, -5.3911],
            "reference": [83.5600, -5.3000],
            "center": [83.6331, -5.3911],
            "line_total": total,
            "current_line_index0": line,
            "progress_line_index0": line,
            "current_line_label": f"scan_block {line + 1}",
            "cos_correction": True,
            "live_section_geometry": {
                "frame": "J2000",
                "unit": "deg",
                "start": geom["start"],
                "stop": geom["stop"],
                "speed_deg_per_sec": geom["speed_deg_per_sec"],
            },
        },
        "antenna": {
            "command_lon_deg": cmd_az,
            "command_lat_deg": cmd_el,
            "command_frame": "altaz",
            "command_unit": "deg",
            "command_time_unix": now - 0.05,
            "encoder_lon_deg": enc_az,
            "encoder_lat_deg": enc_el,
            "encoder_frame": "altaz",
            "encoder_unit": "deg",
            "encoder_time_unix": now - 0.04,
            "tracking_status_available": True,
            "tracking_status_valid": True,
            "tracking_ok": tracking_ok,
            "tracking_error_deg": track_error_deg,
            "tracking_status_time_unix": pointing_publish_time,
            "tracking_status_age_sec": max(0.0, now - pointing_publish_time),
            "tracking_threshold_deg": 12.0 / 3600.0,
            "command_stale": stale_demo,
            "encoder_stale": stale_demo,
            "command_age_sec": max(0.0, now - pointing_publish_time),
            "encoder_age_sec": max(0.0, now - pointing_publish_time),
            "delta_az_deg": enc_az - cmd_az,
            "delta_el_deg": enc_el - cmd_el,
            "delta_az_cos_el_deg": (enc_az - cmd_az) * math.cos(math.radians(cmd_el)),
            "pointing_basis": "demo-time-matched-command-encoder",
        },
        "antenna_command_queue": {
            "active": True,
            "control_id": "demo-section-control",
            "mode": "scan_block",
            "publish_time_unix": queue_publish_time,
            "command_offset_sec": 1.0,
            "min_buffer_sec": 1.0,
            "max_buffer_sec": 3.0,
            "queue_length": 36 + int(10 * (0.5 + 0.5 * math.sin(elapsed / 5.0))),
            "head_lead_sec": 0.8 + 0.3 * math.sin(elapsed / 6.0),
            "tail_lead_sec": 2.0 + 0.4 * math.cos(elapsed / 6.0),
            "last_publish_wall_time_unix": now - 0.05,
            "last_published_cmd_time_unix": now + 2.0,
            "publish_gap_sec": 0.0,
            "generator_exhausted": False,
            "guard_latched": False,
            "reason": "demo",
        },
        "spectrometer": {
            "valid": True,
            "recorder_active": True,
            "saving_enabled": True,
            "acquiring": True,
            "publish_time_unix": spec_publish_time,
            "last_dump_time_unix": now - 0.18,
            "last_record_time_unix": now - 0.20,
            "last_stream_time_unix": now - 0.18,
            "last_dump_age_sec": 0.18,
            "latest_time_spectrometer": time.strftime(
                "%Y-%m-%dT%H:%M:%S", time.gmtime(now - 0.18)
            )
            + "GPS",
            "record_every_n": 1,
            "data_queue_size_max": 4096,
            "n_streams": 1,
            "n_boards": 4,
            "missing_board_count": 0,
            "tp_mode": True,
            "tp_range_start": -1,
            "tp_range_stop": -1,
            "qlook_ch_start": 0,
            "qlook_ch_stop": 100,
            "metadata_position": "ON" if is_line else "PRE",
            "metadata_id": str(line),
            "section_kind": kind,
            "section_label": f"{kind}:{line + 1}",
            "line_index": line,
            "recorder_path": str(record_dir),
            "warning": "" if not stale_demo else "demo stale mode",
        },
        "chopper": {
            "insert": bool(is_line),
            "state": "in" if is_line else "out",
            "position": 4750 if is_line else 0,
            "time_unix": now - 0.10,
        },
        "time_sync": {
            "enabled": True,
            "status": "ok" if time_sync_ok else "bad",
            "label": "sync" if time_sync_ok else "nosync",
            "summary": (
                "time sync: demo sync, 3/3 OK, max offset 0.4 ms"
                if time_sync_ok
                else "time sync: demo nosync, 2/3 OK, worst xffts: timeout"
            ),
            "checked_at_unix": now - 1.0,
            "ok_count": 3 if time_sync_ok else 2,
            "host_count": 3,
            "max_offset_ms": 0.4 if time_sync_ok else None,
            "hosts": [
                {
                    "name": "localhost",
                    "host": "localhost",
                    "method": "chrony",
                    "status": "ok",
                    "synced": True,
                    "offset_ms": 0.1,
                },
                {
                    "name": "xffts",
                    "host": "xffts",
                    "method": "ntpq",
                    "status": "ok" if time_sync_ok else "bad",
                    "synced": time_sync_ok,
                    "offset_ms": 0.4 if time_sync_ok else None,
                    "reason": "synchronised" if time_sync_ok else "timeout",
                },
                {
                    "name": "record",
                    "host": "record",
                    "method": "chrony",
                    "status": "ok",
                    "synced": True,
                    "offset_ms": -0.2,
                },
            ],
        },
        "time": {
            "updated_at_unix": now,
            "elapsed_sec": elapsed,
            "estimated_remaining_sec": eta,
            "remaining_method": "demo section status",
            "remaining_confidence": "medium",
            "remaining_sample_count": int(elapsed * 8),
        },
        "weather": {
            # Keep the flat keys for backward compatibility with older progress.py,
            # and provide the nested out/in form used by the compact v41/v42/v43/v44 cards.
            "temperature_c": 16.4,
            "humidity_percent": 48.0,
            "pressure_hpa": 859.8,
            "wind_speed_mps": 0.0,
            "wind_direction_deg": 0.0,
            "in_temperature_c": 20.0,
            "out": {
                "temperature_c": 16.4,
                "humidity_percent": 48.0,
                "pressure_hpa": 859.8,
                "wind_speed_mps": 0.0,
                "wind_direction_deg": 0.0,
            },
            "in": {
                "temperature_c": 20.0,
            },
        },
        "paths": {
            "snapshot": str(root / "current_observation_progress.json"),
            "events": str(record_dir / "observation_events.jsonl"),
            "plan": str(record_dir / "observation_plan.json"),
        },
    }


class DemoWriter:
    def __init__(
        self, root: Path, *, total: int, update_interval: float, stale_demo: bool
    ) -> None:
        self.root = root
        self.total = total
        self.update_interval = update_interval
        self.stale_demo = stale_demo
        self.record_name = "demo_v44_web_preview_orion_kl"
        self.started_at = time.time()
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._last_section: Optional[Tuple[int, str]] = None
        self._seq = 0
        self.plan = make_plan(total, "Orion-KL")
        self.record_dir = root / safe_name(self.record_name)

    def prepare(self, *, clean: bool) -> None:
        if clean and self.root.exists():
            shutil.rmtree(self.root)
        self.record_dir.mkdir(parents=True, exist_ok=True)
        atomic_write_text(
            self.root / "current_observation_record.txt", self.record_name + "\n"
        )
        atomic_write_json(self.record_dir / "observation_plan.json", self.plan)
        events = self.record_dir / "observation_events.jsonl"
        events.write_text("", encoding="utf-8")
        self._event("observation_started", mode="OTF")
        self.write_once()

    def _event(self, event: str, **extra: Any) -> None:
        self._seq += 1
        payload: Dict[str, Any] = {
            "seq": self._seq,
            "event": event,
            "time_unix": time.time(),
        }
        payload.update(extra)
        append_jsonl(self.record_dir / "observation_events.jsonl", payload)

    def write_once(self) -> None:
        now = time.time()
        line, kind, frac, _start, _stop = section_at(now - self.started_at, self.total)
        key = (line, kind)
        if key != self._last_section:
            if self._last_section is not None:
                prev_line, prev_kind = self._last_section
                self._event(
                    "drive_finished",
                    drive_kind="scan_block",
                    motion_stage=prev_kind,
                    line_index=prev_line,
                )
                if prev_kind == "line":
                    self._event(
                        "plan_item_finished",
                        item_uid="demo:otf:scan_block:0",
                        index0=prev_line,
                        index0_end=prev_line,
                        line_index=prev_line,
                    )
            self._event(
                "drive_started",
                drive_kind="scan_block",
                motion_stage=kind,
                line_index=line,
            )
            if kind == "line":
                self._event(
                    "line_started",
                    item_uid="demo:otf:scan_block:0",
                    index0=line,
                    line_index=line,
                    phase="ON",
                )
            self._last_section = key
        snapshot = make_snapshot(
            self.root,
            self.record_name,
            self.plan,
            self.started_at,
            now,
            total=self.total,
            stale_demo=self.stale_demo,
        )
        atomic_write_json(self.root / "current_observation_progress.json", snapshot)
        atomic_write_json(self.record_dir / "observation_progress.json", snapshot)

    def start(self) -> None:
        def run() -> None:
            while not self._stop.is_set():
                self.write_once()
                self._stop.wait(self.update_interval)

        self._thread = threading.Thread(
            target=run, name="necst-progress-demo-writer", daemon=True
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=2.0)


def find_progress_script(value: Optional[str]) -> Path:
    if value:
        return Path(value).expanduser().resolve()
    here = Path(__file__).resolve()
    sibling = here.with_name("progress.py")
    if sibling.exists():
        return sibling
    raise FileNotFoundError(
        "progress.py was not found next to progress-demo.py. Use --progress-script."
    )


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Preview the NECST progress web dashboard using synthetic v44 data"
    )
    parser.add_argument(
        "--root",
        default="/tmp/necst_progress_demo_v44",
        help="demo progress root directory",
    )
    parser.add_argument("--host", default="127.0.0.1", help="web bind address")
    parser.add_argument("--port", type=int, default=8080, help="web bind port")
    parser.add_argument(
        "--refresh-ms", type=int, default=500, help="browser refresh interval [ms]"
    )
    parser.add_argument(
        "--update-interval",
        type=float,
        default=0.25,
        help="synthetic file update interval [s]",
    )
    parser.add_argument(
        "--lines", type=int, default=30, help="number of synthetic OTF scan lines"
    )
    parser.add_argument(
        "--progress-script",
        default=None,
        help="path to progress.py; default is sibling progress.py",
    )
    parser.add_argument(
        "--write-only",
        action="store_true",
        help="only write demo files; do not launch the web server",
    )
    parser.add_argument(
        "--no-clean",
        action="store_true",
        help="do not remove the demo root before starting",
    )
    parser.add_argument(
        "--open",
        action="store_true",
        help="open the dashboard URL in the default browser",
    )
    parser.add_argument(
        "--stale-demo",
        action="store_true",
        help="intentionally write stale ages to check warning styling",
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="suppress HTTP request logs from progress.py",
    )
    args = parser.parse_args(argv)

    root = Path(args.root).expanduser().resolve()
    writer = DemoWriter(
        root,
        total=max(1, args.lines),
        update_interval=max(0.05, args.update_interval),
        stale_demo=bool(args.stale_demo),
    )
    writer.prepare(clean=not args.no_clean)
    writer.start()

    print(f"Demo progress root: {root}")
    print("Synthetic data are being updated until this process exits.")

    if args.write_only:
        print("Run the dashboard separately, for example:")
        print(
            f"  {sys.executable} /path/to/progress.py --serve --no-ros --root {root} --refresh-ms {args.refresh_ms}"
        )
        try:
            while True:
                time.sleep(1.0)
        except KeyboardInterrupt:
            writer.stop()
            return 0

    progress_script = find_progress_script(args.progress_script)
    cmd = [
        sys.executable,
        str(progress_script),
        "--serve",
        "--no-ros",
        "--root",
        str(root),
        "--host",
        str(args.host),
        "--port",
        str(args.port),
        "--refresh-ms",
        str(args.refresh_ms),
    ]
    if args.quiet:
        cmd.append("--quiet")
    print("Launching dashboard:")
    print("  " + " ".join(cmd))
    proc = subprocess.Popen(cmd)
    url = f"http://{args.host}:{args.port}/"
    if args.open:
        try:
            webbrowser.open(url)
        except Exception:
            pass
    print(f"Preview URL: {url}")
    print("Press Ctrl-C to stop the demo server and synthetic writer.")
    try:
        while proc.poll() is None:
            time.sleep(0.5)
        return int(proc.returncode or 0)
    except KeyboardInterrupt:
        try:
            proc.send_signal(signal.SIGINT)
            proc.wait(timeout=5.0)
        except Exception:
            proc.terminate()
        return 0
    finally:
        writer.stop()


if __name__ == "__main__":
    raise SystemExit(main())
