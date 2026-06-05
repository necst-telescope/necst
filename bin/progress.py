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
import shutil
import subprocess
import sys
import time
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence
from urllib.parse import parse_qs, urlparse

try:
    from necst.web import status_model
except ModuleNotFoundError:
    # Allow direct source-tree execution as ``python3 bin/progress.py`` in an
    # environment where ROS/neclib is not importable yet.  Importing ``necst``
    # normally executes necst/__init__.py, so load this stdlib-only helper by
    # file path as a fallback.
    import importlib.util

    _status_model_path = (
        Path(__file__).resolve().parents[1] / "necst" / "web" / "status_model.py"
    )
    _status_model_spec = importlib.util.spec_from_file_location(
        "necst_progress_status_model", _status_model_path
    )
    if _status_model_spec is None or _status_model_spec.loader is None:
        raise
    status_model = importlib.util.module_from_spec(_status_model_spec)
    _status_model_spec.loader.exec_module(status_model)

try:
    import tomllib  # Python 3.11+
except ModuleNotFoundError:  # pragma: no cover - old Python fallback
    tomllib = None  # type: ignore[assignment]


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
    """Maximum age for live ROS status before it is treated as stale.

    This is intentionally short because Plan View uses live status to decide
    which scan-line section is current.  A stale section status is worse than
    no live status; it makes the dashboard jump to a delayed line fraction.
    """
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


def read_json(path: Path) -> Optional[Dict[str, Any]]:
    try:
        return json.loads(strip_record_file_header(path.read_text(encoding="utf-8")))
    except FileNotFoundError:
        return None
    except json.JSONDecodeError as exc:
        return {"_error": f"invalid JSON in {path}: {exc}"}
    except Exception as exc:
        return {"_error": f"failed to read {path}: {exc}"}


def read_jsonl(
    path: Optional[Path], limit: Optional[int] = None
) -> List[Dict[str, Any]]:
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


def latest_events_path(
    root: Path, snapshot: Optional[Dict[str, Any]] = None
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


def _finite_int(value: Any) -> Optional[int]:
    try:
        out = int(value)
    except Exception:
        return None
    return out


def _bool_config(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    if isinstance(value, (int, float)):
        return bool(value)
    token = str(value).strip().lower()
    if token in {"1", "true", "yes", "on", "enable", "enabled"}:
        return True
    if token in {"0", "false", "no", "off", "disable", "disabled"}:
        return False
    return default


def _config_section_to_plain_dict(
    section: Any, prefix: str
) -> Optional[Dict[str, Any]]:
    """Convert a neclib ConfigurationView or mapping to unprefixed keys."""
    if isinstance(section, Mapping):
        return dict(section)
    if hasattr(section, "items"):
        try:
            return dict(section.items())
        except Exception:
            pass
    if hasattr(section, "parameters"):
        try:
            raw = dict(section.parameters)
        except Exception:
            return None
        prefix_norm = prefix.replace(".", "_") + "_"
        out: Dict[str, Any] = {}
        for key, value in raw.items():
            key_s = str(key)
            if key_s.startswith(prefix_norm):
                key_s = key_s[len(prefix_norm) :]
            out[key_s] = value
        return out
    return None


def _progress_time_sync_from_necst_config() -> Optional[Dict[str, Any]]:
    """Read [progress.time_sync] from the active NECST/neclib site config.

    This intentionally fails closed.  Progress monitor must remain usable even
    when it is run outside a fully configured NECST/ROS environment.
    """
    try:
        from necst import config as necst_config  # type: ignore
    except Exception:
        return None
    for key in ("progress.time_sync", "progress_time_sync"):
        try:
            section = necst_config.get(key)
        except Exception:
            continue
        plain = _config_section_to_plain_dict(section, key)
        if plain is not None:
            return plain
    return None


def _progress_time_sync_from_toml_text(text: str) -> Optional[Dict[str, Any]]:
    """Extract only [progress.time_sync] from TOML-like site config text.

    Some existing NECST site config files contain constructs that the stdlib
    TOML parser rejects even though neclib can read them.  The fallback reader
    must therefore avoid parsing the entire site file.  It only parses the
    requested section so that an unrelated legacy table elsewhere cannot disable
    the optional progress time-sync indicator.
    """
    if tomllib is None:
        return None
    lines = text.splitlines()
    section_lines: List[str] = []
    in_section = False
    header_pattern = re.compile(r"^\s*\[([^\[\]]+)\]\s*(?:#.*)?$")
    array_header_pattern = re.compile(r"^\s*\[\[([^\[\]]+)\]\]\s*(?:#.*)?$")
    for line in lines:
        header = header_pattern.match(line)
        array_header = array_header_pattern.match(line)
        table_name = None
        if header:
            table_name = header.group(1).strip()
        elif array_header:
            table_name = array_header.group(1).strip()
        if table_name is not None:
            if table_name == "progress.time_sync" or table_name.startswith(
                "progress.time_sync."
            ):
                in_section = True
                section_lines.append(line)
                continue
            if in_section:
                break
        if in_section:
            section_lines.append(line)
    if not section_lines:
        return None
    snippet = "\n".join(section_lines) + "\n"
    try:
        data = tomllib.loads(snippet)
    except Exception:
        return None
    progress = data.get("progress") if isinstance(data, Mapping) else None
    if not isinstance(progress, Mapping):
        return None
    section = progress.get("time_sync")
    return dict(section) if isinstance(section, Mapping) else None


def _progress_time_sync_from_toml_file() -> Optional[Dict[str, Any]]:
    """Fallback section reader used when necst/neclib import is unavailable."""
    telescope = os.environ.get("TELESCOPE")
    filename = f"{telescope}_config.toml" if telescope else "config.toml"
    roots: List[Path] = []
    necst_root = os.environ.get("NECST_ROOT")
    if necst_root:
        roots.append(Path(necst_root).expanduser())
    roots.append(Path.home() / ".necst")
    for root in roots:
        path = root / filename
        try:
            section = _progress_time_sync_from_toml_text(
                path.read_text(encoding="utf-8")
            )
        except Exception:
            continue
        if isinstance(section, Mapping):
            return dict(section)
    return None


def _normalize_time_sync_hosts(raw_hosts: Any) -> List[Dict[str, Any]]:
    hosts: List[Dict[str, Any]] = []
    if raw_hosts is None:
        return hosts
    items: Iterable[Any]
    if isinstance(raw_hosts, Mapping):
        mapped: List[Any] = []
        for name, value in raw_hosts.items():
            if isinstance(value, Mapping):
                entry = dict(value)
                entry.setdefault("name", name)
                mapped.append(entry)
            else:
                mapped.append({"name": name, "host": value})
        items = mapped
    elif isinstance(raw_hosts, (list, tuple)):
        items = raw_hosts
    else:
        items = [raw_hosts]

    for item in items:
        if isinstance(item, str):
            name = item.strip()
            host = name
            method = "auto"
        elif isinstance(item, Mapping):
            host_value = (
                item.get("host")
                if item.get("host") not in (None, "")
                else item.get("address")
            )
            host = str(host_value or "").strip()
            name = str(item.get("name") or host).strip()
            method = str(item.get("method") or "auto").strip().lower()
        else:
            continue
        if not host:
            continue
        hosts.append({"name": name or host, "host": host, "method": method or "auto"})
    return hosts


def load_progress_time_sync_config() -> Optional[Dict[str, Any]]:
    """Return normalized progress time-sync config, or None when absent/disabled.

    The only intended configuration location is the active site config, e.g.
    OMU1p85m_config.toml or NANTEN2_config.toml.  No separate profile switch is
    introduced; TELESCOPE/neclib already selects the site.
    """
    raw = _progress_time_sync_from_necst_config()
    if raw is None:
        raw = _progress_time_sync_from_toml_file()
    if not isinstance(raw, Mapping):
        return None
    hosts = _normalize_time_sync_hosts(raw.get("hosts"))
    enabled = _bool_config(raw.get("enabled"), default=bool(hosts))
    if not enabled or not hosts:
        return None
    interval = _finite_float(raw.get("interval_sec"))
    timeout = _finite_float(raw.get("timeout_sec"))
    ok_offset = _finite_float(raw.get("ok_offset_ms"))
    warn_offset = _finite_float(raw.get("warn_offset_ms"))
    bad_after = _finite_int(raw.get("bad_after_failures"))
    return {
        "enabled": True,
        "hosts": hosts,
        "interval_sec": max(10.0, interval if interval is not None else 60.0),
        "timeout_sec": max(0.2, timeout if timeout is not None else 2.0),
        "ok_offset_ms": max(0.0, ok_offset if ok_offset is not None else 5.0),
        "warn_offset_ms": max(0.0, warn_offset if warn_offset is not None else 20.0),
        "bad_after_failures": max(1, bad_after if bad_after is not None else 3),
    }


def _compact_command(argv: Sequence[str]) -> str:
    return " ".join(str(x) for x in argv)


def _parse_float_token(value: Any) -> Optional[float]:
    if value is None:
        return None
    match = re.search(r"[-+]?\d+(?:\.\d*)?(?:[eE][-+]?\d+)?", str(value))
    return _finite_float(match.group(0)) if match else None


def _split_ntp_assignments(text: str) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for token in re.split(r",\s*|\s+", text.replace("\n", " ")):
        if "=" not in token:
            continue
        key, value = token.split("=", 1)
        key = key.strip().lower()
        if key:
            out[key] = value.strip().strip(',')
    return out


class TimeSyncPoller:
    """Low-frequency best-effort NTP/chrony monitor for progress.py only."""

    def __init__(self, config: Mapping[str, Any]) -> None:
        self.hosts = list(config.get("hosts") or [])
        self.interval_sec = float(config.get("interval_sec") or 60.0)
        self.timeout_sec = float(config.get("timeout_sec") or 2.0)
        self.ok_offset_ms = float(config.get("ok_offset_ms") or 5.0)
        self.warn_offset_ms = float(config.get("warn_offset_ms") or 20.0)
        if self.warn_offset_ms < self.ok_offset_ms:
            self.warn_offset_ms = self.ok_offset_ms
        self.bad_after_failures = int(config.get("bad_after_failures") or 3)
        self._lock = threading.RLock()
        self._stop = threading.Event()
        self._failures: Dict[str, int] = {}
        self._state: Dict[str, Any] = {
            "enabled": True,
            "status": "unknown",
            "label": "unknown",
            "summary": "time sync: not checked yet",
            "checked_at_unix": None,
            "hosts": [],
        }
        self._thread = threading.Thread(
            target=self._loop, name="necst-progress-time-sync", daemon=True
        )
        self._thread.start()

    @classmethod
    def from_site_config(cls) -> Optional["TimeSyncPoller"]:
        config = load_progress_time_sync_config()
        if not config:
            return None
        return cls(config)

    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            return copy.deepcopy(self._state)

    def close(self) -> None:
        self._stop.set()
        try:
            if self._thread.is_alive():
                self._thread.join(timeout=1.0)
        except Exception:
            pass

    def _loop(self) -> None:
        while not self._stop.is_set():
            try:
                self.poll_once()
            except Exception as exc:
                with self._lock:
                    self._state = {
                        "enabled": True,
                        "status": "unknown",
                        "label": "unknown",
                        "summary": (
                            "time sync: monitor error "
                            f"({exc.__class__.__name__})"
                        ),
                        "checked_at_unix": time.time(),
                        "hosts": [],
                    }
            self._stop.wait(self.interval_sec)

    def poll_once(self) -> None:
        checked_at = time.time()
        results: List[Dict[str, Any]] = []
        for host_cfg in self.hosts:
            if not isinstance(host_cfg, Mapping):
                continue
            result = self._check_host(host_cfg)
            key = str(result.get("name") or result.get("host") or "host")
            if result.get("status") in {"ok", "warn", "bad"} and not result.get(
                "query_failed"
            ):
                self._failures[key] = 0
            else:
                self._failures[key] = self._failures.get(key, 0) + 1
                result["failures"] = self._failures[key]
                if self._failures[key] >= self.bad_after_failures:
                    result["status"] = "bad"
                    result["synced"] = False
            results.append(result)

        statuses = [str(r.get("status") or "unknown") for r in results]
        ok_count = statuses.count("ok")
        total = len(statuses)
        offsets = [
            abs(float(r["offset_ms"]))
            for r in results
            if _finite_float(r.get("offset_ms")) is not None
        ]
        max_offset = max(offsets) if offsets else None
        if total <= 0:
            status = "unknown"
        elif ok_count == total:
            status = "ok"
        elif any(s in {"bad", "warn"} for s in statuses):
            status = "bad"
        else:
            status = "unknown"
        label = (
            "sync"
            if status == "ok"
            else ("nosync" if status == "bad" else "unknown")
        )
        worst = next(
            (r for r in results if str(r.get("status")) in {"bad", "warn"}),
            next((r for r in results if str(r.get("status")) == "unknown"), None),
        )
        parts = [f"time sync: {label}", f"{ok_count}/{total} OK"]
        if max_offset is not None:
            parts.append(f"max offset {max_offset:.3g} ms")
        if worst is not None and status != "ok":
            reason = str(worst.get("reason") or worst.get("status") or "unknown")
            parts.append(f"worst {worst.get('name')}: {reason}")
        state = {
            "enabled": True,
            "status": status,
            "label": label,
            "summary": ", ".join(parts),
            "checked_at_unix": checked_at,
            "ok_count": ok_count,
            "host_count": total,
            "max_offset_ms": max_offset,
            "hosts": results,
        }
        with self._lock:
            self._state = state

    def _run_command(self, argv: Sequence[str]) -> Dict[str, Any]:
        exe = str(argv[0]) if argv else ""
        if not exe or shutil.which(exe) is None:
            return {
                "ok": False,
                "command_available": False,
                "reason": f"{exe or 'command'} not found",
                "command": _compact_command(argv),
            }
        try:
            proc = subprocess.run(
                list(argv),
                text=True,
                capture_output=True,
                timeout=self.timeout_sec,
                check=False,
            )
        except subprocess.TimeoutExpired:
            return {
                "ok": False,
                "command_available": True,
                "reason": "timeout",
                "command": _compact_command(argv),
            }
        except Exception as exc:
            return {
                "ok": False,
                "command_available": True,
                "reason": exc.__class__.__name__,
                "command": _compact_command(argv),
            }
        text = (proc.stdout or "") + ("\n" + proc.stderr if proc.stderr else "")
        return {
            "ok": proc.returncode == 0 and bool((proc.stdout or "").strip()),
            "command_available": True,
            "returncode": proc.returncode,
            "stdout": proc.stdout or "",
            "stderr": proc.stderr or "",
            "text": text,
            "reason": f"exit {proc.returncode}" if proc.returncode != 0 else "",
            "command": _compact_command(argv),
        }

    def _check_host(self, host_cfg: Mapping[str, Any]) -> Dict[str, Any]:
        name = str(host_cfg.get("name") or host_cfg.get("host") or "host")
        host = str(host_cfg.get("host") or host_cfg.get("address") or name)
        method = str(host_cfg.get("method") or "auto").strip().lower()
        methods = (
            ["chrony", "ntpq", "xntpdq", "ntpdc"]
            if method == "auto"
            else [method]
        )
        last: Optional[Dict[str, Any]] = None
        for candidate in methods:
            result = self._check_host_with_method(name, host, candidate)
            last = result
            # In auto mode, try the next tool only for command/query failures.
            if method == "auto" and result.get("query_failed"):
                continue
            return result
        return last or {
            "name": name,
            "host": host,
            "method": method,
            "status": "unknown",
            "synced": None,
            "reason": "no method usable",
            "query_failed": True,
        }

    def _check_host_with_method(
        self, name: str, host: str, method: str
    ) -> Dict[str, Any]:
        if method in {"chrony", "chronyc"}:
            if host in {"localhost", "127.0.0.1", "::1"}:
                argv = ["chronyc", "-n", "tracking"]
            else:
                argv = ["chronyc", "-n", "-h", host, "tracking"]
            run = self._run_command(argv)
            if not run.get("ok"):
                return self._query_failed_result(name, host, "chrony", run)
            return self._classify_host_result(
                name,
                host,
                "chrony",
                self._parse_chronyc_tracking(str(run.get("stdout") or "")),
            )
        if method == "ntpq":
            argv = ["ntpq", "-n", "-c", "rv", "-c", "peers", host]
            run = self._run_command(argv)
            if not run.get("ok"):
                return self._query_failed_result(name, host, "ntpq", run)
            return self._classify_host_result(
                name,
                host,
                "ntpq",
                self._parse_ntpq_output(str(run.get("stdout") or "")),
            )
        if method == "xntpdq":
            for argv in (
                ["xntpdq", "-n", "-c", "rv", "-c", "peers", host],
                ["xntpdq", "-n", "-p", host],
            ):
                run = self._run_command(argv)
                if run.get("ok"):
                    return self._classify_host_result(
                        name,
                        host,
                        "xntpdq",
                        self._parse_ntpq_output(str(run.get("stdout") or "")),
                    )
                last = run
            return self._query_failed_result(name, host, "xntpdq", last)
        if method == "ntpdc":
            argv = ["ntpdc", "-n", "-c", "sysinfo", "-c", "peers", host]
            run = self._run_command(argv)
            if not run.get("ok"):
                return self._query_failed_result(name, host, "ntpdc", run)
            return self._classify_host_result(
                name,
                host,
                "ntpdc",
                self._parse_ntpdc_output(str(run.get("stdout") or "")),
            )
        return {
            "name": name,
            "host": host,
            "method": method,
            "status": "unknown",
            "synced": None,
            "reason": f"unsupported method {method!r}",
            "query_failed": True,
        }

    def _query_failed_result(
        self, name: str, host: str, method: str, run: Optional[Mapping[str, Any]]
    ) -> Dict[str, Any]:
        return {
            "name": name,
            "host": host,
            "method": method,
            "status": "unknown",
            "synced": None,
            "offset_ms": None,
            "source": "",
            "reason": str((run or {}).get("reason") or "query failed"),
            "query_failed": True,
            "command": str((run or {}).get("command") or ""),
        }

    def _classify_host_result(
        self, name: str, host: str, method: str, parsed: Mapping[str, Any]
    ) -> Dict[str, Any]:
        synced = parsed.get("synced")
        offset_ms = _finite_float(parsed.get("offset_ms"))
        if synced is False:
            status = "bad"
            reason = str(parsed.get("reason") or "unsynchronised")
        elif offset_ms is not None and abs(offset_ms) > self.warn_offset_ms:
            status = "bad"
            reason = f"offset {offset_ms:+.3g} ms"
        elif offset_ms is not None and abs(offset_ms) > self.ok_offset_ms:
            status = "warn"
            reason = f"offset {offset_ms:+.3g} ms"
        elif synced is True:
            status = "ok"
            reason = "synchronised"
        else:
            status = "unknown"
            reason = str(parsed.get("reason") or "sync state unknown")
        return {
            "name": name,
            "host": host,
            "method": method,
            "status": status,
            "synced": synced,
            "offset_ms": offset_ms,
            "source": parsed.get("source") or "",
            "stratum": parsed.get("stratum"),
            "reach": parsed.get("reach"),
            "reason": reason,
            "query_failed": False,
        }

    def _parse_chronyc_tracking(self, text: str) -> Dict[str, Any]:
        out: Dict[str, Any] = {"synced": None, "offset_ms": None, "source": ""}
        for line in text.splitlines():
            if ":" not in line:
                continue
            key, value = line.split(":", 1)
            key_l = key.strip().lower()
            value_s = value.strip()
            if key_l == "reference id":
                out["source"] = value_s
            elif key_l == "stratum":
                out["stratum"] = _finite_int(value_s)
            elif key_l == "system time":
                seconds = _parse_float_token(value_s)
                if seconds is not None:
                    sign = -1.0 if "slow" in value_s.lower() else 1.0
                    out["offset_ms"] = sign * seconds * 1000.0
            elif key_l == "leap status":
                token = value_s.lower()
                if "normal" in token:
                    out["synced"] = True
                elif "not" in token or "unsynchron" in token:
                    out["synced"] = False
                    out["reason"] = value_s
        stratum = _finite_int(out.get("stratum"))
        if stratum is not None and stratum >= 16:
            out["synced"] = False
            out.setdefault("reason", f"stratum {stratum}")
        return out

    def _parse_ntpq_output(self, text: str) -> Dict[str, Any]:
        out: Dict[str, Any] = {"synced": None, "offset_ms": None, "source": ""}
        assignments = _split_ntp_assignments(text)
        if "offset" in assignments:
            # ntpq rv reports offset in milliseconds.
            out["offset_ms"] = _parse_float_token(assignments.get("offset"))
        if "stratum" in assignments:
            out["stratum"] = _finite_int(assignments.get("stratum"))
        leap = str(assignments.get("leap") or "").lower()
        if leap in {"00", "leap_none", "none"}:
            out["synced"] = True
        elif leap in {"11", "leap_alarm", "alarm"}:
            out["synced"] = False
            out["reason"] = f"leap {leap}"
        for line in text.splitlines():
            stripped = line.lstrip()
            if not stripped or stripped[0] not in {"*", "o"}:
                continue
            fields = stripped[1:].split()
            if fields:
                out["source"] = fields[0]
                out["synced"] = True
            if len(fields) >= 9:
                offset = _finite_float(fields[8])
                if offset is not None:
                    out["offset_ms"] = offset
            if len(fields) >= 7:
                out["reach"] = fields[6]
            break
        stratum = _finite_int(out.get("stratum"))
        if stratum is not None and stratum >= 16:
            out["synced"] = False
            out.setdefault("reason", f"stratum {stratum}")
        if out.get("synced") is None and "sync_unspec" in text.lower():
            out["synced"] = False
            out.setdefault("reason", "sync_unspec")
        return out

    def _parse_ntpdc_output(self, text: str) -> Dict[str, Any]:
        out: Dict[str, Any] = {"synced": None, "offset_ms": None, "source": ""}
        for line in text.splitlines():
            if ":" in line:
                key, value = line.split(":", 1)
                key_l = key.strip().lower()
                value_s = value.strip()
                if key_l == "system peer":
                    out["source"] = value_s
                    if value_s and value_s not in {"0.0.0.0", "none"}:
                        out["synced"] = True
                elif key_l == "stratum":
                    out["stratum"] = _finite_int(value_s)
                elif key_l == "leap indicator":
                    token = value_s.lower()
                    if token.startswith("00") or "none" in token:
                        out["synced"] = True
                    elif token.startswith("11") or "alarm" in token:
                        out["synced"] = False
                        out["reason"] = value_s
            stripped = line.lstrip()
            if stripped and stripped[0] in {"*", "o"}:
                fields = stripped[1:].split()
                if fields:
                    out["source"] = fields[0]
                    out["synced"] = True
                # ntpdc peers usually ends with delay, offset, dispersion.
                if len(fields) >= 3:
                    offset = _finite_float(fields[-2])
                    if offset is not None:
                        out["offset_ms"] = (
                            offset * 1000.0 if abs(offset) < 1.0 else offset
                        )
        stratum = _finite_int(out.get("stratum"))
        if stratum is not None and stratum >= 16:
            out["synced"] = False
            out.setdefault("reason", f"stratum {stratum}")
        return out


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


def apply_dynamic_remaining(
    snapshot: Optional[Dict[str, Any]],
) -> Optional[Dict[str, Any]]:
    """Return a display/API snapshot with ETA counted down from completion time.

    ObservationProgressReporter writes an estimated_completion_unix whenever it
    has enough event-history data.  The sidecar itself is updated only at state
    transitions, so a watch/web client should derive the current remaining time
    from the completion timestamp while the observation is still active.
    """
    if not isinstance(snapshot, dict):
        return snapshot
    timing = snapshot.get("time")
    lifecycle = (
        snapshot.get("lifecycle") if isinstance(snapshot.get("lifecycle"), dict) else {}
    )
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


_RADEC_FRAMES = {"j2000", "radec", "ra/dec", "equatorial", "fk5", "icrs"}
_GALACTIC_FRAMES = {"galactic", "gal", "lb", "l,b"}


def _frame_token(frame: Any) -> str:
    return str(frame or "").strip().lower().replace(" ", "")


def _is_radec_frame(frame: Any) -> bool:
    token = _frame_token(frame)
    return (
        token in _RADEC_FRAMES
        or "j2000" in token
        or "radec" in token
        or token.startswith("fk5")
    )


def _is_galactic_frame(frame: Any) -> bool:
    token = _frame_token(frame)
    return token in _GALACTIC_FRAMES or "gal" in token


def _source_sky_coord_from_geometry(
    geom: Mapping[str, Any],
) -> tuple[Optional[tuple[float, float]], str, str]:
    """Return the best source/reference sky coordinate stored in progress geometry."""
    for key in ("target", "reference"):
        value = geom.get(key)
        pair = _coord_pair(value)
        if pair is None:
            continue
        frame = _coord_frame(value, str(geom.get("frame") or ""))
        return pair, frame, key
    return None, "", ""


def _skycoord_from_lonlat_frame(lon_deg: float, lat_deg: float, frame: Any) -> Any:
    """Create an Astropy SkyCoord for common NECST source coordinate frames.

    RA/Dec <-> Galactic conversion must stay in Astropy rather than a hand-coded
    browser rotation matrix.  The browser only formats fields already derived here.
    """
    from astropy import units as u  # type: ignore
    from astropy.coordinates import FK5, SkyCoord  # type: ignore
    from astropy.time import Time  # type: ignore

    token = _frame_token(frame)
    if token == "icrs":
        return SkyCoord(
            ra=float(lon_deg) * u.deg, dec=float(lat_deg) * u.deg, frame="icrs"
        )
    if _is_radec_frame(frame):
        return SkyCoord(
            ra=float(lon_deg) * u.deg,
            dec=float(lat_deg) * u.deg,
            frame=FK5(equinox=Time("J2000")),
        )
    if _is_galactic_frame(frame):
        return SkyCoord(
            l=float(lon_deg) * u.deg, b=float(lat_deg) * u.deg, frame="galactic"
        )
    return None


def _derive_radec_galactic_from_geometry(geom: Mapping[str, Any]) -> Dict[str, Any]:
    pair, frame, source_key = _source_sky_coord_from_geometry(geom)
    out: Dict[str, Any] = {}
    if pair is None:
        return out
    lon_deg, lat_deg = pair
    if source_key:
        out["source_coordinate_key"] = source_key
    if frame:
        out["source_coordinate_frame"] = frame

    # Preserve native-frame values even when Astropy is unavailable.  This keeps
    # LB->display and RA/Dec->display useful, while avoiding any self-written
    # cross-frame conversion.
    if _is_radec_frame(frame):
        out["target_radec_j2000"] = [float(lon_deg) % 360.0, float(lat_deg), "J2000"]
    if _is_galactic_frame(frame):
        out["target_galactic_lb"] = [float(lon_deg) % 360.0, float(lat_deg), "galactic"]

    try:
        coord = _skycoord_from_lonlat_frame(float(lon_deg), float(lat_deg), frame)
        if coord is None:
            return out
        from astropy import units as u  # type: ignore
        from astropy.coordinates import FK5  # type: ignore
        from astropy.time import Time  # type: ignore

        fk5 = coord.transform_to(FK5(equinox=Time("J2000")))
        gal = coord.galactic
        out["target_radec_j2000"] = [
            float(fk5.ra.to_value(u.deg)) % 360.0,
            float(fk5.dec.to_value(u.deg)),
            "J2000",
        ]
        out["target_galactic_lb"] = [
            float(gal.l.to_value(u.deg)) % 360.0,
            float(gal.b.to_value(u.deg)),
            "galactic",
        ]
        out["coordinate_transform_backend"] = "astropy"
    except Exception as exc:
        # Progress display must never break an observation.  If Astropy is not
        # importable in an offline analysis environment, browser/CLI simply show
        # the native-frame coordinate and leave the cross-frame row blank.
        out["coordinate_transform_backend"] = "unavailable"
        out["coordinate_transform_note"] = (
            f"Astropy coordinate transform unavailable: {exc.__class__.__name__}"
        )
    return out


def _site_longitude_from_snapshot(snapshot: Mapping[str, Any]) -> Optional[float]:
    for section, keys in (
        ("site", ("longitude_deg", "lon_deg", "site_lon_deg")),
        ("observer", ("longitude_deg", "lon_deg", "site_lon_deg")),
        ("observation", ("site_longitude_deg", "longitude_deg", "lon_deg")),
        ("telescope", ("longitude_deg", "lon_deg", "site_lon_deg")),
    ):
        obj = snapshot.get(section)
        if not isinstance(obj, Mapping):
            continue
        for key in keys:
            value = _finite_float(obj.get(key))
            if value is not None:
                return value
    return None


def _format_lst_hms(hours: Any) -> Optional[str]:
    value = _finite_float(hours)
    if value is None:
        return None
    value = value % 24.0
    total = int(round(value * 3600.0)) % (24 * 3600)
    h, rem = divmod(total, 3600)
    m, s = divmod(rem, 60)
    return f"{h:02d}:{m:02d}:{s:02d}"


def _lst_hours_with_neclib(unix_sec: float) -> Optional[float]:
    """Use NECST/neclib's canonical Observer.lst when the runtime config exists."""
    try:
        from neclib.coordinates import Observer  # type: ignore
        from necst import config as necst_config  # type: ignore

        location = getattr(necst_config, "location", None)
        if location is None:
            return None
        lst = Observer(location).lst(float(unix_sec))
        if hasattr(lst, "hour"):
            return float(lst.hour)
        if hasattr(lst, "to_value"):
            return float(lst.to_value("hourangle"))
        return float(lst)
    except Exception:
        return None


def _lst_hours_with_astropy(unix_sec: float, lon_deg: float) -> Optional[float]:
    """Use Astropy for LST when neclib Observer is not importable."""
    try:
        from astropy import units as u  # type: ignore
        from astropy.time import Time  # type: ignore

        return float(
            Time(float(unix_sec), format="unix", scale="utc")
            .sidereal_time("mean", longitude=float(lon_deg) * u.deg)
            .hour
        )
    except Exception:
        return None


def _derive_lst(
    snapshot: Mapping[str, Any], *, server_time_unix: Optional[float] = None
) -> Dict[str, Any]:
    timing = snapshot.get("time") if isinstance(snapshot.get("time"), Mapping) else {}
    unix_sec = _finite_float(timing.get("updated_at_unix"))
    if unix_sec is None:
        unix_sec = _finite_float(server_time_unix)
    if unix_sec is None:
        return {}

    lst_hours = _lst_hours_with_neclib(float(unix_sec))
    backend = "neclib.Observer.lst" if lst_hours is not None else ""
    lon_deg = _site_longitude_from_snapshot(snapshot)
    if lst_hours is None and lon_deg is not None:
        lst_hours = _lst_hours_with_astropy(float(unix_sec), float(lon_deg))
        backend = "astropy.time.Time.sidereal_time" if lst_hours is not None else ""
    text = _format_lst_hms(lst_hours)
    if text is None:
        return {}
    out: Dict[str, Any] = {"lst_hours": float(lst_hours) % 24.0, "lst_hms": text}
    if backend:
        out["lst_backend"] = backend
    if lon_deg is not None:
        out["lst_longitude_deg"] = float(lon_deg)
    return out


def apply_display_derivations(
    snapshot: Optional[Dict[str, Any]], *, server_time_unix: Optional[float] = None
) -> Optional[Dict[str, Any]]:
    """Add display-only coordinate/LST derivations to a snapshot.

    The progress sidecar remains the source of truth and keeps original frames.
    Derived fields are added under ``snapshot["display"]`` so CLI/API/Web can
    format the same values.  Cross-frame sky transforms are performed only by
    Astropy; LST is taken from neclib Observer when available, otherwise Astropy.
    """
    if not isinstance(snapshot, dict):
        return snapshot
    out = copy.deepcopy(snapshot)
    geom = out.get("geometry") if isinstance(out.get("geometry"), Mapping) else {}
    display: Dict[str, Any] = {}
    if isinstance(geom, Mapping):
        display.update(_derive_radec_galactic_from_geometry(geom))
    display.update(_derive_lst(out, server_time_unix=server_time_unix))
    if display:
        out.setdefault("display", {}).update(display)
    return out


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
    if pointing:
        antenna_update["pointing_status_available"] = True
        antenna_update["pointing_status_valid"] = bool(pointing.get("valid", False))
        if pointing.get("cmd_az_deg") is not None:
            antenna_update["command_lon_deg"] = pointing.get("cmd_az_deg")
            antenna_update["command_lat_deg"] = pointing.get("cmd_el_deg")
            antenna_update["command_frame"] = "altaz"
            antenna_update["command_unit"] = "deg"
            antenna_update["command_time_unix"] = pointing.get("command_time_unix")
        if pointing.get("enc_az_deg") is not None:
            antenna_update["encoder_lon_deg"] = pointing.get("enc_az_deg")
            antenna_update["encoder_lat_deg"] = pointing.get("enc_el_deg")
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
    else:
        if cmd:
            antenna_update.update(cmd)
        if enc:
            antenna_update.update(enc)
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
        self._lock = threading.RLock()
        self._executor = None
        self._spin_thread: Optional[threading.Thread] = None
        self._spin_mode = "disabled"
        self.control: Dict[str, Any] = {}
        self.command: Dict[str, Any] = {}
        self.encoder: Dict[str, Any] = {}
        self.tracking: Dict[str, Any] = {}
        self.section_status: Dict[str, Any] = {}
        self.pointing_status: Dict[str, Any] = {}
        self.az_unwrap_status: Dict[str, Any] = {}
        self.queue_status: Dict[str, Any] = {}
        self.spectrometer_status: Dict[str, Any] = {}
        self.chopper_status: Dict[str, Any] = {}
        self.weather: Dict[str, Dict[str, Any]] = {}
        if self.requested:
            self._start()

    def _start(self) -> None:
        try:
            repo_root = str(Path(__file__).resolve().parents[1])
            if repo_root not in sys.path:
                sys.path.insert(0, repo_root)
            import rclpy  # type: ignore
            from necst import config as necst_config  # type: ignore
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

            def tracking_cb(msg: Any) -> None:
                # TrackingStatus is produced by NECST's antenna tracking node.
                # It compares the encoder position with the command value at the
                # encoder timestamp, using the control-side interpolation/extrapolation.
                # Progress should not recompute tracking error from two latest samples.
                self.tracking = {
                    "ok": bool(getattr(msg, "ok", False)),
                    "error_deg": _finite_float(getattr(msg, "error", None)),
                    "time_unix": _finite_float(getattr(msg, "time", None)),
                }

            def section_status_cb(msg: Any) -> None:
                self.section_status = {
                    "active": bool(getattr(msg, "active", False)),
                    "control_id": str(getattr(msg, "control_id", "")),
                    "section_uid": str(getattr(msg, "section_uid", "")),
                    "section_plan_index": int(getattr(msg, "section_plan_index", -1)),
                    "section_sequence_index": int(
                        getattr(msg, "section_sequence_index", -1)
                    ),
                    "section_kind": str(getattr(msg, "section_kind", "")),
                    "section_label": str(getattr(msg, "section_label", "")),
                    "line_index": int(getattr(msg, "line_index", -1)),
                    "section_start_unix": _finite_float(
                        getattr(msg, "section_start_unix", None)
                    ),
                    "section_stop_unix": _finite_float(
                        getattr(msg, "section_stop_unix", None)
                    ),
                    "section_duration_sec": _finite_float(
                        getattr(msg, "section_duration_sec", None)
                    ),
                    "query_time_unix": _finite_float(
                        getattr(msg, "query_time_unix", None)
                    ),
                    "publish_time_unix": _finite_float(
                        getattr(msg, "publish_time_unix", None)
                    ),
                    "command_time_unix": _finite_float(
                        getattr(msg, "command_time_unix", None)
                    ),
                    "nominal_fraction": _finite_float(
                        getattr(msg, "nominal_fraction", None)
                    ),
                    "fraction_valid": bool(getattr(msg, "fraction_valid", False)),
                    "tight": bool(getattr(msg, "tight", False)),
                    "science_line": bool(getattr(msg, "science_line", False)),
                    "interrupt_ok": bool(getattr(msg, "interrupt_ok", False)),
                    "geometry_valid": bool(getattr(msg, "geometry_valid", False)),
                    "section_frame": str(getattr(msg, "section_frame", "")),
                    "section_unit": str(getattr(msg, "section_unit", "")),
                    "section_start_lon_deg": _finite_float(
                        getattr(msg, "section_start_lon_deg", None)
                    ),
                    "section_start_lat_deg": _finite_float(
                        getattr(msg, "section_start_lat_deg", None)
                    ),
                    "section_stop_lon_deg": _finite_float(
                        getattr(msg, "section_stop_lon_deg", None)
                    ),
                    "section_stop_lat_deg": _finite_float(
                        getattr(msg, "section_stop_lat_deg", None)
                    ),
                    "section_speed_deg_per_sec": _finite_float(
                        getattr(msg, "section_speed_deg_per_sec", None)
                    ),
                    "status_basis": str(getattr(msg, "status_basis", "")),
                }

            def pointing_status_cb(msg: Any) -> None:
                self.pointing_status = {
                    "valid": bool(getattr(msg, "valid", False)),
                    "publish_time_unix": _finite_float(
                        getattr(msg, "publish_time_unix", None)
                    ),
                    "command_time_unix": _finite_float(
                        getattr(msg, "command_time_unix", None)
                    ),
                    "encoder_time_unix": _finite_float(
                        getattr(msg, "encoder_time_unix", None)
                    ),
                    "cmd_az_deg": _finite_float(getattr(msg, "cmd_az_deg", None)),
                    "cmd_el_deg": _finite_float(getattr(msg, "cmd_el_deg", None)),
                    "enc_az_deg": _finite_float(getattr(msg, "enc_az_deg", None)),
                    "enc_el_deg": _finite_float(getattr(msg, "enc_el_deg", None)),
                    "delta_az_deg": _finite_float(getattr(msg, "delta_az_deg", None)),
                    "delta_el_deg": _finite_float(getattr(msg, "delta_el_deg", None)),
                    "delta_az_cos_el_deg": _finite_float(
                        getattr(msg, "delta_az_cos_el_deg", None)
                    ),
                    "tracking_error_deg": _finite_float(
                        getattr(msg, "tracking_error_deg", None)
                    ),
                    "tracking_threshold_deg": _finite_float(
                        getattr(msg, "tracking_threshold_deg", None)
                    ),
                    "tracking_ok": bool(getattr(msg, "tracking_ok", False)),
                    "cmd_age_sec": _finite_float(getattr(msg, "cmd_age_sec", None)),
                    "enc_age_sec": _finite_float(getattr(msg, "enc_age_sec", None)),
                    "command_stale": bool(getattr(msg, "command_stale", False)),
                    "encoder_stale": bool(getattr(msg, "encoder_stale", False)),
                    "basis": str(getattr(msg, "basis", "")),
                }

            def az_unwrap_status_cb(msg: Any) -> None:
                self.az_unwrap_status = {
                    "enabled": bool(getattr(msg, "enabled", False)),
                    "valid": bool(getattr(msg, "valid", False)),
                    "mode": str(getattr(msg, "mode", "")),
                    "state": str(getattr(msg, "state", "")),
                    "reason": str(getattr(msg, "reason", "")),
                    "publish_time_unix": _finite_float(
                        getattr(msg, "publish_time_unix", None)
                    ),
                    "encoder_time_unix": _finite_float(
                        getattr(msg, "encoder_time_unix", None)
                    ),
                    "raw_az_deg": _finite_float(getattr(msg, "raw_az_deg", None)),
                    "modulo_az_deg": _finite_float(getattr(msg, "modulo_az_deg", None)),
                    "continuous_az_deg": _finite_float(
                        getattr(msg, "continuous_az_deg", None)
                    ),
                    "branch": int(getattr(msg, "branch", 0)),
                    "branch_nonzero": bool(getattr(msg, "branch_nonzero", False)),
                    "branch_changed": bool(getattr(msg, "branch_changed", False)),
                    "state_age_sec": _finite_float(getattr(msg, "state_age_sec", None)),
                    "persist_age_sec": _finite_float(
                        getattr(msg, "persist_age_sec", None)
                    ),
                }

            def queue_status_cb(msg: Any) -> None:
                self.queue_status = {
                    "active": bool(getattr(msg, "active", False)),
                    "control_id": str(getattr(msg, "control_id", "")),
                    "mode": str(getattr(msg, "mode", "")),
                    "publish_time_unix": _finite_float(
                        getattr(msg, "publish_time_unix", None)
                    ),
                    "command_offset_sec": _finite_float(
                        getattr(msg, "command_offset_sec", None)
                    ),
                    "min_buffer_sec": _finite_float(
                        getattr(msg, "min_buffer_sec", None)
                    ),
                    "max_buffer_sec": _finite_float(
                        getattr(msg, "max_buffer_sec", None)
                    ),
                    "queue_length": int(getattr(msg, "queue_length", 0)),
                    "head_lead_sec": _finite_float(getattr(msg, "head_lead_sec", None)),
                    "tail_lead_sec": _finite_float(getattr(msg, "tail_lead_sec", None)),
                    "last_publish_wall_time_unix": _finite_float(
                        getattr(msg, "last_publish_wall_time_unix", None)
                    ),
                    "last_published_cmd_time_unix": _finite_float(
                        getattr(msg, "last_published_cmd_time_unix", None)
                    ),
                    "publish_gap_sec": _finite_float(
                        getattr(msg, "publish_gap_sec", None)
                    ),
                    "generator_exhausted": bool(
                        getattr(msg, "generator_exhausted", False)
                    ),
                    "guard_latched": bool(getattr(msg, "guard_latched", False)),
                    "reason": str(getattr(msg, "reason", "")),
                }

            def spectrometer_status_cb(msg: Any) -> None:
                self.spectrometer_status = {
                    "valid": bool(getattr(msg, "valid", False)),
                    "recorder_active": bool(getattr(msg, "recorder_active", False)),
                    "saving_enabled": bool(getattr(msg, "saving_enabled", False)),
                    "acquiring": bool(getattr(msg, "acquiring", False)),
                    "publish_time_unix": _finite_float(
                        getattr(msg, "publish_time_unix", None)
                    ),
                    "last_dump_time_unix": _finite_float(
                        getattr(msg, "last_dump_time_unix", None)
                    ),
                    "last_record_time_unix": _finite_float(
                        getattr(msg, "last_record_time_unix", None)
                    ),
                    "last_stream_time_unix": _finite_float(
                        getattr(msg, "last_stream_time_unix", None)
                    ),
                    "last_dump_age_sec": _finite_float(
                        getattr(msg, "last_dump_age_sec", None)
                    ),
                    "latest_time_spectrometer": str(
                        getattr(msg, "latest_time_spectrometer", "")
                    ),
                    "record_every_n": int(getattr(msg, "record_every_n", 0)),
                    "data_queue_size_max": int(getattr(msg, "data_queue_size_max", 0)),
                    "n_streams": int(getattr(msg, "n_streams", 0)),
                    "n_boards": int(getattr(msg, "n_boards", 0)),
                    "missing_board_count": int(getattr(msg, "missing_board_count", 0)),
                    "tp_mode": bool(getattr(msg, "tp_mode", False)),
                    "tp_range_start": int(getattr(msg, "tp_range_start", -1)),
                    "tp_range_stop": int(getattr(msg, "tp_range_stop", -1)),
                    "qlook_ch_start": int(getattr(msg, "qlook_ch_start", -1)),
                    "qlook_ch_stop": int(getattr(msg, "qlook_ch_stop", -1)),
                    "metadata_position": str(getattr(msg, "metadata_position", "")),
                    "metadata_id": str(getattr(msg, "metadata_id", "")),
                    "section_kind": str(getattr(msg, "section_kind", "")),
                    "section_label": str(getattr(msg, "section_label", "")),
                    "line_index": int(getattr(msg, "line_index", -1)),
                    "recorder_path": str(getattr(msg, "recorder_path", "")),
                    "warning": str(getattr(msg, "warning", "")),
                }

            def chopper_status_cb(msg: Any) -> None:
                try:
                    position = int(getattr(msg, "position"))
                except Exception:
                    position = None
                try:
                    insert = bool(getattr(msg, "insert"))
                except Exception:
                    insert = None
                try:
                    insert_position = int(necst_config.chopper_motor_position["insert"])
                    remove_position = int(necst_config.chopper_motor_position["remove"])
                except Exception:
                    insert_position = None
                    remove_position = None
                try:
                    simulator_boolean_only = bool(getattr(necst_config, "simulator", False))
                except Exception:
                    simulator_boolean_only = False
                # ChopperSimulator publishes only the boolean insert flag; the
                # int32 position field remains the ROS default 0.  In real
                # hardware mode, position=0 must be treated as a real counter
                # value.  In simulator mode, display position=0 + insert flag as
                # the simulator state even for NANTEN2 OUT where remove=250.

                if (
                    position is not None
                    and position == insert_position
                    and insert is not False
                ):
                    state = "in"
                elif (
                    position is not None
                    and position == remove_position
                    and insert is not True
                ):
                    state = "out"
                elif simulator_boolean_only and position == 0 and insert is True:
                    state = "in"
                elif simulator_boolean_only and position == 0 and insert is False:
                    state = "out"
                elif insert is True:
                    state = "in?"
                elif insert is False:
                    state = "out?"
                else:
                    state = "unknown"

                self.chopper_status = {
                    "insert": insert,
                    "state": state,
                    "position": position,
                    "time_unix": _finite_float(getattr(msg, "time", None)),
                }

            def weather_cb(key: str):
                def _callback(msg: Any) -> None:
                    payload = {
                        "temperature_k": _finite_float(
                            getattr(msg, "temperature", None)
                        ),
                        "in_temperature_k": _finite_float(
                            getattr(msg, "in_temperature", None)
                        ),
                        "pressure_hpa": _finite_float(getattr(msg, "pressure", None)),
                        "humidity_percent": _finite_float(
                            getattr(msg, "humidity", None)
                        ),
                        "in_humidity_percent": _finite_float(
                            getattr(msg, "in_humidity", None)
                        ),
                        "wind_speed_mps": _finite_float(
                            getattr(msg, "wind_speed", None)
                        ),
                        "wind_direction_deg": _finite_float(
                            getattr(msg, "wind_direction", None)
                        ),
                        "rain_rate": _finite_float(getattr(msg, "rain_rate", None)),
                        "time_unix": _finite_float(getattr(msg, "time", None)),
                    }
                    with self._lock:
                        self.weather[key] = payload

                return _callback

            topic.antenna_control_status.subscription(self._node, control_cb)
            topic.altaz_cmd.subscription(self._node, command_cb)
            topic.antenna_encoder.subscription(self._node, encoder_cb)
            try:
                topic.antenna_tracking.subscription(self._node, tracking_cb)
            except Exception:
                # Older deployments may not expose this topic; in that case the
                # monitor should still show Az/El and simply mark tracking status
                # as unavailable.
                pass
            for topic_obj, callback in (
                (getattr(topic, "antenna_section_status", None), section_status_cb),
                (getattr(topic, "antenna_pointing_status", None), pointing_status_cb),
                (getattr(topic, "antenna_az_unwrap_status", None), az_unwrap_status_cb),
                (getattr(topic, "antenna_command_queue_status", None), queue_status_cb),
                (getattr(topic, "spectrometer_status", None), spectrometer_status_cb),
                (getattr(topic, "chopper_status", None), chopper_status_cb),
            ):
                try:
                    if topic_obj is not None:
                        topic_obj.subscription(self._node, callback)
                except Exception:
                    pass
            # Weather topics are indexed (typically /weather/ambient/out and
            # /weather/ambient/in).  Subscribe explicitly to both common keys; if
            # a site has only one of them, ROS simply leaves the other empty.
            try:
                topic.weather["out"].subscription(self._node, weather_cb("out"))
                topic.weather["in"].subscription(self._node, weather_cb("in"))
            except Exception:
                pass
            self.available = True
            self._start_background_spin()
        except Exception as exc:
            self.available = False
            self.error = str(exc)
            try:
                if self._node is not None:
                    self._node.destroy_node()
            except Exception:
                pass
            self._node = None

    def _start_background_spin(self) -> None:
        """Continuously drain ROS callbacks in a background executor.

        Request-driven ``spin_once(0)`` is not enough for the web monitor once
        v39 subscribes to several status topics.  ``spin_once`` processes at
        most one ready callback, so a 1 Hz HTTP refresh can leave section,
        pointing, queue, and spectrometer status several seconds stale.  The
        dashboard must cache live ROS telemetry independently of browser
        refreshes.
        """
        if not (self.available and self._rclpy is not None and self._node is not None):
            return
        try:
            from rclpy.executors import MultiThreadedExecutor  # type: ignore

            executor = MultiThreadedExecutor(num_threads=2)
            executor.add_node(self._node)
            self._executor = executor
            self._spin_mode = "background-executor"
            self._spin_thread = threading.Thread(
                target=executor.spin,
                name="necst-progress-ros-spin",
                daemon=True,
            )
            self._spin_thread.start()
        except Exception as exc:
            # Fall back to explicit spin_once calls used by older progress.py.
            # This is less responsive but keeps --no-ros and old ROS installs usable.
            self._executor = None
            self._spin_thread = None
            self._spin_mode = "request-spin-fallback"
            self.error = f"background ROS spin disabled: {exc}"

    def spin_once(self, timeout_sec: float = 0.0) -> None:
        if not (self.available and self._rclpy is not None and self._node is not None):
            return
        if self._executor is not None:
            return
        try:
            self._rclpy.spin_once(self._node, timeout_sec=timeout_sec)
        except Exception as exc:
            self.error = str(exc)
            self.available = False

    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            payload: Dict[str, Any] = {
                "available": self.available,
                "spin_mode": self._spin_mode,
            }
            if self.error:
                payload["error"] = self.error
            if self.control:
                payload["control"] = dict(self.control)
            if self.command:
                payload["command"] = dict(self.command)
            if self.encoder:
                payload["encoder"] = dict(self.encoder)
            if self.tracking:
                payload["tracking"] = dict(self.tracking)
            if self.section_status:
                payload["section_status"] = dict(self.section_status)
            if self.pointing_status:
                payload["pointing_status"] = dict(self.pointing_status)
            if self.az_unwrap_status:
                payload["az_unwrap_status"] = dict(self.az_unwrap_status)
            if self.queue_status:
                payload["queue_status"] = dict(self.queue_status)
            if self.spectrometer_status:
                payload["spectrometer_status"] = dict(self.spectrometer_status)
            if self.chopper_status:
                payload["chopper_status"] = dict(self.chopper_status)
            if self.weather:
                payload["weather"] = {
                    key: dict(value) for key, value in self.weather.items()
                }
            return payload

    def close(self) -> None:
        try:
            if self._executor is not None:
                try:
                    self._executor.shutdown()
                except Exception:
                    pass
        except Exception:
            pass
        try:
            if self._spin_thread is not None and self._spin_thread.is_alive():
                self._spin_thread.join(timeout=1.0)
        except Exception:
            pass
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


def latest_plan_path(
    root: Path, snapshot: Optional[Dict[str, Any]] = None
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


def fmt_seconds_short(seconds: Optional[float], approx: bool = False) -> str:
    if seconds is None:
        return "--.-s"
    try:
        value = max(0.0, float(seconds))
    except Exception:
        return "--.-s"
    prefix = "≈" if approx else ""
    if value < 3600.0:
        return f"{prefix}{value:.1f}s"
    return f"{prefix}{value / 3600.0:.2f}h"


def compact_eta_method(value: Any, *, max_len: int = 34) -> str:
    text = str(value or "-")
    if text == "-":
        return text
    replacements = {
        "elapsed_plan_fraction_sanity": "sanity",
        "event_history_inter_item_gap": "gap",
        "event_history_item_key": "item",
        "event_history": "hist",
        "scan_block": "block",
    }
    parts = []
    for part in text.split("+"):
        part = part.strip()
        parts.append(replacements.get(part, part))
    out = "+".join(parts)
    if len(out) <= max_len:
        return out
    keep = max(8, max_len - 2)
    return out[:keep] + "…"


def pct_bar(percent: Optional[float], width: int = 50) -> str:
    try:
        p = max(0.0, min(100.0, float(percent)))
    except Exception:
        p = 0.0
    n = int(round(width * p / 100.0))
    return "[" + "#" * n + "-" * (width - n) + f"] {p:5.1f}%"


def _geom_of(item: Mapping[str, Any]) -> Mapping[str, Any]:
    geom = item.get("geometry") if isinstance(item, Mapping) else None
    return geom if isinstance(geom, Mapping) else {}


def _display_geom_context(geom: Mapping[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for key in (
        "target_name",
        "target",
        "reference",
        "offset",
        "mode",
        "frame",
        "unit",
    ):
        if key in geom:
            out[key] = geom[key]
    return out


def _flatten_plan_items_for_display(
    plan: Optional[Mapping[str, Any]],
) -> List[Dict[str, Any]]:
    """Flatten scan-block children for observer-facing progress summaries.

    This mirrors the browser Plan View logic.  Parent plan items are kept so
    point/OFF/HOT items remain visible, while scan_block lines are expanded into
    per-line pseudo-items for OTF line progress.
    """
    raw_items = plan.get("items") if isinstance(plan, Mapping) else None
    if not isinstance(raw_items, list):
        return []
    out: List[Dict[str, Any]] = []
    for raw in raw_items:
        if not isinstance(raw, Mapping):
            continue
        item = dict(raw)
        out.append(item)
        geom = _geom_of(item)
        lines = geom.get("lines")
        if geom.get("kind") in {"scan_block", "scan_block_line"} and isinstance(
            lines, list
        ):
            for line in lines:
                if not isinstance(line, Mapping):
                    continue
                child = dict(item)
                merged_geom = dict(_display_geom_context(geom))
                merged_geom.update(dict(line))
                merged_geom.setdefault("kind", "scan_block_line")
                if "frame" not in merged_geom and geom.get("frame") is not None:
                    merged_geom["frame"] = geom.get("frame")
                if "unit" not in merged_geom and geom.get("unit") is not None:
                    merged_geom["unit"] = geom.get("unit")
                child["geometry"] = merged_geom
                child["_parent_item_uid"] = item.get("item_uid")
                child["item_uid"] = (
                    f"{item.get('item_uid') or 'scan_block'}:line:{line.get('line_index0', len(out))}"
                )
                child.setdefault("index0", item.get("index0"))
                out.append(child)
    return out


def _display_item_mode(item: Mapping[str, Any]) -> str:
    geom = _geom_of(item)
    return str(item.get("mode") or geom.get("mode") or "").upper()


def _int_or_none(value: Any) -> Optional[int]:
    try:
        if value is None:
            return None
        out = int(value)
    except Exception:
        return None
    return out


def _live_scan_line_state(snapshot: Mapping[str, Any]) -> Dict[str, Any]:
    """Return authoritative live scan-line state for display decisions.

    The observation-progress snapshot and antenna section status answer
    different questions.  ``geometry.current_line_index0`` from the former can
    advance while the telescope is still in OFF, standby, or accelerate.  Plan
    View must therefore use an antenna line as ``current`` only when the new
    AntennaSectionStatus says that the current-time section is an actual science
    line.
    """
    activity = (
        snapshot.get("activity")
        if isinstance(snapshot.get("activity"), Mapping)
        else {}
    )
    geometry = (
        snapshot.get("geometry")
        if isinstance(snapshot.get("geometry"), Mapping)
        else {}
    )

    basis = str(activity.get("control_status_basis") or "").lower()
    active = bool(activity.get("control_section_active", False))
    fresh = activity.get("control_section_fresh", True) is not False
    authoritative = (
        active
        and fresh
        and basis not in {"", "idle", "future-buffered", "no-current-section"}
    )

    section_line = _int_or_none(activity.get("control_section_line_index"))
    if section_line is None and authoritative:
        section_line = _int_or_none(geometry.get("current_line_index0"))

    progress_line = _int_or_none(geometry.get("progress_line_index0"))
    if progress_line is None and not authoritative:
        progress_line = _int_or_none(geometry.get("current_line_index0"))

    kind = str(
        activity.get("control_section_kind") or activity.get("motion_stage") or ""
    ).lower()
    science_line = bool(activity.get("control_section_science_line", False))
    line_active = science_line or kind in {"line", "scanning"}

    return {
        "authoritative": authoritative,
        "basis": basis,
        "kind": kind,
        "has_line": section_line is not None and section_line >= 0,
        "line_index": section_line,
        "progress_line_index": progress_line,
        "line_active": line_active,
    }


def _display_status_for_item(
    item: Mapping[str, Any],
    snapshot: Mapping[str, Any],
    events: Iterable[Mapping[str, Any]],
) -> str:
    plan = snapshot.get("plan") if isinstance(snapshot.get("plan"), Mapping) else {}
    lifecycle = (
        snapshot.get("lifecycle")
        if isinstance(snapshot.get("lifecycle"), Mapping)
        else {}
    )
    geometry = (
        snapshot.get("geometry")
        if isinstance(snapshot.get("geometry"), Mapping)
        else {}
    )
    cur = plan.get("item_uid")
    uid = item.get("item_uid")
    parent = item.get("_parent_item_uid")
    current_range = _index_range_from_mapping(plan)
    final_state = str(lifecycle.get("state") or "").lower() in FINAL_LIFECYCLE_STATES

    event_list = list(events or [])
    for event in event_list:
        if event.get("event") == "plan_item_finished" and event.get("item_uid") in {
            uid,
            parent,
        }:
            return "done"
    item_range = _index_range_from_mapping(item)
    for event in event_list:
        if event.get("event") != "plan_item_finished":
            continue
        event_range = _index_range_from_mapping(event)
        if (
            item_range is not None
            and event_range is not None
            and _ranges_overlap(item_range, event_range)
        ):
            return "done"

    try:
        live_line = int(geometry.get("current_line_index0"))
        item_line = int(_geom_of(item).get("line_index0", item.get("line_index0")))
        has_line = True
    except Exception:
        live_line = item_line = -1
        has_line = False
    if not final_state and cur and parent == cur and has_line:
        live_state = _live_scan_line_state(snapshot)
        if live_state.get("authoritative"):
            if live_state.get("has_line"):
                live_line = int(live_state["line_index"])
                if item_line < live_line:
                    return "done"
                if item_line == live_line:
                    return "current"
                return "pending"
            progress_line = live_state.get("progress_line_index")
            if isinstance(progress_line, int):
                if item_line < progress_line:
                    return "done"
                return "pending"
            # A live section exists but it is not a scan-line section.  Do not
            # fall back to marking every child of the active scan block as
            # current; that is the unstable Plan View behaviour seen on sky.
            return "pending"
        if item_line < live_line:
            return "done"
        if item_line == live_line:
            return "current"
        return "pending"

    overlaps_current = bool(
        current_range and item_range and _ranges_overlap(current_range, item_range)
    )
    if not final_state and (
        (cur and (uid == cur or parent == cur)) or overlaps_current
    ):
        return "current"
    return "pending"


def summarize_display_plan(
    snapshot: Optional[Mapping[str, Any]],
    full_plan: Optional[Mapping[str, Any]],
    events: Iterable[Mapping[str, Any]],
) -> Dict[str, Any]:
    """Build an observer-facing progress summary.

    Display should answer an observer's question first:
    "what observing unit am I in, and how many useful units remain?".
    Internal NECST item indices are retained as schedule_step, but they are not
    the primary progress unit for OTF/Grid/PSW/Skydip.
    """
    snap = snapshot if isinstance(snapshot, Mapping) else {}
    obs = (
        snap.get("observation") if isinstance(snap.get("observation"), Mapping) else {}
    )
    current_plan = snap.get("plan") if isinstance(snap.get("plan"), Mapping) else {}
    geom = snap.get("geometry") if isinstance(snap.get("geometry"), Mapping) else {}
    obs_type = str(obs.get("type") or "").lower()

    rows: List[Dict[str, Any]] = []
    for item in _flatten_plan_items_for_display(full_plan):
        g = _geom_of(item)
        kind = str(g.get("kind") or "")
        has_line = (
            kind in {"scan_line", "scan_block_line"}
            and g.get("start") is not None
            and g.get("stop") is not None
        )
        has_point = kind in {"point", "grid_point"} and (
            g.get("target") is not None
            or g.get("reference") is not None
            or g.get("offset") is not None
        )
        has_skydip = kind == "skydip_elevation" or (
            "sky" in obs_type
            and (
                g.get("target") is not None
                or g.get("el_deg") is not None
                or g.get("elevation_deg") is not None
            )
        )
        if not (has_line or has_point or has_skydip):
            continue
        mode = _display_item_mode(item) or ("ON" if has_line else "POINT")
        rows.append(
            {
                "item": item,
                "kind": kind,
                "has_line": has_line,
                "has_point": has_point,
                "has_skydip": has_skydip,
                "mode": mode,
                "status": _display_status_for_item(item, snap, events),
            }
        )

    line_rows = [row for row in rows if row["has_line"] and (row["mode"] in {"", "ON"})]
    if not line_rows:
        line_rows = [row for row in rows if row["has_line"]]
    point_rows = [row for row in rows if row["has_point"]]
    on_point_rows = [
        row for row in point_rows if row["mode"] == "ON" or row["kind"] == "grid_point"
    ]
    skydip_rows = [row for row in rows if row["has_skydip"]]

    def count_status(
        items: List[Dict[str, Any]],
    ) -> tuple[int, int, int, int, Optional[int]]:
        done = sum(1 for row in items if row["status"] == "done")
        cur = sum(1 for row in items if row["status"] == "current")
        rem = sum(1 for row in items if row["status"] == "pending")
        cur_pos: Optional[int] = None
        for idx, row in enumerate(items):
            if row["status"] == "current":
                cur_pos = idx + 1
                break
        return done, cur, rem, len(items), cur_pos

    schedule_step = "-"
    index0 = current_plan.get("index0")
    total = current_plan.get("total")
    index0_end = current_plan.get("index0_end")
    if isinstance(index0, int) and isinstance(total, int) and total > 0:
        if isinstance(index0_end, int) and index0_end >= index0:
            schedule_step = f"{index0 + 1}-{index0_end + 1}/{total}"
        else:
            schedule_step = f"{index0 + 1}/{total}"

    display_kind = "schedule"
    basis = "schedule item"
    progress_label = f"step {schedule_step}"
    current_unit_label = compact_value(current_plan.get("mode") or "-")
    completed_units: Any = "-"
    remaining_units: Any = "-"
    total_units: Any = "-"
    has_otf_lines = False
    current_line_label = "-"
    current_line_number: Optional[int] = None
    completed_lines = active_lines = remaining_lines = 0

    if line_rows:
        done, cur, rem, total_n, cur_pos = count_status(line_rows)
        display_kind = "OTF"
        basis = "ON scan line"
        has_otf_lines = True
        current_line_number = cur_pos
        current_line_label = f"{cur_pos if cur_pos is not None else '-'}/{total_n}"
        progress_label = f"OTF line {current_line_label}"
        current_unit_label = "scan line"
        completed_units, remaining_units, total_units = done, rem, total_n
        completed_lines, active_lines, remaining_lines = done, cur, rem
    elif "sky" in obs_type or skydip_rows:
        done, cur, rem, total_n, cur_pos = count_status(skydip_rows or rows)
        display_kind = "Skydip"
        basis = "elevation step"
        fallback_pos = index0 + 1 if isinstance(index0, int) else None
        step_pos = cur_pos if cur_pos is not None else fallback_pos
        progress_label = f"Skydip step {step_pos if step_pos is not None else '-'}/{total_n if total_n else '-'}"
        current_unit_label = "elevation"
        completed_units, remaining_units, total_units = done, rem, total_n
    elif "grid" in obs_type:
        basis_rows = on_point_rows or point_rows
        done, cur, rem, total_n, cur_pos = count_status(basis_rows)
        display_kind = "Grid"
        basis = "ON grid point"
        progress_label = f"Grid ON {cur_pos if cur_pos is not None else max(0, done)}/{total_n if total_n else '-'}"
        current_unit_label = compact_value(current_plan.get("mode") or "-")
        completed_units, remaining_units, total_units = done, rem, total_n
    elif "psw" in obs_type:
        basis_rows = on_point_rows or point_rows
        done, cur, rem, total_n, cur_pos = count_status(basis_rows)
        display_kind = "PSW"
        basis = "ON target visit"
        progress_label = f"PSW ON {cur_pos if cur_pos is not None else max(0, done)}/{total_n if total_n else '-'}"
        current_unit_label = compact_value(current_plan.get("mode") or "-")
        completed_units, remaining_units, total_units = done, rem, total_n
    elif point_rows:
        done, cur, rem, total_n, cur_pos = count_status(point_rows)
        display_kind = "Point"
        basis = "pointing item"
        progress_label = f"point {cur_pos if cur_pos is not None else schedule_step}/{total_n if total_n else '-'}"
        current_unit_label = compact_value(current_plan.get("mode") or "-")
        completed_units, remaining_units, total_units = done, rem, total_n

    return {
        "display_kind": display_kind,
        "basis": basis,
        "progress_label": progress_label,
        "current_unit_label": current_unit_label,
        "completed_units": completed_units,
        "remaining_units": remaining_units,
        "total_units": total_units,
        "has_otf_lines": has_otf_lines,
        "current_line_label": current_line_label,
        "current_line_number": current_line_number,
        "total_lines": total_units if has_otf_lines else 0,
        "completed_lines": completed_lines,
        "active_lines": active_lines,
        "remaining_lines": remaining_lines,
        "schedule_step": schedule_step,
        "mode": current_plan.get("mode") or "-",
        "role": current_plan.get("role") or "-",
        "target": observer_target_name(obs, geom),
        "label": current_plan.get("label") or "-",
    }


def _index_range_from_mapping(payload: Mapping[str, Any]) -> Optional[tuple[int, int]]:
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


def _ranges_overlap(a: tuple[int, int], b: tuple[int, int]) -> bool:
    return a[0] <= b[1] and b[0] <= a[1]


def _snapshot_plan_finished_by_events(
    plan: Mapping[str, Any], events: Iterable[Mapping[str, Any]]
) -> bool:
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
        if (
            uid_text is not None
            and ev_uid not in (None, "")
            and str(ev_uid) == uid_text
        ):
            return True
        ev_range = _index_range_from_mapping(event)
        if (
            plan_range is not None
            and ev_range is not None
            and _ranges_overlap(plan_range, ev_range)
        ):
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
            text = text[len(prefix) :]
            break
    if len(text) <= max_len:
        return text
    keep = max(4, (max_len - 1) // 2)
    tail = max(4, max_len - keep - 1)
    return f"{text[:keep]}…{text[-tail:]}"


def _coord_pair(value: Any) -> Optional[tuple[float, float]]:
    if isinstance(value, (list, tuple)) and len(value) >= 2:
        try:
            return float(value[0]), float(value[1])
        except Exception:
            return None
    return None


def _coord_frame(value: Any, default: str = "") -> str:
    if (
        isinstance(value, (list, tuple))
        and len(value) >= 3
        and value[2] not in (None, "")
    ):
        return str(value[2])
    return default


def _geom_offset_actual_arcsec(
    geom: Mapping[str, Any],
) -> Optional[tuple[float, float, str]]:
    value = geom.get("offset")
    pair = _coord_pair(value)
    if pair is None:
        return None
    unit = str(geom.get("offset_unit") or geom.get("unit") or "deg").lower()
    scale = 1.0 if "arcsec" in unit else 3600.0
    x, y = pair[0] * scale, pair[1] * scale
    frame = str(
        geom.get("offset_frame") or _coord_frame(value, str(geom.get("frame") or ""))
    )
    if bool(geom.get("cos_correction")):
        lat = None
        for key in ("reference", "target"):
            ref = geom.get(key)
            rpair = _coord_pair(ref)
            if rpair is not None:
                lat = rpair[1]
                break
        if lat is not None:
            x *= math.cos(math.radians(lat))
    return x, y, frame


def offset_arcsec_text(value: Any) -> str:
    pair = _coord_pair(value)
    if pair is None:
        return "-"
    frame = _coord_frame(value)
    suffix = f" {frame}" if frame else ""
    return f"({pair[0]*3600:.1f}, {pair[1]*3600:.1f}) arcsec{suffix}"


def geometry_offset_arcsec_text(geom: Mapping[str, Any]) -> str:
    actual = _geom_offset_actual_arcsec(geom)
    if actual is None:
        return offset_arcsec_text(geom.get("offset"))
    x, y, frame = actual
    suffix = f" {frame}" if frame else ""
    return f"({x:.1f}, {y:.1f}) arcsec{suffix}"


def az_unwrap_compact_text(antenna: Mapping[str, Any]) -> str:
    if not antenna or not antenna.get("az_unwrap_enabled"):
        return ""
    branch = antenna.get("az_unwrap_branch")
    raw = _finite_float(antenna.get("az_unwrap_raw_az_deg"))
    state = compact_value(antenna.get("az_unwrap_state"))
    if branch is None:
        return "unwrap ?"
    try:
        b = int(branch)
    except Exception:
        return f"unwrap {compact_value(branch)}"
    if b == 0:
        return "unwrap 0" if state in {"-", "ok"} else f"unwrap 0 {state}"
    raw_text = "" if raw is None else f" raw={raw:.3f}deg"
    state_text = "" if state in {"-", "ok"} else f" {state}"
    return f"unwrap {b:+d}{raw_text}{state_text}"


def azel_text(lon: Any, lat: Any, frame: Any = "altaz") -> str:
    try:
        x = float(lon)
        y = float(lat)
    except Exception:
        return "-"
    f = str(frame or "").lower()
    if f in {"altaz", "azel", "alt-az"}:
        return f"Az={x:.3f} El={y:.3f} deg"
    return f"lon={x:.3f} lat={y:.3f} {frame or ''}".strip()


def observer_target_name(obs: Mapping[str, Any], geom: Mapping[str, Any]) -> str:
    """Return the best user-facing target name available for progress display."""

    def usable(value: Any) -> Optional[str]:
        text = compact_value(value)
        if text in {"-", "None", "none", "unknown", "null"}:
            return None
        return text

    for value in (geom.get("target_name"), obs.get("target")):
        text = usable(value)
        if text:
            return text

    record = usable(obs.get("record_name"))
    if record:
        tokens = re.split(r"[_\-]+", record)
        skip = {
            "necst",
            "otf",
            "grid",
            "psw",
            "sky",
            "skydip",
            "rsky",
            "hot",
            "off",
            "on",
        }
        filtered = []
        for tok in tokens:
            low = tok.lower()
            if not tok or low in skip:
                continue
            if re.fullmatch(r"20\d{6}", tok) or re.fullmatch(r"\d{4,6}", tok):
                continue
            filtered.append(tok)
        if filtered:
            return "_".join(filtered[-3:])

    obsfile = usable(obs.get("obs_file"))
    if obsfile:
        stem = Path(obsfile).stem
        tokens = [t for t in re.split(r"[_\-]+", stem) if t]
        skip = {"otf", "grid", "psw", "sky", "skydip", "rsky", "obs"}
        filtered = [t for t in tokens if t.lower() not in skip]
        if filtered:
            return "_".join(filtered)
    return "-"


def render(
    snapshot: Optional[Dict[str, Any]],
    events: Iterable[Dict[str, Any]],
    *,
    compact: bool = False,
    full_plan: Optional[Mapping[str, Any]] = None,
) -> str:
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
    event_list = list(events)
    display_summary = summarize_display_plan(snapshot, full_plan, event_list)

    display_status = progress_display_status(lifecycle, plan, event_list)
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
        state_prefix = (
            state
            if status_label.lower() == str(lifecycle.get("state") or "").lower()
            else f"{state} status={status_label}"
        )
        eta_method = compact_eta_method(timing.get("remaining_method"), max_len=22)
        method_part = "" if eta_method == "-" else f" eta={eta_method}"
        progress_label = str(
            display_summary.get("progress_label") or f"step {index_label}"
        )
        if progress_label.startswith("OTF line "):
            progress_part = "line=" + progress_label.split("OTF line ", 1)[1]
        elif progress_label.startswith("Grid ON "):
            progress_part = "grid_on=" + progress_label.split("Grid ON ", 1)[1]
        elif progress_label.startswith("PSW ON "):
            progress_part = "psw_on=" + progress_label.split("PSW ON ", 1)[1]
        elif progress_label.startswith("Skydip step "):
            progress_part = "skydip=" + progress_label.split("Skydip step ", 1)[1]
        else:
            progress_part = progress_label.replace(" ", "=")
        counts_part = (
            f"done={display_summary.get('completed_units', '-')} "
            f"remain={display_summary.get('remaining_units', '-')} "
            f"basis={display_summary.get('basis', '-')}"
        )
        target = observer_target_name(obs, geom)
        target_part = "" if target == "-" else f" target={target}"
        return (
            f"{state_prefix} {obs_type} {progress_part} {phase} "
            f"elapsed={fmt_seconds_short(timing.get('elapsed_sec'))} "
            f"remain={fmt_seconds_short(timing.get('estimated_remaining_sec'), approx=True)} "
            f"conf={compact_value(timing.get('remaining_confidence'))}\n"
            f"drive={drive}:{motion} data={data_state}{target_part} "
            f"metadata={compact_value(data.get('expected_metadata_position'))} "
            f"id={compact_value(data.get('expected_metadata_id'))} {counts_part}{method_part}".rstrip()
        )

    lines = []

    def frame_line(content: str = "") -> str:
        return f"│ {str(content)[:58]:<58}│"

    lines.append("┌──────────────── NECST Observation Progress ────────────────┐")
    status_suffix = (
        ""
        if status_label.lower() == str(lifecycle.get("state") or "").lower()
        else f" status={status_label}"
    )
    lines.append(frame_line(f"state={(state + status_suffix)[:21]}  type={obs_type}"))
    lines.append(frame_line(f"record={record}"))
    obsfile = compact_value(obs.get("obs_file"))
    lines.append(frame_line(f"obsfile={obsfile}"))
    lines.append(
        frame_line(
            f"elapsed={fmt_seconds_short(timing.get('elapsed_sec'))} "
            f"remain={fmt_seconds_short(timing.get('estimated_remaining_sec'), approx=True)} "
            f"conf={compact_value(timing.get('remaining_confidence'))}"
        )
    )
    method = compact_eta_method(timing.get("remaining_method"), max_len=34)
    confidence = compact_value(timing.get("remaining_confidence"))
    if method != "-" or confidence != "-":
        lines.append(frame_line(f"eta_source={method} confidence={confidence}"))
    lines.append("├──────────────────── PLAN ──────────────────────────────────┤")
    label = compact_value(plan.get("label"))
    role = compact_value(plan.get("role"))
    obs_id = compact_value(plan.get("obs_id"))
    lines.append(
        frame_line(
            f"{display_summary.get('progress_label', 'step ' + index_label)}  "
            f"done={display_summary.get('completed_units', '-')}  "
            f"remaining={display_summary.get('remaining_units', '-')}"
        )
    )
    lines.append(
        frame_line(
            f"basis={display_summary.get('basis', 'schedule item')}  "
            f"current={display_summary.get('current_unit_label', phase)}  "
            f"schedule step={display_summary['schedule_step']}"
        )
    )
    lines.append(frame_line(f"phase={phase}  id={obs_id}  role(plan)={role}"))
    if label != "-":
        lines.append(frame_line(f"label={label}"))
    lines.append(f"│ {pct_bar(percent):<58}│")
    lines.append("├──────────────────── CURRENT ACTIVITY ──────────────────────┤")
    line_info = ""
    current_line = geom.get("current_line_index0")
    line_total = geom.get("line_total") or plan.get("line_total")
    if current_line is not None and line_total is not None:
        try:
            line_info = f" line={int(current_line) + 1}/{int(line_total)}"
        except Exception:
            line_info = (
                f" line={compact_value(current_line)}/{compact_value(line_total)}"
            )
    elif current_line is not None:
        line_info = f" line={compact_value(current_line)}"
    elif line_total is not None:
        line_info = f" line_total={line_total}"
    lines.append(frame_line(f"drive={drive} mot={motion} data={data_state}{line_info}"))
    if activity.get("control_section_fraction") is not None:
        frac = _finite_float(activity.get("control_section_fraction"))
        source = compact_value(activity.get("control_status_basis"))
        if frac is not None:
            lines.append(
                frame_line(
                    f"section: command_fraction={100.0*frac:.1f}% basis={source}"
                )
            )
    loc = activity.get("location_context")
    if loc:
        lines.append(frame_line(f"location_context={compact_value(loc)}"))
    lines.append("├──────────────────── COORDINATE ────────────────────────────┤")
    gkind = compact_value(geom.get("kind"))
    frame = compact_value(geom.get("frame"))
    if geom.get("start") is not None or geom.get("stop") is not None:
        lines.append(
            frame_line(
                f"plan : kind={gkind} frame={frame} start={compact_value(geom.get('start'))}"
            )
        )
        lines.append(frame_line(f"       stop={compact_value(geom.get('stop'))}"))
    elif geom.get("target") is not None:
        lines.append(
            frame_line(
                f"plan : kind={gkind} frame={frame} target={compact_value(geom.get('target'))}"
            )
        )
    elif geom:
        line_text = ""
        if (
            geom.get("current_line_index0") is not None
            or geom.get("line_total") is not None
        ):
            try:
                line_text = f" line={int(geom.get('current_line_index0')) + 1}/{int(geom.get('line_total'))}"
            except Exception:
                line_text = f" line={compact_value(geom.get('current_line_index0'))}/{compact_value(geom.get('line_total'))}"
        target_name = compact_value(geom.get("target_name"))
        extra = "" if target_name == "-" else f" target={target_name}"
        lines.append(frame_line(f"plan : kind={gkind} frame={frame}{line_text}{extra}"))
    else:
        lines.append("│ plan : -                                                     │")
    if geom.get("offset") is not None:
        label = "map offset(actual)" if geom.get("cos_correction") else "map offset"
        lines.append(frame_line(f"{label}={geometry_offset_arcsec_text(geom)}"))
    if geom.get("cos_correction") is not None:
        lines.append(
            frame_line(f"cos_correction={compact_value(geom.get('cos_correction'))}")
        )
    if antenna:
        cmd_lon = antenna.get("command_lon_deg")
        cmd_lat = antenna.get("command_lat_deg")
        enc_lon = antenna.get("encoder_lon_deg")
        enc_lat = antenna.get("encoder_lat_deg")
        if cmd_lon is not None or cmd_lat is not None:
            lines.append(
                frame_line(
                    f"cmd  : {azel_text(cmd_lon, cmd_lat, antenna.get('command_frame'))}"
                )
            )
        if enc_lon is not None or enc_lat is not None:
            lines.append(
                frame_line(
                    f"enc  : {azel_text(enc_lon, enc_lat, antenna.get('encoder_frame'))}"
                )
            )
        unwrap_text = az_unwrap_compact_text(antenna)
        if unwrap_text:
            lines.append(frame_line(f"az   : {unwrap_text}"))
        if antenna.get("tracking_status_available"):
            terr = _finite_float(antenna.get("tracking_error_deg"))
            age = _finite_float(antenna.get("tracking_status_age_sec"))
            ok_text = "OK" if antenna.get("tracking_ok") else "not OK"
            terr_text = "-" if terr is None else f"{terr * 3600.0:.1f} arcsec"
            age_text = "-" if age is None else f"age={age:.1f}s"
            basis = compact_value(antenna.get("pointing_basis"))
            lines.append(frame_line(f"trk  : {ok_text} error={terr_text} {age_text}"))
            if basis != "-":
                lines.append(frame_line(f"basis: {basis}"))
            cmd_age = _finite_float(antenna.get("command_age_sec"))
            enc_age = _finite_float(antenna.get("encoder_age_sec"))
            if cmd_age is not None or enc_age is not None:
                lines.append(
                    frame_line(
                        f"age  : cmd={fmt_seconds_short(cmd_age)} enc={fmt_seconds_short(enc_age)} "
                        f"stale={bool(antenna.get('command_stale'))}/{bool(antenna.get('encoder_stale'))}"
                    )
                )
        else:
            lines.append(frame_line("trk  : status unavailable"))
        if not (cmd_lon is not None or enc_lon is not None):
            lines.append(frame_line(f"ant  : {compact_value(antenna)}"))
    lines.append("├──────────────────── DATA ──────────────────────────────────┤")
    lines.append(
        frame_line(
            f"expected: {compact_value(data.get('expected_metadata_position'))} "
            f"id={compact_value(data.get('expected_metadata_id'))} "
            f"line={compact_value(data.get('expected_metadata_line_index'))}"
        )
    )
    queue = (
        snapshot.get("antenna_command_queue")
        if isinstance(snapshot.get("antenna_command_queue"), dict)
        else {}
    )
    if queue:
        q_state = "active" if queue.get("active") else "inactive"
        q_len = compact_value(queue.get("queue_length"))
        head = fmt_seconds_short(queue.get("head_lead_sec"))
        tail = fmt_seconds_short(queue.get("tail_lead_sec"))
        gap = fmt_seconds_short(queue.get("publish_gap_sec"))
        lines.append(
            frame_line(
                f"queue: {q_state} len={q_len} head/tail={head}/{tail} gap={gap}"
            )
        )
        if queue.get("guard_latched") or queue.get("reason"):
            lines.append(
                frame_line(
                    f"queue reason={compact_value(queue.get('reason'))} "
                    f"guard={compact_value(queue.get('guard_latched'))}"
                )
            )
    spec = (
        snapshot.get("spectrometer")
        if isinstance(snapshot.get("spectrometer"), dict)
        else {}
    )
    if spec:
        acq = "acq" if spec.get("acquiring") else "no-acq"
        rec = "rec" if spec.get("recorder_active") else "no-rec"
        save = "save" if spec.get("saving_enabled") else "no-save"
        age = compact_value(spec.get("last_dump_age_sec"))
        lines.append(
            frame_line(
                f"spectrometer: {acq}/{rec}/{save} dump_age={age} "
                f"streams={compact_value(spec.get('n_streams'))} boards={compact_value(spec.get('n_boards'))}"
            )
        )
    if data.get("latest_spectrum_position") is not None:
        lines.append(
            frame_line(
                f"latest  : {compact_value(data.get('latest_spectrum_position'))} "
                f"id={compact_value(data.get('latest_spectrum_id'))} "
                f"age={compact_value(data.get('latest_spectrum_age_sec'))}"
            )
        )
    lines.append("├──────────────────── RECENT EVENTS ─────────────────────────┤")
    event_lines = event_list[-8:] or snapshot.get("recent_events", [])[-8:]
    if not event_lines:
        lines.append(frame_line("-"))
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
        lines.append(frame_line(f"{t}  {body}"))
    lines.append("└─────────────────────────────────────────────────────────────┘")
    lines.append("Press Ctrl-C to quit.  Use --follow for scrolling event log.")
    return "\n".join(lines)


def cmd_once(args: argparse.Namespace) -> int:
    root = progress_root(args.root)
    live = LiveRosCache(enabled=not args.no_ros)
    try:
        live.spin_once(0.05)
        snapshot = apply_display_derivations(
            apply_dynamic_remaining(
                merge_live_telemetry(
                    read_json(current_snapshot_path(root)), live.snapshot()
                )
            )
        )
        if args.json:
            print(
                json.dumps(snapshot or {}, ensure_ascii=False, indent=2, sort_keys=True)
            )
            return 0
        events_path = latest_events_path(root, snapshot)
        events = read_recent_events(events_path, args.events)
        plan_path = latest_plan_path(root, snapshot)
        full_plan = read_json(plan_path) if plan_path else None
        print(render(snapshot, events, compact=args.compact, full_plan=full_plan))
        return 0
    finally:
        live.close()


def cmd_watch(args: argparse.Namespace) -> int:
    root = progress_root(args.root)
    live = LiveRosCache(enabled=not args.no_ros)
    try:
        while True:
            live.spin_once(0.0)
            snapshot = apply_display_derivations(
                apply_dynamic_remaining(
                    merge_live_telemetry(
                        read_json(current_snapshot_path(root)), live.snapshot()
                    )
                )
            )
            events_path = latest_events_path(root, snapshot)
            events = read_recent_events(events_path, args.events)
            plan_path = latest_plan_path(root, snapshot)
            full_plan = read_json(plan_path) if plan_path else None
            if args.no_clear:
                print(
                    render(snapshot, events, compact=args.compact, full_plan=full_plan)
                )
                print()
            else:
                sys.stdout.write("\x1b[H\x1b[2J")
                sys.stdout.write(
                    render(snapshot, events, compact=args.compact, full_plan=full_plan)
                )
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
                            print(
                                f"{t} {ev.get('event', 'event')} "
                                + " ".join(
                                    f"{k}={v}"
                                    for k, v in ev.items()
                                    if k not in {"seq", "event", "time_unix"}
                                )
                            )
            except FileNotFoundError:
                pass
            sys.stdout.flush()
            time.sleep(args.interval)
    except KeyboardInterrupt:
        return 0


def _json_response(
    handler: BaseHTTPRequestHandler, payload: Any, status: int = 200
) -> None:
    body = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True).encode(
        "utf-8"
    )
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json; charset=utf-8")
    handler.send_header("Cache-Control", "no-store")
    handler.send_header("Content-Length", str(len(body)))
    handler.end_headers()
    handler.wfile.write(body)


def _text_response(
    handler: BaseHTTPRequestHandler,
    body: str,
    *,
    content_type: str = "text/html; charset=utf-8",
    status: int = 200,
) -> None:
    data = body.encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", content_type)
    handler.send_header("Cache-Control", "no-store")
    handler.send_header("Content-Length", str(len(data)))
    handler.end_headers()
    handler.wfile.write(data)


def _int_query(
    query: Dict[str, List[str]],
    name: str,
    default: int,
    *,
    minimum: int = 0,
    maximum: int = 1000,
) -> int:
    try:
        value = int(query.get(name, [default])[0])
    except Exception:
        value = default
    return max(minimum, min(maximum, value))


def build_state(
    root: Path,
    *,
    events_limit: int = 12,
    live: Optional[LiveRosCache] = None,
    time_sync: Optional[TimeSyncPoller] = None,
) -> Dict[str, Any]:
    """Build the progress.py API state from the shared read-only status model."""
    if live is not None:
        live.spin_once(0.0)
    time_sync_snapshot = (time_sync.snapshot() if time_sync is not None else None) or {}
    state = status_model.build_progress_status_state(
        root,
        events_limit=events_limit,
        live_payload=live.snapshot() if live is not None else None,
        time_sync_payload=time_sync_snapshot,
    )
    server_time_unix = state["server_time_unix"]
    snapshot = apply_display_derivations(
        apply_dynamic_remaining(
            state["snapshot"] if isinstance(state.get("snapshot"), dict) else None
        ),
        server_time_unix=server_time_unix,
    )
    all_events = state.get("status_events") or []
    plan = state.get("plan") or {}
    state.update(
        {
            "ok": isinstance(snapshot, dict)
            and bool(snapshot)
            and not snapshot.get("_error"),
            "snapshot": snapshot or {},
            "rendered": render(
                snapshot if isinstance(snapshot, dict) else None,
                all_events,
                compact=True,
                full_plan=plan if isinstance(plan, dict) else None,
            ),
            "time_sync": time_sync_snapshot,
            "operator_status": status_model.build_operator_status(
                snapshot if isinstance(snapshot, dict) else None,
                plan=plan if isinstance(plan, dict) else None,
                events=state.get("events") or [],
                paths=state.get("paths") or {},
                server_time_unix=server_time_unix,
            ),
        }
    )
    return state


_HTML_TEMPLATE = """<!doctype html>
<html lang=\"en\">
<head>
<meta charset=\"utf-8\">
<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">
<title>NECST Observation Progress</title>
<style>
:root {
  color-scheme: light dark;
  --ok:#1a7f37; --warn:#b54708; --err:#c1121f;
  --bg:#ffffff; --fg:#1f2328; --panel:#ffffff; --muted:#606975; --border:#8c959f55; --plot-text:#1f2328;
  --grid-row-h: 12.8px; --grid-gap: .38rem;
}
@media (prefers-color-scheme: dark) {
  :root { --bg:#1b1b20; --fg:#eceff4; --panel:#202028; --muted:#aeb6c2; --border:#c8d0dc44; --plot-text:#eceff4; }
}
body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; margin: 0; padding: .42rem; line-height: 1.15; font-size:12.2px; background:var(--bg); color:var(--fg); -webkit-text-size-adjust:100%; text-size-adjust:100%; }
header { display:flex; gap:1rem; align-items:baseline; justify-content:space-between; flex-wrap:wrap; }
h1 { font-size:.98rem; margin:0 0 .18rem; }
.grid {
  display:grid;
  grid-template-columns: repeat(12, minmax(0, 1fr));
  grid-auto-flow: row dense;
  grid-auto-rows: var(--grid-row-h);
  gap: var(--grid-gap);
  align-items:stretch;
}
.card { border:1px solid var(--border); border-radius:8px; padding:.42rem .46rem; box-shadow:0 1px 4px #0001; background:var(--panel); min-height:0; height:100%; box-sizing:border-box; position:relative; overflow:auto; overscroll-behavior:contain; font-size:var(--card-font-size, 12.2px); }
.card-content { --card-visual-scale:1; transform-origin:top left; }
.card h2 { font-size:1.10em; margin:.01rem 0 .22rem; letter-spacing:.01em; padding-right:3.9rem; }
.kv { display:grid; grid-template-columns: 5.9rem minmax(0,1fr); gap:.06rem .28rem; align-items:baseline; }
.obsview .kv { grid-template-columns: 5.8rem minmax(0,1fr); }
.planinfoview .kv, .activityview .kv { grid-template-columns: 4.7rem minmax(0,1fr); }
.envview .kv { grid-template-columns: 4.9rem minmax(0,1fr); }
.geometryview .kv { grid-template-columns: 5.0rem minmax(0,1fr); }
.queueview .kv { grid-template-columns: 5.9rem minmax(0,1fr); }
.spectrometerview .kv { grid-template-columns: 4.6rem minmax(0,1fr); }
.traceview .kv { grid-template-columns: 4.6rem minmax(0,1fr); }
.bigpos { grid-template-columns: 6.5rem minmax(0,1fr); font-size:1.05em; }
.bigpos .v.important-pos { font-size:1.35em; font-weight:700; }
.v.ok-pos { color:var(--ok); font-weight:700; }
.v.important-pos { font-weight:700; }
.v.warning-pos { color:var(--warn); font-weight:800; }
.v.critical-pos { color:var(--err); font-weight:800; }
.v.muted-pos { color:var(--muted); }
.k { color:var(--muted); }
.v { min-width:0; overflow-wrap:break-word; word-break:normal; }
.traceview .v, .eventview .v { overflow-wrap:anywhere; }
#trace .k, #trace .v { font-size:1.00em; line-height:1.05; }
#paths .v { white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }
.filesview .kv { grid-template-columns:4.2rem minmax(0,1fr); }
#paths .k, #paths .v { font-size:.84em; line-height:1.05; }
.status { font-weight:700; padding:.08rem .38rem; border-radius:999px; border:1px solid var(--border); }
.status.running, .status.finished { color:var(--ok); } .status.error, .status.aborted { color:var(--err); } .status.cleanup { color:var(--warn); }
.bar { width:100%; height:.86rem; border:1px solid var(--border); border-radius:999px; overflow:hidden; background:#9992; margin-top:.35rem; }
.fill { height:100%; width:0%; background:currentColor; color:var(--ok); transition:width .2s; }
pre { white-space:pre-wrap; overflow:auto; max-height:7.2rem; margin:.12rem 0 0; font-size:.91em; }
table { border-collapse:collapse; width:100%; font-size:.91em; table-layout:fixed; }
th, td { border-bottom:1px solid var(--border); text-align:left; padding:.10rem .13rem; vertical-align:top; overflow:hidden; text-overflow:ellipsis; }
#events { overflow:auto; max-height:7.2rem; }
#events table { table-layout:auto; }
#events th:nth-child(1), #events td:nth-child(1) { width:4.2rem; white-space:nowrap; }
#events th:nth-child(2), #events td:nth-child(2) { width:7.6rem; }
#events th:nth-child(3), #events td:nth-child(3) { width:auto; }
.plotbox { min-height: 210px; display:flex; align-items:center; justify-content:center; }
.plotbox svg { width:100%; height:100%; max-height:none; color:var(--plot-text); }
.plotbox svg text { fill: currentColor; paint-order: stroke; stroke: var(--bg); stroke-width: 3px; stroke-linejoin: round; }
.legend { display:flex; gap:.45rem; flex-wrap:wrap; margin:.08rem 0 .14rem; font-size:.92em; color:var(--muted); }
.legend-note { flex-basis:auto; font-size:.89em; }
.dot { display:inline-block; width:.62rem; height:.62rem; border-radius:999px; vertical-align:-.06rem; margin-right:.18rem; }
.small { font-size:.70rem; color:var(--muted); }
.card .small { font-size:.92em; }
.queueview, .spectrometerview, .traceview, .envview, .terminalview, .filesview, .eventview { min-height: 0; }
.planview { min-height:0; }
.planview .plotbox { min-height: 0; height: calc(100% - 3.0rem); }
.planview svg { max-height:none; }
#terminal { max-height: none; }
.axis-label { font-size:var(--card-plot-font-size, 11px); fill:currentColor; }
.plot-note { font-size:var(--card-plot-font-size, 11px); fill:currentColor; }
.important { font-weight:700; color:var(--warn); }
.critical { color:var(--err); font-weight:800; }
.warning { margin:.25rem 0 .5rem; padding:.5rem .65rem; border:2px solid var(--err); border-radius:9px; background:#c1121f22; color:var(--fg); font-size:.98rem; font-weight:750; }
.notice { margin:.25rem 0 .5rem; padding:.42rem .55rem; border:1px solid var(--border); border-radius:9px; background:#9991; }
@media (min-width: 1500px) {
  .planview .plotbox { min-height: 200px; }
  .planview svg { max-height:none; }
}
@media (max-width: 1100px) {
  .grid { grid-template-columns: repeat(6, minmax(0, 1fr)); }
  .planview .plotbox { min-height: 210px; }
  .planview svg { max-height:none; }
}
@media (max-width: 1100px) {
  body { padding: .45rem; }
  /* Narrow browser windows must fall back to a normal vertical dashboard.
     Keep user-defined order via flexbox order, but ignore grid row/column spans.
     Without this, grid-auto-rows makes each auto-placed card collapse to one
     tiny row when the viewport becomes narrow. */
  .grid { display:flex; flex-direction:column; gap:.45rem; grid-template-columns:none; grid-auto-rows:auto; }
  .card[data-card-id] { grid-column:auto !important; grid-row:auto !important; min-height:auto !important; height:auto !important; overflow:visible; }
  .plotbox { min-height: 260px; }
  .planview .plotbox { height:min(72vw, 430px) !important; min-height:270px; }
  .planview svg { max-height:none; }
  .kv, .bigpos, .obsview .kv, .planinfoview .kv, .activityview .kv { grid-template-columns: 7.0rem minmax(0, 1fr); }
}
body.narrow-layout .grid { display:flex; flex-direction:column; gap:.45rem; grid-template-columns:none; grid-auto-rows:auto; }
body.narrow-layout .card[data-card-id] { grid-column:auto !important; grid-row:auto !important; min-height:auto !important; height:auto !important; overflow:visible; }
body.narrow-layout .plotbox { min-height:260px; }
body.narrow-layout .planview .plotbox { height:min(72vw, 430px) !important; min-height:270px; }
body.narrow-layout .kv, body.narrow-layout .bigpos, body.narrow-layout .obsview .kv, body.narrow-layout .planinfoview .kv, body.narrow-layout .activityview .kv { grid-template-columns:7.0rem minmax(0, 1fr); }

.header-tools { display:flex; align-items:center; gap:.55rem; flex-wrap:wrap; justify-content:flex-end; }
.layout-tools { display:inline-flex; align-items:center; gap:.22rem; }
.layout-btn { font:inherit; font-size:.70rem; line-height:1.05; padding:.16rem .36rem; border:1px solid var(--border); border-radius:999px; background:var(--panel); color:var(--fg); cursor:pointer; }
.layout-btn:hover { background:#9992; }
body.layout-unlocked .grid { background-image: linear-gradient(to right, #ff7f0e22 1px, transparent 1px), linear-gradient(to bottom, #ff7f0e18 1px, transparent 1px); background-size: calc((100% - 11*var(--grid-gap))/12 + var(--grid-gap)) calc(var(--grid-row-h) + var(--grid-gap)); }
body.layout-unlocked .card { outline:1px dashed #ff7f0e88; cursor:grab; }
body.layout-unlocked .card h2::after { content:"  drag / resize / font"; font-size:.64rem; color:var(--warn); font-weight:500; }
body.layout-unlocked .card.dragging { opacity:.55; cursor:grabbing; }
body.layout-unlocked .card.drag-over { outline:2px solid #ff7f0e; }
body.layout-unlocked .plotbox, body.layout-unlocked pre, body.layout-unlocked table { pointer-events:none; }
.card-font-tools { display:none; position:absolute; right:.30rem; top:.25rem; align-items:center; gap:.10rem; z-index:4; font-size:10px; line-height:1; touch-action:manipulation; }
body.layout-unlocked .card-font-tools, .card:hover .card-font-tools { display:flex; }
.card-font-btn, .card-font-reset { font:inherit; line-height:1; padding:.10rem .18rem; border:1px solid var(--border); border-radius:999px; background:var(--panel); color:var(--fg); cursor:pointer; opacity:.92; }
.card-font-btn:hover, .card-font-reset:hover { background:#9992; }
.card-font-reset { min-width:2.3rem; text-align:center; color:var(--muted); }
body.safari-browser .card-font-reset { min-width:3.9rem; }
body.safari-browser .card.safari-density-dense { padding:.32rem .34rem; }
body.safari-browser .card.safari-density-compact { padding:.37rem .40rem; }
body.safari-browser .card.safari-density-large { padding:.46rem .50rem; }
body.safari-browser .card.safari-density-xlarge { padding:.52rem .56rem; }
body.safari-browser .card.safari-density-dense .card-content { line-height:1.04; }
body.safari-browser .card.safari-density-compact .card-content { line-height:1.08; }
body.safari-browser .card.safari-density-large .card-content { line-height:1.20; }
body.safari-browser .card.safari-density-xlarge .card-content { line-height:1.24; }
body.safari-browser .card.safari-density-dense h2 { margin:0 0 .10rem; }
body.safari-browser .card.safari-density-compact h2 { margin:0 0 .16rem; }
body.safari-browser .card.safari-density-large h2 { margin:.02rem 0 .28rem; }
body.safari-browser .card.safari-density-xlarge h2 { margin:.03rem 0 .34rem; }
body.safari-browser .card.safari-density-dense .kv { gap:.025rem .18rem; }
body.safari-browser .card.safari-density-compact .kv { gap:.04rem .23rem; }
body.safari-browser .card.safari-density-large .kv { gap:.08rem .32rem; }
body.safari-browser .card.safari-density-xlarge .kv { gap:.10rem .38rem; }
body.safari-browser .card.safari-density-dense th, body.safari-browser .card.safari-density-dense td { padding:.055rem .075rem; }
body.safari-browser .card.safari-density-compact th, body.safari-browser .card.safari-density-compact td { padding:.075rem .10rem; }
body.safari-browser .card.safari-density-large th, body.safari-browser .card.safari-density-large td { padding:.13rem .16rem; }
body.safari-browser .card.safari-density-xlarge th, body.safari-browser .card.safari-density-xlarge td { padding:.16rem .19rem; }
body.layout-unlocked .card-font-tools { pointer-events:auto; }
.resize-grip { display:none; position:absolute; right:2px; bottom:2px; width:15px; height:15px; border-right:2px solid var(--warn); border-bottom:2px solid var(--warn); opacity:.75; cursor:nwse-resize; border-radius:2px; }
body.layout-unlocked .resize-grip { display:block; pointer-events:auto; }
body.layout-unlocked .card.resizing { outline:2px solid var(--warn); cursor:nwse-resize; }
.err { color:var(--err); font-weight:700; }
.unwrap-badges{display:flex;flex-wrap:wrap;gap:0.25rem;margin-top:0.15rem}.nowrap{white-space:nowrap}
</style>
</head>
<body>
<header><h1>NECST Observation Progress</h1><div class="header-tools small"><span>Auto refresh: __REFRESH_MS__ ms</span><span class="layout-tools"><button type="button" id="layoutRowMinus" class="layout-btn" title="Make grid rows shorter">Row -</button><button type="button" id="layoutRowPlus" class="layout-btn" title="Make grid rows taller">Row +</button><button type="button" id="layoutToggle" class="layout-btn" title="Unlock card layout editing">Layout: locked</button><button type="button" id="layoutReset" class="layout-btn" title="Restore the standard card order and grid sizes">Reset layout</button></span><a href="/api/state">JSON</a></div></header>
<div id=\"message\" class=\"small\">Loading...</div>
<div class=\"grid\">
<section class="card obsview" data-card-id="observation"><h2>Observation</h2><div class=\"kv\" id=\"observation\"></div></section>
<section class="card positionview" data-card-id="position"><h2>Live Position</h2><div class="kv bigpos" id="position"></div></section>
<section class="card planinfoview" data-card-id="plan"><h2>Plan</h2><div class=\"kv\" id=\"plan\"></div><div class=\"bar\"><div id=\"bar\" class=\"fill\"></div></div></section>
<section class="card activityview" data-card-id="activity"><h2>Activity</h2><div class=\"kv\" id=\"activity\"></div></section>
<section class="card geometryview" data-card-id="geometry"><h2>Geometry</h2><div class="kv" id="geometry"></div></section>
<section class="card planview" data-card-id="planview"><h2>Plan View</h2><div class="legend"><span><i class="dot" style="background:#2ca02c"></i>done</span><span><i class="dot" style="background:#ff7f0e"></i>current</span><span><i class="dot" style="background:#bdbdbd"></i>pending</span><span class="legend-note">OTF lines / ON visits</span></div><div id="plotview" class="plotbox small">-</div></section>
<section class="card traceview" data-card-id="trace"><h2>System Trace</h2><div class="kv" id="trace"></div></section>
<section class="card queueview" data-card-id="queue"><h2>Command Queue</h2><div class="kv" id="queue"></div></section>
<section class="card spectrometerview" data-card-id="spectrometer"><h2>Spectrometer</h2><div class="kv" id="spectrometer"></div></section>
<section class="card envview" data-card-id="environment"><h2>Environment</h2><div class="kv" id="environment"></div></section>
<section class="card terminalview" data-card-id="terminal"><h2>Terminal View</h2><pre id=\"terminal\"></pre></section>
<section class="card filesview" data-card-id="files"><h2>Files</h2><div class=\"kv\" id=\"paths\"></div></section>
<section class="card eventview" data-card-id="events"><h2>Recent Events</h2><div id="events"></div></section>
</div>
<script>
const refreshMs = __REFRESH_MS__;
const layoutStorageKey = 'necst-progress-card-layout-v52';
const oldLayoutStorageKeys = ['necst-progress-card-layout-v51', 'necst-progress-card-layout-v50', 'necst-progress-card-layout-v49', 'necst-progress-card-layout-v48', 'necst-progress-card-layout-v47', 'necst-progress-card-layout-v46', 'necst-progress-card-layout-v45', 'necst-progress-card-layout-v44', 'necst-progress-card-layout-v43', 'necst-progress-card-layout-v42'];
const defaultCardLayout = [
  {id:'position', col:3, row:12},
  {id:'observation', col:3, row:12},
  {id:'activity', col:2, row:12},
  {id:'geometry', col:2, row:12},
  {id:'environment', col:2, row:12},
  {id:'planview', col:5, row:20},
  {id:'plan', col:2, row:20},
  {id:'queue', col:2, row:20},
  {id:'spectrometer', col:3, row:20},
  {id:'trace', col:3, row:7},
  {id:'terminal', col:3, row:7},
  {id:'files', col:3, row:7},
  {id:'events', col:3, row:7}
];
const defaultLayoutCards = defaultCardLayout.map(x => x.id);
let layoutUnlocked = false;
let dragCardId = null;
let activeResize = null;
const defaultGridRowPx = 12.8;
const minGridRowPx = 8.0;
const maxGridRowPx = 72.0;
const gridRowStepPx = 1.0;
const defaultCardFontScale = 1.0;
const minCardFontScale = 0.70;
const maxCardFontScale = 1.40;
const cardFontScaleStep = 0.05;
const supportsCssZoom = (() => { try { return !!(window.CSS && CSS.supports && CSS.supports('zoom', '1')); } catch (_) { return false; } })();
const isSafariBrowser = (() => {
  const ua = String(navigator.userAgent || '');
  const vendor = String(navigator.vendor || '');
  return /Safari/i.test(ua) && /Apple/i.test(vendor) && !/(Chrome|Chromium|CriOS|FxiOS|Edg|OPR|Android)/i.test(ua);
})();
const safariCardFontModes = [
  {label:'Dense', scale:0.78, visual:0.86, className:'safari-density-dense', plotPx:9.5},
  {label:'Compact', scale:0.90, visual:0.94, className:'safari-density-compact', plotPx:10.2},
  {label:'Normal', scale:1.00, visual:1.00, className:'safari-density-normal', plotPx:11.0},
  {label:'Large', scale:1.16, visual:1.12, className:'safari-density-large', plotPx:12.2},
  {label:'XL', scale:1.32, visual:1.24, className:'safari-density-xlarge', plotPx:13.2}
];
const safariDensityClasses = safariCardFontModes.map(m => m.className);
const narrowLayoutThresholdPx = 1100;
function clampNumber(x, lo, hi) {
  const n = Number(x);
  if (!Number.isFinite(n)) return lo;
  return Math.max(lo, Math.min(hi, n));
}
function safariFontModeForScale(scale) {
  const value = clampNumber(scale, minCardFontScale, maxCardFontScale);
  let best = safariCardFontModes[0];
  let bestDiff = Infinity;
  for (const mode of safariCardFontModes) {
    const diff = Math.abs(value - mode.scale);
    if (diff < bestDiff) { best = mode; bestDiff = diff; }
  }
  return best;
}
function safariNextFontScale(current, direction) {
  const mode = safariFontModeForScale(current);
  const index = safariCardFontModes.findIndex(m => m.label === mode.label);
  const nextIndex = clampNumber(index + direction, 0, safariCardFontModes.length - 1);
  return safariCardFontModes[nextIndex].scale;
}
function cardFontReadout(scale) {
  return isSafariBrowser ? safariFontModeForScale(scale).label : `${Math.round(clampNumber(scale, minCardFontScale, maxCardFontScale) * 100)}%`;
}
function applyGridRowHeight(rowPx) {
  const px = clampNumber(rowPx, minGridRowPx, maxGridRowPx);
  document.documentElement.style.setProperty('--grid-row-h', `${px}px`);
  return px;
}
function viewportWidthForLayout() {
  const visual = window.visualViewport && Number.isFinite(window.visualViewport.width) ? window.visualViewport.width : Infinity;
  const inner = Number.isFinite(window.innerWidth) ? window.innerWidth : Infinity;
  const client = document.documentElement && Number.isFinite(document.documentElement.clientWidth) ? document.documentElement.clientWidth : Infinity;
  return Math.min(visual, inner, client);
}
function updateNarrowLayoutClass() {
  const width = viewportWidthForLayout();
  document.body.classList.toggle('narrow-layout', width <= narrowLayoutThresholdPx);
}
function gridColumnCount() {
  const grid = document.querySelector('.grid');
  if (!grid) return 12;
  const text = getComputedStyle(grid).gridTemplateColumns || '';
  const count = text.trim().split(/\\s+/).filter(Boolean).length;
  return Math.max(1, count || 12);
}
function validLayoutOrder(order) {
  if (!Array.isArray(order) || order.length !== defaultLayoutCards.length) return null;
  const seen = new Set(order);
  if (seen.size !== defaultLayoutCards.length) return null;
  for (const id of defaultLayoutCards) if (!seen.has(id)) return null;
  return order.slice();
}
function defaultLayoutState() {
  const sizes = {};
  const fontScales = {};
  for (const spec of defaultCardLayout) {
    sizes[spec.id] = {col: spec.col, row: spec.row};
    fontScales[spec.id] = defaultCardFontScale;
  }
  return {order: defaultLayoutCards.slice(), sizes, rowPx: defaultGridRowPx, fontScales};
}
function normalizeLayoutState(raw) {
  const def = defaultLayoutState();
  if (Array.isArray(raw)) {
    const order = validLayoutOrder(raw);
    return order ? {order, sizes: def.sizes, rowPx: def.rowPx, fontScales: def.fontScales} : def;
  }
  if (!raw || typeof raw !== 'object') return def;
  const order = validLayoutOrder(raw.order) || def.order;
  const sizes = {};
  for (const spec of defaultCardLayout) {
    const src = raw.sizes && typeof raw.sizes === 'object' ? raw.sizes[spec.id] : null;
    sizes[spec.id] = {
      col: clampNumber(src?.col ?? spec.col, 1, 12),
      row: clampNumber(src?.row ?? Math.round((src?.minHeight ?? spec.row * 22) / 22) ?? spec.row, 3, 60)
    };
  }
  const fontScales = {};
  for (const spec of defaultCardLayout) {
    const rawFonts = raw.fontScales && typeof raw.fontScales === 'object' ? raw.fontScales : {};
    fontScales[spec.id] = clampNumber(rawFonts[spec.id] ?? def.fontScales[spec.id], minCardFontScale, maxCardFontScale);
  }
  const rowPx = clampNumber(raw.rowPx ?? raw.gridRowPx ?? def.rowPx, minGridRowPx, maxGridRowPx);
  return {order, sizes, rowPx, fontScales};
}
function readLayoutState() {
  try {
    const raw = JSON.parse(localStorage.getItem(layoutStorageKey) || 'null');
    if (raw) return normalizeLayoutState(raw);
  } catch (_) {}
  return defaultLayoutState();
}
function writeLayoutState(state) {
  const valid = normalizeLayoutState(state);
  try { localStorage.setItem(layoutStorageKey, JSON.stringify(valid)); } catch (_) {}
}
function setCardDraggable(card, enabled) {
  card.draggable = !!enabled;
  card.setAttribute('aria-grabbed', enabled ? 'false' : 'false');
}
function ensureCardContentWrapper(card) {
  if (!card) return null;
  const existing = directChildByClass(card, 'card-content');
  if (existing) return existing;
  const wrapper = document.createElement('div');
  wrapper.className = 'card-content';
  const children = Array.from(card.childNodes);
  for (const child of children) {
    if (child.nodeType === 1 && child.classList && (child.classList.contains('card-font-tools') || child.classList.contains('resize-grip'))) continue;
    wrapper.appendChild(child);
  }
  card.insertBefore(wrapper, card.firstChild);
  return wrapper;
}
function applyCardVisualScale(card, fontScale) {
  if (!card) return;
  const requestedScale = clampNumber(fontScale, minCardFontScale, maxCardFontScale);
  const content = ensureCardContentWrapper(card);
  card.classList.remove(...safariDensityClasses);
  card.dataset.fontScale = String(requestedScale);
  card.style.setProperty('--card-font-scale', String(requestedScale));
  card.style.setProperty('--card-font-size', '12.2px');
  card.style.setProperty('--card-plot-font-size', '11px');
  card.style.fontSize = '12.2px';
  if (!content) return;
  content.style.transformOrigin = 'top left';
  if (isSafariBrowser) {
    // Safari showed non-linear behavior with continuous font-size/zoom scaling
    // (for example, 70% becoming much smaller while nearby values barely changed).
    // Keep Chrome/Firefox unchanged, but use discrete density modes and transform
    // scaling only for Safari so the effect is visible and stable.
    const mode = safariFontModeForScale(requestedScale);
    card.classList.add(mode.className);
    card.dataset.fontMode = mode.label;
    card.style.setProperty('--card-plot-font-size', `${mode.plotPx}px`);
    content.style.setProperty('--card-visual-scale', String(mode.visual));
    content.style.zoom = '';
    content.style.transform = mode.visual === 1 ? '' : `scale(${mode.visual})`;
    content.style.width = mode.visual === 1 ? '' : `${(100 / Math.max(0.01, mode.visual)).toFixed(3)}%`;
    return;
  }
  delete card.dataset.fontMode;
  content.style.setProperty('--card-visual-scale', String(requestedScale));
  if (supportsCssZoom) {
    content.style.zoom = String(requestedScale);
    content.style.transform = '';
    content.style.width = '';
  } else {
    content.style.zoom = '';
    content.style.transform = `scale(${requestedScale})`;
    content.style.width = `${(100 / Math.max(0.01, requestedScale)).toFixed(3)}%`;
  }
}
function applyLayoutState(state) {
  const valid = normalizeLayoutState(state);
  applyGridRowHeight(valid.rowPx);
  updateNarrowLayoutClass();
  const maxCols = gridColumnCount();
  valid.order.forEach((cardId, index) => {
    const el = document.querySelector(`[data-card-id="${cardId}"]`);
    if (!el) return;
    ensureCardContentWrapper(el);
    const size = valid.sizes[cardId] || {col: 3, row: 8};
    const storedCol = clampNumber(size.col, 1, 12);
    const col = clampNumber(storedCol, 1, Math.max(1, maxCols));
    const row = clampNumber(size.row ?? 8, 3, 60);
    el.style.order = String(index);
    el.style.gridColumn = `span ${col}`;
    el.style.gridRow = `span ${row}`;
    el.dataset.layoutCol = String(storedCol);
    el.dataset.layoutRow = String(row);
    const fontScale = clampNumber(valid.fontScales?.[cardId] ?? defaultCardFontScale, minCardFontScale, maxCardFontScale);
    applyCardVisualScale(el, fontScale);
    const fontReadout = directChildByClass(el, 'card-font-tools')?.querySelector('.card-font-reset');
    if (fontReadout) fontReadout.textContent = cardFontReadout(fontScale);
    el.style.minHeight = '';
    delete el.dataset.layoutMinHeight;
  });
}
function currentLayoutState() {
  const def = defaultLayoutState();
  const cards = Array.from(document.querySelectorAll('.card[data-card-id]'));
  cards.sort((a, b) => Number(a.style.order || 0) - Number(b.style.order || 0));
  const order = validLayoutOrder(cards.map(c => c.dataset.cardId)) || readLayoutState().order || def.order;
  const sizes = {};
  const fontScales = {};
  for (const spec of defaultCardLayout) {
    const card = document.querySelector(`[data-card-id="${spec.id}"]`);
    sizes[spec.id] = {
      col: clampNumber(card?.dataset.layoutCol ?? spec.col, 1, 12),
      row: clampNumber(card?.dataset.layoutRow ?? spec.row ?? 8, 3, 60)
    };
    fontScales[spec.id] = clampNumber(card?.dataset.fontScale ?? def.fontScales[spec.id], minCardFontScale, maxCardFontScale);
  }
  const cssRow = parseFloat(getComputedStyle(document.documentElement).getPropertyValue('--grid-row-h'));
  const rowPx = clampNumber(cssRow || def.rowPx, minGridRowPx, maxGridRowPx);
  return {order, sizes, rowPx, fontScales};
}
function moveCardInOrder(order, fromId, toId) {
  const next = order.slice();
  const from = next.indexOf(fromId);
  const to = next.indexOf(toId);
  if (from < 0 || to < 0 || from === to) return next;
  const [item] = next.splice(from, 1);
  next.splice(to, 0, item);
  return next;
}
function saveAndApplyLayout(state) {
  const next = normalizeLayoutState(state);
  applyLayoutState(next);
  writeLayoutState(next);
}
function setLayoutUnlocked(enabled) {
  layoutUnlocked = !!enabled;
  document.body.classList.toggle('layout-unlocked', layoutUnlocked);
  for (const card of document.querySelectorAll('.card[data-card-id]')) setCardDraggable(card, layoutUnlocked);
  const btn = document.getElementById('layoutToggle');
  if (btn) btn.textContent = layoutUnlocked ? 'Layout: unlocked' : 'Layout: locked';
}
function directChildByClass(parent, className) {
  if (!parent || !parent.children) return null;
  for (const child of parent.children) {
    if (child.classList && child.classList.contains(className)) return child;
  }
  return null;
}
function ensureResizeGrip(card) {
  if (directChildByClass(card, 'resize-grip')) return;
  const grip = document.createElement('div');
  grip.className = 'resize-grip';
  grip.title = 'Drag to resize this card in grid columns/rows; Reset layout restores defaults';
  grip.addEventListener('pointerdown', (event) => {
    if (!layoutUnlocked) return;
    event.preventDefault();
    event.stopPropagation();
    const id = card.dataset.cardId;
    const state = currentLayoutState();
    const size = state.sizes[id] || {col: 3, row: 8};
    const grid = document.querySelector('.grid');
    const gridRect = grid ? grid.getBoundingClientRect() : {width: 1200};
    const style = grid ? getComputedStyle(grid) : null;
    const colWidth = Math.max(24, gridRect.width / Math.max(1, gridColumnCount()));
    const rowHeight = style ? parseFloat(style.gridAutoRows) || 18 : 18;
    const rowGap = style ? parseFloat(style.rowGap) || 0 : 0;
    const rowPitch = Math.max(8, rowHeight + rowGap);
    activeResize = {
      id,
      startX: event.clientX,
      startY: event.clientY,
      startCol: clampNumber(size.col, 1, 12),
      startRow: clampNumber(size.row ?? 8, 3, 60),
      colWidth,
      rowPitch,
      state
    };
    card.classList.add('resizing');
    try { card.setPointerCapture(event.pointerId); } catch (_) {}
  });
  card.addEventListener('pointermove', (event) => {
    if (!activeResize || activeResize.id !== card.dataset.cardId) return;
    event.preventDefault();
    const maxCols = gridColumnCount();
    const dx = event.clientX - activeResize.startX;
    const dy = event.clientY - activeResize.startY;
    const nextCol = clampNumber(activeResize.startCol + Math.round(dx / activeResize.colWidth), 1, Math.max(1, maxCols));
    const nextRow = clampNumber(activeResize.startRow + Math.round(dy / activeResize.rowPitch), 3, 60);
    const st = activeResize.state;
    st.sizes[activeResize.id] = {col: nextCol, row: nextRow};
    applyLayoutState(st);
  });
  const finish = () => {
    if (!activeResize || activeResize.id !== card.dataset.cardId) return;
    card.classList.remove('resizing');
    writeLayoutState(currentLayoutState());
    activeResize = null;
  };
  card.addEventListener('pointerup', finish);
  card.addEventListener('pointercancel', finish);
  grip.addEventListener('dragstart', (event) => event.preventDefault());
  card.appendChild(grip);
}
function setCardFontScale(card, scale) {
  if (!card || !card.dataset.cardId) return;
  const state = currentLayoutState();
  const id = card.dataset.cardId;
  state.fontScales[id] = clampNumber(scale, minCardFontScale, maxCardFontScale);
  saveAndApplyLayout(state);
}
function ensureCardFontControls(card) {
  if (directChildByClass(card, 'card-font-tools')) return;
  const tools = document.createElement('div');
  tools.className = 'card-font-tools';
  tools.title = 'Per-card visual text scale. Values are saved in this browser; Reset layout restores defaults.';
  const dec = document.createElement('button');
  dec.type = 'button';
  dec.className = 'card-font-btn';
  dec.textContent = 'A−';
  dec.title = 'Make this card font smaller';
  const reset = document.createElement('button');
  reset.type = 'button';
  reset.className = 'card-font-reset';
  reset.textContent = '100%';
  reset.title = 'Reset this card font size';
  const inc = document.createElement('button');
  inc.type = 'button';
  inc.className = 'card-font-btn';
  inc.textContent = 'A+';
  inc.title = 'Make this card font larger';
  const stopBubble = (event) => { event.stopPropagation(); };
  const stopDrag = (event) => { event.preventDefault(); event.stopPropagation(); };
  const handleClick = (event) => { event.preventDefault(); event.stopPropagation(); };
  for (const el of [dec, reset, inc, tools]) {
    el.draggable = false;
    el.addEventListener('pointerdown', stopBubble);
    el.addEventListener('mousedown', stopBubble);
    el.addEventListener('touchstart', stopBubble, {passive:true});
    el.addEventListener('dragstart', stopDrag);
  }
  dec.addEventListener('click', (event) => { handleClick(event); const cur = clampNumber(card.dataset.fontScale ?? defaultCardFontScale, minCardFontScale, maxCardFontScale); setCardFontScale(card, isSafariBrowser ? safariNextFontScale(cur, -1) : cur - cardFontScaleStep); });
  reset.addEventListener('click', (event) => { handleClick(event); setCardFontScale(card, defaultCardFontScale); });
  inc.addEventListener('click', (event) => { handleClick(event); const cur = clampNumber(card.dataset.fontScale ?? defaultCardFontScale, minCardFontScale, maxCardFontScale); setCardFontScale(card, isSafariBrowser ? safariNextFontScale(cur, +1) : cur + cardFontScaleStep); });
  tools.append(dec, reset, inc);
  card.appendChild(tools);
}
function initLayoutEditor() {
  document.body.classList.toggle('safari-browser', isSafariBrowser);
  applyLayoutState(readLayoutState());
  const toggle = document.getElementById('layoutToggle');
  const reset = document.getElementById('layoutReset');
  const rowMinus = document.getElementById('layoutRowMinus');
  const rowPlus = document.getElementById('layoutRowPlus');
  if (toggle) toggle.addEventListener('click', () => setLayoutUnlocked(!layoutUnlocked));
  if (rowMinus) rowMinus.addEventListener('click', () => {
    const st = currentLayoutState();
    st.rowPx = clampNumber(st.rowPx - gridRowStepPx, minGridRowPx, maxGridRowPx);
    saveAndApplyLayout(st);
  });
  if (rowPlus) rowPlus.addEventListener('click', () => {
    const st = currentLayoutState();
    st.rowPx = clampNumber(st.rowPx + gridRowStepPx, minGridRowPx, maxGridRowPx);
    saveAndApplyLayout(st);
  });
  if (reset) reset.addEventListener('click', () => {
    try { localStorage.removeItem(layoutStorageKey); for (const key of oldLayoutStorageKeys) localStorage.removeItem(key); } catch (_) {}
    applyLayoutState(defaultLayoutState());
    setLayoutUnlocked(false);
  });
  for (const card of document.querySelectorAll('.card[data-card-id]')) {
    ensureCardContentWrapper(card);
    ensureCardFontControls(card);
    ensureResizeGrip(card);
    setCardDraggable(card, false);
    card.addEventListener('dragstart', (event) => {
      if (!layoutUnlocked || activeResize) { event.preventDefault(); return; }
      dragCardId = card.dataset.cardId;
      card.classList.add('dragging');
      try { event.dataTransfer.setData('text/plain', dragCardId); event.dataTransfer.effectAllowed = 'move'; } catch (_) {}
    });
    card.addEventListener('dragend', () => {
      card.classList.remove('dragging');
      for (const c of document.querySelectorAll('.card.drag-over')) c.classList.remove('drag-over');
      dragCardId = null;
    });
    card.addEventListener('dragover', (event) => {
      if (!layoutUnlocked || !dragCardId || dragCardId === card.dataset.cardId) return;
      event.preventDefault();
      try { event.dataTransfer.dropEffect = 'move'; } catch (_) {}
      card.classList.add('drag-over');
    });
    card.addEventListener('dragleave', () => card.classList.remove('drag-over'));
    card.addEventListener('drop', (event) => {
      if (!layoutUnlocked || !dragCardId) return;
      event.preventDefault();
      card.classList.remove('drag-over');
      const fromId = dragCardId;
      const toId = card.dataset.cardId;
      if (!fromId || !toId || fromId === toId) return;
      const state = currentLayoutState();
      state.order = moveCardInOrder(state.order, fromId, toId);
      saveAndApplyLayout(state);
    });
  }
  const onViewportResize = () => applyLayoutState(readLayoutState());
  window.addEventListener('resize', onViewportResize);
  if (window.visualViewport) window.visualViewport.addEventListener('resize', onViewportResize);
}
initLayoutEditor();

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
function compactMethod(v, maxLen=28) {
  let s = String(v ?? '-');
  if (!s || s === '-') return '-';
  const map = {
    elapsed_plan_fraction_sanity: 'sanity',
    event_history_inter_item_gap: 'gap',
    event_history_item_key: 'item',
    event_history: 'hist',
    scan_block: 'block'
  };
  s = s.split('+').map(p => map[p.trim()] || p.trim()).join('+');
  if (s.length <= maxLen) return s;
  return s.slice(0, Math.max(8, maxLen - 1)) + '…';
}

function degToArcsec(v) {
  const n = Number(v);
  return Number.isFinite(n) ? n * 3600.0 : null;
}
function arcsecText(v, digits=1) {
  const n = Number(v);
  return Number.isFinite(n) ? `${n.toFixed(digits)} arcsec` : '-';
}
function utcText(unixSec) {
  const n = Number(unixSec);
  return Number.isFinite(n) ? new Date(n*1000).toISOString().replace('T',' ').replace(/\\.\\d+Z$/,' UTC') : '-';
}
function timeSyncUtcRow(snapshot, utcUnix, serverTimeUnix=null) {
  const base = utcText(utcUnix);
  const ts = snapshot?.time_sync;
  if (!ts || ts.enabled !== true) return ['UTC', base, ''];
  const status = String(ts.status || '').toLowerCase();
  const label = status === 'ok' ? 'sync' : (status === 'bad' || status === 'warn' ? 'nosync' : 'unknown');
  const role = label === 'sync' ? 'ok' : (label === 'nosync' ? 'critical' : 'warning');
  const checkedAge = ageSinceUnix(ts.checked_at_unix, serverTimeUnix);
  const ageTextValue = Number.isFinite(checkedAge) ? ` / checked ${secText(checkedAge)} ago` : '';
  const summary = String(ts.summary || `time sync: ${label}`) + ageTextValue;
  return ['UTC', `${base}  ${label}`, role, summary];
}
function lstDisplayText(snapshot) {
  const text = snapshot?.display?.lst_hms;
  return isBlank(text) ? '-' : String(text);
}
function nestedGet(obj, paths) {
  for (const path of paths) {
    let cur = obj;
    let ok = true;
    for (const key of path) {
      if (!cur || typeof cur !== 'object' || !(key in cur)) { ok = false; break; }
      cur = cur[key];
    }
    if (ok && cur !== undefined && cur !== null && cur !== '') return cur;
  }
  return undefined;
}
function pad2(n) { return String(Math.trunc(Math.abs(n))).padStart(2, '0'); }
function raHmsText(raDeg) {
  let ra = Number(raDeg);
  if (!Number.isFinite(ra)) return '-';
  ra = ((ra % 360) + 360) % 360;
  const total = ra / 15.0 * 3600.0;
  let h = Math.floor(total / 3600.0);
  let m = Math.floor((total - h*3600.0) / 60.0);
  let sec = total - h*3600.0 - m*60.0;
  sec = Math.round(sec * 10) / 10;
  if (sec >= 60.0) { sec -= 60.0; m += 1; }
  if (m >= 60) { m -= 60; h = (h + 1) % 24; }
  return `${String(h).padStart(2,'0')}:${String(m).padStart(2,'0')}:${sec.toFixed(1).padStart(4,'0')}`;
}
function decDmsText(decDeg) {
  const dec = Number(decDeg);
  if (!Number.isFinite(dec)) return '-';
  const sign = dec < 0 ? '-' : '+';
  const a = Math.abs(dec);
  let d = Math.floor(a);
  let m = Math.floor((a - d) * 60.0);
  let sec = Math.round((a - d - m/60.0) * 3600.0);
  if (sec >= 60) { sec -= 60; m += 1; }
  if (m >= 60) { m -= 60; d += 1; }
  return `${sign}${String(d).padStart(2,'0')}:${String(m).padStart(2,'0')}:${String(sec).padStart(2,'0')}`;
}
function frameLooksRadec(frame) {
  const f = String(frame || '').toLowerCase();
  return f.includes('j2000') || f.includes('radec') || f.includes('ra/dec') || f.includes('icrs') || f.includes('fk5');
}
function frameLooksGalactic(frame) {
  const f = String(frame || '').toLowerCase();
  return f.includes('gal') || f === 'lb' || f.includes('l,b');
}
function radecText(pair, frame='J2000') {
  if (!Array.isArray(pair) || pair.length < 2) return '-';
  const a = Number(pair[0]), b = Number(pair[1]);
  if (!Number.isFinite(a) || !Number.isFinite(b)) return '-';
  return `${raHmsText(a)}, ${decDmsText(b)}`;
}
function galText(pair, frame='galactic') {
  if (!Array.isArray(pair) || pair.length < 2) return '-';
  const a = Number(pair[0]), b = Number(pair[1]);
  if (!Number.isFinite(a) || !Number.isFinite(b)) return '-';
  return `${a.toFixed(4)}°, ${b.toFixed(4)}°`;
}
function displayCoordPair(snapshot, key) {
  const value = snapshot?.display?.[key];
  return pair(value);
}
function targetSkyPair(snapshot) {
  const g = snapshot?.geometry || {};
  return pair(g.target) ? {coord:g.target, frame:coordFrame(g.target, g.frame || '')}
    : pair(g.reference) ? {coord:g.reference, frame:coordFrame(g.reference, g.frame || '')}
    : null;
}
function targetRaDecPair(snapshot) {
  const derived = displayCoordPair(snapshot, 'target_radec_j2000');
  if (derived) return derived;
  const t = targetSkyPair(snapshot); if (!t) return null;
  if (frameLooksRadec(t.frame)) return pair(t.coord);
  return null;
}
function targetGalPair(snapshot) {
  const derived = displayCoordPair(snapshot, 'target_galactic_lb');
  if (derived) return derived;
  const t = targetSkyPair(snapshot); if (!t) return null;
  if (frameLooksGalactic(t.frame)) return pair(t.coord);
  return null;
}
function coordPairText(pair, frame='', unit='deg') {
  if (!Array.isArray(pair) || pair.length < 2) return '-';
  const a = Number(pair[0]), b = Number(pair[1]);
  if (!Number.isFinite(a) || !Number.isFinite(b)) return '-';
  const suffix = frame ? ` ${frame}` : '';
  if (String(unit || '').toLowerCase().includes('arcsec')) return `(${a.toFixed(1)}, ${b.toFixed(1)}) arcsec${suffix}`;
  if (frameLooksRadec(frame)) return radecText([a, b], frame || 'J2000');
  if (frameLooksGalactic(frame)) return galText([a, b], frame || 'galactic');
  return `(${a.toFixed(5)}, ${b.toFixed(5)}) deg${suffix}`;
}
function kv(id, obj, keys) {
  const el = document.getElementById(id); el.innerHTML = '';
  for (const [label, key] of keys) {
    const raw = typeof key === 'function' ? key(obj) : obj?.[key];
    const keyName = typeof key === 'string' ? key : '';
    el.insertAdjacentHTML('beforeend', `<div class=\"k\">${esc(label)}</div><div class=\"v\" title=\"${esc(raw ?? '-')}\">${esc(val(raw, label, keyName))}</div>`);
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
function coordFrame(v, fallback='') { return Array.isArray(v) && v.length >= 3 && v[2] !== undefined && v[2] !== null ? String(v[2]) : fallback; }
function coordText(v, unit='deg', digits=3) {
  const p = pair(v); if (!p) return usableText(v) || '-';
  const frame = coordFrame(v);
  const u = unit || 'deg';
  if (u !== 'arcsec' && frameLooksRadec(frame)) return radecText(p, frame || 'J2000');
  if (u !== 'arcsec' && frameLooksGalactic(frame)) return galText(p, frame || 'galactic');
  const scale = u === 'arcsec' ? 3600 : 1;
  const suffix = u === 'arcsec' ? 'arcsec' : 'deg';
  const d = u === 'arcsec' ? 1 : digits;
  return `(${(p[0]*scale).toFixed(d)}, ${(p[1]*scale).toFixed(d)}) ${suffix}${frame ? ' ' + frame : ''}`;
}
function offsetArcsecText(v, label='') {
  const p = pair(v); if (!p) return '-';
  const frame = coordFrame(v);
  const prefix = label ? label + ' ' : '';
  return `${prefix}(${(p[0]*3600).toFixed(1)}, ${(p[1]*3600).toFixed(1)}) arcsec${frame ? ' ' + frame : ''}`;
}
function azElText(lon, lat, frame='altaz') {
  const x=Number(lon), y=Number(lat); if (!Number.isFinite(x) || !Number.isFinite(y)) return '-';
  const f=String(frame || '').toLowerCase();
  if (f === 'altaz' || f === 'alt-az' || f === 'azel') return `Az ${x.toFixed(3)}°, El ${y.toFixed(3)}°`;
  return `${String(frame || 'coord')} lon ${x.toFixed(3)}°, lat ${y.toFixed(3)}°`;
}
function azUnwrapBadgeHtml(a) {
  if (!a || !a.az_unwrap_enabled) return '';
  const b = Number(a.az_unwrap_branch);
  const raw = Number(a.az_unwrap_raw_az_deg);
  const state = String(a.az_unwrap_state || '');
  const role = !a.az_unwrap_valid ? 'warning' : (b !== 0 ? 'important' : 'muted');
  const main = Number.isFinite(b) ? `unwrap ${b > 0 ? '+' : ''}${b}` : 'unwrap ?';
  const rawBadge = Number.isFinite(raw) && b !== 0 ? `<span class="badge nowrap muted">raw ${raw.toFixed(3)}°</span>` : '';
  const stateBadge = state && state !== 'ok' ? `<span class="badge nowrap warning">${esc(state)}</span>` : '';
  return `<div class="unwrap-badges"><span class="badge nowrap ${role}">${esc(main)}</span>${rawBadge}${stateBadge}</div>`;
}
function usableText(v) { const t = val(v); return (!t || t === '-' || t === 'None' || t === 'null' || t === 'unknown') ? '' : t; }
function inferTargetFromName(text) {
  const raw = usableText(text);
  if (!raw) return '';
  const toks = raw.split(/[_-]+/).filter(Boolean);
  const skip = new Set(['necst','otf','grid','psw','sky','skydip','rsky','hot','off','on']);
  const kept = toks.filter(t => !skip.has(t.toLowerCase()) && !/^20\\d{6}$/.test(t) && !/^\\d{4,6}$/.test(t));
  return kept.length ? kept.slice(-3).join('_') : '';
}
function displayTarget(snapshot) {
  const g = snapshot?.geometry || {}; const obs = snapshot?.observation || {};
  return usableText(g.target_name) || usableText(obs.target) || inferTargetFromName(obs.record_name) || inferTargetFromName(obs.obs_file) || '-';
}

function isBlank(v) {
  if (v === null || v === undefined || v === '') return true;
  const s = val(v);
  return !s || s === '-' || s === 'None' || s === 'null' || s === 'unknown';
}
function kvSmart(id, rows) {
  const el = document.getElementById(id); el.innerHTML = '';
  for (const row of rows || []) {
    const label = Array.isArray(row) ? row[0] : '-';
    const raw = Array.isArray(row) ? row[1] : row;
    const role = Array.isArray(row) ? (row[2] || '') : '';
    const title = Array.isArray(row) && row.length >= 4 ? row[3] : raw;
    if (role === 'html') {
      el.insertAdjacentHTML('beforeend', `<div class="k">${esc(label)}</div><div class="v" title="${esc(title ?? raw ?? '-')}">${raw ?? '-'}</div>`);
      continue;
    }
    const cls = role ? ` ${role}-pos` : '';
    el.insertAdjacentHTML('beforeend', `<div class="k">${esc(label)}</div><div class="v${cls}" title="${esc(title ?? raw ?? '-')}">${esc(raw ?? '-')}</div>`);
  }
}
function geometryRows(snapshot) {
  const g = snapshot?.geometry || {};
  const obsType = String(snapshot?.observation?.type || '').toLowerCase();
  const mode = String(snapshot?.plan?.mode || g.mode || '').toUpperCase();
  const rows = [];
  rows.push(['kind', g.kind], ['frame', g.frame], ['target name', g.target_name || displayTarget(snapshot)]);
  const off = actualOffsetArcsecFromGeometry(g);
  const offFrame = g.offset_frame || g.frame || '';
  if (off) {
    rows.push([g.cos_correction ? 'map offset actual' : 'map offset', coordPairText(off, offFrame, 'arcsec')]);
    if (g.cos_correction) rows.push(['offset note', 'x is dLon*cos(lat); raw command dLon is not shown here']);
  }
  const geomFrame = g.frame || '';
  const geomUnit = g.unit || 'deg';
  if (g.target !== undefined) {
    const frame = coordFrame(g.target, g.target_frame || geomFrame);
    const unit = g.target_unit || geomUnit;
    const baseLabel = mode === 'OFF' || mode === 'SKY' ? 'OFF/SKY target' : (obsType.includes('grid') ? 'absolute target' : 'target/ON');
    rows.push([baseLabel, coordPairText(pair(g.target), frame, unit)]);
  }
  if (g.reference !== undefined) {
    const frame = coordFrame(g.reference, g.reference_frame || geomFrame);
    const unit = g.reference_unit || geomUnit;
    const baseLabel = mode === 'ON' ? 'ON/ref.' : 'reference';
    rows.push([baseLabel, coordPairText(pair(g.reference), frame, unit)]);
  }
  if (g.start !== undefined || g.stop !== undefined) {
    const sf = coordFrame(g.start, g.start_frame || geomFrame);
    const stf = coordFrame(g.stop, g.stop_frame || geomFrame);
    rows.push(['scan start', coordPairText(pair(g.start), sf, g.start_unit || geomUnit)], ['scan stop', coordPairText(pair(g.stop), stf, g.stop_unit || geomUnit)]);
  }
  if (g.cos_correction !== undefined) rows.push(['cos correction', g.cos_correction]);
  return rows.filter(r => r[1] !== undefined && r[1] !== null && r[1] !== '');
}
function liveCoordPair(snapshot, frameName) {
  const a = snapshot?.antenna || {};
  const f = String(frameName || '').toLowerCase();
  const candidates = [
    [`encoder_${f}_lon_deg`, `encoder_${f}_lat_deg`],
    [`command_${f}_lon_deg`, `command_${f}_lat_deg`],
    [`${f}_lon_deg`, `${f}_lat_deg`],
    [`${f}_l_deg`, `${f}_b_deg`],
    [`encoder_${f}_l_deg`, `encoder_${f}_b_deg`],
    [`command_${f}_l_deg`, `command_${f}_b_deg`]
  ];
  for (const [kx, ky] of candidates) {
    const x = Number(a[kx]); const y = Number(a[ky]);
    if (Number.isFinite(x) && Number.isFinite(y)) return [x, y];
  }
  return null;
}
function offsetReferenceLatitudeDeg(g, mapCtx=null) {
  const candidates = [g?.reference, g?.target, mapCtx?.center];
  for (const c of candidates) {
    const p = pair(c);
    if (p && Number.isFinite(p[1])) return p[1];
  }
  return null;
}
function actualOffsetArcsecFromGeometry(g, mapCtx=null) {
  const off = pair(g?.offset);
  if (!off) return null;
  const unit = String(g?.offset_unit || g?.unit || 'deg').toLowerCase();
  const scale = unit.includes('arcsec') ? 1.0 : 3600.0;
  let x = off[0] * scale;
  let y = off[1] * scale;
  // In NECST grid/pointing specs with cos_correction=true, offset[0] is the
  // longitude-like command delta.  Display the observer-facing tangent-plane X
  // offset, dLon*cos(lat), so a requested -600 arcsec appears as -600 arcsec.
  if (g?.cos_correction) {
    const lat = offsetReferenceLatitudeDeg(g, mapCtx);
    if (Number.isFinite(lat)) x *= Math.cos(lat * Math.PI / 180.0);
  }
  return [x, y];
}
function plannedOffsetFromGeometry(snapshot) {
  const g = snapshot?.geometry || {};
  return actualOffsetArcsecFromGeometry(g);
}
function angularDeltaDegJS(a, b) {
  let d = Number(a) - Number(b);
  if (!Number.isFinite(d)) return NaN;
  d = ((d + 180.0) % 360.0 + 360.0) % 360.0 - 180.0;
  return d;
}
function skyOffsetArcsecFromCenter(target, center) {
  const t = pair(target), c = pair(center);
  if (!t || !c) return null;
  const dx = angularDeltaDegJS(t[0], c[0]) * Math.cos(c[1] * Math.PI / 180.0) * 3600.0;
  const dy = (t[1] - c[1]) * 3600.0;
  return [dx, dy];
}
function sameSkyFrame(a, b) {
  const fa = String(coordFrame(a) || '').toLowerCase();
  const fb = String(coordFrame(b) || '').toLowerCase();
  return !fa || !fb || fa === fb || (frameLooksRadec(fa) && frameLooksRadec(fb)) || (frameLooksGalactic(fa) && frameLooksGalactic(fb));
}
function firstNumeric(obj, paths) {
  const v = nestedGet(obj, paths);
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}
function temperatureCFromAny(value, options={}) {
  if (value === null || value === undefined || value === '') return null;
  const n = Number(value);
  if (!Number.isFinite(n)) return null;
  const keyHint = String(options.keyHint || '').toLowerCase();
  const kelvin = options.unit === 'K' || /_k$|temperature_k|in_temperature_k/.test(keyHint);
  // A WeatherMsg field that was never filled often arrives as 0.0.
  // For Kelvin fields, <=100 K is not a plausible ambient value and is
  // treated as missing rather than displayed as -273.1 C or 0.0 C.
  if (kelvin) return n > 100 ? n - 273.15 : null;
  if (options.missingZero && Math.abs(n) < 1e-12) return null;
  // Backward-compatible heuristic for legacy snapshots without explicit units.
  if (n > 170) return n - 273.15;
  return n;
}
function firstPresent(obj, keys) {
  if (!obj || typeof obj !== 'object') return {value: null, key: ''};
  for (const key of keys) {
    if (obj[key] !== undefined && obj[key] !== null && obj[key] !== '') return {value: obj[key], key};
  }
  return {value: null, key: ''};
}
function temperatureCFromKeys(obj, keys, options={}) {
  const got = firstPresent(obj, keys);
  return temperatureCFromAny(got.value, {...options, keyHint: got.key});
}
function humidityPercentFromAny(value) {
  if (value === null || value === undefined || value === '') return null;
  const n = Number(value);
  if (!Number.isFinite(n)) return null;
  // NECST WeatherMsg humidity is defined as a 0--1 fraction.  Some older
  // snapshots used *_percent names while still carrying the same fraction.
  if (n >= 0 && n <= 1.000001) return n * 100.0;
  return n;
}
function weatherRowsForKey(snapshot, key, label) {
  const w = snapshot?.weather || {};
  const base = w[key] || {};
  const rows = [];
  const prefix = label === 'out' ? 'out' : label;
  const temp = temperatureCFromKeys(base, ['temperature_c','temperature_degC','temperature_k','temperature']);
  const hum = humidityPercentFromAny(base.humidity ?? base.relative_humidity ?? base.humidity_percent ?? base.relative_humidity_percent);
  const pres = Number(base.pressure_hpa ?? base.pressure_mbar ?? base.pressure);
  const wind = Number(base.wind_speed_mps ?? base.wind_speed);
  const wdir = Number(base.wind_direction_deg ?? base.wind_direction);
  if (temp !== null) rows.push([`${prefix} temp.`, `${temp.toFixed(1)} °C`, '']);
  if (hum !== null) rows.push([`${prefix} humid.`, `${hum.toFixed(0)} %`, '']);
  if (Number.isFinite(pres)) rows.push([`${prefix} press.`, `${pres.toFixed(1)} hPa`, '']);
  // If a site has no wind sensor, WeatherMsg defaults commonly appear as 0/0.
  // Show that as missing rather than as a real direction.  Real non-zero wind
  // still displays normally.
  if (Number.isFinite(wind) && Math.abs(wind) > 1e-12) {
    rows.push([`${prefix} wind`, Number.isFinite(wdir) ? `${wind.toFixed(1)} m/s @ ${wdir.toFixed(0)}°` : `${wind.toFixed(1)} m/s`, '']);
  } else if (Number.isFinite(wind) || Number.isFinite(wdir)) {
    rows.push([`${prefix} wind`, '-', 'muted']);
  }
  return rows;
}
function optionalWeatherRows(snapshot) {
  const rows = [];
  rows.push(...weatherRowsForKey(snapshot, 'out', 'out'));
  rows.push(...weatherRowsForKey(snapshot, 'in', 'in'));
  if (rows.length) return rows;
  const env = snapshot || {};
  const tempRaw = firstNumeric(env, [['weather','temperature_c'], ['weather','temperature_degC'], ['weather','temperature_k'], ['weather','temperature'], ['environment','temperature_c'], ['environment','temperature_degC'], ['site','temperature_c']]);
  const temp = temperatureCFromAny(tempRaw);
  const inTempRaw = firstNumeric(env, [['weather','in_temperature_c'], ['weather','in_temperature_degC'], ['weather','in_temperature_k'], ['weather','in_temperature'], ['environment','in_temperature_c'], ['environment','in_temperature_degC'], ['site','in_temperature_c']]);
  const inTemp = temperatureCFromAny(inTempRaw, {missingZero: true});
  const hum = humidityPercentFromAny(firstNumeric(env, [['weather','humidity'], ['weather','relative_humidity'], ['weather','humidity_percent'], ['weather','relative_humidity_percent'], ['environment','humidity'], ['environment','relative_humidity'], ['environment','humidity_percent'], ['environment','relative_humidity_percent'], ['site','humidity'], ['site','humidity_percent']]));
  const pres = firstNumeric(env, [['weather','pressure_hpa'], ['weather','pressure_mbar'], ['environment','pressure_hpa'], ['environment','pressure_mbar'], ['site','pressure_hpa']]);
  if (temp !== null) rows.push(['out temp.', `${temp.toFixed(1)} °C`, '']);
  if (inTemp !== null) rows.push(['in temp.', `${inTemp.toFixed(1)} °C`, 'important']);
  if (hum !== null) rows.push(['out humid.', `${hum.toFixed(0)} %`, '']);
  if (pres !== null) rows.push(['out press.', `${pres.toFixed(1)} hPa`, '']);
  return rows;
}
function isOtfScanBlock(snapshot) {
  const obsType = String(snapshot?.observation?.type || '').toLowerCase();
  const act = snapshot?.activity || {};
  const plan = snapshot?.plan || {};
  const geom = snapshot?.geometry || {};
  const drive = String(act.drive_kind || plan.drive_kind || '').toLowerCase();
  const kind = String(geom.kind || '').toLowerCase();
  return obsType === 'otf' && (drive === 'scan_block' || kind === 'scan_block');
}
function trackingExpected(snapshot) {
  const act = snapshot?.activity || {};
  const stage = String(act.motion_stage || act.control_section_kind || '').toLowerCase();
  const phase = String(act.phase || snapshot?.plan?.mode || '').toUpperCase();
  if (phase !== 'ON') return false;
  // In scan_block OTF, progress.integration() intentionally spans the whole
  // block so that data/metadata bookkeeping stays active from initial standby
  // through final sections.  Therefore data_state=integrating is not a promise
  // that the antenna is already on a science line.  The official tracking check
  // is meaningful for the observer warning only during the actual line section.
  if (isOtfScanBlock(snapshot)) return stage === 'line';
  const dataState = String(act.data_state || '').toLowerCase();
  return stage === 'tracking' || stage === 'line' || stage === 'scanning' || dataState === 'integrating';
}
function trackingProblem(snapshot) {
  const a = snapshot?.antenna || {};
  if (!trackingExpected(snapshot)) return null;
  if (!a.tracking_status_available) return 'Tracking status is unavailable while ON line/tracking is expected.';
  if (!a.tracking_ok) {
    const err = degToArcsec(a.tracking_error_deg);
    const age = Number(a.tracking_status_age_sec);
    const errText = Number.isFinite(err) ? `${err.toFixed(1)} arcsec` : 'unknown error';
    const ageText = Number.isFinite(age) ? `, age ${age.toFixed(1)} s` : '';
    return `Tracking is NOT OK during ON ${String(snapshot?.activity?.motion_stage || 'tracking')}: ${errText}${ageText}.`;
  }
  return null;
}
function officialTrackingRows(a, snapshot=null) {
  const expected = trackingExpected(snapshot);
  if (!a || !a.tracking_status_available) {
    return [
      ['Tracking status', expected ? 'unavailable while required' : 'unavailable', expected ? 'warning' : 'muted'],
      ['Tracking error', '-', expected ? 'warning' : 'muted']
    ];
  }
  const err = degToArcsec(a.tracking_error_deg);
  const age = Number(a.tracking_status_age_sec);
  const ok = !!a.tracking_ok;
  const ageText = Number.isFinite(age) ? `, age ${age.toFixed(1)} s` : '';
  const warn = trackingProblem(snapshot);
  if (ok) {
    return [
      ['Tracking status', `OK${ageText}`, 'ok'],
      ['Tracking error', arcsecText(err), 'ok']
    ];
  }
  if (!expected) {
    return [
      ['Tracking status', `not OK${ageText} (not required now)`, 'muted'],
      ['Tracking error', arcsecText(err), 'muted']
    ];
  }
  const rows = [
    ['Tracking status', `not OK${ageText}`, 'warning'],
    ['Tracking error', arcsecText(err), 'warning']
  ];
  if (warn) rows.push(['Tracking warning', warn, 'warning']);
  return rows;
}
function secText(v) {
  const n = Number(v);
  return Number.isFinite(n) ? `${n.toFixed(1)} s` : '-';
}
function ageSinceUnix(unix, serverTimeUnix=null) {
  const t = Number(unix);
  const now = Number(serverTimeUnix);
  if (!Number.isFinite(t) || !Number.isFinite(now)) return null;
  return Math.max(0, now - t);
}
function ageRole(age, warn=2.5, critical=8.0) {
  const n = Number(age);
  if (!Number.isFinite(n)) return 'muted';
  if (n >= critical) return 'critical';
  if (n >= warn) return 'warning';
  return 'ok';
}
function boolText(v, yes='yes', no='no') { return v ? yes : no; }
function shortUidText(v) {
  const s = usableText(v);
  if (!s) return '-';
  return s.length > 30 ? `${s.slice(0, 13)}…${s.slice(-12)}` : s;
}
function fractionPercentText(v) {
  const n = Number(v);
  return Number.isFinite(n) ? `${(100.0 * Math.max(0, Math.min(1, n))).toFixed(1)} %` : '-';
}
function signedArcsecText(deg) {
  const n = Number(deg);
  if (!Number.isFinite(n)) return '-';
  const a = n * 3600.0;
  return `${a >= 0 ? '+' : ''}${a.toFixed(1)} arcsec`;
}
function roleFromBoolOk(ok, available=true) {
  if (!available) return 'muted';
  return ok ? 'ok' : 'warning';
}
function commandQueueRows(snapshot, serverTimeUnix=null) {
  const q = snapshot?.antenna_command_queue || {};
  if (!q || !Object.keys(q).length) return [['status', 'not reported', 'muted'], ['source', 'AntennaCommandQueueStatus unavailable', 'muted']];
  const active = !!q.active;
  const gap = Number(q.publish_gap_sec);
  const tail = Number(q.tail_lead_sec);
  const head = Number(q.head_lead_sec);
  const qlen = Number(q.queue_length);
  const statusRole = q.guard_latched ? 'warning' : (active ? 'ok' : 'muted');
  const gapRole = Number.isFinite(gap) ? ageRole(gap, 1.5, 5.0) : 'muted';
  const leadText = `${secText(head)} / ${secText(tail)}`;
  const leadRole = active && Number.isFinite(tail) && tail < 0.2 ? 'warning' : (active ? 'ok' : 'muted');
  const age = ageSinceUnix(q.publish_time_unix, serverTimeUnix);
  const rows = [
    ['status', active ? 'active' : 'inactive', statusRole],
    ['queue length', Number.isFinite(qlen) ? String(qlen) : '-', active && qlen <= 0 ? 'warning' : ''],
    ['head/tail lead', leadText, leadRole],
    ['publish gap', secText(gap), gapRole],
    ['buffer config', `offset ${secText(q.command_offset_sec)}, max ${secText(q.max_buffer_sec)}`, ''],
    ['generator', q.generator_exhausted ? 'exhausted' : 'running/available', q.generator_exhausted ? 'muted' : 'ok'],
    ['guard', q.guard_latched ? 'latched' : 'clear', q.guard_latched ? 'warning' : 'ok'],
    ['status age', secText(age), ageRole(age, 3.0, 10.0)]
  ];
  if (!isBlank(q.mode)) rows.splice(1, 0, ['mode', q.mode, '']);
  if (!isBlank(q.reason)) rows.push(['reason', q.reason, q.guard_latched ? 'warning' : 'muted']);
  return rows;
}

function shortPathText(v, maxLen=38) {
  const s = String(v ?? '');
  if (!s) return '-';
  if (s.length <= maxLen) return s;
  const parts = s.split('/').filter(Boolean);
  if (parts.length >= 2) {
    const tail = parts.slice(-2).join('/');
    if (tail.length <= maxLen - 2) return '…/' + tail;
  }
  const head = Math.max(6, Math.floor((maxLen - 1) / 3));
  const tail = Math.max(10, maxLen - head - 1);
  return s.slice(0, head) + '…' + s.slice(-tail);
}
function compactSpectrometerTime(v) {
  const s = String(v ?? '');
  if (!s) return '-';
  const m = s.match(/T(\\d\\d:\\d\\d:\\d\\d)(.*)$/);
  if (m) return `${m[1]}${m[2] || ''}`;
  return s.length > 20 ? shortPathText(s, 24) : s;
}

function spectrometerIrigbRow(sp) {
  const raw = String(sp?.latest_time_spectrometer ?? '').trim();
  if (!raw) return ['IRIG-B', 'NO TIMESTAMP', 'critical'];
  return raw.endsWith('GPS') ? ['IRIG-B', 'GPS OK', 'ok'] : ['IRIG-B', 'NO GPS', 'critical'];
}
function chopperRow(snapshot, serverTimeUnix=null) {
  const ch = snapshot?.chopper || {};
  if (!ch || !Object.keys(ch).length) return ['chopper', 'not reported', 'muted'];
  const stateRaw = String(ch.state ?? '').trim().toLowerCase();
  const state = stateRaw || (ch.insert === true ? 'in' : (ch.insert === false ? 'out' : 'unknown'));
  const pos = Number(ch.position);
  const age = ageSinceUnix(ch.time_unix, serverTimeUnix);
  const text = `${state.toUpperCase()}${Number.isFinite(pos) ? ` / pos ${pos}` : ''}${Number.isFinite(age) ? ` / ${secText(age)}` : ''}`;
  return ['chopper', text, state === 'unknown' ? 'warning' : 'important'];
}

function spectrometerRows(snapshot, serverTimeUnix=null) {
  const sp = snapshot?.spectrometer || {};
  if (!sp || !Object.keys(sp).length) {
    return [['status', 'not reported', 'muted'], ['source', 'SpectrometerStatus unavailable', 'muted']];
  }
  const valid = !!sp.valid;
  const acquiring = !!sp.acquiring;
  const recording = !!sp.recorder_active;
  const saving = !!sp.saving_enabled;
  const dumpAge = Number(sp.last_dump_age_sec);
  const pubAge = ageSinceUnix(sp.publish_time_unix, serverTimeUnix);
  const missing = Number(sp.missing_board_count);
  const dumpRole = Number.isFinite(dumpAge) ? ageRole(dumpAge, 1.5, 5.0) : 'muted';
  const rows = [
    ['dump', acquiring ? `recent (${secText(dumpAge)})` : `idle/stale (${secText(dumpAge)})`, valid && acquiring ? dumpRole : (valid ? 'warning' : 'muted')],
    ['rec/save', `${recording ? 'active' : 'off'} / ${saving ? 'allowed' : 'blocked'}`, recording && saving ? 'ok' : (valid ? 'warning' : 'muted')],
    ['streams', `${val(sp.n_streams)} str / ${val(sp.n_boards)} boards`, ''],
    ['missing', Number.isFinite(missing) ? String(missing) : '-', Number.isFinite(missing) && missing > 0 ? 'warning' : 'ok'],
    ['meta', [sp.metadata_position, sp.metadata_id].filter(x => !isBlank(x)).join(' / ') || '-', 'important'],
    ['line', isBlank(sp.line_index) || Number(sp.line_index) < 0 ? '-' : String(sp.line_index), ''],
    ['ch', `TP ${val(sp.tp_range_start)}:${val(sp.tp_range_stop)}; q ${val(sp.qlook_ch_start)}:${val(sp.qlook_ch_stop)}`, ''],
    ['age', secText(pubAge), ageRole(pubAge, 3.0, 10.0)]
  ];
  rows.splice(2, 0, spectrometerIrigbRow(sp));
  if (!isBlank(sp.latest_time_spectrometer)) rows.splice(3, 0, ['clock', compactSpectrometerTime(sp.latest_time_spectrometer), 'muted']);
  if (!isBlank(sp.warning)) rows.push(['warning', sp.warning, 'warning']);
  return rows;
}
function systemTraceRows(snapshot, serverTimeUnix=null) {
  const a = snapshot?.activity || {};
  const ant = snapshot?.antenna || {};
  const q = snapshot?.antenna_command_queue || {};
  const sp = snapshot?.spectrometer || {};
  const sectionAge = ageSinceUnix(a.control_section_publish_time_unix, serverTimeUnix);
  const pointingAge = ageSinceUnix(ant.tracking_status_time_unix, serverTimeUnix);
  const queueAge = ageSinceUnix(q.publish_time_unix, serverTimeUnix);
  const specAge = ageSinceUnix(sp.publish_time_unix, serverTimeUnix);
  const frac = a.control_section_fraction;
  const liveMode = snapshot?.live?.spin_mode || 'file-only/no-ros';
  const liveRole = snapshot?.live?.spin_mode === 'background-executor' ? 'ok' : 'warn';
  const sectionBasis = a.control_status_basis || 'fallback/unknown';
  const sectionFresh = a.control_section_fresh === false ? 'stale' : 'fresh/unknown';
  const sectionFreshRole = a.control_section_fresh === false ? 'warn' : 'ok';
  const sectionId = shortUidText(a.control_section_uid);
  const planSeq = `plan ${val(a.control_section_plan_index)} / seq ${val(a.control_section_sequence_index)}`;
  const durationText = a.control_section_duration_sec !== undefined ? secText(a.control_section_duration_sec) : '-';
  const progressText = fractionPercentText(frac);
  const pointingSource = ant.pointing_basis || (ant.tracking_status_available ? 'legacy tracking' : 'none');
  const ageText = `sec ${secText(sectionAge)} / point ${secText(pointingAge)} / queue ${secText(queueAge)} / spec ${secText(specAge)}`;
  const ages = [sectionAge, pointingAge, queueAge, specAge].filter(x => Number.isFinite(Number(x))).map(Number);
  const maxAge = ages.length ? Math.max(...ages) : null;
  return [
    ['ROS/cache', liveMode, liveRole],
    ['section', `${sectionBasis}; ${sectionFresh}`, sectionFreshRole],
    ['sec id', sectionId, 'muted'],
    ['sec index', planSeq, 'muted'],
    ['dur/prog', `${durationText} / ${progressText}`, Number.isFinite(Number(frac)) ? 'important' : 'muted'],
    ['ages', ageText, ageRole(maxAge, 2.0, 5.0)],
    ['pointing', pointingSource, ant.tracking_status_available ? 'ok' : 'muted']
  ];
}
function positionRows(snapshot, psummary=null, serverTimeUnix=null) {
  const a = snapshot?.antenna || {}; const g = snapshot?.geometry || {};
  const frame = g.offset_frame || g.frame || '';
  const off = plannedOffsetFromGeometry(snapshot);
  const phase = String(snapshot?.activity?.phase || snapshot?.plan?.mode || '').toUpperCase();
  const isCal = phase && phase !== 'ON';
  const utcUnix = Number(snapshot?.time?.updated_at_unix ?? serverTimeUnix);
  const radec = targetRaDecPair(snapshot);
  const gal = targetGalPair(snapshot);
  const rows = [
    ['Encoder Az/El', azElText(a.encoder_lon_deg, a.encoder_lat_deg, a.encoder_frame), 'important'],
    ...(a.az_unwrap_enabled ? [['Az unwrap', azUnwrapBadgeHtml(a), 'html']] : []),
    ['Command Az/El', azElText(a.command_lon_deg, a.command_lat_deg, a.command_frame), ''],
    ...officialTrackingRows(a, snapshot),
    [isCal ? 'calibration now' : (g.cos_correction ? 'map offset actual' : 'map offset'), isCal ? phase : (off ? coordPairText(off, frame, 'arcsec') : '-'), 'important'],
    ['progress item', psummary?.progress || '-', 'important'],
    timeSyncUtcRow(snapshot, utcUnix, serverTimeUnix),
    ['LST', lstDisplayText(snapshot), ''],
    ['Target RA, Dec', radec ? radecText(radec, 'J2000') : '-', ''],
    ['Target L, B', gal ? galText(gal, 'galactic') : '-', '']
  ];
  return rows;
}
function activityRows(snapshot, serverTimeUnix=null) {
  const a = snapshot?.activity || {};
  const d = snapshot?.data || {};
  const g = snapshot?.geometry || {};
  const rows = [
    ['phase', a.phase],
    ['drive', a.drive_kind],
    ['motion', a.motion_stage || a.control_section_kind],
    ['data', a.data_state]
  ];
  if (snapshot?.chopper && Object.keys(snapshot.chopper).length) rows.push(chopperRow(snapshot, serverTimeUnix));
  if (!isBlank(a.control_id)) rows.push(['control id', a.control_id]);
  if (!isBlank(g.current_line_index0) && !isBlank(g.line_total)) rows.push(['control line', `${Number(g.current_line_index0)+1}/${g.line_total}`]);
  if (!isBlank(a.location_context)) rows.push(['location', a.location_context]);
  if (!isBlank(a.description)) rows.push(['description', a.description]);
  const hasExpected = !isBlank(d.expected_metadata_position) || !isBlank(d.expected_metadata_id) || !isBlank(d.expected_metadata_line_index);
  if (hasExpected) {
    if (!isBlank(d.expected_metadata_position)) rows.push(['expected metadata', d.expected_metadata_position, 'important']);
    if (!isBlank(d.expected_metadata_id)) rows.push(['expected id', d.expected_metadata_id]);
    if (!isBlank(d.expected_metadata_line_index)) rows.push(['expected line', d.expected_metadata_line_index]);
  }
  return rows.filter(r => !isBlank(r[1]));
}
function weatherTemperatureRow(base, label) {
  const temp = temperatureCFromKeys(base || {}, ['temperature_c','temperature_degC','temperature_k','temperature'], {missingZero: label === 'in'});
  return temp !== null ? [[`${label} temp.`, `${temp.toFixed(1)} °C`, label === 'in' ? 'important' : '']] : [];
}
function environmentRows(snapshot) {
  const rows = [];
  const w = snapshot?.weather || {};
  rows.push(...weatherRowsForKey(snapshot, 'out', 'out'));
  let inRows = weatherTemperatureRow(w.in || {}, 'in');
  if (!inRows.length) {
    inRows = weatherTemperatureRow({
      temperature_c: w.out?.in_temperature_c ?? w.out?.in_temperature_degC,
      temperature_k: w.out?.in_temperature_k ?? w.out?.in_temperature,
    }, 'in');
  }
  if (inRows.length) rows.push(...inRows);
  else if (rows.length || w.out || w.in) rows.push(['in temp.', '-', 'muted']);
  if (!rows.length) {
    rows.push(...optionalWeatherRows(snapshot));
  }
  if (!rows.length) rows.push(['status', 'weather not reported']);
  return rows;
}
function geomOf(item) { return item && typeof item.geometry === 'object' && item.geometry ? item.geometry : {}; }
function geomContext(g) {
  const out = {};
  for (const key of ['target_name','target','reference','offset','offset_unit','offset_frame','mode','frame','unit','cos_correction']) if (g && g[key] !== undefined) out[key] = g[key];
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
function intOrNull(v) {
  if (v === undefined || v === null || v === '') return null;
  const n = Number(v);
  return Number.isFinite(n) ? Math.trunc(n) : null;
}
function liveScanLineState(snapshot) {
  const a = snapshot?.activity || {};
  const g = snapshot?.geometry || {};
  const basis = String(a.control_status_basis || '').toLowerCase();
  const active = a.control_section_active === true;
  const fresh = a.control_section_fresh !== false;
  const authoritative = active && fresh && !['', 'idle', 'future-buffered', 'no-current-section'].includes(basis);
  let sectionLine = intOrNull(a.control_section_line_index);
  if (sectionLine === null && authoritative) sectionLine = intOrNull(g.current_line_index0);
  let progressLine = intOrNull(g.progress_line_index0);
  if (progressLine === null && !authoritative) progressLine = intOrNull(g.current_line_index0);
  const kind = String(a.control_section_kind || a.motion_stage || '').toLowerCase();
  const science = a.control_section_science_line === true;
  const lineActive = science || kind === 'line' || kind === 'scanning';
  return {authoritative, basis, kind, hasLine: sectionLine !== null && sectionLine >= 0, lineIndex: sectionLine, progressLineIndex: progressLine, lineActive};
}

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
    const state = liveScanLineState(snapshot);
    if (state.authoritative) {
      if (state.hasLine) {
        if (itemLine < state.lineIndex) return 'done';
        if (itemLine === state.lineIndex) return 'current';
        return 'pending';
      }
      if (state.progressLineIndex !== null) {
        if (itemLine < state.progressLineIndex) return 'done';
        return 'pending';
      }
      return 'pending';
    }
    if (itemLine < liveLine) return 'done';
    if (itemLine === liveLine) return 'current';
    return 'pending';
  }
  if (!finalState && ((cur && (uid === cur || parent === cur)) || inRange(item, cr))) return 'current';
  return 'pending';
}
const PLOT = {w: 660, h: 460, left: 64, right: 612, top: 42, bottom: 362};
function sx(x, bounds) { return PLOT.left + (x-bounds.xmin) * (PLOT.right-PLOT.left) / Math.max(1e-9, bounds.xmax-bounds.xmin); }
function sy(y, bounds) { return PLOT.bottom - (y-bounds.ymin) * (PLOT.bottom-PLOT.top) / Math.max(1e-9, bounds.ymax-bounds.ymin); }
function colorFor(status) { return status === 'done' ? '#2ca02c' : status === 'current' ? '#ff7f0e' : '#bdbdbd'; }
function renderPlanView(snapshot, plan, events, serverTimeUnix=null) {
  const items = flattenItems(plan);
  const kinds = new Set(items.map(it=>geomOf(it).kind));
  const obsType = String(snapshot?.observation?.type || '').toLowerCase();
  const plot = document.getElementById('plotview');
  if (kinds.has('skydip_elevation') || obsType.includes('sky')) { plot.innerHTML = renderSkydipSvg(snapshot, items, events); return; }
  if ([...kinds].some(k => ['scan_line','scan_block_line','point','grid_point'].includes(k))) { plot.innerHTML = renderMapSvg(snapshot, items, events, serverTimeUnix); return; }
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
function equalAspectBounds(xmin, xmax, ymin, ymax) {
  const innerAspect = (PLOT.right - PLOT.left) / Math.max(1e-9, (PLOT.bottom - PLOT.top));
  let cx = (xmin + xmax) / 2, cy = (ymin + ymax) / 2;
  let w = Math.max(1e-6, xmax - xmin), h = Math.max(1e-6, ymax - ymin);
  const dataAspect = w / h;
  if (dataAspect > innerAspect) h = w / innerAspect;
  else w = h * innerAspect;
  return {xmin: cx - w/2, xmax: cx + w/2, ymin: cy - h/2, ymax: cy + h/2};
}
function paddedBounds(scalePts, fallbackPts) {
  const pts = scalePts.length ? scalePts : fallbackPts;
  let xmin=Math.min(...pts.map(p=>p[0])), xmax=Math.max(...pts.map(p=>p[0])), ymin=Math.min(...pts.map(p=>p[1])), ymax=Math.max(...pts.map(p=>p[1]));
  const data = {dataXmin:xmin, dataXmax:xmax, dataYmin:ymin, dataYmax:ymax};
  const unitSpan = Math.max(Math.abs(xmax-xmin), Math.abs(ymax-ymin));
  const minPad = unitSpan <= 1e-9 ? 0.05 : 0.0;
  const dx=Math.max(minPad, 1e-6, (xmax-xmin)*0.08), dy=Math.max(minPad, 1e-6, (ymax-ymin)*0.08);
  return {...equalAspectBounds(xmin-dx, xmax+dx, ymin-dy, ymax+dy), ...data};
}
function projectFraction(a, b, p) {
  if (!a || !b || !p) return null;
  const vx = b[0] - a[0], vy = b[1] - a[1];
  const den = vx*vx + vy*vy;
  if (den <= 1e-18) return null;
  const t = ((p[0] - a[0]) * vx + (p[1] - a[1]) * vy) / den;
  return Math.max(0, Math.min(1, t));
}
function antennaPoint(snapshot, prefix, requiredFrame) {
  const a = snapshot?.antenna || {};
  const x = Number(a[`${prefix}_lon_deg`]);
  const y = Number(a[`${prefix}_lat_deg`]);
  const frame = String(a[`${prefix}_frame`] || '').toLowerCase();
  if (!Number.isFinite(x) || !Number.isFinite(y)) return null;
  if (requiredFrame && frame && frame !== String(requiredFrame).toLowerCase()) return null;
  return [x, y];
}
function pointDisplayLabel(row) {
  const raw = String(row.pointLabel || row.mode || '').toUpperCase();
  const repeat = Number(row.visitTotal || 1) > 1 ? `×${row.visitTotal}` : '';
  if (raw === 'ON' || raw === 'POINT') {
    if (row.status === 'current') return repeat ? `ON now ${repeat}` : 'ON now';
    return '';  // Avoid clutter: grid maps can have many ON points.
  }
  if (!raw || raw === '-') return '';
  if (raw === 'HOT') return '';
  if (row.status === 'current') return `${raw} now${repeat}`;
  return `${raw}${repeat}`;
}
function renderPoint(row, b) {
  const clipped = clipPoint(row.a, b);
  const x = sx(clipped.p[0], b), y = sy(clipped.p[1], b);
  const r = row.status === 'current' ? 6 : 4;
  const label = pointDisplayLabel(row);
  const shape = row.mode === 'ON' || row.mode === 'POINT'
    ? `<circle cx="${x}" cy="${y}" r="${r}" fill="${colorFor(row.status)}" stroke="${row.status==='current'?'var(--bg)':'none'}" stroke-width="2"/>`
    : `<path d="M ${x} ${y-r-2} L ${x+r+2} ${y} L ${x} ${y+r+2} L ${x-r-2} ${y} Z" fill="${colorFor(row.status)}" stroke="var(--bg)" stroke-width="1.5"/>`;
  const suffix = clipped.clipped ? ' (outside)' : '';
  const text = label ? `<text x="${Math.min(PLOT.w-135, x+7)}" y="${Math.max(16, y-6)}" font-size="12" font-weight="700">${esc(label + suffix)}</text>` : '';
  return shape + text;
}

function mapCenterContext(items, obsType) {
  if (!(obsType.includes('grid') || obsType.includes('psw'))) return null;
  for (const item of items || []) {
    const g = geomOf(item); const mode = itemMode(item);
    if (String(mode || '').toUpperCase() === 'ON') {
      const center = pair(g.reference) ? g.reference : (pair(g.target) ? g.target : null);
      if (center) return {center, frame:coordFrame(center, g.frame || '')};
    }
  }
  return null;
}
function pointPlotCoordinate(g, mode, obsType, kind, mapCtx=null) {
  const upper = String(mode || '').toUpperCase();
  const offsetMap = obsType.includes('grid') || obsType.includes('psw');
  if (offsetMap && upper === 'HOT') {
    // HOT is taken at the calibration load / OFF-position sequence, but it is
    // not a sky map position needed by observers.  Do not draw it in Plan View.
    return {xy: null, source: 'hidden_hot'};
  }
  if (offsetMap && (upper === 'ON' || kind === 'grid_point')) {
    const off = actualOffsetArcsecFromGeometry(g, mapCtx);
    if (off) return {xy: off, source: 'offset_arcsec'};
  }
  if (offsetMap && upper !== 'ON' && upper !== '') {
    // Absolute OFF/SKY positions are not reference bases.  Convert target minus
    // ON center into the same map-offset plane when both coordinates are in the
    // same sky frame.  This makes a far OFF appear at the proper clipped edge
    // instead of incorrectly at (0,0).
    if (mapCtx?.center && pair(g.target) && sameSkyFrame(g.target, mapCtx.center)) {
      const offAbs = skyOffsetArcsecFromCenter(g.target, mapCtx.center);
      if (offAbs) return {xy: offAbs, source: 'absolute_target_offset_arcsec'};
    }
    if (pair(g.offset)) {
      const off = actualOffsetArcsecFromGeometry(g, mapCtx);
      if (off) return {xy: off, source: 'offset_arcsec'};
    }
    return {xy: [1.0e9, 0.0], source: 'external_reference'};
  }
  const target = pair(g.target);
  if (target) return {xy: target, source: 'target'};
  const ref = pair(g.reference);
  if (ref) return {xy: ref, source: 'reference'};
  const off = pair(g.offset);
  if (off) return {xy: off, source: 'offset'};
  return {xy: null, source: ''};
}
function collectMapRows(snapshot, items, events) {
  const rows=[]; const allPts=[]; const boundsPts=[];
  const obsType = String(snapshot?.observation?.type || '').toLowerCase();
  const mapCtx = mapCenterContext(items, obsType);
  for (const item of items) {
    const g = geomOf(item); const k=g.kind; const mode = itemMode(item);
    const status = statusFor(item,snapshot,events);
    if (k === 'scan_line' || k === 'scan_block_line') {
      const a=pair(g.start), b=pair(g.stop);
      if (a&&b) {
        rows.push({item,a,b,status,mode:mode || 'ON',kind:k,lineIndex:g.line_index0,frame:g.frame,unit:g.unit,coordSource:'line'});
        allPts.push(a,b);
        if ((mode || 'ON') === 'ON') boundsPts.push(a,b);
      }
    }
    if (k === 'point' || k === 'grid_point') {
      const plotted = pointPlotCoordinate(g, mode, obsType, k, mapCtx);
      const xy = plotted.xy;
      if (xy) {
        const pointLabel = pointKind(item);
        rows.push({item,a:xy,b:null,status,mode:mode || pointLabel,kind:k,pointLabel,frame:g.frame,unit:g.unit,coordSource:plotted.source});
        allPts.push(xy);
        if ((mode || '').toUpperCase() === 'ON' || k === 'grid_point') boundsPts.push(xy);
      }
    }
  }
  return {rows, allPts, boundsPts};
}
function lineRowsFrom(rows) {
  let lineRows = rows.filter(r => r.b && (r.mode === 'ON' || r.mode === ''));
  if (!lineRows.length) lineRows = rows.filter(r => r.b);
  lineRows.sort((a,b)=>(Number(a.lineIndex ?? 0)-Number(b.lineIndex ?? 0)));
  return lineRows;
}
function lineSummary(rows) {
  const lineRows = lineRowsFrom(rows);
  const currentRow = lineRows.find(r => r.status === 'current') || null;
  const currentNo = currentRow ? lineRows.indexOf(currentRow) + 1 : null;
  return {lineRows, currentRow, currentNo, done: lineRows.filter(r=>r.status==='done').length, current: lineRows.filter(r=>r.status==='current').length, remaining: lineRows.filter(r=>r.status==='pending').length, total: lineRows.length};
}
function pointCounts(rows) {
  return {done: rows.filter(r=>r.status==='done').length, current: rows.filter(r=>r.status==='current').length, remaining: rows.filter(r=>r.status==='pending').length, total: rows.length, currentNo: rows.findIndex(r=>r.status==='current') + 1};
}
function pointKey(row) {
  const x = Number(row.a?.[0]), y = Number(row.a?.[1]);
  const mode = String(row.mode || row.pointLabel || '').toUpperCase();
  const src = row.coordSource || '';
  return `${mode}|${src}|${Number.isFinite(x)?x.toFixed(8):'x'}|${Number.isFinite(y)?y.toFixed(8):'y'}`;
}
function aggregatePointRows(pointRows) {
  const groups = new Map();
  for (const row of pointRows) {
    const key = pointKey(row);
    if (!groups.has(key)) groups.set(key, {...row, visitTotal:0, visitDone:0, visitCurrent:0, visitPending:0});
    const g = groups.get(key);
    g.visitTotal += 1;
    if (row.status === 'current') g.visitCurrent += 1;
    else if (row.status === 'done') g.visitDone += 1;
    else g.visitPending += 1;
    // Aggregate color policy for repeated visits at one physical position:
    // orange=current now; green=visited at least once; gray=not yet visited.
    if (g.visitCurrent > 0) g.status = 'current';
    else if (g.visitDone > 0) g.status = 'done';
    else g.status = 'pending';
  }
  return Array.from(groups.values());
}
function uniquePointCount(pointRows) { return aggregatePointRows(pointRows).length; }
function visitPassText(counts, uniqueCount) {
  if (!uniqueCount || uniqueCount <= 0 || counts.total <= uniqueCount) return '';
  const passes = counts.total / uniqueCount;
  const currentOrdinal = Math.max(1, counts.done + Math.max(1, counts.current));
  const currentPass = Math.min(Math.ceil(currentOrdinal / uniqueCount), Math.ceil(passes));
  const passText = Number.isInteger(passes) ? String(passes) : passes.toFixed(1);
  return `; map points=${uniqueCount}; pass≈${currentPass}/${passText}`;
}
function rowsForDrawing(rows) {
  const lineRows = rows.filter(r => r.b);
  const pointRows = rows.filter(r => !r.b);
  return lineRows.concat(aggregatePointRows(pointRows));
}
function gridSequencePath(pointRows, b) {
  const on = pointRows.filter(r => String(r.mode || r.pointLabel || '').toUpperCase()==='ON' || r.kind === 'grid_point');
  if (on.length < 2) return '';
  const pts = on.map(r => `${sx(r.a[0],b)},${sy(r.a[1],b)}`).join(' ');
  return `<polyline points="${pts}" fill="none" stroke="var(--muted)" stroke-width="1.3" opacity="0.45" stroke-dasharray="4 3"/>`;
}
function scheduleStep(plan) {
  if (Number.isInteger(plan?.index0) && Number.isInteger(plan?.total) && plan.total > 0) {
    const end = Number.isInteger(plan?.index0_end) && plan.index0_end >= plan.index0 ? plan.index0_end : plan.index0;
    return end > plan.index0 ? `${plan.index0+1}-${end+1}/${plan.total}` : `${plan.index0+1}/${plan.total}`;
  }
  return '-';
}
function observerPlanSummary(snapshot, plan, events) {
  const obsType = String(snapshot?.observation?.type || '').toLowerCase();
  const rows = collectMapRows(snapshot, flattenItems(plan), events).rows;
  const ls = lineSummary(rows);
  const p = snapshot?.plan || {};
  const geom = snapshot?.geometry || {};
  const pointRows = rows.filter(r => !r.b);
  const onPointRows = pointRows.filter(r => String(r.mode || r.pointLabel || '').toUpperCase() === 'ON' || r.kind === 'grid_point');
  const currentMode = p.mode ?? snapshot?.activity?.phase ?? '-';
  let progress = `step ${scheduleStep(p)}`;
  let basis = 'schedule item';
  let current = currentMode;
  let completed = '-';
  let remaining = '-';
  let total = '-';
  if (ls.total) {
    progress = `OTF line ${ls.currentNo ?? '-'}/${ls.total}`;
    basis = 'ON scan line'; completed = ls.done; remaining = ls.remaining; total = ls.total; current = 'scan line';
  } else if (obsType.includes('sky')) {
    const sky = skydipRowsFrom(snapshot, flattenItems(plan), events);
    const c = pointCounts(sky);
    let skyNo = c.currentNo;
    if (!skyNo && Number.isInteger(p.index0)) skyNo = p.index0 + 1;
    progress = `Skydip step ${skyNo || '-'}/${c.total || '-'}`;
    basis = 'elevation step'; completed = c.done; remaining = c.remaining; total = c.total; current = 'elevation';
  } else if (obsType.includes('grid')) {
    const c = pointCounts(onPointRows.length ? onPointRows : pointRows);
    progress = `Grid ON ${c.currentNo || c.done}/${c.total || '-'}`;
    basis = 'ON grid point'; completed = c.done; remaining = c.remaining; total = c.total; current = currentMode;
  } else if (obsType.includes('psw')) {
    const c = pointCounts(onPointRows.length ? onPointRows : pointRows);
    progress = `PSW ON ${c.currentNo || c.done}/${c.total || '-'}`;
    basis = 'ON target visit'; completed = c.done; remaining = c.remaining; total = c.total; current = currentMode;
  }
  return {progress, basis, current, completed, remaining, total, schedule_step: scheduleStep(p), mode: p.mode ?? '-', role: p.role ?? '-', target: displayTarget(snapshot), label: p.label ?? '-'};
}
function projectionOnLine(a, b, p, bounds) {
  if (!a || !b || !p) return null;
  const vx = b[0] - a[0], vy = b[1] - a[1];
  const den = vx*vx + vy*vy;
  if (den <= 1e-18) return null;
  const t = ((p[0] - a[0]) * vx + (p[1] - a[1]) * vy) / den;
  const cx = a[0] + t*vx, cy = a[1] + t*vy;
  const dist = Math.hypot(p[0]-cx, p[1]-cy);
  const lineLen = Math.sqrt(den);
  const mapScale = Math.max(Math.abs(bounds.xmax-bounds.xmin), Math.abs(bounds.ymax-bounds.ymin), 1e-9);
  const reliable = t >= -0.08 && t <= 1.08 && dist <= Math.max(0.18*lineLen, 0.025*mapScale);
  return {t: Math.max(0, Math.min(1, t)), rawT: t, dist, reliable};
}
function pointInsideBounds(p, bounds, margin=0.02) {
  if (!p) return false;
  const dx = (bounds.xmax-bounds.xmin)*margin, dy = (bounds.ymax-bounds.ymin)*margin;
  return p[0] >= bounds.xmin-dx && p[0] <= bounds.xmax+dx && p[1] >= bounds.ymin-dy && p[1] <= bounds.ymax+dy;
}
function median(xs) {
  const a = xs.filter(x=>Number.isFinite(x) && x > 0).sort((a,b)=>a-b);
  if (!a.length) return null;
  const m = Math.floor(a.length/2);
  return a.length % 2 ? a[m] : 0.5*(a[m-1]+a[m]);
}
const lineMotionClock = {key:null, start:null, source:null};
function lineMotionKey(snapshot, row) {
  const rec = snapshot?.observation?.record_name || '';
  const uid = snapshot?.plan?.item_uid || row?.item?.item_uid || '';
  const line = snapshot?.geometry?.current_line_index0 ?? row?.lineIndex ?? '';
  const id = snapshot?.data?.expected_metadata_id ?? snapshot?.plan?.obs_id ?? '';
  const sectionUid = snapshot?.activity?.control_section_uid || '';
  const ctrl = snapshot?.live?.control || {};
  const ctrlId = ctrl.id || snapshot?.activity?.control_id || '';
  return `${rec}|${uid}|${line}|${id}|${ctrlId}|${sectionUid}`;
}
function isActualLineMotion(snapshot) {
  // Test/debug invariant: motion_stage=line means the line/scanning section is active.
  const drive = String(snapshot?.activity?.drive_kind || snapshot?.plan?.drive_kind || '').toLowerCase();
  const stage = String(snapshot?.activity?.motion_stage || '').toLowerCase();
  if (!(drive.includes('scan') || drive.includes('block'))) return false;
  // In scan_block mode the dashboard used to start line progress during
  // standby/accelerate/decelerate because integration can already be active.
  // Only the explicit line section should advance the marker on the map.
  return stage === 'line' || stage === 'scanning';
}
function currentLineIndex(snapshot, row) {
  const line = Number(snapshot?.geometry?.current_line_index0 ?? row?.lineIndex);
  return Number.isFinite(line) ? line : null;
}
function lineExpectedDurationSec(row) {
  if (!row?.a || !row?.b) return null;
  const speed = Number(row?.item?.geometry?.speed_deg_per_sec ?? row?.speed_deg_per_sec);
  if (!Number.isFinite(speed) || speed <= 0) return null;
  const length = Math.hypot(row.b[0]-row.a[0], row.b[1]-row.a[1]);
  if (!Number.isFinite(length) || length <= 0) return null;
  return length / speed;
}
function controlLineStartedAt(snapshot, row) {
  const ctrl = snapshot?.live?.control || {};
  const ctrlStage = String(ctrl.section_kind || snapshot?.activity?.control_section_kind || snapshot?.activity?.motion_stage || '').toLowerCase();
  if (!(ctrlStage === 'line' || ctrlStage === 'scanning')) return null;
  const wantedLine = currentLineIndex(snapshot, row);
  const ctrlLine = Number(ctrl.line_index ?? snapshot?.geometry?.current_line_index0);
  if (wantedLine !== null && Number.isFinite(ctrlLine) && ctrlLine !== wantedLine) return null;
  const t = Number(ctrl.time_unix);
  return Number.isFinite(t) ? t : null;
}
function lastMatchedLineDriveStartedAt(events, snapshot, row) {
  let t = null;
  const wantedLine = currentLineIndex(snapshot, row);
  const scanBlock = isOtfScanBlock(snapshot);
  for (const ev of events || []) {
    if (!ev || ev.event !== 'drive_started') continue;
    const drive = String(ev.drive_kind || '').toLowerCase();
    const stage = String(ev.motion_stage || '').toLowerCase();
    if (!(drive.includes('scan') || drive.includes('block'))) continue;
    if (!(stage === 'line' || stage === 'scanning')) continue;
    const evLine = Number(ev.line_index ?? ev.line_index0 ?? ev.current_line_index0);
    // A scan_block line event without a line index is ambiguous.  Reusing it
    // for a later line makes the browser timer jump to 98% at line start.
    if (scanBlock && wantedLine !== null) {
      if (!Number.isFinite(evLine) || evLine !== wantedLine) continue;
    }
    const et = Number(ev.time_unix);
    if (Number.isFinite(et)) t = et;
  }
  return t;
}
function currentLineMotionProgress(snapshot, row, serverTimeUnix, events=null) {
  if (!row) return null;
  const stage = String(snapshot?.activity?.motion_stage || '-');
  const now = Number.isFinite(serverTimeUnix) ? serverTimeUnix : Date.now()/1000;
  const key = lineMotionKey(snapshot, row);
  if (!isActualLineMotion(snapshot)) {
    if (lineMotionClock.key !== key) { lineMotionClock.key = key; lineMotionClock.start = null; lineMotionClock.source = null; }
    return {fraction:null, waiting:true, stage, label:`line not started (${stage})`};
  }
  const statusFraction = Number(snapshot?.activity?.control_section_fraction);
  const statusSource = snapshot?.activity?.control_status_basis || 'section-status';
  if (Number.isFinite(statusFraction)) {
    const fraction = Math.max(0, Math.min(1, statusFraction));
    return {fraction, rawFraction:statusFraction, waiting:false, stage, source:statusSource, label:`Command ${(100*fraction).toFixed(0)}%`};
  }
  const controlStart = controlLineStartedAt(snapshot, row);
  const eventStart = controlStart ?? lastMatchedLineDriveStartedAt(events, snapshot, row);
  const startSource = controlStart !== null ? 'control-status-fallback' : (eventStart !== null ? 'matched-event-fallback' : 'browser-observed-fallback');
  if (lineMotionClock.key !== key || lineMotionClock.start === null) {
    lineMotionClock.key = key;
    lineMotionClock.start = eventStart ?? now;
    lineMotionClock.source = startSource;
  } else if (eventStart !== null && Math.abs(lineMotionClock.start - eventStart) > 0.2) {
    lineMotionClock.start = eventStart;
    lineMotionClock.source = startSource;
  }
  const duration = lineExpectedDurationSec(row);
  if (!duration) return {fraction:null, waiting:false, stage, label:'line section active; duration unknown'};
  const raw = (now - lineMotionClock.start) / duration;
  const fraction = Math.max(0, Math.min(0.98, raw));
  return {fraction, rawFraction:raw, waiting:false, stage, duration, source:lineMotionClock.source, label:`Line ${(100*fraction).toFixed(0)}% (fallback)`};
}
function pointAtFraction(a, b, t) { return [a[0] + (b[0]-a[0])*t, a[1] + (b[1]-a[1])*t]; }
function renderNowMarker(row, p, b, label) {
  if (!row || !p) return '';
  const x=sx(p[0],b), y=sy(p[1],b);
  return `<g><circle cx="${x}" cy="${y}" r="6" fill="#ff7f0e" stroke="var(--bg)" stroke-width="2"/><circle cx="${x}" cy="${y}" r="2" fill="var(--bg)"/><text x="${Math.min(PLOT.w-120, x+9)}" y="${Math.max(18, y-10)}" font-size="11" font-weight="700">${esc(label)}</text></g>`;
}
function mapAxisLabels(rows) {
  const offsetBasis = rows.some(r => r.coordSource === 'offset_arcsec' || (r.coordSource === 'offset' && (String(r.mode || '').toUpperCase() === 'ON' || r.kind === 'grid_point')));
  if (offsetBasis) {
    const r = rows.find(x=>x.coordSource === 'offset_arcsec' || x.coordSource === 'offset') || rows[0] || {};
    const frame = r.frame || 'map';
    return {x:`actual map offset x (arcsec, ${frame})`, y:`map offset y (arcsec, ${frame})`, unit:'arcsec'};
  }
  const r = rows.find(x=>x.frame) || rows[0] || {};
  const frame = r.frame || 'map'; const unit = r.unit || 'deg';
  return {x:`${frame} x (${unit})`, y:`${frame} y (${unit})`, unit};
}
function fmtAxisNumber(v, axes) {
  if (!Number.isFinite(v)) return '-';
  return axes?.unit === 'arcsec' ? v.toFixed(0) : v.toFixed(3);
}
function axisDecorations(b, axes) {
  const x0in = b.xmin <= 0 && 0 <= b.xmax;
  const y0in = b.ymin <= 0 && 0 <= b.ymax;
  const lx = Number.isFinite(b.dataXmin) ? b.dataXmin : b.xmin;
  const rx = Number.isFinite(b.dataXmax) ? b.dataXmax : b.xmax;
  const by = Number.isFinite(b.dataYmin) ? b.dataYmin : b.ymin;
  const ty = Number.isFinite(b.dataYmax) ? b.dataYmax : b.ymax;
  let out = '';
  out += `<text class="plot-note" text-anchor="middle" x="${sx(lx,b)}" y="${PLOT.bottom+18}">${esc(fmtAxisNumber(lx, axes))}</text>`;
  out += `<text class="plot-note" text-anchor="middle" x="${sx(rx,b)}" y="${PLOT.bottom+18}">${esc(fmtAxisNumber(rx, axes))}</text>`;
  out += `<text class="plot-note" text-anchor="end" x="${PLOT.left-9}" y="${sy(by,b)+4}" >${esc(fmtAxisNumber(by, axes))}</text>`;
  out += `<text class="plot-note" text-anchor="end" x="${PLOT.left-9}" y="${sy(ty,b)+4}" >${esc(fmtAxisNumber(ty, axes))}</text>`;
  if (x0in) out += `<line x1="${sx(0,b)}" y1="${PLOT.top}" x2="${sx(0,b)}" y2="${PLOT.bottom}" stroke="var(--muted)" stroke-dasharray="3 3" opacity="0.45"/>`;
  if (y0in) out += `<line x1="${PLOT.left}" y1="${sy(0,b)}" x2="${PLOT.right}" y2="${sy(0,b)}" stroke="var(--muted)" stroke-dasharray="3 3" opacity="0.45"/>`;
  return out;
}
function renderMapSvg(snapshot, items, events, serverTimeUnix=null) {
  const collected = collectMapRows(snapshot, items, events);
  const rows = collected.rows; const allPts = collected.allPts; const boundsPts = collected.boundsPts;
  if (!allPts.length) return '<div>No numeric geometry. This is normal for target-name-only observations.</div>';
  const obsType = String(snapshot?.observation?.type || '').toLowerCase();
  let b=paddedBounds(boundsPts, allPts);
  const ls = lineSummary(rows);
  const currentRow = ls.currentRow;
  const rowFrame = currentRow?.frame || rows.find(r=>r.frame)?.frame || '';
  const cmd = antennaPoint(snapshot, 'command', rowFrame);
  const enc = antennaPoint(snapshot, 'encoder', rowFrame);
  const tel = enc || cmd;
  const proj = currentRow && tel ? projectionOnLine(currentRow.a, currentRow.b, tel, b) : null;
  const telescopeCanOverlay = Boolean(proj?.reliable && pointInsideBounds(tel, b, 0.03));
  const lineProgress = currentRow ? currentLineMotionProgress(snapshot, currentRow, serverTimeUnix, events) : null;
  let nowPoint = null, nowLabel = '';
  if (currentRow && telescopeCanOverlay) {
    nowPoint = tel; nowLabel = `Telescope ${(100*proj.t).toFixed(0)}%`;
  } else if (currentRow && lineProgress && Number.isFinite(lineProgress.fraction)) {
    nowPoint = pointAtFraction(currentRow.a, currentRow.b, lineProgress.fraction);
    nowLabel = lineProgress.label;
  }
  const axes = mapAxisLabels(rows);
  const pointRows = rows.filter(r=>!r.b);
  const onRows = pointRows.filter(r=>String(r.mode || r.pointLabel || '').toUpperCase()==='ON' || r.kind === 'grid_point');
  const drawRows = rowsForDrawing(rows).sort((a,b)=>({pending:0,done:1,current:2}[a.status]-{pending:0,done:1,current:2}[b.status]));
  const sequencePath = obsType.includes('grid') ? gridSequencePath(onRows, b) : '';
  const lineEls = drawRows.map(r => {
    if (!r.b) return renderPoint(r,b);
    const x1=sx(r.a[0],b), y1=sy(r.a[1],b), x2=sx(r.b[0],b), y2=sy(r.b[1],b);
    const arrow = r.status === 'current' ? ' marker-end="url(#arrowhead)"' : '';
    return `<line x1="${x1}" y1="${y1}" x2="${x2}" y2="${y2}" stroke="${colorFor(r.status)}" stroke-width="${r.status==='current'?4:2}" opacity="${r.status==='pending'?0.55:0.95}"${arrow}/>`;
  }).join('');
  const liveEls = nowPoint ? renderNowMarker(currentRow, nowPoint, b, nowLabel) : '';
  let countText = '', note = '';
  if (ls.total) {
    countText = `OTF ${ls.done}/${ls.total} done, cur ${ls.current}, rem ${ls.remaining}`;
    if (nowPoint && telescopeCanOverlay) note = 'marker: live antenna on current line';
    else if (nowPoint) {
      // Backward-compatible test/debug marker: Line marker uses ${lineProgress?.source || 'browser'} line timer.
      const src = String(lineProgress?.source || 'browser');
      if (src === 'current-time' || src === 'section-status') note = `marker: section status (${src})`;
      else note = `marker: ${src} timer`;
    }
    else if (lineProgress?.waiting) note = `marker waits: ${lineProgress.stage}`;
    else {
      const state = liveScanLineState(snapshot);
      if (state.authoritative && !state.lineActive) note = `no marker: ${state.kind || state.basis}`;
      else note = 'no reliable line marker';
    }
  } else if (obsType.includes('grid')) {
    const basisRows = onRows.length ? onRows : pointRows;
    const c = pointCounts(basisRows); const u = uniquePointCount(basisRows);
    countText = `Grid ON ${c.done}/${c.total} done, cur ${c.current}, rem ${c.remaining}${visitPassText(c,u)}`;
    note = 'ON-basis; dashed line = planned ON order';
  } else if (obsType.includes('psw')) {
    const basisRows = onRows.length ? onRows : pointRows;
    const c = pointCounts(basisRows); const u = uniquePointCount(basisRows);
    countText = `PSW ON ${c.done}/${c.total} done, cur ${c.current}, rem ${c.remaining}${visitPassText(c,u)}`;
    note = 'ON-basis; OFF/HOT/SKY are labels';
  } else {
    const c = pointCounts(pointRows);
    countText = `Point ${c.done}/${c.total} done, cur ${c.current}, rem ${c.remaining}`;
    note = 'orange=current; HOT hidden';
  }
  return `<svg viewBox="0 0 ${PLOT.w} ${PLOT.h}" role="img" preserveAspectRatio="xMidYMid meet"><defs><marker id="arrowhead" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="5" markerHeight="5" orient="auto-start-reverse"><path d="M 0 0 L 10 5 L 0 10 z" fill="#ff7f0e"/></marker></defs><rect x="1" y="1" width="${PLOT.w-2}" height="${PLOT.h-2}" fill="none" stroke="#9995"/><line x1="${PLOT.left}" y1="${PLOT.bottom}" x2="${PLOT.right}" y2="${PLOT.bottom}" stroke="#777"/><line x1="${PLOT.left}" y1="${PLOT.top}" x2="${PLOT.left}" y2="${PLOT.bottom}" stroke="#777"/><text class="axis-label" x="${(PLOT.left+PLOT.right)/2-55}" y="${PLOT.bottom+42}">${esc(axes.x)}</text><text class="axis-label" transform="translate(17 ${(PLOT.top+PLOT.bottom)/2+35}) rotate(-90)">${esc(axes.y)}</text>${axisDecorations(b, axes)}${sequencePath}${lineEls}${liveEls}<text x="${PLOT.left}" y="${PLOT.h-36}" font-size="10">${esc(countText)}</text><text x="${PLOT.left}" y="${PLOT.h-18}" font-size="10">${esc(note)}</text></svg>`;
}
function skydipRowsFrom(snapshot, items, events) {
  const rows=[];
  for (let i=0; i<items.length; i++) {
    const g=geomOf(items[i]);
    let el = Number(g.el_deg ?? g.target_el_deg ?? g.elevation_deg);
    const t=pair(g.target);
    if (!Number.isFinite(el) && t) el=t[1];
    if (Number.isFinite(el)) rows.push({x:i+1,y:el,status:statusFor(items[i],snapshot,events), label:itemMode(items[i]) || String(items[i]?.mode || '')});
  }
  return rows;
}
function renderSkydipSvg(snapshot, items, events) {
  const rows=skydipRowsFrom(snapshot, items, events);
  if (!rows.length) return '<div>No skydip elevation geometry. Skydip plot needs elevation values in plan geometry.</div>';
  const ymin0=Math.min(...rows.map(r=>r.y)), ymax0=Math.max(...rows.map(r=>r.y));
  const xmin=1, xmax=Math.max(1, rows.length);
  const span=Math.max(1e-6, Math.abs(ymax0-ymin0));
  const ymin=ymin0 - Math.max(2, 0.08*span), ymax=ymax0 + Math.max(2, 0.08*span);
  const b={xmin:xmin-0.5, xmax:xmax+0.5, ymin, ymax};
  const poly = rows.map(r=>`${sx(r.x,b)},${sy(r.y,b)}`).join(' ');
  let grid = '';
  const ticks = 5;
  for (let i=0; i<ticks; i++) {
    const yv = ymin0 + (ymax0-ymin0) * (ticks===1 ? 0 : i/(ticks-1));
    const yy = sy(yv,b);
    grid += `<line x1="${PLOT.left}" y1="${yy}" x2="${PLOT.right}" y2="${yy}" stroke="var(--muted)" opacity="0.22" stroke-dasharray="3 4"/>`;
    grid += `<text class="plot-note" text-anchor="end" x="${PLOT.left-9}" y="${yy+4}">${esc(yv.toFixed(1))}</text>`;
  }
  const pts = rows.map(r=>{
    const x=sx(r.x,b), y=sy(r.y,b);
    const mode=String(r.label || '').toUpperCase();
    const label = mode === 'HOT' ? 'HOT/load' : (r.status==='current' ? `${mode || 'SKY'} now` : '');
    const text = label ? `<text x="${Math.min(PLOT.w-125, x+8)}" y="${Math.max(18, y-10)}" font-size="11" font-weight="700">${esc(label)}</text>` : '';
    return `<circle cx="${x}" cy="${y}" r="${r.status==='current'?6:4}" fill="${colorFor(r.status)}" stroke="${r.status==='current'?'var(--bg)':'none'}" stroke-width="2"/>${text}`;
  }).join('');
  const c = pointCounts(rows);
  const cur = rows.find(r=>r.status==='current');
  const curText = cur ? `Current elevation ${cur.y.toFixed(1)} deg at step ${cur.x}/${rows.length}` : `Elevation sequence ${rows.length} steps`;
  return `<svg viewBox="0 0 ${PLOT.w} ${PLOT.h}" role="img" preserveAspectRatio="xMidYMid meet"><rect x="1" y="1" width="${PLOT.w-2}" height="${PLOT.h-2}" fill="none" stroke="#9995"/><line x1="${PLOT.left}" y1="${PLOT.bottom}" x2="${PLOT.right}" y2="${PLOT.bottom}" stroke="#777"/><line x1="${PLOT.left}" y1="${PLOT.top}" x2="${PLOT.left}" y2="${PLOT.bottom}" stroke="#777"/>${grid}<polyline points="${poly}" fill="none" stroke="#9467bd" stroke-width="2"/>${pts}<text class="axis-label" x="${(PLOT.left+PLOT.right)/2-58}" y="${PLOT.bottom+42}">sequence step</text><text class="axis-label" transform="translate(17 ${(PLOT.top+PLOT.bottom)/2+35}) rotate(-90)">elevation (deg)</text><text x="${PLOT.left}" y="${PLOT.h-36}" font-size="10">Skydip ${c.done}/${c.total} done, cur ${c.current}, rem ${c.remaining}: ${esc(curText)}</text><text x="${PLOT.left}" y="${PLOT.h-18}" font-size="10">elevation vs sequence; HOT/load labelled when present</text></svg>`;
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
    if (snap?.display?.idle_live_telemetry) {
      msg += '<div class=\"notice\"><span class=\"muted\">No active observation.</span> Showing live antenna telemetry from ROS.</div>';
    } else if (finalState) {
      const updated = typeof timing.updated_at_unix === 'number' ? new Date(timing.updated_at_unix*1000).toLocaleTimeString() : '-';
      msg += `<div class=\"notice\"><span class=\"important\">This observation is ${esc(life.state)}.</span> Last update: ${esc(updated)}. Waiting for the next observation snapshot.</div>`;
    }
    // Do not show tracking as a full-width banner.  Tracking is an observing
    // position detail; show it in the Live Position card where the user can see
    // whether it is required for the current section.
    document.getElementById('message').innerHTML = msg;
    kv('observation', {...obs, state: life.state, record_label: shortRecordName(obs.record_name), target_label: displayTarget(snap), elapsed_sec: timing.elapsed_sec, remaining_sec: timing.estimated_remaining_sec, remaining_method_label: compactMethod(timing.remaining_method), remaining_confidence: timing.remaining_confidence, remaining_samples: timing.remaining_sample_count}, [['state','state'], ['type','type'], ['record','record_label'], ['obsfile','obs_file'], ['target','target_label'], ['elapsed','elapsed_sec'], ['remaining≈','remaining_sec'], ['ETA source','remaining_method_label'], ['ETA conf.','remaining_confidence'], ['ETA samples','remaining_samples']]);
    document.querySelector('#observation div:nth-child(2)').innerHTML = `<span class=\"status ${stateClass(life.state)}\">${esc(life.state ?? '-')}</span>`;
    const statusEvents = s.status_events || s.events || [];
    const psummary = observerPlanSummary(snap, s.plan || {}, statusEvents);
    kv('plan', psummary, [['progress','progress'], ['basis','basis'], ['current','current'], ['completed','completed'], ['remaining','remaining'], ['total','total'], ['mode','mode'], ['role (plan)','role'], ['target','target'], ['label','label']]);
    document.getElementById('bar').style.width = pct(plan).toFixed(1) + '%';
    renderPlanView(snap, s.plan || {}, statusEvents, s.server_time_unix);
    kvSmart('activity', activityRows(snap, s.server_time_unix));
    kvSmart('position', positionRows(snap, psummary, s.server_time_unix));
    kvSmart('geometry', geometryRows(snap));
    kvSmart('queue', commandQueueRows(snap, s.server_time_unix));
    kvSmart('spectrometer', spectrometerRows(snap, s.server_time_unix));
    kvSmart('trace', systemTraceRows(snap, s.server_time_unix));
    kvSmart('environment', environmentRows(snap));
    kv('paths', {snapshot: shortPathText(s.paths?.snapshot, 46), events: shortPathText(s.paths?.events, 46), plan: shortPathText(s.paths?.plan, 46)}, [['snapshot','snapshot'], ['events','events'], ['plan','plan']]);
    const rows = (s.events || []).slice(-10).map(ev => {
      const t = typeof ev.time_unix === 'number' ? new Date(ev.time_unix*1000).toLocaleTimeString() : String(ev.seq ?? '');
      const name = String(ev.event ?? '').replace('integration_', 'int_').replace('plan_item_', 'item_');
      const detail = [ev.phase, ev.id ?? ev.obs_id, ev.line_index ?? ev.line_index0].filter(x => x !== undefined && x !== null && x !== '').join('/');
      return `<tr><td title="${esc(ev.seq ?? '')}">${esc(t)}</td><td title="${esc(ev.event ?? '')}">${esc(name)}</td><td>${esc(detail)}</td></tr>`;
    }).join('');
    document.getElementById('events').innerHTML = `<table><thead><tr><th>time</th><th>event</th><th>detail</th></tr></thead><tbody>${rows || '<tr><td colspan="3">-</td></tr>'}</tbody></table>`;
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
    time_sync = TimeSyncPoller.from_site_config()

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
                _json_response(
                    self,
                    build_state(
                        root, events_limit=limit, live=live, time_sync=time_sync
                    ),
                )
                return
            if parsed.path == "/api/progress":
                _json_response(self, read_json(current_snapshot_path(root)) or {})
                return
            if parsed.path == "/api/events":
                snapshot = read_json(current_snapshot_path(root))
                path = latest_events_path(
                    root, snapshot if isinstance(snapshot, dict) else None
                )
                limit = _int_query(query, "limit", args.events, minimum=0, maximum=1000)
                _json_response(self, read_recent_events(path, limit))
                return
            if parsed.path == "/api/plan":
                snapshot = read_json(current_snapshot_path(root))
                path = latest_plan_path(
                    root, snapshot if isinstance(snapshot, dict) else None
                )
                _json_response(self, (read_json(path) if path else None) or {})
                return
            if parsed.path == "/api/health":
                _json_response(
                    self,
                    {"ok": True, "root": str(root), "server_time_unix": time.time()},
                )
                return
            _json_response(
                self,
                {"ok": False, "error": "not found", "path": parsed.path},
                status=404,
            )

    try:
        server = ThreadingHTTPServer((args.host, args.port), ProgressHandler)
    except OSError as exc:
        print(
            f"Could not start progress server on {args.host}:{args.port}: {exc}",
            file=sys.stderr,
        )
        live.close()
        if time_sync is not None:
            time_sync.close()
        return 1
    host, port = server.server_address[:2]
    print(f"Serving NECST progress dashboard at http://{host}:{port}/")
    print(f"Progress root: {root}")
    if time_sync is not None:
        print("Time sync monitor: enabled from [progress.time_sync]")
    try:
        server.serve_forever(poll_interval=0.5)
    except KeyboardInterrupt:
        print("\nStopping progress dashboard.")
        return 0
    finally:
        server.server_close()
        live.close()
        if time_sync is not None:
            time_sync.close()
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Show NECST observation progress")
    parser.add_argument(
        "--root",
        default=None,
        help="Progress root directory (default: NECST_PROGRESS_ROOT or /tmp/necst_progress)",
    )
    parser.add_argument(
        "--interval", type=float, default=1.0, help="Watch/follow refresh interval [s]"
    )
    parser.add_argument(
        "--events", type=int, default=8, help="Number of recent events to show"
    )
    parser.add_argument(
        "--compact", action="store_true", help="Print compact one-line view"
    )
    parser.add_argument(
        "--json", action="store_true", help="Print raw JSON snapshot or JSONL events"
    )
    parser.add_argument(
        "--watch", action="store_true", help="Continuously redraw a fixed dashboard"
    )
    parser.add_argument(
        "--once", action="store_true", help="Print once and exit (default)"
    )
    parser.add_argument(
        "--follow",
        action="store_true",
        help="Follow observation_events.jsonl as a scrolling log",
    )
    parser.add_argument(
        "--serve", action="store_true", help="Serve a lightweight web dashboard"
    )
    parser.add_argument(
        "--host",
        default="127.0.0.1",
        help="With --serve, bind address (default: 127.0.0.1)",
    )
    parser.add_argument(
        "--port", type=int, default=8080, help="With --serve, bind port (default: 8080)"
    )
    parser.add_argument(
        "--refresh-ms",
        type=int,
        default=500,
        help="With --serve, browser refresh interval [ms]",
    )
    parser.add_argument(
        "--quiet", action="store_true", help="With --serve, suppress HTTP request logs"
    )
    parser.add_argument(
        "--no-ros",
        action="store_true",
        help="Disable optional live ROS telemetry augmentation",
    )
    parser.add_argument(
        "--from-end",
        action="store_true",
        help="With --follow, start at end of current event log",
    )
    parser.add_argument(
        "--no-clear",
        action="store_true",
        help="With --watch, do not clear/redraw the terminal",
    )
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
