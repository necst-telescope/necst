"""Local launcher process tracking for the NECST operator console.

This module only manages local subprocesses started by the console.  It does
not send telescope, recorder, or spectrometer commands.
"""

from __future__ import annotations

import os
import signal
import subprocess
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple


def _now() -> float:
    return time.time()


def _safe_stem(text: Any) -> str:
    stem = "".join(ch if ch.isalnum() or ch in "._-" else "_" for ch in str(text or "launcher"))
    stem = stem.strip("._-") or "launcher"
    return stem[:80]


def make_launcher_log_paths(log_dir: Path, stem: Any) -> Tuple[Path, Path]:
    """Return unique stdout/stderr log paths under *log_dir*."""

    log_dir = Path(log_dir).expanduser()
    log_dir.mkdir(parents=True, exist_ok=True)
    base = f"{time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())}_{_safe_stem(stem)}_{os.getpid()}"
    stdout = log_dir / f"{base}.stdout.log"
    stderr = log_dir / f"{base}.stderr.log"
    idx = 2
    while stdout.exists() or stderr.exists():
        stdout = log_dir / f"{base}_{idx}.stdout.log"
        stderr = log_dir / f"{base}_{idx}.stderr.log"
        idx += 1
    return stdout, stderr


@dataclass
class ManagedProcessRecord:
    """A locally launched child process tracked by the operator console."""

    pid: int
    category: str
    label: str
    action: str = ""
    command: List[str] = field(default_factory=list)
    stdout_path: Optional[str] = None
    stderr_path: Optional[str] = None
    started_at: float = field(default_factory=_now)
    finished_at: Optional[float] = None
    returncode: Optional[int] = None
    status: str = "running"
    final_logged: bool = False
    stop_requested_at: Optional[float] = None
    stop_reason: str = ""
    _popen: Optional[subprocess.Popen[Any]] = field(default=None, repr=False, compare=False)

    def refresh(self) -> bool:
        """Poll the subprocess and return True if it finalized now."""

        if self.status in {"exited", "lost"}:
            return False
        popen = self._popen
        if popen is None:
            # If the console lost the handle, keep the record visible but mark it
            # as lost only once.  We cannot safely infer a return code.
            self.status = "lost"
            self.finished_at = self.finished_at or _now()
            return True
        rc = popen.poll()
        if rc is None:
            self.status = "stopping" if self.stop_requested_at is not None else "running"
            return False
        self.returncode = int(rc)
        self.status = "exited"
        self.finished_at = self.finished_at or _now()
        return True

    def request_stop(self, *, kill: bool = False, reason: str = "") -> None:
        self.refresh()
        if self.status in {"exited", "lost"}:
            return
        popen = self._popen
        if popen is None:
            self.status = "lost"
            self.finished_at = self.finished_at or _now()
            return
        self.stop_requested_at = self.stop_requested_at or _now()
        self.stop_reason = str(reason or self.stop_reason or "operator request")
        try:
            if kill:
                popen.kill()
            else:
                popen.terminate()
            self.status = "stopping"
        except ProcessLookupError:
            self.refresh()
        except Exception:
            # Preserve the record; a later refresh/status call may still reap it.
            self.status = "stopping"

    def wait(self, timeout: float) -> bool:
        popen = self._popen
        if popen is None:
            self.refresh()
            return True
        try:
            rc = popen.wait(timeout=max(0.0, float(timeout)))
            self.returncode = int(rc)
            self.status = "exited"
            self.finished_at = self.finished_at or _now()
            return True
        except subprocess.TimeoutExpired:
            self.refresh()
            return False

    def to_dict(self) -> Dict[str, Any]:
        self.refresh()
        return {
            "pid": self.pid,
            "category": self.category,
            "label": self.label,
            "action": self.action,
            "command": list(self.command),
            "stdout_path": self.stdout_path,
            "stderr_path": self.stderr_path,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "returncode": self.returncode,
            "status": self.status,
            "active": self.status not in {"exited", "lost"},
            "final_logged": bool(self.final_logged),
            "stop_requested_at": self.stop_requested_at,
            "stop_reason": self.stop_reason,
        }


class ProcessRegistry:
    """Thread-safe registry of console-owned launcher subprocesses."""

    def __init__(self) -> None:
        self._records: List[ManagedProcessRecord] = []
        self._lock = threading.RLock()

    def register_from_action_result(
        self,
        *,
        action: str,
        category: str,
        result_data: Mapping[str, Any],
        label: str,
    ) -> Optional[ManagedProcessRecord]:
        popen = result_data.get("_popen") if isinstance(result_data, Mapping) else None
        if popen is None or not hasattr(popen, "pid"):
            return None
        command = result_data.get("command") if isinstance(result_data, Mapping) else None
        if command is None and hasattr(popen, "args"):
            command = popen.args
        if isinstance(command, (str, bytes)):
            command_list = [str(command)]
        else:
            try:
                command_list = [str(x) for x in (command or [])]
            except Exception:
                command_list = []
        record = ManagedProcessRecord(
            pid=int(popen.pid),
            category=str(category or "launcher"),
            label=str(label or action or category or "launcher"),
            action=str(action or ""),
            command=command_list,
            stdout_path=str(result_data.get("stdout_path") or "") or None,
            stderr_path=str(result_data.get("stderr_path") or "") or None,
            _popen=popen,
        )
        with self._lock:
            self._records.append(record)
        return record

    def refresh(self) -> List[ManagedProcessRecord]:
        finalized: List[ManagedProcessRecord] = []
        with self._lock:
            for record in self._records:
                if record.refresh():
                    finalized.append(record)
        return finalized

    def all_records(self, *, refresh: bool = True) -> List[ManagedProcessRecord]:
        if refresh:
            self.refresh()
        with self._lock:
            return list(self._records)

    def active(self, *, category: Optional[str] = None) -> List[ManagedProcessRecord]:
        self.refresh()
        with self._lock:
            out = []
            for record in self._records:
                if record.status in {"exited", "lost"}:
                    continue
                if category is not None and record.category != category:
                    continue
                out.append(record)
            return out

    def counts(self) -> Dict[str, Any]:
        self.refresh()
        with self._lock:
            total = len(self._records)
            active = sum(1 for r in self._records if r.status not in {"exited", "lost"})
            by_category: Dict[str, int] = {}
            for record in self._records:
                by_category[record.category] = by_category.get(record.category, 0) + 1
            return {"total": total, "active": active, "finished": total - active, "by_category": by_category}

    def request_stop(
        self,
        *,
        pid: Optional[int] = None,
        category: Optional[str] = None,
        kill: bool = False,
        reason: str = "",
    ) -> List[ManagedProcessRecord]:
        with self._lock:
            targets = []
            for record in self.active(category=category):
                if pid is not None and int(record.pid) != int(pid):
                    continue
                targets.append(record)
            for record in targets:
                record.request_stop(kill=bool(kill), reason=reason)
            return list(targets)

    def graceful_shutdown(
        self,
        *,
        timeout_sec: float = 3.0,
        kill_timeout_sec: float = 1.0,
        reason: str = "console shutdown",
    ) -> Dict[str, Any]:
        targets = self.active()
        for record in targets:
            record.request_stop(kill=False, reason=reason)
        deadline = _now() + max(0.0, float(timeout_sec))
        while _now() < deadline and any(r.status not in {"exited", "lost"} for r in targets):
            self.refresh()
            time.sleep(0.05)
        still_active = [r for r in targets if r.status not in {"exited", "lost"}]
        for record in still_active:
            record.request_stop(kill=True, reason=reason + " (kill after timeout)")
        if still_active:
            kill_deadline = _now() + max(0.0, float(kill_timeout_sec))
            while _now() < kill_deadline and any(r.status not in {"exited", "lost"} for r in still_active):
                self.refresh()
                time.sleep(0.05)
        self.refresh()
        remaining = [r.to_dict() for r in self.active()]
        stopped = [r.to_dict() for r in targets]
        return {
            "ok": not bool(remaining),
            "message": "launcher shutdown cleanup finished" if not remaining else "some launchers remain active after cleanup",
            "terminated": stopped,
            "remaining": remaining,
            "counts": self.counts(),
        }
