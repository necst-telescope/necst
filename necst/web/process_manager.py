"""Subprocess lifecycle helpers for the NECST Operator Console.

The operator console starts observation and calibration launchers as child
processes so HTTP requests return immediately.  This module keeps the small
piece of lifecycle state needed by the console:

* reject accidental duplicate launcher starts;
* remember PID, command, cwd, and stdout/stderr log paths;
* reap child processes without blocking;
* expose a JSON-serialisable status snapshot.

It intentionally does not send telescope commands.  ABORT/STOP remain normal
operator actions handled elsewhere.
"""

from __future__ import annotations

import os
import signal
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple


_RUNNING_STATES = {"running", "terminating", "killing"}
_FINAL_STATES = {"exited", "lost", "unknown"}


def _now() -> float:
    return time.time()


def _iso_timestamp(ts: Optional[float] = None) -> str:
    value = _now() if ts is None else float(ts)
    return time.strftime("%Y-%m-%dT%H:%M:%S%z", time.localtime(value))


def _safe_name(text: str) -> str:
    out = []
    for ch in str(text or "process"):
        if ch.isalnum() or ch in {"-", "_"}:
            out.append(ch)
        else:
            out.append("_")
    name = "".join(out).strip("_")
    return name or "process"


@dataclass
class ManagedProcessRecord:
    """One launcher process started by the operator console."""

    pid: int
    action: str
    category: str
    command: List[str]
    cwd: str
    started_at: float = field(default_factory=_now)
    stdout_path: Optional[str] = None
    stderr_path: Optional[str] = None
    label: str = "launcher"
    status: str = "running"
    returncode: Optional[int] = None
    ended_at: Optional[float] = None
    final_logged: bool = False
    termination_requested_at: Optional[float] = None
    termination_signal: Optional[str] = None
    termination_reason: Optional[str] = None
    data: Dict[str, Any] = field(default_factory=dict)
    popen: Any = None

    def is_active(self) -> bool:
        return self.status in _RUNNING_STATES

    def to_dict(self) -> Dict[str, Any]:
        duration = None
        end = self.ended_at if self.ended_at is not None else _now()
        try:
            duration = max(0.0, float(end) - float(self.started_at))
        except Exception:
            duration = None
        return {
            "pid": self.pid,
            "action": self.action,
            "category": self.category,
            "label": self.label,
            "status": self.status,
            "returncode": self.returncode,
            "command": list(self.command),
            "cwd": self.cwd,
            "stdout_path": self.stdout_path,
            "stderr_path": self.stderr_path,
            "started_at": self.started_at,
            "started_at_iso": _iso_timestamp(self.started_at),
            "ended_at": self.ended_at,
            "ended_at_iso": _iso_timestamp(self.ended_at) if self.ended_at is not None else None,
            "duration_sec": duration,
            "final_logged": self.final_logged,
            "termination_requested_at": self.termination_requested_at,
            "termination_requested_at_iso": (
                _iso_timestamp(self.termination_requested_at)
                if self.termination_requested_at is not None
                else None
            ),
            "termination_signal": self.termination_signal,
            "termination_reason": self.termination_reason,
            "can_terminate": self.is_active(),
            "data": dict(self.data),
        }


class ProcessRegistry:
    """Track child launchers started by the console process."""

    def __init__(self) -> None:
        self._records: Dict[int, ManagedProcessRecord] = {}

    def register_from_action_result(
        self,
        *,
        action: str,
        category: str,
        result_data: Mapping[str, Any],
        label: Optional[str] = None,
    ) -> Optional[ManagedProcessRecord]:
        """Register a launcher if an action result contains a child PID."""

        pid_value = result_data.get("pid")
        if pid_value is None:
            return None
        try:
            pid = int(pid_value)
        except Exception as exc:
            raise ValueError(f"invalid launcher pid in action result: {pid_value!r}") from exc
        command = [str(x) for x in result_data.get("command", [])]
        cwd = str(result_data.get("cwd") or "")
        data = dict(result_data)
        popen = data.pop("_popen", None)
        record = ManagedProcessRecord(
            pid=pid,
            action=str(action),
            category=str(category),
            label=str(label or action),
            command=command,
            cwd=cwd,
            stdout_path=(
                str(result_data.get("stdout_path"))
                if result_data.get("stdout_path") is not None
                else None
            ),
            stderr_path=(
                str(result_data.get("stderr_path"))
                if result_data.get("stderr_path") is not None
                else None
            ),
            data=data,
            popen=popen,
        )
        self._records[pid] = record
        return record

    def refresh(self) -> List[ManagedProcessRecord]:
        """Non-blockingly reap children and return records newly finalized."""

        finalized: List[ManagedProcessRecord] = []
        for record in list(self._records.values()):
            if not record.is_active():
                continue
            if record.popen is not None:
                returncode = record.popen.poll()
                if returncode is None:
                    continue
                record.ended_at = _now()
                record.status = "exited"
                record.returncode = int(returncode)
                finalized.append(record)
                continue
            try:
                waited_pid, status = os.waitpid(record.pid, os.WNOHANG)
            except ChildProcessError:
                if not self._pid_exists(record.pid):
                    record.status = "lost"
                    record.ended_at = _now()
                    finalized.append(record)
                continue
            except OSError:
                if not self._pid_exists(record.pid):
                    record.status = "lost"
                    record.ended_at = _now()
                    finalized.append(record)
                continue
            if waited_pid == 0:
                continue
            record.ended_at = _now()
            record.status = "exited"
            if os.WIFEXITED(status):
                record.returncode = os.WEXITSTATUS(status)
            elif os.WIFSIGNALED(status):
                record.returncode = -os.WTERMSIG(status)
            else:
                record.returncode = None
            finalized.append(record)
        return finalized

    def active(self, *, category: Optional[str] = None) -> List[ManagedProcessRecord]:
        self.refresh()
        records = [r for r in self._records.values() if r.is_active()]
        if category is not None:
            records = [r for r in records if r.category == category]
        return sorted(records, key=lambda r: r.started_at)

    def all_records(self, *, refresh: bool = True) -> List[ManagedProcessRecord]:
        if refresh:
            self.refresh()
        return sorted(self._records.values(), key=lambda r: r.started_at, reverse=True)

    def counts(self) -> Dict[str, int]:
        self.refresh()
        out = {
            "total": 0,
            "running": 0,
            "terminating": 0,
            "killing": 0,
            "active": 0,
            "exited": 0,
            "failed": 0,
            "lost": 0,
        }
        for record in self._records.values():
            out["total"] += 1
            if record.status in _RUNNING_STATES:
                out["active"] += 1
                out[record.status] = out.get(record.status, 0) + 1
            elif record.status == "exited":
                out["exited"] += 1
                if record.returncode not in (0, None):
                    out["failed"] += 1
            elif record.status == "lost":
                out["lost"] += 1
        return out

    def request_stop(
        self,
        *,
        pid: Optional[int] = None,
        category: Optional[str] = None,
        kill: bool = False,
        reason: str = "operator requested launcher termination",
    ) -> List[ManagedProcessRecord]:
        """Request termination of active launcher children.

        This only acts on local child processes that were started and registered
        by the operator console.  It does not send telescope STOP or ABORT
        commands; those remain separate safety/operator actions.
        """

        self.refresh()
        if pid is not None:
            try:
                pid_int = int(pid)
            except Exception as exc:
                raise ValueError(f"invalid launcher pid: {pid!r}") from exc
            records = [self._records[pid_int]] if pid_int in self._records else []
        else:
            records = self.active(category=category)
        records = [r for r in records if r.is_active()]
        sig = signal.SIGKILL if kill else signal.SIGTERM
        sig_name = "SIGKILL" if kill else "SIGTERM"
        next_state = "killing" if kill else "terminating"
        now = _now()
        for record in records:
            if record.status == "killing":
                continue
            try:
                if record.popen is not None:
                    if kill:
                        record.popen.kill()
                    else:
                        record.popen.terminate()
                else:
                    os.kill(int(record.pid), sig)
            except ProcessLookupError:
                record.status = "lost"
                record.ended_at = _now()
                continue
            except Exception as exc:
                record.termination_reason = f"{reason}; failed to send {sig_name}: {exc}"
                continue
            record.status = next_state
            record.termination_requested_at = now
            record.termination_signal = sig_name
            record.termination_reason = reason
        return records


    def graceful_shutdown(
        self,
        *,
        timeout_sec: float = 3.0,
        kill_timeout_sec: float = 1.0,
        reason: str = "operator console shutdown",
    ) -> Dict[str, Any]:
        """Terminate all active local launcher children and return a summary.

        This is intentionally limited to child processes registered by this
        console.  It does not send telescope STOP/ABORT commands.  The caller
        should use the separate safety/operator actions for telescope state.
        """

        started_at = _now()
        summary: Dict[str, Any] = {
            "started_at": started_at,
            "started_at_iso": _iso_timestamp(started_at),
            "timeout_sec": float(timeout_sec),
            "kill_timeout_sec": float(kill_timeout_sec),
            "terminated": [],
            "killed": [],
            "remaining": [],
            "finalized": [],
            "ok": True,
            "message": "no active local launcher subprocesses",
        }

        active_before = self.active()
        if not active_before:
            summary["finished_at"] = _now()
            summary["finished_at_iso"] = _iso_timestamp(summary["finished_at"])
            summary["counts"] = self.counts()
            return summary

        terminated = self.request_stop(reason=reason, kill=False)
        summary["terminated"] = [r.to_dict() for r in terminated]

        deadline = _now() + max(0.0, float(timeout_sec))
        while _now() < deadline:
            finalized = self.refresh()
            if finalized:
                summary["finalized"].extend(r.to_dict() for r in finalized)
            if not self.active():
                break
            time.sleep(0.05)

        remaining = self.active()
        if remaining:
            killed = self.request_stop(
                reason=f"{reason}; escalated after graceful timeout",
                kill=True,
            )
            summary["killed"] = [r.to_dict() for r in killed]
            kill_deadline = _now() + max(0.0, float(kill_timeout_sec))
            while _now() < kill_deadline:
                finalized = self.refresh()
                if finalized:
                    summary["finalized"].extend(r.to_dict() for r in finalized)
                if not self.active():
                    break
                time.sleep(0.05)

        remaining = self.active()
        summary["remaining"] = [r.to_dict() for r in remaining]
        summary["counts"] = self.counts()
        finished_at = _now()
        summary["finished_at"] = finished_at
        summary["finished_at_iso"] = _iso_timestamp(finished_at)
        summary["ok"] = not bool(remaining)
        if remaining:
            summary["message"] = (
                f"{len(remaining)} local launcher subprocess(es) remain active after shutdown cleanup"
            )
        else:
            summary["message"] = "all local launcher subprocesses were cleaned up"
        return summary

    @staticmethod
    def _pid_exists(pid: int) -> bool:
        try:
            os.kill(int(pid), 0)
            return True
        except ProcessLookupError:
            return False
        except PermissionError:
            return True
        except Exception:
            return False


def make_launcher_log_paths(log_dir: Path, action: str) -> Tuple[Path, Path]:
    """Create deterministic stdout/stderr log paths for a launcher start."""

    directory = Path(log_dir)
    directory.mkdir(parents=True, exist_ok=True)
    stamp = time.strftime("%Y%m%d_%H%M%S", time.localtime())
    token = uuid.uuid4().hex[:8]
    stem = f"{stamp}_{_safe_name(action)}_{token}"
    return directory / f"{stem}.stdout.log", directory / f"{stem}.stderr.log"
