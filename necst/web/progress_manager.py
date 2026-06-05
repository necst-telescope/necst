"""Managed progress-monitor lifecycle helpers for the NECST Operator Console.

The operator console may open a progress monitor in two different situations:

* a progress.py web server is already running outside the console;
* the console starts its own progress.py server for operator convenience.

Only the second case is owned by this console process.  Therefore console
shutdown must stop only the process that the console launched, and must leave an
external progress monitor untouched.
"""

from __future__ import annotations

import json
import subprocess
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from . import process_manager


def _now() -> float:
    return time.time()


@dataclass
class ProgressMonitorState:
    """Small JSON-serialisable snapshot of progress-monitor ownership."""

    url: str
    running: bool = False
    owned_by_console: bool = False
    pid: Optional[int] = None
    status: str = "unknown"
    message: str = ""
    health: Dict[str, Any] = field(default_factory=dict)
    stdout_path: Optional[str] = None
    stderr_path: Optional[str] = None
    command: List[str] = field(default_factory=list)
    started_at: Optional[float] = None
    returncode: Optional[int] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "url": self.url,
            "running": bool(self.running),
            "owned_by_console": bool(self.owned_by_console),
            "pid": self.pid,
            "status": self.status,
            "message": self.message,
            "health": dict(self.health),
            "stdout_path": self.stdout_path,
            "stderr_path": self.stderr_path,
            "command": list(self.command),
            "started_at": self.started_at,
            "returncode": self.returncode,
        }


class ProgressMonitorManager:
    """Start, detect, and stop a progress.py web server when appropriate."""

    def __init__(
        self,
        *,
        progress_script: Path,
        progress_root: Path,
        host: str,
        port: int,
        url: Optional[str] = None,
        log_dir: Optional[Path] = None,
        refresh_ms: int = 500,
        no_ros: bool = False,
        quiet: bool = True,
        python_executable: Optional[str] = None,
    ) -> None:
        self.progress_script = Path(progress_script)
        self.progress_root = Path(progress_root)
        self.host = str(host)
        self.port = int(port)
        self.url = str(url or f"http://{self.host}:{self.port}/")
        self.log_dir = Path(log_dir) if log_dir is not None else None
        self.refresh_ms = max(250, int(refresh_ms))
        self.no_ros = bool(no_ros)
        self.quiet = bool(quiet)
        self.python_executable = str(python_executable or sys.executable)
        self._popen: Optional[subprocess.Popen[Any]] = None
        self._stdout_fh: Any = None
        self._stderr_fh: Any = None
        self._stdout_path: Optional[Path] = None
        self._stderr_path: Optional[Path] = None
        self._command: List[str] = []
        self._started_at: Optional[float] = None
        self._last_message: str = ""
        self._last_health: Dict[str, Any] = {}
        self._external_detected: bool = False

    def _health_url(self) -> str:
        return self.url.rstrip("/") + "/api/health"

    def check_health(self, *, timeout_sec: float = 0.25) -> Tuple[bool, Dict[str, Any], str]:
        """Return ``(ok, payload, message)`` from /api/health."""

        try:
            with urllib.request.urlopen(self._health_url(), timeout=float(timeout_sec)) as resp:
                raw = resp.read(1024 * 256)
            payload = json.loads(raw.decode("utf-8") or "{}")
            if not isinstance(payload, dict):
                payload = {"payload": payload}
            ok = bool(payload.get("ok", True))
            message = "progress monitor health OK" if ok else "progress monitor health returned not-ok"
            self._last_health = payload
            self._last_message = message
            return ok, payload, message
        except urllib.error.HTTPError as exc:
            message = f"progress monitor health HTTP error: {exc.code}"
            return False, {}, message
        except Exception as exc:
            message = f"progress monitor health unavailable: {exc}"
            return False, {}, message

    def _poll_owned_process(self) -> Optional[int]:
        if self._popen is None:
            return None
        rc = self._popen.poll()
        return None if rc is None else int(rc)

    def _is_owned_running(self) -> bool:
        return self._popen is not None and self._poll_owned_process() is None

    def _make_command(self) -> List[str]:
        command = [
            self.python_executable,
            str(self.progress_script),
            "--serve",
            "--root",
            str(self.progress_root),
            "--host",
            self.host,
            "--port",
            str(self.port),
            "--refresh-ms",
            str(self.refresh_ms),
        ]
        if self.quiet:
            command.append("--quiet")
        if self.no_ros:
            command.append("--no-ros")
        return command

    def launch(self) -> Tuple[bool, str, Dict[str, Any]]:
        """Launch progress.py if no external or owned server is already healthy."""

        if self._is_owned_running():
            ok, health, _message = self.check_health(timeout_sec=0.5)
            data = self.status().to_dict()
            data["health"] = health
            data["progress_url"] = self.url
            message = "managed progress monitor is already running"
            self._last_message = message
            return True, message, data

        ok, health, message = self.check_health(timeout_sec=0.35)
        if ok:
            data = self.status().to_dict()
            data.update(
                {
                    "running": True,
                    "owned_by_console": False,
                    "status": "external",
                    "health": health,
                    "progress_url": self.url,
                }
            )
            message = "external progress monitor is already running; it will not be stopped by this console"
            self._last_message = message
            self._external_detected = True
            return True, message, data

        if not self.progress_script.exists():
            message = f"progress.py not found: {self.progress_script}"
            self._last_message = message
            return False, message, self.status().to_dict()

        stdout_path = stderr_path = None
        stdout_fh = stderr_fh = subprocess.DEVNULL
        if self.log_dir is not None:
            stdout_path, stderr_path = process_manager.make_launcher_log_paths(
                Path(self.log_dir), "progress_monitor"
            )
            stdout_fh = stdout_path.open("ab")
            stderr_fh = stderr_path.open("ab")
        command = self._make_command()
        try:
            popen = subprocess.Popen(
                command,
                cwd=str(self.progress_script.resolve().parents[1]),
                stdout=stdout_fh,
                stderr=stderr_fh,
                close_fds=True,
            )
        except Exception as exc:
            for handle in (stdout_fh, stderr_fh):
                try:
                    if handle is not subprocess.DEVNULL:
                        handle.close()
                except Exception:
                    pass
            message = f"failed to start progress monitor: {exc}"
            self._last_message = message
            return False, message, self.status().to_dict()

        self._popen = popen
        self._stdout_fh = stdout_fh if stdout_fh is not subprocess.DEVNULL else None
        self._stderr_fh = stderr_fh if stderr_fh is not subprocess.DEVNULL else None
        self._stdout_path = stdout_path
        self._stderr_path = stderr_path
        self._command = command
        self._started_at = _now()

        # Give the HTTP server a short chance to bind and answer health.  If it
        # is still starting but the process is alive, report success with a
        # pending health status; the browser can refresh and try again.
        last_message = "progress monitor launch requested"
        for _ in range(20):
            if popen.poll() is not None:
                break
            ok, health, health_message = self.check_health(timeout_sec=0.15)
            if ok:
                data = self.status().to_dict()
                data["health"] = health
                data["progress_url"] = self.url
                message = "managed progress monitor started"
                self._last_message = message
                return True, message, data
            last_message = health_message
            time.sleep(0.05)

        rc = popen.poll()
        data = self.status().to_dict()
        data["progress_url"] = self.url
        if rc is None:
            message = f"managed progress monitor started; health is pending ({last_message})"
            self._last_message = message
            return True, message, data
        message = f"progress monitor exited during startup with return code {rc}"
        self._last_message = message
        return False, message, data

    def stop_if_owned(self, *, timeout_sec: float = 2.0) -> Tuple[bool, str, Dict[str, Any]]:
        """Stop only the progress monitor started by this console."""

        popen = self._popen
        if popen is None:
            return True, "no console-owned progress monitor to stop", self.status().to_dict()
        if popen.poll() is None:
            try:
                popen.terminate()
                popen.wait(timeout=float(timeout_sec))
            except subprocess.TimeoutExpired:
                try:
                    popen.kill()
                    popen.wait(timeout=1.0)
                except Exception:
                    pass
            except Exception:
                pass
        rc = popen.poll()
        message = f"console-owned progress monitor stopped with return code {rc}"
        self._last_message = message
        data = self.status().to_dict()
        self._popen = None
        for attr in ("_stdout_fh", "_stderr_fh"):
            handle = getattr(self, attr, None)
            if handle is not None:
                try:
                    handle.close()
                except Exception:
                    pass
                setattr(self, attr, None)
        return True, message, data

    def status(self, *, check_external: bool = False) -> ProgressMonitorState:
        """Return current monitor state without sending telescope commands."""

        rc = self._poll_owned_process()
        owned_running = self._is_owned_running()
        running = owned_running
        status = "managed" if owned_running else "not_running"
        health: Dict[str, Any] = dict(self._last_health)
        message = self._last_message
        if (check_external or self._external_detected) and not owned_running:
            ok, health, message = self.check_health(timeout_sec=0.20)
            running = ok
            status = "external" if ok else "not_running"
            self._external_detected = bool(ok)
        elif owned_running:
            ok, health, message = self.check_health(timeout_sec=0.20)
            running = True
            status = "managed" if ok else "managed_starting"
        elif self._popen is not None and rc is not None:
            running = False
            status = "exited"
            message = f"console-owned progress monitor exited with return code {rc}"
        return ProgressMonitorState(
            url=self.url,
            running=bool(running),
            owned_by_console=bool(owned_running),
            pid=int(self._popen.pid) if self._popen is not None else None,
            status=status,
            message=message,
            health=health,
            stdout_path=str(self._stdout_path) if self._stdout_path is not None else None,
            stderr_path=str(self._stderr_path) if self._stderr_path is not None else None,
            command=list(self._command),
            started_at=self._started_at,
            returncode=rc,
        )
