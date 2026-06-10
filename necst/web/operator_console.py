"""HTTP helpers for the NECST Operator Console.

This module is the thin boundary between the browser-facing Operator Console
and the shared read-only status / operator action layers.  It deliberately keeps
validation and command dispatch small and explicit:

* read-only status is built from :mod:`status_model`;
* write-like actions are sent only through ``necst.core.operator_actions``;
* unimplemented or site-disabled actions are rejected before any telescope
  command can be sent.
"""

from __future__ import annotations

import importlib
import importlib.util
import sys
import json
import math
import os
import shutil
import threading
import time
import uuid
import webbrowser
import urllib.parse
from dataclasses import dataclass, field
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Callable, Dict, List, Mapping, Optional, Tuple

from . import log_reader, observation_log, process_manager, progress_manager, self_check, site_config, status_model, live_telemetry
from ..az_unwrap_limits import assert_mount_az_allowed_when_unwrap_disabled


JsonDict = Dict[str, Any]


SUPPORTED_ACTION_MODES = {"live", "dry-run"}

LIVE_WRITE_ACTIONS = {
    "acquire_authority",
    "mount_move",
    "start_tracking",
    "start_observation",
    "run_rsky",
    "run_skydip",
    "chopper_in",
    "chopper_out",
    "chopper_alarm_reset",
    "chopper_home",
    "chopper_recover",
}

SAFETY_ACTIONS = {
    "stop",
    "stop_tracking",
    "abort_observation",
}

# Operations that should be mutually exclusive from a human-operator point of
# view. UI disabled states are not sufficient: stale browser tabs, double
# clicks, or direct /api/action calls must be rejected server-side too.
EXCLUSIVE_START_ACTIONS = {
    "mount_move",
    "start_tracking",
    "start_observation",
    "run_rsky",
    "run_skydip",
}

# Short server-side mutex for Start-like commands.  This closes the gap where
# two browsers, a stale tab, or a very fast double-click can submit another
# Start before ROS/progress/launcher status has had time to reflect the first
# accepted command.  Keep the hard blocking window short so a quick mount move
# or an already-complete command does not block normal operation for long.
EXCLUSIVE_START_BLOCK_SEC = 3.0

# Longer status hint for human operators.  A browser reload or a second browser
# should still show "STARTING..." while the accepted command is waiting for
# ROS/progress confirmation.  This is display state, not a long mutex; finished
# launchers and confirmed live activity supersede it.
EXCLUSIVE_START_STATUS_SEC = 15.0

# STOP / ABORT are release requests.  After a short grace, if the system
# lifecycle is final/idle and /ctrl/antenna/altaz has disappeared, the operator
# UI must stop trusting stale SKY/section_status or encoder-motion hysteresis.
# This prevents STOP REQUESTED from degrading into ATTENTION while the telescope
# is already stopped and ready for the next command.
SAFETY_RELEASE_ASSUME_IDLE_AFTER_SEC = 1.0

# A progress sidecar can remain in a non-final lifecycle if an observation
# launcher dies or blocks before writing its final snapshot.  Do not keep the
# operator UI in OBSERVATION RUNNING forever when no local launcher is active
# and the sidecar has not been updated for this long.  This only changes the
# console status; it does not send telescope commands or delete data.
STALE_OBSERVATION_PROGRESS_SEC = float(os.environ.get("NECST_CONSOLE_STALE_OBSERVATION_SEC", "60.0"))
ABORT_STUCK_OBSERVATION_SEC = float(os.environ.get("NECST_CONSOLE_ABORT_STUCK_OBSERVATION_SEC", "15.0"))

LIVE_ACTION_GUARD_MESSAGE = (
    "live write actions are guarded by --guard-live-actions; "
    "use --action-mode dry-run for normal no-hardware validation"
)


@dataclass
class ConsoleLogEntry:
    """One operator-log line displayed in the browser and JSONL log."""

    time: str
    ok: bool
    message: str
    action: str = ""
    session_id: str = ""
    data: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ConsoleAuthorityState:
    """Operator-console authority state.

    ``held`` and ``session_id`` are the browser/session gate.  When running in
    live mode, ``necst_held`` means this console process also holds actual NECST
    authority through a persistent Commander.  Observation/RSky/SkyDip launchers
    run as separate processes, so the console must release actual authority
    before starting them to avoid blocking the child process.
    """

    held: bool = False
    session_id: Optional[str] = None
    necst_held: bool = False
    necst_identity: Optional[str] = None
    mode: str = "none"
    note: str = ""


@dataclass
class OperatorConsoleState:
    """Mutable server state that is not part of NECST telemetry."""

    telescope: str
    progress_root: Path
    progress_url: str
    progress_monitor: Optional[progress_manager.ProgressMonitorManager] = None
    action_mode: str = "live"
    live_actions_enabled: bool = True
    status_refresh_ms: int = 1000
    status_no_ros: bool = False
    quiet: bool = False
    events_limit: int = 12
    site_summary: site_config.SiteConfigSummary = field(
        default_factory=lambda: site_config.resolve_site_config()
    )
    mount_limits: Dict[str, float] = field(default_factory=dict)
    site_capabilities: Dict[str, bool] = field(default_factory=dict)
    chopper_config: Dict[str, Any] = field(default_factory=dict)
    live_cache: Optional[live_telemetry.LiveTelemetryCache] = None
    observation_log: Optional[observation_log.ObservationLogManager] = None
    operator_log_path: Optional[Path] = None
    launcher_log_dir: Optional[Path] = None
    obs_roots: List[Path] = field(default_factory=list)
    process_registry: process_manager.ProcessRegistry = field(
        default_factory=process_manager.ProcessRegistry
    )
    log: List[ConsoleLogEntry] = field(default_factory=list)
    authority: ConsoleAuthorityState = field(default_factory=ConsoleAuthorityState)
    authority_handle: Any = None
    last_command_az: Optional[float] = None
    last_command_el: Optional[float] = None
    # Last fixed Az/El requested from the Manual telescope control card.  This is
    # intentionally separate from Command Az/El display values: /ctrl/antenna/altaz
    # can disappear after STOP, while the operator still needs the UI to recognize
    # that the mount is already at the last requested fixed coordinate.
    last_mount_target_az: Optional[float] = None
    last_mount_target_el: Optional[float] = None
    last_mount_target_started_at: Optional[float] = None
    last_mount_target_reached_since: Optional[float] = None
    # Last safety release request sent from this console.  STOP/ABORT are
    # operator intent to release the current manual/observation control, not a
    # request to keep waiting for the previous fixed Az/El target.
    last_safety_release_requested_at: Optional[float] = None
    last_manual_state: str = "idle"
    last_active_task: str = "none"
    last_observation_abort_requested_at: Optional[float] = None
    last_observation_stop_requested_at: Optional[float] = None
    last_observation_safety_labels: List[str] = field(default_factory=list)
    last_chopper_state: str = "unknown"
    last_chopper_position: Optional[float] = None
    shutdown_requested: bool = False
    cleanup_started_at: Optional[float] = None
    cleanup_finished_at: Optional[float] = None
    cleanup_status: str = "not_started"
    cleanup_summary: Dict[str, Any] = field(default_factory=dict)
    shutdown_terminate_launchers: bool = True
    shutdown_launcher_timeout_sec: float = 3.0
    shutdown_launcher_kill_timeout_sec: float = 1.0
    exclusive_start_action: Optional[str] = None
    exclusive_start_session_id: Optional[str] = None
    exclusive_start_started_at: Optional[float] = None
    exclusive_start_message: str = ""
    safe_start: bool = False
    rescue_mode: bool = False
    local_state_reset_archive: Optional[str] = None
    local_state_reset_summary: Dict[str, Any] = field(default_factory=dict)
    last_status_exception_message: str = ""
    last_status_exception_at: Optional[float] = None
    lock: threading.RLock = field(default_factory=threading.RLock)

    def add_log(
        self,
        ok: bool,
        message: str,
        *,
        action: str = "",
        session_id: str = "",
        data: Optional[Mapping[str, Any]] = None,
    ) -> None:
        timestamp = time.strftime("%H:%M:%S", time.localtime())
        entry = ConsoleLogEntry(
            timestamp,
            bool(ok),
            str(message),
            str(action or ""),
            str(session_id or ""),
            dict(data or {}),
        )
        self.log.insert(0, entry)
        del self.log[200:]
        if self.operator_log_path is not None:
            try:
                self.operator_log_path.parent.mkdir(parents=True, exist_ok=True)
                payload = dict(entry.__dict__)
                payload["time_iso"] = time.strftime("%Y-%m-%dT%H:%M:%S%z", time.localtime())
                with self.operator_log_path.open("a", encoding="utf-8") as fh:
                    fh.write(json.dumps(payload, ensure_ascii=False, sort_keys=True) + "\n")
            except Exception:
                # The browser log must remain usable even if the persistent log
                # path becomes temporarily unwritable.
                pass


def _finite_float(value: Any, *, name: str) -> float:
    try:
        number = float(value)
    except Exception as exc:
        raise ValueError(f"{name} is not a finite number") from exc
    if not math.isfinite(number):
        raise ValueError(f"{name} is not a finite number")
    return number


def _optional_positive_int(value: Any, *, name: str) -> Optional[int]:
    if value in (None, ""):
        return None
    try:
        number = int(value)
    except Exception as exc:
        raise ValueError(f"{name} must be an integer") from exc
    if number <= 0:
        raise ValueError(f"{name} must be positive")
    return number


def resolve_site_summary(
    *,
    site_config_path: Optional[os.PathLike[str] | str] = None,
    az_min: Optional[float] = None,
    az_max: Optional[float] = None,
    el_min: Optional[float] = None,
    el_max: Optional[float] = None,
) -> site_config.SiteConfigSummary:
    """Resolve active site settings for console display and validation."""

    return site_config.resolve_site_config(
        site_config_path=site_config_path,
        az_min=az_min,
        az_max=az_max,
        el_min=el_min,
        el_max=el_max,
    )


def resolve_mount_limits(
    *,
    az_min: Optional[float] = None,
    az_max: Optional[float] = None,
    el_min: Optional[float] = None,
    el_max: Optional[float] = None,
    site_config_path: Optional[os.PathLike[str] | str] = None,
) -> Dict[str, float]:
    """Resolve mount limits from site config, then CLI fallback values."""

    return dict(
        resolve_site_summary(
            site_config_path=site_config_path,
            az_min=az_min,
            az_max=az_max,
            el_min=el_min,
            el_max=el_max,
        ).mount_limits
    )

def validate_mount_target(
    params: Mapping[str, Any], mount_limits: Mapping[str, Any]
) -> Tuple[float, float]:
    """Validate mount mechanical Az/El in degrees using site TOML limits."""

    az = _finite_float(params.get("az"), name="Az")
    el = _finite_float(params.get("el"), name="El")
    required = ("az_min", "az_max", "el_min", "el_max")
    if not all(k in mount_limits for k in required):
        raise ValueError("site TOML mount limits are not available")
    az_min = _finite_float(mount_limits.get("az_min"), name="Az min")
    az_max = _finite_float(mount_limits.get("az_max"), name="Az max")
    el_min = _finite_float(mount_limits.get("el_min"), name="El min")
    el_max = _finite_float(mount_limits.get("el_max"), name="El max")
    if az_min >= az_max:
        raise ValueError("site TOML Az min/max are invalid")
    if el_min >= el_max:
        raise ValueError("site TOML El min/max are invalid")
    if not (az_min <= az <= az_max):
        raise ValueError(f"Az={az:g} deg is outside site TOML range {az_min:g}..{az_max:g} deg")
    assert_mount_az_allowed_when_unwrap_disabled(
        az, action_label="operator console mount move"
    )
    if not (el_min <= el <= el_max):
        raise ValueError(f"El={el:g} deg is outside site TOML range {el_min:g}..{el_max:g} deg")
    return az, el


def validate_observation_selection(params: Mapping[str, Any]) -> Tuple[str, str, Optional[int]]:
    """Static obs-file selection validation; does not touch hardware."""

    mode = str(params.get("mode") or "").strip().lower()
    allowed_modes = {"otf", "psw", "grid", "radio_pointing"}
    if mode not in allowed_modes:
        raise ValueError(f"unsupported observation mode: {mode!r}")
    path = str(params.get("file") or "").strip()
    if not path:
        raise ValueError("obs file path/name is empty")
    if not path.lower().endswith((".obs", ".toml")):
        raise ValueError("obs file extension must be .obs or .toml")
    channel = _optional_positive_int(params.get("channel"), name="channel override")
    return mode, path, channel


def _split_env_paths(value: str) -> List[str]:
    parts: List[str] = []
    for chunk in str(value or "").replace(";", os.pathsep).split(os.pathsep):
        chunk = chunk.strip()
        if chunk:
            parts.append(chunk)
    return parts


def _dir_has_obs_files(path: Path) -> bool:
    """Return True when *path* directly contains likely obs files.

    This deliberately checks only one directory level.  The browser can then
    descend interactively; startup should not recursively scan large mounted
    disks or network filesystems.
    """

    try:
        for child in path.iterdir():
            if child.is_file() and child.suffix.lower() in {".obs", ".toml"}:
                return True
    except Exception:
        return False
    return False


def _auto_obs_root_candidates() -> List[Path]:
    """Return NECST-side browse roots visible to this console process.

    The browser cannot see Docker/container files directly.  The console
    therefore exposes a read-only file chooser over the filesystem visible to
    the process running this web server.  When no explicit ``--obs-root`` is
    supplied we choose practical starting locations, including the filesystem
    root, so the UI behaves like an ordinary file chooser rather than showing
    an empty selector.
    """

    raw_candidates: List[str] = []
    for raw in (
        os.environ.get("NECST_CONSOLE_OBS_BASE", ""),
        os.environ.get("NECST_OBS_BASE", ""),
        os.environ.get("NECST_RECORD_ROOT", ""),
        os.environ.get("ROS2_WS", ""),
        os.environ.get("COLCON_PREFIX_PATH", "").split(os.pathsep)[0] if os.environ.get("COLCON_PREFIX_PATH") else "",
        str(Path.cwd()),
        str(Path.home()),
        str(Path.home() / "obs"),
        str(Path.home() / "observations"),
        str(Path.home() / "data"),
        str(Path.home() / "data" / "obs"),
        str(Path.home() / "data" / "observations"),
        "/root/obs",
        "/root/observations",
        "/root/data",
        "/root/ros2_ws",
        "/root",
        "/home/necst/obs",
        "/home/necst/observations",
        "/data/obs",
        "/data/observations",
        "/data/observations/current",
        "/data",
        "/workspaces",
        "/workspace",
        "/mnt/data",
        "/",
    ):
        raw = str(raw or "").strip()
        if raw:
            raw_candidates.append(raw)

    # Put obs-like directories first when they exist, but do not hide the
    # broader filesystem roots.  Users can still narrow the chooser by passing
    # --obs-root or NECST_CONSOLE_OBS_ROOTS.
    obs_names = ("obs", "obsfiles", "obs_files", "observation", "observations", "observation_files")
    expanded: List[Path] = []
    for raw in raw_candidates:
        try:
            base = Path(raw).expanduser()
        except Exception:
            continue
        name = base.name.lower()
        if name not in obs_names:
            expanded.extend(base / name for name in obs_names)
        expanded.append(base)
    return expanded


def resolve_obs_roots(configured_roots: Optional[List[os.PathLike[str] | str]] = None) -> List[Path]:
    """Return NECST-side browse roots for preview and obs selection.

    If ``--obs-root`` or ``NECST_CONSOLE_OBS_ROOTS`` is supplied, those paths
    define the chooser locations.  Otherwise the console provides practical
    defaults visible to the process running the console, including Home, common
    obs/data directories, the current directory, and ``/``.  This makes the
    chooser usable without command-line options while still allowing a site to
    restrict it explicitly.
    """

    configured: List[os.PathLike[str] | str] = []
    if configured_roots:
        configured.extend(str(p) for p in configured_roots if str(p or "").strip())
    for env_name in ("NECST_CONSOLE_OBS_ROOTS", "NECST_OBS_ROOTS"):
        configured.extend(_split_env_paths(os.environ.get(env_name, "")))
    for env_name in ("NECST_CONSOLE_OBS_ROOT", "NECST_OBS_ROOT", "NECST_OBS_DIR"):
        val = os.environ.get(env_name, "").strip()
        if val:
            configured.append(val)

    candidates = configured if configured else _auto_obs_root_candidates()

    roots: List[Path] = []
    seen: set[str] = set()
    for raw in candidates:
        try:
            resolved = Path(raw).expanduser().resolve()
        except Exception:
            continue
        key = str(resolved)
        if key in seen:
            continue
        seen.add(key)
        if resolved.exists() and resolved.is_dir():
            roots.append(resolved)
    return roots


def _obs_root_label(path: Path) -> str:
    try:
        home = Path.home().resolve()
        if path.resolve() == home:
            return f"Home ({path})"
    except Exception:
        pass
    if str(path) == "/":
        return "Filesystem root (/)"
    name = path.name or str(path)
    if name.lower() in {"obs", "observations", "obsfiles", "obs_files"}:
        return f"Obs folder ({path})"
    if name.lower() == "data":
        return f"Data folder ({path})"
    if path == Path.cwd().resolve():
        return f"Current directory ({path})"
    return str(path)


def _path_under_root(path: Path, roots: List[Path]) -> bool:
    try:
        resolved = path.expanduser().resolve()
    except Exception:
        return False
    for root in roots:
        try:
            resolved.relative_to(root.resolve())
            return True
        except Exception:
            continue
    return False


def _safe_obs_path(raw_path: str, roots: List[Path]) -> Path:
    if not roots:
        raise ValueError("no NECST-side locations are available; check filesystem permissions or set --obs-root")
    if not str(raw_path or "").strip():
        raise ValueError("obs path is empty")
    path = Path(str(raw_path)).expanduser()
    if not path.is_absolute():
        path = roots[0] / path
    try:
        resolved = path.resolve()
    except Exception as exc:
        raise ValueError(f"cannot resolve NECST-side path: {exc}") from exc
    if not _path_under_root(resolved, roots):
        root_text = ", ".join(str(r) for r in roots)
        raise ValueError(f"NECST-side path is outside configured locations: {resolved}; roots={root_text}")
    return resolved


def obs_roots_payload(roots: List[Path]) -> JsonDict:
    return {
        "ok": True,
        "roots": [{"path": str(root), "label": _obs_root_label(root)} for root in roots],
        "message": (
            "NECST-side locations are paths visible to the process running this console"
            if roots
            else "no NECST-side locations found"
        ),
    }


def list_server_obs_files(roots: List[Path], directory: str = "") -> JsonDict:
    browse_dir = _safe_obs_path(directory or (str(roots[0]) if roots else ""), roots)
    if not browse_dir.exists() or not browse_dir.is_dir():
        raise ValueError(f"NECST-side directory does not exist or is not a directory: {browse_dir}")
    entries: List[JsonDict] = []
    parent = browse_dir.parent
    if parent != browse_dir and _path_under_root(parent, roots):
        entries.append({"type": "directory", "name": "..", "path": str(parent), "size": None})
    try:
        children = list(browse_dir.iterdir())
    except Exception as exc:
        raise ValueError(f"failed to list NECST-side directory {browse_dir}: {exc}") from exc
    for child in sorted(children, key=lambda p: (not p.is_dir(), p.name.lower())):
        try:
            if child.is_dir():
                entries.append({"type": "directory", "name": child.name, "path": str(child.resolve()), "size": None})
            elif child.is_file() and child.suffix.lower() in {".obs", ".toml"}:
                entries.append({
                    "type": "file",
                    "name": child.name,
                    "path": str(child.resolve()),
                    "size": int(child.stat().st_size),
                })
        except Exception:
            continue
    return {"ok": True, "directory": str(browse_dir), "entries": entries, "root_count": len(roots)}


def preview_server_obs_file(roots: List[Path], path: str, *, max_bytes: int = 65536) -> JsonDict:
    obs_path = _safe_obs_path(path, roots)
    if obs_path.suffix.lower() not in {".obs", ".toml", ".txt"}:
        raise ValueError("NECST-side preview only allows .obs, .toml, or .txt files")
    if not obs_path.exists() or not obs_path.is_file():
        raise ValueError(f"NECST-side obs file does not exist: {obs_path}")
    max_b = max(1024, min(int(max_bytes), 1024 * 1024))
    data = obs_path.read_bytes()
    truncated = len(data) > max_b
    chunk = data[:max_b]
    preview_text = chunk.decode("utf-8", errors="replace")
    return {
        "ok": True,
        "path": str(obs_path),
        "directory": str(obs_path.parent),
        "filename": obs_path.name,
        "text": preview_text,
        "size_bytes": len(data),
        "returned_bytes": len(chunk),
        "line_count": len(preview_text.splitlines()),
        "truncated": bool(truncated),
    }

def validate_site_capability(
    state: OperatorConsoleState,
    capability: str,
    *,
    action_label: Optional[str] = None,
) -> None:
    """Reject actions disabled by the active site config before command dispatch."""

    if bool(state.site_capabilities.get(capability, False)):
        return
    label = action_label or capability
    observatory = state.site_summary.observatory or "unknown site"
    raise ValueError(
        f"{label} is not enabled for {observatory} by the active site config"
    )


def chopper_dry_run_payload(state: OperatorConsoleState, command: str) -> JsonDict:
    """Return dry-run details using configured chopper endpoints."""

    chopper = state.chopper_config
    key = "insert_position" if command == "in" else "remove_position"
    return {
        "command": command,
        "target_position": chopper.get(key),
        "site_config": state.site_summary.to_dict(),
    }


def _load_operator_actions() -> Any:
    """Import the shared action module lazily.

    Importing NECST may require ROS/neclib.  Delaying the import keeps
    ``/health`` and read-only ``/api/status`` usable in reduced environments.
    When the full package import is unavailable, a file-based fallback with
    minimal stubs is used so dry-run console tests can still exercise validation
    and command construction.  Live execution on a real system should use the
    normal installed NECST package path.
    """

    try:
        return importlib.import_module("necst.core.operator_actions")
    except Exception:
        import types

        module_name = "necst.core.operator_actions"
        if module_name in sys.modules:
            return sys.modules[module_name]

        necst_pkg = sys.modules.setdefault("necst", types.ModuleType("necst"))
        core_pkg = sys.modules.setdefault("necst.core", types.ModuleType("necst.core"))
        setattr(necst_pkg, "core", core_pkg)

        config_mod = sys.modules.setdefault("necst.config", types.ModuleType("necst.config"))
        if not hasattr(config_mod, "chopper_motor_position"):
            config_mod.chopper_motor_position = {"insert": 4750, "remove": 19700}
        if not hasattr(config_mod, "simulator"):
            config_mod.simulator = False
        setattr(necst_pkg, "config", config_mod)

        commander_mod = sys.modules.setdefault(
            "necst.core.commander", types.ModuleType("necst.core.commander")
        )
        if not hasattr(commander_mod, "Commander"):
            class _UnavailableCommander:  # pragma: no cover - fallback only
                def __init__(self, *args: Any, **kwargs: Any) -> None:
                    raise RuntimeError("Commander is unavailable in reduced environment")

            commander_mod.Commander = _UnavailableCommander

        core_path = Path(__file__).resolve().parents[1] / "core"
        core_pkg.__path__ = [str(core_path)]  # type: ignore[attr-defined]

        obs_check_name = "necst.core.observation_check"
        if obs_check_name not in sys.modules:
            obs_check_path = core_path / "observation_check.py"
            obs_spec = importlib.util.spec_from_file_location(obs_check_name, obs_check_path)
            if obs_spec is None or obs_spec.loader is None:
                raise RuntimeError(f"failed to load observation_check.py from {obs_check_path}")
            obs_module = importlib.util.module_from_spec(obs_spec)
            sys.modules[obs_check_name] = obs_module
            setattr(core_pkg, "observation_check", obs_module)
            obs_spec.loader.exec_module(obs_module)

        module_path = core_path / "operator_actions.py"
        spec = importlib.util.spec_from_file_location(module_name, module_path)
        if spec is None or spec.loader is None:
            raise RuntimeError(f"failed to load operator_actions.py from {module_path}")
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        setattr(core_pkg, "operator_actions", module)
        spec.loader.exec_module(module)
        return module


def _result_to_response(result: Any) -> Tuple[bool, str, JsonDict]:
    success = bool(getattr(result, "success", False))
    message = str(getattr(result, "message", "") or "")
    data = getattr(result, "data", {})
    if not isinstance(data, dict):
        data = {"result": data}
    return success, message, dict(data)


def _reject_not_connected(action: str) -> Tuple[bool, str, JsonDict]:
    return (
        False,
        f"{action} is not connected yet in this phase; no command was sent",
        {"action": action, "connected": False},
    )


def _live_write_guard(state: OperatorConsoleState, action: str) -> Tuple[bool, str, JsonDict]:
    """Return whether a write-like live action may be dispatched.

    ``--action-mode dry-run`` is the normal no-hardware validation mode.
    Live mode is intended for normal operation and therefore allows write-like
    actions by default.  ``--guard-live-actions`` can still be used as an
    explicit diagnostic/read-only live guard; safety actions are handled
    separately and remain available.
    """

    if state.action_mode != "live":
        return True, "dry-run mode", {"live_write_guard": "not_live"}
    if action not in LIVE_WRITE_ACTIONS:
        return True, "read-only/local/safety action", {"live_write_guard": "not_write_action"}
    if state.live_actions_enabled:
        return True, "live write actions enabled", {
            "live_write_guard": "enabled",
            "live_actions_enabled": True,
        }
    return False, LIVE_ACTION_GUARD_MESSAGE, {
        "action": action,
        "live_write_guard": "blocked",
        "live_actions_enabled": False,
        "guard_option": "--guard-live-actions",
        "safety_actions_still_available": sorted(SAFETY_ACTIONS),
    }


def _authority_allows(state: OperatorConsoleState, session_id: str) -> Tuple[bool, str]:
    if not state.authority.held:
        return True, "temporary NECST authority will be acquired by the action layer"
    if state.authority.session_id == session_id:
        return True, "using this console session's authority"
    return False, "authority is held by another browser session"


def _held_commander_for_session(
    state: OperatorConsoleState, session_id: str
) -> Optional[Any]:
    """Return the persistent Commander only for the owning browser session."""

    if state.action_mode == "dry-run":
        return None
    if not state.authority.held or state.authority.session_id != session_id:
        return None
    return _any_held_commander(state)


def _any_held_commander(state: OperatorConsoleState) -> Optional[Any]:
    """Return this console's held Commander, regardless of browser session.

    This is intentionally used only by safety actions such as STOP, ABORT,
    and Stop tracking.  Those actions must remain available even when another
    browser tab/session has explicitly acquired authority through this console.
    Non-safety actions continue to require the owning browser session.
    """

    if state.action_mode == "dry-run":
        return None
    handle = state.authority_handle
    if handle is None:
        return None
    try:
        if bool(getattr(handle, "held", False)):
            return handle.commander
    except Exception:
        return None
    return None


def _safety_authority_bypass_data(
    state: OperatorConsoleState, session_id: str
) -> JsonDict:
    """Describe whether a safety action bypassed the browser authority gate."""

    if not state.authority.held:
        return {"authority_gate": "free"}
    if state.authority.session_id == session_id:
        return {"authority_gate": "owner_session"}
    return {
        "authority_gate": "safety_override",
        "authority_owner_session_id": state.authority.session_id,
        "safety_override_reason": "STOP/ABORT must remain available across browser sessions",
    }


def _clear_authority_state(state: OperatorConsoleState) -> None:
    state.authority.held = False
    state.authority.session_id = None
    state.authority.necst_held = False
    state.authority.necst_identity = None
    state.authority.mode = "none"
    state.authority.note = ""
    state.authority_handle = None


def _sync_authority_state(
    state: OperatorConsoleState,
    *,
    log_if_cleared: bool = True,
) -> JsonDict:
    """Synchronize browser authority state with the held NECST handle.

    The browser-session gate is meaningful only while the underlying
    OperatorAuthoritySession still reports that it holds authority.  If the
    authorizer restarted, the node lost authority, or a reduced/stub
    environment reports the handle as not held, clear the browser gate so other
    sessions are not blocked by stale state.
    """

    if not state.authority.held and state.authority_handle is None:
        _clear_authority_state(state)
        return {"changed": False, "held": False, "reason": "already_free"}

    handle = state.authority_handle
    if handle is None:
        note = "authority browser gate had no backing handle; cleared stale state"
        _clear_authority_state(state)
        if log_if_cleared:
            state.add_log(False, note, action="authority_sync")
        return {"changed": True, "held": False, "reason": "missing_handle"}

    try:
        handle_held = bool(getattr(handle, "held", False))
    except Exception as exc:
        note = f"authority handle status check failed; cleared stale state: {exc}"
        _clear_authority_state(state)
        if log_if_cleared:
            state.add_log(False, note, action="authority_sync")
        return {"changed": True, "held": False, "reason": "status_check_failed"}

    if not handle_held:
        note = "authority handle no longer reports NECST authority; cleared browser gate"
        _clear_authority_state(state)
        if log_if_cleared:
            state.add_log(False, note, action="authority_sync")
        return {"changed": True, "held": False, "reason": "handle_not_held"}

    try:
        identity = getattr(handle, "identity", None)
    except Exception:
        identity = state.authority.necst_identity

    state.authority.held = True
    state.authority.necst_held = state.action_mode != "dry-run"
    state.authority.necst_identity = None if identity is None else str(identity)
    state.authority.mode = "dry-run" if state.action_mode == "dry-run" else "necst"
    return {
        "changed": False,
        "held": True,
        "reason": "held",
        "identity": state.authority.necst_identity,
    }


def _release_console_authority(
    state: OperatorConsoleState,
    *,
    session_id: Optional[str] = None,
    force: bool = False,
) -> Tuple[bool, str, JsonDict]:
    """Release browser/session authority and any persistent NECST authority."""

    if not state.authority.held and state.authority_handle is None:
        _clear_authority_state(state)
        return True, "authority was already free", {"held": False}
    if (
        not force
        and session_id is not None
        and state.authority.session_id not in {None, session_id}
    ):
        return False, "cannot release authority held by another browser session", {}

    release_data: JsonDict = {}
    release_message = "authority released"
    handle = state.authority_handle
    if handle is not None:
        try:
            result = handle.release()
            ok, message, data = _result_to_response(result)
            release_data = data
            release_message = message or release_message
            if not ok:
                return False, release_message, release_data
        finally:
            _clear_authority_state(state)
    else:
        _clear_authority_state(state)
    return True, release_message, release_data


def _release_before_subprocess_if_needed(
    state: OperatorConsoleState, session_id: str
) -> Optional[str]:
    """Release persistent NECST authority before starting a child launcher."""

    if state.action_mode == "dry-run":
        return None
    if not state.authority.held or state.authority.session_id != session_id:
        return None
    if state.authority_handle is None:
        return None
    ok, message, _data = _release_console_authority(state, session_id=session_id)
    if not ok:
        raise RuntimeError(message)
    return "console NECST authority was released before starting the launcher process"


def _console_time_iso(ts: Optional[float]) -> Optional[str]:
    if ts is None:
        return None
    try:
        return time.strftime("%Y-%m-%dT%H:%M:%S%z", time.localtime(float(ts)))
    except Exception:
        return None


def _shutdown_snapshot(state: OperatorConsoleState) -> JsonDict:
    return {
        "requested": bool(state.shutdown_requested),
        "status": state.cleanup_status,
        "terminate_launchers": bool(state.shutdown_terminate_launchers),
        "launcher_timeout_sec": float(state.shutdown_launcher_timeout_sec),
        "launcher_kill_timeout_sec": float(state.shutdown_launcher_kill_timeout_sec),
        "cleanup_started_at": state.cleanup_started_at,
        "cleanup_started_at_iso": _console_time_iso(state.cleanup_started_at),
        "cleanup_finished_at": state.cleanup_finished_at,
        "cleanup_finished_at_iso": _console_time_iso(state.cleanup_finished_at),
        "summary": dict(state.cleanup_summary),
    }


def _observation_log_context(state: OperatorConsoleState) -> JsonDict:
    """Return current telemetry/context fields for the observer CSV log."""

    context: JsonDict = {
        "enc_az_deg": None,
        "enc_el_deg": None,
        "weather": {},
        "record_dir": "",
    }
    live_payload = state.live_cache.snapshot() if state.live_cache is not None else {}
    if isinstance(live_payload, Mapping):
        encoder = live_payload.get("encoder") if isinstance(live_payload.get("encoder"), Mapping) else {}
        context["enc_az_deg"] = encoder.get("encoder_lon_deg")
        context["enc_el_deg"] = encoder.get("encoder_lat_deg")
        weather = live_payload.get("weather") if isinstance(live_payload.get("weather"), Mapping) else {}
        context["weather"] = dict(weather)
    try:
        status_state = status_model.build_progress_status_state(
            state.progress_root,
            events_limit=1,
            live_payload=live_payload if isinstance(live_payload, Mapping) else None,
        )
        op = status_state.get("operator_status") if isinstance(status_state, Mapping) else {}
        if isinstance(op, Mapping):
            antenna = op.get("antenna") if isinstance(op.get("antenna"), Mapping) else {}
            if context.get("enc_az_deg") is None:
                context["enc_az_deg"] = antenna.get("current_az_deg")
            if context.get("enc_el_deg") is None:
                context["enc_el_deg"] = antenna.get("current_el_deg")
            observation = op.get("observation") if isinstance(op.get("observation"), Mapping) else {}
            paths = op.get("paths") if isinstance(op.get("paths"), Mapping) else {}
            context["record_dir"] = (
                observation.get("local_recording_dir")
                or paths.get("local_recording_dir")
                or observation.get("recording_dir")
                or paths.get("recording_dir")
                or observation.get("progress_record_dir")
                or paths.get("progress_record_dir")
                or ""
            )
        weather_state = status_state.get("weather") if isinstance(status_state.get("weather"), Mapping) else {}
        if weather_state and not context.get("weather"):
            context["weather"] = dict(weather_state)
    except Exception:
        pass
    return context


def _obslog_result(ok: bool, action: str, data: Mapping[str, Any]) -> str:
    if not ok:
        return "failed"
    if action == "start_observation":
        return "running"
    if action == "run_rsky" or action == "run_skydip":
        return "running"
    if action == "abort_observation":
        return "aborted"
    if action in {"stop", "stop_tracking"}:
        return "stopped"
    return "success"


def _active_process_labels(state: OperatorConsoleState, *, category: str) -> List[str]:
    try:
        return [str(record.label or record.category or "") for record in state.process_registry.active(category=category)]
    except Exception:
        return []


def _mode_from_process_label(label: str, *, default: str = "Observation") -> str:
    text = str(label or "")
    if ":" in text:
        candidate = text.split(":", 1)[1].strip()
        if candidate:
            return candidate.upper()
    return default


def _active_observation_mode(state: OperatorConsoleState) -> str:
    labels = _active_process_labels(state, category="observation")
    if labels:
        return _mode_from_process_label(labels[0], default="Observation")
    return "Observation"


def _active_observation_text(state: OperatorConsoleState, *, fallback: str) -> str:
    labels = [label for label in _active_process_labels(state, category="observation") if label]
    if labels:
        return ", ".join(labels)
    return fallback


def _obslog_event_for_action(action: str) -> Optional[str]:
    return {
        "check_observation": "observation_check_clicked",
        "dry_run_observation": "observation_dry_run_clicked",
        "start_observation": "observation_start_clicked",
        "abort_observation": "abort_clicked",
        "mount_move": "mount_move_clicked",
        "mount_move_dry_run": "mount_move_dry_run_clicked",
        "stop": "stop_clicked",
        "start_tracking": "target_tracking_start_clicked",
        "stop_tracking": "target_tracking_stop_clicked",
        "run_rsky": "rsky_start_clicked",
        "run_skydip": "skydip_start_clicked",
        "chopper_in": "chopper_in_clicked",
        "chopper_out": "chopper_out_clicked",
        "chopper_status": "chopper_status_clicked",
        "chopper_alarm_reset": "chopper_alarm_reset_clicked",
        "chopper_home": "chopper_home_clicked",
        "chopper_recover": "chopper_recover_clicked",
        "clear_stale_observation_state": "stale_state_cleared",
        "reset_local_console_state": "local_state_reset",
        "terminate_observation_launcher": "local_observation_launcher_force_kill_clicked",
        "terminate_calibration_launcher": "local_calibration_launcher_force_kill_clicked",
        "terminate_all_launchers": "local_launchers_force_kill_clicked",
        "terminate_process": "local_launcher_force_kill_clicked",
        "kill_process": "local_launcher_force_kill_clicked",
    }.get(action)


def _obslog_mode_for_action(
    action: str,
    params: Mapping[str, Any],
    data: Mapping[str, Any],
    state: Optional[OperatorConsoleState] = None,
) -> str:
    if action in {"check_observation", "dry_run_observation", "start_observation"}:
        return str(data.get("obs_mode") or params.get("mode") or "Observation").upper()
    if action in {"abort_observation", "stop"} and state is not None:
        labels = _active_process_labels(state, category="observation")
        if labels:
            return _mode_from_process_label(labels[0], default="Observation")
    if action in {"mount_move", "mount_move_dry_run"}:
        return "Mount"
    if action in {"start_tracking", "stop_tracking"}:
        return "Tracking"
    if action in {"run_rsky"}:
        return "RSky"
    if action in {"run_skydip"}:
        return "SkyDip"
    if action.startswith("chopper_"):
        return "Chopper"
    if action in {"stop", "abort_observation", "clear_stale_observation_state", "reset_local_console_state"}:
        return "Console"
    if action.startswith("terminate_") or action in {"kill_process"}:
        return "Recovery"
    return "Console"


def _obslog_action_text(
    action: str,
    params: Mapping[str, Any],
    data: Mapping[str, Any],
    state: Optional[OperatorConsoleState] = None,
) -> str:
    if action in {"check_observation", "dry_run_observation", "start_observation"}:
        return observation_log.obsfile_name_for_csv(data.get("obs_path") or params.get("file") or "")
    if action in {"abort_observation", "stop"} and state is not None:
        labels = _active_process_labels(state, category="observation")
        if labels:
            return f"{action}: " + ", ".join(labels)
    if action in {"mount_move", "mount_move_dry_run"}:
        return f"Az={params.get('az', '')}, El={params.get('el', '')}"
    if action == "start_tracking":
        target = params.get("target") or params.get("target_kind") or params.get("kind") or params.get("name") or "target"
        return str(target)
    if action in {"run_rsky", "run_skydip"}:
        return ", ".join(f"{k}={v}" for k, v in params.items() if v not in (None, ""))
    if action in {"chopper_in", "chopper_out"}:
        return action.replace("chopper_", "chopper ").upper()
    if action.startswith("terminate_") or action in {"kill_process"}:
        note = data.get("note") or data.get("message") or ""
        pid = params.get("pid")
        pid_text = f"pid={pid}" if pid not in (None, "") else ""
        return ", ".join(part for part in (str(note or action), pid_text) if part)
    return str(data.get("action") or action)


def _write_observation_log_for_action(
    state: OperatorConsoleState,
    *,
    action: str,
    params: Mapping[str, Any],
    ok: bool,
    data: Mapping[str, Any],
) -> None:
    manager = state.observation_log
    if manager is None:
        return
    event = _obslog_event_for_action(action)
    if event is None:
        return
    context = _observation_log_context(state)
    record_dir = None
    if action == "start_observation":
        record_dir = data.get("recording_dir") or data.get("local_recording_dir") or data.get("progress_record_dir")
    written = manager.write_event(
        context,
        mode=_obslog_mode_for_action(action, params, data, state),
        event=event,
        action_or_obsfile=_obslog_action_text(action, params, data, state),
        result=_obslog_result(ok, action, data),
        record_dir=record_dir,
    )
    if not written:
        state.add_log(
            False,
            f"failed to append observation CSV log row: {manager.last_error or 'unknown error'}",
            action="observation_csv_log",
            data={"source_action": action},
        )


def _obslog_status_payload(state: OperatorConsoleState) -> JsonDict:
    manager = state.observation_log
    if manager is None:
        return {"ok": False, "reason": "observation CSV log is not configured"}
    return {"ok": True, "observation_log": manager.status()}

def run_self_check(state: OperatorConsoleState, *, include_progress_health: bool = True) -> JsonDict:
    """Run read-only console self-checks without sending telescope commands."""

    return self_check.run_console_self_check(
        telescope=state.telescope,
        action_mode=state.action_mode,
        live_actions_enabled=state.live_actions_enabled,
        progress_root=state.progress_root,
        site_summary=state.site_summary,
        mount_limits=state.mount_limits,
        capabilities=state.site_capabilities,
        chopper_config=state.chopper_config,
        operator_log_path=state.operator_log_path,
        launcher_log_dir=state.launcher_log_dir,
        process_registry=state.process_registry,
        progress_monitor=state.progress_monitor,
        events_limit=state.events_limit,
        include_progress_health=include_progress_health,
        live_telemetry_snapshot=(
            state.live_cache.snapshot()
            if state.live_cache is not None
            else {"requested": False, "available": False, "spin_mode": "disabled"}
        ),
    )


def _refresh_processes_and_log(state: OperatorConsoleState) -> None:
    """Reap child launcher processes and log newly observed final states."""

    finalized = state.process_registry.refresh()
    for record in finalized:
        if record.final_logged:
            continue
        record.final_logged = True
        ok = record.returncode in (0, None) and record.status != "lost"
        if record.status == "exited":
            message = (
                f"{record.label} pid={record.pid} exited with return code "
                f"{record.returncode}"
            )
        else:
            message = f"{record.label} pid={record.pid} is no longer tracked"
        state.add_log(ok, message, action="process_exit", data=record.to_dict())
        if state.observation_log is not None and record.category in {"observation", "calibration"}:
            label = str(getattr(record, "label", "") or "")
            result_text = "success" if ok else "failed"
            if record.category == "observation":
                mode = label.split(":", 1)[1].upper() if ":" in label else "Observation"
                safety_labels = list(getattr(state, "last_observation_safety_labels", []) or [])
                safety_matches = (not safety_labels) or (label in safety_labels)
                if state.last_observation_abort_requested_at is not None and safety_matches:
                    event = "observation_aborted"
                    result_text = "aborted"
                elif state.last_observation_stop_requested_at is not None and safety_matches:
                    event = "observation_stopped"
                    result_text = "stopped"
                else:
                    event = "observation_finished" if ok else "observation_failed"
            else:
                mode = label.split(":", 1)[1] if ":" in label else "Calibration"
                event = f"{mode.lower()}_finished" if ok else f"{mode.lower()}_failed"
            written = state.observation_log.write_event(
                _observation_log_context(state),
                mode=mode,
                event=event,
                action_or_obsfile=label or getattr(record, "category", ""),
                result=result_text,
            )
            if not written:
                state.add_log(
                    False,
                    f"failed to append observation CSV final-state row: {state.observation_log.last_error or 'unknown error'}",
                    action="observation_csv_log",
                    data={"source_action": "process_exit", "label": label},
                )
        if record.category == "observation" and not state.process_registry.active(category="observation"):
            state.last_observation_abort_requested_at = None
            state.last_observation_stop_requested_at = None
            state.last_observation_safety_labels = []
            if state.last_active_task == "observation":
                state.last_active_task = "none"
            if state.last_manual_state == "observing sequence":
                state.last_manual_state = "idle"
        if record.category == "calibration" and not state.process_registry.active(category="calibration"):
            if state.last_active_task in {"RSky", "SkyDip"}:
                state.last_active_task = "none"
            if state.last_manual_state == "calibration":
                state.last_manual_state = "idle"


def _active_launcher_summary(
    state: OperatorConsoleState, *, category: Optional[str] = None
) -> Optional[str]:
    active = state.process_registry.active(category=category)
    if not active:
        return None
    labels = [f"{r.label}(pid={r.pid})" for r in active]
    if category:
        return f"active {category} launcher already exists: " + ", ".join(labels)
    return "active launcher already exists: " + ", ".join(labels)



def _active_launcher_categories(state: OperatorConsoleState) -> set[str]:
    """Return active launcher categories currently owned by this console."""

    return {
        str(record.category or "").strip().lower()
        for record in state.process_registry.active()
        if str(record.category or "").strip()
    }


def _progress_snapshot_update_age_sec(status_state: Mapping[str, Any]) -> Optional[float]:
    """Return age of the current progress snapshot update, if known."""

    now = time.time()
    snapshot = status_state.get("snapshot") if isinstance(status_state, Mapping) else {}
    snapshot = snapshot if isinstance(snapshot, Mapping) else {}
    timing = snapshot.get("time") if isinstance(snapshot.get("time"), Mapping) else {}
    candidates = [
        timing.get("updated_at_unix"),
        timing.get("last_update_unix"),
        timing.get("server_time_unix"),
    ]
    raw = status_state.get("raw_snapshot") if isinstance(status_state, Mapping) else {}
    raw = raw if isinstance(raw, Mapping) else {}
    raw_timing = raw.get("time") if isinstance(raw.get("time"), Mapping) else {}
    candidates.extend([
        raw_timing.get("updated_at_unix"),
        raw_timing.get("last_update_unix"),
    ])
    for value in candidates:
        try:
            ts = float(value)
        except Exception:
            continue
        if math.isfinite(ts) and ts > 0:
            return max(0.0, now - ts)
    try:
        path = Path(str((status_state.get("paths") or {}).get("snapshot") or ""))
        if path.exists():
            return max(0.0, now - path.stat().st_mtime)
    except Exception:
        pass
    return None



def _utc_reset_stamp() -> str:
    return time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())


def reset_local_console_state_files(
    *,
    progress_root: os.PathLike[str] | str,
    operator_log_dir: os.PathLike[str] | str,
    reason: str = "operator request",
) -> JsonDict:
    """Archive only local console/progress state files.

    This never sends telescope, recorder, spectrometer, or ROS commands.  It is
    intentionally limited to files that can make the web console believe a stale
    observation is still current after an interrupted previous run.
    """

    stamp = _utc_reset_stamp()
    archive_dir = Path(operator_log_dir).expanduser() / "reset_archive" / stamp
    archive_dir.mkdir(parents=True, exist_ok=True)
    candidates = [
        Path(progress_root).expanduser() / "current_observation_progress.json",
        Path(progress_root).expanduser() / "current_observation_record.txt",
    ]
    moved: List[JsonDict] = []
    missing: List[str] = []
    errors: List[JsonDict] = []
    for path in candidates:
        try:
            if not path.exists():
                missing.append(str(path))
                continue
            target = archive_dir / path.name
            if target.exists():
                target = archive_dir / f"{path.stem}_{uuid.uuid4().hex[:6]}{path.suffix}"
            shutil.move(str(path), str(target))
            moved.append({"from": str(path), "to": str(target)})
        except Exception as exc:
            errors.append({"path": str(path), "error": str(exc)})
    payload: JsonDict = {
        "ok": not bool(errors),
        "reason": str(reason or "operator request"),
        "archive_dir": str(archive_dir),
        "moved": moved,
        "missing": missing,
        "errors": errors,
        "hardware_command_sent": False,
    }
    try:
        manifest = archive_dir / "reset_manifest.json"
        with manifest.open("w", encoding="utf-8") as fh:
            json.dump(payload, fh, ensure_ascii=False, indent=2, sort_keys=True)
            fh.write("\n")
            fh.flush()
            try:
                os.fsync(fh.fileno())
            except Exception:
                pass
        payload["manifest"] = str(manifest)
    except Exception as exc:
        payload.setdefault("errors", []).append({"path": str(archive_dir / "reset_manifest.json"), "error": str(exc)})
        payload["ok"] = False
    return payload


def _reset_local_console_state(state: OperatorConsoleState, *, reason: str = "operator request") -> Tuple[bool, str, JsonDict]:
    operator_dir = state.operator_log_path.parent if state.operator_log_path is not None else Path.home() / ".necst" / "operator_console"
    data = reset_local_console_state_files(
        progress_root=state.progress_root,
        operator_log_dir=operator_dir,
        reason=reason,
    )
    state.local_state_reset_archive = str(data.get("archive_dir") or "")
    state.local_state_reset_summary = dict(data)
    moved_count = len(data.get("moved") or [])
    if state.observation_log is not None:
        try:
            state.observation_log.write_event(
                {},
                mode="Console",
                event="local_state_reset",
                action_or_obsfile=str(data.get("archive_dir") or ""),
                result="success" if data.get("ok") else "failed",
                comment=f"archived {moved_count} local state file(s); no hardware command sent",
            )
        except Exception:
            pass
    message = f"local console state reset; archived {moved_count} file(s); no hardware command sent"
    if not data.get("ok"):
        message = "local console state reset had errors; no hardware command sent"
    return bool(data.get("ok")), message, data

def _stale_observation_state(
    state: OperatorConsoleState,
    status_state: Mapping[str, Any],
    operator_status: Optional[Mapping[str, Any]] = None,
) -> JsonDict:
    """Classify stale or stuck observation-running state.

    ``running_stale`` means the progress pointer still says running, but no
    local observation/calibration launcher is active and the sidecar is old.
    ``launcher_stuck`` means a local observation launcher is still active, but
    the system is in error/failed state or ABORT/STOP has not completed for a
    while.  The latter must be shown as ATTENTION, not OBSERVATION RUNNING.
    """

    op = operator_status if isinstance(operator_status, Mapping) else {}
    if not op:
        op = status_state.get("operator_status") if isinstance(status_state, Mapping) else {}
        op = op if isinstance(op, Mapping) else {}
    system = op.get("system") if isinstance(op.get("system"), Mapping) else {}
    progress = op.get("progress") if isinstance(op.get("progress"), Mapping) else {}
    observation = op.get("observation") if isinstance(op.get("observation"), Mapping) else {}
    lifecycle = str(system.get("state") or "unknown").strip().lower()
    percent = progress.get("percent")
    nonfinal_lifecycle = lifecycle not in {"", "idle", "unknown", "finished", "aborted", "error", "failed"}
    progress_running = bool(nonfinal_lifecycle or percent is not None)
    active_categories = _active_launcher_categories(state)
    active_launcher = bool(active_categories.intersection({"observation", "calibration"}))
    age = _progress_snapshot_update_age_sec(status_state)
    threshold = max(5.0, float(STALE_OBSERVATION_PROGRESS_SEC))
    safety_age = _recent_safety_release_request_age_sec(state, max_age_sec=3600.0)
    abort_stuck_threshold = max(3.0, float(ABORT_STUCK_OBSERVATION_SEC))

    running_stale = bool(
        progress_running
        and not active_launcher
        and age is not None
        and age >= threshold
    )
    error_like_lifecycle = lifecycle in {"error", "failed"}
    abort_stalled = bool(
        active_launcher
        and safety_age is not None
        and safety_age >= abort_stuck_threshold
    )
    launcher_stuck = bool(active_launcher and (error_like_lifecycle or abort_stalled))

    reason = ""
    if launcher_stuck:
        if abort_stalled:
            reason = (
                "ABORT/STOP was requested, but a console-owned observation launcher "
                "is still active after the timeout"
            )
        else:
            reason = (
                "system lifecycle is error/failed while a console-owned observation "
                "launcher is still active"
            )
    elif running_stale:
        reason = (
            "progress sidecar is non-final/still-running, but no console-owned "
            "observation launcher is active and the sidecar update is stale"
        )

    return {
        "running_stale": running_stale,
        "launcher_stuck": launcher_stuck,
        "abort_stalled": abort_stalled,
        "progress_running": bool(progress_running),
        "active_launcher": active_launcher,
        "active_launcher_categories": sorted(active_categories),
        "age_sec": age,
        "threshold_sec": threshold,
        "abort_stuck_threshold_sec": abort_stuck_threshold,
        "safety_release_age_sec": safety_age,
        "lifecycle_state": lifecycle,
        "record_name": observation.get("record_name"),
        "progress_record_dir": observation.get("progress_record_dir"),
        "reason": reason,
        "can_clear": bool(running_stale and not active_launcher),
        "can_terminate_launcher": bool(launcher_stuck and active_launcher),
    }

def _clear_stale_observation_state(state: OperatorConsoleState) -> Tuple[bool, str, JsonDict]:
    """Archive stale current-observation pointers so the console can return READY.

    This is an explicit operator recovery action.  It never sends hardware
    commands and it does not delete the per-record progress directory or data.
    It only moves the global current-observation pointer files out of the way.
    """

    active = _active_launcher_categories(state).intersection({"observation", "calibration"})
    if active:
        return False, (
            "cannot clear stale observation state while a console-owned launcher "
            f"is still active: {', '.join(sorted(active))}. Force-kill the local "
            "observation launcher first after confirming the hardware is safe."
        ), {"active_launcher_categories": sorted(active), "cleared": False}

    root = state.progress_root
    stamp = time.strftime("%Y%m%d_%H%M%S", time.localtime())
    archive_dir = root / "stale_console_clear" / stamp
    moved: List[Dict[str, str]] = []
    missing: List[str] = []
    errors: List[str] = []
    for name in ("current_observation_progress.json", "current_observation_record.txt"):
        src = root / name
        if not src.exists():
            missing.append(str(src))
            continue
        try:
            archive_dir.mkdir(parents=True, exist_ok=True)
            dst = archive_dir / name
            shutil.move(str(src), str(dst))
            moved.append({"from": str(src), "to": str(dst)})
        except Exception as exc:
            errors.append(f"{src}: {exc}")
    if errors:
        return False, "failed to clear one or more stale progress pointer files", {
            "cleared": False,
            "moved": moved,
            "missing": missing,
            "errors": errors,
            "archive_dir": str(archive_dir),
        }

    _clear_exclusive_start_guard(state)
    _clear_safety_release_request(state)
    _clear_last_mount_target(state)
    state.last_manual_state = "idle"
    state.last_active_task = "none"
    state.last_command_az = None
    state.last_command_el = None
    message = "stale observation state cleared"
    if moved:
        message += f"; archived {len(moved)} current progress pointer file(s)"
    else:
        message += "; no current progress pointer files were present"
    return True, message, {
        "cleared": True,
        "moved": moved,
        "missing": missing,
        "archive_dir": str(archive_dir) if moved else None,
        "note": "per-record progress/data directories were not deleted",
    }

def _last_mount_target_reached(
    state: OperatorConsoleState,
    *,
    current_az: Any,
    current_el: Any,
    tol_deg: float = 7.5e-4,
    require_settle_sec: float = 0.5,
    update_state: bool = False,
) -> bool:
    """Return True when the encoder is at the last manual mount target.

    This is not used to display Command Az/El.  It only prevents stale SKY or
    tracking-like low-level status from trapping the operator UI after a fixed
    Az/El mount-move has reached the requested coordinate, especially after STOP
    clears the live command topic.
    """

    if state.last_mount_target_az is None or state.last_mount_target_el is None:
        if update_state:
            state.last_mount_target_reached_since = None
        return False
    try:
        az = float(current_az)
        el = float(current_el)
        target_az = float(state.last_mount_target_az)
        target_el = float(state.last_mount_target_el)
    except Exception:
        if update_state:
            state.last_mount_target_reached_since = None
        return False
    if not (math.isfinite(az) and math.isfinite(el) and math.isfinite(target_az) and math.isfinite(target_el)):
        if update_state:
            state.last_mount_target_reached_since = None
        return False

    reached_now = abs(az - target_az) <= float(tol_deg) and abs(el - target_el) <= float(tol_deg)
    now = time.time()
    if not reached_now:
        if update_state:
            state.last_mount_target_reached_since = None
        return False
    if update_state and state.last_mount_target_reached_since is None:
        state.last_mount_target_reached_since = now
    since = state.last_mount_target_reached_since if state.last_mount_target_reached_since is not None else now
    try:
        settled = (now - float(since)) >= float(require_settle_sec)
    except Exception:
        settled = True
    # For conflict checks we do not want a one-poll delay to trap the operator;
    # for the visible status we use the small settle time to avoid flicker while
    # the antenna is still passing through the target.
    return bool(settled or require_settle_sec <= 0.0)


def _clear_last_mount_target(state: OperatorConsoleState) -> None:
    state.last_mount_target_az = None
    state.last_mount_target_el = None
    state.last_mount_target_started_at = None
    state.last_mount_target_reached_since = None


def _mark_safety_release_request(state: OperatorConsoleState) -> None:
    state.last_safety_release_requested_at = time.time()


def _clear_safety_release_request(state: OperatorConsoleState) -> None:
    state.last_safety_release_requested_at = None


def _recent_safety_release_request_age_sec(
    state: OperatorConsoleState, *, max_age_sec: float = 180.0
) -> Optional[float]:
    started = state.last_safety_release_requested_at
    if started is None:
        return None
    try:
        age = max(0.0, time.time() - float(started))
    except Exception:
        return None
    if age > float(max_age_sec):
        state.last_safety_release_requested_at = None
        return None
    return age



def _prune_exclusive_start_guard(state: OperatorConsoleState) -> None:
    action = str(state.exclusive_start_action or "").strip()
    started = state.exclusive_start_started_at
    if not action or started is None:
        return
    try:
        age = max(0.0, time.time() - float(started))
    except Exception:
        age = EXCLUSIVE_START_STATUS_SEC + 1.0
    if age > EXCLUSIVE_START_STATUS_SEC:
        state.exclusive_start_action = None
        state.exclusive_start_session_id = None
        state.exclusive_start_started_at = None
        state.exclusive_start_message = ""


def _exclusive_start_guard_reason(
    state: OperatorConsoleState, *, requested_action: str
) -> Optional[str]:
    if requested_action not in EXCLUSIVE_START_ACTIONS:
        return None
    _prune_exclusive_start_guard(state)
    action = str(state.exclusive_start_action or "").strip()
    started = state.exclusive_start_started_at
    if not action or started is None:
        return None
    age = max(0.0, time.time() - float(started))
    if age > EXCLUSIVE_START_BLOCK_SEC:
        return None
    message = state.exclusive_start_message or action
    return (
        f"cannot start {requested_action}: {message} was accepted "
        f"{age:.1f} s ago; wait for READY/status confirmation or use STOP/ABORT"
    )


def _mark_exclusive_start_guard(
    state: OperatorConsoleState,
    *,
    action: str,
    session_id: str,
    message: str,
) -> None:
    if action not in EXCLUSIVE_START_ACTIONS:
        return
    state.exclusive_start_action = str(action)
    state.exclusive_start_session_id = str(session_id or "")
    state.exclusive_start_started_at = time.time()
    state.exclusive_start_message = str(message or action)


def _clear_exclusive_start_guard(state: OperatorConsoleState) -> None:
    state.exclusive_start_action = None
    state.exclusive_start_session_id = None
    state.exclusive_start_started_at = None
    state.exclusive_start_message = ""


def _operator_status_operation_conflict(
    state: OperatorConsoleState, *, requested_action: str
) -> Optional[str]:
    """Return a user-facing conflict reason for mutually exclusive starts.

    The browser disables Start-like controls while an operation is active, but
    the server must also be conservative because operators can double-click, use
    stale tabs, or trigger actions from another client. Safety actions are not
    passed here and remain available.
    """

    if requested_action not in EXCLUSIVE_START_ACTIONS:
        return None

    active_launcher = _active_launcher_summary(state)
    if active_launcher is not None:
        return (
            f"cannot start {requested_action}: {active_launcher}. "
            "Use STOP/ABORT or force-kill the stuck local launcher before starting another operation."
        )

    recent_guard = _exclusive_start_guard_reason(state, requested_action=requested_action)
    if recent_guard is not None:
        return recent_guard

    # Dry-run has no live ROS truth, so use local memory to protect the UI from
    # conflicting Start-like actions. In live mode this memory is allowed to be
    # stale, so live/progress truth below is preferred instead.
    if state.action_mode == "dry-run":
        task = str(state.last_active_task or "").strip()
        manual = str(state.last_manual_state or "").strip()
        if task.lower() not in {"", "none", "idle", "unknown"}:
            return f"cannot start {requested_action}: active task is {task}; use STOP/ABORT or wait until READY"
        if manual.lower() in {"moving", "tracking", "calibration", "observing sequence"}:
            return f"cannot start {requested_action}: manual state is {manual}; use STOP/ABORT or wait until READY"
        return None

    try:
        live_payload = state.live_cache.snapshot() if state.live_cache is not None else None
        status_state = status_model.build_progress_status_state(
            state.progress_root,
            events_limit=state.events_limit,
            live_payload=live_payload,
        )
        op = status_state.get("operator_status") if isinstance(status_state, Mapping) else {}
        op = op if isinstance(op, Mapping) else {}
        stale = _stale_observation_state(state, status_state, op)
        if stale.get("running_stale") or stale.get("launcher_stuck"):
            if stale.get("launcher_stuck"):
                return (
                    f"cannot start {requested_action}: observation launcher appears stuck "
                    f"for record {stale.get('record_name') or 'unknown'}; force-kill the local observation launcher first"
                )
            return (
                f"cannot start {requested_action}: stale observation state remains "
                f"for record {stale.get('record_name') or 'unknown'}; use Clear stale observation state"
            )
        system = op.get("system") if isinstance(op.get("system"), Mapping) else {}
        motion = op.get("motion") if isinstance(op.get("motion"), Mapping) else {}
        sys_state = str(system.get("state") or "").strip().lower()
        if sys_state not in {"", "idle", "unknown", "finished", "aborted", "error", "failed"}:
            return f"cannot start {requested_action}: system state is {sys_state}; use STOP/ABORT or wait until READY"
        at_target_idle = bool(
            motion.get("mount_target_reached")
            and motion.get("live_motion_active") is False
        )
        antenna = op.get("antenna") if isinstance(op.get("antenna"), Mapping) else {}
        command_absent = antenna.get("command_az_deg") is None and antenna.get("command_el_deg") is None
        motion_source_lower = str(motion.get("live_motion_source") or "").strip().lower()
        safety_release_age = _recent_safety_release_request_age_sec(state)
        safety_release_idle = bool(
            state.action_mode != "dry-run"
            and safety_release_age is not None
            and sys_state in {"", "idle", "unknown", "finished", "aborted", "error", "failed"}
            and command_absent
            and safety_release_age >= SAFETY_RELEASE_ASSUME_IDLE_AFTER_SEC
        )
        if safety_release_idle:
            at_target_idle = True
        operator_target_idle = False
        if sys_state in {"", "idle", "unknown", "finished", "aborted", "error", "failed"}:
            operator_target_idle = _last_mount_target_reached(
                state,
                current_az=antenna.get("current_az_deg"),
                current_el=antenna.get("current_el_deg"),
                require_settle_sec=0.0,
                update_state=False,
            )
        if operator_target_idle:
            at_target_idle = True
        active_task = str(motion.get("active_task") or "").strip()
        if active_task.lower() not in {"", "none", "idle", "unknown"} and not at_target_idle:
            return f"cannot start {requested_action}: active task is {active_task}; use STOP/ABORT or wait until READY"
        stage = str(motion.get("stage") or "").strip()
        # Low-level antenna control can report a hold/tracking-like stage after
        # a fixed Az/El mount-move has already reached the target.  The shared
        # status model normally normalizes that to idle, but keep this guard
        # tolerant so a stale or site-specific label does not trap the operator.
        nonblocking_stages = {
            "",
            "none",
            "idle",
            "unknown",
            "at_target",
            "at target",
            "holding",
            "hold",
            "command_hold",
            "command hold",
            "target_hold",
            "target hold",
        }
        if stage.lower() not in nonblocking_stages and not at_target_idle:
            return f"cannot start {requested_action}: mount/activity stage is {stage}; use STOP/ABORT or wait until READY"
    except Exception:
        # If status probing fails, do not block the action solely because the
        # guard could not inspect telemetry. The action-specific validators and
        # NECST command layer still run below. The next /api/status will expose
        # the telemetry problem to the operator.
        return None
    return None

def _launcher_log_paths(
    state: OperatorConsoleState,
    action: str,
) -> Tuple[Optional[Path], Optional[Path]]:
    if state.launcher_log_dir is None:
        return None, None
    return process_manager.make_launcher_log_paths(state.launcher_log_dir, action)


def _register_launcher_if_needed(
    state: OperatorConsoleState,
    *,
    action: str,
    category: str,
    data: Mapping[str, Any],
    label: str,
) -> Optional[process_manager.ManagedProcessRecord]:
    record = state.process_registry.register_from_action_result(
        action=action,
        category=category,
        result_data=data,
        label=label,
    )
    # Keep Popen handles inside ProcessRegistry only; never expose them in JSON.
    try:
        data.pop("_popen", None)  # type: ignore[attr-defined]
    except Exception:
        pass
    if record is not None:
        state.add_log(
            True,
            f"{label} launcher registered: pid={record.pid}",
            action="process_start",
            data=record.to_dict(),
        )
    return record



def _request_launcher_stop(
    state: OperatorConsoleState,
    *,
    action: str,
    params: Mapping[str, Any],
    category: Optional[str] = None,
    kill: bool = False,
) -> Tuple[bool, str, JsonDict]:
    """Force-kill local launcher subprocesses started by this console only.

    This recovery action only signals local child processes owned by the
    operator console.  It never sends telescope STOP, recorder STOP, or XFFTS
    STOP commands.
    """

    pid_value = params.get("pid")
    pid: Optional[int] = None
    if pid_value not in (None, ""):
        try:
            pid = int(pid_value)
        except Exception as exc:
            raise ValueError(f"invalid launcher pid: {pid_value!r}") from exc
    if pid is None and params.get("category") not in (None, ""):
        category = str(params.get("category"))
    records = state.process_registry.request_stop(
        pid=pid,
        category=category,
        kill=kill,
        reason=f"operator console action {action}",
    )
    if not records:
        selector = f"pid={pid}" if pid is not None else f"category={category or 'any'}"
        return False, f"no active launcher matched {selector}; no local launcher process was force-killed", {
            "action": action,
            "pid": pid,
            "category": category,
            "kill": bool(kill),
            "terminated": [],
        }
    signal_name = "SIGKILL" if kill else "SIGTERM"
    data = {
        "action": action,
        "pid": pid,
        "category": category,
        "kill": bool(kill),
        "signal": signal_name,
        "terminated": [record.to_dict() for record in records],
        "note": "local launcher force-kill only; telescope STOP/ABORT/recorder/XFFTS stop are separate actions",
    }
    labels = ", ".join(f"{r.label}(pid={r.pid})" for r in records)
    return True, f"requested {signal_name} for local launcher(s) only: {labels}", data


def dispatch_action(
    state: OperatorConsoleState,
    action: str,
    params: Mapping[str, Any],
    session_id: str,
) -> Tuple[bool, str, JsonDict]:
    """Validate and dispatch one browser action.

    Return ``(ok, message, data)``.  Invalid or not-yet-connected actions return
    ``ok=False`` before any operator command is sent.
    """

    action = str(action or "").strip()
    params = params if isinstance(params, Mapping) else {}
    _refresh_processes_and_log(state)
    _sync_authority_state(state)

    if action == "clear_log":
        state.log.clear()
        return True, "operation log cleared", {"action": action}

    if action == "obslog_status":
        data = _obslog_status_payload(state)
        return bool(data.get("ok")), "observation CSV log status", data

    if action == "obslog_comment":
        manager = state.observation_log
        if manager is None:
            return False, "observation CSV log is not configured", {"action": action}
        comment = str(params.get("comment") or "").strip()
        if not comment:
            return False, "comment is empty; no observation-log row was written", {"action": action}
        written = manager.write_event(
            _observation_log_context(state),
            comment=comment,
            mode="Comment",
            event="comment",
            action_or_obsfile="manual comment",
            result="success",
        )
        if not written:
            return False, f"failed to append observation CSV log comment: {manager.last_error or 'unknown error'}", {
                "action": action,
                "observation_log": manager.status(),
            }
        return True, "comment appended to observation CSV log", {"action": action, "observation_log": manager.status()}

    if action == "obslog_new":
        manager = state.observation_log
        if manager is None:
            return False, "observation CSV log is not configured", {"action": action}
        manager.open_new(prefix=params.get("prefix") or manager.prefix, observer=params.get("user") or manager.observer)
        return True, "new observation CSV log opened", {"action": action, "observation_log": manager.status()}

    if action == "obslog_open_existing":
        manager = state.observation_log
        if manager is None:
            return False, "observation CSV log is not configured", {"action": action}
        manager.open_existing(params.get("path"), observer=params.get("user") or manager.observer)
        return True, "existing observation CSV log opened for append", {"action": action, "observation_log": manager.status()}

    if action == "launch_progress":
        if state.progress_monitor is None:
            return True, f"progress monitor URL: {state.progress_url}", {
                "action": action,
                "url": state.progress_url,
                "progress_url": state.progress_url,
                "managed": False,
            }
        ok, message, data = state.progress_monitor.launch()
        data = dict(data)
        data["action"] = action
        data["url"] = data.get("url") or state.progress_url
        data["progress_url"] = data.get("progress_url") or data.get("url") or state.progress_url
        state.progress_url = str(data["progress_url"])
        return ok, message, data

    if action == "list_processes":
        records = [r.to_dict() for r in state.process_registry.all_records(refresh=True)]
        return True, f"{len(records)} launcher process record(s)", {
            "action": action,
            "processes": records,
            "process_counts": state.process_registry.counts(),
            "launcher_log_choices": log_reader.launcher_log_choices(records),
            "shutdown": _shutdown_snapshot(state),
        }

    if action == "read_operator_log":
        limit = params.get("limit", 100)
        data = log_reader.read_operator_log(state.operator_log_path, limit=limit)
        data["action"] = action
        return bool(data.get("ok")), str(data.get("reason") or "operator log read"), data

    if action == "read_log_file":
        extra_roots = []
        if state.progress_monitor is not None and getattr(state.progress_monitor, "log_dir", None) is not None:
            extra_roots.append(Path(state.progress_monitor.log_dir))
        data = log_reader.read_text_log(
            params.get("path"),
            launcher_log_dir=state.launcher_log_dir,
            operator_log_path=state.operator_log_path,
            max_bytes=params.get("max_bytes", 32768),
            extra_roots=extra_roots,
        )
        data["action"] = action
        return bool(data.get("ok")), str(data.get("reason") or "log file read"), data

    if action == "cleanup_status":
        records = [r.to_dict() for r in state.process_registry.all_records(refresh=True)]
        return True, "console cleanup status", {
            "action": action,
            "processes": records,
            "process_counts": state.process_registry.counts(),
            "progress_monitor": (
                state.progress_monitor.status(check_external=True).to_dict()
                if state.progress_monitor is not None
                else {"url": state.progress_url, "running": False, "owned_by_console": False}
            ),
            "shutdown": _shutdown_snapshot(state),
        }

    if action == "reset_local_console_state":
        ok, message, data = _reset_local_console_state(state, reason="browser action")
        data["action"] = action
        return ok, message, data

    if action == "clear_stale_observation_state":
        ok, message, data = _clear_stale_observation_state(state)
        data["action"] = action
        return ok, message, data

    live_allowed, live_reason, live_data = _live_write_guard(state, action)
    if not live_allowed:
        return False, live_reason, live_data

    if action == "self_check":
        include_progress_health = params.get("include_progress_health", True)
        result = run_self_check(
            state,
            include_progress_health=bool(include_progress_health),
        )
        return bool(result.get("ok")), "operator console self-check completed", {
            "action": action,
            "self_check": result,
        }

    if action in {"terminate_process", "kill_process"}:
        return _request_launcher_stop(
            state,
            action=action,
            params=params,
            kill=(action == "kill_process"),
        )

    if action == "terminate_observation_launcher":
        return _request_launcher_stop(
            state,
            action=action,
            params=params,
            category="observation",
            kill=False,
        )

    if action == "terminate_calibration_launcher":
        return _request_launcher_stop(
            state,
            action=action,
            params=params,
            category="calibration",
            kill=False,
        )

    if action == "terminate_all_launchers":
        return _request_launcher_stop(
            state,
            action=action,
            params=params,
            category=None,
            kill=False,
        )

    conflict_reason = _operator_status_operation_conflict(state, requested_action=action)
    if conflict_reason is not None:
        return False, conflict_reason, {"action": action, "operation_conflict": True}

    if action == "progress_status":
        progress = (
            state.progress_monitor.status(check_external=True).to_dict()
            if state.progress_monitor is not None
            else {"url": state.progress_url, "running": False, "owned_by_console": False}
        )
        return True, "progress monitor status", {
            "action": action,
            "progress_monitor": progress,
            "progress_url": progress.get("url") or state.progress_url,
        }

    if action == "acquire_authority":
        if state.authority.held and state.authority.session_id != session_id:
            return False, "authority is already held by another browser session", {}
        if state.authority.held and state.authority.session_id == session_id:
            return True, "authority is already held by this browser session", {
                "necst_held": state.authority.necst_held,
                "identity": state.authority.necst_identity,
                "mode": state.authority.mode,
            }
        actions = _load_operator_actions()
        handle = actions.OperatorAuthoritySession(dry_run=state.action_mode == "dry-run")
        result = handle.acquire()
        ok, message, data = _result_to_response(result)
        if not ok:
            return ok, message, data
        state.authority_handle = handle
        state.authority.held = True
        state.authority.session_id = session_id
        state.authority.necst_held = bool(data.get("held")) and state.action_mode != "dry-run"
        state.authority.necst_identity = data.get("identity")
        state.authority.mode = "dry-run" if state.action_mode == "dry-run" else "necst"
        state.authority.note = message
        return True, message, data

    if action == "release_authority":
        return _release_console_authority(state, session_id=session_id)

    if action == "check_observation":
        mode, path, channel = validate_observation_selection(params)
        result = _load_operator_actions().check_obs_file(
            mode,
            path,
            channel=channel,
            require_exists=state.action_mode != "dry-run",
        )
        ok, message, data = _result_to_response(result)
        data["action"] = action
        preview = str(params.get("preview") or "")
        if preview:
            data["preview_line_count"] = len(preview.splitlines())
            data["preview_size_bytes"] = len(preview.encode("utf-8", errors="replace"))
        return ok, message, data

    if action == "dry_run_observation":
        mode, path, channel = validate_observation_selection(params)
        result = _load_operator_actions().dry_run_observation(
            mode,
            path,
            channel=channel,
            require_exists=state.action_mode != "dry-run",
        )
        ok, message, data = _result_to_response(result)
        data["action"] = action
        return ok, message, data

    if action in {"mount_move", "start_tracking", "start_observation", "run_rsky", "run_skydip", "chopper_in", "chopper_out", "chopper_alarm_reset", "chopper_home", "chopper_recover"}:
        allowed, reason = _authority_allows(state, session_id)
        if not allowed:
            return False, reason, {"action": action}

    if action == "mount_move_dry_run":
        validate_site_capability(state, "mount_move", action_label="mount move")
        az, el = validate_mount_target(params, state.mount_limits)
        return True, f"dry-run mount move OK: Az={az:.6f} deg, El={el:.6f} deg; no command was sent", {
            "action": action,
            "az_deg": az,
            "el_deg": el,
            "unit": "deg",
            "direct_mode": True,
            "az_target_mode": "mount",
        }

    if action == "mount_move":
        validate_site_capability(state, "mount_move", action_label="mount move")
        az, el = validate_mount_target(params, state.mount_limits)
        if state.action_mode == "dry-run":
            state.last_command_az = az
            state.last_command_el = el
            state.last_mount_target_az = az
            state.last_mount_target_el = el
            state.last_mount_target_started_at = time.time()
            state.last_mount_target_reached_since = None
            return True, f"dry-run mount move: Az={az:.6f} deg, El={el:.6f} deg; no command was sent", {
                "action": action,
                "az_deg": az,
                "el_deg": el,
                "unit": "deg",
                "direct_mode": True,
                "az_target_mode": "mount",
                "dry_run": True,
            }
        result = _load_operator_actions().mount_move(
            az,
            el,
            wait=False,
            dry_run=False,
            commander=_held_commander_for_session(state, session_id),
        )
        ok, message, data = _result_to_response(result)
        if ok:
            _mark_exclusive_start_guard(
                state,
                action=action,
                session_id=session_id,
                message=f"mount move Az={az:.4f} deg El={el:.4f} deg",
            )
            _clear_safety_release_request(state)
            state.last_command_az = az
            state.last_command_el = el
            state.last_mount_target_az = az
            state.last_mount_target_el = el
            state.last_mount_target_started_at = time.time()
            state.last_mount_target_reached_since = None
            state.last_manual_state = "moving"
            state.last_active_task = "manual mount move"
        return ok, message, data

    if action == "stop":
        safety_data = _safety_authority_bypass_data(state, session_id)
        if state.action_mode == "dry-run":
            data = {"action": action, "dry_run": True}
            data.update(safety_data)
            return True, "dry-run STOP: antenna stop command was not sent", data
        result = _load_operator_actions().antenna_stop(
            commander=_any_held_commander(state),
        )
        ok, message, data = _result_to_response(result)
        data.update(safety_data)
        if ok:
            active_obs_labels = _active_process_labels(state, category="observation")
            if active_obs_labels:
                state.last_observation_stop_requested_at = time.time()
                state.last_observation_safety_labels = active_obs_labels
            _clear_exclusive_start_guard(state)
            _mark_safety_release_request(state)
            state.last_manual_state = "stopped"
            state.last_active_task = "none"
            state.last_command_az = None
            state.last_command_el = None
            # Keep last_mount_target_* so the status adapter can still recognize
            # that the fixed mount-move target has been reached after STOP clears
            # /ctrl/antenna/altaz.
        return ok, message, data

    if action == "start_tracking":
        validate_site_capability(state, "target_tracking", action_label="target tracking")
        actions = _load_operator_actions()
        dry_run = state.action_mode == "dry-run"
        result = actions.start_target_tracking(
            params.get("kind"),
            name=params.get("name"),
            coord1=params.get("coord1"),
            coord2=params.get("coord2"),
            offset_frame=params.get("offset_frame", "target_frame"),
            offset_x_arcsec=params.get("offset_x_arcsec", 0),
            offset_y_arcsec=params.get("offset_y_arcsec", 0),
            cos_correction=params.get("cos_correction", True),
            wait=False,
            dry_run=dry_run,
            commander=_held_commander_for_session(state, session_id),
        )
        ok, message, data = _result_to_response(result)
        if ok and not dry_run:
            _mark_exclusive_start_guard(
                state,
                action=action,
                session_id=session_id,
                message="target tracking",
            )
            _clear_safety_release_request(state)
            state.last_command_az = None
            state.last_command_el = None
            _clear_last_mount_target(state)
            state.last_manual_state = "tracking"
            state.last_active_task = "target tracking"
        return ok, message, data

    if action == "stop_tracking":
        safety_data = _safety_authority_bypass_data(state, session_id)
        if state.action_mode == "dry-run":
            data = {"action": action, "dry_run": True}
            data.update(safety_data)
            return True, "dry-run Stop tracking: antenna stop command was not sent", data
        result = _load_operator_actions().stop_tracking(
            commander=_any_held_commander(state),
        )
        ok, message, data = _result_to_response(result)
        data.update(safety_data)
        if ok:
            _clear_exclusive_start_guard(state)
            _mark_safety_release_request(state)
            state.last_manual_state = "stopped"
            state.last_active_task = "none"
            state.last_command_az = None
            state.last_command_el = None
        return ok, message, data

    if action == "abort_observation":
        safety_data = _safety_authority_bypass_data(state, session_id)
        if state.action_mode == "dry-run":
            data = {"action": action, "dry_run": True}
            data.update(safety_data)
            return True, "dry-run ABORT: observation abort command was not sent", data
        result = _load_operator_actions().abort_observation(
            requester="necst console",
            commander=_any_held_commander(state),
        )
        ok, message, data = _result_to_response(result)
        data.update(safety_data)
        if ok:
            active_obs_labels = _active_process_labels(state, category="observation")
            if active_obs_labels:
                state.last_observation_abort_requested_at = time.time()
                state.last_observation_safety_labels = active_obs_labels
            _clear_exclusive_start_guard(state)
            _mark_safety_release_request(state)
            state.last_manual_state = "stopped"
            state.last_active_task = "none"
            state.last_command_az = None
            state.last_command_el = None
            _clear_last_mount_target(state)
        return ok, message, data

    if action == "start_observation":
        validate_site_capability(state, "observation_start", action_label="observation start")
        mode, path, channel = validate_observation_selection(params)
        dry_run = state.action_mode == "dry-run"
        actions = _load_operator_actions()
        if not dry_run:
            # Validate the launcher command completely before releasing a held
            # console authority.  Otherwise a typo in the obs-file path could
            # drop the operator's authority and then fail before starting
            # anything, which is safe but confusing during operations.
            preflight = actions.start_observation(
                mode,
                path,
                channel=channel,
                background=True,
                dry_run=True,
                check_exists=True,
            )
            pre_ok, pre_message, pre_data = _result_to_response(preflight)
            if not pre_ok:
                pre_data["preflight_before_authority_release"] = True
                return False, pre_message, pre_data
        release_notice = _release_before_subprocess_if_needed(state, session_id)
        stdout_path, stderr_path = _launcher_log_paths(state, "start_observation")
        result = actions.start_observation(
            mode,
            path,
            channel=channel,
            background=True,
            dry_run=dry_run,
            check_exists=not dry_run,
            stdout_path=stdout_path,
            stderr_path=stderr_path,
        )
        ok, message, data = _result_to_response(result)
        if isinstance(data, dict):
            data.setdefault("obs_mode", mode)
            data.setdefault("obs_path", path)
            if channel not in (None, ""):
                data.setdefault("channel", channel)
        if ok and not dry_run:
            _mark_exclusive_start_guard(
                state,
                action=action,
                session_id=session_id,
                message=f"observation {mode}",
            )
            _clear_safety_release_request(state)
            state.last_command_az = None
            state.last_command_el = None
            _clear_last_mount_target(state)
            state.last_manual_state = "observing sequence"
            state.last_active_task = "observation"
            _register_launcher_if_needed(
                state,
                action=action,
                category="observation",
                data=data,
                label=f"observation:{mode}",
            )
            if release_notice:
                message = f"{release_notice}; {message}"
        return ok, message, data

    if action == "run_rsky":
        validate_site_capability(state, "rsky", action_label="RSky")
        dry_run = state.action_mode == "dry-run"
        actions = _load_operator_actions()
        if not dry_run:
            preflight = actions.run_rsky(
                n=params.get("n", 1),
                integ=params.get("integ", 2),
                channel=params.get("ch"),
                background=True,
                dry_run=True,
            )
            pre_ok, pre_message, pre_data = _result_to_response(preflight)
            if not pre_ok:
                pre_data["preflight_before_authority_release"] = True
                return False, pre_message, pre_data
        release_notice = _release_before_subprocess_if_needed(state, session_id)
        stdout_path, stderr_path = _launcher_log_paths(state, "run_rsky")
        result = actions.run_rsky(
            n=params.get("n", 1),
            integ=params.get("integ", 2),
            channel=params.get("ch"),
            background=True,
            dry_run=dry_run,
            stdout_path=stdout_path,
            stderr_path=stderr_path,
        )
        ok, message, data = _result_to_response(result)
        if ok and not dry_run:
            _mark_exclusive_start_guard(
                state,
                action=action,
                session_id=session_id,
                message="RSky",
            )
            _clear_safety_release_request(state)
            state.last_command_az = None
            state.last_command_el = None
            _clear_last_mount_target(state)
            state.last_manual_state = "calibration"
            state.last_active_task = "RSky"
            _register_launcher_if_needed(
                state,
                action=action,
                category="calibration",
                data=data,
                label="calibration:RSky",
            )
            if release_notice:
                message = f"{release_notice}; {message}"
        return ok, message, data

    if action == "run_skydip":
        validate_site_capability(state, "skydip", action_label="SkyDip")
        dry_run = state.action_mode == "dry-run"
        actions = _load_operator_actions()
        if not dry_run:
            preflight = actions.run_skydip(
                integ=params.get("integ", 2),
                channel=params.get("ch"),
                tp_range=params.get("tp_range"),
                background=True,
                dry_run=True,
            )
            pre_ok, pre_message, pre_data = _result_to_response(preflight)
            if not pre_ok:
                pre_data["preflight_before_authority_release"] = True
                return False, pre_message, pre_data
        release_notice = _release_before_subprocess_if_needed(state, session_id)
        stdout_path, stderr_path = _launcher_log_paths(state, "run_skydip")
        result = actions.run_skydip(
            integ=params.get("integ", 2),
            channel=params.get("ch"),
            tp_range=params.get("tp_range"),
            background=True,
            dry_run=dry_run,
            stdout_path=stdout_path,
            stderr_path=stderr_path,
        )
        ok, message, data = _result_to_response(result)
        if ok and not dry_run:
            _mark_exclusive_start_guard(
                state,
                action=action,
                session_id=session_id,
                message="SkyDip",
            )
            _clear_safety_release_request(state)
            state.last_command_az = None
            state.last_command_el = None
            _clear_last_mount_target(state)
            state.last_manual_state = "calibration"
            state.last_active_task = "SkyDip"
            _register_launcher_if_needed(
                state,
                action=action,
                category="calibration",
                data=data,
                label="calibration:SkyDip",
            )
            if release_notice:
                message = f"{release_notice}; {message}"
        return ok, message, data

    if action == "chopper_status":
        validate_site_capability(state, "chopper_status", action_label="chopper status")
        if state.action_mode == "dry-run":
            return True, "dry-run chopper status: no telemetry query was sent", {
                "action": action,
                "dry_run": True,
                "site_config": state.site_summary.to_dict(),
            }
        result = _load_operator_actions().chopper_status(
            commander=_held_commander_for_session(state, session_id),
        )
        ok, message, data = _result_to_response(result)
        if ok:
            state.last_chopper_state = str(data.get("state") or state.last_chopper_state)
        return ok, message, data

    if action in {"chopper_in", "chopper_out"}:
        validate_site_capability(state, "chopper_move", action_label="chopper move")
        command = "in" if action == "chopper_in" else "out"
        if state.action_mode == "dry-run":
            data = chopper_dry_run_payload(state, command)
            data.update({"action": action, "dry_run": True})
            return True, f"dry-run chopper {command.upper()}: command was not sent", data
        result = _load_operator_actions().chopper_move(
            command,
            wait=False,
            commander=_held_commander_for_session(state, session_id),
        )
        ok, message, data = _result_to_response(result)
        if ok:
            state.last_chopper_state = "IN" if command == "in" else "OUT"
            if data.get("target_position") is not None:
                state.last_chopper_position = float(data["target_position"])
        return ok, message, data

    if action in {"chopper_alarm_reset", "chopper_home", "chopper_recover"}:
        validate_site_capability(
            state, "chopper_maintenance", action_label="chopper maintenance"
        )
        maintenance = {
            "chopper_alarm_reset": "alarm-reset",
            "chopper_home": "home",
            "chopper_recover": "recover",
        }[action]
        if state.action_mode == "dry-run":
            return True, f"dry-run chopper {maintenance}: command was not sent", {
                "action": action,
                "dry_run": True,
                "command": maintenance,
            }
        result = _load_operator_actions().chopper_maintenance(
            maintenance,
            commander=_held_commander_for_session(state, session_id),
        )
        return _result_to_response(result)

    return False, f"unknown action: {action!r}", {"action": action}


def _operator_status_to_v7_status(
    state: OperatorConsoleState, status_state: Mapping[str, Any]
) -> JsonDict:
    """Adapt canonical operator_status to the v7 console-demo JSON shape."""

    op = status_state.get("operator_status") if isinstance(status_state, Mapping) else {}
    if not isinstance(op, Mapping):
        op = {}
    system = op.get("system") if isinstance(op.get("system"), Mapping) else {}
    motion = op.get("motion") if isinstance(op.get("motion"), Mapping) else {}
    antenna = op.get("antenna") if isinstance(op.get("antenna"), Mapping) else {}
    chopper = op.get("chopper") if isinstance(op.get("chopper"), Mapping) else {}
    progress = op.get("progress") if isinstance(op.get("progress"), Mapping) else {}
    observation = op.get("observation") if isinstance(op.get("observation"), Mapping) else {}
    paths = op.get("paths") if isinstance(op.get("paths"), Mapping) else {}
    warnings = op.get("warnings") if isinstance(op.get("warnings"), list) else []

    sys_state = str(system.get("state") or "unknown").lower()
    if sys_state in {"", "unknown"}:
        sys_state = "idle"

    process_records = state.process_registry.all_records(refresh=False)
    active_process_categories = {
        str(record.category or "")
        for record in process_records
        if bool(
            record.is_active()
            if hasattr(record, "is_active")
            else getattr(record, "status", "") not in {"exited", "lost"}
        )
    }
    stale_observation = _stale_observation_state(state, status_state, op)
    if stale_observation.get("running_stale") or stale_observation.get("launcher_stuck"):
        sys_state = "stale_observation"
        warnings = list(warnings) + [
            stale_observation.get("reason")
            or "stale/stuck observation state remains; recover before starting a new operation"
        ]
    elif sys_state == "idle":
        if "observation" in active_process_categories:
            sys_state = "observing"
        elif "calibration" in active_process_categories:
            sys_state = "calibrating"

    motion_live_active = motion.get("live_motion_active")
    motion_mount_target_reached = bool(motion.get("mount_target_reached"))
    motion_mount_hold_at_target = bool(motion.get("mount_hold_at_target"))
    motion_at_target_idle = bool(
        (motion_mount_target_reached or motion_mount_hold_at_target)
        and motion_live_active is False
    )

    raw_active_task = str(motion.get("active_task") or "").strip()
    if motion_at_target_idle:
        raw_active_task = "idle"
    if raw_active_task.lower() in {"", "idle", "none", "unknown"}:
        # In live mode, do not resurrect the last console-side action.  That
        # stale fallback made the Manual panel keep showing "moving" after
        # an abort or after the command/status topics disappeared.  Dry-run
        # still uses the local action memory because there are no live topics.
        active_task = str(state.last_active_task or "none") if state.action_mode == "dry-run" else "none"
    else:
        active_task = raw_active_task
    if active_task.lower() in {"", "idle", "unknown"}:
        active_task = "none"

    raw_manual_state = str(motion.get("stage") or "").strip()
    if motion_at_target_idle:
        raw_manual_state = "idle"
    if raw_manual_state.lower() in {"", "idle", "none", "unknown"}:
        manual_state = str(state.last_manual_state or "idle") if state.action_mode == "dry-run" else "idle"
    else:
        manual_state = raw_manual_state
    if manual_state.lower() in {"", "none", "unknown"}:
        manual_state = "idle"

    if state.action_mode != "dry-run" and manual_state.lower() == "tracking":
        # Some low-level antenna controllers use a generic "tracking" stage
        # for fixed Az/El command hold after mount_move.  Do not present that as
        # target tracking unless the console actually started target tracking.
        last_manual = str(state.last_manual_state or "").strip().lower()
        if last_manual == "moving":
            manual_state = "moving"
            if active_task.lower() in {"", "idle", "none", "unknown", "tracking"}:
                active_task = "manual mount move"
        elif last_manual == "tracking":
            if active_task.lower() in {"", "idle", "none", "unknown", "tracking"}:
                active_task = "target tracking"

    current_az = antenna.get("current_az_deg")
    current_el = antenna.get("current_el_deg")
    try:
        az = float(current_az) if current_az is not None else None
        el = float(current_el) if current_el is not None else None
    except Exception:
        az, el = None, None
    if az is None or el is None:
        warnings = list(warnings) + ["antenna current Az/El is unavailable"]

    motion_source_lower = str(motion.get("live_motion_source") or "").strip().lower()
    operator_mount_target_reached = False
    operator_mount_target_idle = False
    if state.action_mode != "dry-run" and sys_state in {"", "idle", "unknown", "finished", "aborted", "error", "failed"}:
        operator_mount_target_reached = _last_mount_target_reached(
            state,
            current_az=az,
            current_el=el,
            require_settle_sec=0.5,
            update_state=True,
        )
        # If the encoder itself still reports motion, do not override.  But stale
        # SKY/section/control activity after STOP or fixed-target hold should not
        # keep the UI locked once the last manual mount target has settled.
        operator_mount_target_idle = bool(
            operator_mount_target_reached and motion_source_lower != "encoder_delta"
        )
    if operator_mount_target_idle:
        manual_state = "idle"
        active_task = "none"
        motion_live_active = False
        motion_mount_target_reached = True
        motion_mount_hold_at_target = True
        motion_source_lower = "operator_mount_target_idle"

    cmd_az = antenna.get("command_az_deg")
    cmd_el = antenna.get("command_el_deg")
    if state.action_mode == "dry-run":
        if cmd_az is None:
            cmd_az = state.last_command_az
        if cmd_el is None:
            cmd_el = state.last_command_el

    command_absent = cmd_az is None and cmd_el is None
    safety_release_age = _recent_safety_release_request_age_sec(state)
    safety_release_idle = bool(
        state.action_mode != "dry-run"
        and safety_release_age is not None
        and sys_state in {"", "idle", "unknown", "finished", "aborted", "error", "failed"}
        and command_absent
        and safety_release_age >= SAFETY_RELEASE_ASSUME_IDLE_AFTER_SEC
    )
    if safety_release_idle:
        # STOP/ABORT means the operator intentionally released the current
        # manual control.  Once the command topic is gone and the system
        # lifecycle is final/idle, do not keep waiting for the old target and do
        # not let stale SKY/section_status or encoder-motion hysteresis trap the
        # UI in STOP REQUESTED/ATTENTION.
        manual_state = "idle"
        active_task = "none"
        motion_live_active = False
        motion_mount_hold_at_target = False
        motion_source_lower = "operator_safety_release"

    if state.action_mode == "dry-run":
        chopper_state = str(chopper.get("state") or state.last_chopper_state or "unknown")
        chopper_position = chopper.get("position")
        if chopper_position is None:
            chopper_position = state.last_chopper_position
    else:
        chopper_state = str(chopper.get("state") or "unknown")
        chopper_position = chopper.get("position")
    chopper_age = chopper.get("age_sec")
    if chopper_age is None:
        age_text = "unknown"
    else:
        try:
            age_text = f"{float(chopper_age):.1f}s"
        except Exception:
            age_text = "unknown"

    percent = progress.get("percent")
    observation_progress_running = (
        sys_state not in {"idle", "finished", "aborted", "error", "unknown"}
        or percent is not None
    )
    monitor_status = (
        state.progress_monitor.status(check_external=False).to_dict()
        if state.progress_monitor is not None
        else {"url": state.progress_url, "running": False, "owned_by_console": False}
    )
    progress_running = bool(monitor_status.get("running")) or bool(observation_progress_running)

    return {
        "telescope": state.telescope,
        "state": sys_state,
        "manual_state": manual_state,
        "active_task": active_task,
        "motion_live_active": motion_live_active,
        "motion_live_source": motion_source_lower or motion.get("live_motion_source"),
        "mount_target_reached": motion_mount_target_reached,
        "mount_hold_at_target": motion_mount_hold_at_target,
        "operator_last_mount_target_reached": bool(operator_mount_target_reached),
        "operator_last_mount_target_idle": bool(operator_mount_target_idle),
        "operator_last_mount_target_az": state.last_mount_target_az,
        "operator_last_mount_target_el": state.last_mount_target_el,
        "operator_last_mount_target_reached_since": state.last_mount_target_reached_since,
        "operator_safety_release_idle": bool(safety_release_idle),
        "operator_safety_release_age_sec": safety_release_age,
        "mount_target_reached_tol_deg": motion.get("mount_target_reached_tol_deg"),
        "encoder_motion_deadband_deg": motion.get("encoder_motion_deadband_deg"),
        "encoder_motion_delta_az_deg": motion.get("encoder_motion_delta_az_deg"),
        "encoder_motion_delta_el_deg": motion.get("encoder_motion_delta_el_deg"),
        "az": az,
        "el": el,
        "command_az": cmd_az,
        "command_el": cmd_el,
        "chopper": {
            "state": chopper_state,
            "position": chopper_position if chopper_position is not None else "unknown",
            "age": age_text,
        },
        "stale_observation": stale_observation,
        "progress": {
            "running": bool(progress_running),
            "url": str(monitor_status.get("url") or state.progress_url),
            "percent": percent,
            "label": progress.get("label"),
            "observation_running": bool(observation_progress_running),
            "monitor": monitor_status,
            "owned_by_console": bool(monitor_status.get("owned_by_console")),
            "monitor_status": monitor_status.get("status"),
        },
        "observation": {
            "record_name": observation.get("record_name"),
            "obs_file": observation.get("obs_file"),
            "recording_dir": observation.get("recording_dir") or paths.get("recording_dir"),
            "local_recording_dir": observation.get("local_recording_dir") or paths.get("local_recording_dir"),
            "record_path_display_mode": observation.get("record_path_display_mode") or paths.get("record_path_display_mode") or "local",
            "progress_record_dir": observation.get("progress_record_dir") or paths.get("progress_record_dir"),
        },
        "authority": {
            "held": state.authority.held,
            "session_id": state.authority.session_id,
            "necst_held": state.authority.necst_held,
            "necst_identity": state.authority.necst_identity,
            "mode": state.authority.mode,
            "note": state.authority.note,
            "sync_reason": (
                _sync_authority_state(state, log_if_cleared=False).get("reason")
                if state.authority.held or state.authority_handle is not None
                else "already_free"
            ),
            "safety_actions_bypass_browser_gate": True,
            "safety_actions": ["stop", "stop_tracking", "abort_observation", "terminate_*_launcher"],
        },
        "mount_limits": dict(state.mount_limits),
        "site": state.site_summary.to_dict(),
        "capabilities": dict(state.site_capabilities),
        "warning_count": len(warnings) + len(state.site_summary.warnings),
        "warnings": list(warnings) + list(state.site_summary.warnings),
        "log": [entry.__dict__ for entry in state.log],
        "processes": [r.to_dict() for r in state.process_registry.all_records(refresh=False)],
        "process_counts": state.process_registry.counts(),
        "exclusive_start_guard": {
            "action": state.exclusive_start_action,
            "session_id": state.exclusive_start_session_id,
            "started_at": state.exclusive_start_started_at,
            "age_sec": (
                max(0.0, time.time() - float(state.exclusive_start_started_at))
                if state.exclusive_start_started_at is not None
                else None
            ),
            "message": state.exclusive_start_message,
            "guard_sec": EXCLUSIVE_START_STATUS_SEC,
            "blocking_sec": EXCLUSIVE_START_BLOCK_SEC,
        },
        "launcher_log_choices": log_reader.launcher_log_choices([
            r.to_dict() for r in state.process_registry.all_records(refresh=False)
        ]),
        "shutdown": _shutdown_snapshot(state),
        "self_check_endpoint": "/api/self-check",
        "operator_log_path": str(state.operator_log_path) if state.operator_log_path is not None else None,
        "launcher_log_dir": str(state.launcher_log_dir) if state.launcher_log_dir is not None else None,
        "observation_log": (state.observation_log.status() if state.observation_log is not None else {"ok": False}),
        "action_mode": state.action_mode,
        "live_actions": {
            "enabled": bool(state.live_actions_enabled),
            "guarded": state.action_mode == "live" and not state.live_actions_enabled,
            "write_actions": sorted(LIVE_WRITE_ACTIONS),
            "safety_actions": sorted(SAFETY_ACTIONS),
            "message": (
                "live write actions enabled by default in live mode"
                if state.action_mode == "live" and state.live_actions_enabled
                else (
                    "live write actions blocked by --guard-live-actions"
                    if state.action_mode == "live"
                    else "dry-run mode: no live command is sent"
                )
            ),
        },
        "status_refresh_ms": int(state.status_refresh_ms),
        "live_telemetry": (
            state.live_cache.snapshot()
            if state.live_cache is not None
            else {"requested": False, "available": False, "spin_mode": "disabled"}
        ),
        "operator_status": op,
    }


def _build_console_status_unchecked(state: OperatorConsoleState) -> JsonDict:
    with state.lock:
        _refresh_processes_and_log(state)
        _sync_authority_state(state)
        _prune_exclusive_start_guard(state)
        live_payload = state.live_cache.snapshot() if state.live_cache is not None else None
        status_state = status_model.build_progress_status_state(
            state.progress_root,
            events_limit=state.events_limit,
            live_payload=live_payload,
        )
        payload = _operator_status_to_v7_status(state, status_state)
        if state.safe_start or state.rescue_mode:
            payload["safe_start"] = bool(state.safe_start)
            payload["rescue_mode"] = bool(state.rescue_mode)
            payload.setdefault("warnings", []).append(
                "Console started in safe/rescue mode; no hardware command was sent automatically."
            )
            payload["warning_count"] = len(payload.get("warnings") or [])
        if state.local_state_reset_archive:
            payload["local_state_reset"] = dict(state.local_state_reset_summary or {"archive_dir": state.local_state_reset_archive})
        return payload


def build_minimal_rescue_status(state: OperatorConsoleState, exc: BaseException) -> JsonDict:
    """Return a status payload that is deliberately hard to break.

    The browser must keep showing STOP/ABORT and local recovery controls even if
    the normal status builder has a bug or a corrupted local pointer file.
    """

    message = f"{type(exc).__name__}: {exc}"
    now = time.time()
    try:
        process_records = state.process_registry.all_records(refresh=False)
        processes = [r.to_dict() for r in process_records]
        process_counts = state.process_registry.counts()
    except Exception as proc_exc:
        processes = []
        process_counts = {"total": 0, "active": 0, "finished": 0, "by_category": {}, "error": str(proc_exc)}
    try:
        obslog_status = state.observation_log.status() if state.observation_log is not None else {"ok": False}
    except Exception as log_exc:
        obslog_status = {"ok": False, "last_error": str(log_exc)}
    try:
        log_entries = [entry.__dict__ for entry in state.log]
    except Exception:
        log_entries = []
    try:
        site_dict = state.site_summary.to_dict()
    except Exception:
        site_dict = {"source": "unavailable"}
    try:
        mount_limits = dict(state.mount_limits)
    except Exception:
        mount_limits = {}
    try:
        progress_status = state.progress_monitor.status().to_dict() if state.progress_monitor is not None else {}
    except Exception as progress_exc:
        progress_status = {"status": "error", "message": str(progress_exc)}
    warnings = [
        "Normal console status generation failed; showing minimal rescue status.",
        message,
        "STOP/ABORT and local recovery actions remain available. No hardware command was sent automatically.",
    ]
    payload: JsonDict = {
        "telescope": state.telescope,
        "progress_url": state.progress_url,
        "progress": {
            "url": state.progress_url,
            "running": False,
            "owned_by_console": bool(progress_status.get("owned_by_console")),
            "monitor": progress_status,
        },
        "state": "status_error",
        "manual_state": "unknown",
        "active_task": "status builder failed",
        "az": None,
        "el": None,
        "command_az": None,
        "command_el": None,
        "mount_limits": mount_limits,
        "site": site_dict,
        "capabilities": dict(getattr(state, "site_capabilities", {}) or {}),
        "warning_count": len(warnings),
        "warnings": warnings,
        "authority": {
            "held": bool(getattr(state.authority, "held", False)),
            "session_id": getattr(state.authority, "session_id", None),
            "necst_held": bool(getattr(state.authority, "necst_held", False)),
            "mode": getattr(state.authority, "mode", "unknown"),
            "safety_actions_bypass_browser_gate": True,
            "safety_actions": ["stop", "stop_tracking", "abort_observation", "terminate_*_launcher"],
        },
        "chopper": {
            "state": getattr(state, "last_chopper_state", "unknown") or "unknown",
            "position": getattr(state, "last_chopper_position", None),
            "age": "status unavailable",
        },
        "observation": {},
        "stale_observation": {
            "running_stale": True,
            "launcher_stuck": bool((process_counts or {}).get("active", 0)),
            "can_clear": True,
            "can_terminate_launcher": bool((process_counts or {}).get("active", 0)),
            "record_name": "unknown",
            "reason": "normal status builder failed; clear only local console/progress state after confirming hardware safety",
        },
        "log": log_entries,
        "processes": processes,
        "process_counts": process_counts,
        "launcher_log_choices": [],
        "shutdown": _shutdown_snapshot(state),
        "self_check_endpoint": "/api/self-check",
        "operator_log_path": str(state.operator_log_path) if state.operator_log_path is not None else None,
        "launcher_log_dir": str(state.launcher_log_dir) if state.launcher_log_dir is not None else None,
        "observation_log": obslog_status,
        "action_mode": state.action_mode,
        "live_actions": {
            "enabled": bool(state.live_actions_enabled),
            "guarded": state.action_mode == "live" and not bool(state.live_actions_enabled),
            "write_actions": sorted(LIVE_WRITE_ACTIONS),
            "safety_actions": sorted(SAFETY_ACTIONS),
            "message": "minimal rescue status after status builder failure",
        },
        "status_refresh_ms": int(state.status_refresh_ms),
        "live_telemetry": {"requested": not bool(state.status_no_ros), "available": False, "spin_mode": "rescue", "error": message},
        "operator_status": {},
        "status_error": {"message": message, "time": now},
        "safe_start": bool(state.safe_start),
        "rescue_mode": bool(state.rescue_mode),
        "local_state_reset": dict(state.local_state_reset_summary or {}),
    }
    return payload


def build_console_status(state: OperatorConsoleState) -> JsonDict:
    try:
        return _build_console_status_unchecked(state)
    except Exception as exc:
        message = f"{type(exc).__name__}: {exc}"
        # Log sparingly so a refresh loop does not flood the internal JSONL log.
        try:
            with state.lock:
                last = getattr(state, "last_status_exception_message", "")
                last_at = float(getattr(state, "last_status_exception_at", 0.0) or 0.0)
                if message != last or (time.time() - last_at) > 30.0:
                    state.last_status_exception_message = message
                    state.last_status_exception_at = time.time()
                    state.add_log(False, f"normal status generation failed; using minimal rescue status: {message}", action="status_rescue")
        except Exception:
            pass
        return build_minimal_rescue_status(state, exc)


def load_demo_html(status_refresh_ms: int = 1000) -> str:
    """Load the v7 demo HTML so the real console keeps the approved layout."""

    demo_path = Path(__file__).resolve().parents[2] / "bin" / "console-demo.py"
    text = demo_path.read_text(encoding="utf-8")
    marker = 'HTML = r"""'
    start = text.find(marker)
    if start < 0:
        raise RuntimeError("failed to find HTML block in bin/console-demo.py")
    start += len(marker)
    end = text.find('"""', start)
    if end < 0:
        raise RuntimeError("failed to find end of HTML block in bin/console-demo.py")
    html = text[start:end]
    html = html.replace(
        "NECST Operator Console Demo",
        "NECST Operator Console",
    ).replace(
        "standalone layout preview / no ROS / no telescope command",
        "real console entry point / actions use necst.core.operator_actions",
    )
    config_script = (
        "<script>window.NECST_CONSOLE_CONFIG = "
        + json.dumps({"statusRefreshMs": int(status_refresh_ms)})
        + ";</script>\n"
    )
    return html.replace("</head>", config_script + "</head>")


class OperatorConsoleHTTPServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(
        self,
        server_address: Tuple[str, int],
        handler_class: type[BaseHTTPRequestHandler],
        *,
        state: OperatorConsoleState,
        html: Optional[str] = None,
    ) -> None:
        super().__init__(server_address, handler_class)
        self.state = state
        self.html = html if html is not None else load_demo_html(state.status_refresh_ms)


class OperatorConsoleHandler(BaseHTTPRequestHandler):
    server: OperatorConsoleHTTPServer

    def log_message(self, fmt: str, *args: Any) -> None:
        if not self.server.state.quiet:
            super().log_message(fmt, *args)

    def _send_json(self, payload: Mapping[str, Any], status: int = 200) -> None:
        body = json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_text(self, text: str, content_type: str = "text/html; charset=utf-8") -> None:
        body = text.encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:  # noqa: N802 - stdlib method name
        if self.path in {"/", "/index.html"}:
            self._send_text(self.server.html)
            return
        if self.path == "/health":
            self._send_json({
                "ok": True,
                "action_mode": self.server.state.action_mode,
                "live_actions_enabled": bool(self.server.state.live_actions_enabled),
                "status_refresh_ms": int(self.server.state.status_refresh_ms),
            })
            return
        if self.path == "/api/status":
            self._send_json(build_console_status(self.server.state))
            return
        if self.path == "/api/rescue/status":
            self._send_json(build_minimal_rescue_status(self.server.state, RuntimeError("rescue status requested")))
            return
        parsed = urllib.parse.urlparse(self.path)
        query = urllib.parse.parse_qs(parsed.query)
        if parsed.path == "/api/obs-roots":
            self._send_json(obs_roots_payload(self.server.state.obs_roots))
            return
        if parsed.path == "/api/obs-list":
            try:
                directory = (query.get("dir") or [""])[0]
                self._send_json(list_server_obs_files(self.server.state.obs_roots, directory))
            except Exception as exc:
                self._send_json({"ok": False, "reason": str(exc), "entries": []}, status=400)
            return
        if parsed.path == "/api/obs-preview":
            try:
                path = (query.get("path") or [""])[0]
                max_bytes = int((query.get("max_bytes") or [65536])[0])
                self._send_json(preview_server_obs_file(self.server.state.obs_roots, path, max_bytes=max_bytes))
            except Exception as exc:
                self._send_json({"ok": False, "reason": str(exc), "text": ""}, status=400)
            return
        if self.path == "/api/operator-status":
            state = self.server.state
            try:
                live_payload = state.live_cache.snapshot() if state.live_cache is not None else None
                status_state = status_model.build_progress_status_state(
                    state.progress_root,
                    events_limit=state.events_limit,
                    live_payload=live_payload,
                )
                self._send_json(status_state.get("operator_status", {}))
            except Exception as exc:
                self._send_json({"ok": False, "reason": str(exc), "operator_status": {}}, status=200)
            return
        if parsed.path == "/api/processes":
            state = self.server.state
            with state.lock:
                _refresh_processes_and_log(state)
                process_records = [r.to_dict() for r in state.process_registry.all_records(refresh=False)]
                self._send_json({
                    "processes": process_records,
                    "process_counts": state.process_registry.counts(),
                    "launcher_log_choices": log_reader.launcher_log_choices(process_records),
                    "progress_monitor": (
                        state.progress_monitor.status(check_external=True).to_dict()
                        if state.progress_monitor is not None
                        else {"url": state.progress_url, "running": False, "owned_by_console": False}
                    ),
                    "shutdown": _shutdown_snapshot(state),
                    "operator_log_path": str(state.operator_log_path) if state.operator_log_path is not None else None,
                    "launcher_log_dir": str(state.launcher_log_dir) if state.launcher_log_dir is not None else None,
                })
            return
        if parsed.path == "/api/operator-log":
            state = self.server.state
            with state.lock:
                _refresh_processes_and_log(state)
                limit = (query.get("limit") or [100])[0]
                self._send_json(log_reader.read_operator_log(state.operator_log_path, limit=limit))
            return
        if parsed.path == "/api/log-file":
            state = self.server.state
            with state.lock:
                path = (query.get("path") or [""])[0]
                max_bytes = (query.get("max_bytes") or [32768])[0]
                extra_roots = []
                if state.progress_monitor is not None and getattr(state.progress_monitor, "log_dir", None) is not None:
                    extra_roots.append(Path(state.progress_monitor.log_dir))
                self._send_json(log_reader.read_text_log(
                    path,
                    launcher_log_dir=state.launcher_log_dir,
                    operator_log_path=state.operator_log_path,
                    max_bytes=max_bytes,
                    extra_roots=extra_roots,
                ))
            return
        if parsed.path == "/api/self-check":
            state = self.server.state
            with state.lock:
                _refresh_processes_and_log(state)
                self._send_json(run_self_check(state, include_progress_health=True))
            return
        self.send_error(HTTPStatus.NOT_FOUND, "not found")

    def do_POST(self) -> None:  # noqa: N802 - stdlib method name
        if self.path != "/api/action":
            self.send_error(HTTPStatus.NOT_FOUND, "not found")
            return
        try:
            length = int(self.headers.get("Content-Length", "0"))
            payload = json.loads(self.rfile.read(length).decode("utf-8") or "{}")
        except Exception as exc:
            self._send_json({"ok": False, "reason": f"invalid JSON: {exc}"}, status=400)
            return
        action = str(payload.get("action") or "")
        params = payload.get("params") if isinstance(payload.get("params"), dict) else {}
        session_id = str(payload.get("session_id") or uuid.uuid4())
        with self.server.state.lock:
            try:
                manager = self.server.state.observation_log
                obslog_user = payload.get("obslog_user")
                if manager is not None and obslog_user not in (None, ""):
                    manager.update_observer(obslog_user)
                ok, reason, data = dispatch_action(
                    self.server.state,
                    action,
                    params,
                    session_id,
                )
            except Exception as exc:
                ok, reason, data = False, str(exc), {"exception_type": type(exc).__name__}
            self.server.state.add_log(ok, reason, action=action, session_id=session_id, data=data)
            if isinstance(data, Mapping):
                _write_observation_log_for_action(
                    self.server.state,
                    action=action,
                    params=params,
                    ok=ok,
                    data=data,
                )
        response: JsonDict = {"ok": ok, "reason": reason, "data": data}
        if isinstance(data, Mapping):
            if data.get("progress_url") is not None:
                response["progress_url"] = data.get("progress_url")
            elif data.get("url") is not None and action == "launch_progress":
                response["progress_url"] = data.get("url")
        self._send_json(response)



def _run_shutdown_cleanup(state: OperatorConsoleState) -> JsonDict:
    """Release console-owned resources during process shutdown.

    This is local cleanup only.  It does not send telescope STOP/ABORT.  Active
    launcher children started by this console are terminated so they are not
    orphaned when the console process exits.
    """

    state.shutdown_requested = True
    state.cleanup_started_at = time.time()
    state.cleanup_finished_at = None
    state.cleanup_status = "running"
    summary: JsonDict = {
        "authority": {},
        "progress_monitor": {},
        "launchers": {},
    }

    ok, message, data = _release_console_authority(state, force=True)
    summary["authority"] = {"ok": ok, "message": message, "data": data}
    state.add_log(ok, f"console shutdown: {message}", action="authority_release", data=data)

    if state.shutdown_terminate_launchers:
        launcher_summary = state.process_registry.graceful_shutdown(
            timeout_sec=float(state.shutdown_launcher_timeout_sec),
            kill_timeout_sec=float(state.shutdown_launcher_kill_timeout_sec),
            reason="operator console shutdown",
        )
        summary["launchers"] = launcher_summary
        state.add_log(
            bool(launcher_summary.get("ok")),
            "console shutdown: " + str(launcher_summary.get("message")),
            action="launcher_shutdown_cleanup",
            data=launcher_summary,
        )
    else:
        remaining = [r.to_dict() for r in state.process_registry.active()]
        launcher_summary = {
            "ok": not bool(remaining),
            "message": "launcher shutdown cleanup disabled",
            "remaining": remaining,
            "counts": state.process_registry.counts(),
        }
        summary["launchers"] = launcher_summary
        state.add_log(
            bool(launcher_summary.get("ok")),
            "console shutdown: launcher cleanup disabled",
            action="launcher_shutdown_cleanup",
            data=launcher_summary,
        )

    if state.progress_monitor is not None:
        p_ok, p_message, p_data = state.progress_monitor.stop_if_owned()
        summary["progress_monitor"] = {
            "ok": p_ok,
            "message": p_message,
            "data": p_data,
        }
        state.add_log(
            p_ok,
            f"console shutdown: {p_message}",
            action="progress_monitor_stop",
            data=p_data,
        )
    else:
        summary["progress_monitor"] = {
            "ok": True,
            "message": "no progress monitor manager",
            "data": {},
        }

    state.cleanup_finished_at = time.time()
    state.cleanup_status = "finished"
    state.cleanup_summary = summary
    return summary

def run_server(
    *,
    host: str,
    port: int,
    telescope: str,
    progress_root: Path,
    progress_url: str,
    progress_host: str = "127.0.0.1",
    progress_port: int = 8091,
    progress_refresh_ms: int = 500,
    progress_no_ros: bool = False,
    progress_log_dir: Optional[os.PathLike[str] | str] = None,
    obs_roots: Optional[List[os.PathLike[str] | str]] = None,
    status_no_ros: bool = False,
    action_mode: str = "live",
    live_actions_enabled: bool = True,
    status_refresh_ms: int = 1000,
    quiet: bool = False,
    open_browser: bool = False,
    events_limit: int = 12,
    site_config_path: Optional[os.PathLike[str] | str] = None,
    operator_log_dir: Optional[os.PathLike[str] | str] = None,
    launcher_log_dir: Optional[os.PathLike[str] | str] = None,
    obslog_dir: Optional[os.PathLike[str] | str] = None,
    obslog_prefix: Optional[str] = None,
    obslog_user: Optional[str] = None,
    shutdown_terminate_launchers: bool = True,
    shutdown_launcher_timeout_sec: float = 3.0,
    shutdown_launcher_kill_timeout_sec: float = 1.0,
    safe_start: bool = False,
    reset_local_state: bool = False,
    rescue: bool = False,
    az_min: Optional[float] = None,
    az_max: Optional[float] = None,
    el_min: Optional[float] = None,
    el_max: Optional[float] = None,
    stop_event: Optional[threading.Event] = None,
) -> int:
    if action_mode not in SUPPORTED_ACTION_MODES:
        raise ValueError(f"action_mode must be one of {sorted(SUPPORTED_ACTION_MODES)}")
    site_summary = resolve_site_summary(
        site_config_path=site_config_path,
        az_min=az_min,
        az_max=az_max,
        el_min=el_min,
        el_max=el_max,
    )
    limits = dict(site_summary.mount_limits)
    resolved_operator_log_dir = (
        Path(operator_log_dir).expanduser()
        if operator_log_dir not in (None, "")
        else Path(os.environ.get("NECST_OPERATOR_LOG_DIR", Path.home() / ".necst" / "operator_console")).expanduser()
    )
    resolved_launcher_log_dir = (
        Path(launcher_log_dir)
        if launcher_log_dir not in (None, "")
        else resolved_operator_log_dir / "launcher_logs"
    )
    resolved_progress_log_dir = (
        Path(progress_log_dir)
        if progress_log_dir not in (None, "")
        else resolved_operator_log_dir / "progress_logs"
    )
    resolved_operator_log_dir.mkdir(parents=True, exist_ok=True)
    resolved_launcher_log_dir.mkdir(parents=True, exist_ok=True)
    resolved_progress_log_dir.mkdir(parents=True, exist_ok=True)
    operator_log_path = resolved_operator_log_dir / "operator_console.jsonl"
    rescue = bool(rescue)
    safe_start = bool(safe_start or rescue)
    reset_local_state = bool(reset_local_state or rescue)
    local_state_reset_summary: JsonDict = {}
    if reset_local_state:
        local_state_reset_summary = reset_local_console_state_files(
            progress_root=progress_root,
            operator_log_dir=resolved_operator_log_dir,
            reason="console startup --reset-local-state" + ("/--rescue" if rescue else ""),
        )
    resolved_obs_roots = resolve_obs_roots(obs_roots)
    observer_csv_log = observation_log.ObservationLogManager.create(
        configured_dir=obslog_dir,
        # Do not use --obs-root here: it is an obs-file browser location, not
        # necessarily the data/record root.  ObservationLogManager still uses
        # NECST_CONSOLE_PC_RECORD_ROOT, NECST_RECORD_ROOT, etc. for
        # <record root>/obslogs before falling back to ~/.necst.
        record_roots=None,
        prefix=obslog_prefix or os.environ.get("NECST_OBSLOG_PREFIX", observation_log.DEFAULT_PREFIX),
        observer=obslog_user or os.environ.get("NECST_OBSLOG_USER", observation_log.DEFAULT_OBSERVER),
        telescope=telescope,
    )
    progress_script = Path(__file__).resolve().parents[2] / "bin" / "progress.py"
    progress_monitor = progress_manager.ProgressMonitorManager(
        progress_script=progress_script,
        progress_root=Path(progress_root),
        host=str(progress_host),
        port=int(progress_port),
        url=str(progress_url),
        log_dir=resolved_progress_log_dir,
        refresh_ms=int(progress_refresh_ms),
        no_ros=bool(progress_no_ros),
        quiet=True,
    )
    state = OperatorConsoleState(
        telescope=telescope,
        progress_root=Path(progress_root),
        progress_url=progress_url,
        progress_monitor=progress_monitor,
        action_mode=action_mode,
        live_actions_enabled=bool(live_actions_enabled),
        status_refresh_ms=max(200, int(status_refresh_ms)),
        status_no_ros=bool(status_no_ros),
        quiet=quiet,
        events_limit=int(events_limit),
        site_summary=site_summary,
        mount_limits=limits,
        site_capabilities=dict(site_summary.capabilities),
        chopper_config=dict(site_summary.chopper),
        observation_log=observer_csv_log,
        operator_log_path=operator_log_path,
        launcher_log_dir=resolved_launcher_log_dir,
        obs_roots=list(resolved_obs_roots),
        shutdown_terminate_launchers=bool(shutdown_terminate_launchers),
        shutdown_launcher_timeout_sec=float(shutdown_launcher_timeout_sec),
        shutdown_launcher_kill_timeout_sec=float(shutdown_launcher_kill_timeout_sec),
        safe_start=bool(safe_start),
        rescue_mode=bool(rescue),
        local_state_reset_archive=str(local_state_reset_summary.get("archive_dir") or ""),
        local_state_reset_summary=dict(local_state_reset_summary),
    )
    state.live_cache = live_telemetry.LiveTelemetryCache(enabled=not bool(status_no_ros))
    if state.live_cache is not None and state.live_cache.available and not bool(safe_start):
        # Wait briefly for the first encoder/pointing sample so the console
        # starts with real Current Az/El whenever ROS status topics are alive.
        # This is read-only; it does not send any telescope command.
        try:
            state.live_cache.wait_for_initial_position(timeout_sec=2.0)
        except Exception as exc:
            state.live_cache.error = f"initial live telemetry wait failed: {exc}"
    state.add_log(True, f"console started in action_mode={action_mode}" + (" (safe/rescue mode)" if (safe_start or rescue) else ""))
    if safe_start or rescue:
        state.add_log(
            True,
            "console safe/rescue start active; no hardware command was sent automatically",
            action="console_safe_start",
            data={"safe_start": bool(safe_start), "rescue": bool(rescue)},
        )
    if local_state_reset_summary:
        state.add_log(
            bool(local_state_reset_summary.get("ok")),
            "local console state reset at startup; no hardware command sent",
            action="local_state_reset",
            data=local_state_reset_summary,
        )
        try:
            observer_csv_log.write_event(
                {},
                mode="Console",
                event="local_state_reset",
                action_or_obsfile=str(local_state_reset_summary.get("archive_dir") or ""),
                result="success" if local_state_reset_summary.get("ok") else "failed",
                comment="startup reset; no hardware command sent",
            )
        except Exception:
            pass
    state.add_log(
        bool(observer_csv_log.status().get("ok")),
        "observation CSV log configured",
        action="observation_csv_log",
        data=observer_csv_log.status(),
    )
    state.add_log(
        True,
        "operator log append file configured",
        action="operator_log",
        data={
            "operator_log_path": str(operator_log_path),
            "operator_log_dir": str(resolved_operator_log_dir),
            "launcher_log_dir": str(resolved_launcher_log_dir),
            "progress_log_dir": str(resolved_progress_log_dir),
            "append_only": True,
        },
    )
    state.add_log(
        bool(resolved_obs_roots),
        (
            "NECST-side file chooser locations configured: " + ", ".join(str(p) for p in resolved_obs_roots)
            if resolved_obs_roots
            else "NECST-side file chooser found no existing locations; check filesystem permissions or use --obs-root"
        ),
        action="necst_side_obs_browser",
        data={"obs_roots": [str(p) for p in resolved_obs_roots]},
    )
    if state.live_cache is not None:
        live_snapshot = state.live_cache.snapshot()
        if live_snapshot.get("available") and live_snapshot.get("has_position"):
            state.add_log(
                True,
                f"live telemetry enabled ({live_snapshot.get('spin_mode')}); position sample received",
                action="live_telemetry",
                data={"sample_counts": live_snapshot.get("sample_counts", {})},
            )
        elif live_snapshot.get("available"):
            state.add_log(
                False,
                f"live telemetry enabled ({live_snapshot.get('spin_mode')}) but no encoder/pointing position sample received yet",
                action="live_telemetry",
                data={"sample_counts": live_snapshot.get("sample_counts", {})},
            )
        else:
            state.add_log(False, f"live telemetry unavailable: {live_snapshot.get('error', 'disabled')}", action="live_telemetry")
    if action_mode == "live" and not live_actions_enabled:
        state.add_log(False, LIVE_ACTION_GUARD_MESSAGE, action="live_write_guard")
    if action_mode == "live" and live_actions_enabled:
        state.add_log(True, "live write actions enabled by default in live mode", action="live_write_guard")
    state.add_log(
        True,
        "site config: "
        f"source={site_summary.source}"
        + (f", path={site_summary.source_path}" if site_summary.source_path else "")
        + (f", observatory={site_summary.observatory}" if site_summary.observatory else ""),
    )
    for warning in site_summary.warnings:
        state.add_log(False, warning)
    if not limits:
        state.add_log(False, "site TOML mount limits are unavailable; mount move will be rejected")

    server = OperatorConsoleHTTPServer(
        (str(host), int(port)),
        OperatorConsoleHandler,
        state=state,
    )
    url = f"http://{host}:{port}/"
    print("NECST operator console")
    print(f"  URL: {url}")
    print(f"  progress root: {Path(progress_root)}")
    print(f"  progress URL: {progress_url}")
    print(f"  managed progress bind: {progress_host}:{int(progress_port)}")
    print(f"  action mode: {action_mode}")
    print(f"  live write actions enabled: {bool(live_actions_enabled)}")
    print(f"  safe start: {bool(safe_start)}")
    print(f"  rescue mode: {bool(rescue)}")
    if local_state_reset_summary:
        print(f"  local state reset archive: {local_state_reset_summary.get('archive_dir')}")
    if action_mode == "live" and not live_actions_enabled:
        print("  live write guard: ON by --guard-live-actions (write actions blocked; STOP/ABORT remain available)")
    print(f"  status refresh: {max(200, int(status_refresh_ms))} ms")
    print(f"  console live telemetry ROS: {'disabled' if status_no_ros else 'enabled'}")
    print(f"  operator log: {operator_log_path}")
    print(f"  observation CSV log: {observer_csv_log.status().get('csv_path')}")
    print(f"  observation CSV meta: {observer_csv_log.status().get('meta_path')}")
    print(f"  launcher logs: {resolved_launcher_log_dir}")
    print(f"  progress logs: {resolved_progress_log_dir}")
    print(
        "  shutdown cleanup: "
        f"terminate_launchers={bool(shutdown_terminate_launchers)}, "
        f"timeout={float(shutdown_launcher_timeout_sec)}s, "
        f"kill_timeout={float(shutdown_launcher_kill_timeout_sec)}s"
    )
    print(f"  site config source: {site_summary.source}" + (f" ({site_summary.source_path})" if site_summary.source_path else ""))
    print(f"  observatory: {site_summary.observatory or 'unknown'}")
    if limits:
        print(
            "  mount limits: "
            f"Az {limits.get('az_min')}..{limits.get('az_max')} deg, "
            f"El {limits.get('el_min')}..{limits.get('el_max')} deg"
        )
    else:
        print("  mount limits: unavailable")
    print(
        "  chopper: "
        f"available={site_summary.chopper.get('available')}, "
        f"IN={site_summary.chopper.get('insert_position')}, "
        f"OUT={site_summary.chopper.get('remove_position')}, "
        f"maintenance_supported={site_summary.chopper.get('maintenance_supported')}"
    )
    disabled = sorted(k for k, v in site_summary.capabilities.items() if not v)
    if disabled:
        print("  disabled capabilities: " + ", ".join(disabled))
    if open_browser:
        try:
            webbrowser.open(url)
        except Exception:
            pass

    thread = threading.Thread(target=server.serve_forever, name="necst-console", daemon=True)
    thread.start()
    event = stop_event or threading.Event()
    try:
        while not event.wait(0.2):
            pass
    finally:
        with state.lock:
            _run_shutdown_cleanup(state)
            # graceful_shutdown() updates ProcessRegistry records, but final
            # observation/calibration rows are appended by _refresh_processes_and_log().
            # Run it once more before closing the observer CSV log so launcher
            # exits during console shutdown are not silently lost.
            try:
                _refresh_processes_and_log(state)
            except Exception as exc:
                state.add_log(False, f"failed to refresh launcher exits during shutdown: {exc}", action="process_exit")
            if state.observation_log is not None:
                try:
                    state.observation_log.close(write_log_closed=True)
                except Exception as exc:
                    state.add_log(False, f"failed to close observation CSV log: {exc}", action="observation_csv_log")
            if state.live_cache is not None:
                try:
                    state.live_cache.close()
                except Exception as exc:
                    state.add_log(False, f"failed to close live telemetry cache: {exc}", action="live_telemetry")
        server.shutdown()
        server.server_close()
        thread.join(timeout=2.0)
    return 0
