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

from . import log_reader, process_manager, progress_manager, self_check, site_config, status_model, live_telemetry
from ..ctrl.antenna.az_unwrap import assert_mount_az_allowed_when_unwrap_disabled


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
    privilege through a persistent Commander.  Observation/RSky/SkyDip launchers
    run as separate processes, so the console must release actual privilege
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
    operator_log_path: Optional[Path] = None
    launcher_log_dir: Optional[Path] = None
    process_registry: process_manager.ProcessRegistry = field(
        default_factory=process_manager.ProcessRegistry
    )
    log: List[ConsoleLogEntry] = field(default_factory=list)
    authority: ConsoleAuthorityState = field(default_factory=ConsoleAuthorityState)
    authority_handle: Any = None
    last_command_az: Optional[float] = None
    last_command_el: Optional[float] = None
    last_manual_state: str = "idle"
    last_active_task: str = "none"
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
        return True, "temporary NECST privilege will be acquired by the action layer"
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
    OperatorAuthoritySession still reports that it holds privilege.  If the
    authorizer restarted, the node lost privilege, or a reduced/stub
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
        note = "authority handle no longer reports NECST privilege; cleared browser gate"
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
    """Release browser/session authority and any persistent NECST privilege."""

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
    """Release persistent NECST privilege before starting a child launcher."""

    if state.action_mode == "dry-run":
        return None
    if not state.authority.held or state.authority.session_id != session_id:
        return None
    if state.authority_handle is None:
        return None
    ok, message, _data = _release_console_authority(state, session_id=session_id)
    if not ok:
        raise RuntimeError(message)
    return "console NECST privilege was released before starting the launcher process"


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
        if record.category == "observation" and not state.process_registry.active(category="observation"):
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
    """Terminate local launcher subprocesses started by this console only."""

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
        return False, f"no active launcher matched {selector}; no local process was terminated", {
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
        "note": "local launcher termination only; telescope STOP/ABORT is a separate action",
    }
    labels = ", ".join(f"{r.label}(pid={r.pid})" for r in records)
    return True, f"requested {signal_name} for local launcher(s): {labels}", data


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
            state.last_command_az = az
            state.last_command_el = el
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
            state.last_manual_state = "stopped"
            state.last_active_task = "none"
            state.last_command_az = None
            state.last_command_el = None
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
            state.last_command_az = None
            state.last_command_el = None
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
        return ok, message, data

    if action == "start_observation":
        validate_site_capability(state, "observation_start", action_label="observation start")
        mode, path, channel = validate_observation_selection(params)
        active_reason = _active_launcher_summary(state, category="observation")
        if active_reason is not None:
            return False, active_reason, {"action": action, "category": "observation"}
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
        if ok and not dry_run:
            state.last_command_az = None
            state.last_command_el = None
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
        active_reason = _active_launcher_summary(state, category="calibration")
        if active_reason is not None:
            return False, active_reason, {"action": action, "category": "calibration"}
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
            state.last_command_az = None
            state.last_command_el = None
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
        active_reason = _active_launcher_summary(state, category="calibration")
        if active_reason is not None:
            return False, active_reason, {"action": action, "category": "calibration"}
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
            state.last_command_az = None
            state.last_command_el = None
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
        if bool(record.is_active())
    }
    if sys_state == "idle":
        if "observation" in active_process_categories:
            sys_state = "observing"
        elif "calibration" in active_process_categories:
            sys_state = "calibrating"

    raw_active_task = str(motion.get("active_task") or "").strip()
    if raw_active_task.lower() in {"", "idle", "none", "unknown"}:
        active_task = str(state.last_active_task or "none")
    else:
        active_task = raw_active_task
    if active_task.lower() in {"", "idle", "unknown"}:
        active_task = "none"

    raw_manual_state = str(motion.get("stage") or "").strip()
    if raw_manual_state.lower() in {"", "idle", "none", "unknown"}:
        manual_state = str(state.last_manual_state or "idle")
    else:
        manual_state = raw_manual_state
    if manual_state.lower() in {"", "none", "unknown"}:
        manual_state = "idle"

    current_az = antenna.get("current_az_deg")
    current_el = antenna.get("current_el_deg")
    try:
        az = float(current_az) if current_az is not None else None
        el = float(current_el) if current_el is not None else None
    except Exception:
        az, el = None, None
    if az is None or el is None:
        warnings = list(warnings) + ["antenna current Az/El is unavailable"]

    cmd_az = antenna.get("command_az_deg")
    cmd_el = antenna.get("command_el_deg")
    if cmd_az is None:
        cmd_az = state.last_command_az
    if cmd_el is None:
        cmd_el = state.last_command_el

    chopper_state = str(chopper.get("state") or state.last_chopper_state or "unknown")
    chopper_position = chopper.get("position")
    if chopper_position is None:
        chopper_position = state.last_chopper_position
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
        "az": az,
        "el": el,
        "command_az": cmd_az,
        "command_el": cmd_el,
        "chopper": {
            "state": chopper_state,
            "position": chopper_position if chopper_position is not None else "unknown",
            "age": age_text,
        },
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
        "launcher_log_choices": log_reader.launcher_log_choices([
            r.to_dict() for r in state.process_registry.all_records(refresh=False)
        ]),
        "shutdown": _shutdown_snapshot(state),
        "self_check_endpoint": "/api/self-check",
        "operator_log_path": str(state.operator_log_path) if state.operator_log_path is not None else None,
        "launcher_log_dir": str(state.launcher_log_dir) if state.launcher_log_dir is not None else None,
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


def build_console_status(state: OperatorConsoleState) -> JsonDict:
    with state.lock:
        _refresh_processes_and_log(state)
        _sync_authority_state(state)
        live_payload = state.live_cache.snapshot() if state.live_cache is not None else None
        status_state = status_model.build_progress_status_state(
            state.progress_root,
            events_limit=state.events_limit,
            live_payload=live_payload,
        )
        return _operator_status_to_v7_status(state, status_state)


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
        if self.path == "/api/operator-status":
            state = self.server.state
            live_payload = state.live_cache.snapshot() if state.live_cache is not None else None
            status_state = status_model.build_progress_status_state(
                state.progress_root,
                events_limit=state.events_limit,
                live_payload=live_payload,
            )
            self._send_json(status_state.get("operator_status", {}))
            return
        parsed = urllib.parse.urlparse(self.path)
        query = urllib.parse.parse_qs(parsed.query)
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
                ok, reason, data = dispatch_action(
                    self.server.state,
                    action,
                    params,
                    session_id,
                )
            except Exception as exc:
                ok, reason, data = False, str(exc), {"exception_type": type(exc).__name__}
            self.server.state.add_log(ok, reason, action=action, session_id=session_id, data=data)
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
    shutdown_terminate_launchers: bool = True,
    shutdown_launcher_timeout_sec: float = 3.0,
    shutdown_launcher_kill_timeout_sec: float = 1.0,
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
        operator_log_path=operator_log_path,
        launcher_log_dir=resolved_launcher_log_dir,
        shutdown_terminate_launchers=bool(shutdown_terminate_launchers),
        shutdown_launcher_timeout_sec=float(shutdown_launcher_timeout_sec),
        shutdown_launcher_kill_timeout_sec=float(shutdown_launcher_kill_timeout_sec),
    )
    state.live_cache = live_telemetry.LiveTelemetryCache(enabled=not bool(status_no_ros))
    state.add_log(True, f"console started in action_mode={action_mode}")
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
    if state.live_cache is not None:
        live_snapshot = state.live_cache.snapshot()
        if live_snapshot.get("available"):
            state.add_log(True, f"live telemetry enabled ({live_snapshot.get('spin_mode')})", action="live_telemetry")
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
    if action_mode == "live" and not live_actions_enabled:
        print("  live write guard: ON by --guard-live-actions (write actions blocked; STOP/ABORT remain available)")
    print(f"  status refresh: {max(200, int(status_refresh_ms))} ms")
    print(f"  console live telemetry ROS: {'disabled' if status_no_ros else 'enabled'}")
    print(f"  operator log: {operator_log_path}")
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
            if state.live_cache is not None:
                try:
                    state.live_cache.close()
                except Exception as exc:
                    state.add_log(False, f"failed to close live telemetry cache: {exc}", action="live_telemetry")
        server.shutdown()
        server.server_close()
        thread.join(timeout=2.0)
    return 0
