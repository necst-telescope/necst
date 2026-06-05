"""Shared operator action layer for NECST operator-facing tools.

This module intentionally contains command semantics, not GUI or CLI formatting.
Terminal commands and the future Operator Console should call these functions so
that STOP, ABORT, mount move, and chopper actions use the same validation and
Commander calls.
"""

from __future__ import annotations

import math
import os
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Literal, Optional, Sequence

from .. import config
from .commander import Commander
from ..ctrl.antenna.az_unwrap import assert_mount_az_allowed_when_unwrap_disabled
from . import observation_check


@dataclass(frozen=True)
class OperatorActionResult:
    """Result returned by an operator action.

    Parameters
    ----------
    action
        Stable action name, e.g. ``"antenna_stop"``.
    success
        True if the requested action completed or was accepted.
    message
        Operator-facing summary.
    data
        Machine-readable details for CLI/Web callers.
    """

    action: str
    success: bool
    message: str = ""
    data: Dict[str, Any] = field(default_factory=dict)


class OperatorActionError(RuntimeError):
    """Raised when an operator action is rejected before it can be sent."""


class OperatorActionTimeout(TimeoutError):
    """Raised when an operator action was sent but completion was not observed."""


class OperatorAuthoritySession:
    """Hold NECST privilege for a long-lived operator console session.

    This is intentionally small: it owns exactly one ``Commander`` instance and
    releases/destroys it explicitly.  It is useful for direct Commander actions
    such as mount move and chopper commands.  File-based observation launchers
    run in separate processes and therefore must not be started while this
    process is holding privilege for them.
    """

    def __init__(self, *, dry_run: bool = False) -> None:
        self.dry_run = bool(dry_run)
        self._commander: Optional[Commander] = None
        self._should_shutdown = False
        self._dry_run_held = False

    @property
    def commander(self) -> Commander:
        if self._commander is None:
            raise OperatorActionError("NECST privilege is not held by this session")
        return self._commander

    @property
    def held(self) -> bool:
        if self.dry_run:
            return bool(self._dry_run_held)
        if self._commander is None:
            return False
        try:
            return bool(self._commander.has_privilege)
        except Exception:
            return False

    @property
    def identity(self) -> Optional[str]:
        if self.dry_run:
            return "dry-run-console-authority" if self._dry_run_held else None
        try:
            return str(getattr(self._commander, "identity"))
        except Exception:
            return None

    def acquire(self) -> OperatorActionResult:
        if self.held:
            return OperatorActionResult(
                action="acquire_authority",
                success=True,
                message="NECST privilege is already held by this session",
                data={"held": True, "identity": self.identity, "dry_run": self.dry_run},
            )
        if self.dry_run:
            self._dry_run_held = True
            return OperatorActionResult(
                action="acquire_authority",
                success=True,
                message="dry run: NECST privilege was not requested",
                data={"held": True, "identity": self.identity, "dry_run": True},
            )

        self._should_shutdown = _ensure_rclpy()
        self._commander = Commander()
        try:
            _acquire_privilege(self._commander)
        except Exception:
            try:
                self._commander.destroy_node()
            finally:
                self._commander = None
                _shutdown_if_needed(self._should_shutdown)
                self._should_shutdown = False
            raise
        return OperatorActionResult(
            action="acquire_authority",
            success=True,
            message="NECST privilege acquired for this console session",
            data={"held": True, "identity": self.identity, "dry_run": False},
        )

    def release(self) -> OperatorActionResult:
        identity = self.identity
        if self.dry_run:
            was_held = self._dry_run_held
            self._dry_run_held = False
            return OperatorActionResult(
                action="release_authority",
                success=True,
                message=(
                    "dry run: NECST privilege was not held"
                    if not was_held
                    else "dry run: NECST privilege release was simulated"
                ),
                data={"held": False, "identity": identity, "dry_run": True},
            )

        if self._commander is None:
            return OperatorActionResult(
                action="release_authority",
                success=True,
                message="NECST privilege was already free",
                data={"held": False, "identity": identity, "dry_run": False},
            )
        try:
            _safe_quit_privilege(self._commander)
            return OperatorActionResult(
                action="release_authority",
                success=True,
                message="NECST privilege released",
                data={"held": False, "identity": identity, "dry_run": False},
            )
        finally:
            try:
                self._commander.destroy_node()
            finally:
                self._commander = None
                _shutdown_if_needed(self._should_shutdown)
                self._should_shutdown = False


def _ensure_rclpy() -> bool:
    """Initialize rclpy if needed and return whether this function did it."""
    import rclpy

    if rclpy.ok():
        return False
    rclpy.init()
    return True


def _shutdown_if_needed(should_shutdown: bool) -> None:
    if not should_shutdown:
        return
    import rclpy

    rclpy.shutdown()


def _finite_float(value: Any, *, name: str) -> float:
    try:
        number = float(value)
    except Exception as exc:
        raise OperatorActionError(f"{name} is not a finite number") from exc
    if not math.isfinite(number):
        raise OperatorActionError(f"{name} is not a finite number")
    return number


def _bool_value(value: Any, *, name: str = "boolean value") -> bool:
    if isinstance(value, bool):
        return value
    if value in (None, ""):
        return False
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "on"}:
        return True
    if text in {"0", "false", "no", "off"}:
        return False
    raise OperatorActionError(f"{name} must be true or false")


def _parse_sexagesimal_angle(value: Any, *, name: str, is_ra: bool) -> float:
    """Parse decimal degrees or sexagesimal angle text into degrees.

    RA accepts decimal degrees, HH:MM:SS.s, or 05h35m17.3s.  Dec accepts
    decimal degrees, +/-DD:MM:SS.s, or -05d23m28s.
    """

    text = str(value or "").strip()
    if not text:
        raise OperatorActionError(f"{name} is empty")

    lower = text.lower()
    sexagesimal = any(ch in lower for ch in ":hmsd") or len(lower.split()) > 1
    if not sexagesimal:
        number = _finite_float(text, name=name)
        return number

    normalized = lower
    for ch in "hmsd":
        normalized = normalized.replace(ch, ":")
    normalized = normalized.replace(" ", ":")
    while "::" in normalized:
        normalized = normalized.replace("::", ":")
    normalized = normalized.strip(":")
    parts = [p for p in normalized.split(":") if p != ""]
    if not 1 <= len(parts) <= 3:
        raise OperatorActionError(f"{name} must have 1 to 3 sexagesimal fields")

    sign = 1.0
    first = parts[0].strip()
    if first.startswith("+"):
        first = first[1:]
    elif first.startswith("-"):
        sign = -1.0
        first = first[1:]
    try:
        a0 = float(first)
        a1 = float(parts[1]) if len(parts) >= 2 else 0.0
        a2 = float(parts[2]) if len(parts) >= 3 else 0.0
    except Exception as exc:
        raise OperatorActionError(f"{name} contains a non-numeric sexagesimal field") from exc
    if not all(math.isfinite(x) for x in (a0, a1, a2)):
        raise OperatorActionError(f"{name} is not finite")
    if a1 < 0 or a1 >= 60 or a2 < 0 or a2 >= 60:
        raise OperatorActionError(f"{name} minutes/seconds must satisfy 0 <= value < 60")
    value_in_units = sign * (abs(a0) + a1 / 60.0 + a2 / 3600.0)
    return value_in_units * 15.0 if is_ra else value_in_units


def _validate_radec(ra_value: Any, dec_value: Any) -> tuple[float, float]:
    ra_deg = _parse_sexagesimal_angle(ra_value, name="RA", is_ra=True)
    dec_deg = _parse_sexagesimal_angle(dec_value, name="Dec", is_ra=False)
    if not (0.0 <= ra_deg < 360.0):
        raise OperatorActionError("RA must satisfy 0 <= value < 360 deg")
    if not (-90.0 <= dec_deg <= 90.0):
        raise OperatorActionError("Dec must satisfy -90 <= value <= 90 deg")
    return ra_deg, dec_deg


def _validate_galactic(l_value: Any, b_value: Any) -> tuple[float, float]:
    l_deg = _finite_float(l_value, name="Galactic l")
    b_deg = _finite_float(b_value, name="Galactic b")
    if not (0.0 <= l_deg < 360.0):
        raise OperatorActionError("Galactic l must satisfy 0 <= value < 360 deg")
    if not (-90.0 <= b_deg <= 90.0):
        raise OperatorActionError("Galactic b must satisfy -90 <= value <= 90 deg")
    return l_deg, b_deg


def _offset_arcsec_to_deg(value: Any, *, name: str) -> float:
    arcsec = _finite_float(value, name=name)
    if abs(arcsec) > 36000.0:
        raise OperatorActionError(f"{name} must be within +/-36000 arcsec")
    return arcsec / 3600.0


def _normalize_offset_frame(value: Any, *, target_frame: Optional[str]) -> str:
    frame = str(value or "target_frame").strip().lower().replace("-", "_")
    aliases = {
        "target": "target_frame",
        "targetframe": "target_frame",
        "target_frame": "target_frame",
        "altaz": "altaz",
        "azel": "altaz",
        "az_el": "altaz",
        "radec": "fk5",
        "ra_dec": "fk5",
        "fk5": "fk5",
        "galactic": "galactic",
    }
    if frame not in aliases:
        raise OperatorActionError(
            "offset_frame must be target_frame, altaz, radec/fk5, or galactic"
        )
    resolved = aliases[frame]
    if resolved == "target_frame":
        return target_frame or "altaz"
    return resolved


def _tracking_request_payload(
    kind: Any,
    *,
    name: Any = None,
    coord1: Any = None,
    coord2: Any = None,
    offset_frame: Any = "target_frame",
    offset_x_arcsec: Any = 0,
    offset_y_arcsec: Any = 0,
    cos_correction: Any = True,
    wait: bool = False,
    timeout_sec: Optional[float] = None,
    dry_run: bool = False,
    used_held_authority: bool = False,
) -> tuple[Dict[str, Any], Dict[str, Any]]:
    """Validate target-tracking inputs and return Commander kwargs + payload."""

    normalized = str(kind or "").strip().lower().replace("-", "_")
    if normalized not in {"sun", "moon", "name", "radec", "galactic"}:
        raise OperatorActionError(
            "target kind must be sun, moon, name, radec, or galactic"
        )

    target_frame: Optional[str] = None
    target_label = normalized
    commander_kwargs: Dict[str, Any] = {"unit": "deg", "wait": bool(wait)}
    if timeout_sec is not None:
        commander_kwargs["timeout_sec"] = max(0.0, _finite_float(timeout_sec, name="timeout_sec"))

    if normalized in {"sun", "moon"}:
        commander_kwargs["name"] = normalized
        target_label = normalized
    elif normalized == "name":
        target_name = str(name or "").strip()
        if not target_name:
            raise OperatorActionError("object name is empty")
        commander_kwargs["name"] = target_name
        target_label = target_name
    elif normalized == "radec":
        ra_deg, dec_deg = _validate_radec(coord1, coord2)
        target_frame = "fk5"
        commander_kwargs["target"] = (ra_deg, dec_deg, target_frame)
        target_label = f"RA={ra_deg:.9g} deg, Dec={dec_deg:.9g} deg"
    else:
        l_deg, b_deg = _validate_galactic(coord1, coord2)
        target_frame = "galactic"
        commander_kwargs["target"] = (l_deg, b_deg, target_frame)
        target_label = f"l={l_deg:.9g} deg, b={b_deg:.9g} deg"

    off_x_deg = _offset_arcsec_to_deg(offset_x_arcsec, name="offset X")
    off_y_deg = _offset_arcsec_to_deg(offset_y_arcsec, name="offset Y")
    resolved_offset_frame = _normalize_offset_frame(
        offset_frame, target_frame=target_frame
    )
    if off_x_deg != 0.0 or off_y_deg != 0.0:
        commander_kwargs["offset"] = (off_x_deg, off_y_deg, resolved_offset_frame)

    cos = _bool_value(cos_correction, name="cos_correction")
    commander_kwargs["cos_correction"] = cos
    commander_kwargs["az_target_mode"] = "sky"

    payload: Dict[str, Any] = {
        "kind": normalized,
        "target_label": target_label,
        "target_frame": target_frame,
        "offset_x_arcsec": off_x_deg * 3600.0,
        "offset_y_arcsec": off_y_deg * 3600.0,
        "offset_frame": resolved_offset_frame,
        "cos_correction": cos,
        "wait": bool(wait),
        "timeout_sec": commander_kwargs.get("timeout_sec"),
        "dry_run": bool(dry_run),
        "used_held_authority": bool(used_held_authority),
    }
    if "name" in commander_kwargs:
        payload["name"] = commander_kwargs["name"]
    if "target" in commander_kwargs:
        payload["target"] = commander_kwargs["target"]
    if "offset" in commander_kwargs:
        payload["offset"] = commander_kwargs["offset"]
    return commander_kwargs, payload


def _acquire_privilege(com: Commander) -> None:
    if not com.get_privilege():
        raise OperatorActionError("failed to acquire NECST privilege")


def _safe_quit_privilege(com: Commander) -> None:
    try:
        com.quit_privilege()
    except Exception:
        pass


def antenna_stop(
    *,
    confirm_timeout_sec: float = 1.0,
    settle_sec: float = 0.0,
    commander: Optional[Commander] = None,
) -> OperatorActionResult:
    """Send the bounded antenna STOP request.

    This is the safety STOP path.  It does not perform observation cleanup.
    """

    timeout = max(0.0, _finite_float(confirm_timeout_sec, name="confirm_timeout_sec"))
    settle = max(0.0, _finite_float(settle_sec, name="settle_sec"))

    owns_commander = commander is None
    should_shutdown = _ensure_rclpy() if owns_commander else False
    com = Commander() if owns_commander else commander
    assert com is not None
    try:
        com.antenna("stop", timeout_sec=timeout)
        if settle > 0:
            time.sleep(settle)
        return OperatorActionResult(
            action="antenna_stop",
            success=True,
            message="antenna stop request sent",
            data={
                "confirm_timeout_sec": timeout,
                "settle_sec": settle,
                "used_held_authority": not owns_commander,
            },
        )
    finally:
        if owns_commander:
            com.destroy_node()
            _shutdown_if_needed(should_shutdown)


def abort_observation(
    *,
    reason: str = "operator requested abort",
    requester: str = "necst abort",
    antenna_stop_after_abort: bool = True,
    repeat: int = 3,
    interval_sec: float = 0.1,
    settle_sec: float = 0.2,
    commander: Optional[Commander] = None,
) -> OperatorActionResult:
    """Request cooperative observation abort.

    This publishes the observation-abort request and, by default, also sends the
    bounded antenna STOP request so blocking point/scan waits return promptly.
    """

    repeat = max(1, int(repeat))
    interval = max(0.0, _finite_float(interval_sec, name="interval_sec"))
    settle = max(0.0, _finite_float(settle_sec, name="settle_sec"))

    owns_commander = commander is None
    should_shutdown = _ensure_rclpy() if owns_commander else False
    com = Commander() if owns_commander else commander
    assert com is not None
    try:
        if settle > 0:
            time.sleep(settle)
        request_id = com.observation_abort(
            reason=reason,
            requester=requester,
            repeat=repeat,
            interval_sec=interval,
        )
        if antenna_stop_after_abort:
            com.antenna("stop")
        return OperatorActionResult(
            action="abort_observation",
            success=True,
            message="abort request sent",
            data={
                "request_id": request_id,
                "reason": str(reason or "operator requested abort"),
                "requester": str(requester or "necst abort"),
                "antenna_stop_after_abort": bool(antenna_stop_after_abort),
                "repeat": repeat,
                "interval_sec": interval,
                "settle_sec": settle,
                "used_held_authority": not owns_commander,
            },
        )
    finally:
        if owns_commander:
            com.destroy_node()
            _shutdown_if_needed(should_shutdown)


def mount_move(
    az_deg: Any,
    el_deg: Any,
    *,
    wait: bool = True,
    timeout_sec: Optional[float] = None,
    dry_run: bool = False,
    commander: Optional[Commander] = None,
) -> OperatorActionResult:
    """Move to explicit mount mechanical Az/El.

    Definitions
    -----------
    Input Az/El are mount mechanical coordinates in degrees.  The command sent
    to Commander is always ``direct_mode=True`` and ``az_target_mode="mount"``.
    ``Az=360`` is therefore a mount angle, not automatically wrapped to zero.
    """

    az = _finite_float(az_deg, name="az_deg")
    el = _finite_float(el_deg, name="el_deg")
    try:
        unwrap_disabled_limit = assert_mount_az_allowed_when_unwrap_disabled(
            az, action_label="mount_move"
        )
    except ValueError as exc:
        raise OperatorActionError(str(exc)) from exc
    if timeout_sec is None:
        timeout = None
    else:
        timeout = max(0.0, _finite_float(timeout_sec, name="timeout_sec"))

    payload: Dict[str, Any] = {
        "az_deg": az,
        "el_deg": el,
        "frame": "altaz",
        "unit": "deg",
        "direct_mode": True,
        "az_target_mode": "mount",
        "wait": bool(wait),
        "timeout_sec": timeout,
        "dry_run": bool(dry_run),
        "used_held_authority": commander is not None,
    }
    if unwrap_disabled_limit is not None:
        payload["az_unwrap_disabled_raw_limit"] = unwrap_disabled_limit
    if dry_run:
        return OperatorActionResult(
            action="mount_move",
            success=True,
            message="dry run: mount move command was not sent",
            data=payload,
        )

    owns_commander = commander is None
    should_shutdown = _ensure_rclpy() if owns_commander else False
    com = Commander() if owns_commander else commander
    assert com is not None
    try:
        _acquire_privilege(com)
        cmd_id = com.antenna(
            "point",
            target=(az, el, "altaz"),
            unit="deg",
            direct_mode=True,
            az_target_mode="mount",
            wait=False,
        )
        payload["command_id"] = cmd_id
        if wait:
            com.wait_mount_point(az, el, command_id=cmd_id, timeout_sec=timeout)
        return OperatorActionResult(
            action="mount_move",
            success=True,
            message="mount move command sent",
            data=payload,
        )
    finally:
        if owns_commander:
            _safe_quit_privilege(com)
            com.destroy_node()
            _shutdown_if_needed(should_shutdown)


def _chopper_config_positions() -> tuple[Optional[int], Optional[int]]:
    try:
        insert_position = int(config.chopper_motor_position["insert"])
        remove_position = int(config.chopper_motor_position["remove"])
        return insert_position, remove_position
    except Exception:
        return None, None


def _chopper_msg_position(msg: Any) -> Optional[int]:
    try:
        return int(getattr(msg, "position"))
    except Exception:
        return None


def _chopper_msg_insert_flag(msg: Any) -> Optional[bool]:
    try:
        value = getattr(msg, "insert")
    except Exception:
        return None
    return None if value is None else bool(value)


def _chopper_simulator_uses_boolean_only_status() -> bool:
    """Return True when active chopper status is expected to be simulator-like."""
    try:
        return bool(getattr(config, "simulator", False))
    except Exception:
        return False


def _chopper_zero_is_boolean_only_simulator_for_target(target_position: int) -> bool:
    """Return True when position=0 may stand for simulator-only boolean state."""
    return _chopper_simulator_uses_boolean_only_status() and int(target_position) != 0


def chopper_status_matches(msg: Any, target_position: int, target_insert: bool) -> bool:
    """Return True if chopper telemetry confirms the requested endpoint."""
    position = _chopper_msg_position(msg)
    insert_flag = _chopper_msg_insert_flag(msg)
    target_position = int(target_position)

    if position is not None and position == target_position:
        return insert_flag is None or insert_flag == bool(target_insert)
    if insert_flag is None or insert_flag != bool(target_insert):
        return False
    if position is None:
        return True
    if position == 0 and _chopper_zero_is_boolean_only_simulator_for_target(
        target_position
    ):
        return True
    return False


def format_chopper_status(msg: Any) -> tuple[str, str]:
    """Return operator-facing chopper state and detail string."""
    position = _chopper_msg_position(msg)
    insert_position, remove_position = _chopper_config_positions()
    insert_flag = _chopper_msg_insert_flag(msg)
    simulator_boolean_only = _chopper_simulator_uses_boolean_only_status()

    if (
        position is not None
        and insert_position is not None
        and position == insert_position
        and insert_flag is not False
    ):
        state = "IN"
    elif (
        position is not None
        and remove_position is not None
        and position == remove_position
        and insert_flag is not True
    ):
        state = "OUT"
    elif simulator_boolean_only and position == 0 and insert_flag is True:
        state = "IN"
    elif simulator_boolean_only and position == 0 and insert_flag is False:
        state = "OUT"
    elif insert_flag is True:
        state = "IN?"
    elif insert_flag is False:
        state = "OUT?"
    else:
        state = "UNKNOWN"

    timestamp = getattr(msg, "time", None)
    try:
        age_sec = max(0.0, time.time() - float(timestamp))
        age_text = f", age={age_sec:.1f}s"
    except Exception:
        age_text = ""

    detail = f"position={position}, insert={insert_flag}{age_text}"
    return state, detail


def wait_chopper_position(
    com: Commander, target_position: int, target_insert: bool, timeout_sec: float
) -> Any:
    """Wait for chopper status to reach target position/flag with a timeout."""
    timeout = max(0.0, _finite_float(timeout_sec, name="timeout_sec"))
    deadline = time.monotonic() + timeout
    last_state = "UNKNOWN"
    last_detail = "no status received"
    while time.monotonic() <= deadline:
        try:
            msg = com.get_message("chopper", timeout_sec=0.2)
            state, detail = format_chopper_status(msg)
            last_state, last_detail = state, detail
            if chopper_status_matches(msg, target_position, target_insert):
                return msg
        except Exception as exc:
            last_state, last_detail = "UNKNOWN", str(exc)
        time.sleep(0.1)
    raise OperatorActionTimeout(
        "chopper did not reach target position "
        f"{target_position} within {timeout:.1f}s "
        f"(last: {last_state}, {last_detail})"
    )


def chopper_status(
    *,
    timeout_sec: float = 10.0,
    settle_sec: float = 0.2,
    commander: Optional[Commander] = None,
) -> OperatorActionResult:
    """Read chopper telemetry without sending a command."""
    timeout = max(0.0, _finite_float(timeout_sec, name="timeout_sec"))
    settle = max(0.0, _finite_float(settle_sec, name="settle_sec"))

    owns_commander = commander is None
    should_shutdown = _ensure_rclpy() if owns_commander else False
    com = Commander() if owns_commander else commander
    assert com is not None
    try:
        if settle > 0:
            time.sleep(settle)
        msg = com.get_message("chopper", timeout_sec=timeout)
        state, detail = format_chopper_status(msg)
        return OperatorActionResult(
            action="chopper_status",
            success=True,
            message=f"chopper {state}: {detail}",
            data={"state": state, "detail": detail, "used_held_authority": not owns_commander},
        )
    finally:
        if owns_commander:
            com.destroy_node()
            _shutdown_if_needed(should_shutdown)


def chopper_move(
    position: Literal["in", "out"],
    *,
    wait: bool = True,
    timeout_sec: float = 10.0,
    settle_sec: float = 0.2,
    repeat: int = 3,
    interval_sec: float = 0.1,
    commander: Optional[Commander] = None,
) -> OperatorActionResult:
    """Move the chopper IN or OUT through the shared Commander path."""
    command = str(position).strip().lower()
    if command not in {"in", "out"}:
        raise OperatorActionError("chopper position must be 'in' or 'out'")

    if command == "in":
        commander_cmd = "insert"
        label = "IN"
        target_insert = True
        target_position = int(config.chopper_motor_position["insert"])
    else:
        commander_cmd = "remove"
        label = "OUT"
        target_insert = False
        target_position = int(config.chopper_motor_position["remove"])

    timeout = max(0.0, _finite_float(timeout_sec, name="timeout_sec"))
    settle = max(0.0, _finite_float(settle_sec, name="settle_sec"))
    interval = max(0.0, _finite_float(interval_sec, name="interval_sec"))
    repeat = max(1, int(repeat))

    owns_commander = commander is None
    should_shutdown = _ensure_rclpy() if owns_commander else False
    com = Commander() if owns_commander else commander
    assert com is not None
    try:
        if settle > 0:
            time.sleep(settle)
        _acquire_privilege(com)
        for i in range(repeat):
            com.chopper(commander_cmd, wait=False)
            if i + 1 < repeat:
                time.sleep(interval)

        state = label
        detail = f"target_position={target_position}, wait={bool(wait)}"
        if wait:
            msg = wait_chopper_position(
                com,
                target_position,
                target_insert,
                timeout_sec=timeout,
            )
            state, detail = format_chopper_status(msg)
        return OperatorActionResult(
            action=f"chopper_{command}",
            success=True,
            message=f"chopper {state}: {detail}",
            data={
                "command": command,
                "commander_cmd": commander_cmd,
                "target_position": target_position,
                "target_insert": target_insert,
                "state": state,
                "detail": detail,
                "wait": bool(wait),
                "timeout_sec": timeout,
                "settle_sec": settle,
                "repeat": repeat,
                "interval_sec": interval,
                "used_held_authority": not owns_commander,
            },
        )
    finally:
        if owns_commander:
            _safe_quit_privilege(com)
            com.destroy_node()
            _shutdown_if_needed(should_shutdown)


def chopper_maintenance(
    command: Literal["alarm-reset", "home", "recover"],
    *,
    settle_sec: float = 0.2,
    commander: Optional[Commander] = None,
) -> OperatorActionResult:
    """Run a chopper maintenance service request."""
    normalized = str(command).strip().lower()
    aliases = {
        "alarm_reset": "alarm-reset",
        "reset-alarm": "alarm-reset",
        "zero": "home",
        "zero-point": "home",
    }
    normalized = aliases.get(normalized, normalized)
    if normalized not in {"alarm-reset", "home", "recover"}:
        raise OperatorActionError(
            "chopper maintenance command must be alarm-reset, home, or recover"
        )
    settle = max(0.0, _finite_float(settle_sec, name="settle_sec"))

    owns_commander = commander is None
    should_shutdown = _ensure_rclpy() if owns_commander else False
    com = Commander() if owns_commander else commander
    assert com is not None
    try:
        if settle > 0:
            time.sleep(settle)
        _acquire_privilege(com)
        result = com.chopper_maintenance(normalized)  # type: ignore[arg-type]
        success = bool(getattr(result, "success", False))
        message = str(getattr(result, "message", "")).strip()
        status = "completed" if success else "failed"
        return OperatorActionResult(
            action=f"chopper_{normalized.replace('-', '_')}",
            success=success,
            message=f"chopper {normalized} {status}" + (f": {message}" if message else ""),
            data={
                "command": normalized,
                "service_success": success,
                "service_message": message,
                "settle_sec": settle,
                "used_held_authority": not owns_commander,
            },
        )
    finally:
        if owns_commander:
            _safe_quit_privilege(com)
            com.destroy_node()
            _shutdown_if_needed(should_shutdown)


def start_target_tracking(
    kind: Any,
    *,
    name: Any = None,
    coord1: Any = None,
    coord2: Any = None,
    offset_frame: Any = "target_frame",
    offset_x_arcsec: Any = 0,
    offset_y_arcsec: Any = 0,
    cos_correction: Any = True,
    wait: bool = False,
    timeout_sec: Optional[float] = None,
    dry_run: bool = False,
    commander: Optional[Commander] = None,
) -> OperatorActionResult:
    """Start target tracking through the shared Commander path.

    Definitions
    -----------
    ``kind`` selects one of: ``sun``, ``moon``, ``name``, ``radec``,
    ``galactic``.  RA/Dec and Galactic coordinates are validated in degrees;
    RA also accepts sexagesimal hour notation.  Offsets are accepted in arcsec
    and converted to degrees before calling Commander.
    """

    commander_kwargs, payload = _tracking_request_payload(
        kind,
        name=name,
        coord1=coord1,
        coord2=coord2,
        offset_frame=offset_frame,
        offset_x_arcsec=offset_x_arcsec,
        offset_y_arcsec=offset_y_arcsec,
        cos_correction=cos_correction,
        wait=wait,
        timeout_sec=timeout_sec,
        dry_run=dry_run,
        used_held_authority=commander is not None,
    )
    if dry_run:
        return OperatorActionResult(
            action="start_target_tracking",
            success=True,
            message=(
                "dry run: target tracking command was not sent "
                f"({payload['target_label']})"
            ),
            data=payload,
        )

    owns_commander = commander is None
    should_shutdown = _ensure_rclpy() if owns_commander else False
    com = Commander() if owns_commander else commander
    assert com is not None
    try:
        _acquire_privilege(com)
        cmd_id = com.antenna("point", **commander_kwargs)
        payload["command_id"] = cmd_id
        return OperatorActionResult(
            action="start_target_tracking",
            success=True,
            message=f"target tracking command sent: {payload['target_label']}",
            data=payload,
        )
    finally:
        if owns_commander:
            _safe_quit_privilege(com)
            com.destroy_node()
            _shutdown_if_needed(should_shutdown)


def stop_tracking(
    *,
    confirm_timeout_sec: float = 1.0,
    settle_sec: float = 0.0,
    commander: Optional[Commander] = None,
) -> OperatorActionResult:
    """Stop target tracking by using the same bounded antenna STOP path."""

    result = antenna_stop(
        confirm_timeout_sec=confirm_timeout_sec,
        settle_sec=settle_sec,
        commander=commander,
    )
    return OperatorActionResult(
        action="stop_tracking",
        success=result.success,
        message=(
            "target tracking stop requested via antenna STOP"
            if result.success
            else result.message
        ),
        data=dict(result.data),
    )


_OBSERVATION_MODE_SCRIPTS: Dict[str, str] = {
    "otf": "otf.py",
    "psw": "psw_file.py",
    "grid": "grid.py",
    "radio_pointing": "radio_pointing.py",
    "radio-pointing": "radio_pointing.py",
    "radiopointing": "radio_pointing.py",
}


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _script_path(script_name: str) -> Path:
    path = _repo_root() / "bin" / script_name
    if not path.exists():
        raise OperatorActionError(f"NECST launcher script is missing: {path}")
    return path


def _python_env_for_source_tree() -> Dict[str, str]:
    env = os.environ.copy()
    root = str(_repo_root())
    current = env.get("PYTHONPATH")
    env["PYTHONPATH"] = root if not current else f"{root}{os.pathsep}{current}"
    return env


def _positive_int_or_none(value: Any, *, name: str) -> Optional[int]:
    if value in (None, ""):
        return None
    try:
        number = int(value)
    except Exception as exc:
        raise OperatorActionError(f"{name} must be an integer") from exc
    if number <= 0:
        raise OperatorActionError(f"{name} must be positive")
    return number


def _positive_float(value: Any, *, name: str) -> float:
    number = _finite_float(value, name=name)
    if number <= 0:
        raise OperatorActionError(f"{name} must be positive")
    return number


def _validate_obs_path(
    file_path: Any,
    *,
    check_exists: bool,
    name: str = "obs file",
) -> str:
    path = str(file_path or "").strip()
    if not path:
        raise OperatorActionError(f"{name} path/name is empty")
    if not path.lower().endswith((".obs", ".toml")):
        raise OperatorActionError(f"{name} extension must be .obs or .toml")
    if check_exists:
        p = Path(path)
        if not p.exists():
            raise OperatorActionError(f"{name} does not exist on this computer: {path}")
        if not p.is_file():
            raise OperatorActionError(f"{name} is not a regular file: {path}")
        if not os.access(p, os.R_OK):
            raise OperatorActionError(f"{name} is not readable: {path}")
    return path


def _run_launcher_command(
    action: str,
    argv: Sequence[str],
    *,
    background: bool,
    dry_run: bool,
    stdout_path: Any = None,
    stderr_path: Any = None,
) -> OperatorActionResult:
    command = [str(x) for x in argv]
    stdout_file = str(stdout_path) if stdout_path not in (None, "") else None
    stderr_file = str(stderr_path) if stderr_path not in (None, "") else None
    data: Dict[str, Any] = {
        "command": command,
        "background": bool(background),
        "dry_run": bool(dry_run),
        "cwd": str(_repo_root()),
        "stdout_path": stdout_file,
        "stderr_path": stderr_file,
    }
    if dry_run:
        return OperatorActionResult(
            action=action,
            success=True,
            message=f"dry run: {action} launcher was not started",
            data=data,
        )

    if background:
        stdout_handle = None
        stderr_handle = None
        try:
            if stdout_file is not None:
                Path(stdout_file).parent.mkdir(parents=True, exist_ok=True)
                stdout_handle = open(stdout_file, "ab", buffering=0)
            if stderr_file is not None:
                Path(stderr_file).parent.mkdir(parents=True, exist_ok=True)
                stderr_handle = open(stderr_file, "ab", buffering=0)
            proc = subprocess.Popen(
                command,
                cwd=str(_repo_root()),
                env=_python_env_for_source_tree(),
                stdout=stdout_handle if stdout_handle is not None else None,
                stderr=stderr_handle if stderr_handle is not None else None,
                start_new_session=True,
            )
        finally:
            if stdout_handle is not None:
                stdout_handle.close()
            if stderr_handle is not None:
                stderr_handle.close()
        data["pid"] = proc.pid
        # Keep the Popen object for the web console process registry.  The key
        # is private and must be removed before any JSON response is emitted.
        data["_popen"] = proc
        return OperatorActionResult(
            action=action,
            success=True,
            message=f"{action} launcher started in background (pid={proc.pid})",
            data=data,
        )

    run_kwargs: Dict[str, Any] = {}
    stdout_handle = None
    stderr_handle = None
    try:
        if stdout_file is not None:
            Path(stdout_file).parent.mkdir(parents=True, exist_ok=True)
            stdout_handle = open(stdout_file, "ab", buffering=0)
            run_kwargs["stdout"] = stdout_handle
        if stderr_file is not None:
            Path(stderr_file).parent.mkdir(parents=True, exist_ok=True)
            stderr_handle = open(stderr_file, "ab", buffering=0)
            run_kwargs["stderr"] = stderr_handle
        completed = subprocess.run(
            command,
            cwd=str(_repo_root()),
            env=_python_env_for_source_tree(),
            check=False,
            **run_kwargs,
        )
    finally:
        if stdout_handle is not None:
            stdout_handle.close()
        if stderr_handle is not None:
            stderr_handle.close()
    data["returncode"] = completed.returncode
    return OperatorActionResult(
        action=action,
        success=completed.returncode == 0,
        message=f"{action} launcher exited with return code {completed.returncode}",
        data=data,
    )


def check_obs_file(
    mode: Any,
    file_path: Any,
    *,
    channel: Any = None,
    require_exists: bool = True,
    use_neclib_parser: bool = True,
) -> OperatorActionResult:
    """Check an observation file without touching telescope hardware.

    This performs static validation, v16 default expansion, reference-file
    checks, and an optional neclib mode-specific parser pass.  It never moves
    the antenna/chopper, starts recorder/gate, applies SG settings, or queues
    an observation.
    """

    report = observation_check.check_observation_file(
        mode,
        file_path,
        channel=channel,
        require_exists=require_exists,
        use_neclib_parser=use_neclib_parser,
    )
    return OperatorActionResult(
        action="check_observation",
        success=report.ok,
        message=report.summary,
        data=report.to_dict(),
    )


def dry_run_observation(
    mode: Any,
    file_path: Any,
    *,
    channel: Any = None,
    require_exists: bool = True,
    use_neclib_parser: bool = True,
) -> OperatorActionResult:
    """Build a read-only observation execution-plan preview.

    The dry run is intentionally static: it returns the likely sequence, v16
    defaults, reference-file status, and rough timing information, but sends no
    mount/chopper/recording/gate/SG commands and does not queue observation
    execution.
    """

    report = observation_check.dry_run_observation_plan(
        mode,
        file_path,
        channel=channel,
        require_exists=require_exists,
        use_neclib_parser=use_neclib_parser,
    )
    return OperatorActionResult(
        action="dry_run_observation",
        success=report.ok,
        message=report.summary,
        data=report.to_dict(),
    )


def start_observation(
    mode: Any,
    file_path: Any,
    *,
    channel: Any = None,
    background: bool = True,
    dry_run: bool = False,
    check_exists: bool = True,
    stdout_path: Any = None,
    stderr_path: Any = None,
) -> OperatorActionResult:
    """Start a file-based observation through the existing NECST launcher.

    Inputs
    ------
    mode
        One of ``otf``, ``psw``, ``grid``, or ``radio_pointing``.
    file_path
        Path on the NECST computer.  In live mode this must exist and be readable.
    channel
        Optional positive integer spectral-channel override.
    background
        True starts a separate process and returns immediately.  This is the
        intended mode for the web Operator Console so the HTTP request is not
        held until the observation finishes.
    dry_run
        Validate and report the exact command without starting a process.
    check_exists
        File existence/readability validation.  Keep True for live start; tests
        and UI dry-runs may set False only when no command is sent.
    """

    normalized_mode = str(mode or "").strip().lower()
    if normalized_mode not in _OBSERVATION_MODE_SCRIPTS:
        raise OperatorActionError(
            "observation mode must be OTF, PSW, Grid, or Radio Pointing"
        )
    if not dry_run and not check_exists:
        raise OperatorActionError("live observation start requires obs file existence check")
    obs_path = _validate_obs_path(
        file_path,
        check_exists=bool(check_exists),
        name="obs file",
    )
    ch = _positive_int_or_none(channel, name="channel override")
    script = _script_path(_OBSERVATION_MODE_SCRIPTS[normalized_mode])
    argv = [sys.executable, str(script), "--file", obs_path]
    if ch is not None:
        argv.extend(["--channel", str(ch)])
    result = _run_launcher_command(
        "start_observation",
        argv,
        background=background,
        dry_run=dry_run,
        stdout_path=stdout_path,
        stderr_path=stderr_path,
    )
    result.data.update({"mode": normalized_mode, "file": obs_path, "channel": ch})
    return result


def run_rsky(
    *,
    n: Any = 1,
    integ: Any = 2,
    channel: Any = None,
    background: bool = True,
    dry_run: bool = False,
    stdout_path: Any = None,
    stderr_path: Any = None,
) -> OperatorActionResult:
    """Run an RSky calibration action through the existing launcher."""

    n_int = _positive_int_or_none(n, name="RSky n") or 1
    integ_sec = _positive_float(integ, name="RSky integ")
    ch = _positive_int_or_none(channel, name="RSky channel")
    script = _script_path("rsky.py")
    argv = [sys.executable, str(script), "-n", str(n_int), "--integ", f"{integ_sec:g}"]
    if ch is not None:
        argv.extend(["--channel", str(ch)])
    result = _run_launcher_command(
        "run_rsky",
        argv,
        background=background,
        dry_run=dry_run,
        stdout_path=stdout_path,
        stderr_path=stderr_path,
    )
    result.data.update({"n": n_int, "integ_sec": integ_sec, "channel": ch})
    return result


def _parse_tp_range(value: Any) -> list[int]:
    if value in (None, ""):
        return []
    if isinstance(value, str):
        tokens = value.replace(",", " ").split()
    else:
        try:
            tokens = list(value)
        except TypeError as exc:
            raise OperatorActionError("SkyDip tp_range must be a string or sequence") from exc
    try:
        out = [int(x) for x in tokens]
    except Exception as exc:
        raise OperatorActionError("SkyDip tp_range must contain integers") from exc
    if len(out) % 2 != 0:
        raise OperatorActionError("SkyDip tp_range must contain START END pairs")
    return out


def run_skydip(
    *,
    integ: Any = 2,
    channel: Any = None,
    tp_range: Any = None,
    background: bool = True,
    dry_run: bool = False,
    stdout_path: Any = None,
    stderr_path: Any = None,
) -> OperatorActionResult:
    """Run a SkyDip calibration action through the existing launcher."""

    integ_sec = _positive_float(integ, name="SkyDip integ")
    ch = _positive_int_or_none(channel, name="SkyDip channel")
    tp_values = _parse_tp_range(tp_range)
    script = _script_path("skydip.py")
    argv = [sys.executable, str(script), "--integ", f"{integ_sec:g}"]
    if ch is not None:
        argv.extend(["--channel", str(ch)])
    if tp_values:
        argv.append("--tp_range")
        argv.extend(str(x) for x in tp_values)
    result = _run_launcher_command(
        "run_skydip",
        argv,
        background=background,
        dry_run=dry_run,
        stdout_path=stdout_path,
        stderr_path=stderr_path,
    )
    result.data.update(
        {"integ_sec": integ_sec, "channel": ch, "tp_range": tp_values}
    )
    return result
