"""Small operator-facing emergency and manual mount command CLIs."""

from __future__ import annotations

import argparse
import sys
import time
from typing import Optional

import rclpy

from .. import config
from .commander import Commander


def _ensure_rclpy() -> bool:
    """Initialize rclpy if needed and return whether this function did it."""
    if rclpy.ok():
        return False
    rclpy.init()
    return True


def _shutdown_if_needed(should_shutdown: bool) -> None:
    if should_shutdown:
        rclpy.shutdown()


def main_stop(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        prog="necst stop",
        description=(
            "Stop antenna drive immediately. This is an antenna stop, not an "
            "observation-cleanup abort."
        ),
    )
    parser.add_argument(
        "--confirm-timeout-sec",
        type=float,
        default=1.0,
        help=(
            "Compatibility name for the bounded stop-request duration in seconds "
            "(default: 1).  This command no longer reads speed telemetry; it only "
            "publishes the existing Commander manual_stop request path."
        ),
    )
    parser.add_argument(
        "--settle-sec",
        type=float,
        default=0.0,
        help="Extra seconds to keep the command process alive after stop (default: 0).",
    )
    args = parser.parse_args(argv)

    should_shutdown = _ensure_rclpy()
    com = Commander()
    try:
        com.antenna("stop", timeout_sec=max(0.0, float(args.confirm_timeout_sec)))
        if args.settle_sec > 0:
            time.sleep(float(args.settle_sec))
        return 0
    finally:
        com.destroy_node()
        _shutdown_if_needed(should_shutdown)


def main_abort(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        prog="necst abort",
        description=(
            "Request cooperative observation abort and, by default, stop antenna "
            "motion to make blocking point/scan waits return promptly."
        ),
    )
    parser.add_argument(
        "--reason",
        default="operator requested abort",
        help="Reason written to progress/abort telemetry.",
    )
    parser.add_argument(
        "--requester",
        default="necst abort",
        help="Requester name written to the abort request payload.",
    )
    parser.add_argument(
        "--no-antenna-stop",
        action="store_true",
        help="Only publish the abort request; do not also send antenna stop.",
    )
    parser.add_argument(
        "--repeat",
        type=int,
        default=3,
        help="Number of abort-request publications (default: 3).",
    )
    parser.add_argument(
        "--interval-sec",
        type=float,
        default=0.1,
        help="Interval between repeated abort-request publications (default: 0.1).",
    )
    parser.add_argument(
        "--settle-sec",
        type=float,
        default=0.2,
        help="Discovery/settle delay before first publication (default: 0.2).",
    )
    args = parser.parse_args(argv)

    should_shutdown = _ensure_rclpy()
    com = Commander()
    try:
        if args.settle_sec > 0:
            time.sleep(float(args.settle_sec))
        request_id = com.observation_abort(
            reason=args.reason,
            requester=args.requester,
            repeat=max(1, int(args.repeat)),
            interval_sec=max(0.0, float(args.interval_sec)),
        )
        print(f"abort request sent: {request_id}")
        if not args.no_antenna_stop:
            print("sending antenna stop...")
            com.antenna("stop")
        return 0
    finally:
        com.destroy_node()
        _shutdown_if_needed(should_shutdown)


def main_mount_move(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        prog="necst mount-move",
        description=(
            "Move to an explicit mount mechanical Az/El using direct raw AltAz "
            "pointing. Az is not wrapped: Az=360 means mount Az=360 deg."
        ),
    )
    parser.add_argument(
        "az_pos",
        nargs="?",
        type=float,
        metavar="AZ",
        help="Mount Az [deg]. Example: necst mount-move 360 50",
    )
    parser.add_argument(
        "el_pos",
        nargs="?",
        type=float,
        metavar="EL",
        help="Mount El [deg]. Example: necst mount-move 360 50",
    )
    parser.add_argument("--az", type=float, default=None, help="Mount Az [deg].")
    parser.add_argument("--el", type=float, default=None, help="Mount El [deg].")
    parser.add_argument(
        "--no-wait",
        action="store_true",
        help="Return immediately after sending the command. The antenna keeps moving.",
    )
    parser.add_argument(
        "--timeout-sec",
        type=float,
        default=None,
        help="Optional timeout while waiting for convergence.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the command that would be sent without moving the antenna.",
    )
    args = parser.parse_args(argv)

    positional_given = (args.az_pos is not None) or (args.el_pos is not None)
    options_given = (args.az is not None) or (args.el is not None)
    if positional_given and options_given:
        parser.error("use either positional AZ EL or --az/--el, not both")
    if positional_given:
        if args.az_pos is None or args.el_pos is None:
            parser.error("positional form requires exactly two values: AZ EL")
        az, el = float(args.az_pos), float(args.el_pos)
    else:
        if (args.az is None) != (args.el is None):
            parser.error("--az and --el must be specified together")
        if args.az is None or args.el is None:
            parser.error(
                "missing target: use 'necst mount-move AZ EL' or '--az AZ --el EL'"
            )
        az, el = float(args.az), float(args.el)

    print("Mount move command")
    print(f"  target Az = {az:.6f} deg")
    print(f"  target El = {el:.6f} deg")
    print("  frame     = altaz")
    print("  direct    = true")
    print("  az mode   = mount")
    if args.dry_run:
        return 0

    should_shutdown = _ensure_rclpy()
    com = Commander()
    try:
        if not com.get_privilege():
            print("failed to acquire NECST privilege", file=sys.stderr)
            return 2
        cmd_id = com.antenna(
            "point",
            target=(az, el, "altaz"),
            unit="deg",
            direct_mode=True,
            az_target_mode="mount",
            wait=False,
        )
        print(f"command id: {cmd_id}")
        if not args.no_wait:
            try:
                com.wait_mount_point(
                    az, el, command_id=cmd_id, timeout_sec=args.timeout_sec
                )
            except KeyboardInterrupt:
                print("interrupted: sending antenna stop...", file=sys.stderr)
                com.antenna("stop")
                return 130
        return 0
    except KeyboardInterrupt:
        print("interrupted: sending antenna stop...", file=sys.stderr)
        try:
            com.antenna("stop")
        except Exception:
            pass
        return 130
    finally:
        try:
            com.quit_privilege()
        except Exception:
            pass
        com.destroy_node()
        _shutdown_if_needed(should_shutdown)


def _chopper_config_positions() -> tuple[Optional[int], Optional[int]]:
    try:
        insert_position = int(config.chopper_motor_position["insert"])
        remove_position = int(config.chopper_motor_position["remove"])
        return insert_position, remove_position
    except Exception:
        return None, None


def _chopper_msg_position(msg) -> Optional[int]:
    try:
        return int(getattr(msg, "position"))
    except Exception:
        return None


def _chopper_msg_insert_flag(msg) -> Optional[bool]:
    try:
        value = getattr(msg, "insert")
    except Exception:
        return None
    return None if value is None else bool(value)


def _chopper_simulator_uses_boolean_only_status() -> bool:
    """Return True when the active chopper status is expected to be simulator-like.

    NECST's ChopperSimulator publishes only the boolean insert flag; the int32
    position field remains the ROS default value 0.  Real hardware must keep
    using motor positions, so this fallback is enabled only when the active site
    config explicitly says simulator=true.
    """
    try:
        return bool(getattr(config, "simulator", False))
    except Exception:
        return False


def _chopper_zero_is_boolean_only_simulator_for_target(
    target_position: int,
) -> bool:
    """Return True when position=0 may stand for a simulator-only boolean state.

    If the requested endpoint itself is 0, position=0 is a real endpoint and is
    already handled by the explicit position match.  The boolean-only fallback
    is therefore needed only for simulator targets whose configured endpoint is
    non-zero, e.g. NANTEN2 OUT/remove=250 while ChopperSimulator still publishes
    position=0, insert=False.
    """
    return _chopper_simulator_uses_boolean_only_status() and int(target_position) != 0


def _chopper_status_matches(msg, target_position: int, target_insert: bool) -> bool:
    position = _chopper_msg_position(msg)
    insert_flag = _chopper_msg_insert_flag(msg)
    target_position = int(target_position)

    # Trust an explicit motor step only when the optional boolean flag is not
    # contradicting it.  The real controller derives the flag from the motor
    # endpoint, so a conflicting flag/position pair is inconsistent telemetry
    # and must not be treated as completed.
    if position is not None and position == target_position:
        return insert_flag is None or insert_flag == bool(target_insert)

    # If the boolean flag is absent or contradicts the requested state, the
    # requested state has not been confirmed.
    if insert_flag is None or insert_flag != bool(target_insert):
        return False

    # At this point the boolean flag agrees with the requested state but the
    # motor position did not explicitly match the target.  Accept boolean-only
    # telemetry only when the position field is genuinely absent.  The ROS
    # simulator publishes an int32 default 0 even though it only models the
    # boolean state, so accept that special case only when the active NECST
    # configuration is explicitly in simulator mode.  In real hardware mode,
    # position=0 can be an actual counter value and must not be treated as
    # "unknown".
    if position is None:
        return True
    if (
        position == 0
        and _chopper_zero_is_boolean_only_simulator_for_target(target_position)
    ):
        return True
    return False


def _format_chopper_status(msg) -> tuple[str, str]:
    """Return operator-facing chopper status and detail string."""
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


def _wait_chopper_position(
    com: Commander, target_position: int, target_insert: bool, timeout_sec: float
) -> None:
    """Wait for chopper status to reach target position/flag with a timeout."""
    deadline = time.monotonic() + max(0.0, float(timeout_sec))
    last_state = "UNKNOWN"
    last_detail = "no status received"
    while time.monotonic() <= deadline:
        try:
            msg = com.get_message("chopper", timeout_sec=0.2)
            state, detail = _format_chopper_status(msg)
            last_state, last_detail = state, detail
            if _chopper_status_matches(msg, target_position, target_insert):
                return
        except Exception as exc:
            last_state, last_detail = "UNKNOWN", str(exc)
        time.sleep(0.1)
    raise TimeoutError(
        "chopper did not reach target position "
        f"{target_position} within {timeout_sec:.1f}s "
        f"(last: {last_state}, {last_detail})"
    )


def main_chopper(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        prog="necst chopper",
        description=(
            "Move or query the chopper.  'in' inserts the ambient load; "
            "'out' removes it.  'status' only reads telemetry."
        ),
    )
    parser.add_argument(
        "command",
        choices=[
            "in",
            "out",
            "status",
            "insert",
            "remove",
            "?",
            "alarm-reset",
            "alarm_reset",
            "reset-alarm",
            "home",
            "zero",
            "zero-point",
            "recover",
        ],
        help=(
            "Chopper command: in/out/status/alarm-reset/home/recover. "
            "insert/remove/? and zero/zero-point are aliases."
        ),
    )
    parser.add_argument(
        "--no-wait",
        action="store_true",
        help="Return after publishing the command without waiting for status.",
    )
    parser.add_argument(
        "--timeout-sec",
        type=float,
        default=10.0,
        help="Maximum time to wait for in/out completion (default: 10).",
    )
    parser.add_argument(
        "--settle-sec",
        type=float,
        default=0.2,
        help="ROS discovery settle delay before publishing/querying (default: 0.2).",
    )
    parser.add_argument(
        "--repeat",
        type=int,
        default=3,
        help="Number of command publications for in/out (default: 3).",
    )
    parser.add_argument(
        "--interval-sec",
        type=float,
        default=0.1,
        help="Interval between repeated command publications (default: 0.1).",
    )
    args = parser.parse_args(argv)

    command = str(args.command).lower()
    if command == "insert":
        command = "in"
    elif command == "remove":
        command = "out"
    elif command == "?":
        command = "status"
    elif command in {"alarm_reset", "reset-alarm"}:
        command = "alarm-reset"
    elif command in {"zero", "zero-point"}:
        command = "home"

    should_shutdown = _ensure_rclpy()
    com = Commander()
    try:
        if args.settle_sec > 0:
            time.sleep(float(args.settle_sec))

        if command == "status":
            try:
                msg = com.get_message(
                    "chopper", timeout_sec=max(0.0, float(args.timeout_sec))
                )
            except Exception as exc:
                print(f"failed to read chopper status: {exc}", file=sys.stderr)
                return 1
            state, detail = _format_chopper_status(msg)
            print(f"chopper {state}: {detail}")
            return 0

        if command in {"alarm-reset", "home", "recover"}:
            print(f"Chopper maintenance command: {command}")
            if not com.get_privilege():
                print("failed to acquire NECST privilege", file=sys.stderr)
                return 2
            try:
                result = com.chopper_maintenance(command)
            except Exception as exc:
                print(f"failed to execute chopper {command}: {exc}", file=sys.stderr)
                return 1
            status = "completed" if bool(getattr(result, "success", False)) else "failed"
            message = str(getattr(result, "message", "")).strip()
            print(f"chopper {command} {status}" + (f": {message}" if message else ""))
            return 0 if bool(getattr(result, "success", False)) else 1

        if command == "in":
            commander_cmd = "insert"
            label = "IN"
            target_insert = True
            target_position = int(config.chopper_motor_position["insert"])
        elif command == "out":
            commander_cmd = "remove"
            label = "OUT"
            target_insert = False
            target_position = int(config.chopper_motor_position["remove"])
        else:
            parser.error(f"unknown command: {args.command!r}")

        print(f"Chopper command: {label}")
        print(f"  target position = {target_position}")

        if not com.get_privilege():
            print("failed to acquire NECST privilege", file=sys.stderr)
            return 2

        repeat = max(1, int(args.repeat))
        interval = max(0.0, float(args.interval_sec))
        for i in range(repeat):
            com.chopper(commander_cmd, wait=False)
            if i + 1 < repeat:
                time.sleep(interval)

        if not args.no_wait:
            try:
                _wait_chopper_position(
                    com,
                    target_position,
                    target_insert,
                    timeout_sec=max(0.0, float(args.timeout_sec)),
                )
                msg = com.get_message("chopper", timeout_sec=0.5)
                state, detail = _format_chopper_status(msg)
                print(f"chopper {state}: {detail}")
            except KeyboardInterrupt:
                print("interrupted while waiting for chopper status", file=sys.stderr)
                return 130
        return 0
    except TimeoutError as exc:
        print(f"timeout: {exc}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        print("interrupted", file=sys.stderr)
        return 130
    finally:
        try:
            com.quit_privilege()
        except Exception:
            pass
        com.destroy_node()
        _shutdown_if_needed(should_shutdown)

