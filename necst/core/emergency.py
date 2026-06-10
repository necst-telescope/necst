"""Small operator-facing emergency and manual command CLIs."""

from __future__ import annotations

import argparse
import sys
from typing import Optional

from .. import config
from .operator_actions import (
    OperatorActionError,
    OperatorActionTimeout,
    abort_observation,
    antenna_stop,
    chopper_maintenance,
    chopper_move,
    chopper_status,
    format_chopper_status as _format_chopper_status,
    mount_move,
    wait_chopper_position as _wait_chopper_position,
)


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

    try:
        antenna_stop(
            confirm_timeout_sec=args.confirm_timeout_sec,
            settle_sec=args.settle_sec,
        )
        return 0
    except OperatorActionError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        print("interrupted", file=sys.stderr)
        return 130


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

    try:
        result = abort_observation(
            reason=args.reason,
            requester=args.requester,
            antenna_stop_after_abort=not args.no_antenna_stop,
            repeat=args.repeat,
            interval_sec=args.interval_sec,
            settle_sec=args.settle_sec,
        )
        print(f"abort request sent: {result.data.get('request_id')}")
        if not args.no_antenna_stop:
            print("sending antenna stop...")
        return 0
    except OperatorActionError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        print("interrupted", file=sys.stderr)
        return 130


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

    try:
        result = mount_move(
            az,
            el,
            wait=not args.no_wait,
            timeout_sec=args.timeout_sec,
            dry_run=args.dry_run,
        )
        if not args.dry_run:
            print(f"command id: {result.data.get('command_id')}")
        return 0
    except KeyboardInterrupt:
        print("interrupted: sending antenna stop...", file=sys.stderr)
        try:
            antenna_stop()
        except Exception:
            pass
        return 130
    except OperatorActionError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    except Exception as exc:
        print(str(exc), file=sys.stderr)
        return 1


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

    try:
        if command == "status":
            result = chopper_status(
                timeout_sec=args.timeout_sec,
                settle_sec=args.settle_sec,
            )
            print(result.message)
            return 0

        if command in {"alarm-reset", "home", "recover"}:
            print(f"Chopper maintenance command: {command}")
            result = chopper_maintenance(command, settle_sec=args.settle_sec)
            print(result.message)
            return 0 if result.success else 1

        if command == "in":
            label = "IN"
            target_position = int(config.chopper_motor_position["insert"])
        elif command == "out":
            label = "OUT"
            target_position = int(config.chopper_motor_position["remove"])
        else:
            parser.error(f"unknown command: {args.command!r}")

        print(f"Chopper command: {label}")
        print(f"  target position = {target_position}")

        result = chopper_move(
            command,
            wait=not args.no_wait,
            timeout_sec=args.timeout_sec,
            settle_sec=args.settle_sec,
            repeat=args.repeat,
            interval_sec=args.interval_sec,
        )
        if not args.no_wait:
            print(result.message)
        return 0
    except OperatorActionTimeout as exc:
        print(f"timeout: {exc}", file=sys.stderr)
        return 1
    except OperatorActionError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        print("interrupted", file=sys.stderr)
        return 130
    except Exception as exc:
        print(str(exc), file=sys.stderr)
        return 1
