"""Small operator-facing emergency and manual mount command CLIs."""

from __future__ import annotations

import argparse
import sys
import time
from typing import Optional

import rclpy

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
            parser.error("missing target: use 'necst mount-move AZ EL' or '--az AZ --el EL'")
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
        finally:
            return 130
    finally:
        try:
            com.quit_privilege()
        except Exception:
            pass
        com.destroy_node()
        _shutdown_if_needed(should_shutdown)
