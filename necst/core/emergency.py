"""Small operator-facing emergency and manual mount command CLIs."""

from __future__ import annotations

import argparse
import sys
import time
from typing import Optional

import rclpy
from rclpy.node import Node
from necst_msgs.msg import AlertMsg, TimedAzElFloat64

from .. import config, namespace, topic
from .commander import Commander


def _ensure_rclpy() -> bool:
    """Initialize rclpy if needed and return whether this function did it.

    Emergency CLI commands must be able to publish a stop pulse after Ctrl-C.
    Some rclpy versions install SIGINT handlers by default and may shut down the
    context before our KeyboardInterrupt handler can publish the stop request.  If
    the local rclpy supports it, disable rclpy signal handlers for these CLIs and
    let Python raise KeyboardInterrupt in the normal way.
    """
    if rclpy.ok():
        return False
    try:
        from rclpy.signals import SignalHandlerOptions

        no_handlers = getattr(SignalHandlerOptions, "NO", None)
        if no_handlers is None:
            no_handlers = getattr(SignalHandlerOptions, "NO_INSTALL", None)
        if no_handlers is not None:
            rclpy.init(signal_handler_options=no_handlers)
        else:
            rclpy.init()
    except (ImportError, AttributeError, TypeError):
        rclpy.init()
    return True


def _shutdown_if_needed(should_shutdown: bool) -> None:
    if should_shutdown and rclpy.ok():
        rclpy.shutdown()


def _default_stop_pulse_sec() -> float:
    """Default duration for the manual-stop pulse.

    AlertHandlerNode discovers alert topics periodically.  Keep the manual_stop
    publisher alive long enough for a cold subscriber to discover the latched
    alert, while still guaranteeing that the operator CLI returns promptly.
    """
    try:
        scan_interval = float(config.ros_topic_scan_interval_sec)
    except Exception:
        scan_interval = 1.0
    return max(1.2, scan_interval + 0.25)


def _publish_antenna_stop_pulse(
    node: Node,
    *,
    pulse_sec: Optional[float] = None,
    interval_sec: float = 0.05,
    clear: bool = True,
    direct_zero: bool = True,
    settle_sec: float = 0.1,
) -> None:
    """Publish a non-blocking antenna stop pulse.

    This is intentionally not a speed-confirmation routine.  It performs two
    independent safety actions:

    1. publish manual_stop=True long enough for antenna control nodes to clear
       command queues and run their immediate-stop guards;
    2. publish direct zero speed commands to the motor-driver command topic, so
       Ctrl-C can stop motion even if controller-side speed telemetry is absent.

    By default the manual_stop alert is cleared before returning.  The stop
    command is therefore a pulse, not a latched operator state.
    """
    pulse = _default_stop_pulse_sec() if pulse_sec is None else max(0.0, float(pulse_sec))
    interval = max(0.01, float(interval_sec))

    stop_msg = AlertMsg(critical=True, warning=True, target=[namespace.antenna])
    clear_msg = AlertMsg(critical=False, warning=False, target=[namespace.antenna])
    alert_pub = topic.manual_stop_alert.publisher(node)
    speed_pub = topic.antenna_speed_cmd.publisher(node) if direct_zero else None

    if settle_sec > 0:
        # Give ROS discovery a brief chance after creating the emergency
        # publishers.  Sleeping before publisher creation does not help discovery.
        time.sleep(float(settle_sec))

    def publish_zero() -> None:
        if speed_pub is None:
            return
        speed_pub.publish(TimedAzElFloat64(az=0.0, el=0.0, time=time.time()))

    deadline = time.monotonic() + pulse
    first = True
    while first or (time.monotonic() < deadline):
        first = False
        alert_pub.publish(stop_msg)
        publish_zero()
        time.sleep(interval)

    if clear:
        # Repeat the clear and zero commands a few times so that a transiently busy
        # executor has multiple chances to observe the final state.  The final
        # state of the latched alert is non-critical, avoiding a manual_stop latch
        # that would block the next observation/move.
        for _ in range(3):
            publish_zero()
            alert_pub.publish(clear_msg)
            time.sleep(interval)


def _add_stop_pulse_arguments(parser: argparse.ArgumentParser, *, prefix: str = "") -> None:
    label = f"{prefix}-" if prefix else ""
    parser.add_argument(
        f"--{label}pulse-sec",
        type=float,
        default=None,
        help=(
            "Seconds to hold manual_stop=True before clearing it.  Default is "
            "max(1.2, ros_topic_scan_interval_sec + 0.25)."
        ),
    )
    parser.add_argument(
        f"--{label}interval-sec",
        type=float,
        default=0.05,
        help="Publication interval for stop pulse messages (default: 0.05).",
    )
    parser.add_argument(
        f"--{label}hold",
        action="store_true",
        help="Leave manual_stop asserted instead of clearing it before return.",
    )
    parser.add_argument(
        f"--{label}no-direct-zero",
        action="store_true",
        help="Do not also publish direct zero speed commands to the motor topic.",
    )


def main_stop(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        prog="necst stop",
        description=(
            "Stop antenna drive immediately. This is an antenna stop, not an "
            "observation-cleanup abort.  The default command sends a stop pulse "
            "and returns; it does not wait for speed telemetry."
        ),
    )
    # Backward-compatible accepted option.  It no longer drives the default stop
    # path, because reading speed telemetry is precisely what made `necst stop`
    # hang on the telescope.
    parser.add_argument(
        "--confirm-timeout-sec",
        type=float,
        default=None,
        help=(
            "Deprecated compatibility option.  The default stop path does not "
            "wait for speed telemetry.  Use --pulse-sec to adjust stop pulse length."
        ),
    )
    parser.add_argument(
        "--settle-sec",
        type=float,
        default=0.2,
        help="Discovery/settle delay before first publication (default: 0.2).",
    )
    _add_stop_pulse_arguments(parser)
    args = parser.parse_args(argv)

    should_shutdown = _ensure_rclpy()
    node = Node("emergency_stop_cli", namespace=namespace.core)
    try:
        _publish_antenna_stop_pulse(
            node,
            pulse_sec=args.pulse_sec,
            interval_sec=args.interval_sec,
            clear=not args.hold,
            direct_zero=not args.no_direct_zero,
            settle_sec=args.settle_sec,
        )
        return 0
    finally:
        node.destroy_node()
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
    _add_stop_pulse_arguments(parser, prefix="stop")
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
            print("sending antenna stop pulse...")
            _publish_antenna_stop_pulse(
                com,
                pulse_sec=args.stop_pulse_sec,
                interval_sec=args.stop_interval_sec,
                clear=not args.stop_hold,
                direct_zero=not args.stop_no_direct_zero,
                settle_sec=0.05,
            )
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
                com.wait_mount_point(az, el, timeout_sec=args.timeout_sec)
            except KeyboardInterrupt:
                print("interrupted: sending antenna stop pulse...", file=sys.stderr)
                _publish_antenna_stop_pulse(com, settle_sec=0.05)
                return 130
        return 0
    except KeyboardInterrupt:
        print("interrupted: sending antenna stop pulse...", file=sys.stderr)
        try:
            _publish_antenna_stop_pulse(com, settle_sec=0.05)
        finally:
            return 130
    finally:
        try:
            com.quit_privilege()
        except Exception:
            pass
        com.destroy_node()
        _shutdown_if_needed(should_shutdown)
