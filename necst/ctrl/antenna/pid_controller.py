import time as pytime
from collections import deque
from copy import deepcopy
import threading
from typing import Deque, List, Optional

from neclib.controllers import PIDController
from neclib.safety import Decelerate
from neclib.utils import ParameterList
from necst_msgs.msg import CoordMsg, PIDMsg, TimedAzElFloat64

from ... import config, namespace, topic
from ...core import AlertHandlerNode


class AntennaPIDController(AlertHandlerNode):
    NodeName = "controller"
    Namespace = namespace.antenna

    def __init__(self) -> None:
        super().__init__(self.NodeName, namespace=self.Namespace)
        self.logger = self.get_logger()
        pid_param = config.antenna_pid_param
        max_speed = config.antenna_max_speed
        max_accel = config.antenna_max_acceleration

        self.controller = {
            "az": PIDController(
                pid_param=pid_param.az,
                max_speed=max_speed.az,
                max_acceleration=max_accel.az,
            ),
            "el": PIDController(
                pid_param=pid_param.el,
                max_speed=max_speed.el,
                max_acceleration=max_accel.el,
            ),
        }
        self.decelerate_calc = {
            "az": Decelerate(
                config.antenna_drive_critical_limit_az.map(lambda x: x.to_value("deg")),
                max_accel.az.to_value("deg/s^2"),
            ),
            "el": Decelerate(
                config.antenna_drive_critical_limit_el.map(lambda x: x.to_value("deg")),
                max_accel.el.to_value("deg/s^2"),
            ),
        }
        topic.altaz_cmd.subscription(self, self.update_command)
        topic.antenna_encoder.subscription(self, self.update_encoder_reading)
        topic.pid_param.subscription(self, self.change_pid_param)

        # Optional: reset command buffer / PID states on mode changes (epoch changes).
        if hasattr(topic, "antenna_control_status"):
            topic.antenna_control_status.subscription(self, self.update_control_status)

        self._control_id: Optional[str] = None
        self._epoch_reset_requested = False

        self.enc = ParameterList.new(5, CoordMsg)

        # ---------------------------------------------------------------------
        # Command buffer (thread-safe)
        #
        # - MultiThreadedExecutor can run callbacks concurrently.
        # - `update_command()` appends commands; `update_encoder_reading()` triggers
        #   `speed_command()` which consumes commands.
        # - Use deque for O(1) popleft.
        # - Avoid sorting on every append; only sort when out-of-order is detected.
        # - Cap buffer size to prevent unbounded growth if something goes wrong.
        # ---------------------------------------------------------------------
        self._cmd_lock = threading.Lock()
        self.command_list: Deque[CoordMsg] = deque()
        self._cmd_unsorted = False
        self._last_cmd_time: Optional[float] = None
        self._max_cmd_buffer = 2000  # ~40 s at 50 Hz (very conservative)

        self.command_publisher = topic.antenna_speed_cmd.publisher(self)

        self.gc = self.create_guard_condition(self.immediate_stop_no_resume)

    def update_command(self, msg: CoordMsg) -> None:
        """Receive an altaz command message and enqueue it.

        Notes
        -----
        - Most of the time `msg.time` is monotonically increasing, but mode switches
          or network jitter may occasionally reorder messages.
        - We avoid sorting on every callback; if out-of-order is detected, we mark
          the buffer "unsorted" and sort once on the next consumption.
        """
        t = getattr(msg, "time", None)
        if not isinstance(t, float):
            return

        with self._cmd_lock:
            # Dedup / monotonicity hint.
            if self._last_cmd_time is not None and t <= self._last_cmd_time:
                # Potential out-of-order (or duplicate). Keep it but mark unsorted.
                # Exact duplicates can be safely ignored.
                if abs(t - self._last_cmd_time) < 1e-12:
                    return
                self._cmd_unsorted = True

            self.command_list.append(msg)
            self._last_cmd_time = t

            # Hard cap to avoid unbounded growth.
            while len(self.command_list) > self._max_cmd_buffer:
                self.command_list.popleft()

    def update_encoder_reading(self, msg: CoordMsg) -> None:
        self.enc.push(msg)
        if all(isinstance(p.time, float) for p in self.enc):
            self.enc.sort(key=lambda x: x.time)
        self.speed_command()

    def update_control_status(self, msg) -> None:
        """Reset command buffer / PID states when control epoch changes.

        This is the "smart" fix for small unintended jerks at mode-switch boundaries:
        cut the history *only* when the generator/trajectory changes.
        """
        new_id = getattr(msg, "id", None)
        if not isinstance(new_id, str):
            return

        # First status sets baseline.
        if self._control_id is None:
            self._control_id = new_id
            return

        if new_id == self._control_id:
            return

        self._control_id = new_id

        # Drop queued commands from the previous epoch.
        with self._cmd_lock:
            self.command_list.clear()
            self._cmd_unsorted = False
            self._last_cmd_time = None

        # Request PID state reset at the next speed_command() tick.
        self._epoch_reset_requested = True


    def immediate_stop_no_resume(self) -> None:
        with self._cmd_lock:
            self.command_list.clear()
            self._cmd_unsorted = False
            self._last_cmd_time = None

        self.logger.warning("Immediate stop ordered.", throttle_duration_sec=5)
        enc = self.enc[-1]
        if any(not isinstance(p, float) for p in (enc.lon, enc.lat)):
            az_speed = el_speed = 0.0
        else:
            p = dict(k_i=0, k_d=0, k_c=0, accel_limit_off=-1)
            with self.controller["az"].params(**p), self.controller["el"].params(**p):
                _az_speed = self.controller["az"].get_speed(enc.lon, enc.lon, stop=True)
                _el_speed = self.controller["el"].get_speed(enc.lat, enc.lat, stop=True)
            az_speed = float(self.decelerate_calc["az"](enc.lon, _az_speed))
            el_speed = float(self.decelerate_calc["el"](enc.lat, _el_speed))
        msg = TimedAzElFloat64(az=az_speed, el=el_speed, time=pytime.time())
        self.command_publisher.publish(msg)

    def _sort_commands_locked(self) -> None:
        """Sort the internal command buffer by cmd.time (must hold _cmd_lock)."""
        if not self._cmd_unsorted:
            return
        if len(self.command_list) <= 1:
            self._cmd_unsorted = False
            return
        lst: List[CoordMsg] = list(self.command_list)
        lst.sort(key=lambda x: x.time)
        self.command_list = deque(lst)
        self._cmd_unsorted = False
        # Reset last time hint
        self._last_cmd_time = self.command_list[-1].time if self.command_list else None

    def _discard_outdated_commands_locked(self, now: float) -> None:
        """Drop commands that are already in the past (must hold _cmd_lock)."""
        # Keep at least one command to allow graceful stop handling.
        while len(self.command_list) > 1:
            if self.command_list[0].time < now:
                self.command_list.popleft()
            else:
                break

    def speed_command(self) -> None:
        if self.status.critical():
            self.logger.warning("Guard condition activated", throttle_duration_sec=1)
            self.gc.trigger()
            return

        now = pytime.time()
        dt = 1 / config.antenna_command_frequency

        # Fetch one command to execute (thread-safe) without holding the lock
        # during PID computations.
        cmd: Optional[CoordMsg] = None
        need_stop = False

        with self._cmd_lock:
            self._sort_commands_locked()
            self._discard_outdated_commands_locked(now)

            if len(self.command_list) == 0:
                need_stop = True
            else:
                # If the earliest command is still in the future (beyond immediate next tick), do nothing.
                if self.command_list[0].time > now + dt:
                    return

                if (len(self.command_list) == 1) and (self.command_list[0].time > now - dt):
                    cmd = deepcopy(self.command_list[0])
                    if now - cmd.time > dt:
                        cmd.time = now  # Not a real-time command.
                elif len(self.command_list) == 1:
                    cmd = self.command_list.popleft()
                    cmd.time = now
                else:
                    cmd = self.command_list.popleft()

        if need_stop:
            self.immediate_stop_no_resume()
            # initialize command
            self.controller["az"]._initialize()
            self.controller["el"]._initialize()
            return

        if cmd is None:
            return

        # Encoder: keep original behavior (oldest after sorting), to minimize side-effects.
        enc = self.enc[-1]

        if not isinstance(enc.time, float):
            return

        # If mode/trajectory epoch changed, reset PID internal states to current encoder
        # position to avoid a brief unintended jerk due to stale speed / integral history.
        if self._epoch_reset_requested:
            self._epoch_reset_requested = False
            if all(isinstance(p, float) for p in (enc.lon, enc.lat)):
                try:
                    self.controller["az"]._set_initial_parameters(
                        enc.lon,
                        enc.lon,
                        reset_cmd_speed=True,
                        cmd_time_seed=enc.time,
                        enc_time_seed=enc.time,
                    )
                    self.controller["el"]._set_initial_parameters(
                        enc.lat,
                        enc.lat,
                        reset_cmd_speed=True,
                        cmd_time_seed=enc.time,
                        enc_time_seed=enc.time,
                    )
                except TypeError:
                    # Backward compatible fallback (older PIDController signature).
                    self.controller["az"]._set_initial_parameters(enc.lon, enc.lon)
                    self.controller["el"]._set_initial_parameters(enc.lat, enc.lat)
            else:
                self.controller["az"]._initialize()
                self.controller["el"]._initialize()

        try:
            _az_speed = self.controller["az"].get_speed(
                cmd.lon, enc.lon, cmd_time=cmd.time, enc_time=enc.time
            )

            _el_speed = self.controller["el"].get_speed(
                cmd.lat, enc.lat, cmd_time=cmd.time, enc_time=enc.time
            )

            self.logger.debug(
                f"Az. Error={self.controller['az'].error[-1]:9.6f}deg "
                f"V_target={self.controller['az'].target_speed[-1]:9.6f}deg/s "
                f"Result={self.controller['az'].cmd_speed[-1]:9.6f}deg/s",
                throttle_duration_sec=0.5,
            )
            self.logger.debug(
                f"El. Error={self.controller['el'].error[-1]:9.6f}deg "
                f"V_target={self.controller['el'].target_speed[-1]:9.6f}deg/s "
                f"Result={self.controller['el'].cmd_speed[-1]:9.6f}deg/s",
                throttle_duration_sec=0.5,
            )

            az_speed = float(self.decelerate_calc["az"](enc.lon, _az_speed))
            el_speed = float(self.decelerate_calc["el"](enc.lat, _el_speed))

            cmd_time = enc.time
            msg = TimedAzElFloat64(az=az_speed, el=el_speed, time=cmd_time)
            self.command_publisher.publish(msg)

        except ZeroDivisionError:
            self.logger.debug("Duplicate command is supplied.")
        except ValueError:
            pass

    def change_pid_param(self, msg: PIDMsg) -> None:
        axis = msg.axis.lower()
        Kp, Ki, Kd = (getattr(self.controller[axis], k) for k in ("k_p", "k_i", "k_d"))
        self.logger.info(
            f"PID parameter for {axis=} has been changed from {(Kp, Ki, Kd) = } "
            f"to ({msg.k_p}, {msg.k_i}, {msg.k_d})"
        )
        self.controller[axis].k_p = msg.k_p
        self.controller[axis].k_i = msg.k_i
        self.controller[axis].k_d = msg.k_d
