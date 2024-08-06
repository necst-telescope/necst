import time as pytime
from copy import deepcopy
from typing import List

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

        self.enc = ParameterList.new(5, CoordMsg)
        self.command_list: List[CoordMsg] = []

        self.command_publisher = topic.antenna_speed_cmd.publisher(self)

        self.gc = self.create_guard_condition(self.immediate_stop_no_resume)

    def update_command(self, msg: CoordMsg) -> None:
        self.command_list.append(msg)
        self.command_list.sort(key=lambda x: x.time)

    def update_encoder_reading(self, msg: CoordMsg) -> None:
        self.enc.push(msg)
        if all(isinstance(p.time, float) for p in self.enc):
            self.enc.sort(key=lambda x: x.time)
        self.speed_command()

    def immediate_stop_no_resume(self) -> None:
        self.command_list.clear()

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

    def discard_outdated_commands(self) -> None:
        now = pytime.time()
        while len(self.command_list) > 1:
            if self.command_list[0].time < now:
                self.command_list.pop(0)
            else:
                break

    def speed_command(self) -> None:
        if self.status.critical():
            self.logger.warning("Guard condition activated", throttle_duration_sec=1)
            self.gc.trigger()
            return

        self.discard_outdated_commands()
        now = pytime.time()
        # Check if any command is available.
        if len(self.command_list) == 0:
            self.immediate_stop_no_resume()
            return

        # Check if command for immediate future exists or not.
        if self.command_list[0].time > now + 2 / config.antenna_command_frequency:
            return

        if (len(self.command_list) == 1) and (self.command_list[0].time > now - 1):
            cmd = deepcopy(self.command_list[0])
            if now - cmd.time > 1 / config.antenna_command_frequency:
                cmd.time = now  # Not a real-time command.
        elif len(self.command_list) == 1:
            cmd = self.command_list.pop(0)
            cmd.time = now
        else:
            cmd = self.command_list.pop(0)

        enc = self.enc[0]

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
            print(msg)

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
