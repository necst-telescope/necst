import time
from typing import List, Tuple

from neclib.controllers import PIDController
from neclib.safety import Decelerate
from necst_msgs.msg import CoordMsg, PIDMsg, TimedAzElFloat64

from ... import config, namespace, topic
from ...core import AlertHandlerNode


class AntennaPIDController(AlertHandlerNode):

    NodeName = "controller"
    Namespace = namespace.antenna

    def __init__(self, **kwargs):
        super().__init__(self.NodeName, namespace=self.Namespace, **kwargs)
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
        topic.altaz_cmd.subscription(self, self.update_command)
        topic.antenna_encoder.subscription(self, self.update_encoder_reading)
        self.publisher = topic.antenna_speed_cmd.publisher(self)
        self.create_timer(1 / config.antenna_command_frequency, self.calc_pid)
        topic.pid_param.subscription(self, self.change_pid_param)
        self.az_enc = self.el_enc = self.t_enc = None
        self.cmd_list: List[CoordMsg] = []

        self.decelerate_az = Decelerate(
            config.antenna_drive_critical_limit_az.map(lambda x: x.to_value("deg")),
            config.antenna_max_acceleration_az.to_value("deg/s^2"),
        )
        self.decelerate_el = Decelerate(
            config.antenna_drive_critical_limit_el.map(lambda x: x.to_value("deg")),
            config.antenna_max_acceleration_el.to_value("deg/s^2"),
        )

        self.gc = self.create_guard_condition(self._emergency_stop)

    def _emergency_stop(self) -> None:
        if any(p is None for p in [self.az_enc, self.el_enc]):
            _az_speed, _el_speed = 0, 0
        else:
            _az_speed = self.controller["az"].get_speed(self.az_enc, self.az_enc)
            _el_speed = self.controller["el"].get_speed(self.el_enc, self.el_enc)
        az_speed = float(self.decelerate_az(self.az_enc, _az_speed))
        el_speed = float(self.decelerate_el(self.el_enc, _el_speed))
        msg = TimedAzElFloat64(az=float(az_speed), el=float(el_speed), time=time.time())
        self.publisher.publish(msg)

    def get_valid_command(self) -> Tuple[float, float]:
        now = time.time()
        if (self.t_enc is not None) and (self.t_enc < now - 5):
            self.az_enc, self.el_enc = None, None

        self.cmd_list.sort(key=lambda msg: msg.time)
        if (len(self.cmd_list) > 1) and (
            self.cmd_list[0].time > now + config.antenna_command_frequency
        ):
            return None, None

        while len(self.cmd_list) > 1:
            msg = self.cmd_list.pop(0)
            if msg.time >= now:
                return msg.lon, msg.lat

        if len(self.cmd_list) == 1:
            # Avoid running out of commands, to prevent sporadic zero commands
            msg = self.cmd_list[0]
            if msg.time <= now - 1:  # For up to 1 second
                self.cmd_list.pop(0)
            return msg.lon, msg.lat
        return None, None

    def calc_pid(self) -> None:
        if self.status.critical():
            self.logger.warning("Guard condition activated", throttle_duration_sec=1)
            self.gc.trigger()
            return

        lon, lat = self.get_valid_command()
        if any(p is None for p in [self.az_enc, self.el_enc]):
            # Encoder reading isn't available at all, no calculation can be performed
            az_speed = 0.0
            el_speed = 0.0
        elif (self.t_enc < time.time() - 1) or any(p is None for p in [lon, lat]):
            # If ncoder reading is stale, or real-time command coordinate isn't
            # available, decelerate to 0 with `max_acceleration`.
            with self.controller["az"].params(
                k_i=0, k_d=0, accel_limit_off=-1
            ), self.controller["el"].params(k_i=0, k_d=0, accel_limit_off=-1):
                # Decay speed to zero
                az_speed = self.controller["az"].get_speed(self.az_enc, self.az_enc)
                el_speed = self.controller["el"].get_speed(self.el_enc, self.el_enc)
        else:
            _az_speed = self.controller["az"].get_speed(lon, self.az_enc)
            _el_speed = self.controller["el"].get_speed(lat, self.el_enc)
            az_speed = float(self.decelerate_az(self.az_enc, _az_speed))
            el_speed = float(self.decelerate_el(self.el_enc, _el_speed))
        msg = TimedAzElFloat64(az=az_speed, el=el_speed, time=time.time())
        self.publisher.publish(msg)

    def update_command(self, msg: CoordMsg) -> None:
        self.cmd_list.append(msg)

    def update_encoder_reading(self, msg: CoordMsg) -> None:
        self.az_enc = msg.lon
        self.el_enc = msg.lat
        self.t_enc = msg.time

    def change_pid_param(self, msg: PIDMsg) -> None:
        axis = msg.axis.lower()
        Kp, Ki, Kd = (getattr(self.controller[axis], k) for k in ("k_p", "k_i", "k_d"))
        self.logger.warning(
            f"PID parameter for {axis=} has been changed from {(Kp, Ki, Kd) = } "
            f"to ({msg.k_p}, {msg.k_i}, {msg.k_d})"
        )
        self.controller[axis].k_p = msg.k_p
        self.controller[axis].k_i = msg.k_i
        self.controller[axis].k_d = msg.k_d


def main(args=None):
    import rclpy

    rclpy.init(args=args)
    node = AntennaPIDController()
    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    finally:
        node.destroy_node()
        rclpy.try_shutdown()
