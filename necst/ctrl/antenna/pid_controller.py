import time
from typing import List, Tuple

from neclib.controllers import PIDController
from neclib.safety import Decelerate
from rclpy.node import Node

from ... import config, namespace, qos
from necst_msgs.msg import CoordMsg, PIDMsg, TimedAzElFloat64


class AntennaPIDController(Node):

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
        self.create_subscription(CoordMsg, "altaz", self.update_command, qos.realtime)
        self.create_subscription(
            CoordMsg, "encoder", self.update_encoder_reading, qos.realtime
        )
        self.publisher = self.create_publisher(TimedAzElFloat64, "speed", qos.realtime)
        self.create_timer(1 / config.antenna_command_frequency, self.calc_pid)
        self.create_subscription(
            PIDMsg, "pid_param", self.change_pid_param, qos.reliable
        )
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

    def get_valid_command(self) -> Tuple[float, float]:
        now = time.time()
        self.cmd_list.sort(key=lambda msg: msg.time)
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
        lon, lat = self.get_valid_command()
        if any(param is None for param in [self.az_enc, self.el_enc]):
            az_speed = 0.0
            el_speed = 0.0
        if any(param is None for param in [lon, lat]):
            original_Ki, original_Kd = {}, {}
            for axis in ["az", "el"]:
                self.controller[axis].max_acceleration /= 10
                original_Ki[axis] = self.controller[axis].k_i
                original_Kd[axis] = self.controller[axis].k_d
                self.controller[axis].k_i = 0
                self.controller[axis].k_d = 0
            # Decay speed to zero
            az_speed = self.controller["az"].get_speed(self.az_enc, self.az_enc)
            el_speed = self.controller["el"].get_speed(self.el_enc, self.el_enc)
            # Reset acceleration
            for axis in ["az", "el"]:
                self.controller[axis].max_acceleration *= 10
                self.controller[axis].k_i = original_Ki[axis]
                self.controller[axis].k_d = original_Kd[axis]
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
