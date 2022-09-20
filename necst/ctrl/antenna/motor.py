import time

from neclib.devices import antenna_motor
from rclpy.node import Node

from necst_msgs.msg import TimedAzElFloat64
from ... import config, namespace, qos


class AntennaMotor(Node):

    NodeName = "motor_driver"
    Namespace = namespace.antenna

    def __init__(self) -> None:
        super().__init__(self.NodeName, namespace=self.Namespace)
        self.publisher = {
            "speed": self.create_publisher(
                TimedAzElFloat64, "actual_speed", qos.realtime
            ),
            "step": self.create_publisher(
                TimedAzElFloat64, "actual_step", qos.realtime
            ),
        }
        self.create_subscription(TimedAzElFloat64, "speed", self.command, qos.realtime)
        self.create_timer(1 / config.antenna_command_frequency, self.stream_speed)
        self.create_timer(1 / config.antenna_command_frequency, self.stream_step)

        self.motor = antenna_motor()

    def command(self, msg: TimedAzElFloat64) -> None:
        now = time.time()
        if msg.time < now:
            return
        while msg.time > time.time():
            time.sleep(1e-5)
        self.motor.set_speed(msg.az, "az")
        self.motor.set_speed(msg.el, "el")

    def stream_speed(self) -> None:
        readout_az = self.motor.get_speed("az")
        readout_el = self.motor.get_speed("el")
        speed_msg = TimedAzElFloat64(az=readout_az, el=readout_el, time=time.time())
        self.publisher["speed"].publish(speed_msg)

    def stream_step(self) -> None:
        readout_az = self.motor.get_step("az")
        readout_el = self.motor.get_step("el")
        step_msg = TimedAzElFloat64(az=readout_az, el=readout_el, time=time.time())
        self.publisher["step"].publish(step_msg)


def main(args=None):
    import rclpy

    rclpy.init(args=args)
    node = AntennaMotor()
    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    finally:
        node.destroy_node()
        rclpy.try_shutdown()


if __name__ == "__main__":
    main()
