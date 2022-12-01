import time

from neclib.devices import AntennaMotor as AntennaMotorDevice
from necst_msgs.msg import TimedAzElFloat64, TimedAzElInt64

from ... import config, namespace, topic
from ...core import DeviceNode


class AntennaMotor(DeviceNode):

    NodeName = "motor_driver"
    Namespace = namespace.antenna

    def __init__(self) -> None:
        super().__init__(self.NodeName, namespace=self.Namespace)
        self.publisher = {
            "speed": topic.antenna_motor_speed.publisher(self),
            "step": topic.antenna_motor_step.publisher(self),
        }
        topic.antenna_speed_cmd.subscription(self, self.speed_command)
        self.create_timer(1 / config.antenna_command_frequency, self.stream_speed)
        self.create_timer(1 / config.antenna_command_frequency, self.stream_step)

        self.motor = AntennaMotorDevice()

    def speed_command(self, msg: TimedAzElFloat64) -> None:
        now = time.time()
        if msg.time < now - 0.05:
            return
        while msg.time > time.time():
            time.sleep(1e-5)
        self.motor.set_speed(msg.az, "az")
        self.motor.set_speed(msg.el, "el")

    def stream_speed(self) -> None:
        readout_az = self.motor.get_speed("az").to_value("deg/s").item()
        readout_el = self.motor.get_speed("el").to_value("deg/s").item()
        speed_msg = TimedAzElFloat64(
            az=float(readout_az), el=float(readout_el), time=time.time()
        )
        self.publisher["speed"].publish(speed_msg)

    def stream_step(self) -> None:
        readout_az = self.motor.get_step("az")
        readout_el = self.motor.get_step("el")
        step_msg = TimedAzElInt64(az=readout_az, el=readout_el, time=time.time())
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
        node.motor.io.output_do([0, 0, 0, 0])
        node.destroy_node()
        rclpy.try_shutdown()


if __name__ == "__main__":
    main()
