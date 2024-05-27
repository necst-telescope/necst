import time

from neclib.devices import AntennaMotor as AntennaMotorDevice
from necst_msgs.msg import TimedAzElFloat64, TimedAzElInt64, Spectral

from ... import config, namespace, topic
from ...core import DeviceNode


class AntennaMotor(DeviceNode):

    NodeName = "motor_driver"
    Namespace = namespace.antenna

    def __init__(self) -> None:
        super().__init__(self.NodeName, namespace=self.Namespace)
        self.logger = self.get_logger()

        self.publisher = {
            "speed": topic.antenna_motor_speed.publisher(self),
            "step": topic.antenna_motor_step.publisher(self),
        }
        topic.antenna_speed_cmd.subscription(self, self.speed_command)
        self.create_timer(1 / 10, self.stream_speed)
        self.create_timer(1 / 10, self.stream_step)
        self.create_timer(5, self.check_command)

        self.last_cmd_time = time.time()

        self.motor = AntennaMotorDevice()

    def check_command(self) -> None:
        timelimit = config.antenna_command_offset_sec
        if time.time() - self.last_cmd_time > timelimit:
            self.motor.set_speed(0, "az")
            self.motor.set_speed(0, "el")
            self.logger.warning(
                f"No command supplied for {timelimit} s, stopping the antenna",
                throttle_duration_sec=10,
            )

    def speed_command(self, msg: TimedAzElFloat64) -> None:
        # now = time.time()
        # if msg.time < now - 0.05:
        #     return
        # while msg.time > time.time():
        #     time.sleep(1e-5)
        self.motor.set_speed(msg.az, "az")
        self.motor.set_speed(msg.el, "el")

        self.last_cmd_time = time.time()

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
