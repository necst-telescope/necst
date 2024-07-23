import time

from neclib.devices import DomeMotor as DomeMotorDevice
from necst_msgs.msg import TimedAzElFloat64, TimedAzElInt64

from ... import config, namespace, topic
from ...core import DeviceNode


class DomeMotor(DeviceNode):
    NodeName = "dome_motor_driver"
    Namespace = namespace.dome

    def __init__(self) -> None:
        super().__init__(self.NodeName, namespace=self.Namespace)
        self.logger = self.get_logger()

        self.publisher = {
            "speed": topic.dome_motor_speed.publisher(self),
            "step": topic.dome_motor_step.publisher(self),
        }
        topic.dome_speed_cmd.subscription(self, self.speed_command)
        self.create_timer(1 / 10, self.stream_speed)
        self.create_timer(1 / 10, self.stream_step)
        self.create_timer(5, self.check_command)

        self.last_cmd_time = time.time()

        self.motor = DomeMotorDevice()

    def check_command(self) -> None:
        timelimit = config.antenna_command_offset_sec
        if time.time() - self.last_cmd_time > timelimit:
            self.motor.stop()
            self.logger.warning(
                f"No command supplied for {timelimit} s, stopping the antenna",
                throttle_duration_sec=10,
            )

    def speed_command(self, msg: TimedAzElFloat64) -> None:
        # if msg.time < now - 0.05:
        #     return
        # while msg.time > time.time():
        #     time.sleep(1e-5)
        if msg.speed:
            self.motor.stop()
        else:
            self.motor.set_speed(msg.turn, msg.speed)

        self.last_cmd_time = time.time()

    # def stream_speed(self) -> None:
    #     readout_az = self.motor.get_speed("az").to_value("deg/s").item()
    #     speed_msg = TimedAzElFloat64(az=float(readout_az), time=time.time())
    #     self.publisher["speed"].publish(speed_msg)

    # def stream_step(self) -> None:
    #     readout_az = self.motor.get_step("az")
    #     step_msg = TimedAzElInt64(az=readout_az, time=time.time())
    #     self.publisher["step"].publish(step_msg)


def main(args=None):
    import rclpy

    rclpy.init(args=args)
    node = DomeMotor()
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
