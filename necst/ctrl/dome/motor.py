import time

from neclib.devices import DomeMotor as DomeMotorDevice
from necst_msgs.msg import DomeCommand, DomeStatus
from necst_msgs.srv import DomeOC

from ... import config, namespace, topic, service
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

        self.status_publisher = topic.dome_status.publisher(self)

        topic.dome_speed_cmd.subscription(self, self.speed_command)
        service.dome_oc.service(self, self.move)

        self.create_timer(1 / 10, self.stream_speed)
        self.create_timer(1 / 10, self.stream_step)
        self.create_timer(5, self.check_command)

        self.create_timer(1, self.telemetry)

        self.last_cmd_time = time.time()

        self.motor = DomeMotorDevice()

    def check_command(self) -> None:
        timelimit = config.dome_command_offset_sec
        if time.time() - self.last_cmd_time > timelimit:
            self.motor.dome_stop()
            self.logger.warning(
                f"No command supplied for {timelimit} s, stopping the dome",
                throttle_duration_sec=10,
            )

    def speed_command(self, msg: DomeCommand) -> None:
        if msg.speed == "stop":
            self.motor.dome_stop()
        else:
            self.motor.dome_move(msg.speed, msg.turn)

        self.last_cmd_time = time.time()

    def move(self, request: DomeOC.Request, response: DomeOC.Response):
        self.motor.dome_oc(request.position)
        response.check = True
        return response

    def telemetry(self):
        status = self.motor.dome_status()

        msg = DomeStatus(
            right_act=status[0],
            right_pos=status[1],
            left_act=status[2],
            left_pos=status[3],
        )

        self.status_publisher.publish(msg)
        return


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
