import time

from neclib.devices import DomeMotor as DomeMotorDevice
from necst_msgs.msg import DomeCommand
from necst_msgs.srv import DomeOC, DomeLimit

from ... import config, namespace, topic, service
from ...core import DeviceNode


class DomeMotor(DeviceNode):
    NodeName = "dome_motor_driver"
    Namespace = namespace.dome

    def __init__(self) -> None:
        super().__init__(self.NodeName, namespace=self.Namespace)
        self.logger = self.get_logger()

        self.status_publisher = topic.dome_status.publisher(self)

        topic.dome_speed_cmd.subscription(self, self.speed_command)
        topic.dome_oc.subscription(self, self.move)

        if config.observatory == "NANTEN2":
            service.dome_limit.service(self, self.limit_check)

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

    def move(self, msg: DomeOC):
        self.telemetry()
        position = "open" if msg.open else "close"
        self.motor.dome_oc(position)
        self.telemetry()
        return

    def telemetry(self) -> None:
        status = self.motor.dome_status()
        if status[1] == status[3] == "OPEN":
            msg = DomeOC(open=True, time=time.time())
        elif status[1] == status[3] == "CLOSE":
            msg = DomeOC(open=False, time=time.time())
        else:
            self.logger.warning(
                f"Dome door is off the expected position (={status[1]})",
                throttle_duration_sec=5,
            )
            return
        self.status_publisher.publish(msg)

    def limit_check(self, request: DomeLimit.Request, response: DomeLimit.Response):
        if not request.check:
            response.limit = 0
            return response
        while True:
            limit1 = self.motor.dome_limit_check()
            time.sleep(0.002)
            limit2 = self.motor.dome_limit_check()
            if limit1 == limit2:
                response.limit = limit1
                return response
            continue


def main(args=None):
    import rclpy

    rclpy.init(args=args)
    node = DomeMotor()
    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    finally:
        node.motor.dome_stop()
        node.destroy_node()
        rclpy.try_shutdown()


if __name__ == "__main__":
    main()
