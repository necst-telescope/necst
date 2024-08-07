import time

from neclib.devices import DomeMotor as DomeMotorDevice
from necst_msgs.msg import DomeCommand, DomeOC, DomeLimit

from ... import config, namespace, topic
from ...core import DeviceNode


class DomeMotor(DeviceNode):
    NodeName = "dome_motor_driver"
    Namespace = namespace.dome

    def __init__(self) -> None:
        super().__init__(self.NodeName, namespace=self.Namespace)
        self.logger = self.get_logger()

        self.status_publisher = topic.dome_status.publisher(self)
        self.dome_publisher = topic.dome_limit.publisher(self)

        topic.dome_speed_cmd.subscription(self, self.speed_command)
        topic.dome_oc.subscription(self, self.move)
        topic.dome_limit_cmd.subscription(self, self.limit_check)

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
        if status[0] == status[2] == "OFF":
            if status[1] == status[3] == "OPEN":
                msg = DomeOC(open=True, move=False, time=time.time())
            elif status[1] == status[3] == "CLOSE":
                msg = DomeOC(open=False, move=False, time=time.time())
        else:
            msg = DomeOC(open=True, move=True, time=time.time())
        self.status_publisher.publish(msg)

    def limit_check(self, msg: DomeLimit):
        if not msg.check:
            re_msg = DomeLimit(check=False, limit=0)
        else:
            limit1 = self.motor.dome_limit_check()
            time.sleep(0.002)
            limit2 = self.motor.dome_limit_check()
            if limit1 == limit2:
                re_msg = DomeLimit(check=False, limit=limit1)
            else:
                re_msg = DomeLimit(check=False, limit=0)
        self.dome_publisher.publish(re_msg)


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
