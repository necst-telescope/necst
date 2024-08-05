import time

from neclib.devices import DriveMotor as DriveMotorDevice
from necst_msgs.msg import DriveMsg

from ... import namespace, topic
from ...core import DeviceNode


class DriveMotor(DeviceNode):
    NodeName = "drive"
    Namespace = namespace.drive

    def __init__(self) -> None:
        super().__init__(self.NodeName, namespace=self.Namespace)
        self.logger = self.get_logger()

        self.motor = DriveMotorDevice()

        topic.drive_cmd.subscription(self, self.move)
        self.pub = topic.drive_status.publisher(self)
        self.create_timer(1, self.telemetry)

    def move(self, msg: DriveMsg) -> None:
        self.telemetry()
        if msg.separation.lower() == "drive":
            on = "on" if msg.drive else "off"
            self.motor.drive_move(on)
        elif msg.separation.lower() == "contactor":
            on = "on" if msg.contactor else "off"
            self.motor.contactor_move(on)
        self.telemetry()

    def telemetry(self) -> None:
        status = self.motor.drive_contactor_status()
        if status[0] == "ON":
            if status[1] == "ON":
                msg = DriveMsg(
                    separation="status", drive=True, contactor=True, time=time.time()
                )
            else:
                msg = DriveMsg(
                    separation="status", drive=True, contactor=False, time=time.time()
                )
        else:
            if status[1] == "ON":
                msg = DriveMsg(
                    separation="status", drive=False, contactor=True, time=time.time()
                )
            else:
                msg = DriveMsg(
                    separation="status", drive=False, contactor=False, time=time.time()
                )
        self.pub.publish(msg)


def main(args=None):
    import rclpy

    rclpy.init(args=args)
    node = DriveMotor()
    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    finally:
        node.destroy_node()
        rclpy.try_shutdown()


if __name__ == "__main__":
    main()
