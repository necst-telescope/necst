import time

from neclib.devices import MembraneMotor as MembraneMotorDevice
from necst_msgs.msg import MembraneMsg

from ... import namespace, topic
from ...core import DeviceNode


class MembraneMotor(DeviceNode):
    NodeName = "membrane"
    Namespace = namespace.membrane

    def __init__(self) -> None:
        super().__init__(self.NodeName, namespace=self.Namespace)
        self.logger = self.get_logger()

        self.motor = MembraneMotorDevice()

        topic.membrane_cmd.subscription(self, self.move)
        self.pub = topic.membrane_status.publisher(self)
        self.create_timer(1, self.telemetry)

    def move(self, msg: MembraneMsg) -> None:
        self.telemetry()
        position = "open" if msg.open else "close"
        self.motor.memb_oc(position)
        self.telemetry()

    def telemetry(self) -> None:
        status = self.motor.memb_status()
        if status[0] == "OFF":
            if status[1] == "OPEN":
                msg = MembraneMsg(open=True, move=False, time=time.time())
            elif status[1] == "CLOSE":
                msg = MembraneMsg(open=False, move=False, time=time.time())
        else:
            msg = MembraneMsg(open=True, move=True, time=time.time())
            return
        self.pub.publish(msg)


def main(args=None):
    import rclpy

    rclpy.init(args=args)
    node = MembraneMotor()
    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    finally:
        node.motor.membrane_pose()
        node.destroy_node()
        rclpy.try_shutdown()


if __name__ == "__main__":
    main()
