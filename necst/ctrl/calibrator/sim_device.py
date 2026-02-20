import time

import rclpy
from neclib.simulators.chopper import ChopperEmulator
from necst_msgs.msg import ChopperMsg
from rclpy.node import Node

from necst import namespace, topic


class ChopperSimulator(Node):
    NodeName = "chopper_simulator"
    Namespace = namespace.calib

    def __init__(self):
        super().__init__(self.NodeName, namespace=self.Namespace)
        topic.chopper_cmd.subscription(self, self.move)
        self.pub = topic.chopper_status.publisher(self)
        self.motor = ChopperEmulator()
        self.create_timer(1, self.telemetry)

    def move(self, msg):
        self.telemetry()
        position = "insert" if msg.insert else "remove"
        self.motor.set_step(position, "chopper")
        self.telemetry()

    def telemetry(self) -> None:
        position = self.motor.get_step("chopper")
        if position == "insert":
            msg = ChopperMsg(insert=True, time=time.time())
        elif position == "remove":
            msg = ChopperMsg(insert=False, time=time.time())
        else:
            self.logger.warning(
                f"Chopper wheel is off the expected position (={position})",
                throttle_duration_sec=5,
            )
            return
        self.pub.publish(msg)


def main(args=None):
    rclpy.init(args=args)
    node = ChopperSimulator()
    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    finally:
        node.destroy_node()
        rclpy.try_shutdown()


if __name__ == "__main__":
    main()