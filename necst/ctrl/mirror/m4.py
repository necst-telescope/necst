import time

from neclib.devices import M4Motor
from necst_msgs.msg import MirrorMsg

from ... import config, namespace, topic
from ...core import DeviceNode


class ChopperController(DeviceNode):
    NodeName = "mirror"
    Namespace = namespace.mirror

    def __init__(self) -> None:
        super().__init__(self.NodeName, namespace=self.Namespace)
        self.logger = self.get_logger()

        self.motor = M4Motor()

        topic.mirror_cmd.subscription(self, self.move)
        self.pub = topic.mirror_status.publisher(self)
        self.create_timer(1, self.telemetry)

    def move(self, msg: MirrorMsg) -> None:
        self.telemetry()
        position = "IN" if msg.in else "OUT"

        if position == "IN":
            step = 1
        else:
            step = -1

        # set_stepがいるのか分からなくなった
        self.motor.set_step(step)
        self.motor.move(position)
        self.telemetry()

    def telemetry(self) -> None:
        position = self.motor.get_pos()
        if position == "ERROR":
            self.logger.warning(
                f"Mirror is off the expected position (={position})",
                throttle_duration_sec=5,
            )
            return
        else:
            msg = MirrorMsg(position=position, time=time.time())

        self.pub.publish(msg)