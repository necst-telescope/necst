import time

from neclib.devices import M4Motor
from necst_msgs.msg import MirrorMsg

from ... import namespace, topic
from ...core import DeviceNode


class M4Controller(DeviceNode):
    NodeName = "mirror"
    Namespace = namespace.mirror

    def __init__(self) -> None:
        super().__init__(self.NodeName, namespace=self.Namespace)
        self.logger = self.get_logger()

        self.motor = M4Motor()

        topic.mirror_m4_cmd.subscription(self, self.move)
        self.pub = topic.mirror_m4_status.publisher(self)
        self.create_timer(1, self.telemetry)

    def move(self, msg: MirrorMsg) -> None:
        self.telemetry()
        position = msg.position
        self.motor.set_step(position)
        self.telemetry()

    def telemetry(self) -> None:
        position = self.motor.get_step()
        if position == "ERROR":
            self.logger.warning(
                f"Mirror is off the expected position (={position})",
                throttle_duration_sec=5,
            )
            return
        else:
            msg = MirrorMsg(position=position, time=time.time())

        self.pub.publish(msg)
