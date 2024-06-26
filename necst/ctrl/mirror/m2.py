import time

from neclib.devices import M2Motor
from necst_msgs.msg import MirrorMsg

from ... import namespace, topic
from ...core import DeviceNode


class M4Controller(DeviceNode):
    NodeName = "mirror"
    Namespace = namespace.mirror

    def __init__(self) -> None:
        super().__init__(self.NodeName, namespace=self.Namespace)
        self.logger = self.get_logger()

        self.motor = M2Motor()

        topic.mirror_m2_cmd.subscription(self, self.move)
        self.pub = topic.mirror_m2_status.publisher(self)
        self.create_timer(1, self.telemetry)

    def move(self, msg: MirrorMsg) -> None:
        status = self.telemetry()
        self.motor.set_step(msg.distance, status, "m2")
        self.telemetry()

    def telemetry(self):
        status = self.motor.get_step("m2")
        msg = MirrorMsg(status=status, time=time.time())

        self.pub.publish(msg)
        return status
