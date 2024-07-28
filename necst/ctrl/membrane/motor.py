import time

from neclib.devices import Membrane as MembraneMotor
from necst_msgs.msg import MembraneMsg

from ... import namespace, topic
from ...core import DeviceNode


class MembraneController(DeviceNode):
    NodeName = "membrane"
    Namespace = namespace.membrane

    def __init__(self) -> None:
        super().__init__(self.NodeName, namespace=self.Namespace)
        self.logger = self.get_logger()

        self.motor = MembraneMotor()

        topic.membrane_cmd.subscription(self, self.move)
        self.pub = topic.membrane_status.publisher(self)
        self.create_timer(1, self.telemetry)

    def move(self, msg: MembraneMsg) -> None:
        self.telemetry()
        position = "open" if msg.open else "close"
        self.motor.memb_mobe(position)
        self.telemetry()

    def telemetry(self) -> None:
        status = self.motor.memb_status()
        if status[1] == "open":
            msg = MembraneMsg(open=True, time=time.time())
        elif status[1] == "close":
            msg = MembraneMsg(open=False, time=time.time())
        else:
            self.logger.warning(
                f"Membrane is off the expected position (={status[1]})",
                throttle_duration_sec=5,
            )
            return
        self.pub.publish(msg)
