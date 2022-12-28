import time

from neclib.devices import ChopperMotor
from necst_msgs.msg import ChopperMsg

from ... import config, get_logger, namespace, topic
from ...core import DeviceNode


class ChopperController(DeviceNode):

    NodeName = "chopper"
    Namespace = namespace.calib

    def __init__(self) -> None:
        super().__init__(self.NodeName, namespace=self.Namespace)
        self.logger = get_logger(self.__class__.__name__)

        self.motor = ChopperMotor()

        topic.chopper_cmd.subscription(self, self.move)
        self.pub = topic.chopper_status.publisher(self)
        self.create_timer(1, self.telemetry)

    def move(self, msg: ChopperMsg) -> None:
        self.telemetry()
        position = "insert" if msg.insert else "remove"
        self.motor.set_step(position, "chopper")
        self.telemetry()

    def telemetry(self) -> None:
        position = self.motor.get_step("chopper")
        if position == config.chopper_position_in:
            msg = ChopperMsg(insert=True, time=time.time())
        elif position == config.chopper_position_away:
            msg = ChopperMsg(insert=False, time=time.time())
        else:
            self.logger.warning(
                f"Chopper wheel is off the expected position (={position})"
            )
            return
        self.pub.publish(msg)
