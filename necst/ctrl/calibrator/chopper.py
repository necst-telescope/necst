import time

from neclib.devices import ChopperMotor
from necst_msgs.msg import ChopperMsg

from ... import config, namespace, topic
from ...core import DeviceNode


class ChopperController(DeviceNode):
    NodeName = "chopper"
    Namespace = namespace.calib

    def __init__(self) -> None:
        super().__init__(self.NodeName, namespace=self.Namespace)
        self.logger = self.get_logger()

        self.motor = ChopperMotor()

        topic.chopper_cmd.subscription(self, self.move)
        self.pub = topic.chopper_status.publisher(self)
        self.create_timer(1, self.telemetry)

    def move(self, msg: ChopperMsg) -> None:
        self.telemetry()
        # position = "insert" if msg.insert else "remove"
        if msg.position is not None:
            position = msg.position
        else:
            position = "insert" if msg.insert else "remove"
        self.motor.set_step(position, "chopper")
        self.telemetry()

    def telemetry(self) -> None:
        position = self.motor.get_step("chopper")
        if position == config.chopper_motor_position["insert"]:
            msg = ChopperMsg(insert=True, position=position, time=time.time())
        elif position == config.chopper_motor_position["remove"]:
            msg = ChopperMsg(insert=False, position=position, time=time.time())
        else:
            self.logger.warning(
                f"Chopper wheel is off the expected position (={position})",
                throttle_duration_sec=5,
            )
            return
        self.pub.publish(msg)
