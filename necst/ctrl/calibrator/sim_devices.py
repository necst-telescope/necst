import time

import rclpy
from neclib.simulators.chopper import ChopperEmulator
from std_srvs.srv import Trigger
from necst_msgs.msg import ChopperMsg
from rclpy.node import Node

from necst import config, namespace, service, topic


class ChopperSimulator(Node):
    NodeName = "chopper_simulator"
    Namespace = namespace.calib

    def __init__(self):
        super().__init__(self.NodeName, namespace=self.Namespace)
        topic.chopper_cmd.subscription(self, self.move)
        service.chopper_alarm_reset.service(self, self.alarm_reset)
        service.chopper_home.service(self, self.home)
        service.chopper_recover.service(self, self.recover)
        self.pub = topic.chopper_status.publisher(self)
        self.motor = ChopperEmulator()
        self.create_timer(1, self.telemetry)

    @staticmethod
    def _maintenance_unsupported_reason() -> str | None:
        observatory = str(getattr(config, "observatory", "")).upper()
        if "NANTEN2" in observatory:
            return (
                "chopper maintenance is not supported for NANTEN2 "
                "(サポートしていません)"
            )
        return None

    def _maintenance_noop(
        self, response: Trigger.Response, operation: str
    ) -> Trigger.Response:
        unsupported = self._maintenance_unsupported_reason()
        if unsupported is not None:
            response.success = False
            response.message = unsupported
            return response
        response.success = True
        response.message = f"simulator chopper {operation} completed as no-op"
        return response

    def alarm_reset(
        self, request: Trigger.Request, response: Trigger.Response
    ) -> Trigger.Response:
        del request
        return self._maintenance_noop(response, "alarm-reset")

    def home(
        self, request: Trigger.Request, response: Trigger.Response
    ) -> Trigger.Response:
        del request
        result = self._maintenance_noop(response, "home")
        if result.success:
            self.motor.set_step("insert", "chopper")
            self.telemetry()
        return result

    def recover(
        self, request: Trigger.Request, response: Trigger.Response
    ) -> Trigger.Response:
        del request
        result = self._maintenance_noop(response, "recover")
        if result.success:
            self.motor.set_step("insert", "chopper")
            self.telemetry()
        return result

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
