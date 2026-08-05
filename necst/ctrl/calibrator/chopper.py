import time

from neclib.devices import ChopperMotor
from std_srvs.srv import Trigger
from necst_msgs.msg import ChopperMsg

from ... import config, namespace, service, topic
from ...core import DeviceNode


class ChopperController(DeviceNode):
    NodeName = "chopper"
    Namespace = namespace.calib

    def __init__(self) -> None:
        super().__init__(self.NodeName, namespace=self.Namespace)
        self.logger = self.get_logger()

        self.motor = ChopperMotor()

        topic.chopper_cmd.subscription(self, self.move)
        service.chopper_alarm_reset.service(self, self.alarm_reset)
        service.chopper_home.service(self, self.home)
        service.chopper_recover.service(self, self.recover)
        self.pub = topic.chopper_status.publisher(self)
        self.create_timer(1, self.telemetry)

    @staticmethod
    def _maintenance_unsupported_reason() -> str | None:
        """Return a reason string when maintenance I/O is unsupported here."""
        observatory = str(getattr(config, "observatory", "")).upper()
        if "NANTEN2" in observatory:
            return (
                "chopper maintenance is not supported for NANTEN2 "
                "(サポートしていません)"
            )
        if "OMU1P85M" not in observatory:
            return (
                "chopper maintenance is supported only for OMU1p85m in this "
                f"implementation (current observatory={observatory or 'unknown'})"
            )
        return None

    def _run_maintenance_operation(
        self,
        response: Trigger.Response,
        operation: str,
    ) -> Trigger.Response:
        unsupported = self._maintenance_unsupported_reason()
        if unsupported is not None:
            self.logger.warning(unsupported)
            response.success = False
            response.message = unsupported
            return response

        try:
            if operation == "alarm-reset":
                self.motor.remove_alarm()
                message = "chopper alarm reset completed"
            elif operation == "home":
                self.motor.chopper_zero_point()
                message = "chopper home completed; counter was reset to 0"
            elif operation == "recover":
                self.motor.remove_alarm()
                self.motor.chopper_zero_point()
                message = "chopper recover completed; alarm reset and home finished"
            else:
                raise ValueError(f"unknown chopper maintenance operation: {operation}")

            self.telemetry()
            response.success = True
            response.message = message
            self.logger.info(message)
        except Exception as exc:
            response.success = False
            response.message = f"chopper {operation} failed: {exc}"
            self.logger.error(response.message)
        return response

    def alarm_reset(
        self, request: Trigger.Request, response: Trigger.Response
    ) -> Trigger.Response:
        del request
        return self._run_maintenance_operation(response, "alarm-reset")

    def home(
        self, request: Trigger.Request, response: Trigger.Response
    ) -> Trigger.Response:
        del request
        return self._run_maintenance_operation(response, "home")

    def recover(
        self, request: Trigger.Request, response: Trigger.Response
    ) -> Trigger.Response:
        del request
        return self._run_maintenance_operation(response, "recover")

    def move(self, msg: ChopperMsg) -> None:
        self.telemetry()
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
            self.logger.info(
                f"Chopper wheel position is (={position})",
                throttle_duration_sec=5,
            )
            msg = ChopperMsg(insert=True, position=position, time=time.time())
        self.pub.publish(msg)
