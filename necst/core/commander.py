import time as pytime
from collections import defaultdict
from functools import partial
from typing import Any, Literal, Optional

from neclib.utils import ConditionChecker
from necst_msgs.msg import AlertMsg, CoordMsg

from .. import NECSTTimeoutError, config, namespace, topic, utils
from .auth import PrivilegedNode, require_privilege


class Commander(PrivilegedNode):

    NodeName = "commander"
    Namespace = namespace.core

    def __init__(self):
        super().__init__(self.NodeName, namespace=self.Namespace)
        self.publisher = {
            "coord": topic.raw_coord.publisher(self),
            "alert_stop": topic.manual_stop_alert.publisher(self),
        }
        self.subscription = {
            "encoder": topic.antenna_encoder.subscription(
                self, partial(self.__callback, "encoder")
            ),
            "altaz": topic.altaz_cmd.subscription(
                self, partial(self.__callback, "altaz")
            ),
            "speed": topic.antenna_speed_cmd.subscription(
                self, partial(self.__callback, "speed")
            ),
        }

        self.parameters = defaultdict(lambda: None)

    def __callback(self, name: str, msg: Any):
        self.parameters[name] = msg

    @require_privilege
    def antenna(
        self,
        cmd: Literal["stop", "point", "scan", "jog"],
        *,
        lon: Optional[float] = None,
        lat: Optional[float] = None,
        unit: Optional[str] = None,
        frame: Optional[str] = None,
        time: Optional[float] = 0.0,
        name: Optional[str] = None,
        wait: bool = True,
    ) -> None:
        cmd = cmd.upper()
        if cmd == "STOP":
            with utils.spinning(self):
                while True:
                    self.publisher["alert_stop"].publish(
                        AlertMsg(
                            critical=True, warning=True, target=[namespace.antenna]
                        )
                    )
                    pytime.sleep(1 / config.antenna_command_frequency)
                    speed = self.parameters["speed"]
                    if (
                        (speed is not None)
                        and (abs(speed.az) < 1e-5)
                        and (abs(speed.el) < 1e-5)
                    ):
                        self.publisher["alert_stop"].publish(
                            AlertMsg(
                                critical=False,
                                warning=False,
                                target=[namespace.antenna],
                            )
                        )
                        break
            return

        elif cmd == "POINT":
            if name is not None:
                msg = CoordMsg(time=time, name=name)
            else:
                msg = CoordMsg(
                    lon=float(lon), lat=float(lat), unit=unit, frame=frame, time=time
                )
            self.publisher["coord"].publish(msg)

            if wait:
                self.wait_convergence("antenna")
            return

        else:
            raise NotImplementedError(f"Command '{cmd}' isn't implemented yet.")

    @require_privilege
    def chopper(self, cmd: Literal["insert", "eject"]):
        """Calibrator."""
        if cmd.lower() == "insert":
            ...
        elif cmd.lower() == "eject":
            ...
        else:
            raise NotImplementedError(f"Command '{cmd}' isn't implemented yet.")

    def wait_convergence(
        self, target: Literal["antenna", "dome"], timeout_sec: Optional[float] = None
    ) -> None:
        param_name = {
            "antenna": ["encoder", "altaz"],
            "dome": ["", ""],  # TODO: Implement.
        }
        threshold = {
            "antenna": config.antenna_pointing_accuracy.to_value("deg"),
            "dome": ...,
        }
        ENC, CMD = param_name[target]

        timelimit = None if timeout_sec is None else pytime.time() + timeout_sec
        checker = ConditionChecker(10, reset_on_failure=True)
        with utils.spinning(self):
            while True:
                if (self.parameters[ENC] is None) or (self.parameters[CMD] is None):
                    pytime.sleep(0.05)
                    continue
                error_az = self.parameters[ENC].lon - self.parameters[CMD].lon
                error_el = self.parameters[ENC].lat - self.parameters[CMD].lat
                if checker.check(
                    error_az**2 + error_el**2 < threshold[target] ** 2
                ):
                    return
                if (timelimit is not None) and (pytime.time() > timelimit):
                    break
                pytime.sleep(0.05)
        raise NECSTTimeoutError("Couldn't confirm drive convergence")
