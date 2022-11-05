import time as pytime
from collections import defaultdict
from functools import partial
from typing import Any, Literal

from neclib.utils import ConditionChecker

from .. import config, namespace, qos, utils
from .auth import PrivilegedNode, require_privilege
from necst_msgs.msg import CoordMsg


class Commander(PrivilegedNode):

    NodeName = "commander"
    Namespace = namespace.core

    def __init__(self):
        super().__init__(self.NodeName, namespace=self.Namespace)
        self.publisher = {
            "coord": self.create_publisher(
                CoordMsg, f"{namespace.antenna}/raw_coord", qos.reliable
            ),
        }
        self.subscription = {
            "encoder": self.create_subscription(
                CoordMsg,
                f"{namespace.antenna}/encoder",
                partial(self.__callback, "encoder"),
                qos.realtime,
            ),
            "altaz": self.create_subscription(
                CoordMsg,
                f"{namespace.antenna}/altaz",
                partial(self.__callback, "altaz"),
                qos.realtime,
            ),
        }

        self.parameters = defaultdict(lambda: None)

    def __callback(self, name: str, msg: Any):
        self.publisher["coord"].publish(CoordMsg(name=name))
        self.parameters[name] = msg

    @require_privilege
    def antenna(
        self,
        cmd: Literal["stop", "point", "scan", "jog"],
        *,
        lon: float = None,
        lat: float = None,
        unit: str = None,
        frame: str = None,
        time: float = 0.0,
        name: str = None,
        wait: bool = True,
    ) -> None:
        if cmd.lower() == "stop":
            # with utils.spinning(self):
            while True:
                if self.parameters["speed"] is None:
                    # self.publisher["coord"].publish(CoordMsg(name="continue"))
                    pytime.sleep(0.05)
                    continue
                speed = self.parameters["speed"]
                if (abs(speed.az) < 1e-3) and (abs(speed.el) < 1e-3):
                    break
                self.publisher["coord"].publish(self.parameters["encoder"])
                pytime.sleep(config.antenna_command_interval_sec)
            return

        elif cmd.lower() == "point":
            if name is not None:
                msg = CoordMsg(time=time, name=name)
            else:
                msg = CoordMsg(
                    lon=float(lon), lat=float(lat), unit=unit, frame=frame, time=time
                )
            self.publisher["coord"].publish(msg)

            if wait:
                self.tracking_check("antenna")
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

    @require_privilege
    def chopper(self, cmd: Literal["insert", "eject"]):
        ...

    def tracking_check(
        self, target: Literal["antenna", "dome"], timeout_sec: float = None
    ) -> bool:
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
                if (self.params[ENC] is None) or (self.params[CMD] is None):
                    pytime.sleep(0.05)
                    continue
                error_az = self.params[ENC].az - self.params[CMD].az
                error_el = self.params[ENC].el - self.params[CMD].el
                if checker.check(
                    error_az**2 + error_el**2 < threshold[target] ** 2
                ):
                    return True
                if (timelimit is not None) and (pytime.time() > timelimit):
                    return False
                pytime.sleep(0.05)
