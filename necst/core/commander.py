import time as pytime
from collections import defaultdict
from functools import partial
from typing import Any, Literal, Optional, Tuple, Union

from neclib.coordinates import StandbyPosition
from neclib.utils import ConditionChecker
from necst_msgs.msg import AlertMsg, ChopperMsg, CoordCmdMsg, CoordMsg, PIDMsg

from .. import NECSTTimeoutError, config, namespace, topic
from .auth import PrivilegedNode, require_privilege


class Commander(PrivilegedNode):

    NodeName = "commander"
    Namespace = namespace.core

    def __init__(self) -> None:
        super().__init__(self.NodeName, namespace=self.Namespace)
        self.publisher = {
            "coord": topic.raw_coord.publisher(self),
            "alert_stop": topic.manual_stop_alert.publisher(self),
            "pid_param": topic.pid_param.publisher(self),
            "chopper": topic.chopper_cmd.publisher(self),
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
            "chopper": topic.chopper_status.subscription(
                self, partial(self.__callback, "chopper")
            ),
        }

        self.parameters = defaultdict[str, Optional[Any]](lambda: None)

    def __callback(self, name: str, msg: Any) -> None:
        self.parameters[name] = msg

    def get_message(self, key: str) -> Any:
        while self.parameters[key] is None:
            pytime.sleep(0.01)
        return self.parameters[key]

    @require_privilege(escape_cmd=["?"])
    def antenna(
        self,
        cmd: Literal["stop", "point", "scan", "?"],
        *,
        lon: Optional[Union[int, float]] = None,
        lat: Optional[Union[int, float]] = None,
        start: Optional[Tuple[float, float]] = None,
        end: Optional[Tuple[float, float]] = None,
        speed: Optional[float] = None,
        unit: Optional[str] = None,
        frame: Optional[str] = None,
        time: Union[int, float] = 0,
        name: Optional[str] = None,
        wait: bool = True,
    ) -> None:
        """Control antenna direction and motion."""
        CMD = cmd.upper()
        if CMD == "STOP":
            target = [namespace.antenna]
            msg = AlertMsg(critical=True, warning=True, target=target)
            checker = ConditionChecker(5, reset_on_failure=True)
            while not checker.check(
                (self.parameters["speed"] is not None)
                and (abs(self.parameters["speed"].az) < 1e-5)
                and (abs(self.parameters["speed"].el) < 1e-5)
            ):
                self.publisher["alert_stop"].publish(msg)
                pytime.sleep(1 / config.antenna_command_frequency)

            msg = AlertMsg(critical=False, warning=False, target=target)
            self.publisher["alert_stop"].publish(msg)
            return

        elif CMD == "POINT":
            if name is not None:
                msg = CoordCmdMsg(time=[float(time)], name=name)
            else:
                msg = CoordCmdMsg(
                    lon=[float(lon)],
                    lat=[float(lat)],
                    unit=unit,
                    frame=frame,
                    time=[float(time)],
                )
            self.publisher["coord"].publish(msg)
            return self.wait_convergence("antenna") if wait else None

        elif cmd == "SCAN":
            standby = StandbyPosition()
            standby_lon, standby_lat = standby.standby_position(
                start=start, end=end, unit=unit
            )
            self.antenna(
                "point",
                lon=standby_lon,
                lat=standby_lat,
                unit=unit,
                frame=frame,
                wait=True,
            )

            msg = CoordCmdMsg(
                lon=[standby_lon, end[0]],
                lat=[standby_lat, end[1]],
                unit=unit,
                frame=frame,
                time=[time],
                speed=speed,
            )
            self.publisher["coord"].publish(msg)
            # TODO: Wait regardless of ``wait``.

            self.antenna("stop")
            return  # TODO: Wait if necessary.
        else:
            raise NotImplementedError(f"Command {cmd!r} isn't implemented yet.")

    @require_privilege(escape_cmd=["?"])
    def chopper(self, cmd: Literal["insert", "remove", "?"], wait: bool = True):
        """Calibrator."""
        CMD = cmd.upper()
        if CMD == "?":
            return self.get_message("chopper")
        elif CMD == "INSERT":
            msg = ChopperMsg(insert=True, time=pytime.time())
            self.publisher["chopper"].publish(msg)
        elif CMD == "REMOVE":
            msg = ChopperMsg(insert=False, time=pytime.time())
            self.publisher["chopper"].publish(msg)
        else:
            raise ValueError(f"Unknown command: {cmd!r}")

        if wait:
            target_status = CMD == "INSERT"
            while self.get_message("chopper").insert is not target_status:
                pytime.sleep(0.1)

    def wait_convergence(
        self,
        target: Literal["antenna", "dome"],
        timeout_sec: Optional[Union[int, float]] = None,
    ) -> None:
        """Wait until the motion has been converged."""
        _param_name = {
            "antenna": ["encoder", "altaz"],
            "dome": ["", ""],  # TODO: Implement.
        }
        _threshold = {
            "antenna": config.antenna_pointing_accuracy.to_value("deg"),
            "dome": ...,
        }
        ENC, CMD = _param_name[target.lower()]
        threshold = _threshold[target.lower()]

        timelimit = None if timeout_sec is None else pytime.time() + timeout_sec
        checker = ConditionChecker(10, reset_on_failure=True)
        while (timelimit is None) or (pytime.time() < timelimit):
            if (self.parameters[ENC] is None) or (self.parameters[CMD] is None):
                pytime.sleep(0.05)
                continue
            error_az = self.parameters[ENC].lon - self.parameters[CMD].lon
            error_el = self.parameters[ENC].lat - self.parameters[CMD].lat
            if checker.check(error_az**2 + error_el**2 < threshold**2):
                return
            pytime.sleep(0.05)
        raise NECSTTimeoutError("Couldn't confirm drive convergence")

    @require_privilege
    def pid_parameter(
        self,
        Kp: Union[int, float],
        Ki: Union[int, float],
        Kd: Union[int, float],
        axis: Literal["az", "el"],
    ) -> None:
        """Change PID parameters."""
        if axis.lower() not in ("az", "el"):
            raise ValueError(f"Unknown axis {axis!r}")
        msg = PIDMsg(k_p=float(Kp), k_i=float(Ki), k_d=float(Kd), axis=axis.lower())
        self.publisher["pid_param"].publish(msg)
        # TODO: Consider demand for parameter getter
