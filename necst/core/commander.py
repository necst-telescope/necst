import time as pytime
from collections import defaultdict
from functools import partial
from typing import Any, Literal, Optional, Tuple, Union

from neclib.coordinates import standby_position
from neclib.data import LinearInterp
from neclib.utils import ConditionChecker, ParameterList, read_file
from necst_msgs.msg import (
    AlertMsg,
    BiasMsg,
    ChopperMsg,
    CoordCmdMsg,
    LocalSignal,
    PIDMsg,
    Spectral,
)
from necst_msgs.srv import File, RecordSrv

from .. import NECSTTimeoutError, config, namespace, service, topic
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
            "spectra_meta": topic.spectra_meta.publisher(self),
            "qlook_meta": topic.qlook_meta.publisher(self),
            "sis_bias": topic.sis_bias_cmd.publisher(self),
            "lo_signal": topic.lo_signal_cmd.publisher(self),
        }
        _cfg_ant = config.antenna_command
        _altaz_offset = int(_cfg_ant.frequency * _cfg_ant.offset_sec + 1)
        self.subscription = {
            "encoder": topic.antenna_encoder.subscription(
                self, partial(self.__callback, "encoder", 1)
            ),
            "altaz": topic.altaz_cmd.subscription(
                self, partial(self.__callback, "altaz", _altaz_offset)
            ),
            "speed": topic.antenna_speed_cmd.subscription(
                self, partial(self.__callback, "speed", 1)
            ),
            "chopper": topic.chopper_status.subscription(
                self, partial(self.__callback, "chopper", 1)
            ),
            "antenna_control": topic.antenna_control_status.subscription(
                self, partial(self.__callback, "antenna_control", 1)
            ),
            "sis_bias": topic.sis_bias.subscription(
                self, partial(self.__callback, "sis_bias", 1)
            ),
            "lo_signal": topic.lo_signal.subscription(
                self, partial(self.__callback, "lo_signal", 1)
            ),
        }
        self.client = {
            "record_path": service.record_path.client(self),
            "record_file": service.record_file.client(self),
        }

        self.parameters = defaultdict[str, ParameterList[Any]](lambda: ParameterList())

    def __callback(self, key: str, keep: int, msg: Any) -> None:
        if len(self.parameters[key]) == 0:
            self.parameters[key] = ParameterList.new(keep, None)

        self.parameters[key].push(msg)

    def get_message(
        self,
        key: str,
        time: Optional[Union[int, float]] = None,
        timeout_sec: Optional[Union[int, float]] = None,
        *,
        interp: bool = False,
    ) -> Optional[Any]:
        start = pytime.monotonic()
        while (timeout_sec is None) or (pytime.monotonic() - start < timeout_sec):
            if all(x is None for x in self.parameters[key]):
                pytime.sleep(0.05)
                continue
            msgs = [x for x in self.parameters[key] if x is not None]
            assert len(msgs) > 0
            msg_type = msgs[-1].__class__
            msg_fields = msg_type.get_fields_and_field_types().keys()
            if "time" not in msg_fields:
                return msgs[-1]

            if interp:
                interpolate = LinearInterp("time", msg_fields)
                return interpolate(msg_type(time=time), msgs)
            else:
                nearest, *_ = sorted(msgs, key=lambda x: abs(x.time - time))
                return nearest
        raise NECSTTimeoutError(f"No message has been received on topic {key!r}")

    @require_privilege(escape_cmd=["?", "stop"])
    def antenna(
        self,
        cmd: Literal["stop", "point", "scan", "?"],
        /,
        *,
        lon: Optional[Union[int, float]] = None,
        lat: Optional[Union[int, float]] = None,
        start: Optional[Tuple[Union[int, float], Union[int, float]]] = None,
        end: Optional[Tuple[Union[int, float], Union[int, float]]] = None,
        speed: Union[int, float] = 0,
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
            return self.publisher["alert_stop"].publish(msg)

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

        elif CMD == "SCAN":
            standby_lon, standby_lat = standby_position(
                start=start, end=end, unit=unit, margin=config.antenna_scan_margin
            )
            standby_lon = float(standby_lon.value)
            standby_lat = float(standby_lat.value)
            self.antenna(
                "point",
                lon=standby_lon,
                lat=standby_lat,
                unit=unit,
                frame=frame,
                wait=True,
            )

            msg = CoordCmdMsg(
                lon=[float(start[0]), float(end[0])],
                lat=[float(start[1]), float(end[1])],
                unit=unit,
                frame=frame,
                time=[float(time)],
                speed=float(speed),
            )
            self.publisher["coord"].publish(msg)
            if wait:
                self.wait_convergence("antenna", mode="control")
                self.antenna("stop")
            return
        elif CMD in ["?"]:
            raise NotImplementedError(f"Command {cmd!r} isn't implemented yet.")
        else:
            raise ValueError(f"Unknown command: {cmd!r}")

    @require_privilege(escape_cmd=["?"])
    def chopper(self, cmd: Literal["insert", "remove", "?"], /, *, wait: bool = True):
        """Calibrator."""
        CMD = cmd.upper()
        if CMD == "?":
            return self.get_message("chopper", timeout_sec=10)
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
        /,
        *,
        mode: Literal["control", "error"] = "error",
        timeout_sec: Optional[Union[int, float]] = None,
    ) -> None:
        """Wait until the motion has been converged."""
        TARGET, MODE = target.upper(), mode.upper()

        if TARGET == "ANTENNA":
            ENC_TOPIC = "encoder"
            CMD_TOPIC = "altaz"
            CTRL_TOPIC = "antenna_control"
            THRESHOLD = config.antenna_pointing_accuracy.to_value("deg")
            WAIT_DURATION = config.antenna_command_offset_sec
        elif TARGET == "DOME":
            raise NotImplementedError(
                f"This function for target {target!r} isn't implemented yet."
            )
        else:
            raise ValueError(f"Unknown target: {target!r}")

        # Drive should have delay of offset duration
        pytime.sleep(WAIT_DURATION)

        start = pytime.monotonic()
        checker = ConditionChecker(10, reset_on_failure=True)

        if MODE == "ERROR":
            while (timeout_sec is None) or (pytime.monotonic() - start < timeout_sec):
                now = pytime.time()
                try:
                    enc = self.get_message(ENC_TOPIC, now, 0.01)
                    cmd = self.get_message(CMD_TOPIC, now, 0.01, interp=True)
                    error_az, error_el = enc.lon - cmd.lon, enc.lat - cmd.lat
                    error = (error_az**2 + error_el**2) ** 0.5
                    self.logger.debug(
                        f"Error = {error:10.6f} deg"
                        f" {'[OK]' if error < THRESHOLD else '[NG]'}",
                        throttle_duration_sec=0.1,
                    )
                    if checker.check(error < THRESHOLD):
                        self.logger.debug("Tracking OK")
                        return
                except NECSTTimeoutError:
                    pass
                pytime.sleep(0.05)
        elif MODE == "CONTROL":
            while (timeout_sec is None) or (pytime.monotonic() - start < timeout_sec):
                now = pytime.time()
                try:
                    controlled = self.get_message(CTRL_TOPIC, now, 0.01).controlled
                    if checker.check(not controlled):
                        return
                except NECSTTimeoutError:
                    pass
                pytime.sleep(0.05)
        else:
            raise ValueError(f"Unknown mode: {mode!r}")

        raise NECSTTimeoutError("Couldn't confirm drive convergence")

    @require_privilege(escape_cmd=["?"])
    def pid_parameter(
        self,
        cmd: Literal["set", "?"],
        /,
        *,
        Kp: Union[int, float],
        Ki: Union[int, float],
        Kd: Union[int, float],
        axis: Literal["az", "el"],
    ) -> None:
        """Change PID parameters."""
        CMD = cmd.upper()
        if CMD == "SET":
            if axis.lower() not in ("az", "el"):
                raise ValueError(f"Unknown axis {axis!r}")
            msg = PIDMsg(k_p=float(Kp), k_i=float(Ki), k_d=float(Kd), axis=axis.lower())
            self.publisher["pid_param"].publish(msg)
        elif CMD == "?":
            raise NotImplementedError(f"Command {cmd!r} is not implemented yet.")
            # TODO: Consider demand for parameter getter
        else:
            raise ValueError(f"Unknown command: {cmd!r}")

    def metadata(
        self,
        cmd: Literal["set", "?"],
        /,
        *,
        position: str,
        id: str,
        time: Optional[float] = None,
    ) -> None:
        CMD = cmd.upper()
        if CMD == "SET":
            time = pytime.time() if time is None else time
            msg = Spectral(position=position, id=str(id), time=time)
            return self.publisher["spectra_meta"].publish(msg)
        elif CMD == "?":
            # May return metadata, by subscribing to the resized spectral data.
            raise NotImplementedError(f"Command {cmd!r} is not implemented yet.")
        else:
            raise ValueError(f"Unknown command: {cmd!r}")

    def quick_look(
        self,
        mode: Literal["ch", "rf", "if", "vlsr"],
        /,
        *,
        range: Tuple[Union[int, float], Union[int, float]],
        integ: Union[float, int] = 1,
    ) -> None:
        MODE = mode.upper()
        if MODE == "CH":
            range = tuple(map(int, range))
            self.publisher["qlook_meta"].publish(Spectral(ch=range, integ=float(integ)))
        elif MODE in ("IF", "RF", "VLSR"):
            raise NotImplementedError(f"Mode {mode!r} is not implemented yet.")
        else:
            raise ValueError(f"Unknown mode: {mode!r}")

    def record(
        self,
        cmd: Literal["start", "stop", "file", "?"],
        /,
        *,
        name: str = "",
        content: Optional[str] = None,
    ) -> None:
        CMD = cmd.upper()
        if CMD == "START":
            recording = False
            while not recording:
                req = RecordSrv.Request(name=name.lstrip("/"), stop=False)
                future = self.client["record_path"].call_async(req)
                self.wait_until_future_complete(future)
                recording = future.result().recording
            return
        elif CMD == "STOP":
            recording = True
            while recording:
                req = RecordSrv.Request(name=name, stop=True)
                future = self.client["record_path"].call_async(req)
                self.wait_until_future_complete(future)
                recording = future.result().recording
            return
        elif CMD == "FILE":
            if content is None:
                content = read_file(name)
            req = File.Request(data=str(content), path=name)
            future = self.client["record_file"].call_async(req)
            return self.wait_until_future_complete(future)
        elif CMD == "?":
            raise NotImplementedError(f"Command {cmd!r} is not implemented yet.")
        else:
            raise ValueError(f"Unknown command: {cmd!r}")

    @require_privilege(escape_cmd=["?"])
    def sis_bias(
        self,
        cmd: Literal["set", "?"],
        /,
        *,
        mV: Optional[Union[int, float]] = None,
        id: Optional[str] = None,
    ) -> None:
        CMD = cmd.upper()
        if CMD == "SET":
            if not -8 <= mV <= 8:
                # TODO: Implement the checker in neclib.devices, and define limit values
                # in config
                raise ValueError(f"Unsafe voltage: {mV} mV")
            self.publisher["sis_bias"].publish(BiasMsg(voltage=float(mV), id=id))
        elif CMD == "?":
            return self.get_message("sis_bias", timeout_sec=10)
        else:
            raise ValueError(f"Unknown command: {cmd!r}")

    @require_privilege(escape_cmd=["?"])
    def attenuator(
        self, cmd: Literal["set", "?"], /, *, dB: Optional[Union[int, float]]
    ) -> None:
        ...

    @require_privilege(escape_cmd=["?"])
    def signal_generator(
        self,
        cmd: Literal["set", "?"],
        /,
        *,
        GHz: Optional[Union[int, float]] = None,
        dBm: Optional[Union[int, float]] = None,
    ) -> None:
        CMD = cmd.upper()
        if CMD == "SET":
            msg = LocalSignal(freq=float(GHz), power=float(dBm))
            self.publisher["local_signal"].publish(msg)
        elif CMD == "?":
            return self.get_message("lo_signal", timeout_sec=10)
        else:
            raise ValueError(f"Unknown command: {cmd!r}")

    sg = signal_generator
    patt = attenuator
    qlook = quick_look
    pid = pid_parameter
