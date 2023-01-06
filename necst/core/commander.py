import time as pytime
from dataclasses import dataclass
from functools import partial
from typing import Any, Dict, Literal, Optional, Tuple, Union

from neclib.coordinates import standby_position
from neclib.data import LinearInterp
from neclib.utils import ConditionChecker, ParameterList, read_file
from necst_msgs.msg import (
    AlertMsg,
    BiasMsg,
    ChopperMsg,
    CoordCmdMsg,
    DeviceReading,
    LocalSignal,
    PIDMsg,
    Spectral,
)
from necst_msgs.srv import File, RecordSrv
from rclpy.publisher import Publisher
from rclpy.subscription import Subscription

from .. import NECSTTimeoutError, config, namespace, service, topic
from ..utils import Topic
from .auth import PrivilegedNode, require_privilege


@dataclass
class _SubscriptionCfg:
    topic: Topic
    keep: int


class Commander(PrivilegedNode):

    NodeName = "commander"
    Namespace = namespace.core

    def __init__(self) -> None:
        super().__init__(self.NodeName, namespace=self.Namespace)
        self.__publisher: Dict[str, Topic] = {
            "coord": topic.raw_coord,
            "alert_stop": topic.manual_stop_alert,
            "pid_param": topic.pid_param,
            "chopper": topic.chopper_cmd,
            "spectra_meta": topic.spectra_meta,
            "qlook_meta": topic.qlook_meta,
            "sis_bias": topic.sis_bias_cmd,
            "lo_signal": topic.lo_signal_cmd,
            "attenuator": topic.attenuator_cmd,
        }
        self.publisher: Dict[str, Publisher] = {}

        _cfg_ant = config.antenna_command
        _altaz_offset = int(_cfg_ant.frequency * _cfg_ant.offset_sec + 1)
        self.__subscription: Dict[str, _SubscriptionCfg] = {
            "encoder": _SubscriptionCfg(topic.antenna_encoder, 1),
            "altaz": _SubscriptionCfg(topic.altaz_cmd, _altaz_offset),
            "speed": _SubscriptionCfg(topic.antenna_speed_cmd, 1),
            "chopper": _SubscriptionCfg(topic.chopper_status, 1),
            "antenna_control": _SubscriptionCfg(topic.antenna_control_status, 1),
            "sis_bias": _SubscriptionCfg(topic.sis_bias, 1),
            "lo_signal": _SubscriptionCfg(topic.lo_signal, 1),
            "thermometer": _SubscriptionCfg(topic.thermometer, 1),
            "attenuator": _SubscriptionCfg(topic.attenuator, 1),
        }
        self.subscription: Dict[str, Subscription] = {}
        self.client = {
            "record_path": service.record_path.client(self),
            "record_file": service.record_file.client(self),
        }
        self.__check_topic()

        self.parameters: Dict[str, ParameterList] = {}
        self.create_timer(1, self.__check_topic)

    def __callback(self, msg: Any, *, key: str, keep: int = 1) -> None:
        if key not in self.parameters:
            self.parameters[key] = ParameterList.new(keep, None)
        self.parameters[key].push(msg)

    def __check_topic(self) -> None:
        for k, v in self.__publisher.items():
            if k not in self.publisher:
                self.publisher[k] = v.publisher(self)
            if v.support_index:
                for _k, _v in v.get_children(self).items():
                    key = f"{k}.{_k}"
                    if key not in self.publisher:
                        self.publisher[key] = _v.publisher(self)

        for k, v in self.__subscription.items():
            if k not in self.subscription:
                callback = partial(self.__callback, key=k, keep=v.keep)
                self.subscription[k] = v.topic.subscription(self, callback)
            if v.topic.support_index:
                for _k, _v in v.topic.get_children(self).items():
                    key = f"{k}.{_k}"
                    if key not in self.subscription:
                        callback = partial(self.__callback, key=key, keep=v.keep)
                        self.subscription[key] = _v.subscription(self, callback)

    def get_message(
        self,
        key: str,
        *,
        time: Optional[Union[int, float]] = None,
        timeout_sec: Optional[Union[int, float]] = None,
        interp: bool = False,
    ) -> Dict[str, Any]:
        if time is None:
            time = pytime.time()
        start = pytime.monotonic()
        while (timeout_sec is None) or (pytime.monotonic() - start < timeout_sec):
            keys = [k for k in self.parameters if (k == key) or k.startswith(f"{key}.")]
            if all(self.parameters[k][-1] is None for k in keys):
                pytime.sleep(0.05)
                continue

            message = {}
            for k in keys:
                msgs = [x for x in self.parameters[k] if x is not None]
                if len(msgs) == 0:
                    message[k] = None
                msg_type = msgs[-1].__class__
                msg_fields = msg_type.get_fields_and_field_types().keys()
                if "time" not in msg_fields:
                    message[k] = msgs[-1]

                if interp:
                    interpolate = LinearInterp("time", msg_fields)
                    message[k] = interpolate(msg_type(time=time), msgs)
                else:
                    nearest, *_ = sorted(msgs, key=lambda x: abs(x.time - time))
                    message[k] = nearest
            return message[key] if set(message.keys()) == {key} else message

        raise NECSTTimeoutError(f"No message has been received on topic {key!r}")

    @require_privilege(escape_cmd=["?", "stop"])
    def antenna(
        self,
        cmd: Literal["stop", "point", "scan", "error", "?"],
        /,
        *,
        start: Optional[Tuple[Union[int, float], Union[int, float]]] = None,
        stop: Optional[Tuple[Union[int, float], Union[int, float]]] = None,
        target: Optional[Tuple[Union[int, float], Union[int, float], str]] = None,
        reference: Optional[Tuple[Union[int, float], Union[int, float], str]] = None,
        offset: Optional[Tuple[Union[int, float], Union[int, float], str]] = None,
        scan_frame: str = None,
        unit: Optional[str] = None,
        name: Optional[str] = None,
        wait: bool = True,
        speed: Optional[Union[int, float]] = None,
    ) -> None:
        """Control antenna direction and motion."""
        CMD = cmd.upper()
        if CMD == "STOP":
            msg = AlertMsg(critical=True, warning=True, target=[namespace.antenna])
            checker = ConditionChecker(5, reset_on_failure=True)
            now = pytime.time()
            current_speed = self.get_message("speed", time=now, timeout_sec=0.1)
            # TODO: Add timeout handler
            while not checker.check(
                (abs(current_speed.az) < 1e-5) and (abs(current_speed.el) < 1e-5)
            ):
                self.publisher["alert_stop"].publish(msg)
                current_speed = self.get_message("speed", time=now, timeout_sec=0.1)
                pytime.sleep(1 / config.antenna_command_frequency)

            msg = AlertMsg(critical=False, warning=False, target=[namespace.antenna])
            pytime.sleep(0.5)
            # Avoid time inconsistency between this lift of alert and the next command
            return self.publisher["alert_stop"].publish(msg)

        elif CMD == "POINT":
            kwargs = {}
            if name is not None:
                kwargs.update(name=name)
            elif target is not None:
                kwargs.update(
                    lon=[float(target[0])],
                    lat=[float(target[1])],
                    frame=target[2],
                    unit=unit,
                )
            elif reference is not None:
                kwargs.update(
                    lon=[float(reference[0])],
                    lat=[float(reference[1])],
                    frame=reference[2],
                    unit=unit,
                )
            else:
                raise ValueError("No valid target specified")

            if offset is not None:
                kwargs.update(
                    offset_lon=[float(offset[0])],
                    offset_lat=[float(offset[1])],
                    offset_frame=offset[2],
                    unit=unit,
                )
            msg = CoordCmdMsg(**kwargs)
            self.publisher["coord"].publish(msg)
            return self.wait("antenna") if wait else None

        elif CMD == "SCAN":
            point_kwargs = dict(wait=True)
            scan_kwargs = dict(speed=float(speed), unit=unit)
            if name is not None:
                self.logger.warning(
                    "Gentle acceleration before this scan mode isn't implemented yet"
                )
                point_kwargs.update(name=name)
                scan_kwargs.update(
                    name=name,
                    offset_lon=[float(start[0]), float(stop[0])],
                    offset_lat=[float(start[1]), float(stop[1])],
                    offset_frame=scan_frame,
                )
            elif reference is not None:
                self.logger.warning(
                    "Gentle acceleration before this scan mode isn't implemented yet"
                )
                point_kwargs.update(target=reference, unit=unit)
                scan_kwargs.update(
                    lon=[float(reference[0])],
                    lat=[float(reference[1])],
                    frame=reference[2],
                    offset_lon=[float(start[0]), float(stop[0])],
                    offset_lat=[float(start[1]), float(stop[1])],
                    offset_frame=scan_frame,
                )
            else:
                standby_lon, standby_lat = standby_position(
                    start=start, end=stop, unit=unit, margin=config.antenna_scan_margin
                )
                point_kwargs.update(
                    target=(standby_lon.value, standby_lat.value, scan_frame),
                    unit=unit,
                )
                scan_kwargs.update(
                    lon=[float(start[0]), float(stop[0])],
                    lat=[float(start[1]), float(stop[1])],
                    frame=scan_frame,
                )
            self.antenna("point", **point_kwargs)

            msg = CoordCmdMsg(**scan_kwargs)
            self.publisher["coord"].publish(msg)
            return self.wait("antenna", mode="control") if wait else None

        elif CMD == "ERROR":
            now = pytime.time()
            enc = self.get_message("encoder", time=now, timeout_sec=0.01)
            cmd = self.get_message("altaz", time=now, timeout_sec=0.01, interp=True)
            return enc.lon - cmd.lon, enc.lat - cmd.lat

        elif CMD in ["?"]:
            return self.get_message("encoder", timeout_sec=10)
        else:
            raise ValueError(f"Unknown command: {cmd!r}")

    @require_privilege(escape_cmd=["?"])
    def chopper(self, cmd: Literal["insert", "remove", "?"], /, *, wait: bool = True):
        """Control the position of ambient temperature radio absorber."""
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

    def wait(
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
            ERROR_GETTER = self.antenna
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
                    error_az, error_el = ERROR_GETTER("error")
                    error = (error_az**2 + error_el**2) ** 0.5
                    self.logger.debug(
                        f"Error = {error:9.6f} deg"
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
                    error_az, error_el = ERROR_GETTER("error")
                    error = (error_az**2 + error_el**2) ** 0.5
                    self.logger.debug(
                        f"Error = {error:9.6f} deg"
                        f" {'[OK]' if error < THRESHOLD else '[NG]'}",
                        throttle_duration_sec=0.1,
                    )

                    control_status = self.get_message(
                        CTRL_TOPIC, time=now, timeout_sec=0.01
                    )
                    if control_status.time > now:
                        pytime.sleep(control_status.time - now)
                    if checker.check(not control_status.controlled):
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
        Kp: Optional[Union[int, float]] = None,
        Ki: Optional[Union[int, float]] = None,
        Kd: Optional[Union[int, float]] = None,
        axis: Optional[Literal["az", "el"]] = None,
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
        position: Optional[str] = None,
        id: Optional[str] = None,
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
            self.logger.info(f"Recording at {name!r}")
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
            self.publisher["sis_bias"].publish(BiasMsg(voltage=float(mV), id=id))
        elif CMD == "?":
            return self.get_message("sis_bias", timeout_sec=10)
        else:
            raise ValueError(f"Unknown command: {cmd!r}")

    @require_privilege(escape_cmd=["?"])
    def attenuator(
        self,
        cmd: Literal["set", "?"],
        /,
        *,
        dB: Optional[Union[int, float]] = None,
        id: Optional[str] = None,
    ) -> None:
        CMD = cmd.upper()
        if CMD == "SET":
            msg = DeviceReading(value=float(dB), time=pytime.time(), id=id)
            self.publisher["attenuator"].publish(msg)
        elif CMD == "?":
            return self.get_message("attenuator", timeout_sec=10)
        else:
            raise ValueError(f"Unknown command: {cmd!r}")

    def thermometer(self, cmd: Literal["?"] = "?", /) -> None:
        CMD = cmd.upper()
        if CMD == "?":
            return self.get_message("thermometer", timeout_sec=10)
        else:
            raise ValueError(f"Unknown command: {cmd!r}")

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
    sis = sis_bias
