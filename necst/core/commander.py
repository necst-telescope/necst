"""Interface to send command to any remotely controlled part of telescope."""

import time as pytime
from dataclasses import dataclass
from functools import partial
from typing import Any, Dict, Literal, Optional, Tuple, Union

from neclib.core import read
from neclib.utils import ConditionChecker, ParameterList
from necst_msgs.msg import (
    AlertMsg,
    Binning,
    Boolean,
    ChopperMsg,
    DeviceReading,
    LocalSignal,
    PIDMsg,
    Sampling,
    SISBias,
    Spectral,
)
from necst_msgs.srv import CoordinateCommand, File, RecordSrv, CCDCommand, DomeSync
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
    """Send command to drive any remotely controlled devices.

    The execution of some commands require privilege, to reject conflicting commands
    from different users.

    Examples
    --------
    You can stop the antenna without privilege, for safety reason
    >>> from necst.core import Commander
    >>> com = Commander()
    >>> com.antenna("stop")

    Driving the antenna to certain position requires privilege
    >>> from necst.core import Commander
    >>> com = Commander()
    >>> com.get_privilege()
    >>> com.antenna("point", target=(30, 45, "altaz"))

    """

    NodeName = "commander"
    Namespace = namespace.core

    def __init__(self) -> None:
        super().__init__(self.NodeName, namespace=self.Namespace)
        self.__publisher: Dict[str, Topic] = {
            "cmd_trans": topic.antenna_cmd_transition,
            "alert_stop": topic.manual_stop_alert,
            "pid_param": topic.pid_param,
            "chopper": topic.chopper_cmd,
            "spectra_meta": topic.spectra_meta,
            "qlook_meta": topic.qlook_meta,
            "sis_bias": topic.sis_bias_cmd,
            "lo_signal": topic.lo_signal_cmd,
            "attenuator": topic.attenuator_cmd,
            "spectra_smpl": topic.spectra_rec,
            "channel_binning": topic.channel_binning,
            "dome_alert_stop": topic.manual_stop_dome_alert,
        }
        self.publisher: Dict[str, Publisher] = {}

        self.__subscription: Dict[str, _SubscriptionCfg] = {
            "antenna_track": _SubscriptionCfg(topic.antenna_tracking, 1),
            "encoder": _SubscriptionCfg(topic.antenna_encoder, 1),
            "altaz": _SubscriptionCfg(topic.altaz_cmd, 1),
            "speed": _SubscriptionCfg(topic.antenna_speed_cmd, 1),
            "chopper": _SubscriptionCfg(topic.chopper_status, 1),
            "antenna_control": _SubscriptionCfg(topic.antenna_control_status, 1),
            "sis_bias": _SubscriptionCfg(topic.sis_bias, 1),
            "hemt_bias": _SubscriptionCfg(topic.hemt_bias, 1),
            "lo_signal": _SubscriptionCfg(topic.lo_signal, 1),
            "thermometer": _SubscriptionCfg(topic.thermometer, 1),
            "attenuator": _SubscriptionCfg(topic.attenuator, 1),
            "dome_track": _SubscriptionCfg(topic.dome_tracking, 1),
            "dome_encoder": _SubscriptionCfg(topic.dome_encoder, 1),
            "dome_speed": _SubscriptionCfg(topic.dome_speed_cmd, 1),
        }
        self.subscription: Dict[str, Subscription] = {}
        self.client = {
            "record_path": service.record_path.client(self),
            "record_file": service.record_file.client(self),
            "raw_coord": service.raw_coord.client(self),
            "dome_coord": service.dome_coord.client(self),
            "ccd_cmd": service.ccd_cmd.client(self),
            "dome_sync": service.dome_sync.client(self),
            "dome_pid_sync": service.dome_pid_sync.client(self),
        }

        self.parameters: Dict[str, ParameterList] = {}
        self.create_timer(1, self.__check_topic)

        self.__check_topic()

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
    ) -> Dict[str, Any]:
        """Get the latest message on the topic."""
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

                nearest, *_ = sorted(msgs, key=lambda x: abs(x.time - time))
                message[k] = nearest
            return message[key] if set(message.keys()) == {key} else message

        raise NECSTTimeoutError(f"No message has been received on topic {key!r}")

    @require_privilege(escape_cmd=["?", "stop", "error"])
    def antenna(
        self,
        cmd: Literal["stop", "point", "scan", "error", "?"],
        /,
        *,
        target: Optional[Tuple[Union[int, float], Union[int, float], str]] = None,
        start: Optional[Tuple[Union[int, float], Union[int, float]]] = None,
        stop: Optional[Tuple[Union[int, float], Union[int, float]]] = None,
        reference: Optional[Tuple[Union[int, float], Union[int, float], str]] = None,
        offset: Optional[Tuple[Union[int, float], Union[int, float], str]] = None,
        scan_frame: str = None,
        unit: Optional[str] = None,
        name: Optional[str] = None,
        wait: bool = True,
        speed: Optional[Union[int, float]] = None,
        direct_mode: bool = False,
    ) -> None:
        """Control antenna direction and motion.

        Parameters
        ----------
        cmd
            Command to be sent to the antenna.
        target
            Position (longitude, latitude, coordinate frame) to point the antenna to.
        start
            Start position (longitude, latitude) of the scan in the ``scan_frame``.
        stop
            Stop position (longitude, latitude) of the scan in the ``scan_frame``.
        reference
            Reference position (longitude, latitude, coordinate frame) of scan drive.
        offset
            Offset amount (longitude, latitude, coordinate frame) to add to command
            coordinate.
        scan_frame
            Coordinate frame the scan will be performed in.
        unit
            Unit of the all the angular value supplied as arguments.
        name
            Name of the target to point the antenna to.
        wait
            Whether to wait for the command to be completed.
        speed
            Speed of the scan in ``unit``/s.

        Notes
        -----
        .. list-table:: Allowed argument combinations
           :header-rows: 1

           * - ``cmd``
             - arguments
           * - ``stop``
             - None (all arguments except ``cmd`` are ignored)
           * - ``point``
             - ``target`` and ``unit``
           * -
             - ``target``, ``offset`` and ``unit``
           * -
             - ``name``
           * -
             - ``name``, ``offset`` and ``unit``
           * - ``scan``
             - ``start``, ``stop``, ``reference``, ``scan_frame``, ``unit`` and
               ``speed``
           * -
             - ``start``, ``stop``, ``reference``, ``offset``, ``scan_frame``, ``unit``
               and ``speed``
           * -
             - ``start``, ``stop``, ``name``, ``scan_frame``, ``unit`` and ``speed``
           * -
             - ``start``, ``stop``, ``name``, ``offset``, ``scan_frame``, ``unit`` and
               ``speed``
           * - ``error``
             - None (all arguments except ``cmd`` are ignored)

        Examples
        --------
        Stop the antenna immediately

        >>> com.antenna("stop")

        The following also stops the antenna immediately. Arguments are ignored.

        >>> com.antenna("stop", wait=False)

        Drive to certain position (Az., El.) = (30, 45)deg

        >>> com.antenna("point", target=(30, 45, "altaz"), unit="deg")

        Drive to certain position and track the target (R.A., Dec.) = (0, 45)deg

        >>> com.antenna("point", target=(0, 45, "fk5"), unit="deg")

        Send command to drive to (Az., El.) = (30, 45)deg but don't wait for the command
        to complete

        >>> com.antenna("point", target=(30, 45, "azel"), unit="deg", wait=False)

        Point to a position defined as "0.25deg east (negative azimuth) of center of the
        moon"

        >>> com.antenna("point", offset=(-0.25, 0, "altaz"), unit="deg", name="moon")

        Scan the moon in the azimuth direction, starting from 0.5deg to the east from
        the target, at speed of 600 arcsec/s

        >>> com.antenna(
        ...     "scan", start=(-0.5, 0), stop=(0.5, 0), name="moon",
        ...     scan_frame="altaz", speed=1/6, unit="deg"
        ... )

        Scan the target (RA, Dec.) = (0, 45)deg in constant galactic longitude, at speed
        150 arcsec/s

        >>> com.antenna(
        ...     "scan", start=(0, 1), stop=(0, -1), target=(0, 45, "radec"),
        ...     scan_frame="galactic", speed=1/150, unit="deg"
        ... )

        Get current error between the commanded and actual antenna position

        >>> com.antenna("error")

        """
        CMD = cmd.upper()
        if CMD == "STOP":
            msg = AlertMsg(critical=True, warning=True, target=[namespace.antenna])
            checker = ConditionChecker(5, reset_on_failure=True)
            now = pytime.time()
            current_speed = self.get_message("dome_speed", time=now, timeout_sec=0.1)
            # TODO: Add timeout handler
            while not checker.check(
                (abs(current_speed.az) < 1e-5) and (abs(current_speed.el) < 1e-5)
            ):
                self.publisher["dome_alert"].publish(msg)
                current_speed = self.get_message("speed", time=now, timeout_sec=0.1)
                pytime.sleep(1 / config.antenna_command_frequency)

            msg = AlertMsg(critical=False, warning=False, target=[namespace.antenna])
            self.publisher["alert_stop"].publish(msg)

            # Ensure the next command is executed after the lift of alert
            return pytime.sleep(0.5)

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
                    direct_mode=direct_mode,
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
            req = CoordinateCommand.Request(**kwargs)
            res = self._send_request(req, self.client["raw_coord"])
            if wait:
                self.wait("antenna")
            return res.id

        elif CMD == "SCAN":
            scan_kwargs = dict(speed=float(speed), unit=unit)
            if name is not None:
                scan_kwargs.update(
                    name=name,
                    offset_lon=[float(start[0]), float(stop[0])],
                    offset_lat=[float(start[1]), float(stop[1])],
                    offset_frame=scan_frame,
                )
            elif (reference is not None) or (target is not None):
                given_as = reference if target is None else target
                scan_kwargs.update(
                    lon=[float(given_as[0])],
                    lat=[float(given_as[1])],
                    frame=given_as[2],
                    offset_lon=[float(start[0]), float(stop[0])],
                    offset_lat=[float(start[1]), float(stop[1])],
                    offset_frame=scan_frame,
                )
            else:
                scan_kwargs.update(
                    lon=[float(start[0]), float(stop[0])],
                    lat=[float(start[1]), float(stop[1])],
                    frame=scan_frame,
                )

            req = CoordinateCommand.Request(**scan_kwargs)
            res = self._send_request(req, self.client["raw_coord"])
            self.wait("antenna")
            self.publisher["cmd_trans"].publish(Boolean(data=True, time=pytime.time()))
            if wait:
                self.wait("antenna", mode="control", id=res.id)
            return res.id

        elif CMD == "ERROR":
            now = pytime.time()
            return self.get_message("antenna_track", time=now, timeout_sec=0.01)

        elif CMD in ["?"]:
            return self.get_message("encoder", timeout_sec=10)
        else:
            raise ValueError(f"Unknown command: {cmd!r}")

    @require_privilege(escape_cmd=["?", "stop", "error"])
    def dome(
        self,
        cmd: Literal["point", "sync", "stop", "?"],
        /,
        *,
        target: Optional[Tuple[Union[int, float], Union[int, float], str]] = None,
        unit: Optional[str] = None,
        dome_sync: bool = False,
        direct_mode: bool = False,
        wait: bool = True,
    ) -> None:

        CMD = cmd.upper()
        if CMD == "POINT":
            kwargs = {}
            kwargs.update(
                lon=[float(target[0])],
                lat=[45.0],
                frame=target[2],
                unit=unit,
                direct_mode=direct_mode,
            )
            req = CoordinateCommand.Request(**kwargs)
            res = self._send_request(req, self.client["dome_coord"])
            if wait:
                self.wait("dome")
            return res.id

        elif CMD == "SYNC":
            if dome_sync:
                enc = self.get_message("dome_encoder", timeout_sec=10)
                az_now = enc.lon
                print(f"az: {az_now}")
                kwargs = {}
                kwargs.update(
                    lon=[float(az_now)],
                    lat=[45.0],
                    frame="altaz",
                    unit="deg",
                    direct_mode=direct_mode,
                )
                req = CoordinateCommand.Request(**kwargs)
                res = self._send_request(req, self.client["dome_coord"])
                self.wait("dome")
                pytime.sleep(0.5)
            req = DomeSync.Request(dome_sync=dome_sync)
            res = self._send_request(req, self.client["dome_sync"])
            print(f"{res.check} DomeController Synced")
            if res.check:
                req = DomeSync.Request(dome_sync=dome_sync)
                res = self._send_request(req, self.client["dome_pid_sync"])
                print(f"{res.check} DomePIDController Synced")
            return res.check
        elif CMD == "STOP":
            msg = AlertMsg(critical=True, warning=True, target=[namespace.dome])
            checker = ConditionChecker(5, reset_on_failure=True)
            now = pytime.time()
            current_speed = self.get_message("dome_speed", time=now, timeout_sec=0.1)
            # TODO: Add timeout handler
            while not checker.check(not (current_speed.speed == "stop")):
                self.publisher["dome_alert_stop"].publish(msg)
                current_speed = self.get_message(
                    "dome_speed", time=now, timeout_sec=0.1
                )
                pytime.sleep(1 / config.dome_command_frequency)

            msg = AlertMsg(critical=False, warning=False, target=[namespace.dome])
            self.publisher["dome_alert_stop"].publish(msg)
            # Ensure the next command is executed after the lift of alert
            return pytime.sleep(0.5)
        elif CMD == "ERROR":
            now = pytime.time()
            return self.get_message("dome_track", time=now, timeout_sec=0.01)
        elif CMD in ["?"]:
            return self.get_message("dome_encoder", timeout_sec=10)

    @require_privilege(escape_cmd=["?"])
    def ccd(
        self,
        cmd: Literal["capture", "?"],
        /,
        *,
        name: str = "",
    ) -> None:
        """Control the ccd.

        Parameters
        ----------
        cmd : {"capture", "?"}
            Command to be sent to the ccd.

        Examples
        --------
        Capture the photo

        >>> com.ccd("capture", name="/home/pi/data/photo.JPG")

        """
        CMD = cmd.upper()
        if CMD == "CAPTURE":
            req = CCDCommand.Request(capture=True, savepath=name)
            res = self._send_request(req, self.client["ccd_cmd"])
            return res.captured
        elif CMD == "?":
            raise NotImplementedError(f"Command {cmd!r} is not implemented yet.")
        else:
            raise ValueError(f"Unknown command: {cmd!r}")

    @require_privilege(escape_cmd=["?"])
    def chopper(self, cmd: Literal["insert", "remove", "?"], /, *, wait: bool = True):
        """Control the position of ambient temperature radio absorber.

        Parameters
        ----------
        cmd : {"insert", "remove", "?"}
            Command to be sent to the chopper.
        wait : bool, optional
            If True, wait until the chopper has been moved to the target position.
            The default is True.

        Examples
        --------
        Insert the absorber

        >>> com.chopper("insert")

        Remove the absorber but don't wait until it has been removed

        >>> com.chopper("remove", wait=False)

        """
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
        id: Optional[str] = None,
        timeout_sec: Optional[Union[int, float]] = None,
        dome_sync: bool = False,
    ) -> None:
        """Wait until the motion has been converged."""
        TARGET, MODE = target.upper(), mode.upper()

        if TARGET == "ANTENNA":
            ERROR_GETTER = self.antenna
            CTRL_TOPIC = "antenna_control"
            WAIT_DURATION = config.antenna_command_offset_sec
        elif TARGET == "DOME":
            ERROR_GETTER = self.dome
            WAIT_DURATION = config.dome_command_offset_sec
        else:
            raise ValueError(f"Unknown target: {target!r}")

        # Drive should have delay of offset duration
        pytime.sleep(WAIT_DURATION)

        start = pytime.monotonic()
        checker = ConditionChecker(10, reset_on_failure=True)

        if MODE == "ERROR":
            while (timeout_sec is None) or (pytime.monotonic() - start < timeout_sec):
                try:
                    error = ERROR_GETTER("error")
                    self.logger.debug(
                        f"Error={error.error:9.5f}deg [{'OK' if error.ok else 'NG'}]",
                        throttle_duration_sec=0.3,
                    )
                    if checker.check(error.ok):
                        self.logger.debug("Tracking OK")
                        return
                except NECSTTimeoutError:
                    pass
                pytime.sleep(0.05)
        elif MODE == "CONTROL":
            experienced = False
            while (timeout_sec is None) or (pytime.monotonic() - start < timeout_sec):
                now = pytime.time()
                try:
                    error = ERROR_GETTER("error")
                    self.logger.debug(
                        f"Error={error.error:9.5f}deg [{'OK' if error.ok else 'NG'}]",
                        throttle_duration_sec=0.3,
                    )

                    ctrl = self.get_message(CTRL_TOPIC, timeout_sec=0.01)
                    if ctrl.id == id:
                        experienced = True
                    finished = experienced and (ctrl.id != id)
                    appendix = ctrl.interrupt_ok and (ctrl.id == id)
                    if checker.check(finished or appendix):
                        if ctrl.time > now:
                            pytime.sleep(ctrl.time - now)
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
        """Change PID parameters.

        Parameters
        ----------
        cmd
            Command to execute.
        Kp
            Proportional gain.
        Ki
            Integral gain.
        Kd
            Derivative gain.
        axis
            Axis to change the parameter.

        Examples
        --------
        Change the PID parameters of the azimuth axis to (0.1, 0.1, 0.1)

        >>> com.pid_parameter("set", Kp=0.1, Ki=0.1, Kd=0.1, axis="az")

        """
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
        intercept: bool = True,
        optical_data: Optional[list] = [],
        # require_track: bool = True,
        # section_id: Optional[str] = None,
    ) -> None:
        """Set metadata of the spectral data.

        Parameters
        ----------
        cmd
            Command to execute.
        position
            Type of the current observation, may be "SKY", "ON", "OFF", etc. Should be
            less than or equal to 8 characters.
        id
            ID of the current observation, for sequential control. Should be less than
            or equal to 16 characters.
        time
            Time to change the metadata, in UNIX time.
        intercept
            If True, the change will be done immediately. Otherwise, the change will be
            done after the current control section is finished.
        Examples
        --------
        Set the metadata to (position="ON", id="scan01") immediately after current
        control section is finished

        >>> com.metadata("set", position="ON", id="scan01")

        Set the metadata to (position="SKY", id="calib01"), without waiting for the
        current control section to finish

        >>> com.metadata("set", position="SKY", id="calib01", intercept=False)

        Set the metadata to (position="OFF", id="scan02") at 2021-01-01 00:00:00

        >>> com.metadata(
        ...     "set", position="OFF", id="scan02", time=1609459200, intercept=False)

        """
        CMD = cmd.upper()
        if CMD == "SET":
            if not intercept:
                self.antenna("stop")
                while self.get_message("antenna_control").tight:
                    pytime.sleep(0.05)
            time = pytime.time() if time is None else time
            msg = Spectral(data=optical_data, position=position, id=str(id), time=time)
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
        """Configure the quick look mode.

        Parameters
        ----------
        mode
            Mode to configure.
        range
            Upper and lower limit of ``mode`` to show.
        integ
            Integration time in second.

        Examples
        --------
        Configure the quick look to show the spectrometer channel of (24000, 25000)
        with 1 second integration time

        >>> com.quick_look("ch", (24000, 25000), integ=1)

        """
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
        cmd: Literal["start", "stop", "file", "reduce", "binning", "?"],
        /,
        *,
        name: str = "",
        content: Optional[str] = None,
        nth: Optional[int] = None,
        ch: Optional[int] = None,
    ) -> None:
        """Control the recording.

        Parameters
        ----------
        cmd
            Command to execute.
        name
            Name of the file to record when ``cmd`` is ``file``. Otherwise, name of the
            directory to save the record.
        content
            Content of the file to record.
        nth
            Every $n$th spectral data will be recorded.
        ch
            Number of spectral channels

        Examples
        --------
        Start recording at ``otf_2021-01-01`` (the directory will be created under the
        path specified by environment variable ``NECST_RECORD_ROOT``)

        >>> com.record("start", name="otf_2021-01-01")

        Stop recording

        >>> com.record("stop")

        Record the file ``test.txt`` with the content ``Hello, world!``

        >>> com.record("file", name="test.txt", content="Hello, world!")

        Record the local file ``/home/user/test.txt``

        >>> com.record("file", name="/home/user/test.txt")

        Record the remote file ``http://192.168.xx.xx:/files/test.txt``

        >>> com.record("file", name="http://192.168.xx.xx:/files/test.txt")

        Reduce the sampling rate to 1/10

        >>> com.record("reduce", nth=10)

        Change the number of spectral channels

        >>> com.record("binning", ch=8192)

        """
        CMD = cmd.upper()
        if CMD == "START":
            recording = False
            while not recording:
                req = RecordSrv.Request(name=name.lstrip("/"), stop=False)
                res = self._send_request(req, self.client["record_path"])
                recording = res.recording
            self.logger.info(f"Recording at {name!r}")
            return
        elif CMD == "STOP":
            recording = True
            while recording:
                req = RecordSrv.Request(name=name, stop=True)
                res = self._send_request(req, self.client["record_path"])
                recording = res.recording
            return
        elif CMD == "FILE":
            if content is None:
                content = read(name)
            req = File.Request(data=str(content), path=name)
            return self._send_request(req, self.client["record_file"])
        elif CMD == "REDUCE":
            msg = Sampling(nth=nth)
            return self.publisher["spectra_smpl"].publish(msg)
        elif CMD == "BINNING":
            msg = Binning(ch=ch)
            if ch > 100:
                self.quick_look(
                    "ch", range=(0, 100), integ=1
                )  # reset to default values
            else:
                self.quick_look("ch", range=(0, ch), integ=1)
            return self.publisher["channel_binning"].publish(msg)
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
        """Control the SIS bias voltage.

        Parameters
        ----------
        cmd
            Command to execute.
        mV
            Bias voltage in mV.
        id
            Device identifier, may be defined in the configuration file.

        Examples
        --------
        Set the SIS bias voltage to 100 mV on the device ``LSB``

        >>> com.sis_bias("set", mV=100, id="LSB")

        Read the SIS bias voltage on the device ``USB``

        >>> com.sis_bias("?", id="USB")

        """
        CMD = cmd.upper()
        if CMD == "SET":
            self.publisher["sis_bias"].publish(SISBias(voltage=float(mV), id=id))
        elif CMD == "?":
            return self.get_message("sis_bias", timeout_sec=10)
        else:
            raise ValueError(f"Unknown command: {cmd!r}")

    @require_privilege(escape_cmd=["?"])
    def hemt_bias(
        self,
        cmd: Literal["?"],
        /,
        *,
        id: Optional[str] = None,
    ) -> None:
        """Read the HEMT bias voltage.

        Parameters
        ----------
        cmd
            Command to execute.

        id
            Device identifier, may be defined in the configuration file.

        Examples
        --------
        Read the HEMT bias voltage on the device ``USB``
        >>> com.hemt_bias("?", id="USB")

        """
        CMD = cmd.upper()

        if CMD == "?":
            return self.get_message("hemt_bias", timeout_sec=10)
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
        """Control the attenuator.

        Parameters
        ----------
        cmd
            Command to execute.
        dB
            Loss in dB.
        id
            Device identifier, may be defined in the configuration file.

        Examples
        --------
        Set the loss to 10 dB on the device ``LSB``

        >>> com.attenuator("set", dB=10, id="LSB")

        Read the loss on the device ``USB``

        >>> com.attenuator("?", id="USB")

        """
        CMD = cmd.upper()
        if CMD == "SET":
            msg = DeviceReading(value=float(dB), time=pytime.time(), id=id)
            self.publisher["attenuator"].publish(msg)
        elif CMD == "?":
            return self.get_message("attenuator", timeout_sec=10)
        else:
            raise ValueError(f"Unknown command: {cmd!r}")

    def thermometer(self, cmd: Literal["?"] = "?", /) -> None:
        """Get the thermometer reading.

        Parameters
        ----------
        cmd
            Command to execute.

        Examples
        --------
        Get the thermometer reading

        >>> com.thermometer("?")

        """
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
        id: Optional[str] = None,
    ) -> None:
        """Control the signal generator.

        Parameters
        ----------
        cmd
            Command to execute.
        GHz
            Frequency in GHz.
        dBm
            Power in dBm.
        id
            Device identifier, may be defined in the configuration file.

        Examples
        --------
        Set the frequency to 100 GHz and the power to 10 dBm on the device ``LSB2nd``

        >>> com.signal_generator("set", GHz=100, dBm=10, id="LSB2nd")

        Read the frequency and the power on the device ``1st``

        >>> com.signal_generator("?", id="1st")

        """
        CMD = cmd.upper()
        if CMD == "SET":
            msg = LocalSignal(freq=float(GHz), power=float(dBm), id=id)
            self.publisher["local_signal"].publish(msg)
        elif CMD == "?":
            return self.get_message("lo_signal", timeout_sec=10)
        else:
            raise ValueError(f"Unknown command: {cmd!r}")

    sg = signal_generator
    """Alias of :meth:`signal_generator`."""
    patt = attenuator
    """Alias of :meth:`attenuator`."""
    qlook = quick_look
    """Alias of :meth:`quick_look`."""
    pid = pid_parameter
    """Alias of :meth:`pid_parameter`."""
    sis = sis_bias
    """Alias of :meth:`sis_bias`."""
    hemt = hemt_bias
    """Alias of :meth:`hemt_bias`."""
