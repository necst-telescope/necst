__all__ = ["HorizontalCoord"]

import time
from typing import Optional, Tuple

from neclib.coordinates import CoordinateGeneratorManager, DriveLimitChecker, PathFinder
from neclib.coordinates.path_finder import ControlStatus as LibControlStatus
from necst_msgs.msg import Boolean, ControlStatus, CoordCmdMsg, CoordMsg, WeatherMsg

from ... import config, namespace, topic
from ...core import AlertHandlerNode


class HorizontalCoord(AlertHandlerNode):

    NodeName = "altaz_coord"
    Namespace = namespace.antenna

    def __init__(self) -> None:
        super().__init__(self.NodeName, namespace=self.Namespace)
        self.logger = self.get_logger()

        self.cmd = None
        self.enc_az = self.enc_el = None
        self.enc_time = 0

        self.finder = PathFinder(
            config.location,
            config.antenna_pointing_parameter_path,
            obsfreq=config.observation_frequency,  # TODO: Make ``obsfreq`` changeable.
        )
        drive_limit = config.antenna_drive
        self.optimizer = {
            "az": DriveLimitChecker(
                drive_limit.critical_limit_az, drive_limit.warning_limit_az
            ),
            "el": DriveLimitChecker(
                drive_limit.critical_limit_el, drive_limit.warning_limit_el
            ),
        }

        self.publisher = topic.altaz_cmd.publisher(self)
        topic.raw_coord.subscription(self, self._update_cmd)
        topic.antenna_encoder.subscription(self, self._update_enc)
        topic.weather.subscription(self, self._update_weather)
        topic.antenna_cmd_transition.subscription(self, self.next)

        self.status_publisher = topic.antenna_control_status.publisher(self)

        self.create_timer(1 / config.antenna_command_frequency, self.command_realtime)
        self.create_timer(0.5, self.convert)

        self.result_queue = []
        self.tracking_ok = False
        self.executing_generator = CoordinateGeneratorManager()

        self.gc = self.create_guard_condition(self._clear_cmd)

    def _clear_cmd(self) -> None:
        self.cmd = None
        self.result_queue.clear()

    def _update_cmd(self, msg: CoordCmdMsg) -> None:
        """Update the target coordinate command.

        When new command has been received, conversion result will stop for a moment,
        since coordinate conversion for new coordinate cannot be performed immediately.
        This suspension won't affect PID control, as it checks the command time.

        Notes
        -----
        The conversion result will be cleared immediately after receiving the new
        command. This won't irregular drive, as this command doesn't contain detailed
        and frequent coordinate information. This is the case for scan command, as the
        command only contains start/stop position and scan speed.

        """
        self.cmd = msg
        self._parse_cmd(msg)
        self.result_queue.clear()

    def _update_enc(self, msg: CoordMsg) -> None:
        if (msg.unit != "deg") or (msg.frame != "altaz"):
            self.logger.debug("Invalid encoder reading detected.")
            return
        self.enc_az = msg.lon
        self.enc_el = msg.lat
        self.enc_time = msg.time

    def command_realtime(self) -> None:
        if self.status.critical():
            self.logger.warning("Guard condition activated", throttle_duration_sec=1)
            # Avoid sudden resumption of telescope drive
            return self.gc.trigger()

        # No realtime-ness check is performed, just filter outdated commands out
        now = time.time()
        cmd = None
        if len(self.result_queue) > 0:
            while len(self.result_queue) > 0:
                cmd = self.result_queue.pop(0)
                if cmd[2] > now:
                    break

        if cmd:
            msg = CoordMsg(
                lon=cmd[0], lat=cmd[1], time=cmd[2], unit="deg", frame="altaz"
            )
            self.publisher.publish(msg)

    def _parse_cmd(self, msg: CoordCmdMsg) -> None:
        target_coord = (msg.lon, msg.lat)
        offset_coord = (msg.offset_lon, msg.offset_lat)

        target_scan = all(len(x) == 2 for x in target_coord)
        offset_scan = all(len(x) == 2 for x in offset_coord)
        scan = target_scan or offset_scan
        named = msg.name != ""
        with_offset = any(len(x) != 0 for x in offset_coord)

        if (not scan) and (not named) and (not with_offset):
            self.logger.debug(f"Got POINT-TO-COORD command: {msg}")
            new_generator = self.finder.track(
                msg.lon[0], msg.lat[0], frame=msg.frame, unit=msg.unit
            )
        elif (not scan) and (not named) and with_offset:
            self.logger.debug(f"Got POINT-TO-COORD-WITH-OFFSET command: {msg}")
            new_generator = self.finder.track_with_offset(
                msg.lon[0],
                msg.lat[0],
                frame=msg.frame,
                offset=(msg.offset_lon[0], msg.offset_lat[0], msg.offset_frame),
                unit=msg.unit,
            )
        elif (not scan) and named and (not with_offset):
            self.logger.debug(f"Got POINT-TO-NAMED-TARGET command: {msg}")
            new_generator = self.finder.track_by_name(msg.name)
        elif (not scan) and named and with_offset:
            self.logger.debug(f"Got POINT-TO-NAMED-TARGET-WITH-OFFSET command: {msg}")
            new_generator = self.finder.track_by_name_with_offset(
                msg.name,
                offset=(msg.offset_lon[0], msg.offset_lat[0], msg.offset_frame),
                unit=msg.unit,
            )
        elif target_scan and (not named):
            self.logger.debug(f"Got SCAN-IN-ABSOLUTE-COORD command: {msg}")
            new_generator = self.finder.linear_with_acceleration(
                start=(msg.lon[0], msg.lat[0]),
                end=(msg.lon[1], msg.lat[1]),
                frame=msg.frame,
                speed=msg.speed,
                unit=msg.unit,
                margin=config.antenna_scan_margin,
            )
        elif offset_scan and (not named):
            self.logger.debug(f"Got SCAN-IN-RELATIVE-COORD command: {msg}")
            new_generator = self.finder.offset_linear(
                start=(msg.offset_lon[0], msg.offset_lat[0]),
                end=(msg.offset_lon[1], msg.offset_lat[1]),
                frame=msg.offset_frame,
                reference=(msg.lon, msg.lat, msg.frame),
                speed=msg.speed,
                unit=msg.unit,
            )
        elif offset_scan and named:
            self.logger.debug(f"Got SCAN-IN-RELATIVE-TO-NAMED-TARGET command: {msg}")
            new_generator = self.finder.offset_linear_by_name(
                start=(msg.offset_lon[0], msg.offset_lat[0]),
                end=(msg.offset_lon[1], msg.offset_lat[1]),
                frame=msg.offset_frame,
                name=msg.name,
                speed=msg.speed,
                unit=msg.unit,
            )
        else:
            raise ValueError(f"Cannot determine command type for {msg}")
        self.executing_generator.attach(new_generator)

    def convert(self) -> None:
        if (self.cmd is not None) and (self.enc_time < time.time() - 5):
            # Don't resume normal operation after communication with encoder lost for 5s
            self.logger.error(
                "Lost the communication with the encoder. Command to drive to "
                f"{self.cmd} has been discarded."
            )
            self.cmd = None
        if self.cmd is None:
            return self.telemetry(None)

        if (len(self.result_queue) > 1) and (
            self.result_queue[-1][2] > time.time() + config.antenna_command_offset_sec
        ):
            # This function will be called twice per 1s, to ensure no run-out of command
            # but it can cause overloading the data, so judge command update necessity.
            return

        try:
            az, el, t, status = next(self.executing_generator)
            self.telemetry(status)
        except (StopIteration, TypeError):
            self.cmd = None
            self.executing_generator.attach(None)
            return self.telemetry(None)

        az, el = self._validate_drive_range(az, el)
        for _az, _el, _t in zip(az, el, t):
            if any(x is None for x in [_az, _el, _t]):
                continue
            # Remove chronologically duplicated/overlapping commands
            self.result_queue = list(filter(lambda x: x[2] < _t, self.result_queue))

            cmd = (float(_az.to_value("deg")), float(_el.to_value("deg")), _t)
            self.result_queue.append(cmd)

    def _validate_drive_range(self, az, el) -> Tuple:  # All values are Quantity.
        enc_az = 180 if self.enc_az is None else self.enc_az
        enc_el = 45 if self.enc_el is None else self.enc_el

        _az = self.optimizer["az"].optimize(enc_az, az.to_value("deg"), unit="deg")
        _el = self.optimizer["el"].optimize(enc_el, el.to_value("deg"), unit="deg")

        if (_az is not None) and (_el is not None):
            return _az, _el
        return [], []

    def _update_weather(self, msg: WeatherMsg) -> None:
        self.finder.temperature = msg.temperature
        self.finder.pressure = msg.pressure
        self.finder.relative_humidity = msg.humidity

    def telemetry(self, status: Optional[LibControlStatus]) -> None:
        if status is None:
            msg = ControlStatus(
                controlled=False,
                tight=False,
                remote=True,
                time=time.time(),
            )
        else:
            msg = ControlStatus(
                controlled=status.controlled,
                tight=status.tight,
                remote=True,
                time=status.start,
            )
        self.status_publisher.publish(msg)

    def next(self, msg: Boolean) -> None:
        self.executing_generator.will_send(True)
