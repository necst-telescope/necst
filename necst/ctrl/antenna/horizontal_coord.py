__all__ = ["HorizontalCoord"]

import time
from typing import Optional, Tuple

from neclib.coordinates import CoordinateGeneratorManager, DriveLimitChecker, PathFinder
from neclib.coordinates.paths import ControlContext
from necst_msgs.msg import Boolean, ControlStatus, CoordMsg, WeatherMsg
from necst_msgs.srv import CoordinateCommand

from ... import config, namespace, service, topic
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
        self.direct_mode = None

        self.finder = PathFinder(
            config.location, config.antenna_pointing_parameter_path
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
        topic.antenna_encoder.subscription(self, self._update_enc)
        topic.weather.subscription(self, self._update_weather)
        topic.antenna_cmd_transition.subscription(self, self.next)
        service.raw_coord.service(self, self._update_cmd)

        self.status_publisher = topic.antenna_control_status.publisher(self)

        self.create_timer(1 / config.antenna_command_frequency, self.command_realtime)
        self.create_timer(0.5, self.convert)

        self.result_queue = []
        self.tracking_ok = False
        self.executing_generator = CoordinateGeneratorManager()
        self.last_status = None

        self.gc = self.create_guard_condition(self._clear_cmd)

    def _clear_cmd(self) -> None:
        self.cmd = None
        self.result_queue.clear()

    def _update_cmd(
        self, request: CoordinateCommand.Request, response: CoordinateCommand.Response
    ) -> CoordinateCommand.Response:
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
        self.cmd = request
        self._parse_cmd(request)
        self.result_queue.clear()

        response.id = str(id(self.executing_generator.get()))
        return response

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
                if cmd[4] > now:
                    break

        if cmd:
            # print(cmd)
            msg = CoordMsg(
                lon=cmd[0],
                lat=cmd[1],
                dAz=cmd[2],
                dEl=cmd[3],
                time=cmd[4],
                unit="deg",
                frame="altaz",
            )
            self.publisher.publish(msg)

    def _parse_cmd(self, msg: CoordinateCommand.Request) -> None:
        target_coord = (msg.lon, msg.lat)
        offset_coord = (msg.offset_lon, msg.offset_lat)

        target_scan = all(len(x) == 2 for x in target_coord)
        offset_scan = all(len(x) == 2 for x in offset_coord)
        scan = target_scan or offset_scan
        named = msg.name != ""
        with_offset = any(len(x) != 0 for x in offset_coord)

        if msg.direct_mode:
            self.direct_mode = True
        else:
            self.direct_mode = False
        self.finder.direct_mode = self.direct_mode

        if (not scan) and (not named) and (not with_offset):
            self.logger.debug(f"Got POINT-TO-COORD command: {msg}")
            new_generator = self.finder.track(
                msg.lon[0],
                msg.lat[0],
                msg.frame,
                unit=msg.unit,
            )
        elif (not scan) and (not named) and with_offset:
            self.logger.debug(f"Got POINT-TO-COORD-WITH-OFFSET command: {msg}")
            new_generator = self.finder.track(
                msg.lon[0],
                msg.lat[0],
                msg.frame,
                offset=(msg.offset_lon[0], msg.offset_lat[0], msg.offset_frame),
                unit=msg.unit,
            )
        elif (not scan) and named and (not with_offset):
            self.logger.debug(f"Got POINT-TO-NAMED-TARGET command: {msg}")
            new_generator = self.finder.track(msg.name)
        elif (not scan) and named and with_offset:
            self.logger.debug(f"Got POINT-TO-NAMED-TARGET-WITH-OFFSET command: {msg}")
            new_generator = self.finder.track(
                msg.name,
                offset=(msg.offset_lon[0], msg.offset_lat[0], msg.offset_frame),
                unit=msg.unit,
            )
        elif target_scan and (not named):
            self.logger.debug(f"Got SCAN-IN-ABSOLUTE-COORD command: {msg}")
            new_generator = self.finder.linear(
                start=(msg.lon[0], msg.lat[0]),
                stop=(msg.lon[1], msg.lat[1]),
                scan_frame=msg.frame,
                speed=abs(msg.speed),
                unit=msg.unit,
                margin=msg.margin,
            )
        elif offset_scan and (not named):
            self.logger.debug(f"Got SCAN-IN-RELATIVE-COORD command: {msg}")
            new_generator = self.finder.linear(
                msg.lon,
                msg.lat,
                msg.frame,
                start=(msg.offset_lon[0], msg.offset_lat[0]),
                stop=(msg.offset_lon[1], msg.offset_lat[1]),
                scan_frame=msg.offset_frame,
                speed=abs(msg.speed),
                unit=msg.unit,
                margin=msg.margin,
            )
        elif offset_scan and named:
            self.logger.debug(f"Got SCAN-IN-RELATIVE-TO-NAMED-TARGET command: {msg}")
            new_generator = self.finder.linear(
                msg.name,
                start=(msg.offset_lon[0], msg.offset_lat[0]),
                stop=(msg.offset_lon[1], msg.offset_lat[1]),
                scan_frame=msg.offset_frame,
                speed=abs(msg.speed),
                unit=msg.unit,
                margin=msg.margin,
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
            coord = next(self.executing_generator)
            self.telemetry(coord.context)
        except (StopIteration, TypeError):
            self.cmd = None
            self.executing_generator.clear()
            return self.telemetry(None)

        print(coord.dAz, coord.dEl)
        az, el = self._validate_drive_range(coord.az, coord.el)
        for _az, _el, _dAz, _dEl, _t in zip(az, el, coord.dAz, coord.dEl, coord.time):
            if any(x is None for x in [_az, _el, _t]):
                continue
            # Remove chronologically duplicated/overlapping commands
            self.result_queue = list(filter(lambda x: x[2] < _t, self.result_queue))

            cmd = (
                float(_az.to_value("deg")),
                float(_el.to_value("deg")),
                float(_dAz.to_value("deg")),
                float(_dEl.to_value("deg")),
                _t,
            )
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
        if (self.last_status is not None) and (self.last_status.tight):
            # Updating weather data may cause jump in calculated coordinate, so the
            # parameter update is disabled while antenna is in tight control.
            return

        if self.direct_mode:
            self.finder.temperature = 0
            self.finder.pressure = 0
            self.finder.relative_humidity = 0

        else:
            self.finder.temperature = msg.temperature
            self.finder.pressure = msg.pressure
            self.finder.relative_humidity = msg.humidity

    def telemetry(self, status: Optional[ControlContext]) -> None:
        if status is None:
            msg = ControlStatus(
                controlled=False,
                tight=False,
                remote=True,
                id=str(id(self.executing_generator.get())),
                interrupt_ok=True,
                time=time.time() + config.antenna_command_offset_sec,
            )
        else:
            msg = ControlStatus(
                controlled=True,
                tight=status.tight,
                remote=True,
                id=str(id(self.executing_generator.get())),
                interrupt_ok=status.infinite and (not status.waypoint),
                time=status.start,
            )
        self.last_status = msg
        self.status_publisher.publish(msg)

    def next(self, msg: Boolean) -> None:
        self.executing_generator.will_send(True)
