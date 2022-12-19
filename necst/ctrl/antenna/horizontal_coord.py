__all__ = ["HorizontalCoord"]

import time
from functools import partial
from typing import Generator, Optional, Tuple

from neclib.coordinates import DriveLimitChecker, PathFinder
from necst_msgs.msg import ControlStatus, CoordCmdMsg, CoordMsg, TimedFloat64

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
        )  # TODO: Take weather data into account.
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
        self.status_publisher = topic.antenna_control_status.publisher(self)
        topic.raw_coord.subscription(self, self._update_cmd)
        topic.antenna_encoder.subscription(self, self._update_enc)

        callback_temp = partial(self.change_weather, "temperature")
        callback_pres = partial(self.change_weather, "pressure")
        callback_hum = partial(self.change_weather, "humidty")

        topic.weather_temperature.subscription(self, callback_temp)
        topic.weather_pressure.subscription(self, callback_pres)
        topic.weather_humidity.subscription(self, callback_hum)

        self.create_timer(1 / config.antenna_command_frequency, self.command_realtime)
        self.create_timer(1, self.convert)
        self.create_timer(1, self.telemetry)

        self.result_queue = []
        self.last_result = None

        self.executing_generator: Optional[Generator] = None

        self.gc = self.create_guard_condition(self._clear_cmd)

    def _clear_cmd(self) -> None:
        self.cmd = None
        self.last_result = None
        self.result_queue.clear()

    def telemetry(self) -> None:
        msg = ControlStatus(
            controlled=len(self.result_queue) == 0, remote=True, time=time.time()
        )
        self.status_publisher.publish(msg)

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
            self.logger.warning("Invalid encoder reading detected.")
            return
        self.enc_az = msg.lon
        self.enc_el = msg.lat
        self.enc_time = msg.time

    def command_realtime(self) -> None:
        if self.status.critical():
            self.logger.warning("Guard condition activated", throttle_duration_sec=1)
            # Avoid sudden resumption of telescope drive
            self.gc.trigger()
            return

        # No realtime-ness check is performed, just filter outdated commands out
        now = time.time()
        cmd = None
        if (len(self.result_queue) == 0) and (self.last_result is not None):
            cmd = self.last_result
        else:
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
        if self.executing_generator is not None:
            self.executing_generator.close()

        if all(len(x) == 2 for x in (msg.lon, msg.lat)):
            self.logger.info(f"Got LINEAR drive command: {msg}")
            start, end = (msg.lon[0], msg.lat[0]), (msg.lon[1], msg.lat[1])
            self.executing_generator = self.finder.linear_with_acceleration(
                start=start,
                end=end,
                frame=msg.frame,
                speed=msg.speed,
                unit=msg.unit,
                margin=config.antenna_scan_margin,
            )
        elif msg.name != "":
            self.logger.info(f"Got NAME drive command: {msg}")
            self.executing_generator = self.finder.track_by_name(msg.name)
        elif all(len(x) == 1 for x in (msg.lon, msg.lat)):
            self.logger.info(f"Got POINT drive command: {msg}")
            self.executing_generator = self.finder.track(
                lon=msg.lon[0], lat=msg.lat[0], frame=msg.frame, unit=msg.unit
            )
        else:
            raise ValueError(f"Cannot determine command type for {msg}")

    def convert(self) -> None:
        if (self.cmd is not None) and (self.enc_time < time.time() - 5):
            # Don't resume normal operation after communication with encoder lost for 5s
            self.logger.error(
                "Lost the communication with the encoder. Command to drive to "
                f"{self.cmd} has been discarded."
            )
            self.cmd = None
        if self.cmd is None:
            return

        try:
            az, el, t = next(self.executing_generator)
        except (StopIteration, TypeError):
            self.cmd = None
            return

        az, el = self._validate_drive_range(az, el)
        for _az, _el, _t in zip(az, el, t):
            if any(x is None for x in [_az, _el, _t]):
                continue
            cmd = (float(_az.to_value("deg")), float(_el.to_value("deg")), _t)

            # Remove chronologically duplicated/overlapping commands
            self.result_queue = list(filter(lambda x: x[2] < _t, self.result_queue))
            self.result_queue.append(cmd)

    def _validate_drive_range(self, az, el) -> Tuple:  # All values are Quantity.
        enc_az = 180 if self.enc_az is None else self.enc_az
        enc_el = 45 if self.enc_el is None else self.enc_el

        _az = self.optimizer["az"].optimize(enc_az, az.to_value("deg"), unit="deg")
        _el = self.optimizer["el"].optimize(enc_el, el.to_value("deg"), unit="deg")

        if (_az is not None) and (_el is not None):
            return _az, _el
        return [], []

    def change_weather(self, kind: str, msg: TimedFloat64) -> None:
        if kind == "temperature":
            self.finder.temperature = msg.data
        elif kind == "pressure":
            self.finder.pressure = msg.data
        else:
            self.finder.relative_humidity = msg.data
