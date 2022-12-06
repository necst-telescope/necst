__all__ = ["HorizontalCoord"]

import queue
import time
from functools import partial
from typing import Tuple

from neclib.coordinates import DriveLimitChecker, PathFinder
from necst_msgs.msg import CoordCmdMsg, CoordMsg, TimedFloat64
from rclpy.node import Node

from ... import config, namespace, topic


class HorizontalCoord(Node):

    NodeName = "altaz_coord"
    Namespace = namespace.antenna

    def __init__(self) -> None:
        super().__init__(self.NodeName, namespace=self.Namespace)
        self.logger = self.get_logger()

        self.cmd = None
        self.enc_az = self.enc_el = None

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

        self.result_queue = queue.Queue()
        self.last_result = None

    def _update_cmd(self, msg: CoordCmdMsg) -> None:
        self.cmd = msg
        self.result_queue = queue.Queue()

    def _update_enc(self, msg: CoordMsg) -> None:
        if (msg.unit != "deg") or (msg.frame != "altaz"):
            self.logger.warning("Invalid encoder reading detected.")
            return
        self.enc_az = msg.lon
        self.enc_el = msg.lat

    def command_realtime(self) -> None:
        now = time.time()
        cmd = None
        if self.result_queue.empty() and (self.last_result is not None):
            cmd = self.last_result
        else:
            while not self.result_queue.empty():
                cmd = self.result_queue.get()
                if cmd[2] > now:
                    break
        print(f"Published altaz standby cmd: {cmd}")

        if cmd:
            msg = CoordMsg(
                lon=cmd[0], lat=cmd[1], time=cmd[2], unit="deg", frame="altaz"
            )
            self.publisher.publish(msg)
            print(f"Published altaz: {msg}")

    def convert(self) -> None:
        if self.cmd is None:
            return

        name_query = bool(self.cmd.name)
        obstime = self.cmd.time[0]
        if obstime == 0.0:
            obstime = None
        elif obstime < time.time() + config.antenna_command_offset_sec:
            self.logger.warning("Got outdated command, ignoring...")
            return

        if all(len(x) == 2 for x in [self.cmd.lon, self.cmd.lat]):  # SCAN
            az, el, t = self.finder.linear(
                start=(self.cmd.lon[0], self.cmd.lat[0]),
                end=(self.cmd.lon[1], self.cmd.lat[1]),
                frame=self.cmd.frame,
                speed=self.cmd.speed,
                unit=self.cmd.unit,
            )
        else:  # POINT
            if name_query:
                az, el, t = self.finder.get_altaz_by_name(self.cmd.name, obstime)
            else:
                az, el, t = self.finder.get_altaz(
                    self.cmd.lon[0],
                    self.cmd.lat[0],
                    self.cmd.frame,
                    unit=self.cmd.unit,
                    obstime=obstime,
                )

        az, el = self._validate_drive_range(az, el)
        for _az, _el, _t in zip(az, el, t):
            if any(x is None for x in [_az, _el, _t]):
                continue
            cmd = (float(_az.to_value("deg")), float(_el.to_value("deg")), _t)
            self.result_queue.put(cmd)
            print(f"result_queue.put({cmd})")

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
