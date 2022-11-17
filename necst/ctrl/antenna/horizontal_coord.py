__all__ = ["HorizontalCoord"]

import queue
import time
from typing import Tuple
from functools import partial

from neclib.coordinates import CoordCalculator, DriveLimitChecker
from rclpy.node import Node

from necst import config, namespace, qos
from necst_msgs.msg import CoordMsg, TimedFloat64


class HorizontalCoord(Node):

    NodeName = "altaz_coord"
    Namespace = namespace.antenna

    def __init__(self) -> None:
        super().__init__(self.NodeName, namespace=self.Namespace)
        self.logger = self.get_logger()

        self.cmd = None
        self.enc_az = self.enc_el = None

        self.calculator = CoordCalculator(
            config.location, config.antenna_pointing_parameter_path
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

        self.publisher = self.create_publisher(CoordMsg, "altaz", qos.realtime)
        self.create_subscription(CoordMsg, "raw_coord", self._update_cmd, qos.reliable)
        self.create_subscription(CoordMsg, "encoder", self._update_enc, qos.realtime)

        callback_temp = partial(self.change_weather, "temperature")
        callback_pres = partial(self.change_weather, "pressuer")
        callback_hum = partial(self.change_weather, "humidty")

        self.create_subscription(
            TimedFloat64, f"{namespace.ctrl}/temperature", callback_temp, qos.realtime
        )
        self.create_subscription(
            TimedFloat64, f"{namespace.ctrl}/pressuer", callback_pres, qos.realtime
        )
        self.create_subscription(
            TimedFloat64, f"{namespace.ctrl}/humidity", callback_hum, qos.realtime
        )

        self.create_timer(1 / config.antenna_command_frequency, self.command_realtime)
        self.create_timer(1, self.convert)

        self.result_queue = queue.Queue()
        self.last_result = None

    def _update_cmd(self, msg: CoordMsg) -> None:
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

        if cmd:
            msg = CoordMsg(
                lon=cmd[0], lat=cmd[1], time=cmd[2], unit="deg", frame="altaz"
            )
            self.publisher.publish(msg)

    def convert(self) -> None:
        if self.cmd is None:
            return

        name_query = bool(self.cmd.name)
        obstime = self.cmd.time
        if obstime == 0.0:
            obstime = None
        elif obstime < time.time() + config.antenna_command_offset_sec:
            self.logger.warning("Got outdated command, ignoring...")
            return

        if name_query:
            az, el, t = self.calculator.get_altaz_by_name(self.cmd.name, obstime)
        else:
            az, el, t = self.calculator.get_altaz(
                self.cmd.lon,
                self.cmd.lat,
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
            self.calculator.temperature = msg.data
        elif kind == "pressuer":
            self.calculator.pressure = msg.data
        else:
            self.calculator.relative_humidity = msg.data
