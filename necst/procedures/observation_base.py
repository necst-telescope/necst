import os
import time
from abc import ABC, abstractmethod
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any, Generator, Optional, Union, final

import rclpy
from neclib import NECSTAuthorityError, get_logger
from neclib.parameters import PointingError

from .. import config
from ..core import Commander


class Observation(ABC):

    observation_type: str
    target: Optional[str] = None

    parameter_files = ("config.toml", "pointing_param.toml")

    def __init__(self, *args, **kwargs) -> None:
        self.logger = get_logger(self.__class__.__name__)
        self._record_name: Optional[str] = None
        self.execute(*args, **kwargs)

    def execute(self, *args, **kwargs) -> None:
        with self.ros2env():
            self.start = time.monotonic()
            self.com = Commander()
            privileged = self.com.get_privilege()
            try:
                if not privileged:
                    raise NECSTAuthorityError("Couldn't acquire privilege")
                self.com.metadata("set", position="", id="")
                self.com.record("start", name=self.record_name)
                self.record_parameter_files()
                rclpy.uninstall_signal_handlers()
                self.run(*args, **kwargs)
            finally:
                self.com.record("stop")
                self.com.antenna("stop")
                self.com.quit_privilege()
                self.com.destroy_node()
                _observing_duration = (time.monotonic() - self.start) / 60
                self.logger.info(
                    f"Observation finished, took {_observing_duration:.2f} min."
                )
                self.logger.info(f"Record name: \033[1m{self.record_name!r}\033[0m")
                rclpy.install_signal_handlers()

    @contextmanager
    def ros2env(self) -> Generator[None, None, None]:
        should_shutdown = not rclpy.ok()
        if should_shutdown:
            rclpy.init()
        try:
            yield
        finally:
            if should_shutdown:
                rclpy.shutdown()

    @final
    @property
    def record_name(self) -> str:
        # TODO: Make configurable, and make target name available.
        if self._record_name is None:
            now = datetime.utcnow().strftime("%Y%m%d_%H%M%S_")
            target = "" if self.target is None else f"_{self.target}"
            self._record_name = f"necst_{now}{self.observation_type}{target}".lower()
        return self._record_name

    def record_parameter_files(self) -> None:
        root = os.environ.get("NECST_ROOT", Path.home() / ".necst")
        for filename in self.parameter_files:
            try:
                path = f"{str(root).rstrip('/')}/{filename.lstrip('/')}"
                self.com.record("file", name=path)
            except FileNotFoundError:
                self.logger.error(f"Failed to save parameter file {filename!r}")

    @abstractmethod
    def run(self, *args, **kwargs) -> None:
        ...

    def hot(self, integ_time: Union[int, float], id: Any) -> None:
        # TODO: Remove this workaround, by attaching control section ID to spectra
        # metadata command; if it's "", don't require tight control
        if not self.com.get_message("antenna_control").tight:
            enc = self.com.get_message("encoder")
            params = PointingError.from_file(config.antenna_pointing_parameter_path)
            az, el = params.apparent2refracted(az=enc.lon, el=enc.lat, unit="deg")
            self.com.antenna(
                "point", target=(az.value, el.value, "altaz"), unit="deg", wait=False
            )

        self.logger.info("Starting HOT...")
        self.com.chopper("insert")
        self.com.metadata("set", position="HOT", id=str(id))
        time.sleep(integ_time)
        self.com.metadata("set", position="", id=str(id))
        self.com.chopper("remove")
        self.logger.debug("Complete HOT")

    def sky(self, integ_time: Union[int, float], id: Any) -> None:
        self.logger.info("Starting SKY...")
        self.com.chopper("remove")
        self.com.metadata("set", position="SKY", id=str(id))
        time.sleep(integ_time)
        self.com.metadata("set", position="", id=str(id))
        self.logger.debug("Complete SKY")

    def off(self, integ_time: Union[int, float], id: Any) -> None:
        self.logger.info("Starting OFF...")
        self.com.chopper("remove")
        self.com.metadata("set", position="OFF", id=str(id))
        time.sleep(integ_time)
        self.com.metadata("set", position="", id=str(id))
        self.logger.debug("Complete OFF")

    def on(self, integ_time: Union[int, float], id: Any) -> None:
        self.logger.info("Starting ON...")
        self.com.chopper("remove")
        self.com.metadata("set", position="ON", id=str(id))
        time.sleep(integ_time)
        self.com.metadata("set", position="", id=str(id))
        self.logger.debug("Complete ON")

    def valid_frame(self, frame: str) -> str:
        conversion_table = {"j2000": "fk5", "b1950": "fk4"}
        return conversion_table.get(frame.lower(), frame)
