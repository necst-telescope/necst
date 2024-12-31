import os
import time
from abc import ABC, abstractmethod
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any, Generator, Optional, Union, final

import rclpy
from neclib import NECSTAuthorityError, get_logger
from neclib.coordinates import PointingError


from .. import config
from ..core import Commander


class Observation(ABC):
    r"""Observation runner.

    Parameters
    ----------
    record_name
        Record name. This will prefixed by auto-generated observation identifier
        ``necst_{start_datetime}_{observation_type}``.
    **kwargs
        Keyword arguments passed to :meth:`run`.

    Examples
    --------
    >>> obs = necst.procedures.Observation(...)
    >>> obs.execute()

    """

    observation_type: str
    target: Optional[str] = None

    def __init__(self, record_name: Optional[str] = None, /, **kwargs) -> None:
        try:
            self.telescope = os.environ.get("TELESCOPE")
            self.parameter_files = (
                f"{self.telescope}_config.toml",
                "pointing_param.toml",
                "device_setting.toml",
            )
        except KeyError:
            self.parameter_files = (
                "config.toml",
                "pointing_param.toml",
                "device_setting.toml",
            )
        self.logger = get_logger(self.__class__.__name__)
        self._record_suffix: Optional[str] = record_name
        self._record_qualname = None
        self._kwargs = kwargs
        self._start: Optional[float] = None

    def execute(self) -> None:
        self._start = time.time()
        with self.ros2env():
            self.com = Commander()
            privileged = self.com.get_privilege()
            try:
                if not privileged:
                    raise NECSTAuthorityError("Couldn't acquire privilege")
                if "save" in self._kwargs.keys():
                    savespec = self._kwargs.pop("save")
                    self.com.record("savespec", save=savespec)
                if "rate" in self._kwargs.keys():
                    conv_rate = int(self._kwargs.pop("rate") * 10)
                    self.com.record("reduce", nth=conv_rate)
                if "ch" in self._kwargs.keys():
                    self.binning(self._kwargs.pop("ch"))
                if "tp" in self._kwargs.keys():
                    tp_bool = self._kwargs.pop("tp")
                    self.com.record("tp_mode", tp_mode=tp_bool)
                self.com.metadata("set", position="", id="")
                self.com.record("start", name=self.record_name)
                self.record_parameter_files()
                rclpy.uninstall_signal_handlers()
                self.run(**self._kwargs)
            finally:
                self.com.record("stop")
                self.com.record("tp_mode", tp=False)
                self.com.record("savespec", save=True)
                self.com.antenna("stop")
                self.binning(config.spectrometer.max_ch)  # set max channel number
                self.com.quit_privilege()
                self.com.destroy_node()
                _observing_duration = (time.time() - self._start) / 60
                self.logger.info(
                    f"Observation finished, took {_observing_duration:.2f} min."
                )
                self.logger.info(f"Record name: \033[1m{self.record_name!r}\033[0m")
                rclpy.install_signal_handlers()

    @property
    def start_datetime(self) -> Optional[str]:
        if self._start is None:
            return None
        if not hasattr(self, "_start_datetime"):
            dt = datetime.fromtimestamp(self._start)
            self._start_datetime = dt.strftime(r"%Y%m%d_%H%M%S")
        return self._start_datetime

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
        if self._record_qualname is None:
            obstype = self.observation_type
            self._record_qualname = f"necst_{obstype}_{self.start_datetime}"
            if self._record_suffix:
                self._record_qualname += f"_{self._record_suffix}"
        return self._record_qualname.lower()

    def record_parameter_files(self) -> None:
        root = os.environ.get("NECST_ROOT", Path.home() / ".necst")
        for filename in self.parameter_files:
            try:
                path = f"{str(root).rstrip('/')}/{filename.lstrip('/')}"
                self.com.record("file", name=path)
            except FileNotFoundError:
                self.logger.error(f"Failed to save parameter file {filename!r}")

    @abstractmethod
    def run(self, *args, **kwargs) -> None: ...

    def hot(self, integ_time: Union[int, float], id: Any) -> None:
        # TODO: Remove this workaround, by attaching control section ID to spectra
        # metadata command; if it's "", don't require tight control
        # This will use SpectralMetadata.srv
        if not self.com.get_message("antenna_control").tight:
            enc = self.com.get_message("encoder")
            params = PointingError.from_file(config.antenna_pointing_parameter_path)
            az, el = params.apparent_to_refracted(az=enc.lon, el=enc.lat, unit="deg")
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

    def binning(self, ch):
        if ch is not None:
            if (ch & (ch - 1)) == 0:
                self.com.record("binning", ch=ch)
            else:
                raise ValueError(f"Input channel number {ch} is not power of 2.")
