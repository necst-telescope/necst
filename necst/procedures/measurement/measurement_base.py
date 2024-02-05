import os
import time
from abc import ABC, abstractmethod
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Generator, Optional, final

import rclpy
from neclib import NECSTAuthorityError, get_logger

from ...core import Commander


class Measurement(ABC):
    """Measurement runner.

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

    parameter_files = ("config.toml", "pointing_param.toml")

    def __init__(self, record_name: Optional[str] = None, /, **kwargs) -> None:
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
                if "rate" in self._kwargs.keys():
                    conv_rate = int(self._kwargs.pop("rate") * 10)
                    self.com.record("reduce", nth=conv_rate)
                # self.binning(self._kwargs.pop("ch"))
                self.com.metadata("set", position="", id="")
                self.com.record("start", name=self.record_name)
                self.record_parameter_files()
                rclpy.uninstall_signal_handlers()
                self.run(**self._kwargs)
            finally:
                self.com.record("stop")
                # self.binning(config.spectrometer.max_ch)  # set max channel number
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
    def run(self, *args, **kwargs) -> None:
        ...
