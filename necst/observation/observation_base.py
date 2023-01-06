import os
import time
from abc import ABC, abstractmethod
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any, Generator, Optional, Union, final

import rclpy
from neclib import get_logger

from ..core import Commander


class Observation(ABC):

    observation_type: str
    target: Optional[str] = None

    parameter_files = ("config.toml", "pointing_param.toml")

    def __init__(self, *args, **kwargs) -> None:
        self.logger = get_logger(self.__class__.__name__)
        self.execute(*args, **kwargs)

    def execute(self, *args, **kwargs) -> None:
        with self.ros2env():
            self.com = Commander()
            self.com.get_privilege()
            try:
                self.com.metadata("set", position="", id="")
                self.com.record("start", name=self.record_name)
                self.record_parameter_files()
                self.run(*args, **kwargs)
            finally:
                self.com.record("stop")
                self.com.quit_privilege()
                self.com.destroy_node()

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
        now = datetime.utcnow().strftime("%Y%m%d_%H%M%S_")
        target = "" if self.target is None else f"_{self.target}"
        return f"necst_{now}{self.observation_type}{target}"

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
        self.com.chopper("insert")
        self.com.metadata("set", position="HOT", id=str(id))
        time.sleep(integ_time)
        self.com.metadata("set", position="", id=str(id))
        self.com.chopper("remove")

    def sky(self, integ_time: Union[int, float], id: Any) -> None:
        self.com.metadata("set", position="SKY", id=str(id))
        time.sleep(integ_time)
        self.com.metadata("set", position="", id=str(id))
