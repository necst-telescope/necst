from abc import ABC, abstractmethod
from contextlib import contextmanager
from datetime import datetime
from typing import Generator, Optional, final

import rclpy
from neclib import get_logger

from ..core import Commander


class Observation(ABC):

    observation_type: str
    target: Optional[str] = None

    def __init__(self, *args, **kwargs) -> None:
        self.logger = get_logger(self.__class__.__name__)

        with self.ros2env():
            self.com = Commander()
            self.com.get_privilege()
            try:
                self.com.record("start", name=self.record_name)
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

    @abstractmethod
    def run(self, *args, **kwargs) -> None:
        ...
