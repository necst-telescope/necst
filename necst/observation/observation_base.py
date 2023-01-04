import traceback
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Optional, final

import rclpy
from neclib import get_logger

from ..core import Commander


class Observation(ABC):

    observation_type: str
    target: Optional[str] = None

    def __init__(self, *args, **kwargs) -> None:
        self.logger = get_logger(self.__class__.__name__)

        rclpy.init()
        self.com = Commander()
        self.com.get_privilege()
        try:
            self.com.record("start", self.record_name)
            self.run(*args, **kwargs)
        except Exception:
            self.logger.error(traceback.format_exc())
        finally:
            self.com.record("stop")
            self.com.quit_privilege()
            self.com.destroy_node()
            rclpy.shutdown()

    @final
    @property
    def record_name(self) -> str:
        now = datetime.utcnow().strftime("%Y%m%d_%H%M%S_")
        target = "" if self.target is None else f"_{self.target}"
        return f"necst_{now}_{self.observation_type}{target}"

    @abstractmethod
    def run(self, *args, **kwargs) -> None:
        ...
