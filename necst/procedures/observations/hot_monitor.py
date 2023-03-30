from typing import Union

from .observation_base import Observation


class HotMonitor(Observation):

    observation_type = "HotMonitor"

    def run(self, integ_time: Union[int, float]) -> None:
        self.logger.info(f"Starting {integ_time}s Hot Monitor")
        self.hot(integ_time, idx)