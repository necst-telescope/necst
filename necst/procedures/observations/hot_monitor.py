from typing import Union

from .observation_base import Observation


class HotMonitor(Observation):

    observation_type = "HotMonitor"

    # TODO: Add monitoring mode argument. -> Total power or Average(Reduce data).
    # This source code is reducing data in 1/10.

    def run(self, integ_time: Union[int, float]) -> None:
        self.com.record("reduce", nth=10)
        self.logger.info(f"Starting {integ_time}hours Hot Monitor")
        self.integ_sec = integ_time * 3600
        self.hot(self.integ_sec, f"{integ_time}h")
