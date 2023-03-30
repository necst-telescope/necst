from typing import Union

from .observation_base import Observation

class HotMonitor(Observation):

    observation_type = "HotMonitor"

    #TODO: Add monitoring mode argument. -> Total power or Average(Reduce data).
    # This source code is reducing data in 1/10.

    def run(self, integ_time: Union[int, float], mode: str["ave", "total"]) -> None:
        
        self.com.record("reduce", nth = 10)
        self.logger.info(f"Starting {integ_time}s Hot Monitor")
        self.hot(integ_time, idx)