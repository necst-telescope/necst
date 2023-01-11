from typing import Union

from .observation_base import Observation


class RSky(Observation):

    observation_type = "RSky"

    def run(self, n: int, integ_time: Union[int, float]) -> None:
        for idx in range(n):
            self.logger.info(f"Starting {idx}th/{n} sequence")
            self.hot(integ_time, idx)
            self.sky(integ_time, idx)
