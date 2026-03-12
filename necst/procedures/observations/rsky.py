from typing import Union

from .observation_base import Observation


class RSky(Observation):
    observation_type = "RSky"

    def run(self, n: int, integ_time: Union[int, float]) -> None:
        current_position = self.com.get_message("encoder")
        self.com.antenna(
            "point",
            target=(current_position.lon, 70, "altaz"),
            unit="deg",
            wait=True,
            direct_mode=True,
        )
        for idx in range(n):
            self.logger.info(f"Starting {idx}th/{n} sequence")
            self.hot(integ_time, idx)
            self.sky(integ_time, idx)
