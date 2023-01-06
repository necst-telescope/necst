from typing import Union

from .observation_base import Observation


class RSky(Observation):

    observation_type = "RSky"

    def run(self, n: int, integ_time: Union[int, float]) -> None:
        current_position = self.com.get_message("encoder")
        self.com.antenna(
            "point",
            target=(current_position.lon, 45, "altaz"),
            unit="deg",
            wait=True,
        )

        for _ in range(n):
            self.hot(integ_time, _)
            self.sky(integ_time, _)
