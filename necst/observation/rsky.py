import time
from typing import Union

from .observation_base import Observation


class RSky(Observation):

    observation_type = "RSky"

    def run(self, n: int, integ_time: Union[int, float]) -> None:
        self.com.metadata("set", position="", id="")
        current_position = self.com.get_message("encoder")
        self.com.antenna(
            "point",
            lon=current_position.lon,
            lat=45,
            frame="altaz",
            unit="deg",
            wait=True,
        )

        for _ in range(n):
            self.com.chopper("insert")
            self.com.metadata("set", position="HOT", id=str(_))
            time.sleep(integ_time)
            self.com.metadata("set", position="")
            self.com.chopper("remove")
            self.com.metadata("set", position="SKY", id=str(_))
            time.sleep(integ_time)
            self.com.metadata("set", position="")
