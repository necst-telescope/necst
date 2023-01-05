import time
from typing import Any, Union

from .observation_base import Observation


class Skydip(Observation):

    observation_type = "Skydip"

    elevations = [80, 50, 40, 30, 25, 22, 20]

    def run(self, integ_time: Union[int, float]) -> None:
        self.com.metadata("set", position="", id="")
        current_position = self.com.get_message("encoder")
        self.com.antenna(
            "point",
            target=(current_position.lon, 80),
            frame="altaz",
            unit="deg",
            wait=True,
        )

        self.hot(integ_time)

        for el in self.elevations:
            self.com.antenna(
                "point",
                target=(current_position.lon, el),
                frame="altaz",
                unit="deg",
                wait=True,
            )
            self.sky(integ_time, id=el)

        self.hot(integ_time)

    def hot(self, integ_time: Union[int, float]) -> None:
        self.com.chopper("insert")
        self.com.metadata("set", position="HOT")
        time.sleep(integ_time)
        self.com.metadata("set", position="")
        self.com.chopper("remove")

    def sky(self, integ_time: Union[int, float], id: Any) -> None:
        self.com.metadata("set", position="SKY", id=str(id))
        time.sleep(integ_time)
        self.com.metadata("set", position="")
