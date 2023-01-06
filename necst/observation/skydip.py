from typing import Union

from .observation_base import Observation


class Skydip(Observation):

    observation_type = "Skydip"

    elevations = [80, 50, 40, 30, 25, 22, 20]

    def run(self, integ_time: Union[int, float]) -> None:
        self.com.metadata("set", position="", id="")
        current_position = self.com.get_message("encoder")
        self.com.antenna(
            "point",
            target=(current_position.lon, 80, "altaz"),
            unit="deg",
            wait=True,
        )

        self.hot(integ_time, "")

        for el in self.elevations:
            self.com.antenna(
                "point",
                target=(current_position.lon, el, "altaz"),
                unit="deg",
                wait=True,
            )
            self.sky(integ_time, id=el)

        self.hot(integ_time, "")
