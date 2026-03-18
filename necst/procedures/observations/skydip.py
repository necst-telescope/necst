from typing import Union

from .observation_base import Observation


class Skydip(Observation):
    observation_type = "Skydip"

    elevations = [70, 50, 40, 30, 25, 22, 20]

    def run(self, integ_time: Union[int, float]) -> None:
        self.com.metadata("set", position="", id="")
        current_position = self.com.get_message("encoder")
        current_position_cor = current_position.lon + current_position.dlon
        self.com.antenna(
            "point",
            target=(current_position_cor, self.elevations[0], "altaz"),
            unit="deg",
            wait=True,
        )

        self.hot(integ_time, "")

        for el in self.elevations:
            self.logger.info(f"Starting integration at El = {el} deg")
            self.com.antenna(
                "point",
                target=(current_position_cor, el, "altaz"),
                unit="deg",
                wait=True,
            )
            self.sky(integ_time, id=el)

        self.hot(integ_time, "")
