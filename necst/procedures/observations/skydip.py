from typing import Union

from neclib.parameters import PointingError

from ... import config
from .observation_base import Observation


class Skydip(Observation):

    observation_type = "Skydip"

    elevations = [80, 50, 40, 30, 25, 22, 20]

    def run(self, integ_time: Union[int, float]) -> None:
        self.com.metadata("set", position="", id="")
        current_position = self.com.get_message("encoder")
        params = PointingError.from_file(config.antenna_pointing_parameter_path)
        current_lon, *_ = params.apparent2refracted(
            az=current_position.lon, el=current_position.lat, unit="deg"
        )
        self.com.antenna(
            "point",
            target=(current_lon.to_value("deg"), self.elevations[0], "altaz"),
            unit="deg",
            wait=True,
        )

        self.hot(integ_time, "")

        for el in self.elevations:
            self.com.antenna(
                "point",
                target=(current_lon.to_value("deg"), el, "altaz"),
                unit="deg",
                wait=True,
            )
            self.sky(integ_time, id=el)

        self.hot(integ_time, "")
