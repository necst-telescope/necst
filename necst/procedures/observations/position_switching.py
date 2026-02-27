from typing import Union

from .observation_base import Observation


class PositionSwitching(Observation):
    observation_type = "PositionSwitching"

    def run(
        self,
        hot_integ_time: Union[int, float],
        sky_integ_time: Union[int, float],
        off_position: list[float, float],
        on_position: list[float, float],
        n: int,
    ) -> None:
        self.com.metadata("set", position="", id="")

        self.hot(hot_integ_time, "")

        for i in range(n):
            self.com.antenna(
                "point",
                target=(off_position[0], off_position[1], "fk5"),
                unit="deg",
                wait=True,
            )

            self.sky(sky_integ_time, "off")

            self.com.antenna(
                "point",
                target=(on_position[0], on_position[1], "fk5"),
                unit="deg",
                wait=True,
            )

            self.sky(sky_integ_time, "on")

        self.hot(hot_integ_time, "")
