from typing import Generator, Tuple, Union

from .observation_base import Observation


class RadioPointing(Observation):

    observation_type = "RadioPointing"

    def run(
        self,
        target: Union[str, Tuple[float, float, str]],
        method: int = 9,
        frame: str = "altaz",
        separation: float = 1 / 60,
        unit: str = "deg",
        integ_time: Union[int, float] = 5,
    ) -> None:
        if (method - 1) % 4 != 0:
            raise ValueError("Invalid pointing method")

        kwargs = dict(wait=True, unit=unit)
        if isinstance(target, str):
            kwargs.update(name=target)
        else:
            kwargs.update(target=target[:2], frame=target[2])

        self.com.antenna("point", **kwargs)

        self.hot(integ_time, "")

        offsets = self.get_offset(method, separation)
        for i, (offset_lon, offset_lat) in enumerate(offsets):
            self.com.antenna("point", offset=(offset_lon, offset_lat, frame), **kwargs)
            self.sky(integ_time, i)

        self.hot(integ_time, "")

    def get_offset(
        self, method: int, separation: float
    ) -> Generator[Tuple[float, float], None, None]:
        n_per_arm = int((method - 1) / 4)
        for offset in range(-n_per_arm, n_per_arm + 1):
            yield (offset * separation, 0.0)
        for offset in range(-n_per_arm, n_per_arm + 1):
            yield (0.0, offset * separation)
