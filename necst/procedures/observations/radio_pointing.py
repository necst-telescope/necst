from itertools import count
from typing import Any, Generator, Tuple, Union

from .observation_base import Observation


class RadioPointing(Observation):

    observation_type = "RadioPointing"

    def run(
        self,
        target: Union[str, Tuple[float, float, str]],
        method: int = 9,
        offset_frame: str = "altaz",
        separation: float = 1 / 60,
        speed: float = 1 / 15,
        unit: str = "deg",
        n_iter: int = 0,
        integ_time: Union[int, float] = 5,
    ) -> None:
        if (method > 0) and ((method - 1) % 4 != 0):
            raise ValueError("Invalid pointing method")

        antenna_point_kwargs = dict(wait=True, unit=unit)
        antenna_scan_kwargs = dict(
            frame=offset_frame, unit=unit, speed=speed, wait=True
        )
        if isinstance(target, str):
            antenna_point_kwargs.update(name=target)
            antenna_scan_kwargs.update(name=target)
        else:
            antenna_point_kwargs.update(target=target)
            antenna_scan_kwargs.update(reference=target)

        self.com.antenna("point", **antenna_point_kwargs)
        self.hot(integ_time, -1)

        iterate_counter = range(n_iter) if n_iter > 0 else count()

        if method <= 0:
            for idx in iterate_counter:
                self.logger.info(f"Starting {idx}th sequence")
                self.discrete(
                    idx,
                    method,
                    separation,
                    offset_frame,
                    integ_time,
                    antenna_point_kwargs,
                )
        else:
            for idx in iterate_counter:
                self.scan(idx, separation, antenna_scan_kwargs)

        self.hot(integ_time, 9999)

    def discrete(
        self,
        id: Any,
        method: int,
        separation: float,
        offset_frame: str,
        integ_time: Union[int, float],
        antenna_point_kwargs,
    ) -> None:
        offsets = self.get_offset(method, separation)
        for i, (offset_lon, offset_lat) in enumerate(offsets):
            self.logger.info(f"Starting integration at {i}th point")
            self.com.antenna(
                "point",
                offset=(offset_lon, offset_lat, offset_frame),
                **antenna_point_kwargs,
            )
            self.sky(integ_time, f"{id}-{i}")

    def scan(self, id: Any, width: float, antenna_scan_kwargs) -> None:
        self.logger.info("Starting X-scan")
        self.com.metadata("set", position="SKY", id=f"{id}-x", delay=True)
        self.com.antenna(
            "scan", start=(-1 * width, 0), stop=(width, 0), **antenna_scan_kwargs
        )
        self.com.metadata("set", position="", id="")

        self.logger.info("Starting Y-scan")
        self.com.metadata("set", position="SKY", id=f"{id}-y", delay=True)
        self.com.antenna(
            "scan", start=(0, -1 * width), stop=(0, width), **antenna_scan_kwargs
        )
        self.com.metadata("set", position="", id="")

    def get_offset(
        self, method: int, separation: float
    ) -> Generator[Tuple[float, float], None, None]:
        n_per_arm = int((method - 1) / 4)
        for offset in range(-n_per_arm, n_per_arm + 1):
            yield (offset * separation, 0.0)
        for offset in range(-n_per_arm, n_per_arm + 1):
            yield (0.0, offset * separation)
