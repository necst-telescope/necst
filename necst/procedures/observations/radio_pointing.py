import os
from itertools import count
from typing import Any, Generator, Tuple, Union

from neclib.parameters import ObservationSpec

from .observation_base import Observation


class RadioPointing(Observation):

    observation_type = "RadioPointing"

    def run(self, path: Union[str, os.PathLike]) -> None:
        p = ObservationSpec.from_file(path)
        self.com.record("file", name=path)

        if (p.method > 0) and ((p.method - 1) % 4 != 0):
            raise ValueError("Invalid pointing method")

        antenna_scan_kwargs = dict(
            scan_frame=p.delta_coord, unit="deg", speed=p.speed, wait=True
        )
        if any(p[x] is None for x in ["lambda_on", "beta_on"]):
            antenna_scan_kwargs.update(name=p.target)
        else:
            on = (p.lambda_on.to_value("deg"), p.beta_on.to_value("deg"), p.coord_sys)
            antenna_scan_kwargs.update(reference=on)

        self.track_off_point(p)
        self.hot(p.integ_hot.to_value("s"), -1)
        self.off(p.integ_off.to_value("s"), id=-1)
        # TODO: Run every time interval is met

        iterate_counter = range(p.n) if p.n > 0 else count()

        if p.method > 0:
            for idx in iterate_counter:
                self.logger.info(f"Starting {idx}th sequence")
                self.discrete(idx, p)
        else:
            sep = (
                p.max_separation_az.to_value("deg"),
                p.max_separation_el.to_value("deg"),
            )
            for idx in iterate_counter:
                self.scan(idx, sep, antenna_scan_kwargs)

        self.hot(p.integ_hot.to_value("s"), 9999)

    def discrete(
        self,
        id: Any,
        spec: ObservationSpec,
    ) -> None:
        separation = (spec.grid_az.to_value("deg"), spec.grid_el.to_value("deg"))
        offsets = self.get_offset(spec.method, separation)
        for i, (offset_lon, offset_lat) in enumerate(offsets):
            self.logger.info(f"Starting integration at {i}th point")
            self.track_on_point(spec, offset=(offset_lon, offset_lat, spec.delta_coord))
            self.sky(spec.integ_on.to_value("s"), f"{id}-{i}")

    def scan(self, id: Any, width: Tuple[float, float], antenna_scan_kwargs) -> None:
        width_x, width_y = width

        self.logger.info("Starting X-scan")
        self.com.metadata("set", position="SKY", id=f"{id}-x", intercept=False)
        self.com.antenna(
            "scan", start=(-1 * width_x, 0), stop=(width_x, 0), **antenna_scan_kwargs
        )
        self.com.metadata("set", position="", id="")

        self.logger.info("Starting Y-scan")
        self.com.metadata("set", position="SKY", id=f"{id}-y", intercept=False)
        self.com.antenna(
            "scan", start=(0, -1 * width_y), stop=(0, width_y), **antenna_scan_kwargs
        )
        self.com.metadata("set", position="", id="")

    def get_offset(
        self, method: int, separation: Tuple[float, float]
    ) -> Generator[Tuple[float, float], None, None]:
        separation_x, separation_y = separation
        n_per_arm = int((method - 1) / 4)
        for offset in range(-n_per_arm, n_per_arm + 1):
            yield (offset * separation_x, 0.0)
        for offset in range(-n_per_arm, n_per_arm + 1):
            yield (0.0, offset * separation_y)

    def track_off_point(self, spec: ObservationSpec) -> None:
        if spec.relative:
            offset = (
                spec.delta_lambda.to_value("deg"),
                spec.delta_beta.to_value("deg"),
                spec.delta_coord,
            )
            self.track_on_point(spec, offset=offset)
        off = (
            spec.lambda_off.to_value("deg"),
            spec.beta_off.to_value("deg"),
            spec.coord_sys,
        )
        kwargs = dict(wait=True, unit="deg", target=off)
        self.com.antenna("point", **kwargs)

    def track_on_point(
        self, spec: ObservationSpec, offset: Tuple[float, float, str]
    ) -> None:
        kwargs = dict(wait=True, unit="deg")
        if any(spec[x] is None for x in ["lambda_on", "beta_on"]):
            kwargs.update(name=spec.target)
        else:
            on = (
                spec.lambda_on.to_value("deg"),
                spec.beta_on.to_value("deg"),
                spec.coord_sys,
            )
            kwargs.update(target=on)
        self.com.antenna("point", offset=offset, **kwargs)
