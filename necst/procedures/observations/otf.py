import math
import time
from typing import Optional, Tuple, Union

from neclib.parameters import ObsParams

from .observation_base import Observation


class OTF(Observation):

    observation_type = "OTF"

    @staticmethod
    def position_angle(p: ObsParams) -> float:
        pa = p.position_angle.to_value("rad")
        if p.StartPositionX.to_value("deg") < 0:
            pa = math.pi - pa
        if p.StartPositionY.to_value("deg") < 0:
            pa *= -1
        if p.SCAN_DIRECTION.upper() == "Y":
            pa -= math.pi / 4
        return pa

    @classmethod
    def offset_coord_repr(cls, p: ObsParams):
        conversion_table = {"j2000": "fk5", "b1950": "fk4"}
        frame = conversion_table.get(p.COORD_SYS.lower(), p.COORD_SYS)
        pa = cls.position_angle(p)
        return f"origin={frame}({p.LambdaOn}, {p.BetaOn}), rotation={pa}rad"

    def drive_to_off_position(self, p: ObsParams) -> None:
        kwargs = dict(unit="deg", wait=True)
        if p.RELATIVE:
            source = (p.LambdaOn.to_value("deg"), p.BetaOn.to_value("deg"), p.COORD_SYS)
            offset = (
                p.deltaLambda.to_value("deg"),
                p.deltaBeta.to_value("deg"),
                p.COORD_SYS,
            )
            kwargs.update(offset=offset, reference=source)
        else:
            target = (
                p.LambdaOff.to_value("deg"),
                p.BetaOff.to_value("deg"),
                p.COORD_SYS,
            )
            kwargs.update(target=target)
        self.com.antenna("point", **kwargs)

    def get_scan_coord(
        self, idx: int, p: ObsParams
    ) -> Tuple[Tuple[float, float], Tuple[float, float]]:
        scan_length = p.scan_length * p.scan_velocity
        pa = self.position_angle(p)
        x_scan = p.SCAN_DIRECTION.upper() == "X"
        start = (
            p.StartPositionX * math.cos(pa) + (0 if x_scan else p.scan_spacing * idx),
            p.StartPositionY * math.sin(pa) + (p.scan_spacing * idx if x_scan else 0),
        )
        stop = (
            start[0] + scan_length * math.cos(pa),
            start[1] + scan_length * math.sin(pa),
        )
        return start, stop

    def run(self, path: str) -> None:
        p = ObsParams.from_file(path)

        hot_interval_in_time = p.load_interval.unit.is_equivalent("s")
        off_interval_in_time = p.off_interval.unit.is_equivalent("s")

        hot_observation_interval_manager = _IntervalChecker(
            p.load_interval.to_value("s") if hot_interval_in_time else p.load_interval,
            hot_interval_in_time,
        )
        off_observation_interval_manager = _IntervalChecker(
            p.off_interval.to_value("s") if off_interval_in_time else p.off_interval,
            off_interval_in_time,
        )

        for idx in range(int(p.n)):
            if hot_observation_interval_manager.check(idx):
                self.hot(p.integ_hot.to_value("s"), idx)
                hot_observation_interval_manager.update(idx)

            if off_observation_interval_manager.check(idx):
                self.drive_to_off_position(p)
                self.off(p.integ_off.to_value("s"), idx)
                off_observation_interval_manager.update(idx)

            start, stop = self.get_scan_coord(idx, p)
            self.com.antenna(
                "scan",
                start=start,
                stop=stop,
                scan_frame=self.offset_coord_repr(p),
                reference=(0, 0, self.offset_coord_repr(p)),
                speed=p.scan_velocity.to_value("deg/s"),
                unit="deg",
                wait=True,
            )

        self.drive_to_off_position(p)

        self.hot(p.integ_hot.to_value("s"), 9999)
        self.off(p.integ_off.to_value("s"), 9999)


class _IntervalChecker:
    def __init__(self, interval: Union[int, float], in_time: bool) -> None:
        self.in_time = in_time
        self.interval = interval
        self.last = None

    def update(self, scan_idx: Optional[int] = None) -> None:
        if self.in_time:
            self.last = time.time()
        elif scan_idx is not None:
            self.last = int(scan_idx)
        else:
            raise ValueError("scan_idx should be given for scan number based interval")

    def check(self, scan_idx: Optional[int] = None) -> bool:
        if self.last is None:
            cond = True
        elif self.in_time:
            cond = time.time() - self.last > self.interval
        elif scan_idx is not None:
            cond = int(scan_idx) - self.last > self.interval
        else:
            raise ValueError("scan_idx should be given for scan number based interval")

        if cond:
            self.update(scan_idx)
        return cond
