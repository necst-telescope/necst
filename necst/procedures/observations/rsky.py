from typing import Union

from .observation_base import Observation


class RSky(Observation):
    observation_type = "RSky"

    def run(self, n: int, integ_time: Union[int, float]) -> None:
        plan = []
        for idx in range(n):
            plan.append(
                {
                    "item_uid": f"rsky:{idx:05d}:HOT",
                    "index0": 2 * idx,
                    "total": 2 * n,
                    "label": f"HOT {idx + 1}/{n}",
                    "mode": "HOT",
                    "role": "calibration",
                    "obs_id": str(idx),
                    "drive_kind": "hold",
                    "integration_sec": float(integ_time),
                    "geometry": {"kind": "current_position", "unit": "deg"},
                }
            )
            plan.append(
                {
                    "item_uid": f"rsky:{idx:05d}:SKY",
                    "index0": 2 * idx + 1,
                    "total": 2 * n,
                    "label": f"SKY {idx + 1}/{n}",
                    "mode": "SKY",
                    "role": "calibration",
                    "obs_id": str(idx),
                    "drive_kind": "hold",
                    "integration_sec": float(integ_time),
                    "geometry": {"kind": "current_sky_position", "unit": "deg"},
                }
            )
        self.progress.set_plan(plan, observation_type=self.observation_type, n=int(n))

        for idx in range(n):
            self.logger.info(f"Starting {idx}th/{n} sequence")
            hot_item = plan[2 * idx]
            with self.progress.item(**hot_item):
                with self.progress.drive(
                    kind="hold", stage="tracking", geometry=hot_item["geometry"]
                ):
                    self.hot(integ_time, idx, geometry=hot_item["geometry"])
            sky_item = plan[2 * idx + 1]
            with self.progress.item(**sky_item):
                with self.progress.drive(
                    kind="hold", stage="tracking", geometry=sky_item["geometry"]
                ):
                    self.sky(integ_time, idx, geometry=sky_item["geometry"])
