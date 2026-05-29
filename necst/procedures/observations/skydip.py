from typing import Any, Union

from .observation_base import Observation


class Skydip(Observation):
    observation_type = "Skydip"

    elevations = [70, 50, 40, 30, 25, 22, 20]

    @staticmethod
    def _to_deg_float(value: Any) -> float:
        if hasattr(value, "to_value"):
            return float(value.to_value("deg"))
        if hasattr(value, "value"):
            return float(value.value)
        return float(value)

    def _hot_plan_item(self, index0: int, total: int, label: str, geometry=None):
        return {
            "item_uid": f"skydip:{index0:05d}:HOT",
            "index0": int(index0),
            "total": int(total),
            "label": label,
            "mode": "HOT",
            "role": "calibration",
            "obs_id": "",
            "drive_kind": "hold",
            "geometry": geometry or {"kind": "current_position", "unit": "deg"},
        }

    def _sky_plan_item(self, index0: int, total: int, el: Union[int, float], az: Any):
        az_deg = self._to_deg_float(az)
        el_deg = float(el)
        return {
            "item_uid": f"skydip:{index0:05d}:SKY:el{el}",
            "index0": int(index0),
            "total": int(total),
            "label": f"SKY El={el} deg",
            "mode": "SKY",
            "role": "calibration",
            "obs_id": str(el),
            "drive_kind": "point",
            "geometry": {
                "kind": "skydip_elevation",
                "frame": "altaz",
                "unit": "deg",
                "target": [az_deg, el_deg, "altaz"],
                "az_deg": az_deg,
                "el_deg": el_deg,
            },
        }

    def run(self, integ_time: Union[int, float]) -> None:
        self.com.metadata("set", position="", id="")
        current_position = self.com.get_message("encoder")
        current_position_cor = current_position.lon + current_position.dlon
        total = len(self.elevations) + 2
        first_sky_item = self._sky_plan_item(
            1, total, self.elevations[0], current_position_cor
        )
        first_geometry = first_sky_item["geometry"]
        plan = [
            self._hot_plan_item(
                0,
                total,
                "HOT before skydip",
                geometry={
                    **first_geometry,
                    "kind": "calibration_at_skydip_start",
                    "calibration_context": "before_skydip",
                },
            ),
            first_sky_item,
        ]
        plan.extend(
            self._sky_plan_item(i + 1, total, el, current_position_cor)
            for i, el in enumerate(self.elevations[1:], start=1)
        )
        plan.append(
            self._hot_plan_item(
                total - 1,
                total,
                "HOT after skydip",
                geometry={
                    **plan[-1]["geometry"],
                    "kind": "calibration_at_skydip_end",
                    "calibration_context": "after_skydip",
                },
            )
        )
        self.progress.set_plan(plan, observation_type=self.observation_type)

        with self.progress.item(**plan[0]):
            with self.progress.drive(
                kind="point", stage="moving", geometry=first_geometry
            ):
                self.com.antenna(
                    "point",
                    target=(current_position_cor, self.elevations[0], "altaz"),
                    unit="deg",
                    wait=True,
                )
            with self.progress.drive(
                kind="hold", stage="tracking", geometry=plan[0]["geometry"]
            ):
                self.hot(
                    integ_time,
                    "",
                    location_context="skydip_start_position",
                    geometry=plan[0]["geometry"],
                )

        for i, el in enumerate(self.elevations):
            item = plan[i + 1]
            geometry = item["geometry"]
            self.logger.info(f"Starting integration at El = {el} deg")
            with self.progress.item(**item):
                with self.progress.drive(
                    kind="point", stage="moving", geometry=geometry
                ):
                    self.com.antenna(
                        "point",
                        target=(current_position_cor, el, "altaz"),
                        unit="deg",
                        wait=True,
                    )
                with self.progress.drive(
                    kind="point", stage="tracking", geometry=geometry
                ):
                    self.sky(integ_time, id=el, geometry=geometry)

        with self.progress.item(**plan[-1]):
            with self.progress.drive(
                kind="hold", stage="tracking", geometry=plan[-1]["geometry"]
            ):
                self.hot(
                    integ_time,
                    "",
                    location_context="skydip_end_position",
                    geometry=plan[-1]["geometry"],
                )
