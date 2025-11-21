import os
from typing import IO, Optional, Type, Union

from neclib.coordinates.observations import OTFSpec, RadioPointingSpec, PSWSpec
from neclib.coordinates.observations.observation_spec_base import (
    ObservationMode,
    ObservationSpec,
)

from ..observation_base import Observation
from ... import config


class FileBasedObservation(Observation):
    SpecParser: Type[ObservationSpec]

    def __init__(self, record_name: Optional[str] = None, /, **kwargs) -> None:
        file = kwargs["file"]
        self.obsspec = self.SpecParser.from_file(file)
        _record_name = record_name or f"{self.obsspec.target}"
        super().__init__(_record_name, **kwargs)

    def _coord_to_tuple(self, coord: tuple):
        if len(coord) == 2:
            return tuple(x.to_value("deg") for x in coord)
        elif len(coord) == 3:
            return tuple(x.to_value("deg") for x in coord[:2]) + (coord[2],)
        raise ValueError(f"Invalid coordinate: {coord}")

    def run(self, file: Union[os.PathLike, str, IO]) -> None:
        scan_frag = 1

        bydirectional = self.obsspec.bydirectional > 0
        reset_scan = self.obsspec.reset_scan > 0
        margin = config.antenna.scan_margin.value

        print(f"check: {bydirectional}, {reset_scan}")
        if reset_scan:
            direction = self.obsspec.scan_direction.lower()
            start_position = self.obsspec["start_position_" + direction]
            if self.obsspec.relative:
                delta = (
                    self.obsspec.delta_lambda.value
                    if direction == "x"
                    else self.obsspec.delta_beta.value
                )
            else:
                off_point = (
                    self.obsspec.lambda_off.value
                    if direction == "x"
                    else self.obsspec.beta_off.value
                )
                on_point = (
                    self.obsspec.lambda_on.value
                    if direction == "x"
                    else self.obsspec.beta_on.value
                )
                delta = off_point - on_point
            if delta * start_position > 0:
                reset = 1
            else:
                reset = -1

        self.com.record("file", name=file)

        for waypoint in self.obsspec:
            if waypoint.mode == ObservationMode.HOT:  # Hot observation
                self.hot(waypoint.integration.to_value("s"), waypoint.id)
                continue

            kwargs = dict(unit="deg")
            if waypoint.name_query:
                kwargs.update(name=waypoint.target or waypoint.reference)
            else:
                _target, _reference = waypoint.target, waypoint.reference
                target = self._coord_to_tuple(_target) if _target else None
                reference = self._coord_to_tuple(_reference) if _reference else None
                kwargs.update(target=target, reference=reference)
            if waypoint.is_scan:
                if scan_frag > 0:
                    kwargs.update(
                        start=self._coord_to_tuple(waypoint.start),
                        stop=self._coord_to_tuple(waypoint.stop),
                        scan_frame=waypoint.scan_frame,
                        speed=waypoint.speed.to_value("deg/s"),
                    )
                else:
                    kwargs.update(
                        start=self._coord_to_tuple(waypoint.stop),
                        stop=self._coord_to_tuple(waypoint.start),
                        scan_frame=waypoint.scan_frame,
                        speed=waypoint.speed.to_value("deg/s"),
                    )
                if bydirectional:
                    scan_frag *= -1
            if waypoint.with_offset:
                kwargs.update(offset=self._coord_to_tuple(waypoint.offset))

            if waypoint.mode in (ObservationMode.OFF, ObservationMode.SKY):
                if not waypoint.is_scan:
                    self.com.antenna("point", **kwargs)
                    self.off(waypoint.integration.to_value("s"), waypoint.id)
                    if reset_scan and bydirectional:
                        scan_frag = reset
                else:
                    raise ValueError("Scan drive is not supported for OFF/SKY mode.")

            if waypoint.mode == ObservationMode.ON:
                if waypoint.is_scan:
                    self.logger.info("Move to ON...")

                    start = kwargs["start"]
                    reference = kwargs["reference"]
                    start_position = (start[0] + reference[0], start[1] + reference[1])
                    target = start_position + (waypoint.scan_frame,)
                    offset_margin = scan_frag * margin

                    if direction == "x":
                        offset_position = (-offset_margin, 0)
                    elif direction == "y":
                        offset_position = (0, -offset_margin)
                    self.com.antenna(
                        "point",
                        target=target,
                        unit="deg",
                        offset=offset_position + (waypoint.scan_frame,),
                    )

                    self.logger.info("Starting ON...")
                    self.com.metadata(
                        "set", position="ON", id=waypoint.id, intercept=False
                    )
                    print(kwargs)
                    self.com.antenna("scan", **kwargs)
                    self.com.metadata("set", position="", id="")
                else:
                    self.com.antenna("point", **kwargs)
                    self.on(waypoint.integration.to_value("s"), waypoint.id)


class OTF(FileBasedObservation):
    observation_type = "OTF"
    SpecParser = OTFSpec


class RadioPointing(FileBasedObservation):
    observation_type = "RadioPointing"
    SpecParser = RadioPointingSpec

class PSW(FileBasedObservation):
    observation_type = "PSW"
    SpecParser = PSWSpec

