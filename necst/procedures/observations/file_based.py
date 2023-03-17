import os
from typing import IO, Optional, Type, Union

from neclib.coordinates.observations import OTFSpec, RadioPointingSpec
from neclib.coordinates.observations.observation_spec_base import (
    ObservationMode,
    ObservationSpec,
)

from .observation_base import Observation


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
                kwargs.update(
                    start=self._coord_to_tuple(waypoint.start),
                    stop=self._coord_to_tuple(waypoint.stop),
                    scan_frame=waypoint.scan_frame,
                    speed=waypoint.speed.to_value("deg/s"),
                )
            if waypoint.with_offset:
                kwargs.update(offset=self._coord_to_tuple(waypoint.offset))

            if waypoint.mode in (ObservationMode.OFF, ObservationMode.SKY):
                if not waypoint.is_scan:
                    self.com.antenna("point", **kwargs)
                    self.off(waypoint.integration.to_value("s"), waypoint.id)
                else:
                    raise ValueError("Scan drive is not supported for OFF/SKY mode.")

            if waypoint.mode == ObservationMode.ON:
                if waypoint.is_scan:
                    self.logger.info("Starting ON...")
                    self.com.metadata(
                        "set", position="ON", id=waypoint.id, intercept=False
                    )
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
