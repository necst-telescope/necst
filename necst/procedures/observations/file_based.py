import os
from typing import IO, Optional, Type, Union

from neclib.coordinates import ScanBlockLine
from neclib.coordinates.observations import (
    GridSpec,
    OTFSpec,
    PSWSpec,
    RadioPointingSpec,
)
from neclib.coordinates.observations.observation_spec_base import (
    ObservationMode,
    ObservationSpec,
)
from neclib.coordinates.paths import (
    build_scan_block_sections,
    margin_start_of,
    plan_scan_block_kinematics,
)

from .observation_base import Observation
from ... import config
from .spectral_recording_sequence import (
    apply_setup_with_commander,
    build_spectral_recording_observation_setup,
    save_sidecars_with_commander,
    set_gate_with_commander,
    clear_setup_with_commander,
    reject_legacy_recording_kwargs_for_setup,
    SpectralRecordingObservationSetup,
)


class FileBasedObservation(Observation):
    SpecParser: Type[ObservationSpec]

    def __init__(self, record_name: Optional[str] = None, /, **kwargs) -> None:
        file = kwargs["file"]
        self._obs_file = file
        self.obsspec = self.SpecParser.from_file(file)
        self._spectral_recording_setup: Optional[SpectralRecordingObservationSetup] = None
        self._spectral_recording_gate_open = False
        _record_name = record_name or f"{self.obsspec.target}"
        super().__init__(_record_name, **kwargs)

    def before_record_controls(self) -> None:
        params = getattr(self.obsspec, "parameters", {}) or {}
        setup = build_spectral_recording_observation_setup(
            params=params,
            obs_file=self._obs_file,
            default_setup_id=self.record_name,
        )
        if setup is None:
            return
        reject_legacy_recording_kwargs_for_setup(self._kwargs, setup)
        apply_setup_with_commander(self.com, setup)
        self._spectral_recording_setup = setup
        self._spectral_recording_gate_open = False
        self.logger.info(
            "Applied spectral recording setup before recorder start; "
            f"setup_id={setup.setup_id!r}, setup_hash={setup.setup_hash}"
        )

    def after_record_start(self) -> None:
        setup = self._spectral_recording_setup
        if setup is None:
            return
        save_sidecars_with_commander(self.com, setup)
        set_gate_with_commander(self.com, setup, allow_save=True)
        self._spectral_recording_gate_open = True
        self.logger.info(
            "Saved spectral recording sidecars and opened setup gate; "
            f"n_sidecars={len(setup.sidecars)}"
        )

    def before_record_stop(self) -> None:
        setup = self._spectral_recording_setup
        if setup is None:
            return
        try:
            if self._spectral_recording_gate_open:
                set_gate_with_commander(self.com, setup, allow_save=False)
                self.logger.info("Closed spectral recording setup gate before recorder stop")
        finally:
            self._spectral_recording_gate_open = False

    def after_record_stop(self) -> None:
        setup = self._spectral_recording_setup
        if setup is None:
            return
        try:
            clear_setup_with_commander(self.com, setup, strict=True)
            self.logger.info("Cleared spectral recording setup after recorder stop")
        finally:
            self._spectral_recording_setup = None
            self._spectral_recording_gate_open = False

    def allow_legacy_recording_cleanup_controls(self) -> bool:
        return self._spectral_recording_setup is None

    def _coord_to_tuple(self, coord: tuple):
        if len(coord) == 2:
            return tuple(x.to_value("deg") for x in coord)
        elif len(coord) == 3:
            return tuple(x.to_value("deg") for x in coord[:2]) + (coord[2],)
        raise ValueError(f"Invalid coordinate: {coord}")

    def _scan_start_stop(self, waypoint, scan_frag: int):
        if scan_frag > 0:
            start_q, stop_q = waypoint.start, waypoint.stop
        else:
            start_q, stop_q = waypoint.stop, waypoint.start
        start = self._coord_to_tuple(start_q)
        stop = self._coord_to_tuple(stop_q)
        return start_q, stop_q, start, stop

    def _make_scan_block_line(
        self, waypoint, scan_frag: int, margin_deg: float, *, line_index: int
    ) -> ScanBlockLine:
        start_q, stop_q, _, _ = self._scan_start_stop(waypoint, scan_frag)
        margin = margin_deg * start_q[0].unit
        return ScanBlockLine(
            start=(start_q[0], start_q[1]),
            stop=(stop_q[0], stop_q[1]),
            speed=waypoint.speed,
            margin=margin,
            label=str(waypoint.id),
            line_index=int(line_index),
        )

    def _scan_block_supported_for_waypoint(
        self, waypoint, *, use_scan_block: bool
    ) -> bool:
        if not use_scan_block:
            return False
        if not waypoint.is_scan:
            return False
        if waypoint.mode != ObservationMode.ON:
            return False
        if getattr(waypoint, "with_offset", False):
            offset = getattr(waypoint, "offset", None)
            if (offset is None) or (len(offset) != 3):
                return False
            if offset[2] != waypoint.scan_frame:
                self.logger.warning(
                    "use_scan_block requested, but falling back to legacy scan because "
                    "point-to-entry cannot combine block offset and scan offset in "
                    f"different frames: offset_frame={offset[2]!r}, "
                    f"scan_frame={waypoint.scan_frame!r}."
                )
                return False
        return True

    def _combined_entry_offset(self, entry, waypoint):
        entry_lon = entry[0].to_value("deg")
        entry_lat = entry[1].to_value("deg")
        if not getattr(waypoint, "with_offset", False):
            return entry_lon, entry_lat, waypoint.scan_frame
        offset = waypoint.offset
        return (
            entry_lon + offset[0].to_value("deg"),
            entry_lat + offset[1].to_value("deg"),
            waypoint.scan_frame,
        )

    def _scan_context_kwargs(self, waypoint, *, cos_scan: bool, unit: str = "deg"):
        kwargs = dict(unit=unit, cos_correction=cos_scan)
        if waypoint.name_query:
            kwargs.update(name=waypoint.target or waypoint.reference)
        else:
            _target, _reference = waypoint.target, waypoint.reference
            target = self._coord_to_tuple(_target) if _target else None
            reference = self._coord_to_tuple(_reference) if _reference else None
            if target is not None:
                kwargs.update(target=target)
            if reference is not None:
                kwargs.update(reference=reference)
        if getattr(waypoint, "with_offset", False):
            kwargs.update(offset=self._coord_to_tuple(waypoint.offset))
        return kwargs

    def _run_hot_off_pair(self, hot_waypoint, off_waypoint, *, cos_point: bool) -> None:
        kwargs = self._scan_context_kwargs(off_waypoint, cos_scan=cos_point)
        if off_waypoint.is_scan:
            raise ValueError("Scan drive is not supported for OFF/SKY mode.")

        # Operational policy:
        # - OFF cadence is decided at the scheduling layer and is kept intact.
        # - HOT is not taken as a standalone calibration before ON.
        # - When a HOT waypoint is paired with the following OFF waypoint,
        #   we first move to the OFF position and wait there for arrival/settle
        #   (Commander.antenna('point') waits by default), then execute HOT and
        #   OFF without issuing any additional antenna command in between.
        self.com.antenna("point", **kwargs)
        self.hot(
            hot_waypoint.integration.to_value("s"),
            hot_waypoint.id,
            preserve_tracking=True,
        )
        self.off(off_waypoint.integration.to_value("s"), off_waypoint.id)

    def _scan_block_context_signature(self, waypoint):
        if waypoint.name_query:
            ref_key = ("name", waypoint.target or waypoint.reference)
        else:
            target = self._coord_to_tuple(waypoint.target) if waypoint.target else None
            reference = (
                self._coord_to_tuple(waypoint.reference) if waypoint.reference else None
            )
            ref_key = ("coord", target, reference)
        offset = (
            self._coord_to_tuple(waypoint.offset)
            if getattr(waypoint, "with_offset", False)
            else None
        )
        return (
            waypoint.scan_frame,
            ref_key,
            offset,
        )

    def _preflight_scan_block_kinematics(self, lines) -> None:
        report = plan_scan_block_kinematics(lines)

        for item in report["lines"]:
            duration_scale = float(item["duration_scale"].to_value(""))
            if duration_scale > 1.0 + 1e-6:
                msg = (
                    "scan_block line-edge slowed for kinematic limits: "
                    f"line_index={item['line_index']} label={item['label']!r}, "
                    f"duration x{duration_scale:.3f}, "
                    f"peak_acc={item['peak_acceleration'].to_value('deg/s^2'):.6f} "
                    "deg/s^2"
                )
                if "peak_jerk" in item:
                    msg += f", peak_jerk={item['peak_jerk'].to_value('deg/s^3'):.6f} deg/s^3"
                self.logger.info(msg)

        for item in report["turns"]:
            duration_scale = float(item["duration_scale"].to_value(""))
            if duration_scale > 1.0 + 1e-6:
                msg = (
                    "scan_block turn slowed for kinematic limits: "
                    f"{item['from_line_index']}->{item['to_line_index']}, "
                    f"duration x{duration_scale:.3f}, "
                    f"peak_speed={item['peak_speed'].to_value('deg/s'):.6f} deg/s, "
                    f"peak_acc={item['peak_acceleration'].to_value('deg/s^2'):.6f} "
                    "deg/s^2"
                )
                if "peak_jerk" in item:
                    msg += f", peak_jerk={item['peak_jerk'].to_value('deg/s^3'):.6f} deg/s^3"
                self.logger.info(msg)

    def _move_to_scan_block_entry(
        self, waypoint, *, line: ScanBlockLine, cos_scan: bool
    ) -> None:
        entry = margin_start_of(line)
        entry_offset = self._combined_entry_offset(entry, waypoint)
        point_kwargs = dict(unit="deg", cos_correction=cos_scan)
        if waypoint.name_query:
            point_kwargs.update(name=waypoint.target or waypoint.reference)
            point_kwargs.update(offset=entry_offset)
        else:
            _target, _reference = waypoint.target, waypoint.reference
            target = self._coord_to_tuple(_target) if _target else None
            reference = self._coord_to_tuple(_reference) if _reference else None
            if target is not None:
                point_kwargs.update(target=target, offset=entry_offset)
            elif reference is not None:
                point_kwargs.update(target=reference, offset=entry_offset)
            else:
                point_kwargs.update(
                    target=(entry_offset[0], entry_offset[1], waypoint.scan_frame)
                )
        self.logger.info("Move to scan-block entry standby...")
        self.com.antenna("point", wait=False, **point_kwargs)

    def _run_on_scan_block(
        self,
        waypoints,
        *,
        scan_frags,
        cos_scan: bool,
        margin_deg: float,
        include_final_standby: bool,
        final_standby_duration_sec: float,
    ) -> None:
        lines = [
            self._make_scan_block_line(wp, frag, margin_deg, line_index=i)
            for i, (wp, frag) in enumerate(zip(waypoints, scan_frags))
        ]
        self._preflight_scan_block_kinematics(lines)
        first_waypoint = waypoints[0]
        sections = build_scan_block_sections(
            lines,
            include_initial_standby=True,
            include_final_decelerate=True,
            include_final_standby=include_final_standby,
            final_standby_duration=final_standby_duration_sec,
        )
        scan_kwargs = self._scan_context_kwargs(first_waypoint, cos_scan=cos_scan)
        block_id = str(first_waypoint.id)
        self.logger.info(f"Starting ON (scan_block, n_lines={len(lines)})...")
        self.com.scan_block(
            sections=sections,
            scan_frame=first_waypoint.scan_frame,
            metadata_position="ON",
            metadata_id=block_id,
            **scan_kwargs,
        )
        self.com.metadata("set", position="", id="")

    def run(self, file: Union[os.PathLike, str, IO], **kwargs) -> None:
        scan_frag = 1
        margin = config.antenna.scan_margin.value

        params = getattr(self.obsspec, "parameters", {}) or {}
        cos_global = bool(params.get("cos_correction", False))
        cos_scan = bool(params.get("scan_cos_correction", cos_global))
        cos_point = bool(params.get("point_cos_correction", cos_global))
        requested_use_scan_block = bool(params.get("use_scan_block", False))
        scan_block_supported_obstypes = {"OTF", "RadioPointing"}
        use_scan_block = requested_use_scan_block and (
            self.observation_type in scan_block_supported_obstypes
        )
        if requested_use_scan_block and not use_scan_block:
            supported = ", ".join(sorted(scan_block_supported_obstypes))
            self.logger.warning(
                "use_scan_block is currently supported only for "
                f"{supported}; falling back to legacy scan."
            )
        merge_scan_blocks = bool(params.get("merge_scan_blocks", False))
        scan_block_final_standby = bool(params.get("scan_block_final_standby", False))
        scan_block_final_standby_duration_sec = float(
            params.get("scan_block_final_standby_duration_sec", 1.0)
        )

        if self.observation_type == "OTF":
            bydirectional = self.obsspec.bydirectional > 0
            reset_scan = self.obsspec.reset_scan > 0
            direction = self.obsspec.scan_direction.lower()

            if reset_scan:
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
        else:
            reset_scan = True
            bydirectional = False
            direction = None
            reset = 1

        all_waypoints = list(self.obsspec)
        self.com.record("file", name=file)
        i = 0
        while i < len(all_waypoints):
            waypoint = all_waypoints[i]

            if waypoint.mode == ObservationMode.HOT:
                next_waypoint = (
                    all_waypoints[i + 1] if (i + 1) < len(all_waypoints) else None
                )
                if (
                    next_waypoint is not None
                    and next_waypoint.mode == ObservationMode.OFF
                ):
                    self._run_hot_off_pair(
                        waypoint,
                        next_waypoint,
                        cos_point=cos_point,
                    )
                    if reset_scan and bydirectional:
                        scan_frag = reset
                    i += 2
                    continue
                self.hot(waypoint.integration.to_value("s"), waypoint.id)
                i += 1
                continue

            if waypoint.mode in (ObservationMode.OFF, ObservationMode.SKY):
                kwargs = self._scan_context_kwargs(waypoint, cos_scan=cos_point)
                if not waypoint.is_scan:
                    self.com.antenna("point", **kwargs)
                    self.off(waypoint.integration.to_value("s"), waypoint.id)
                    if reset_scan and bydirectional:
                        scan_frag = reset
                else:
                    raise ValueError("Scan drive is not supported for OFF/SKY mode.")
                i += 1
                continue

            if waypoint.mode == ObservationMode.ON and waypoint.is_scan:
                use_block = self._scan_block_supported_for_waypoint(
                    waypoint,
                    use_scan_block=use_scan_block,
                )
                if use_block:
                    if merge_scan_blocks:
                        signature = self._scan_block_context_signature(waypoint)
                        block_waypoints = []
                        block_frags = []
                        frag = scan_frag
                        j = i
                        while j < len(all_waypoints):
                            cand = all_waypoints[j]
                            if not self._scan_block_supported_for_waypoint(
                                cand, use_scan_block=use_scan_block
                            ):
                                break
                            if self._scan_block_context_signature(cand) != signature:
                                break
                            block_waypoints.append(cand)
                            block_frags.append(frag)
                            if bydirectional:
                                frag *= -1
                            j += 1
                        self._run_on_scan_block(
                            block_waypoints,
                            scan_frags=block_frags,
                            cos_scan=cos_scan,
                            margin_deg=margin,
                            include_final_standby=scan_block_final_standby,
                            final_standby_duration_sec=(
                                scan_block_final_standby_duration_sec
                            ),
                        )
                        scan_frag = frag
                        i = j
                        continue

                    current_scan_frag = scan_frag
                    if bydirectional:
                        scan_frag *= -1
                    self._run_on_scan_block(
                        [waypoint],
                        scan_frags=[current_scan_frag],
                        cos_scan=cos_scan,
                        margin_deg=margin,
                        include_final_standby=scan_block_final_standby,
                        final_standby_duration_sec=(
                            scan_block_final_standby_duration_sec
                        ),
                    )
                    i += 1
                    continue

            kwargs = self._scan_context_kwargs(
                waypoint,
                cos_scan=cos_scan if waypoint.is_scan else cos_point,
            )
            current_scan_frag = scan_frag
            if waypoint.is_scan:
                _, _, start, stop = self._scan_start_stop(waypoint, current_scan_frag)
                kwargs.update(
                    start=start,
                    stop=stop,
                    scan_frame=waypoint.scan_frame,
                    speed=waypoint.speed.to_value("deg/s"),
                )
                if bydirectional:
                    scan_frag *= -1

            if waypoint.mode == ObservationMode.ON:
                if waypoint.is_scan:
                    if self.observation_type == "OTF":
                        start = kwargs["start"]
                        reference = kwargs.get("reference") or (0, 0)
                        start_position = (
                            start[0] + reference[0],
                            start[1] + reference[1],
                        )
                        target = start_position + (waypoint.scan_frame,)
                        offset_margin = current_scan_frag * margin

                        if direction == "x":
                            offset_position = (-offset_margin, 0)
                        elif direction == "y":
                            offset_position = (0, -offset_margin)
                        else:
                            offset_position = (-offset_margin, -offset_margin)

                        self.logger.info("Move to ON...")
                        self.com.antenna(
                            "point",
                            target=target,
                            unit="deg",
                            offset=(offset_position + (waypoint.scan_frame,)),
                            cos_correction=cos_scan,
                        )

                    self.logger.info("Starting ON...")
                    self.com.metadata("set", position="ON", id=waypoint.id)
                    self.com.antenna("scan", **kwargs)
                    self.com.metadata("set", position="", id="")
                else:
                    self.com.antenna("point", **kwargs)
                    self.on(waypoint.integration.to_value("s"), waypoint.id)
            i += 1


class OTF(FileBasedObservation):
    observation_type = "OTF"
    SpecParser = OTFSpec


class RadioPointing(FileBasedObservation):
    observation_type = "RadioPointing"
    SpecParser = RadioPointingSpec


class PSW(FileBasedObservation):
    observation_type = "PSW"
    SpecParser = PSWSpec


class Grid(FileBasedObservation):
    observation_type = "Grid"
    SpecParser = GridSpec
