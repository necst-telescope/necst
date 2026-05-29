__all__ = ["HorizontalCoord"]

import time
from dataclasses import replace

import astropy.units as u
from collections import deque
import threading
from typing import Any, Dict, Optional, Tuple

import numpy as np

from neclib.coordinates import (
    CoordinateGeneratorManager,
    DriveLimitChecker,
    PathFinder,
    ScanBlockSection as FinderScanBlockSection,
)
from neclib.coordinates.paths import ControlContext
from necst_msgs.msg import (
    AntennaCommandQueueStatus,
    AntennaSectionStatus,
    Boolean,
    ControlStatus,
    CoordMsg,
    ScanBlockSection as ScanBlockSectionMsg,
    WeatherMsg,
)
from necst_msgs.srv import CoordinateCommand, ScanBlockCommand

from ... import config, namespace, service, topic
from ...core import AlertHandlerNode


class HorizontalCoord(AlertHandlerNode):
    NodeName = "altaz_coord"
    Namespace = namespace.antenna

    def __init__(self) -> None:
        super().__init__(self.NodeName, namespace=self.Namespace)
        self.logger = self.get_logger()

        self.cmd = None
        self.enc_az = self.enc_el = None
        self.enc_time = 0
        self.direct_mode = None
        self.az_target_mode = "sky"

        self.finder = PathFinder(
            config.location, config.antenna_pointing_parameter_path
        )
        drive_limit = config.antenna_drive
        self.optimizer = {
            "az": DriveLimitChecker(
                drive_limit.critical_limit_az, drive_limit.warning_limit_az
            ),
            "el": DriveLimitChecker(
                drive_limit.critical_limit_el, drive_limit.warning_limit_el
            ),
        }

        self.publisher = topic.altaz_cmd.publisher(self)
        topic.antenna_encoder.subscription(self, self._update_enc)
        topic.weather.subscription(self, self._update_weather)
        topic.antenna_cmd_transition.subscription(self, self.next)
        service.raw_coord.service(self, self._update_cmd)
        service.scan_block.service(self, self._update_scan_block_cmd)

        self.status_publisher = topic.antenna_control_status.publisher(self)
        self.section_status_publisher = topic.antenna_section_status.publisher(self)
        self.queue_status_publisher = topic.antenna_command_queue_status.publisher(self)

        self.create_timer(1 / config.antenna_command_frequency, self.command_realtime)
        self.create_timer(0.2, self.convert)
        self.create_timer(0.2, self.publish_control_diagnostics)

        self.result_queue = deque()
        self._rq_lock = threading.Lock()
        self._gen_lock = threading.RLock()
        self._history_lock = threading.Lock()

        self._min_buffer_sec = 0.5
        self._max_buffer_sec = 3.0
        self._max_groups_per_convert = 10

        self.tracking_ok = False
        self.executing_generator = CoordinateGeneratorManager()
        self.last_status = None

        # Execution lifecycle state.
        # `_current_exec_id` must remain stable until the buffered tail of that
        # execution has been completely published. Commander.wait(mode="control")
        # treats an ID change as completion, so changing it at generator exhaustion is
        # too early if `result_queue` still holds future commands.
        self._exec_seq = 0
        self._idle_exec_id = self._next_exec_id()
        self._current_exec_id: Optional[str] = None
        self._generator_exhausted = False
        self._last_active_context: Optional[ControlContext] = None
        # Realtime-executed state (not convert-side future state)
        self._last_published_cmd_time: float = 0.0
        self._last_published_context: Optional[ControlContext] = None
        self._last_publish_wall_time: float = 0.0
        self._last_publish_cmd_stamp: float = 0.0
        self._current_mode: str = "idle"
        self._guard_latched: bool = False
        self._published_command_history = deque(
            maxlen=max(200, int(config.antenna_command_frequency * 10))
        )
        self._last_queue_status_reason: str = "init"

        self.gc = self.create_guard_condition(self._clear_cmd)

    def _next_exec_id(self) -> str:
        self._exec_seq += 1
        return f"exec-{self._exec_seq}"

    def _transition_to_idle(self, *, reason: str = "unknown") -> None:
        now = time.time()
        with self._rq_lock:
            qlen = len(self.result_queue)
            tail_lead = (self.result_queue[-1][4] - now) if self.result_queue else None
        req = self.cmd
        req_desc = self._describe_request(req)
        self.logger.warning(
            f"Transitioning to idle: reason={reason},"
            f"old_exec={self._current_exec_id or self._idle_exec_id}, "
            f"old_mode={self._current_mode}, queue_len={qlen}, "
            f"tail_lead={tail_lead if tail_lead is not None else 'None'},"
            f"now={now:.6f}, req={req_desc}"
        )
        with self._gen_lock:
            self.executing_generator.clear()
            self._current_exec_id = None
            self._generator_exhausted = False
            self._last_active_context = None
            self._last_published_context = None
            self._last_published_cmd_time = 0.0
            self._idle_exec_id = self._next_exec_id()
        with self._history_lock:
            self._published_command_history.clear()
        self.cmd = None
        self._current_mode = "idle"
        self._last_queue_status_reason = reason

    def _clear_cmd(self) -> None:
        with self._rq_lock:
            qlen = len(self.result_queue)
        if (self.cmd is None) and (self._current_mode == "idle") and (qlen == 0):
            # Guard callback may be triggered repeatedly while critical is latched.
            # Once we are already idle with an empty queue, do nothing.
            return
        self.logger.warning(
            f"Guard/clear_cmd invoked: now={time.time():.6f}, queue_len={qlen}, "
            f"last_published_cmd_time={self._last_published_cmd_time:.6f},"
            f" last_publish_wall={self._last_publish_wall_time:.6f}"
        )
        self._transition_to_idle(reason="guard_or_clear_cmd")
        with self._rq_lock:
            self.result_queue.clear()
        with self._history_lock:
            self._published_command_history.clear()

    def _update_cmd(
        self, request: CoordinateCommand.Request, response: CoordinateCommand.Response
    ) -> CoordinateCommand.Response:
        mode = self._infer_mode(request)
        now = time.time()

        with self._gen_lock:
            self.cmd = request
            self._generator_exhausted = False
            self._last_active_context = None
            self._last_published_context = None
            self._last_published_cmd_time = 0.0
            with self._history_lock:
                self._published_command_history.clear()
            self._current_exec_id = self._next_exec_id()
            self._current_mode = mode
            self._parse_cmd(request)
            new_queue = self._prefill_current_execution(
                now=now,
                min_extra_sec=self._min_buffer_sec,
                max_extra_sec=self._max_buffer_sec,
                max_groups=max(self._max_groups_per_convert, 20),
            )

        with self._rq_lock:
            if self.result_queue:
                head = self.result_queue[0][4] - now
                tail = self.result_queue[-1][4] - now
                self.logger.warning(
                    "Replacing buffered commands due to new raw_coord request: "
                    f"req={mode}, exec_id={self._current_exec_id}, "
                    f"n={len(self.result_queue)}, "
                    f"head={head:.3f}s, tail={tail:.3f}s, now={now:.6f}"
                )
            self.result_queue = deque(new_queue)
            qlen = len(self.result_queue)
            head = (self.result_queue[0][4] - now) if self.result_queue else None
            tail = (self.result_queue[-1][4] - now) if self.result_queue else None

        self.logger.warning(
            f"raw_coord accepted: exec_id={self._current_exec_id},"
            f" mode={mode}, name='{request.name}', "
            f"lon={list(request.lon)}, lat={list(request.lat)}, frame={request.frame}, "
            f"off_lon={list(request.offset_lon)}, "
            f"off_lat={list(request.offset_lat)}, off_frame={request.offset_frame}, "
            f"absolute={all(len(x) == 2 for x in (request.lon, request.lat))}, "
            f"relative="
            f"{all(len(x) == 2 for x in (request.offset_lon, request.offset_lat))},"
            f"named={request.name != ''}, speed={request.speed}, "
            f"direct_mode={request.direct_mode}, "
            f"az_target_mode={getattr(request, 'az_target_mode', '')!r}, "
            f"now={now:.6f}"
        )
        self.logger.warning(
            f"prefill_on_accept: exec_id={self._current_exec_id},"
            f"mode={mode}, queue_len={qlen}, "
            f"head_lead={head if head is not None else 'None'},"
            f"tail_lead={tail if tail is not None else 'None'}, now={now:.6f}"
        )

        response.id = self._current_exec_id or self._idle_exec_id
        return response

    def _infer_mode(self, request: CoordinateCommand.Request) -> str:
        target_coord = (request.lon, request.lat)
        offset_coord = (request.offset_lon, request.offset_lat)
        target_scan = all(len(x) == 2 for x in target_coord)
        offset_scan = all(len(x) == 2 for x in offset_coord)
        return "scan" if (target_scan or offset_scan) else "point"

    def _describe_request(self, req: Any) -> str:
        if req is None:
            return "(idle)"

        def _safe_list(obj: Any, name: str):
            value = getattr(obj, name, None)
            if value is None:
                return []
            try:
                return list(value)
            except Exception:
                return value

        pieces = [f"mode={self._current_mode}"]
        for attr in ("name", "frame", "offset_frame"):
            if hasattr(req, attr):
                pieces.append(f"{attr}={getattr(req, attr)!r}")
        for attr in ("lon", "lat", "offset_lon", "offset_lat"):
            if hasattr(req, attr):
                pieces.append(f"{attr}={_safe_list(req, attr)!r}")
        for attr in ("speed", "margin", "direct_mode", "az_target_mode", "obsfreq"):
            if hasattr(req, attr):
                pieces.append(f"{attr}={getattr(req, attr)!r}")
        if hasattr(req, "sections"):
            try:
                pieces.append(f"n_sections={len(req.sections)}")
            except Exception:
                pieces.append("n_sections=?")
        return "(" + ", ".join(pieces) + ")"

    _SCAN_BLOCK_KIND_MAP: Dict[int, str] = {
        int(ScanBlockSectionMsg.FIRST_STANDBY): "initial_standby",
        int(ScanBlockSectionMsg.ACCELERATE): "accelerate",
        int(ScanBlockSectionMsg.LINE): "line",
        int(ScanBlockSectionMsg.TURN): "turn",
        int(ScanBlockSectionMsg.DECELERATE): "decelerate",
        int(ScanBlockSectionMsg.FINAL_STANDBY): "final_standby",
    }

    def _convert_scan_block_section(
        self, msg: ScanBlockSectionMsg
    ) -> FinderScanBlockSection:
        if int(msg.kind) == int(ScanBlockSectionMsg.MOVE_TO_ENTRY):
            raise ValueError(
                "MOVE_TO_ENTRY is not accepted by control-side scan_block. "
                "Use a separate point/move execution before the ON block."
            )
        try:
            kind = self._SCAN_BLOCK_KIND_MAP[int(msg.kind)]
        except KeyError as exc:
            raise ValueError(
                f"Unsupported scan block section kind: {msg.kind!r}"
            ) from exc

        start = (float(msg.start[0]), float(msg.start[1]))
        stop = (float(msg.stop[0]), float(msg.stop[1]))
        kwargs: Dict[str, Any] = dict(
            kind=kind,
            start=start,
            stop=stop,
            speed=float(msg.speed),
            margin=float(msg.margin),
            label=msg.label,
            line_index=int(msg.line_index),
            tight=bool(msg.tight),
        )
        if float(msg.duration_hint) > 0:
            kwargs["duration"] = float(msg.duration_hint)
        if float(getattr(msg, "turn_radius_hint", 0.0)) > 0:
            kwargs["turn_radius_hint"] = float(msg.turn_radius_hint)
        return FinderScanBlockSection(**kwargs)

    def _parse_scan_block_cmd(self, msg: ScanBlockCommand.Request) -> None:
        if msg.direct_mode:
            self.direct_mode = True
        else:
            self.direct_mode = False
        self.finder.direct_mode = self.direct_mode
        # scan_block sections are observation-derived scan coordinates.  Keep the
        # existing sky/modulo optimization regardless of direct_mode so a section
        # endpoint at 360 deg does not request an unintended full mount revolution.
        self.az_target_mode = "sky"

        sections = [
            self._convert_scan_block_section(section) for section in msg.sections
        ]
        if len(sections) == 0:
            raise ValueError("Scan block request must include at least one section.")
        if float(getattr(msg, "obsfreq", 0.0)) > 0:
            self.finder.obsfreq = float(msg.obsfreq) * u.GHz

        section_frames = {
            str(section.frame) for section in msg.sections if str(section.frame) != ""
        }
        if len(section_frames) != 1:
            raise ValueError(
                "Current control-side scan_block implementation requires exactly one "
                f"shared section frame, got: {sorted(section_frames)!r}"
            )
        scan_frame = next(iter(section_frames))

        with_offset = any(len(x) != 0 for x in (msg.offset_lon, msg.offset_lat))
        if with_offset and ((len(msg.offset_lon) != 1) or (len(msg.offset_lat) != 1)):
            raise ValueError(
                "Block-wide offset must be specified as exactly one (lon, lat) pair."
            )
        offset = None
        if with_offset:
            offset = (msg.offset_lon[0], msg.offset_lat[0], msg.offset_frame)

        args = []
        named = msg.name != ""
        has_reference = (len(msg.lon) == 1) and (len(msg.lat) == 1)
        if named:
            args = [msg.name]
        elif has_reference:
            args = [msg.lon[0], msg.lat[0], msg.frame]
        elif (len(msg.lon) != 0) or (len(msg.lat) != 0):
            raise ValueError(
                "Reference coordinate must be specified as exactly one (lon, lat) pair."
            )

        new_generator = self.finder.scan_block(
            *args,
            unit=msg.unit,
            scan_frame=scan_frame,
            sections=sections,
            offset=offset,
            cos_correction=getattr(msg, "cos_correction", False),
        )
        with self._gen_lock:
            self.executing_generator.attach(new_generator)

    def _update_scan_block_cmd(
        self, request: ScanBlockCommand.Request, response: ScanBlockCommand.Response
    ) -> ScanBlockCommand.Response:
        mode = "scan_block"
        now = time.time()

        with self._gen_lock:
            self.cmd = request
            self._generator_exhausted = False
            self._last_active_context = None
            self._last_published_context = None
            self._last_published_cmd_time = 0.0
            with self._history_lock:
                self._published_command_history.clear()
            self._current_exec_id = self._next_exec_id()
            self._current_mode = mode
            self._parse_scan_block_cmd(request)
            new_queue = self._prefill_current_execution(
                now=now,
                min_extra_sec=self._min_buffer_sec,
                max_extra_sec=self._max_buffer_sec,
                max_groups=max(self._max_groups_per_convert, 20),
            )

        with self._rq_lock:
            if self.result_queue:
                head = self.result_queue[0][4] - now
                tail = self.result_queue[-1][4] - now
                self.logger.warning(
                    "Replacing buffered commands due to new scan_block request: "
                    f"exec_id={self._current_exec_id}, n={len(self.result_queue)}, "
                    f"head={head:.3f}s, tail={tail:.3f}s, now={now:.6f}"
                )
            self.result_queue = deque(new_queue)
            qlen = len(self.result_queue)
            head = (self.result_queue[0][4] - now) if self.result_queue else None
            tail = (self.result_queue[-1][4] - now) if self.result_queue else None

        self.logger.warning(
            f"scan_block accepted: exec_id={self._current_exec_id}, "
            f"name='{request.name}', frame={request.frame}, unit={request.unit}, "
            f"n_sections={len(request.sections)}, direct_mode={request.direct_mode}, "
            f"now={now:.6f}"
        )
        self.logger.warning(
            f"prefill_on_accept: exec_id={self._current_exec_id}, "
            f"mode={mode}, queue_len={qlen}, "
            f"head_lead={head if head is not None else 'None'}, "
            f"tail_lead={tail if tail is not None else 'None'}, now={now:.6f}"
        )

        response.id = self._current_exec_id or self._idle_exec_id
        return response

    def _normalize_az_target_mode(self, mode: Any) -> str:
        mode = str(mode or "auto").strip().lower()
        aliases = {
            "": "auto",
            "auto": "auto",
            "sky": "sky",
            "mount": "mount",
            "mechanical": "mount",
            "machine": "mount",
        }
        try:
            return aliases[mode]
        except KeyError as exc:
            raise ValueError(
                f"Invalid az_target_mode={mode!r}; use 'auto', 'sky', or 'mount'."
            ) from exc

    def _is_raw_altaz_point_command(self, msg: CoordinateCommand.Request) -> bool:
        target_coord = (msg.lon, msg.lat)
        offset_coord = (msg.offset_lon, msg.offset_lat)
        target_scan = all(len(x) == 2 for x in target_coord)
        offset_scan = all(len(x) == 2 for x in offset_coord)
        named = msg.name != ""
        with_offset = any(len(x) != 0 for x in offset_coord)
        frame = str(getattr(msg, "frame", "")).strip().lower()
        return (
            (not target_scan)
            and (not offset_scan)
            and (not named)
            and (not with_offset)
            and (len(msg.lon) == 1)
            and (len(msg.lat) == 1)
            and frame in {"altaz", "azel", "horizontal"}
        )

    def _resolve_az_target_mode(self, msg: CoordinateCommand.Request) -> str:
        requested = self._normalize_az_target_mode(
            getattr(msg, "az_target_mode", "auto")
        )
        is_raw_altaz_point = self._is_raw_altaz_point_command(msg)

        if requested == "auto":
            # Backward safety rule:
            # - manual/direct raw AltAz point commands are mount mechanical angles;
            # - observation-derived coordinates, named targets, offsets, and scans
            #   remain sky modulo coordinates.
            if bool(getattr(msg, "direct_mode", False)) and is_raw_altaz_point:
                return "mount"
            return "sky"

        if requested == "mount":
            if not is_raw_altaz_point:
                raise ValueError(
                    "az_target_mode='mount' is only valid for raw numeric AltAz point "
                    "commands without name, offset, or scan endpoints. Use "
                    "az_target_mode='sky' for observation coordinates."
                )
            if not bool(getattr(msg, "direct_mode", False)):
                raise ValueError(
                    "az_target_mode='mount' requires direct_mode=True because "
                    "mount Az is an explicit continuous mechanical angle and must "
                    "not be modified by pointing/atmospheric correction."
                )
        return requested

    def _prefill_current_execution(
        self,
        *,
        now: float,
        min_extra_sec: Optional[float] = None,
        max_extra_sec: Optional[float] = None,
        max_groups: int = 10,
    ):
        offset = float(config.antenna_command_offset_sec)
        min_extra_sec = (
            self._min_buffer_sec if min_extra_sec is None else float(min_extra_sec)
        )
        max_extra_sec = (
            self._max_buffer_sec if max_extra_sec is None else float(max_extra_sec)
        )
        min_horizon = now + offset + min_extra_sec
        max_horizon = now + offset + max_extra_sec
        min_accept_t = now + offset
        eps_dup = 1e-9

        new_queue = deque()
        groups = 0
        last_t = min_accept_t

        while groups < max_groups:
            try:
                with self._gen_lock:
                    coord = next(self.executing_generator)
                self._last_active_context = self._snapshot_context(coord.context)
            except (StopIteration, TypeError):
                self.cmd = None
                self._generator_exhausted = True
                with self._gen_lock:
                    self.executing_generator.clear()
                break

            groups += 1
            try:
                t_last = float(coord.time[-1]) if len(coord.time) else None
            except Exception:
                t_last = None
            if (t_last is None) or (t_last <= min_accept_t + eps_dup):
                continue

            az, el = self._validate_drive_range(coord.az, coord.el)
            if len(az) == 0:
                continue

            context_snapshot = self._snapshot_context(coord.context)
            for _az, _el, _dAz, _dEl, _t in zip(
                az, el, coord.dAz, coord.dEl, coord.time
            ):
                if any(x is None for x in (_az, _el, _t)):
                    continue
                t_val = float(_t)
                if t_val <= float(last_t) + eps_dup:
                    continue
                cmd = (
                    float(_az.to_value("deg")),
                    float(_el.to_value("deg")),
                    float(_dAz.to_value("deg")),
                    float(_dEl.to_value("deg")),
                    t_val,
                    context_snapshot,
                )
                new_queue.append(cmd)
                last_t = t_val

            while new_queue and new_queue[-1][4] > max_horizon:
                new_queue.pop()

            if new_queue and (new_queue[-1][4] >= min_horizon):
                break

        return new_queue

    def _snapshot_context(self, context: ControlContext) -> ControlContext:
        """Freeze mutable ControlContext metadata before queue/history storage."""
        try:
            return replace(context)
        except Exception:
            frozen = ControlContext()
            try:
                frozen.update(context)
            except Exception:
                pass
            return frozen

    def _context_section_uid(self, context: Optional[ControlContext]) -> str:
        if context is None:
            return ""
        base = str(getattr(context, "section_uid", "") or "")
        exec_id = self._current_exec_id or self._idle_exec_id
        if not base:
            base = (
                f"p{int(getattr(context, 'section_plan_index', -1)):04d}:"
                f"s{int(getattr(context, 'section_sequence_index', -1)):06d}:"
                f"{str(getattr(context, 'kind', '') or 'section')}:"
                f"l{int(getattr(context, 'line_index', -1))}"
            )
        return f"{exec_id}:{base}"[:96]

    @staticmethod
    def _finite_float(value: Any, default: float = float("nan")) -> float:
        try:
            out = float(value)
        except Exception:
            return default
        return out

    def _find_current_context(
        self, query_time: float
    ) -> Tuple[Optional[ControlContext], float, str]:
        """Return the context matching the current wall time, not the future horizon.

        The antenna command stream is pre-published up to antenna_command_offset_sec
        into the future.  Progress/status displays must not use the farthest future
        command as the current section.  This method first tries section interval
        membership and then falls back to the newest published command with
        command_time <= query_time.
        """
        with self._history_lock:
            history = list(self._published_command_history)

        interval_match = None
        interval_capable = False
        for cmd_time, context, wall_time in reversed(history):
            start = getattr(context, "start", None)
            stop = getattr(context, "stop", None)
            try:
                start_f = float(start)
                stop_f = float(stop)
            except Exception:
                continue
            if not (start_f == start_f and stop_f == stop_f):
                continue
            interval_capable = True
            if start_f <= query_time < stop_f:
                interval_match = (cmd_time, context, wall_time)
                break
        if interval_match is not None:
            return interval_match[1], float(query_time), "current-time"

        if interval_capable:
            # We have interval-aware v39 contexts, but none covers the current
            # wall time.  Reporting the latest past section as active would be
            # another form of stale-state misidentification.  Let QueueStatus
            # describe the gap/lag instead.
            return None, float(query_time), "no-current-section"

        past = [entry for entry in history if float(entry[0]) <= query_time]
        if past:
            cmd_time, context, _wall_time = past[-1]
            return context, float(cmd_time), "last-published-past"

        if history:
            # All stored commands are still in the future.  Do not report that
            # future section as the current section; this is exactly the horizon
            # vs current-time ambiguity that the new status topic is meant to avoid.
            return None, float(query_time), "future-buffered"

        return None, float(query_time), "idle"

    def _make_section_status(
        self, query_time: Optional[float] = None
    ) -> AntennaSectionStatus:
        now = time.time() if query_time is None else float(query_time)
        context, command_time, basis = self._find_current_context(now)
        publish_time = time.time()
        exec_id = self._current_exec_id or self._idle_exec_id
        if context is None:
            return AntennaSectionStatus(
                active=False,
                control_id=exec_id,
                section_uid="",
                section_plan_index=-1,
                section_sequence_index=-1,
                section_kind="",
                section_label="",
                line_index=-1,
                section_start_unix=float("nan"),
                section_stop_unix=float("nan"),
                section_duration_sec=float("nan"),
                query_time_unix=now,
                publish_time_unix=publish_time,
                command_time_unix=command_time,
                nominal_fraction=float("nan"),
                fraction_valid=False,
                tight=False,
                science_line=False,
                interrupt_ok=True,
                geometry_valid=False,
                section_frame="",
                section_unit="",
                section_start_lon_deg=float("nan"),
                section_start_lat_deg=float("nan"),
                section_stop_lon_deg=float("nan"),
                section_stop_lat_deg=float("nan"),
                section_speed_deg_per_sec=float("nan"),
                status_basis=basis,
            )

        start = self._finite_float(getattr(context, "start", None))
        stop = self._finite_float(getattr(context, "stop", None))
        duration = self._finite_float(getattr(context, "duration", None))
        if not (duration == duration) and (start == start) and (stop == stop):
            duration = stop - start
        fraction = float("nan")
        fraction_valid = False
        if (duration == duration) and duration > 0 and (start == start):
            fraction = max(0.0, min(1.0, (command_time - start) / duration))
            fraction_valid = True

        kind = str(getattr(context, "kind", "") or "")[:64]
        line_index = int(getattr(context, "line_index", -1))
        tight = bool(getattr(context, "tight", False))
        infinite = bool(getattr(context, "infinite", False))
        waypoint = bool(getattr(context, "waypoint", False))
        geometry_valid = bool(getattr(context, "geometry_valid", False))
        return AntennaSectionStatus(
            active=True,
            control_id=exec_id,
            section_uid=self._context_section_uid(context),
            section_plan_index=int(getattr(context, "section_plan_index", -1)),
            section_sequence_index=int(getattr(context, "section_sequence_index", -1)),
            section_kind=kind,
            section_label=str(getattr(context, "label", "") or "")[:64],
            line_index=line_index,
            section_start_unix=start,
            section_stop_unix=stop,
            section_duration_sec=duration,
            query_time_unix=now,
            publish_time_unix=publish_time,
            command_time_unix=command_time,
            nominal_fraction=fraction,
            fraction_valid=fraction_valid,
            tight=tight,
            science_line=bool(tight and kind in {"line", "scanning"}),
            interrupt_ok=bool(infinite and (not waypoint)),
            geometry_valid=geometry_valid,
            section_frame=str(getattr(context, "section_frame", "") or "")[:32],
            section_unit=str(getattr(context, "section_unit", "") or "")[:16],
            section_start_lon_deg=self._finite_float(
                getattr(context, "section_start_lon_deg", None)
            ),
            section_start_lat_deg=self._finite_float(
                getattr(context, "section_start_lat_deg", None)
            ),
            section_stop_lon_deg=self._finite_float(
                getattr(context, "section_stop_lon_deg", None)
            ),
            section_stop_lat_deg=self._finite_float(
                getattr(context, "section_stop_lat_deg", None)
            ),
            section_speed_deg_per_sec=self._finite_float(
                getattr(context, "section_speed_deg_per_sec", None)
            ),
            status_basis=basis[:32],
        )

    def _make_queue_status(
        self, publish_time: Optional[float] = None, reason: str = "timer"
    ) -> AntennaCommandQueueStatus:
        now = time.time() if publish_time is None else float(publish_time)
        with self._rq_lock:
            qlen = len(self.result_queue)
            head_lead = (
                (self.result_queue[0][4] - now) if self.result_queue else float("nan")
            )
            tail_lead = (
                (self.result_queue[-1][4] - now) if self.result_queue else float("nan")
            )
        publish_gap = (
            now - self._last_publish_wall_time
            if self._last_publish_wall_time
            else float("nan")
        )
        return AntennaCommandQueueStatus(
            active=bool(
                self._current_exec_id is not None or self.cmd is not None or qlen > 0
            ),
            control_id=self._current_exec_id or self._idle_exec_id,
            mode=str(self._current_mode or "idle")[:32],
            publish_time_unix=now,
            command_offset_sec=float(config.antenna_command_offset_sec),
            min_buffer_sec=float(self._min_buffer_sec),
            max_buffer_sec=float(self._max_buffer_sec),
            queue_length=int(qlen),
            head_lead_sec=float(head_lead),
            tail_lead_sec=float(tail_lead),
            last_publish_wall_time_unix=float(self._last_publish_wall_time),
            last_published_cmd_time_unix=float(self._last_published_cmd_time),
            publish_gap_sec=float(publish_gap),
            generator_exhausted=bool(self._generator_exhausted),
            guard_latched=bool(self._guard_latched),
            reason=str(reason or self._last_queue_status_reason or "")[:64],
        )

    def publish_control_diagnostics(self) -> None:
        self.section_status_publisher.publish(self._make_section_status())
        self.queue_status_publisher.publish(self._make_queue_status(reason="timer"))

    def _update_enc(self, msg: CoordMsg) -> None:
        if (msg.unit != "deg") or (msg.frame != "altaz"):
            self.logger.debug("Invalid encoder reading detected.")
            return
        self.enc_az = msg.lon
        self.enc_el = msg.lat
        self.enc_time = msg.time

    def command_realtime(self) -> None:
        if self.status.critical():
            if not self._guard_latched:
                now = time.time()
                with self._rq_lock:
                    qlen = len(self.result_queue)
                    tail_lead = (
                        (self.result_queue[-1][4] - now) if self.result_queue else None
                    )
                self.logger.warning(
                    f"Guard condition activated:"
                    f"now={now:.6f}, last_publish_wall={self._last_publish_wall_time}, "
                    f"last_publish_cmd_time={self._last_published_cmd_time},"
                    f" queue_len={qlen}, "
                    f"tail_lead={tail_lead if tail_lead is not None else 'None'}, "
                    f"exec_id={self._current_exec_id or self._idle_exec_id},"
                    f" mode={self._current_mode}",
                    throttle_duration_sec=1,
                )
                self._guard_latched = True
                return self.gc.trigger()
            return

        if self._guard_latched:
            self.logger.warning(f"Guard latch cleared: now={time.time():.6f}")
            self._guard_latched = False

        now = time.time()
        offset = float(config.antenna_command_offset_sec)
        target_time = now + offset

        cmds = []
        queue_len = 0
        head_lead = None

        with self._rq_lock:
            while self.result_queue and self.result_queue[0][4] <= now:
                self.result_queue.popleft()

            queue_len = len(self.result_queue)
            if queue_len > 0:
                head_lead = self.result_queue[0][4] - now

            max_publish = max(50, int(config.antenna_command_frequency))
            while (
                self.result_queue
                and self.result_queue[0][4] <= target_time
                and len(cmds) < max_publish
            ):
                cmds.append(self.result_queue.popleft())

        if head_lead is not None:
            self.logger.debug(
                f"Queue Info: length={queue_len},"
                f"head_lead={head_lead:.3f}s, published={len(cmds)}",
                throttle_duration_sec=1.0,
            )
        else:
            self.logger.debug(
                "Queue Info: length=0 (Buffer empty!)",
                throttle_duration_sec=1.0,
            )

        if self._last_publish_wall_time and (now - self._last_publish_wall_time > 0.5):
            self.logger.warning(
                f"Command publish gap detected: "
                f"gap={now - self._last_publish_wall_time:.3f}s"
                f"queue_len={queue_len},"
                f"head_lead={head_lead if head_lead is not None else 'None'}, "
                f"exec_id={self._current_exec_id or self._idle_exec_id},"
                f"mode={self._current_mode}, now={now:.6f}"
            )

        for cmd in cmds:
            msg = CoordMsg(
                lon=float(cmd[0]),
                lat=float(cmd[1]),
                dlon=float(cmd[2]),
                dlat=float(cmd[3]),
                time=float(cmd[4]),
                unit="deg",
                frame="altaz",
            )
            self.publisher.publish(msg)
            self._last_publish_wall_time = now
            self._last_publish_cmd_stamp = float(cmd[4])
            self._last_published_cmd_time = max(
                self._last_published_cmd_time, float(cmd[4])
            )
            self._last_published_context = cmd[5]
            with self._history_lock:
                self._published_command_history.append((float(cmd[4]), cmd[5], now))

        # IMPORTANT: publish control status from the REALTIME side,
        # not convert-side future state.
        if cmds and (self._last_published_context is not None):
            self.telemetry(self._last_published_context)

    def _parse_cmd(self, msg: CoordinateCommand.Request) -> None:
        target_coord = (msg.lon, msg.lat)
        offset_coord = (msg.offset_lon, msg.offset_lat)

        target_scan = all(len(x) == 2 for x in target_coord)
        offset_scan = all(len(x) == 2 for x in offset_coord)
        scan = target_scan or offset_scan
        named = msg.name != ""
        with_offset = any(len(x) != 0 for x in offset_coord)

        if msg.direct_mode:
            self.direct_mode = True
        else:
            self.direct_mode = False
        self.finder.direct_mode = self.direct_mode
        self.az_target_mode = self._resolve_az_target_mode(msg)
        self.logger.info(
            "Resolved az_target_mode: "
            f"requested={getattr(msg, 'az_target_mode', '')!r}, "
            f"resolved={self.az_target_mode!r}, frame={msg.frame!r}, "
            f"direct_mode={msg.direct_mode}"
        )

        if (not scan) and (not named) and (not with_offset):
            self.logger.debug(f"Got POINT-TO-COORD command: {msg}")
            new_generator = self.finder.track(
                msg.lon[0],
                msg.lat[0],
                msg.frame,
                unit=msg.unit,
                cos_correction=getattr(msg, "cos_correction", False),
            )
        elif (not scan) and (not named) and with_offset:
            self.logger.debug(f"Got POINT-TO-COORD-WITH-OFFSET command: {msg}")
            new_generator = self.finder.track(
                msg.lon[0],
                msg.lat[0],
                msg.frame,
                offset=(msg.offset_lon[0], msg.offset_lat[0], msg.offset_frame),
                unit=msg.unit,
                cos_correction=getattr(msg, "cos_correction", False),
            )
        elif (not scan) and named and (not with_offset):
            self.logger.debug(f"Got POINT-TO-NAMED-TARGET command: {msg}")
            new_generator = self.finder.track(
                msg.name, cos_correction=getattr(msg, "cos_correction", False)
            )
        elif (not scan) and named and with_offset:
            self.logger.debug(f"Got POINT-TO-NAMED-TARGET-WITH-OFFSET command: {msg}")
            new_generator = self.finder.track(
                msg.name,
                offset=(msg.offset_lon[0], msg.offset_lat[0], msg.offset_frame),
                unit=msg.unit,
                cos_correction=getattr(msg, "cos_correction", False),
            )
        elif target_scan and (not named):
            self.logger.debug(f"Got SCAN-IN-ABSOLUTE-COORD command: {msg}")
            new_generator = self.finder.linear(
                start=(msg.lon[0], msg.lat[0]),
                stop=(msg.lon[1], msg.lat[1]),
                scan_frame=msg.frame,
                speed=abs(msg.speed),
                unit=msg.unit,
                margin=msg.margin,
                cos_correction=getattr(msg, "cos_correction", False),
            )
        elif offset_scan and (not named):
            self.logger.debug(f"Got SCAN-IN-RELATIVE-COORD command: {msg}")
            new_generator = self.finder.linear(
                msg.lon,
                msg.lat,
                msg.frame,
                start=(msg.offset_lon[0], msg.offset_lat[0]),
                stop=(msg.offset_lon[1], msg.offset_lat[1]),
                scan_frame=msg.offset_frame,
                speed=abs(msg.speed),
                unit=msg.unit,
                margin=msg.margin,
                cos_correction=getattr(msg, "cos_correction", False),
            )
        elif offset_scan and named:
            self.logger.debug(f"Got SCAN-IN-RELATIVE-TO-NAMED-TARGET command: {msg}")
            new_generator = self.finder.linear(
                msg.name,
                start=(msg.offset_lon[0], msg.offset_lat[0]),
                stop=(msg.offset_lon[1], msg.offset_lat[1]),
                scan_frame=msg.offset_frame,
                speed=abs(msg.speed),
                unit=msg.unit,
                margin=msg.margin,
                cos_correction=getattr(msg, "cos_correction", False),
            )
        else:
            raise ValueError(f"Cannot determine command type for {msg}")
        with self._gen_lock:
            self.executing_generator.attach(new_generator)

    def convert(self) -> None:
        now = time.time()
        if (
            (self.cmd is not None)
            and (self.enc_time != 0)
            and (self.enc_time < now - 5)
        ):
            self.logger.error(
                "Lost the communication with the encoder. Command to drive to "
                f"{self.cmd} has been discarded."
            )
            self._transition_to_idle(reason="encoder_lost")
            with self._rq_lock:
                self.result_queue.clear()
            return self.telemetry(None)

        if self._generator_exhausted:
            with self._rq_lock:
                draining = len(self.result_queue) > 0
                tail_lead = (
                    (self.result_queue[-1][4] - now) if self.result_queue else None
                )
            if draining:
                if tail_lead is not None:
                    self.logger.debug(
                        f"Generator exhausted;"
                        f"draining buffered tail for {tail_lead:.3f}s",
                        throttle_duration_sec=0.5,
                    )
                # Keep status tied to the last realtime-published context
                # while future commands are still in flight.
                if self._last_published_context is not None:
                    return self.telemetry(self._last_published_context)
                if self._last_active_context is not None:
                    return self.telemetry(self._last_active_context)
                return

            # Queue may be empty ~offset sec
            # before the last future command is actually due.
            if self._last_published_cmd_time > now:
                self.logger.debug(
                    f"Generator exhausted and queue empty,"
                    f"but waiting for last published command time: "
                    f"last_cmd_t={self._last_published_cmd_time:.6f}, now={now:.6f}",
                    throttle_duration_sec=0.5,
                )
                if self._last_published_context is not None:
                    return self.telemetry(self._last_published_context)
                if self._last_active_context is not None:
                    return self.telemetry(self._last_active_context)
                return

            self.logger.info("Buffered tail drained; finishing current execution")
            self._transition_to_idle(reason="drained")
            return self.telemetry(None)

        if self.cmd is None:
            return self.telemetry(None)

        eps_dup = 1e-9

        for _ in range(self._max_groups_per_convert):
            now = time.time()
            offset = float(config.antenna_command_offset_sec)
            min_horizon = now + offset + self._min_buffer_sec
            max_horizon = now + offset + self._max_buffer_sec
            min_accept_t = now + offset

            with self._rq_lock:
                while self.result_queue and self.result_queue[-1][4] > max_horizon:
                    self.result_queue.pop()
                tail_time = self.result_queue[-1][4] if self.result_queue else None

            if (tail_time is not None) and (tail_time >= min_horizon):
                break

            try:
                with self._gen_lock:
                    coord = next(self.executing_generator)
                self._last_active_context = self._snapshot_context(coord.context)
            except (StopIteration, TypeError):
                self.cmd = None
                self._generator_exhausted = True
                with self._gen_lock:
                    self.executing_generator.clear()
                with self._rq_lock:
                    qlen = len(self.result_queue)
                    tail_lead = (
                        (self.result_queue[-1][4] - time.time())
                        if self.result_queue
                        else None
                    )
                self.logger.warning(
                    "Coordinate generator exhausted; entering drain mode: "
                    f"queue_len={qlen},"
                    f"tail_lead={tail_lead if tail_lead is not None else 'None'}"
                )
                if qlen > 0:
                    if self._last_published_context is not None:
                        return self.telemetry(self._last_published_context)
                    if self._last_active_context is not None:
                        return self.telemetry(self._last_active_context)
                    return

                if self._last_published_cmd_time > time.time():
                    if self._last_published_context is not None:
                        return self.telemetry(self._last_published_context)
                    if self._last_active_context is not None:
                        return self.telemetry(self._last_active_context)
                    return

                self._transition_to_idle(reason="exhausted_empty")
                return self.telemetry(None)

            try:
                t_last = float(coord.time[-1]) if len(coord.time) else None
            except Exception:
                t_last = None
            if (t_last is None) or (t_last <= min_accept_t + eps_dup):
                continue

            az, el = self._validate_drive_range(coord.az, coord.el)
            if len(az) == 0:
                continue

            with self._rq_lock:
                last_t = self.result_queue[-1][4] if self.result_queue else None
                if last_t is None:
                    last_t = min_accept_t
                else:
                    last_t = max(float(last_t), float(min_accept_t))

                context_snapshot = self._snapshot_context(coord.context)
                for _az, _el, _dAz, _dEl, _t in zip(
                    az, el, coord.dAz, coord.dEl, coord.time
                ):
                    if any(x is None for x in (_az, _el, _t)):
                        continue
                    t_val = float(_t)
                    if t_val <= float(last_t) + eps_dup:
                        continue

                    cmd = (
                        float(_az.to_value("deg")),
                        float(_el.to_value("deg")),
                        float(_dAz.to_value("deg")),
                        float(_dEl.to_value("deg")),
                        t_val,
                        context_snapshot,
                    )
                    self.result_queue.append(cmd)
                    last_t = t_val

                while self.result_queue and self.result_queue[-1][4] > max_horizon:
                    self.result_queue.pop()

    def _validate_drive_range(self, az, el) -> Tuple:
        enc_az = 180 if self.enc_az is None else self.enc_az
        enc_el = 45 if self.enc_el is None else self.enc_el

        if self.az_target_mode == "mount":
            _az = self._validate_mount_az_target(az)
        else:
            _az = self.optimizer["az"].optimize(enc_az, az.to_value("deg"), unit="deg")
        _el = self.optimizer["el"].optimize(enc_el, el.to_value("deg"), unit="deg")

        if (_az is not None) and (_el is not None):
            return _az, _el
        return [], []

    def _validate_mount_az_target(self, az):
        """Validate explicit mount azimuth without modulo candidate selection.

        ``az_target_mode='mount'`` means the numeric Az value is already the
        intended continuous mount coordinate.  Do not call DriveLimitChecker here,
        because it intentionally treats 0, 360, -360, ... as equivalent sky
        azimuths and would convert e.g. target=360 near current=-5 into target=0.
        """
        az_q = az.to("deg") if hasattr(az, "to") else az
        az_deg = np.asanyarray(
            az_q.to_value("deg") if hasattr(az_q, "to_value") else az_q
        )
        critical = self.optimizer["az"].limit
        warning = self.optimizer["az"].preferred_limit
        critical_lower = critical.lower.to_value("deg")
        critical_upper = critical.upper.to_value("deg")
        warning_lower = warning.lower.to_value("deg")
        warning_upper = warning.upper.to_value("deg")
        if not bool(((critical_lower <= az_deg) & (az_deg <= critical_upper)).all()):
            self.logger.warning(
                "Mount Az target is out of critical drive range: "
                f"target={az_q}, limit={critical}"
            )
            return None
        if not bool(((warning_lower <= az_deg) & (az_deg <= warning_upper)).all()):
            self.logger.warning("Mount Az target nears drive range limit.")
        return az_q

    def _update_weather(self, msg: WeatherMsg) -> None:
        if (self.last_status is not None) and (self.last_status.tight):
            return

        if self.direct_mode:
            self.finder.temperature = 0
            self.finder.pressure = 0
            self.finder.relative_humidity = 0
        else:
            self.finder.temperature = msg.temperature
            self.finder.pressure = msg.pressure
            self.finder.relative_humidity = msg.humidity

    def telemetry(self, status: Optional[ControlContext]) -> None:
        if status is None:
            exec_id = self._idle_exec_id
            msg = ControlStatus(
                controlled=False,
                tight=False,
                remote=True,
                id=exec_id,
                interrupt_ok=True,
                time=time.time() + config.antenna_command_offset_sec,
                section_kind="",
                section_label="",
                line_index=-1,
            )
        else:
            exec_id = self._current_exec_id or self._idle_exec_id
            msg = ControlStatus(
                controlled=True,
                tight=status.tight,
                remote=True,
                id=exec_id,
                interrupt_ok=status.infinite and (not status.waypoint),
                time=status.start,
                section_kind=str(getattr(status, "kind", "") or "")[:64],
                section_label=str(getattr(status, "label", "") or "")[:64],
                line_index=int(getattr(status, "line_index", -1)),
            )
        self.last_status = msg
        self.status_publisher.publish(msg)

    def next(self, msg: Boolean) -> None:
        with self._gen_lock:
            self.executing_generator.will_send(True)
