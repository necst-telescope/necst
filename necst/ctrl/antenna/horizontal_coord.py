__all__ = ["HorizontalCoord"]

import time
from collections import deque
import threading
from typing import Optional, Tuple

from neclib.coordinates import CoordinateGeneratorManager, DriveLimitChecker, PathFinder
from neclib.coordinates.paths import ControlContext
from necst_msgs.msg import Boolean, ControlStatus, CoordMsg, WeatherMsg
from necst_msgs.srv import CoordinateCommand

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

        self.status_publisher = topic.antenna_control_status.publisher(self)

        self.create_timer(1 / config.antenna_command_frequency, self.command_realtime)
        self.create_timer(0.2, self.convert)

        self.result_queue = deque()
        self._rq_lock = threading.Lock()
        self._gen_lock = threading.RLock()

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

        self.gc = self.create_guard_condition(self._clear_cmd)

    def _next_exec_id(self) -> str:
        self._exec_seq += 1
        return f"exec-{self._exec_seq}"

    def _transition_to_idle(self) -> None:
        with self._gen_lock:
            self.executing_generator.clear()
            self._current_exec_id = None
            self._generator_exhausted = False
            self._last_active_context = None
            self._idle_exec_id = self._next_exec_id()
        self.cmd = None

    def _clear_cmd(self) -> None:
        self._transition_to_idle()
        with self._rq_lock:
            self.result_queue.clear()


    def _describe_request(self, msg: CoordinateCommand.Request) -> str:
        try:
            lon = list(getattr(msg, 'lon', []) or [])
        except Exception:
            lon = []
        try:
            lat = list(getattr(msg, 'lat', []) or [])
        except Exception:
            lat = []
        try:
            off_lon = list(getattr(msg, 'offset_lon', []) or [])
        except Exception:
            off_lon = []
        try:
            off_lat = list(getattr(msg, 'offset_lat', []) or [])
        except Exception:
            off_lat = []
        name = getattr(msg, 'name', '')
        direct_mode = getattr(msg, 'direct_mode', None)
        speed = getattr(msg, 'speed', None)
        frame = getattr(msg, 'frame', None)
        off_frame = getattr(msg, 'offset_frame', None)

        absolute = (len(lon) > 0) and (len(lat) > 0)
        relative = (len(off_lon) > 0) and (len(off_lat) > 0)
        named = (name != '')
        scan = (len(lon) == 2) or (len(off_lon) == 2)
        if scan:
            mode = 'scan'
        else:
            mode = 'point'
        return (
            f"mode={mode}, name={name!r}, lon={lon}, lat={lat}, frame={frame}, "
            f"off_lon={off_lon}, off_lat={off_lat}, off_frame={off_frame}, "
            f"absolute={absolute}, relative={relative}, named={named}, "
            f"speed={speed}, direct_mode={direct_mode}"
        )

    def _update_cmd(
        self, request: CoordinateCommand.Request, response: CoordinateCommand.Response
    ) -> CoordinateCommand.Response:
        with self._gen_lock:
            self.cmd = request
            self._generator_exhausted = False
            self._last_active_context = None
            self._current_exec_id = self._next_exec_id()
            self._parse_cmd(request)

        now = time.time()
        self.logger.warning(
            f"raw_coord accepted: exec_id={self._current_exec_id}, "
            f"{self._describe_request(request)}, now={now:.6f}"
        )

        with self._rq_lock:
            if self.result_queue:
                now = time.time()
                head = self.result_queue[0][4] - now
                tail = self.result_queue[-1][4] - now
                self.logger.warning(
                    "Clearing buffered commands due to new raw_coord request: "
                    f"n={len(self.result_queue)}, head={head:.3f}s, tail={tail:.3f}s"
                )
            self.result_queue.clear()

        response.id = self._current_exec_id or self._idle_exec_id
        return response

    def _update_enc(self, msg: CoordMsg) -> None:
        if (msg.unit != "deg") or (msg.frame != "altaz"):
            self.logger.debug("Invalid encoder reading detected.")
            return
        self.enc_az = msg.lon
        self.enc_el = msg.lat
        self.enc_time = msg.time

    def command_realtime(self) -> None:
        if self.status.critical():
            self.logger.warning("Guard condition activated", throttle_duration_sec=1)
            return self.gc.trigger()

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

            max_publish = 5
            while (
                self.result_queue
                and self.result_queue[0][4] <= target_time
                and len(cmds) < max_publish
            ):
                cmds.append(self.result_queue.popleft())

        if head_lead is not None:
            self.logger.debug(
                f"Queue Info: length={queue_len}, head_lead={head_lead:.3f}s, published={len(cmds)}",
                throttle_duration_sec=1.0,
            )
        else:
            self.logger.debug(
                "Queue Info: length=0 (Buffer empty!)",
                throttle_duration_sec=1.0,
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

        if (not scan) and (not named) and (not with_offset):
            self.logger.debug(f"Got POINT-TO-COORD command: {msg}")
            new_generator = self.finder.track(
                msg.lon[0],
                msg.lat[0],
                msg.frame,
                unit=msg.unit,
                cos_correction=getattr(msg, 'cos_correction', False),
            )
        elif (not scan) and (not named) and with_offset:
            self.logger.debug(f"Got POINT-TO-COORD-WITH-OFFSET command: {msg}")
            new_generator = self.finder.track(
                msg.lon[0],
                msg.lat[0],
                msg.frame,
                offset=(msg.offset_lon[0], msg.offset_lat[0], msg.offset_frame),
                unit=msg.unit,
                cos_correction=getattr(msg, 'cos_correction', False),
            )
        elif (not scan) and named and (not with_offset):
            self.logger.debug(f"Got POINT-TO-NAMED-TARGET command: {msg}")
            new_generator = self.finder.track(msg.name, cos_correction=getattr(msg, 'cos_correction', False))
        elif (not scan) and named and with_offset:
            self.logger.debug(f"Got POINT-TO-NAMED-TARGET-WITH-OFFSET command: {msg}")
            new_generator = self.finder.track(
                msg.name,
                offset=(msg.offset_lon[0], msg.offset_lat[0], msg.offset_frame),
                unit=msg.unit,
                cos_correction=getattr(msg, 'cos_correction', False),
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
                cos_correction=getattr(msg, 'cos_correction', False),
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
                cos_correction=getattr(msg, 'cos_correction', False),
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
                cos_correction=getattr(msg, 'cos_correction', False),
            )
        else:
            raise ValueError(f"Cannot determine command type for {msg}")
        with self._gen_lock:
            self.executing_generator.attach(new_generator)

    def convert(self) -> None:
        if (self.cmd is not None) and (self.enc_time != 0) and (self.enc_time < time.time() - 5):
            self.logger.error(
                "Lost the communication with the encoder. Command to drive to "
                f"{self.cmd} has been discarded."
            )
            self._transition_to_idle()
            with self._rq_lock:
                self.result_queue.clear()
            return self.telemetry(None)

        if self._generator_exhausted:
            with self._rq_lock:
                draining = len(self.result_queue) > 0
                tail_lead = (self.result_queue[-1][4] - time.time()) if self.result_queue else None
            if draining:
                if tail_lead is not None:
                    self.logger.debug(
                        f"Generator exhausted; draining buffered tail for {tail_lead:.3f}s",
                        throttle_duration_sec=0.5,
                    )
                if self._last_active_context is not None:
                    return self.telemetry(self._last_active_context)
                return

            self.logger.info("Buffered tail drained; finishing current execution")
            self._transition_to_idle()
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
                self._last_active_context = coord.context
                self.telemetry(coord.context)
            except (StopIteration, TypeError):
                self.cmd = None
                self._generator_exhausted = True
                with self._rq_lock:
                    qlen = len(self.result_queue)
                    tail_lead = (self.result_queue[-1][4] - time.time()) if self.result_queue else None
                self.logger.warning(
                    "Coordinate generator exhausted; entering drain mode: "
                    f"queue_len={qlen}, tail_lead={tail_lead if tail_lead is not None else 'None'}"
                )
                if qlen > 0:
                    if self._last_active_context is not None:
                        return self.telemetry(self._last_active_context)
                    return

                self._transition_to_idle()
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
                    )
                    self.result_queue.append(cmd)
                    last_t = t_val

                while self.result_queue and self.result_queue[-1][4] > max_horizon:
                    self.result_queue.pop()

    def _validate_drive_range(self, az, el) -> Tuple:
        enc_az = 180 if self.enc_az is None else self.enc_az
        enc_el = 45 if self.enc_el is None else self.enc_el

        _az = self.optimizer["az"].optimize(enc_az, az.to_value("deg"), unit="deg")
        _el = self.optimizer["el"].optimize(enc_el, el.to_value("deg"), unit="deg")

        if (_az is not None) and (_el is not None):
            return _az, _el
        return [], []

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
            )
        self.last_status = msg
        self.status_publisher.publish(msg)

    def next(self, msg: Boolean) -> None:
        with self._gen_lock:
            self.executing_generator.will_send(True)
