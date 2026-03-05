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

        # ---------------------------------------------------------------------
        # Command buffer (producer: convert(), consumer: command_realtime()).
        #
        # - Each entry is a 5-tuple:
        #     (az_deg, el_deg, dAz_deg, dEl_deg, cmd_time_unix)
        # - `cmd_time_unix` is the *future* time when the command should be valid.
        # - `command_realtime()` publishes the earliest future command and drops
        #   outdated ones.
        # - `convert()` replenishes the buffer in groups produced by
        #   CoordinateGeneratorManager (PathFinder.from_function()).
        #
        # Performance / safety notes:
        # - Use deque for O(1) popleft.
        # - Avoid rebuilding the entire queue for dedup (the original code did a
        #   list(filter(...)) per point).
        # - When convert() is delayed (e.g., heavy astropy transforms/IERS/cache
        #   spikes), replenish multiple groups in one call to avoid run-out.
        # - Fast-forward blocks that are already behind the desired schedule
        #   (now + command_offset).
        # ---------------------------------------------------------------------
        self.result_queue = deque()
        self._rq_lock = threading.Lock()

        self._gen_lock = threading.RLock()

        # Buffer policy (relative to "now + antenna_command_offset_sec"):
        # Keep at least this much headroom, but do not grow without bound.
        # These values were chosen to be conservative for 50 Hz control.
        self._min_buffer_sec = 0.5
        self._max_buffer_sec = 3.0
        self._max_groups_per_convert = 10  # capped to avoid long blocking in convert()

        self.tracking_ok = False
        self.executing_generator = CoordinateGeneratorManager()
        self.last_status = None

        self.gc = self.create_guard_condition(self._clear_cmd)

    def _clear_cmd(self) -> None:
        self.cmd = None
        with self._rq_lock:
            self.result_queue.clear()

    def _update_cmd(
        self, request: CoordinateCommand.Request, response: CoordinateCommand.Response
    ) -> CoordinateCommand.Response:
        """Update the target coordinate command.

        When new command has been received, conversion result will stop for a moment,
        since coordinate conversion for new coordinate cannot be performed immediately.
        This suspension won't affect PID control, as it checks the command time.

        Notes
        -----
        The conversion result will be cleared immediately after receiving the new
        command. This won't irregular drive, as this command doesn't contain detailed
        and frequent coordinate information. This is the case for scan command, as the
        command only contains start/stop position and scan speed.

        """
        with self._gen_lock:
            # Prevent convert() from consuming a half-updated generator/cmd.
            self.cmd = request
            self._parse_cmd(request)
            with self._rq_lock:
                self.result_queue.clear()
            response.id = str(id(self.executing_generator.get()))
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
            # Avoid sudden resumption of telescope drive
            return self.gc.trigger()

        # Publish the earliest future command and drop outdated ones.
        now = time.time()
        cmd = None
        with self._rq_lock:
            while self.result_queue and self.result_queue[0][4] <= now:
                self.result_queue.popleft()
            if self.result_queue and self.result_queue[0][4] > now:
                cmd = self.result_queue.popleft()

        if cmd is not None:
            msg = CoordMsg(
                lon=cmd[0],
                lat=cmd[1],
                dlon=cmd[2],
                dlat=cmd[3],
                time=cmd[4],
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
        """Replenish `result_queue` with future commands.

        This method is called periodically (0.2 s). It generates command groups
        from `executing_generator` and appends them to `result_queue`.

        Key design:
        - Keep a buffer window ahead of wall clock time:
            [now + offset + min_buffer, now + offset + max_buffer]
        - If convert() was delayed and the generator is behind, fast-forward by
          discarding groups whose last cmd.time is not beyond (now + offset).
        - Generate multiple groups per call (capped) to catch up quickly.
        """
        if (self.cmd is not None) and (self.enc_time != 0) and (self.enc_time < time.time() - 5):
            # Don't resume normal operation after communication with encoder lost for 5s
            self.logger.error(
                "Lost the communication with the encoder. Command to drive to "
                f"{self.cmd} has been discarded."
            )
            self.cmd = None
        if self.cmd is None:
            return self.telemetry(None)

        eps_dup = 1e-9

        # Replenishment loop.
        for _ in range(self._max_groups_per_convert):
            now = time.time()
            offset = float(config.antenna_command_offset_sec)
            min_horizon = now + offset + self._min_buffer_sec
            max_horizon = now + offset + self._max_buffer_sec
            min_accept_t = now + offset  # keep schedule in the future

            # Trim too-far future (safety net) and check current tail.
            with self._rq_lock:
                while self.result_queue and self.result_queue[-1][4] > max_horizon:
                    self.result_queue.pop()
                tail_time = self.result_queue[-1][4] if self.result_queue else None

            if (tail_time is not None) and (tail_time >= min_horizon):
                break

            try:
                with self._gen_lock:
                    coord = next(self.executing_generator)
                self.telemetry(coord.context)
            except (StopIteration, TypeError):
                self.cmd = None
                with self._gen_lock:
                    self.executing_generator.clear()
                return self.telemetry(None)

            # Fast-forward: if this whole group is not beyond min_accept_t,
            # discard it quickly and continue.
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
                # Ensure we never append commands at/before min_accept_t
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
                        # duplicate/backward timestamp or too-close
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

                # Safety trim again (in case a group overshoots max_horizon)
                while self.result_queue and self.result_queue[-1][4] > max_horizon:
                    self.result_queue.pop()

        # If we never produced a valid coord.context in the loop, keep previous status.

    def _validate_drive_range(self, az, el) -> Tuple:  # All values are Quantity.
        enc_az = 180 if self.enc_az is None else self.enc_az
        enc_el = 45 if self.enc_el is None else self.enc_el

        _az = self.optimizer["az"].optimize(enc_az, az.to_value("deg"), unit="deg")
        _el = self.optimizer["el"].optimize(enc_el, el.to_value("deg"), unit="deg")

        if (_az is not None) and (_el is not None):
            return _az, _el
        return [], []

    def _update_weather(self, msg: WeatherMsg) -> None:
        if (self.last_status is not None) and (self.last_status.tight):
            # Updating weather data may cause jump in calculated coordinate, so the
            # parameter update is disabled while antenna is in tight control.
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
        with self._gen_lock:
            exec_id = str(id(self.executing_generator.get()))
        if status is None:
            msg = ControlStatus(
                controlled=False,
                tight=False,
                remote=True,
                id=exec_id,
                interrupt_ok=True,
                time=time.time() + config.antenna_command_offset_sec,
            )
        else:
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
