__all__ = ["DomeController"]

import time
from typing import Optional, Tuple

from neclib.coordinates import CoordinateGeneratorManager, DriveLimitChecker, PathFinder
from neclib.coordinates.paths import ControlContext
from necst_msgs.msg import ControlStatus, CoordMsg
from necst_msgs.srv import CoordinateCommand, DomeSync

from ... import config, namespace, service, topic
from ...core import AlertHandlerNode


class DomeController(AlertHandlerNode):
    NodeName = "dome_coord"
    Namespace = namespace.dome

    def __init__(self) -> None:
        super().__init__(self.NodeName, namespace=self.Namespace)
        self.logger = self.get_logger()

        self.cmd = None
        self.enc_az = None
        self.enc_time = 0
        self.dome_sync = False

        self.finder = PathFinder(
            config.location, config.antenna_pointing_parameter_path
        )
        drive_limit = config.dome_drive
        self.optimizer = DriveLimitChecker(
            drive_limit.critical_limit_az, drive_limit.warning_limit_az
        )

        self.publisher = topic.dome_altaz_cmd.publisher(self)
        topic.dome_encoder.subscription(self, self._update_enc)
        service.dome_coord.service(self, self._update_cmd)
        service.dome_sync.service(self, self._update_sync_mode)

        self.status_publisher = topic.dome_control_status.publisher(self)

        self.create_timer(1 / config.dome_command_frequency, self.command_realtime)
        self.create_timer(0.5, self.convert)

        self.result_queue = []
        self.tracking_ok = False
        self.executing_generator = CoordinateGeneratorManager()
        self.last_status = None

        self.direct_mode = True

        self.gc = self.create_guard_condition(self._clear_cmd)

    def _clear_cmd(self) -> None:
        self.cmd = None
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
        if self.dome_sync:
            response.check = False
            return response
        else:
            self.cmd = request
            self._parse_cmd(request)
            self.result_queue.clear()

            response.id = str(id(self.executing_generator.get()))
            return response

    def _update_sync_mode(
        self, request: DomeSync.Request, response: DomeSync.Response
    ) -> DomeSync.Response:
        self.dome_sync = request.dome_sync
        response.check = self.dome_sync
        self._clear_cmd()
        return response

    def _update_enc(self, msg: CoordMsg) -> None:
        if (msg.unit != "deg") or (msg.frame != "altaz"):
            self.logger.debug("Invalid encoder reading detected.")
            return
        self.enc_az = msg.lon
        self.enc_time = msg.time

    def command_realtime(self) -> None:
        if self.status.critical():
            self.logger.warning("Guard condition activated", throttle_duration_sec=1)
            # Avoid sudden resumption of telescope drive
            return self.gc.trigger()

        # No realtime-ness check is performed, just filter outdated commands out
        now = time.time()
        cmd = None
        if len(self.result_queue) > 0:
            while len(self.result_queue) > 0:
                cmd = self.result_queue.pop(0)
                if cmd[2] > now:
                    break

        if cmd:
            msg = CoordMsg(
                lon=cmd[0], lat=cmd[1], time=cmd[2], unit="deg", frame="altaz"
            )
            self.publisher.publish(msg)

    def _parse_cmd(self, msg: CoordinateCommand.Request) -> None:
        if msg.direct_mode:
            self.direct_mode = True
        else:
            self.direct_mode = False
        self.finder.direct_mode = self.direct_mode
        self.logger.debug(f"Got POINT-TO-COORD command: {msg}")
        new_generator = self.finder.track(
            msg.lon[0],
            msg.lat[0],
            msg.frame,
            unit=msg.unit,
        )
        self.executing_generator.attach(new_generator)

    def convert(self) -> None:
        if (self.cmd is not None) and (self.enc_time < time.time() - 5):
            # Don't resume normal operation after communication with encoder lost for 5s
            self.logger.error(
                "Lost the communication with the encoder. Command to drive to "
                f"{self.cmd} has been discarded."
            )
            self.cmd = None
        if self.cmd is None:
            return self.telemetry(None)

        if (len(self.result_queue) > 1) and (
            self.result_queue[-1][2] > time.time() + config.dome_command_offset_sec
        ):
            # This function will be called twice per 1s, to ensure no run-out of command
            # but it can cause overloading the data, so judge command update necessity.
            return

        try:
            coord = next(self.executing_generator)
            self.telemetry(coord.context)
        except (StopIteration, TypeError):
            self.cmd = None
            self.executing_generator.clear()
            return self.telemetry(None)

        az = self._validate_drive_range(coord.az)
        for _az, _t in zip(az, coord.time):
            if any(x is None for x in [_az, _t]):
                continue
            # Remove chronologically duplicated/overlapping commands
            self.result_queue = list(filter(lambda x: x[2] < _t, self.result_queue))

            cmd = (float(_az.to_value("deg")), 45.0, _t)
            self.result_queue.append(cmd)

    def _validate_drive_range(self, az) -> Tuple:  # All values are Quantity.
        enc_az = 180 if self.enc_az is None else self.enc_az

        _az = self.optimizer.optimize(enc_az, az.to_value("deg"), unit="deg")

        if _az is not None:
            return _az
        return []

    def telemetry(self, status: Optional[ControlContext]) -> None:
        if status is None:
            msg = ControlStatus(
                controlled=False,
                tight=False,
                remote=True,
                id=str(id(self.executing_generator.get())),
                interrupt_ok=True,
                time=time.time() + config.antenna_command_offset_sec,
            )
        else:
            msg = ControlStatus(
                controlled=True,
                tight=status.tight,
                remote=True,
                id=str(id(self.executing_generator.get())),
                interrupt_ok=status.infinite and (not status.waypoint),
                time=status.start,
            )
        self.last_status = msg
        self.status_publisher.publish(msg)
