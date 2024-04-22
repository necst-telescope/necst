import time as pytime
from copy import deepcopy
from typing import List, Optional, Tuple

from neclib.controllers import PIDController
from neclib.data import LinearInterp
from neclib.safety import Decelerate
from neclib.utils import ParameterList
from necst_msgs.msg import CoordMsg, TimedAzElFloat64
from necst_msgs.srv import DomeSync

from ... import config, namespace, topic, service
from ...core import AlertHandlerNode


class DomePIDController(AlertHandlerNode):
    NodeName = "controller"
    Namespace = namespace.dome

    def __init__(self) -> None:
        super().__init__(self.NodeName, namespace=self.Namespace)
        self.logger = self.get_logger()
        pid_param = config.dome_pid_param
        max_speed = config.dome_max_speed
        max_accel = config.dome_max_acceleration
        self.controller = PIDController(
            pid_param=pid_param.az,
            max_speed=max_speed.az,
            max_acceleration=max_accel.az,
        )
        self.decelerate_calc = Decelerate(
            config.antenna_drive_critical_limit_el.map(lambda x: x.to_value("deg")),
            max_accel.el.to_value("deg/s^2"),
        )

        self.dome_sync = False

        topic.dome_altaz_cmd.subscription(self, self.update_command)
        topic.dome_encoder.subscription(self, self.update_encoder_reading)

        topic.altaz_cmd.subscription(self, self.update_command_sync)
        topic.antenna_encoder.subscription(self, self.update_antenna_encoder_reading)

        service.dome_pid_sync.service(self, self._update_sync_mode)

        self.enc = ParameterList.new(5, CoordMsg)
        self.command_list: List[CoordMsg] = []

        self.antenna_enc = ParameterList.new(5, CoordMsg)

        self.command_publisher = topic.dome_speed_cmd.publisher(self)
        self.create_timer(1 / config.dome_command_frequency, self.speed_command)

        self.coord_interp = LinearInterp(
            "time", CoordMsg.get_fields_and_field_types().keys()
        )

        self.gc = self.create_guard_condition(self.immediate_stop_no_resume)

    def _update_sync_mode(
        self, request: DomeSync.Request, response: DomeSync.Response
    ) -> DomeSync.Response:
        self.dome_sync = request.dome_sync
        response.check = True
        return response

    def update_command(self, msg: CoordMsg) -> None:
        if not self.dome_sync:
            self.command_list.append(msg)
            self.command_list.sort(key=lambda x: x.time)
        else:
            pass

    def update_command_sync(self, msg: CoordMsg) -> None:
        if self.dome_sync:
            self.command_list.append(msg)

            self.command_list.sort(key=lambda x: x.time)
        else:
            pass

    def update_encoder_reading(self, msg: CoordMsg) -> None:
        self.enc.push(msg)
        if all(isinstance(p.time, float) for p in self.enc):
            self.enc.sort(key=lambda x: x.time)

    def update_antenna_encoder_reading(self, msg: CoordMsg) -> None:
        self.antenna_enc.push(msg)
        if all(isinstance(p.time, float) for p in self.antenna_enc):
            self.antenna_enc.sort(key=lambda x: x.time)

    def interpolated_encoder_reading(self, time: float) -> Optional[CoordMsg]:
        """Perform linear interpolation on encoder reading."""
        *_, newer = self.enc
        if any(not isinstance(p.time, float) for p in self.enc) or (
            newer.time < time - 1
        ):
            self.logger.warning(
                "Encoder reading not available.", throttle_duration_sec=5
            )
            return

        return self.coord_interp(CoordMsg(time=time), self.enc)

    def immediate_stop_no_resume(self) -> None:
        self.command_list.clear()

        self.logger.warning("Immediate stop ordered.", throttle_duration_sec=5)
        enc = self.enc[-1]
        if any(not isinstance(p, float) for p in (enc.lon, enc.lat)):
            az_speed = 0.0
        else:
            p = dict(k_i=0, k_d=0, k_c=0, accel_limit_off=-1)
            with self.controller.params(**p):
                _az_speed = self.controller["az"].get_speed(enc.lon, enc.lon)
            az_speed = float(self.decelerate_calc(enc.lon, _az_speed))
        msg = TimedAzElFloat64(az=az_speed, time=pytime.time())
        self.command_publisher.publish(msg)

    def discard_outdated_commands(self) -> None:
        now = pytime.time()
        while len(self.command_list) > 1:
            if self.command_list[0].time < now:
                self.command_list.pop(0)
            else:
                break

    def get_coordinate_command(self) -> Optional[Tuple[CoordMsg, CoordMsg]]:
        self.discard_outdated_commands()
        now = pytime.time()

        # Check if any command is available.
        if len(self.command_list) == 0:
            self.immediate_stop_no_resume()
            return

        # Check if command for immediate future exists or not.
        if self.command_list[0].time > now + 2 / config.antenna_command_frequency:
            return

        if (len(self.command_list) == 1) and (self.command_list[0].time > now - 1):
            cmd = deepcopy(self.command_list[0])
            if now - cmd.time > 1 / config.antenna_command_frequency:
                cmd.time = now  # Not a real-time command.
        elif len(self.command_list) == 1:
            cmd = self.command_list.pop(0)
            cmd.time = now
        else:
            cmd = self.command_list.pop(0)
        enc = self.interpolated_encoder_reading(cmd.time)

        # Check if recent encoder reading is available or not.
        if enc is None:
            self.immediate_stop_no_resume()
            return
        return cmd, enc

    def speed_command(self) -> None:
        if self.status.critical():
            self.logger.warning("Guard condition activated", throttle_duration_sec=1)
            self.gc.trigger()
            return

        next_command = self.get_coordinate_command()
        if next_command is None:
            return
        cmd, enc = next_command
        try:
            _az_speed = self.controller.get_speed(cmd.lon, enc.lon, time=cmd.time)

            self.logger.debug(
                f"Az. Error={self.controller.error[-1]:9.6f}deg "
                f"V_target={self.controller.target_speed[-1]:9.6f}deg/s "
                f"Result={self.controller.cmd_speed[-1]:9.6f}deg/s",
                throttle_duration_sec=0.5,
            )

            az_speed = float(self.decelerate_calc(enc.lon, _az_speed))
            msg = TimedAzElFloat64(az=az_speed, time=cmd.time)
            self.command_publisher.publish(msg)
        except ZeroDivisionError:
            self.logger.debug("Duplicate command is supplied.")
        except ValueError:
            pass
