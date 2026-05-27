import time
import math

from neclib.data import LinearExtrapolate
from neclib.utils import ConditionChecker, ParameterList
from necst_msgs.msg import AntennaAzUnwrapStatus, AntennaPointingStatus, CoordMsg, TrackingStatus
from rclpy.node import Node

from ... import config, namespace, topic


class AntennaTrackingStatus(Node):
    NodeName = "tracking_status"
    Namespace = namespace.antenna

    def __init__(self) -> None:
        super().__init__(self.NodeName, namespace=self.Namespace)

        self.tracking_checker = ConditionChecker(
            int(0.5 * config.antenna_command_frequency), reset_on_failure=True
        )

        self.cmd = ParameterList.new(2, CoordMsg)
        self.enc = ParameterList.new(1, CoordMsg)
        self.az_unwrap_status = None

        self.threshold = config.antenna_pointing_accuracy.to_value("deg").item()
        self.coord_ext = LinearExtrapolate(
            "time", CoordMsg.get_fields_and_field_types().keys()
        )

        self.pub = topic.antenna_tracking.publisher(self)
        self.pointing_pub = topic.antenna_pointing_status.publisher(self)
        self._pointing_status_period = 0.2
        self._last_pointing_status_publish = 0.0
        topic.antenna_encoder.subscription(self, lambda msg: self.enc.push(msg))
        topic.antenna_az_unwrap_status.subscription(self, self._update_az_unwrap_status)
        topic.altaz_cmd.subscription(self, lambda msg: self.cmd.push(msg))
        self.create_timer(
            1 / config.antenna_command_frequency, self.antenna_tracking_status
        )

    def _update_az_unwrap_status(self, msg: AntennaAzUnwrapStatus) -> None:
        self.az_unwrap_status = msg

    def _az_unwrap_active_for(self, enc_time: float, now: float) -> bool:
        msg = self.az_unwrap_status
        if msg is None:
            return False
        if not bool(getattr(msg, "enabled", False)) or not bool(getattr(msg, "valid", False)):
            return False
        t = float(getattr(msg, "encoder_time_unix", float("nan")))
        return abs(t - float(enc_time)) < 1.0 and (now - float(getattr(msg, "publish_time_unix", now))) < 2.0

    @staticmethod
    def _nan() -> float:
        return float("nan")

    @staticmethod
    def _rate_deg_per_sec(newer: CoordMsg, older: CoordMsg, attr: str) -> float:
        try:
            dt = float(newer.time) - float(older.time)
            if dt == 0:
                return float("nan")
            return (float(getattr(newer, attr)) - float(getattr(older, attr))) / dt
        except Exception:
            return float("nan")

    def _publish_pointing_unavailable(self, *, now: float, basis: str) -> None:
        if (now - self._last_pointing_status_publish) < self._pointing_status_period:
            return
        self._last_pointing_status_publish = now
        self.pointing_pub.publish(
            AntennaPointingStatus(
                valid=False,
                publish_time_unix=now,
                command_time_unix=float("nan"),
                encoder_time_unix=float("nan"),
                cmd_az_deg=float("nan"),
                cmd_el_deg=float("nan"),
                enc_az_deg=float("nan"),
                enc_el_deg=float("nan"),
                delta_az_deg=float("nan"),
                delta_el_deg=float("nan"),
                delta_az_cos_el_deg=float("nan"),
                tracking_error_deg=9999.0,
                tracking_threshold_deg=float(self.threshold),
                tracking_ok=False,
                cmd_age_sec=float("nan"),
                enc_age_sec=float("nan"),
                command_stale=True,
                encoder_stale=True,
                cmd_az_rate_deg_per_sec=float("nan"),
                cmd_el_rate_deg_per_sec=float("nan"),
                enc_az_rate_deg_per_sec=float("nan"),
                enc_el_rate_deg_per_sec=float("nan"),
                basis=basis[:32],
            )
        )

    def antenna_tracking_status(self) -> None:
        now = time.time()
        if any(not isinstance(p.time, float) for p in self.cmd):
            self.tracking_checker.check(False)
            msg = TrackingStatus(ok=False, error=9999.0, time=now)
            self.pub.publish(msg)
            self._publish_pointing_unavailable(now=now, basis="missing-command")
            return
        if any(not isinstance(p.time, float) for p in self.enc):
            self.tracking_checker.check(False)
            msg = TrackingStatus(ok=False, error=9999.0, time=now)
            self.pub.publish(msg)
            self._publish_pointing_unavailable(now=now, basis="missing-encoder")
            return

        enc = self.enc[0]
        cmd = self.coord_ext(CoordMsg(time=enc.time), self.cmd)

        if self._az_unwrap_active_for(enc.time, now):
            daz = enc.lon - cmd.lon
        else:
            daz = ((enc.lon - cmd.lon + 180.0) % 360.0) - 180.0
        del_ = enc.lat - cmd.lat
        cos_el = math.cos(math.radians(enc.lat))
        daz_cos_el = daz * cos_el
        error = (daz_cos_el**2 + del_**2) ** 0.5

        ok = self.tracking_checker.check(error < self.threshold)
        msg = TrackingStatus(ok=ok, error=error, time=now)
        self.pub.publish(msg)

        cmd_rate_az = self._rate_deg_per_sec(self.cmd[0], self.cmd[1], "lon")
        cmd_rate_el = self._rate_deg_per_sec(self.cmd[0], self.cmd[1], "lat")
        cmd_age = max(0.0, now - float(cmd.time))
        enc_age = max(0.0, now - float(enc.time))
        stale_sec = max(2.0, 2.0 / max(float(config.antenna_command_frequency), 1.0))
        if (now - self._last_pointing_status_publish) < self._pointing_status_period:
            return
        self._last_pointing_status_publish = now
        self.pointing_pub.publish(
            AntennaPointingStatus(
                valid=True,
                publish_time_unix=now,
                command_time_unix=float(cmd.time),
                encoder_time_unix=float(enc.time),
                cmd_az_deg=float(cmd.lon),
                cmd_el_deg=float(cmd.lat),
                enc_az_deg=float(enc.lon),
                enc_el_deg=float(enc.lat),
                delta_az_deg=float(daz),
                delta_el_deg=float(del_),
                delta_az_cos_el_deg=float(daz_cos_el),
                tracking_error_deg=float(error),
                tracking_threshold_deg=float(self.threshold),
                tracking_ok=bool(ok),
                cmd_age_sec=float(cmd_age),
                enc_age_sec=float(enc_age),
                command_stale=bool(cmd_age > stale_sec),
                encoder_stale=bool(enc_age > stale_sec),
                cmd_az_rate_deg_per_sec=float(cmd_rate_az),
                cmd_el_rate_deg_per_sec=float(cmd_rate_el),
                enc_az_rate_deg_per_sec=float("nan"),
                enc_el_rate_deg_per_sec=float("nan"),
                basis="interpolated",
            )
        )
