import time
import math

from neclib.data import LinearExtrapolate
from neclib.utils import ConditionChecker, ParameterList
from necst_msgs.msg import (
    AntennaAzUnwrapStatus,
    AntennaPointingStatus,
    CoordMsg,
    TrackingStatus,
)
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
        self._last_cmd_receipt_time = float("nan")
        self._last_enc_receipt_time = float("nan")

        self.threshold = config.antenna_pointing_accuracy.to_value("deg").item()
        self.coord_ext = LinearExtrapolate(
            "time", CoordMsg.get_fields_and_field_types().keys()
        )

        self.pub = topic.antenna_tracking.publisher(self)
        self.pointing_pub = topic.antenna_pointing_status.publisher(self)
        self._pointing_status_period = 0.2
        self._last_pointing_status_publish = 0.0
        topic.antenna_encoder.subscription(self, self._update_encoder)
        topic.antenna_az_unwrap_status.subscription(self, self._update_az_unwrap_status)
        topic.altaz_cmd.subscription(self, self._update_command)
        self.create_timer(
            1 / config.antenna_command_frequency, self.antenna_tracking_status
        )

    def _update_command(self, msg: CoordMsg) -> None:
        self.cmd.push(msg)
        self._last_cmd_receipt_time = time.time()

    def _update_encoder(self, msg: CoordMsg) -> None:
        self.enc.push(msg)
        self._last_enc_receipt_time = time.time()

    def _update_az_unwrap_status(self, msg: AntennaAzUnwrapStatus) -> None:
        self.az_unwrap_status = msg

    def _az_unwrap_active_for(self, enc_time: float, now: float) -> bool:
        msg = self.az_unwrap_status
        if msg is None:
            return False
        if not bool(getattr(msg, "enabled", False)) or not bool(
            getattr(msg, "valid", False)
        ):
            return False
        t = float(getattr(msg, "encoder_time_unix", float("nan")))
        return (
            abs(t - float(enc_time)) < 1.0
            and (now - float(getattr(msg, "publish_time_unix", now))) < 2.0
        )

    @staticmethod
    def _finite_float(value: float) -> bool:
        try:
            return math.isfinite(float(value))
        except Exception:
            return False

    @staticmethod
    def _rate_deg_per_sec(newer: CoordMsg, older: CoordMsg, attr: str) -> float:
        try:
            dt = float(newer.time) - float(older.time)
            if dt == 0:
                return float("nan")
            return (float(getattr(newer, attr)) - float(getattr(older, attr))) / dt
        except Exception:
            return float("nan")

    @staticmethod
    def _fresh_age_sec(receipt_time: float, now: float) -> float:
        try:
            age = now - float(receipt_time)
        except Exception:
            return float("inf")
        return age if math.isfinite(age) and age >= 0.0 else float("inf")

    @staticmethod
    def _tracking_freshness_limit_sec() -> float:
        # Keep this consistent with the progress/operator display freshness.
        # The check is based on local callback receipt time, not on CoordMsg.time,
        # because CoordMsg.time may be a planned command time in the future.
        return max(2.0, 2.0 / max(float(config.antenna_command_frequency), 1.0))

    def _samples_have_finite_times(self) -> bool:
        return all(self._finite_float(p.time) for p in self.cmd) and all(
            self._finite_float(p.time) for p in self.enc
        )

    def _tracking_input_ready(self, *, now: float) -> bool:
        """Return True only while live command and encoder samples are fresh.

        When the antenna command stream stops after STOP/ABORT or observation end,
        the two latest command samples remain in ``self.cmd``.  LinearExtrapolate
        can then continue making diagnostic values, which made Progress/Console
        show a continuing tracking error even though no command is active.  Use
        callback receipt ages here so stale command history is not published as a
        fresh tracking status.
        """

        if not self._samples_have_finite_times():
            return False
        limit = self._tracking_freshness_limit_sec()
        return (
            self._fresh_age_sec(self._last_cmd_receipt_time, now) <= limit
            and self._fresh_age_sec(self._last_enc_receipt_time, now) <= limit
        )

    def _publish_pointing_status(self, *, now: float, **kwargs) -> None:
        if (now - self._last_pointing_status_publish) < self._pointing_status_period:
            return
        self._last_pointing_status_publish = now
        self.pointing_pub.publish(AntennaPointingStatus(**kwargs))

    def antenna_tracking_status(self) -> None:
        now = time.time()
        if not self._tracking_input_ready(now=now):
            # No publish here by design.  A fresh /ctrl/antenna/tracking_status or
            # /ctrl/antenna/pointing_status message means there is a fresh command
            # basis.  After STOP/ABORT, displays should see the old samples become
            # stale instead of receiving new extrapolated diagnostics.
            self.tracking_checker.check(False)
            return

        enc = self.enc[0]
        cmd = self.coord_ext(CoordMsg(time=enc.time), self.cmd)
        if not all(
            self._finite_float(v)
            for v in (cmd.lon, cmd.lat, enc.lon, enc.lat, cmd.time, enc.time)
        ):
            self.tracking_checker.check(False)
            return

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
        cmd_age = self._fresh_age_sec(self._last_cmd_receipt_time, now)
        enc_age = self._fresh_age_sec(self._last_enc_receipt_time, now)
        stale_sec = self._tracking_freshness_limit_sec()
        self._publish_pointing_status(
            now=now,
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
