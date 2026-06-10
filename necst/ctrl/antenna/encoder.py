import time

from neclib.devices import AntennaEncoder
from necst_msgs.msg import CoordMsg

from .az_unwrap import (
    AzUnwrapRuntime,
    fill_get_response,
    fill_set_response,
    report_from_status,
)

from ... import namespace, topic, service, config
from ...core import DeviceNode


class AntennaEncoderController(DeviceNode):
    NodeName = "encoder_readout"
    Namespace = namespace.antenna

    def __init__(self) -> None:
        super().__init__(self.NodeName, namespace=self.Namespace)
        self.publisher = topic.antenna_encoder.publisher(self)
        self.unwrap_publisher = topic.antenna_az_unwrap_status.publisher(self)
        self.encoder = AntennaEncoder()
        self.az_unwrap = AzUnwrapRuntime()
        self.az_unwrap_state_get_service = service.az_unwrap_state_get.service(
            self, self.get_az_unwrap_state
        )
        self.az_unwrap_state_set_service = service.az_unwrap_state_set.service(
            self, self.set_az_unwrap_state
        )
        self.create_timer(1 / config.antenna_enc_frequency, self.stream)


    def get_az_unwrap_state(self, _request, response):
        """Return Az unwrap state from the encoder node process."""
        report = self.az_unwrap.get_state_report()
        return fill_get_response(response, report)

    def set_az_unwrap_state(self, request, response):
        """Set Az unwrap state in the encoder node process.

        The service intentionally writes through self.az_unwrap so the JSON goes
        to the state_path used by encoder_readout, not to the caller's ~/.necst.
        """
        raw_az = float(request.raw_az_deg) if bool(request.use_raw_az) else None
        continuous_az = (
            float(request.continuous_az_deg)
            if bool(request.use_continuous_az)
            else None
        )
        branch = int(request.branch) if bool(request.use_branch) else None
        try:
            status = self.az_unwrap.manual_initialize(
                raw_az=raw_az,
                continuous_az=continuous_az,
                branch=branch,
                reason="manual state set via encoder service",
            )
            try:
                self.unwrap_publisher.publish(status)
            except Exception as exc:
                self.get_logger().error(
                    f"Az unwrap status publish after service set failed: {exc}",
                    throttle_duration_sec=1,
                )
            report = report_from_status(self.az_unwrap, status)
            return fill_set_response(
                response,
                success=report["success"],
                state=report["state"],
                reason=report["reason"],
                state_path=report["state_path"],
                enabled=report["enabled"],
                valid=report["valid"],
                raw_az_deg=report["raw_az_deg"],
                modulo_az_deg=report["modulo_az_deg"],
                continuous_az_deg=report["continuous_az_deg"],
                branch=report["branch"],
                period_deg=report["period_deg"],
                drive_min_deg=report["drive_min_deg"],
                drive_max_deg=report["drive_max_deg"],
            )
        except Exception as exc:
            self.get_logger().error(f"Az unwrap service set rejected: {exc}")
            report = self.az_unwrap.get_state_report()
            return fill_set_response(
                response,
                success=False,
                state="rejected",
                reason=str(exc),
                state_path=report.get("state_path", ""),
                enabled=report.get("enabled", False),
                valid=report.get("valid", False),
                raw_az_deg=report.get("raw_az_deg", -1.0),
                modulo_az_deg=report.get("modulo_az_deg", -1.0),
                continuous_az_deg=report.get("continuous_az_deg", -1.0),
                branch=report.get("branch", 0),
                period_deg=report.get("period_deg", 360.0),
                drive_min_deg=report.get("drive_min_deg", 0.0),
                drive_max_deg=report.get("drive_max_deg", 360.0),
            )

    def stream(self) -> None:
        record_time = time.time()
        readings = self.encoder.get_reading()
        raw_az = readings["az"].to_value("deg").item()
        el = readings["el"].to_value("deg").item()
        try:
            az, unwrap_status = self.az_unwrap.process(
                raw_az, el_deg=el, encoder_time=record_time
            )
        except RuntimeError as exc:
            last_status = self.az_unwrap._last_status
            log_method = self.get_logger().error
            if (
                last_status is not None
                and getattr(last_status, "state", "") == "startup-raw-wait"
            ):
                log_method = self.get_logger().warning
            log_method(
                f"Az unwrap failed; encoder sample suppressed: {exc}",
                throttle_duration_sec=1,
            )
            if last_status is not None:
                self.unwrap_publisher.publish(last_status)
            return
        # Publish unwrap status before the continuous encoder sample so
        # tracking_status can select direct-difference Az error for the same
        # encoder timestamp without a one-sample race.  Status publication must
        # never suppress the encoder topic; if a status message ever fails
        # validation, log it and keep publishing the finite continuous encoder
        # sample.
        if unwrap_status.enabled:
            try:
                self.unwrap_publisher.publish(unwrap_status)
            except Exception as exc:
                self.get_logger().error(
                    f"Az unwrap status publish failed; continuing encoder publish: {exc}",
                    throttle_duration_sec=1,
                )
        msg = CoordMsg(
            lon=az,
            lat=el,
            unit="deg",
            frame="altaz",
            time=record_time,
        )
        self.publisher.publish(msg)


def main(args=None):
    import rclpy

    rclpy.init(args=args)
    node = AntennaEncoderController()
    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    finally:
        try:
            node.az_unwrap.close()
        except Exception:
            pass
        node.destroy_node()
        rclpy.try_shutdown()


if __name__ == "__main__":
    main()
