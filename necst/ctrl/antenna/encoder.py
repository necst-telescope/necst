import time

from neclib.devices import AntennaEncoder
from necst_msgs.msg import CoordMsg

from .az_unwrap import AzUnwrapRuntime

from ... import namespace, topic, config
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
        self.create_timer(1 / config.antenna_enc_frequency, self.stream)

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
            if last_status is not None and getattr(last_status, "state", "") == "startup-raw-wait":
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
