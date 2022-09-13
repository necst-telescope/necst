__all__ = ["HorizontalCoord"]

import time
from typing import Tuple

import rclpy
from neclib.coordinates import CoordCalculator
from neclib.utils import optimum_angle
from rclpy.node import Node

from necst import config
from necst_msgs.msg import CoordMsg


class HorizontalCoord(Node):

    NodeName = "altaz_coord"
    Namespace = f"/necst/{config.observatory}/ctrl/antenna"

    def __init__(self) -> None:
        super().__init__(self.NodeName, namespace=self.Namespace)
        self.logger = self.get_logger()
        self.publisher = self.create_publisher(CoordMsg, "altaz", 1)
        self.create_subscription(CoordMsg, "raw_coord", self.convert, 1)
        self.create_subscription(CoordMsg, "encoder", self._update_encoder_reading, 1)

        self.enc_az = self.enc_el = None

        self.converter = CoordCalculator(
            config.location, config.pointing_parameter_path
        )  # TODO: Handle weather data.

    def _update_encoder_reading(self, msg: CoordMsg) -> None:
        if (msg.unit != "deg") or (msg.frame != "altaz"):
            self.logger.warning("Invalid encoder reading detected.")
            return
        self.enc_az = msg.lon
        self.enc_el = msg.lat

    def _check_time_order(self, due_time: float) -> bool:
        return time.time() < due_time

    def _validate_drive_range(self, az: float, el: float) -> Tuple[float, float]:
        enc_az = 180 if self.enc_az is None else self.enc_az
        enc_el = 45 if self.enc_el is None else self.enc_el
        try:
            az = optimum_angle(
                enc_az,
                az,
                [v.to_value("deg") for v in config.antenna_drive_range_az],
                10,
                unit="deg",
            )
            el = optimum_angle(
                enc_el,
                el,
                [v.to_value("deg") for v in config.antenna_drive_range_el],
                10,  # TODO: Read from config.
                unit="deg",
            )
            return az, el
        except IndexError:
            self.logger.error(
                f"Command ({az, el}) out of drive range ({config.antenna_drive_range})."
            )
            return None, None

    def convert(self, msg: CoordMsg) -> None:
        name_query = bool(msg.name)
        due_time = msg.time
        if name_query:
            az, el = self.converter.get_altaz_by_name(msg.name, msg.time)
        else:
            az, el = self.converter.get_altaz(
                msg.lon, msg.lat, msg.frame, unit=msg.unit, obstime=msg.time
            )

        az, el = self._validate_drive_range(az.to_value("deg"), el.to_value("deg"))

        if self._check_time_order(due_time) and (az is not None) and (el is not None):
            converted = CoordMsg(
                lon=az, lat=el, unit="deg", frame="altaz", time=due_time
            )
            self.publisher.publish(converted)
            return
        self.logger.warning("Got outdated command, ignoring...")


def main(args=None):
    rclpy.init(args=args)
    node = HorizontalCoord()
    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    finally:
        node.destroy_node()
        rclpy.try_shutdown()


if __name__ == "__main__":
    main()
