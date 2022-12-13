"""Observation program for skydip.

Keep the Azimuth angles in the facing direction.
Observe at Elevation angles of 80, 50, 40, 30, 25, 22 and 20deg.
For each, integrate over the specified time.

Examples
--------
$ ./skydip_obs.py -i 2

"""

#!/usr/bin/env python3
import argparse
import time

import rclpy
from neclib.parameters import PointingError
from necst import config
from necst.core import Commander


def Skydip(integ_time):
    com = Commander()
    com.get_privilege()

    default_pos = com.get_message("encoder")
    params = PointingError.from_file(config.antenna_pointing_parameter_path)
    convert_lon, *_ = params.apparent2refracted(
        az=default_pos.lon, el=default_pos.lat, unit="deg"
    )

    com.chopper("insert")
    time.sleep(integ_time)
    com.chopper("remove")
    time.sleep(integ_time)

    El = [80, 50, 40, 30, 25, 22, 20]
    for i in El:
        com.antenna(
            "point",
            lon=convert_lon.to_value("deg"),
            lat=i,
            frame="altaz",
            unit="deg",
            wait=True,
        )
        time.sleep(integ_time)

    com.chopper("insert")
    time.sleep(integ_time)
    com.chopper("remove")
    time.sleep(integ_time)

    com.quit_privilege()
    com.destroy_node()


if __name__ == "__main__":
    description = "Skydip Observation"
    p = argparse.ArgumentParser(description=description)
    p.add_argument(
        "-i",
        "--integ_time",
        type=float,
        help="Integration time for the skydip obs.",
        default=2,
    )
    args = p.parse_args()

    rclpy.init()

    try:
        Skydip(integ_time=args.integ_time)
    except KeyboardInterrupt:
        pass
    finally:
        rclpy.try_shutdown()
