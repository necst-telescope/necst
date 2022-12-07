"""Observation program for rsky.

Elevation angle fixed at 45deg. 
Repeats the blackbody in and out the specified number of times.
For each, integrate over the specified time.

Examples
--------
$ ./rsky_obs.py -n 1 -i 2

"""

#!/usr/bin/env python3
import argparse
import time

import rclpy
from neclib.parameters import PointingError
from necst import config
from necst.core import Commander


def RSky(n, integ_time):
    com = Commander()
    com.get_privilege()

    default_pos = com.parameters["encoder"]
    params = PointingError.from_file(config.antenna_pointing_parameter_path)
    convert_lon, *_ = params.apparent2refracted(
        az=default_pos.lon, el=default_pos.lat, unit="deg"
    )

    com.antenna(
        "point",
        lon=convert_lon.to_value("deg"),
        lat=45,
        frame="altaz",
        unit="deg",
        wait=True,
    )
    for _ in range(n):
        com.chopper("insert")
        time.sleep(integ_time)
        com.chopper("remove")
        time.sleep(integ_time)
    com.quit_privilege()
    com.destroy_node()


if __name__ == "__main__":
    description = "R sky Observation"
    p = argparse.ArgumentParser(description=description)
    p.add_argument(
        "-n",
        type=int,
        help="Number of repetitions",
        default=1,
    )
    p.add_argument(
        "-i",
        "--integ_time",
        type=float,
        help="Integration time for the R-sky obs.",
        default=2,
    )
    args = p.parse_args()

    rclpy.init()

    try:
        RSky(n=args.n, integ_time=args.integ_time)
    except KeyboardInterrupt:
        pass
    finally:
        rclpy.try_shutdown()
