#!/usr/bin/env python3
import argparse
import time

import rclpy
from neclib.parameters import PointingError
from necst.core import Commander


def rsky(n, integ_time):
    com = Commander()
    com.get_privilege()

    default_lon = com.parameters["encoder"].az
    params = PointingError.from_file("path/to/params.toml")
    convert_lon = params.apparent2refracted(az=default_lon, unit="deg")

    com.antenna("point", lon=convert_lon, lat=45, frame="altaz", unit="deg", wait=True)
    for i in range(n):
        com.chopper("insert")
        time.sleep(integ_time)
        com.chopper("remove")
        time.sleep(integ_time)


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
        "--integ_time",
        type=float,
        help="Integration time for the R-sky obs.",
        default=2,
    )
    args = p.parse_args()

    rclpy.init()

    try:
        rsky(n=args.n, integ_time=args.integ_time)
    except KeyboardInterrupt:
        pass
    finally:
        com.quit_privilege()
        rclpy.try_shutdown()
