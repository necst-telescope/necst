#!/usr/bin/env python3
import time
import argparse

import rclpy
from necst.core import Commander
from neclib.parameters import PointingError


rclpy.init
com = Commander()
com.get_privilege()

def rsky(n, integ_time):
    default_lon = com.parameters[encoder.az]
    params = PointingError.from_file("path/to/params.toml")
    convert_lon = params.apparent2refracted(az=default_lon, unit="deg")

    com.antenna("point", lon=convert_lon, lat=45, frame="altaz", unit="deg", wait=True)
    for i in range(n):
        com.chopper("insert")
        time.sleep(integ_time)
        com.chopper("eject")
        time.sleep(integ_time)

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

try:
    rsky(n=args.n, integ_time=args.integ_time)
except KeyboardInterrupt:
    pass
finally:
    com.quit_privilege()
    rclpy.try_shutdown()
