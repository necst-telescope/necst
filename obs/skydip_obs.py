#!/usr/bin/env python3
import time
import argparse

import rclpy
from necst.core import Commander
from neclib.parameters import PointingError


rclpy.init()
com = Commander()
com.get_privilege()

def skydip(integ_time):
    default_lon = com.parameters[encoder.az]
    params = PointingError.from_file("path/to/params.toml")
    convert_lon = params.apparent2refracted(az=default_lon, unit="deg")

    com.chopper("insert")
    time.sleep(integ_time)
    com.chopper("eject")
    time.sleep(integ_time)

    z = [80, 70, 60, 45, 30, 25, 20]
    for i in z:
        com.antenna("point", lon=convert_lon, lat=i, frame="altaz", unit="deg", wait=True)
        time.sleep(integ_time)

    com.chopper("insert")
    time.sleep(integ_time)
    com.chopper("eject")
    time.sleep(integ_time)

description = "Skydip Observation"
p = argparse.ArgumentParser(description=description)
p.add_argument(
    "--integ_time",
    type=float,
    help="Integration time for the skydip obs.",
    default=2,
)
args = p.parse_args()

try:
    rsky(integ_time=args.integ_time)
except KeyboardInterrupt:
    pass
finally:
    com.quit_privilege()
    rclpy.try_shutdown()
