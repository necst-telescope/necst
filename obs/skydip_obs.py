#!/usr/bin/env python3
"""Observation program for skydip.

Keep the Azimuth angles in the facing direction.
Observe at Elevation angles of 80, 50, 40, 30, 25, 22 and 20deg.
For each, integrate over the specified time.

Examples
--------
$ ./skydip_obs.py -i 2

"""


import argparse

from necst.observation import Skydip

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

    Skydip(integ_time=args.integ_time)
