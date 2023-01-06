#!/usr/bin/env python3
"""Observation program for rsky.

Elevation angle fixed at 45deg.
Repeats the blackbody in and out the specified number of times.
For each, integrate over the specified time.

Examples
--------
$ ./rsky_obs.py -n 1 -i 2

"""

import argparse

from necst.procedures import RSky

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

    RSky(n=args.n, integ_time=args.integ_time)
