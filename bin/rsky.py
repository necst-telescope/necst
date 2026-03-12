#!/usr/bin/env python3
"""R-Sky observation.

Elevation angle fixed at 45deg.
Repeats the blackbody in and out the specified number of times.
For each, integrate over the specified time.
The number of channel can be set 2^n.

Examples
--------
$ rsky -n 1 -i 2

"""

import argparse

from necst.procedures import RSky

if __name__ == "__main__":
    description = "R-Sky Observation"
    p = argparse.ArgumentParser(description=description)
    p.add_argument(
        "-n",
        type=int,
        help="Number of repetitions",
        default=1,
    )
    p.add_argument(
        "-i",
        "--integ",
        type=float,
        help="Integration time for the R-Sky observation.",
        default=2,
    )
    p.add_argument(
        "-c",
        "--channel",
        type=int,
        help="Number of spectral channels.",
    )
    p.add_argument(
        "-dc",
        "--dome_close",
        type=bool,
        default=False,
        help="If ``True``, dome close at the end of the observation.",
    )
    args = p.parse_args()

    obs = RSky(
        n=args.n, integ_time=args.integ, ch=args.channel, dome_close=args.dome_close
    )
    obs.execute()
