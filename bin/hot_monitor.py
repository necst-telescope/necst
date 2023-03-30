#!/usr/bin/env python3
"""Hot Monitor.

Elevation angle fixed at 45deg.
Repeats the blackbody in and out the specified number of times.
For each, integrate over the specified time.

Examples
--------
$ hotmonitor -i 3600 -m "ave"

"""

import argparse

from necst.procedures import HotMonitor

if __name__ == "__main__":
    description = "Hot Monitor"
    p = argparse.ArgumentParser(description=description)
    p.add_argument(
        "-i",
        "--integ",
        type=float,
        help="Integration time for the Hot Monitor in unit of hours.",
        required=True,
    )
    #TODO: Add monitoring mode argument. -> Total power or Average(Reduce data).
    # This source code is reducing data in 1/10.

    args = p.parse_args()

    obs = HotMonitor(nteg_time=args.integ)
    obs.execute()
