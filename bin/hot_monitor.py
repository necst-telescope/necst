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
        help="Integration time for the R-Sky observation.",
        default=2,
    )
    p.add_argument(
        "-m",
        "--mode",
        type=str,
        help="Monitoring Mode",
        required=True,
    )
    
    args = p.parse_args()

    obs = HotMonitor(mode=args.mode, integ_time=args.integ)
    obs.execute()
