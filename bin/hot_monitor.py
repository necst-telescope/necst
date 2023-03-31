#!/usr/bin/env python3
"""Hot Monitor.

Inserting the blackbody in the specified integrate time.
The sampling rate of the recorder is fixed in 1/10 of default rate.

Examples
--------
Hot Monitoring in 5hours

$ hotmonitor -i 5

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

    obs = HotMonitor(integ_time=args.integ)
    obs.execute()
