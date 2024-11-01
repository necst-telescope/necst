#!/usr/bin/env python3
"""Hot Monitor.

Inserting the blackbody in the specified integrate time.
The sampling rate cannot be set under 0.1s/1data.
Recording rate cannot be specified smaller than two decimal places.
The rate of 0.1s/1data is default rate.
The number of channel can be set 2^n.

Examples
--------
Hot Monitoring in 5hours. Recording rate is 1 data in 10s.

$ necst hot_monitor -i 5 -r 10 -c 64

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
    p.add_argument(
        "-r",
        "--rate",
        type=float,
        help="Recording time for 1 data in unit of seconds.",
        required=True,
    )
    p.add_argument(
        "-c",
        "--channel",
        type=int,
        help="Number of spectral channels.",
    )

    # TODO: Add monitoring mode argument. -> Total power or Average(Reduce data).

    args = p.parse_args()

    obs = HotMonitor(integ_time=args.integ, rate=args.rate)
    obs.execute()
