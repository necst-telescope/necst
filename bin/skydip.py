#!/usr/bin/env python3
"""Skydip observation.

Keep the azimuth angles in the facing direction.
Observe at elevation angles of 80, 50, 40, 30, 25, 22 and 20deg.
For each, integrate over the specified time.
The number of channel can be set 2^n.

Examples
--------
$ skydip -i 2

"""


import argparse

from necst.procedures import Skydip

if __name__ == "__main__":
    description = "Skydip Observation"
    p = argparse.ArgumentParser(description=description)
    p.add_argument(
        "-i",
        "--integ",
        type=float,
        help="Integration time for the Skydip observation.",
        default=2,
    )
    p.add_argument(
        "-c",
        "--channel",
        type=int,
        help="Number of spectral channels.",
    )
    p.add_argument(
        "--tp_range",
        type=int,
        nargs="*",
        help="enable Total Power mode."
            "If you want to set multiple ranges, for example -t 100 200 500 600",
        default=[],
    )
    args = p.parse_args()

    obs = Skydip(integ_time=args.integ, ch=args.channel, tp_range=args.tp_range)
    obs.execute()
