#!/usr/bin/env python3
"""Position Switching observation.

TBD

Examples
--------
$ necst position_switching -hot 30.0 -

"""

import argparse

from necst.procedures import PositionSwitching

if __name__ == "__main__":
    description = "Position Switching Observation"
    p = argparse.ArgumentParser(description=description)
    p.add_argument(
        "-hot",
        type=float,
        help="HOT Load intesration time in sec",
        default=30.0,
    )
    p.add_argument(
        "-sky",
        type=float,
        help="SKY Load integration time in sec",
        default=20.0,
    )
    p.add_argument(
        "-off",
        nargs="*",
        type=float,
        help="OFF position in [RA, Dec]",
    )
    p.add_argument(
        "-on",
        nargs="*",
        type=float,
        help="ON position in [RA, Dec]",
    )
    p.add_argument("-n", type=int, help="Observation iteration number.")

    args = p.parse_args()

    obs = PositionSwitching(
        hot_integ_time=args.hot,
        sky_integ_time=args.sky,
        off_position=args.off,
        on_position=args.on,
        n=args.n,
        ch=args.channel,
    )
    obs.execute()
