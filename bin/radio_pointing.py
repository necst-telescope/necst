#!/usr/bin/env python3
"""Radio pointing observation.

This terminal command only supports target name specification; target cannot be
specified by its coordinate.

Examples
--------
$ radio_pointing -t "IRC+10216"

"""

import argparse

from necst.procedures import RadioPointing

if __name__ == "__main__":
    description = "Radio Pointing Observation"
    p = argparse.ArgumentParser(description=description)
    p.add_argument(
        "-n",
        type=int,
        help="Number of repetitions",
        default=-1,
    )
    p.add_argument(
        "-i",
        "--integ",
        type=float,
        help="Integration time for the R-sky obs.",
        default=2,
    )
    p.add_argument("--speed", type=float, help="Scan speed in [deg/s].", default=1 / 30)
    p.add_argument(
        "-s",
        "--separation",
        type=float,
        help="Separation between observing points for PSW mode. Separation from target "
        "to start/stop point for scan mode.",
        default=1 / 60,
    )
    p.add_argument(
        "-p",
        "--points",
        type=int,
        help="Number of points to observe. If 0 or negative value specified, scan "
        "observation is performed.",
        default=9,
    )
    p.add_argument(
        "-t", "--target", type=str, help="Name of target celestial body.", required=True
    )
    args = p.parse_args()

    RadioPointing(
        n_iter=args.n,
        integ_time=args.integ,
        unit="deg",
        speed=args.speed,
        separation=args.separation,
        offset_frame="altaz",
        method=args.points,
        target=args.target,
    )
