#!/usr/bin/env python3
"""Optical Pointing observation.

All parameters for the observation should be given as a TOML format parameter file.
->Update?

Examples: Target file is "target.dat" and the (bright, darkest) magnitude is (1, 3).
--------
$ necst optical_pointing -f "target.dat" -l 1.0 -u 3.0

"""

import argparse
from datetime import datetime

from necst.procedures import OpticalPointing

if __name__ == "__main__":
    description = "Optical Pointing Observation"
    p = argparse.ArgumentParser(description=description)
    p.add_argument(
        "-f",
        "--file",
        type=str,
        help="Path to pointing target file",
        required=True,
    )
    p.add_argument(
        "-l",
        "--lower_mag",
        type=float,
        help="The lower (brightest) limit of target magnitude. e.g. 1.0",
        required=True,
    )
    p.add_argument(
        "-u",
        "--upper_mag",
        type=float,
        help="The upper (darkest) limit of target magnitude. e.g. 3.0",
        required=True,
    )
    p.add_argument(
        "-d",
        "--drive_test",
        type=bool,
        default=False,
        help="If ``True``, execute only driving test (without capturing).",
        required=False,
    )
    p.add_argument(
        "-t",
        "--time",
        type=str,
        help=(
            "Observation time (if inputted, system quits without driving). "
            "e.g. 2023-12-01 09:00:00"
        ),
        required=False,
    )
    p.add_argument(
        "-c",
        "--channel",
        type=int,
        help="Number of spectral channels.",
    )
    args = p.parse_args()

    if args.time is None:
        obstime = None
    else:
        obstime = datetime.strptime(args.time, "%Y/%m/%d %H:%M:%S")

    obs = OpticalPointing(
        file=args.file,
        magnitude=(args.lower_mag, args.upper_mag),
        drive_test=args.drive_test,
        obstime=obstime,
    )
    obs.execute()
