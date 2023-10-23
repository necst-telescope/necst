#!/usr/bin/env python3
"""Optical Pointing observation.

All parameters for the observation should be given as a TOML format parameter file.
->Update?

Examples: Target file is "target.toml" and the (bright, darkest) magnitude is (1, 3).
--------
$ necst optical_pointing -f "target.dat" --magnitude_min 1.0 --magnitude_max 3.0

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
        "--magnitude_min",
        type=float,
        help="The brightest limit of target magnitude. e.g. 1.0",
        required=True,
    )
    p.add_argument(
        "--magnitude_max",
        type=float,
        help="The darkest limit of target magnitude. e.g. 3.0",
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
        help="Observation time (if inputted, system quits without driving). e.g. 2023-12-01 09:00:00",
        required=False,
    )
    args = p.parse_args()

    obs = OpticalPointing(
        file=args.file,
        magnitude=(args.magnitude_min, args.magnitude_max),
        drive_test=args.drive_test,
        obstime=datetime.strptime(args.time, "%Y/%m/%d %H:%M:%S"),
    )
    obs.execute()
