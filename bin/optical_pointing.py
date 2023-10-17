#!/usr/bin/env python3
"""Optical Pointing observation.

All parameters for the observation should be given as a TOML format parameter file.
->Update?

Examples: Target file is "target.toml" and the (bright, darkest) magnitude of target is (1, 3).
--------
$ necst optical_pointing -f "target.dat" -m (1, 3)

"""

import argparse
from datetime import datetime
from typing import Tuple, Union

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
        "-m",
        "--magnitude",
        type=Tuple[Union[float, int], Union[float, int]],
        help="The (brightest, darkest) limit of target magnitude. e.g. (1, 3)",
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
        type=datetime,
        help="Observation time (if inputted, system quits without driving). e.g. 2023-12-01 09:00:00",
        required=False,
    )
    args = p.parse_args()

    obs = OpticalPointing(
        file=args.file,
        magnitude=args.magnitude,
        drive_test=args.drive_test,
        obstime=args.time,
    )
    obs.execute()
