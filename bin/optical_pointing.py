#!/usr/bin/env python3
"""Optical Pointing observation.

All parameters for the observation should be given as a TOML format parameter file.
->Update?

Examples: Target file is "target.toml" and the greatest magnitude of target is 4.0.
--------
$ necst optical_pointing -f "target.dat" -m 4.0

"""

import argparse

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
        type=int,
        help="The upper limit of target magnitude.",
        required=True,
    )
    args = p.parse_args()

    obs = OpticalPointing(file=args.file, mag=args.magnitude)
    obs.execute()
