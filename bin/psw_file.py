#!/usr/bin/env python3
"""On-The-Fly observation.

All parameters for the observation should be given as a TOML format parameter file.
The number of channel can be set 2^n.

Examples
--------
$ necst otf -f "otf_orikl.toml"

"""

import argparse

from necst.procedures import PSW

if __name__ == "__main__":
    description = "Potision Switching Observation"
    p = argparse.ArgumentParser(description=description)
    p.add_argument(
        "-f",
        "--file",
        type=str,
        help="Path to observation parameter file",
        required=True,
    )
    p.add_argument(
        "-c",
        "--channel",
        type=int,
        help="Number of spectral channels.",
    )
    args = p.parse_args()

    obs = PSW(file=args.file, ch=args.channel)
    obs.execute()