#!/usr/bin/env python3
"""On-The-Fly observation.

All parameters for the observation should be given as a TOML format parameter file.

Examples
--------
$ necst otf -f "otf_orikl.toml"

"""

import argparse

from necst.procedures import OTF

if __name__ == "__main__":
    description = "On-The-Fly Observation"
    p = argparse.ArgumentParser(description=description)
    p.add_argument(
        "-f",
        "--file",
        type=str,
        help="Path to observation parameter file",
        required=True,
    )
    args = p.parse_args()

    obs = OTF(file=args.file)
    obs.execute()
