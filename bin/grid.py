#!/usr/bin/env python3
"""
Examples
--------
$ necst grid -f "grid_orikl.toml"

"""

import argparse

from necst.procedures import Grid

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

    obs = Grid(file=args.file, ch=args.channel)
    obs.execute()
