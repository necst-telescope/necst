#!/usr/bin/env python3
"""Radio pointing observation.

This terminal command only supports target name specification; target cannot be
specified by its coordinate.
The number of channel can be set 2^n.

Examples
--------
$ radio_pointing -f  "pt_orikl.toml"

"""

import argparse

from necst.procedures import RadioPointing

if __name__ == "__main__":
    description = "Radio Pointing Observation"
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
    p.add_argument(
        "-tp",
        "--tp_mode",
        action="store_true",
        help="Save Total Power.",
    )
    p.add_argument(
        "--tp_range",
        type=int,
        nargs=2,
        help="Channel range of Total Power.",
        metavar=["START", "END"],
    )
    if p.tp_range:
        p.tp_mode = True

    args = p.parse_args()

    obs = RadioPointing(
        file=args.file, ch=args.channel, tp=args.tp_mode, tp_range=args.tp_range
    )
    obs.execute()
