#!/usr/bin/env python3
"""Radio pointing observation.

This terminal command only supports target name specification; target cannot be
specified by its coordinate.
The number of channel can be set 2^n.

Examples
--------
$ radio_pointing -f  "pt_orikl.toml" -tp --tp_range 200 300

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
        type=bool, 
        action="store_true",
        default=False, 
        help="Total Power Mode",
    )
    p.add_argument(
        "--tp_range",
        type=int,
        nargs=2,
        default=None,
        help="Channel range of Total Power.",
        metavar="[START, END]",
    )

    args = p.parse_args()

    if args.tp_range:
        if len(args.tp_range) != 2 or args.tp_range[0] > args.tp_range[1]:
            p.error("Invalid channel range for Total Power.")
        obs = RadioPointing(
            file=args.file, ch=args.channel, tp_mode=True, tp_range=args.tp_range
            )
    else:
        obs = RadioPointing(
            file=args.file, ch=args.channel, tp_mode=args.tp_mode, tp_range=[]
            )
    obs.execute()
