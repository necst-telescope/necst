#!/usr/bin/env python3
"""Radio pointing observation.

This terminal command only supports target name specification; target cannot be
specified by its coordinate.
The number of channel can be set 2^n.

Examples
--------
$ radio_pointing -f  "pt_orikl.toml" -tp 200 300

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
        type=int,
        nargs="*",
        default=None,
        help="Channel range of Total Power.",
        metavar="[START, END]",
    )

    args = p.parse_args()

    if args.tp_mode is not None:
        if len(args.tp_mode) == 0:
            args.tp_mode = []
        elif len(args.tp_mode) != 2:
            p.error("-tp option requires NO or TWO arguments")

    obs = RadioPointing(file=args.file, ch=args.channel, tp_mode=args.tp_mode)
    obs.execute()
