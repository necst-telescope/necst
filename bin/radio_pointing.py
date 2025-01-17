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
        "--tp_range",
        type=int,
        nargs=2,
        default=[],
        help="Channel range of Total Power.",
        metavar="[START, END]",
    )

    args = p.parse_args()

    if args.tp_range:
        tp_mode = True
    else:
        tp_mode = False

    obs = RadioPointing(
        file=args.file, ch=args.channel, tp_mode=tp_mode, tp_range=args.tp_range
    )
    obs.execute()
