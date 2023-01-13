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
    p.add_argument(
        "-r",
        "--resume-scan",
        type=int,
        default=1,
        help="1-based scan index the observation starts from.",
    )
    args = p.parse_args()

    OTF(path=args.file, resume_scan=args.resume_scan)
