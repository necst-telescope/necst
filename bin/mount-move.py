#!/usr/bin/env python3
"""Run a mount mechanical Az/El move through the unified `necst` CLI."""

import sys

from necst.core.emergency import main_mount_move

if __name__ == "__main__":
    sys.exit(main_mount_move())
