#!/usr/bin/env python3
"""Inspect or initialize the NECST Az unwrap state file."""

import sys

from necst.ctrl.antenna.az_unwrap import main

if __name__ == "__main__":
    sys.exit(main())
