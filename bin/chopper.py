#!/usr/bin/env python3
"""Move or query the chopper through the unified `necst` CLI."""

import sys

from necst.core.emergency import main_chopper

if __name__ == "__main__":
    sys.exit(main_chopper())
