#!/usr/bin/env python3
"""Stop antenna motion through the unified `necst` CLI."""

import sys

from necst.core.emergency import main_stop


if __name__ == "__main__":
    sys.exit(main_stop())
