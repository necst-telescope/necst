#!/usr/bin/env python3
"""Request cooperative observation abort through the unified `necst` CLI."""

import sys

from necst.core.emergency import main_abort

if __name__ == "__main__":
    sys.exit(main_abort())
