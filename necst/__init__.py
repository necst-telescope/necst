"""NEw Control System for Telescope.
"""

import os
from importlib.metadata import version

import neclib
from rclpy.logging import get_logger

try:
    __version__ = version("necst")
except:  # noqa: E722
    __version__ = "0.0.0"

logger = get_logger("necst")
config = neclib.config

from neclib.exceptions import *  # noqa: E402, F401, F403

from .definitions import *  # noqa: E402, F401, F403

if os.environ.get("NECST_DEBUG_MODE", None):
    import sys
    import time
    import traceback

    from rclpy.node import Node
    from rclpy.task import Future

    def __del__(self: Future) -> None:
        if self._exception is not None and not self._exception_fetched:
            exc = traceback.format_exception(self._exception)
            exc.insert(0, f"[{time.time()}]\n")
            print("".join(exc), file=sys.stderr)

    def get_logger(self: Node):
        self._logger.set_level(10)
        return self._logger

    Future.__del__ = __del__
    Node.get_logger = get_logger
