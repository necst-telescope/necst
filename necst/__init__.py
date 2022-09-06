from importlib.metadata import version

import neclib
from rclpy.logging import get_logger


try:
    __version__ = version("necst")
except:  # noqa: E722
    __version__ = "0.0.0"

logger = get_logger("necst")
config = neclib.config
