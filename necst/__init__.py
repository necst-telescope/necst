from .core import AntennaController
from importlib.metadata import version

import neclib


try:
    __version__ = version("necst")
except:  # noqa: E722
    __version__ = "0.0.0"

config = neclib.config
