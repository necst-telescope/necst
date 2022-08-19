from importlib.metadata import version

try:
    __version__ = version("necst")
except:  # noqa: E722
    __version__ = "0.0.0"

import neclib

config = neclib.config
