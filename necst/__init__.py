from importlib.metadata import version  # Python 3.8+

try:
    __version__ = version("necst")
except:  # noqa: E722
    __version__ = "0.0.0"

from neclib.configuration import configure

configure()
