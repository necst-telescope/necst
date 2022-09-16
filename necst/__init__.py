from dataclasses import dataclass
from importlib.metadata import version

import neclib


try:
    __version__ = version("necst")
except:  # noqa: E722
    __version__ = "0.0.0"

config = neclib.config


@dataclass
class namespace:
    root: str = f"/necst/{config.observatory}"

    ctrl: str = f"{root}/ctrl"
    antenna: str = f"{ctrl}/antenna"

    core: str = f"{root}/core"
    auth: str = f"{core}/auth"
