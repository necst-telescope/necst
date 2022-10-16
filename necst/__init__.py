"""NEw Control System for Telescope.
"""

from importlib.metadata import version

import neclib
from rclpy.duration import Duration
from rclpy.qos import (
    DurabilityPolicy,
    HistoryPolicy,
    LivelinessPolicy,
    QoSProfile,
    ReliabilityPolicy,
)
from rclpy.logging import get_logger


try:
    __version__ = version("necst")
except:  # noqa: E722
    __version__ = "0.0.0"

logger = get_logger("necst")
config = neclib.config


class namespace:
    root: str = f"/necst/{config.observatory}"

    ctrl: str = f"{root}/ctrl"
    antenna: str = f"{ctrl}/antenna"

    core: str = f"{root}/core"
    auth: str = f"{core}/auth"
    alert: str = f"{core}/alert"


class qos:
    __default = {
        "deadline": Duration(seconds=0.01),
        "history": HistoryPolicy.KEEP_LAST,
        "lifespan": Duration(),  # Default value.
        "liveliness": LivelinessPolicy.SYSTEM_DEFAULT,
        "liveliness_lease_duration": Duration(),  # Default value.
    }
    __reliable = {"depth": 100, "reliability": ReliabilityPolicy.RELIABLE}
    __realtime = {"depth": 1, "reliability": ReliabilityPolicy.BEST_EFFORT}
    __latch = {"durability": DurabilityPolicy.TRANSIENT_LOCAL}
    __nolatch = {"durability": DurabilityPolicy.VOLATILE}

    __lowest = {
        "deadline": Duration(),  # Default value.
        "history": HistoryPolicy.KEEP_LAST,
        "lifespan": Duration(),  # Default value.
        "liveliness": LivelinessPolicy.AUTOMATIC,
        "liveliness_lease_duration": Duration(),  # Default value.
        "reliability": ReliabilityPolicy.BEST_EFFORT,
        "durability": DurabilityPolicy.VOLATILE,
    }

    reliable = QoSProfile(**__default, **__reliable, **__nolatch)
    realtime = QoSProfile(**__default, **__realtime, **__nolatch)
    reliable_latched = QoSProfile(**__default, **__reliable, **__latch)
    realtime_latched = QoSProfile(**__default, **__realtime, **__latch)
    lowest = QoSProfile(depth=10, **__lowest)
