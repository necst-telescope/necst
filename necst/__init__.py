"""NEw Control System for Telescope.
"""

from importlib.metadata import version

import neclib
from rclpy.duration import Duration
from rclpy.logging import get_logger
from rclpy.node import Node
from rclpy.qos import (
    DurabilityPolicy,
    HistoryPolicy,
    LivelinessPolicy,
    QoSProfile,
    ReliabilityPolicy,
)


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

    @staticmethod
    def adaptive(topicname: str, node: Node) -> QoSProfile:
        """Automatically choose suitable QoS policy to subscribe to the topic."""
        topic_info = node.get_publishers_info_by_topic(topicname)
        qos_info = map(lambda t: t.qos_profile, topic_info)

        reliability = (
            ReliabilityPolicy.RELIABLE
            if all(q.reliability == ReliabilityPolicy.RELIABLE for q in qos_info)
            else ReliabilityPolicy.BEST_EFFORT
        )
        durability = (
            DurabilityPolicy.TRANSIENT_LOCAL
            if all(q.durability == DurabilityPolicy.TRANSIENT_LOCAL for q in qos_info)
            else DurabilityPolicy.VOLATILE
        )
        deadline = max(qos_info, key=lambda q: q.deadline.nanoseconds).deadline
        liveliness = (
            LivelinessPolicy.MANUAL_BY_TOPIC
            if all(q.liveliness == LivelinessPolicy.MANUAL_BY_TOPIC for q in qos_info)
            else LivelinessPolicy.AUTOMATIC
        )
        lease_duration = max(
            qos_info, key=lambda q: q.liveliness_lease_duration.nanoseconds
        ).liveliness_lease_duration

        return QoSProfile(
            reliability=reliability,
            durability=durability,
            deadline=deadline,
            liveliness=liveliness,
            liveliness_lease_duration=lease_duration,
        )
