__all__ = ["namespace", "topic", "qos", "service"]

from neclib import config, get_logger
from rclpy.duration import Duration
from rclpy.node import Node
from rclpy.qos import (
    DurabilityPolicy,
    HistoryPolicy,
    LivelinessPolicy,
    QoSProfile,
    ReliabilityPolicy,
)

logger = get_logger(__name__)


class namespace:
    root: str = f"/necst/{config.observatory}"

    ctrl: str = f"{root}/ctrl"
    antenna: str = f"{ctrl}/antenna"
    calib: str = f"{ctrl}/calib"

    core: str = f"{root}/core"
    auth: str = f"{core}/auth"
    alert: str = f"{core}/alert"

    rx: str = f"{root}/rx"

    data: str = f"{root}/data"


class qos:
    __default = {
        "deadline": Duration(nanoseconds=int(1e7)),
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

    @classmethod
    def adaptive(cls, topicname: str, node: Node) -> QoSProfile:
        """Automatically choose suitable QoS policy to subscribe to the topic."""
        topic_info = node.get_publishers_info_by_topic(topicname)
        if not topic_info:
            topic_info = node.get_subscriptions_info_by_topic(topicname)
        if not topic_info:
            return cls.lowest

        qos_info = list(map(lambda t: t.qos_profile, topic_info))

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

        logger.debug(
            f"QoS profile for topic {topicname!r}:\n\t{reliability = }\n"
            f"\t{durability = }\n\t{deadline = }\n\t{liveliness = }\n"
            f"\t{lease_duration = }\n\thistory = KEEP_LAST (default)\n\tdepth = 10"
        )

        return QoSProfile(
            reliability=reliability,
            durability=durability,
            deadline=deadline,
            liveliness=liveliness,
            liveliness_lease_duration=lease_duration,
            history=HistoryPolicy.KEEP_LAST,
            depth=10,
        )


class topic:
    from necst_msgs.msg import (
        AlertMsg,
        ChopperMsg,
        ControlStatus,
        CoordCmdMsg,
        CoordMsg,
        PIDMsg,
        Spectral,
        TimedAzElFloat64,
        TimedAzElInt64,
        TimedFloat64,
    )

    from .utils import Topic

    raw_coord = Topic(CoordCmdMsg, "raw_coord", qos.reliable, namespace.antenna)
    antenna_encoder = Topic(CoordMsg, "encoder", qos.realtime, namespace.antenna)
    antenna_speed_cmd = Topic(
        TimedAzElFloat64, "speed", qos.realtime, namespace.antenna
    )
    altaz_cmd = Topic(CoordMsg, "altaz", qos.realtime, namespace.antenna)
    drive_range_alert_az = Topic(
        AlertMsg, "antenna_drive_range/az", qos.reliable_latched, namespace.alert
    )
    drive_range_alert_el = Topic(
        AlertMsg, "antenna_drive_range/el", qos.reliable_latched, namespace.alert
    )
    drive_speed_alert_az = Topic(
        AlertMsg, "antenna_speed/az", qos.reliable_latched, namespace.alert
    )
    drive_speed_alert_el = Topic(
        AlertMsg, "antenna_speed/el", qos.reliable_latched, namespace.alert
    )
    manual_stop_alert = Topic(
        AlertMsg, "manual_stop", qos.reliable_latched, namespace.alert
    )
    weather_temperature = Topic(
        TimedFloat64, "temperature", qos.realtime, namespace.root
    )
    weather_pressure = Topic(TimedFloat64, "pressure", qos.realtime, namespace.root)
    weather_humidity = Topic(TimedFloat64, "humidity", qos.realtime, namespace.root)
    antenna_motor_speed = Topic(
        TimedAzElFloat64, "actual_speed", qos.realtime, namespace.antenna
    )
    antenna_motor_step = Topic(
        TimedAzElInt64, "actual_step", qos.realtime, namespace.antenna
    )
    antenna_control_status = Topic(
        ControlStatus, "controlled", qos.realtime, namespace.antenna
    )
    pid_param = Topic(PIDMsg, "pid_param", qos.reliable, namespace.antenna)
    chopper_cmd = Topic(ChopperMsg, "chopper_cmd", qos.reliable, namespace.calib)
    chopper_status = Topic(
        ChopperMsg, "chopper_status", qos.reliable, namespace.calib
    )  # Set to reliable, because of low data acquisition frequency.
    quick_spectra = Topic(Spectral, "quick_spectra", qos.realtime, namespace.rx)
    spectra_meta = Topic(Spectral, "spectra_meta", qos.reliable, namespace.rx)


class service:
    from necst_msgs.srv import AuthoritySrv, File, RecordSrv
    from std_srvs.srv import Empty

    from .utils import Service

    privilege_request = Service(AuthoritySrv, "request", namespace.auth)
    privilege_ping = Service(Empty, "ping", namespace.auth)
    record_path = Service(RecordSrv, "record_path", namespace.core)
    record_file = Service(File, "record_file", namespace.core)
