from typing import List

from rclpy.node import Node

from necst_msgs.msg import AlertMsg
from .guard_base import Guard
from ... import config, namespace, qos


class AlertHandler(Node):

    NodeName = "alert_handler"
    Namespace = namespace.alert

    def __init__(self) -> None:
        self.alert_subscription = {}
        self.warning_condition = {}
        self.critical_condition = {}

        self.logger = self.get_logger()
        self.create_timer(config.ros_logging_interval_sec, self.stream)

        self.__guards: List[Guard] = []

    def register(self, guard: Guard) -> None:
        if type(guard) is type:
            raise TypeError("Guard should be instantiated.")
        if guard in self.__guards:
            raise ValueError("# of guards for 1 topic should be no greater than 1.")

        for msg_type, topic_name, qos_profile in guard.MetaTopics:
            self.create_subscription(
                msg_type,
                topic_name,
                lambda msg: guard.update(topic_name, msg),
                qos_profile,
            )

        msg_type, topic_name, qos_profile = guard.TargetTopic
        checked_topic_name = topic_name.rstrip("/") + "/checked"
        p = self.create_publisher(msg_type, checked_topic_name, qos_profile)
        self.create_subscription(
            msg_type,
            topic_name,
            lambda msg: p.publish(
                **guard.check(msg, critical=self.critical, warning=self.warning)
            ),
            qos_profile,
        )

        self.__guards.append(guard)

    def _subscribe_alert_topics(self) -> None:
        topics = self.get_topic_names_and_types()
        alert_topics = filter(lambda _, type_: AlertMsg.__name__ in type_, topics)
        for name, _ in alert_topics:
            self.alert_subscription[name] = self.create_subscription(
                AlertMsg, name, self.update_status, qos.lowest
            )

    def update_status(self, msg: AlertMsg) -> None:
        issuer = msg.issuer
        self.warning_condition[issuer] = msg.warning
        self.critical_condition[issuer] = msg.critical

    @property
    def warning(self) -> bool:
        return any(self.warning_condition.values())

    @property
    def critical(self) -> bool:
        return any(self.critical_condition.values())

    def stream(self) -> None:
        if self.critical:
            issuers = [k for k, v in self.critical_condition if v]
            self.logger.fatal(f"Critical condition detected {issuers}")
        if self.warning:
            issuers = [k for k, v in self.warning_condition if v]
            self.logger.warning(f"Warning condition detected {issuers}")
