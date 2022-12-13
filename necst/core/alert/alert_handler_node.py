from functools import partial

from neclib.safety import Status
from necst_msgs.msg import AlertMsg
from rclpy.node import Node

from ... import config, qos


class AlertHandlerNode(Node):
    """Node who knows system alert status.

    This and subclass of this will subscribe to all alert topics, to use the alert
    status of whole system.

    Examples
    --------
    >>> class MyClass(necst.core.AlertHandlerNode):
    ...     def __init__(self):
    ...         self.pub = self.create_publisher(AlertMsg, "alert_any", 1)
    ...         self.create_timer(1, self.stream)
    ...     def stream(self):
    ...         subscribing_topics = [s.topic_name for s in self.subscriptions]
    ...         w = [self.status[topic].warning for topic in subscribing_topics]
    ...         c = [self.status[topic].critical for topic in subscribing_topics]
    ...         msg = AlertMsg(warning=any(w), critical=any(c), issuer="alert_any")
    ...         self.pub.publish(msg)

    """

    def __init__(self, node_name: str, **kwargs):
        super().__init__(node_name, **kwargs)
        self.__status = Status(["warning", "critical"])
        self.__registered_alert_topics = []
        self.create_timer(config.ros_topic_scan_interval_sec, self.__scan_alert_topics)

    @property
    def status(self) -> Status:
        return self.__status

    def __scan_alert_topics(self) -> None:
        topic_list = self.get_topic_names_and_types()
        for name, (type_, *_) in topic_list:
            already_subscribed = name in self.__registered_alert_topics
            if (AlertMsg.__name__ in type_.split("/")) and (not already_subscribed):
                callback = partial(self.__update_status, name)
                self.create_subscription(
                    AlertMsg, name, callback, qos.adaptive(name, self)
                )
                self.__registered_alert_topics.append(name)

    def __update_status(self, topic_name: str, msg: AlertMsg) -> None:
        ns = self.get_namespace()
        if all(target.find(ns) != 0 for target in msg.target):
            return
        self.__status[topic_name] = {"warning": msg.warning, "critical": msg.critical}
