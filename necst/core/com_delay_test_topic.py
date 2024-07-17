import time
from typing import Dict

from necst_msgs.msg import TimeOnly
from rclpy.publisher import Publisher

from .. import namespace, topic
from rclpy.node import Node


class ComDelayTestTopic(Node):
    NodeName = "comdelaytesttopic"
    Namespace = namespace.core

    def __init__(self) -> None:
        super().__init__(self.NodeName, namespace=self.Namespace)

        self.logger = self.get_logger()

        self.publisher: Dict[str, Publisher] = {}
        topic.com_delay_get_time.subscription(self, self.stream)

    def stream(self, msg: TimeOnly) -> None:
        for publisher in self.publisher.items():
            msg = TimeOnly(
                input_topic_time=msg.input_topic_time, output_topic_time=time.time()
            )
            publisher.publish(msg)
