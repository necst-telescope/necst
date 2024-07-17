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
        topic.timeonly.subscription(self, self.stream)
        topic.com_delay_get_time.publisher(self)
        # self.create_timer(1, self.check_publisher)

    # def check_publisher(self) -> None:
    #     self.publisher = topic.com_delay_topic.publisher(self)

    def stream(self, msg: TimeOnly) -> None:
        self.publisher["com_delay_get_time"].publish(
            TimeOnly(
                input_topic_time=msg.input_topic_time, output_topic_time=time.time()
            )
        )
