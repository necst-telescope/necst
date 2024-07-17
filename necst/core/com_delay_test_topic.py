import time

from necst_msgs.msg import TimeOnly

from .. import namespace, topic
from rclpy.node import Node


class ComDelayTestTopic(Node):
    NodeName = "comdelaytesttopic"
    Namespace = namespace.core

    def __init__(self) -> None:
        super().__init__(self.NodeName, namespace=self.Namespace)

        self.logger = self.get_logger()

        self.publisher = topic.com_delay_get_time.publisher(self)
        topic.timeonly.subscription(self, self.stream)
        topic.com_delay_get_time.publisher(self)

    def stream(self, msg: TimeOnly) -> None:
        self.publisher.publish(
            TimeOnly(
                input_topic_time=msg.input_topic_time, output_topic_time=time.time()
            )
        )
