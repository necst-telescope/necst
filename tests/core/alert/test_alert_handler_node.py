import time

from necst_msgs.msg import AlertMsg

from necst import config, qos
from necst.core import AlertHandlerNode
from necst.utils import spinning

from ...conftest import TesterNode


class TestAlertHandlerNode(TesterNode):

    NodeName = "alert_handler"

    def test_subscription_count(self):
        node = AlertHandlerNode("test_handler")
        default_subscription_count = len(list(node.subscriptions))
        self.node.create_publisher(AlertMsg, "test", qos.realtime)
        with spinning(node):
            time.sleep(config.ros_topic_scan_interval_sec + 0.5)
            assert len(list(node.subscriptions)) == default_subscription_count + 1
            subscribing_topics = [s.topic_name for s in node.subscriptions]
            assert "/test" in subscribing_topics
