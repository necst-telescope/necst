import pytest
from rclpy.qos import LivelinessPolicy, QoSProfile
from std_msgs.msg import Float64

from necst import qos

from .conftest import TesterNode, destroy


class TestQoSDetector(TesterNode):

    NodeName = "test_qos_detector"

    @pytest.mark.parametrize("qos_policy", [qos.reliable, qos.realtime, qos.lowest])
    def test_adaptive(self, qos_policy: QoSProfile):
        topic_info = self.node.get_topic_names_and_types()
        assert "qos_test" not in [info[0] for info in topic_info]

        pub = self.node.create_publisher(Float64, "qos_test", qos_policy)

        detected = qos.adaptive("qos_test", self.node)
        assert detected.reliability == qos_policy.reliability
        assert detected.durability == qos_policy.durability
        assert detected.deadline.nanoseconds >= qos_policy.deadline.nanoseconds
        assert detected.liveliness in [
            qos_policy.liveliness,
            LivelinessPolicy.AUTOMATIC,
        ]
        assert (
            detected.liveliness_lease_duration.nanoseconds
            >= qos_policy.liveliness_lease_duration.nanoseconds
        )

        destroy(pub, node=self.node)

    @pytest.mark.parametrize("qos_policy", [qos.reliable, qos.realtime, qos.lowest])
    def test_adaptive_no_publisher(self, qos_policy: QoSProfile):
        topic_info = self.node.get_topic_names_and_types()
        assert "qos_test" not in [info[0] for info in topic_info]

        sub = self.node.create_subscription(
            Float64, "qos_test", lambda msg: None, qos_policy
        )

        detected = qos.adaptive("qos_test", self.node)
        assert detected.reliability == qos_policy.reliability
        assert detected.durability == qos_policy.durability
        assert detected.deadline.nanoseconds >= qos_policy.deadline.nanoseconds
        assert detected.liveliness in [
            qos_policy.liveliness,
            LivelinessPolicy.AUTOMATIC,
        ]
        assert (
            detected.liveliness_lease_duration.nanoseconds
            >= qos_policy.liveliness_lease_duration.nanoseconds
        )

        destroy(sub, node=self.node)

    def test_adaptive_nonexistent_topic(self):
        topic_info = self.node.get_topic_names_and_types()
        assert "qos_test" not in [info[0] for info in topic_info]

        detected = qos.adaptive("qos_test", self.node)
        assert detected.reliability == qos.lowest.reliability
        assert detected.durability == qos.lowest.durability
        assert detected.deadline.nanoseconds >= qos.lowest.deadline.nanoseconds
        assert detected.liveliness == qos.lowest.liveliness
        assert (
            detected.liveliness_lease_duration.nanoseconds
            >= qos.lowest.liveliness_lease_duration.nanoseconds
        )
