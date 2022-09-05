import pytest
import rclpy
from rclpy.executors import SingleThreadedExecutor, MultiThreadedExecutor


class TesterNode:
    """Test class with rclpy configuration.

    The implementation is inspired by the rclpy test suite.
    https://github.com/ros2/rclpy/tree/rolling/rclpy/test

    The ``unittest.TestCase`` implementations are replaced with PyTest's equivalents.
    https://docs.pytest.org/en/6.2.x/xunit_setup.html#class-level-setup-teardown

    """

    NodeName: str

    @classmethod
    def setup_class(cls) -> None:
        rclpy.init()
        cls.node = rclpy.create_node(cls.NodeName)

    @classmethod
    def teardown_class(cls) -> None:
        cls.node.destroy_node()
        rclpy.shutdown()


executor_type = pytest.mark.parametrize(
    "executor_type", [SingleThreadedExecutor, MultiThreadedExecutor]
)
