import concurrent.futures
from contextlib import contextmanager
from typing import Sequence, Union

import pytest
import rclpy
from rclpy.exceptions import InvalidHandle
from rclpy.executors import SingleThreadedExecutor, MultiThreadedExecutor
from rclpy.node import Node


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


@contextmanager
def spinning(node: Union[Node, Sequence[Node]]) -> None:
    """Run tests with spinning some nodes.

    Examples
    --------
    >>> with spinning(node) as e:
    ...     assert my_node.subscribed_parameter is not None

    """
    node = [node] if isinstance(node, Node) else node
    executor = rclpy.get_global_executor()
    _ = [executor.add_node(n) for n in node]
    try:
        with concurrent.futures.ThreadPoolExecutor() as ex:

            def spin():
                while not ex.stopped():
                    executor.spin_once()

            _ = ex.submit(spin)
        yield ex
    finally:
        ex.shutdown(cancel_futures=True)
        _ = [n.destroy_node() for n in executor.get_nodes()]
        _ = [executor.remove_node(n) for n in executor.get_nodes()]


def is_destroyed(node: Node):
    try:
        node.get_name()
        return False
    except InvalidHandle:
        return True
