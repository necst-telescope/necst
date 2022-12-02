import contextlib
from typing import Any, Sequence, Union

import pytest
import rclpy
from neclib import config
from rclpy.executors import Executor, MultiThreadedExecutor, SingleThreadedExecutor
from rclpy.node import Node

from necst.core import AlertHandlerNode


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


class TesterAlertHandlingNode:
    """Test class with alert handling."""

    NodeName: str

    @classmethod
    def setup_class(cls) -> None:
        rclpy.init()
        cls.node = AlertHandlerNode(cls.NodeName)

    @classmethod
    def teardown_class(cls) -> None:
        cls.node.destroy_node()
        rclpy.shutdown()


executor_type = pytest.mark.parametrize(
    "executor_type", [SingleThreadedExecutor, MultiThreadedExecutor]
)


def destroy(ros_obj: Union[Any, Sequence[Any]], node: Node = None):
    from rclpy.client import Client
    from rclpy.guard_condition import GuardCondition
    from rclpy.publisher import Publisher
    from rclpy.service import Service
    from rclpy.subscription import Subscription
    from rclpy.timer import Rate, Timer

    def _destroy(obj: Any, n: Node = None):
        handle = {
            Node: lambda node, _: node.destroy_node(),
            Client: lambda cli, node: node.destroy_client(cli),
            Service: lambda srv, node: node.destroy_service(srv),
            Publisher: lambda pub, node: node.destroy_publisher(pub),
            Subscription: lambda sub, node: node.destroy_subscription(sub),
            Timer: lambda timer, node: node.destroy_timer(timer),
            Rate: lambda rate, node: node.destroy_rate(rate),
            GuardCondition: lambda gc, node: node.destroy_guard_condition(gc),
            Executor: lambda executor, _: executor.shutdown(),
        }
        if (not isinstance(obj, (Node, Executor))) and (n is None):
            raise TypeError(f"{type(obj)} cannot cleanly be destroyed without node obj")
        for k in handle.keys():
            if isinstance(obj, k):
                handle[k](obj, n)
                return
        raise ValueError()

    ros_obj = ros_obj if isinstance(ros_obj, Sequence) else [ros_obj]
    _ = [_destroy(obj, node) for obj in ros_obj]


@contextlib.contextmanager
def temp_config(**kwargs):
    temp_values = kwargs
    original_values = {k: getattr(config, k) for k in temp_values}
    [setattr(config, k, v) for k, v in temp_values.items()]
    try:
        yield
    finally:
        [setattr(config, k, v) for k, v in original_values.items()]
