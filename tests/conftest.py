import threading
from typing import Any, Optional, Sequence, Union

import pytest
import rclpy
from rclpy.executors import Executor, MultiThreadedExecutor, SingleThreadedExecutor
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
        import neclib

        neclib.configure()

        rclpy.init()
        cls.node = rclpy.create_node(cls.NodeName)

    @classmethod
    def teardown_class(cls) -> None:
        cls.node.destroy_node()
        rclpy.shutdown()


executor_type = pytest.mark.parametrize(
    "executor_type", [SingleThreadedExecutor, MultiThreadedExecutor]
)


class spinning:
    """Run tests with spinning some nodes.

    The implementation is inspired by:
    https://github.com/ros2/system_tests/blob/rolling/test_cli/test/utils.py#L77

    Examples
    --------
    >>> with spinning(node) as e:
    ...     assert my_node.subscribed_parameter is not None

    """

    def __init__(
        self,
        node: Union[Node, Sequence[Node]] = [],
        *,
        executor: Optional[Executor] = None,
    ) -> None:
        self.executor = executor or rclpy.get_global_executor()
        self.nodes = [node] if isinstance(node, Node) else node
        _ = [self.executor.add_node(n) for n in self.nodes]

    def __enter__(self) -> None:
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def _run(self) -> None:
        while not self._stop.is_set():
            self.executor.spin_once(timeout_sec=0.25)

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self._stop.set()
        self._thread.join()
        _ = [self.executor.remove_node(n) for n in self.nodes]


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
