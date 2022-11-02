__all__ = ["spinning"]

import threading
from typing import Optional, Sequence, Union

import rclpy
from rclpy.executors import Executor
from rclpy.node import Node


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
