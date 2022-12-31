__all__ = ["spinning", "ros2context"]

import threading
from contextlib import contextmanager
from typing import Optional, Sequence, Union

import rclpy
from neclib import get_logger
from rclpy.executors import Executor, MultiThreadedExecutor, SingleThreadedExecutor
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
        n_thread: int = 1,
    ) -> None:
        # Default executor should be new instance, since the use of
        # `get_global_executor` can cause deadlock.
        default_private_executor = (
            SingleThreadedExecutor()
            if n_thread == 1
            else MultiThreadedExecutor(n_thread)
        )
        self.executor = executor or default_private_executor
        self.using_private_executor = executor is None

        nodes = [node] if isinstance(node, Node) else node
        executor_already_attached = [n for n in nodes if n.executor is not None]
        if len(executor_already_attached) > 0:
            get_logger(self.__class__.__name__).warning(
                f"Cannot spin {executor_already_attached}; executor already attached."
            )

        self.nodes = list(filter(lambda n: n.executor is None, nodes))
        [self.executor.add_node(n) for n in self.nodes if n.executor is None]

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
        if self.using_private_executor:
            self.executor.shutdown()


@contextmanager
def ros2context(args=None):
    logger = get_logger(__name__)
    rclpy.init(args=args)
    try:
        yield
    except Exception as e:
        logger.error(str(e))
    finally:
        rclpy.shutdown()
