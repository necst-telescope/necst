import time
from threading import Event, Thread, current_thread
from typing import Optional, Type

import rclpy
from rclpy.executors import Executor, ExternalShutdownException, MultiThreadedExecutor
from rclpy.exceptions import InvalidHandle
from rclpy.node import Node
from rclpy.task import Future


class NotReassignableExecutor:
    def __set_name__(self, owner: Node, name: str):
        self.private_name = "_" + name

    def __set__(self, obj: Node, value: Executor) -> None:
        if isinstance(getattr(obj, self.private_name, None), Executor):
            raise ValueError(f"Cannot reassign executor for ServerNode {obj!r}.")
        if not isinstance(value, Executor):
            raise TypeError(f"Cannot assign object of type {type(value)!r} to executor")
        setattr(obj, self.private_name, value)

    def __get__(self, obj: Node, objtype: Type = None) -> Optional[Executor]:
        return getattr(obj, self.private_name, None)


class ServerNode(Node):
    """Node which may serve some ROS service.

    Server should catch and respond to service request anytime it's ready.

    """

    NodeName: str
    Namespace: str = ""

    executor = NotReassignableExecutor()

    def __init__(self, node_name: str, max_nest: int = 2, **kwargs) -> None:
        super().__init__(node_name, **kwargs)
        executor = MultiThreadedExecutor(max_nest)
        executor.add_node(self)
        self.thread = None
        self.event = None
        self._servernode_destroyed = False

    def start_server(self) -> None:
        self.stop_server()
        self.thread = Thread(target=self.__spin, daemon=True)
        self.event = Event()
        self.thread.start()

    def __spin(self) -> None:
        while rclpy.ok() and (self.event is not None) and (not self.event.is_set()):
            try:
                self.executor.spin_once(0.1)
            except (ExternalShutdownException, InvalidHandle, RuntimeError):
                break
            # Timeout=0 significantly affects system performance.

    def stop_server(self) -> None:
        if self.event is not None:
            self.event.set()
        if self.thread is not None and self.thread is not current_thread():
            self.thread.join(timeout=2.0)
        self.event = None
        self.thread = None

    def destroy_node(self) -> None:
        if getattr(self, "_servernode_destroyed", False):
            return
        self._servernode_destroyed = True

        self.stop_server()
        executor = getattr(self, "_executor", None)
        if executor is not None:
            try:
                nodes = list(executor.get_nodes())
            except (InvalidHandle, RuntimeError):
                nodes = []
            for node in nodes:
                try:
                    executor.remove_node(node)
                except (InvalidHandle, RuntimeError):
                    pass
            try:
                executor.shutdown()
            except (InvalidHandle, RuntimeError):
                pass
        try:
            super().destroy_node()
        except (InvalidHandle, RuntimeError):
            pass

    def wait_until_future_complete(
        self, future: Future, timeout_sec: Optional[float] = None
    ) -> None:
        """Block until future completes, or timeout expires.

        The executor is attached by default, and spinning after calling ``start_server``
        so recommended method of waiting until asynchronous call of service complete,
        i.e. ``spin_until_future_complete`` will raise a ``ValueError: generator already
        executing``.
        This method will substitute the function, with minimal change of function name.

        Notes
        -----
        The implementation of timeout handling is based on
        `Executor.spin_until_future_complete <https://github.com/ros2/rclpy/blob/c7feb4b70c33aaf22e8ad467605c0a58451299c9/rclpy/rclpy/executors.py#L282-L302>`_

        """  # noqa: E501
        start = time.monotonic()
        end = None if timeout_sec is None else start + timeout_sec
        while not future.done():
            now = time.monotonic()
            if (end is not None) and (now > end):
                return
            time.sleep(0.01)
