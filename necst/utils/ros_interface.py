"""General utilities around ROS interfaces (msg / srv / action)."""

__all__ = [
    "interface_type_path",
    "get_absolute_name",
    "wait_for_server_to_pick_up",
    "Topic",
    "Service",
]

from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any, Callable, Generic, Optional, Sequence, TypeVar, Union

from rclpy.client import Client
from rclpy.node import Node
from rclpy.publisher import Publisher
from rclpy.qos import QoSProfile
from rclpy.service import Service as ROS2Service
from rclpy.subscription import Subscription

from necst import config, logger


def interface_type_path(interface: Any, sep: str = "/") -> str:
    """Get interface type identifier.

    Examples
    --------
    >>> necst.utils.get_console_path(std_msgs.msg.Float64)
    'std_msgs/msg/Float64'

    """
    module, kind = interface.__module__.split(".")[:-1]
    name = interface.__qualname__
    return sep.join([module, kind, name])


S = TypeVar("S", str, Sequence[str])


def get_absolute_name(name: S, namespace: Optional[str] = None) -> S:
    """Absolute name for node, topic, service, etc.

    Examples
    --------
    >>> node = rclpy.create_node("node_name", namespace="/test")
    >>> necst.utils.get_absolute_name(node.get_name(), node.get_namespace())
    '/test/node_name'

    """

    if isinstance(name, str):
        return str(PurePosixPath(namespace) / name)
    return [str(PurePosixPath(namespace) / n) for n in name]


def wait_for_server_to_pick_up(
    client: Client, timeout_sec: Optional[float] = None
) -> bool:
    """Wait until ROS Service server is available to client.

    Examples
    --------
    >>> node = rclpy.create_node(...)
    >>> cli = node.create_client(...)
    >>> assert necst.utils.wait_for_server_to_pick_up(cli)
    >>> cli.call_async(...)

    """
    if client.service_is_ready():
        return True

    srv_name = client.srv_name
    logger.debug(f"Waiting for server to pick up the client ({srv_name})")
    timeout = config.ros_service_timeout_sec if timeout_sec is None else timeout_sec
    if client.wait_for_service(timeout_sec=timeout):
        logger.debug(f"Server is now active ({srv_name})")
        return True

    logger.error(f"Couldn't connect to server ({srv_name})")
    return False


T = TypeVar("T")


@dataclass
class Topic(Generic[T]):
    msg_type: Any
    topic: str
    qos_profile: Union[int, QoSProfile]
    namespace: Optional[str] = None

    def __post_init__(self):
        # Normalize the input style
        if self.namespace is not None:
            self.namespace = f"/{self.namespace.strip('/')}"

        # Check topic and namespace consistency
        if self.topic.startswith("/"):
            ns, topic = self.topic.rsplit("/", 1)
            if (self.namespace is not None) and (ns.find(self.namespace) != 0):
                raise ValueError(
                    f"Fully qualified name is given (topic={self.topic!r}) "
                    f"but conflicting namespace {self.namespace!r} is provided."
                )
            self.topic = topic
            self.namespace = ns

    @property
    def _qualname(self) -> str:
        if self.namespace is not None:
            return str(Path(self.namespace) / self.topic)
        raise ValueError("No namespace information is provided.")

    def subscription(
        self, node: Node, callback: Callable[[Any], None], **kwargs
    ) -> Subscription:
        self._get_namespace_if_unknown(node)
        return node.create_subscription(
            self.msg_type, self._qualname, callback, self.qos_profile, **kwargs
        )

    def publisher(self, node: Node, **kwargs) -> Publisher:
        self._get_namespace_if_unknown(node)
        return node.create_publisher(
            self.msg_type, self._qualname, self.qos_profile, **kwargs
        )

    def _get_namespace_if_unknown(self, node: Node) -> None:
        if self.namespace is None:
            self.namespace = node.get_namespace()


@dataclass
class Service(Generic[T]):
    srv_type: Any
    srv_name: str
    namespace: Optional[str] = None
    qos_profile: Optional[Union[int, QoSProfile]] = None

    def __post_init__(self):
        # Normalize the input style
        if self.namespace is not None:
            self.namespace = f"/{self.namespace.strip('/')}"

        # Check topic and namespace consistency
        if self.srv_name.startswith("/"):
            ns, srv_name = self.srv_name.rsplit("/", 1)
            if (self.namespace is not None) and (ns.find(self.namespace) != 0):
                raise ValueError(
                    f"Fully qualified name is given (srv_name={self.srv_name!r}) "
                    f"but conflicting namespace {self.namespace!r} is provided."
                )
            self.srv_name = srv_name
            self.namespace = ns

    @property
    def _qualname(self) -> str:
        if self.namespace is not None:
            return str(Path(self.namespace) / self.srv_name)
        raise ValueError("No namespace information is provided.")

    def service(
        self, node: Node, callback: Callable[[Any, Any], Any], **kwargs
    ) -> ROS2Service:
        self._get_namespace_if_unknown(node)
        return node.create_service(self.srv_type, self._qualname, callback, **kwargs)

    def client(self, node: Node, **kwargs) -> Client:
        self._get_namespace_if_unknown(node)
        return node.create_client(self.srv_type, self._qualname, **kwargs)

    def _get_namespace_if_unknown(self, node: Node) -> None:
        if self.namespace is None:
            self.namespace = node.get_namespace()
