"""General utilities around ROS interfaces (msg / srv / action)."""

__all__ = [
    "interface_type_path",
    "get_absolute_name",
    "wait_for_server_to_pick_up",
    "Topic",
    "Service",
    "import_msg",
    "serialize",
]

import array
import importlib
import re
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import (
    Any,
    Callable,
    Dict,
    Generator,
    Generic,
    Optional,
    Sequence,
    TypeVar,
    Union,
)

from rclpy.client import Client
from rclpy.node import Node
from rclpy.publisher import Publisher
from rclpy.qos import QoSProfile
from rclpy.service import Service as ROS2Service
from rclpy.subscription import Subscription

from .. import config, logger


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
    timeout = config.ros.service_timeout_sec if timeout_sec is None else timeout_sec
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
    support_index: bool = False

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

    def __getitem__(self, key: str) -> "Topic":
        if not self.support_index:
            raise IndexError(
                f"{self.__class__.__name__} object constructed without `support_index` "
                "option isn't subscriptable"
            )
        key = key.replace(".", "/")
        return Topic(
            self.msg_type, f"{self.topic}/{key}", self.qos_profile, self.namespace
        )

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
            logger.warning(
                f"Inferring namespace for topic {self.topic!r}. Caution inconsistency."
            )
            self.namespace = node.get_namespace()

    def get_children(self, node: Node) -> Dict[str, "Topic"]:
        topics = node.get_topic_names_and_types()
        ns = f"{self.namespace}/{self.topic}"
        children = {}
        for topic_name, (msg_type, *_) in topics:
            if topic_name.startswith(ns) and import_msg(msg_type) is self.msg_type:
                key = topic_name[len(ns) + 1 :]
                child = Topic(
                    self.msg_type,
                    f"{self.topic}/{key}",
                    self.qos_profile,
                    self.namespace,
                )
                children[key] = child

        return children


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
            logger.warning(
                f"Inferring namespace for service {self.srv_name!r}. "
                "Caution inconsistency."
            )
            self.namespace = node.get_namespace()


def import_msg(path: str) -> Any:
    module_name, msg_name = path.replace("/", ".").rsplit(".", 1)
    module = importlib.import_module(module_name)
    msg = getattr(module, msg_name)
    return msg


def serialize(msg: Any) -> Generator[Dict[str, Any], None, None]:
    for fname, ftype in msg.get_fields_and_field_types().items():
        sequence = re.match(r"sequence<[\w\s/,<>]+>", ftype) is not None
        string_length = int(re.sub(r".*string<(\d+)>.*|.*", r"\1", ftype) or -1)
        array_length = int(re.sub(r".*,\s(\d+)>|.*", r"\1", ftype) or -1)
        base_type = re.sub(
            r".*<(.*)<\d+>,.*>|.*<([A-Za-z][\w/]*)[\s,\d]*>|(\w*)<\d*>",
            r"\1\2\3",
            ftype,
        )

        if "/" in base_type:
            logger.warning(
                "Cannot record nested custom message "
                f"{fname}({ftype})={getattr(msg, fname)}",
                throttle_duration_sec=30,
            )
            continue
        value = getattr(msg, fname)

        typecode = getattr(value, "typecode", None)
        if isinstance(value, property):
            logger.warning(
                f"Field value for {fname!r} was property; "
                "message class may directly been passed"
            )
            continue
        if isinstance(value, array.array):
            value = value.tolist()
        if (array_length != -1) and sequence:
            if typecode is None:
                _type = str
            elif typecode in "bBhHiIlLqQ":
                _type = int
            elif typecode in "fd":
                _type = float
            else:
                _type = str
            value.extend([_type()] * (array_length - len(value)))
        if (string_length != -1) and sequence:
            value = [v.ljust(string_length, " ") for v in value]
        if (string_length != -1) and (not sequence):
            value = value.ljust(string_length, " ")

        yield {"key": fname, "type": base_type, "value": value}
