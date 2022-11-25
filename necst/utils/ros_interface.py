"""General utilities around ROS interfaces (msg / srv / action)."""

__all__ = ["interface_type_path", "get_absolute_name", "wait_for_server_to_pick_up"]

from pathlib import PurePosixPath
from typing import Any, Sequence, TypeVar

from rclpy.client import Client

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


def get_absolute_name(name: S, namespace: str = None) -> S:
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


def wait_for_server_to_pick_up(client: Client, timeout_sec: float = None) -> bool:
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
