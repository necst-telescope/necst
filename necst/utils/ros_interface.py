"""Utilities around ROS interface files (.msg / .srv / .action)."""

__all__ = ["console_path"]

from typing import Any


def console_path(interface: Any) -> str:
    """Get interface type identifier.

    Examples
    --------
    >>> necst.utils.get_console_path(std_msgs.msg.Float64)
    'std_msgs/msg/Float64'
    """
    module, kind = interface.__module__.split(".")[:-1]
    name = interface.__qualname__
    return "/".join([module, kind, name])
