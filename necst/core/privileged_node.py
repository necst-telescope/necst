__all__ = ["PrivilegedNode"]

import functools
import uuid
from typing import Any, Callable, Final

import rclpy
from rclpy.callback_groups import MutuallyExclusiveCallbackGroup
from rclpy.executors import SingleThreadedExecutor, MultiThreadedExecutor
from rclpy.node import Node
from std_srvs.srv import Empty

from .. import utils
from necst import config
from necst_msgs.srv import AuthoritySrv


class PrivilegedNode(Node):
    r"""Manage privilege for conflict-unsafe operations.

    Using this privilege client requires communication with running ``Authorizer`` node.

    Attributes
    ----------
    identity: str
        Unique identity of this privilege client instance.
    has_privilege: bool
        Whether this client has privilege or not.
    request_cli: rclpy.client.Client
        ROS 2 service client, to send privilege request.
    signal_handler: function
        Function to finalize the node releasing the privilege.
    ping_srv: rclpy.service.Service or None
        ROS 2 service server, to respond to status check by ``Authorizer``.

    Examples
    --------
    >>> class MyNode(necst.core.PrivilegedNode):
    ...     def __init__(self):
    ...         super().__init__("my_node")
    ...
    ...     @necst.core.PrivilegedNode.require_privilege
    ...     def some_operation(self):
    ...         ...
    ...
    ... def main(args=None):
    ...     rclpy.init(args=args)
    ...     node = MyNode()
    ...     logger = node.get_logger()
    ...     try:
    ...         assert node.get_privilege(), "Privilege isn't granted"
    ...         rclpy.spin(node)
    ...     except KeyboardInterrupt:
    ...         pass
    ...     except AssertionError:
    ...         import traceback
    ...         logger.warning(f"Error :\n{traceback.format_exc()}")
    ...     finally:
    ...         node.destroy_node()
    ...         rclpy.try_shutdown()
    ...
    ... if __name__ == "__main__":
    ...     main()

    """

    Namespace: Final[str] = f"/necst/{config.observatory}/core/auth"

    def __init__(self, node_name: str, **kwargs) -> None:
        super().__init__(node_name, **kwargs)
        self.logger = self.get_logger()

        self.request_cli = self.create_client(
            AuthoritySrv,
            f"{self.Namespace}/request",
            callback_group=MutuallyExclusiveCallbackGroup(),
        )
        self.ping_srv = None

        # User-defined ROS node name + universally unique identifier of computer system
        # + memory address of this node
        self.identity = f"{node_name}-{uuid.uuid1()}-{id(self)}"

        self._set_privilege(False)

    @property
    def has_privilege(self) -> bool:
        """``True`` if privilege is granted for this node."""
        return self.__has_privilege

    def _set_privilege(self, has_privilege: bool):
        self.__has_privilege = has_privilege
        if has_privilege:
            self.ping_srv = self.create_service(
                Empty,
                f"{self.Namespace}/ping",
                utils.respond_to_ping,
                callback_group=MutuallyExclusiveCallbackGroup(),
            )
        else:
            if self.ping_srv is not None:
                self.ping_srv.destroy()
            self.ping_srv = None

    def _send_request(self, request: AuthoritySrv.Request) -> AuthoritySrv.Response:
        if not utils.wait_for_server_to_pick_up(self.request_cli):
            return AuthoritySrv.Response(privilege=False)

        executor = self.executor
        if self.executor is None:
            executor = MultiThreadedExecutor()
            # Executing on global executor somehow causes deadlock
            executor.add_node(self)

        future = self.request_cli.call_async(request)
        self.logger.info("future created")

        self.executor.spin_until_future_complete(future, 5)
        self.logger.info("future completed")
        # NOTE: Using `rclpy.spin_until_future_complete(self, future, self.executor)`
        # will cause deadlock.

        return future.result()

    def get_privilege(self) -> bool:
        """Request for privilege.

        Returns
        -------
        privileged: bool
            ``True`` if successfully acquired privilege.

        """
        if self.has_privilege:
            self.logger.info("This node already has privilege")
            return self.has_privilege

        self.logger.info("request")
        request = AuthoritySrv.Request(requester=self.identity)
        response = self._send_request(request)

        self._set_privilege(response.privilege)

        if self.has_privilege:
            self.logger.info("Request approved, now this node has privilege")
        else:
            self.logger.info("Request declined")
        return self.has_privilege

    def quit_privilege(self) -> bool:
        """Give up privilege.

        Returns
        -------
        privileged: bool
            ``False`` if successfully released the privilege.

        """
        if not self.has_privilege:
            self.logger.info("This node doesn't have privilege")
            return self.has_privilege

        request = AuthoritySrv.Request(requester=self.identity, remove=True)
        response = self._send_request(request)

        self._set_privilege(response.privilege)

        if not self.has_privilege:
            self.logger.info("Successfully released privilege")
        else:
            self.logger.info("Request declined")
        return self.has_privilege

    @staticmethod
    def require_privilege(callable_obj: Callable[[Any], Any]) -> Callable[[Any], Any]:
        """Decorator to mark conflict-unsafe functions.

        Use ``@PrivilegedNode.require_privilege``. Other form of reference including
        ``@super().require_privilege`` and
        ``client = PrivilegedNode(); @client.require_privilege`` won't work.

        Examples
        --------
        >>> @necst.core.PrivilegedNode.require_privilege
        ... def some_unsafe_operation(*args):
        ...     ...

        """

        @functools.wraps(callable_obj)
        def run_with_privilege_check(self, *args, **kwargs):
            """Run the function if privilege is granted for this client.

            The implementation is collection of workarounds, and is inspired by the
            following StackOverflow answer.
            https://stackoverflow.com/a/59157026

            """
            if self.has_privilege:
                return callable_obj(self, *args, **kwargs)

            self.logger.error(
                f"Executing '{callable_obj}' requires privilege, "
                "but this node doesn't have one."
            )
            return

        return run_with_privilege_check


def main(args=None):
    rclpy.init(args=args)
    node = PrivilegedNode("node_name")
    try:
        node.get_privilege()
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    finally:
        node.destroy_node()
        rclpy.try_shutdown()


if __name__ == "__main__":
    main()
