__all__ = ["PrivilegedNode"]

import functools
import uuid
from typing import Any, Callable

import rclpy
from rclpy.callback_groups import MutuallyExclusiveCallbackGroup
from rclpy.exceptions import InvalidHandle
from rclpy.node import Node
from std_srvs.srv import Empty

from .. import config, namespace, utils
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
        ROS 2 service server, to respond to status check from ``Authorizer``.

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
    ...         # `Authorizer` should be running somewhere, to get privilege.
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

    Namespace: str = namespace.auth

    def __init__(self, node_name: str, **kwargs) -> None:
        super().__init__(node_name, **kwargs)
        self.logger = self.get_logger()

        self.request_cli = self.create_client(
            AuthoritySrv,
            f"{namespace.auth}/request",
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

    def _set_privilege(self, privileged: bool):
        self.__has_privilege = privileged
        if privileged:
            try:
                self.ping_srv = self.create_service(
                    Empty,
                    f"{namespace.auth}/ping",
                    utils.respond_to_ping,
                    callback_group=MutuallyExclusiveCallbackGroup(),
                )
            except InvalidHandle:
                self.logger.error(
                    "Failed to verify privilege, it may be granted to other node."
                )
                self._set_privilege(False)
        else:
            if self.ping_srv is not None:
                self.ping_srv.destroy()
            self.ping_srv = None

    def _send_request(self, request: AuthoritySrv.Request) -> AuthoritySrv.Response:
        default_response = AuthoritySrv.Response(privilege=False)

        if not utils.wait_for_server_to_pick_up(self.request_cli):
            return default_response

        executor = self.executor
        if executor is None:
            executor = rclpy.get_global_executor()
            executor.add_node(self)

        future = self.request_cli.call_async(request)
        executor.spin_until_future_complete(future, 2 * config.ros_service_timeout_sec)
        # NOTE: Using `rclpy.spin_until_future_complete(self, future, self.executor)`
        # will cause deadlock. Reason unknown.
        # NOTE: Authority server also waits for `ros_service_timeout_sec` for status
        # checking, so this node waits for twice the duration.

        result = future.result()
        if result is None:
            self.logger.error(
                "Authority server not responding. Make sure it's running via `spin`."
            )
            return default_response
        return result

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

    def destroy_node(self) -> None:
        """Add minimal privilege removal procedure.

        More "formal" privilege removal i.e. ``quit_privilege`` cannot be invoked, when
        Ctrl-C is detected. This method doesn't interfere with the behavior, instead
        make intra-node privilege check (``require_privilege``) can detect the removal.

        The server will recognize the state change via its own ``_ping`` method.

        """
        self._set_privilege(False)
        super().destroy_node()


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
