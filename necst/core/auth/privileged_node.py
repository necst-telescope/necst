__all__ = ["PrivilegedNode", "require_privilege"]

import functools
import uuid
from typing import Any, Callable, List, Optional

import rclpy
from neclib import NECSTTimeoutError
from necst_msgs.srv import AuthoritySrv
from rclpy.callback_groups import ReentrantCallbackGroup
from rclpy.client import Client
from rclpy.exceptions import InvalidHandle

from ... import config, namespace, service, utils
from ..server_node import ServerNode


class PrivilegedNode(ServerNode):
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
    ...     @necst.core.require_privilege
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

        self.request_cli = service.privilege_request.client(
            self, callback_group=ReentrantCallbackGroup()
        )
        self.ping_srv = None

        # User-defined ROS node name + universally unique identifier of computer system
        # + memory address of this node
        self.identity = f"{node_name}-{uuid.uuid1()}-{id(self)}"

        self._set_privilege(False)
        self.start_server()

    @property
    def has_privilege(self) -> bool:
        """``True`` if privilege is granted for this node."""
        return self.__has_privilege

    def _set_privilege(self, privileged: bool):
        self.__has_privilege = privileged
        if privileged:
            try:
                self.ping_srv = service.privilege_ping.service(
                    self,
                    utils.respond_to_ping,
                    callback_group=ReentrantCallbackGroup(),
                )
            except InvalidHandle:
                # Multiple server for single service cannot exist in ROS 2 system.
                self.logger.error(
                    "Failed to verify privilege, it may be granted to other node."
                )
                self._set_privilege(False)
        else:
            if self.ping_srv is not None:
                self.ping_srv.destroy()
            self.ping_srv = None

    def _send_request(self, request: Any, client: Client) -> Any:
        srv_name = client.srv_name
        if not utils.wait_for_server_to_pick_up(client):
            raise NECSTTimeoutError(
                f"Server for {srv_name!r} not responding. Make sure it's spinning."
            )

        future = client.call_async(request)
        self.wait_until_future_complete(future, 2 * config.ros_service_timeout_sec)
        # NOTE: Using `rclpy.spin_until_future_complete(self, future, self.executor)`
        # will cause deadlock. Reason unknown.
        # NOTE: Authority server also waits for `ros_service_timeout_sec` for status
        # checking, so this node waits for twice the duration.

        result = future.result()
        if result is None:
            raise NECSTTimeoutError(
                f"Server for {srv_name!r} not responding. Make sure it's spinning."
            )
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
        try:
            response = self._send_request(request, self.request_cli)
        except NECSTTimeoutError:
            self.logger.error("Failed to get privilege; server not responding.")
            return

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
        try:
            response = self._send_request(request, self.request_cli)
        except NECSTTimeoutError:
            self.logger.error(
                "Released privilege without authorization; server not responding"
            )
            response = AuthoritySrv.Response(privilege=False)

        self._set_privilege(response.privilege)

        if not self.has_privilege:
            self.logger.info("Successfully released privilege")
        else:
            self.logger.info("Request declined")
        return self.has_privilege

    @staticmethod
    def require_privilege(
        callable_obj: Optional[Callable[..., Any]] = None, *, escape_cmd: List[Any] = []
    ) -> Callable[..., Any]:
        """Decorator to mark conflict-unsafe functions.

        Parameters
        ----------
        escape_cmds
            List of commands allowed to execute ignoring the privilege. Command should
            passsed as the first argument of the decorated function.

        Notes
        -----
        This decorator assumes the function is defined and called as method of arbitrary
        class, not a standalone function object.

        Examples
        --------
        >>> @necst.core.require_privilege
        ... def some_unsafe_operation(self, *args):
        ...     ...

        >>> @necst.core.require_privilege(escape_cmds=["?"])
        ... def operation_or_query(self, command, *args):
        ...     ...

        """

        def get_first_arg(args, kwargs):
            if len(args) > 0:
                ret = args[0]
            elif len(kwargs) > 0:
                ret = next(iter(kwargs.values()))
            else:
                ret = None
            return ret.lower() if isinstance(ret, str) else ret

        def create_wrapper_func(_callable: Callable[..., Any]) -> Callable[..., Any]:
            @functools.wraps(_callable)
            def run_with_privilege_check(self, *args, **kwargs):
                """Run the function if privilege is granted for this client.

                The implementation is collection of workarounds, and is inspired by the
                following StackOverflow answer.
                https://stackoverflow.com/a/59157026

                """
                escape = [cmd.lower() for cmd in escape_cmd if isinstance(cmd, str)]
                if self.has_privilege or (get_first_arg(args, kwargs) in escape):
                    return _callable(self, *args, **kwargs)

                self.logger.error(
                    f"Executing {_callable!r} requires privilege, but this node doesn't"
                    " have one. Run `get_privilege()` method to acquire it."
                )
                return

            return run_with_privilege_check

        if callable_obj is None:
            return create_wrapper_func
        return create_wrapper_func(callable_obj)

    def destroy_node(self) -> None:
        """Add minimal privilege removal procedure.

        More "formal" privilege removal i.e. ``quit_privilege`` cannot be invoked, when
        Ctrl-C is detected. This method doesn't interfere with the behavior, instead
        make intra-node privilege check (``require_privilege``) can detect the removal.

        The server will recognize the state change via its own ``_ping`` method.

        """
        self._set_privilege(False)
        self._executor.shutdown()
        super().destroy_node()


require_privilege = PrivilegedNode.require_privilege


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
