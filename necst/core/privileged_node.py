__all__ = ["PrivilegedNode"]

import functools
import uuid
from typing import Any, Callable, Optional

import rclpy
import rclpy.signals
from rclpy.callback_groups import MutuallyExclusiveCallbackGroup
from rclpy.client import Client
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
    have_privilege: bool
        Whether this client has privilege or not.
    cli: rclpy.client.Client
        ROS 2 service client to communicate with privilege server.
    signal_handler: function
        Function to finalize the node releasing the privilege.

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

    Namespace = f"/necst/{config.observatory}/core/auth"

    def __init__(self, node_name: str, **kwargs) -> None:
        super().__init__(node_name, **kwargs)
        self.logger = self.get_logger()
        self.cli = self.create_client(
            AuthoritySrv,
            f"{self.Namespace}/request",
            callback_group=MutuallyExclusiveCallbackGroup(),  # Avoid deadlock.
        )

        # User-defined ROS node name + universally unique identifier of computer system
        # + memory address of this node
        self.identity = f"{node_name}-{uuid.uuid1()}-{id(self)}"

        self._set_privilege(False)

    @property
    def have_privilege(self) -> bool:
        return self.__have_privilege

    def _set_privilege(self, have_privilege: bool):
        self.__have_privilege = have_privilege
        if have_privilege:
            self.srv = self.create_service(
                Empty,
                "ping",
                utils.respond_ping,
                callback_group=MutuallyExclusiveCallbackGroup(),
            )
        else:
            ... if self.srv is None else self.srv.destroy()
            self.srv = None

    def _wait_for_server_to_pick_up(self, client: Client) -> Optional[Client]:
        if client.service_is_ready():
            return client

        self.logger.info("Waiting for authority server to pick up this node...")
        if client.wait_for_service(timeout_sec=config.ros_service_timeout_sec):
            self.logger.info("Server is now active.")
            return client
        self.logger.error("Couldn't connect to authority server.")

    def _send_request(self, request: AuthoritySrv.Request) -> AuthoritySrv.Response:
        if self._wait_for_server_to_pick_up(self.cli) is None:
            return AuthoritySrv.Response(privilege=False)

        future = self.cli.call_async(request)
        rclpy.spin_until_future_complete(self, future, self.executor)
        # self.executor.spin_until_future_complete(future)
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
        if self.have_privilege:
            self.logger.info("This node already has privilege")
            return self.have_privilege

        request = AuthoritySrv.Request(requester=self.identity)
        response = self._send_request(request)

        self.have_privilege = response.privilege

        if response.privilege:
            self.logger.info("Request approved, now this node has privilege")
        else:
            self.logger.info("Request declined")
        return self.have_privilege

    def quit_privilege(self) -> bool:
        """Give up privilege.

        Returns
        -------
        privileged: bool
            ``False`` if successfully released the privilege.

        """
        if not self.have_privilege:
            self.logger.info("This node doesn't have privilege")
            return self.have_privilege

        request = AuthoritySrv.Request(requester=self.identity, remove=True)
        response = self._send_request(request)

        self.have_privilege = response.privilege

        if not response.privilege:
            self.logger.info("Successfully released privilege")
        else:
            self.logger.info("Request declined")
        return self.have_privilege

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
            if self.have_privilege:
                return callable_obj(self, *args, **kwargs)

            self.logger.error(
                f"Executing '{callable_obj}' requires privilege, "
                "but this node doesn't have one."
            )
            return

        return run_with_privilege_check


def main(args=None):
    from rclpy.executors import SingleThreadedExecutor

    rclpy.init(args=args)
    node = PrivilegedNode("node_name")
    e = SingleThreadedExecutor()
    e.add_node(node)
    try:
        # breakpoint()
        # rclpy.spin(node)
        e.spin()
    except KeyboardInterrupt:
        node.logger.info("ctrl-c")
    finally:
        node.destroy_node()
        rclpy.try_shutdown()


if __name__ == "__main__":
    main()
