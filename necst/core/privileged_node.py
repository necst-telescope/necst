__all__ = ["PrivilegedNode"]

import functools
import signal
import uuid
from types import FrameType
from typing import Any, Callable, NoReturn, Optional

import rclpy.signals
from rclpy.callback_groups import MutuallyExclusiveCallbackGroup
from rclpy.client import Client
from rclpy.node import Node

from necst import config
from necst_msgs.srv import AuthoritySrv


class PrivilegedNode(Node):
    r"""Manage privilege for conflict-unsafe operations.

    Using this privilege client requires communication with running `Authorizer` node.

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
            callback_group=MutuallyExclusiveCallbackGroup(),
        )

        # User-defined ROS node name + universally unique identifier of computer system
        # + memory address of this node
        self.identity = f"{self.NodeName}-{uuid.uuid1()}-{id(self)}"
        self.have_privilege: bool = False

        self._setup_signal_handler()

    def _setup_signal_handler(self) -> None:
        """Ensure 'quit_privilege' to be called on shutdown.

        ROS 2 'Context' will shutdown immediately after a signal was received.
        This disables any shutdown process which relies on ROS communication (topic or
        service), and that is what this node want to do on shutdown; release privilege
        via ROS 'Service'.
        https://github.com/ros2/rclpy/issues/532

        To realize the functionality, this node will run with ROS default signal handler
        disabled. On signal event, run 'quit_privilege' then load the default handler,
        and delegate the rest of signal handling to it.

        """
        rclpy.signals.uninstall_signal_handlers()

        def handler(signal_num: int, stack_frame: Optional[FrameType]) -> NoReturn:
            self.executor.spin_once(timeout_sec=0.1)
            # NOTE: Complete currently running callback. Omitting this will cause
            # `ValueError: generator already executing` for `SingleThreadedExecutor`.

            self.quit_privilege()

            self.have_privilege and self.logger.fatal("Failed to release privilege")
            rclpy.signals.install_signal_handlers()
            raise KeyboardInterrupt

        self.signal_handler = signal.signal(signal.SIGINT, handler)

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
        self.executor.spin_until_future_complete(future)
        # NOTE: Using `rclpy.spin_until_future_complete(self, future, self.executor)`
        # will cause deadlock.

        return future.result()

    def get_privilege(self) -> bool:
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
        @functools.wraps(callable_obj)
        def run_with_privilege_check(self, *args, **kwargs):
            if self.have_privilege:
                return callable_obj(self, *args, **kwargs)

            self.logger.error(
                f"Executing '{callable_obj}' requires privilege, "
                "but this node doesn't have one."
            )
            return

        return run_with_privilege_check
