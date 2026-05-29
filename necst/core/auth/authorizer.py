__all__ = ["Authorizer"]

from typing import Optional
import uuid

import rclpy
from necst_msgs.srv import AuthoritySrv
from rclpy.callback_groups import MutuallyExclusiveCallbackGroup, ReentrantCallbackGroup
from rclpy.executors import SingleThreadedExecutor
from rclpy.exceptions import InvalidHandle
from rclpy.node import Node
from std_srvs.srv import Empty

from ... import config, namespace, service, utils


class Authorizer(Node):
    """Singleton privilege server.

    To interact with this server, subclass `PrivilegeNode`.

    This server doesn't authenticate the client, i.e. any node who describes itself as
    privileged node (by identity string) can have access to privileged operations.

    Attributes
    ----------
    request_srv: rclpy.service.Service
        ROS 2 service server, listens on privilege (removal) requests.
    ping_cli: rclpy.client.Client
        ROS 2 service client, checks status of privileged nodes.

    Examples
    --------
    >>> server = necst.core.Authorizer()
    >>> rclpy.spin(server)

    """

    NodeName = "authorizer"
    Namespace = namespace.auth

    def __init__(self, **kwargs) -> None:
        super().__init__(self.NodeName, namespace=self.Namespace, **kwargs)
        self.logger = self.get_logger()
        self.__approved: Optional[str] = None
        self._ping_node = None
        self._ping_executor = None
        self.ping_cli = None
        self.request_srv = None

        if self._has_duplicate_authorizer():
            self.logger.error("Authority server is already running. Destroying this.")
            self.destroy_node()
            raise RuntimeError("Authority server is already running")

        self.request_srv = service.privilege_request.service(
            self, self._authorize, callback_group=MutuallyExclusiveCallbackGroup()
        )

        # The authorization callback must not spin this node's own executor while it
        # is already executing the service callback.  ROS 2 Jazzy is stricter about
        # such nested spins and the ping future may never complete.  Use an
        # independent lightweight node/executor only for checking the current
        # privileged node's ping service.
        self._ping_node = rclpy.create_node(
            f"{self.NodeName}_ping_client_{uuid.uuid4().hex[:8]}",
            namespace=self.Namespace,
            context=self.context,
        )
        self._ping_executor = SingleThreadedExecutor()
        self._ping_executor.add_node(self._ping_node)
        self.ping_cli = service.privilege_ping.client(
            self._ping_node, callback_group=ReentrantCallbackGroup()
        )

    @property
    def approved(self) -> Optional[str]:
        """Identity string of currently privileged node."""
        return self.__approved

    def _ping(self, timeout_sec: Optional[float] = None) -> bool:
        """Assume auth ping server only responds if it has privilege.

        This method can be called from the privilege-request service callback.  Do
        not spin ``self.executor`` here: in ROS 2 Jazzy, spinning the same executor
        from inside a mutually-exclusive callback can leave the ping future pending
        until timeout.  The dedicated ping client node/executor created in
        ``__init__`` avoids that nested-spin path.
        """
        self.logger.info("Checking status of current privileged node...")
        if timeout_sec is None:
            timeout_sec = config.ros_service_timeout_sec

        ping_cli = self.ping_cli
        ping_executor = self._ping_executor
        if ping_cli is None or ping_executor is None:
            self.logger.info("Ping client is not available.")
            return False

        try:
            if not utils.wait_for_server_to_pick_up(ping_cli, timeout_sec=timeout_sec):
                self.logger.info(
                    "Ping server on current privileged node is unreachable."
                )
                return False

            request = Empty.Request()
            future = ping_cli.call_async(request)
            ping_executor.spin_until_future_complete(future, timeout_sec)
        except (InvalidHandle, RuntimeError):
            self.logger.info(
                "Ping client/executor was invalid while checking privilege."
            )
            return False

        if not future.done():
            self.logger.info("Ping request to current privileged node timed out.")
            return False
        if future.result() is None:
            self.logger.info("Ping request to current privileged node failed.")
            return False
        return True

    def _authorize(
        self, request: AuthoritySrv.Request, response: AuthoritySrv.Response
    ) -> AuthoritySrv.Response:
        anonymous_request = not bool(request.requester)
        request_from_privileged_node = request.requester == self.approved
        removal_request = request.remove

        if anonymous_request:
            self.logger.warning("Decline privilege request (anonymous request)")
            response.privilege = False
        elif removal_request and request_from_privileged_node:
            self.logger.info(f"Privilege is removed from '{request.requester}'")
            self.__approved = None
            response.privilege = False
        elif removal_request:
            self.logger.warning(
                "Decline privilege removal request (request from unprivileged node)"
            )
            response.privilege = False
        elif self.approved is None:
            self.logger.info(f"Privilege is granted for '{request.requester}'")
            self.__approved = request.requester
            response.privilege = True
        elif self._ping():  # Current privileged node is responsive
            self.logger.warning(
                f"Decline privilege request from '{request.requester}' "
                "(other node has privilege)"
            )
            response.privilege = False
        else:
            self.logger.warning(
                f"Privilege is removed from '{self.approved}' (unresponsive)"
            )
            self.logger.info(f"Privilege is granted for '{request.requester}'")
            self.__approved = request.requester
            response.privilege = True

        return response

    def destroy_node(self) -> None:
        """Destroy the authorizer and its dedicated ping helper safely."""
        ping_node = getattr(self, "_ping_node", None)
        ping_executor = getattr(self, "_ping_executor", None)

        if ping_executor is not None:
            if ping_node is not None:
                try:
                    ping_executor.remove_node(ping_node)
                except (InvalidHandle, RuntimeError):
                    pass
            try:
                ping_executor.shutdown()
            except (InvalidHandle, RuntimeError):
                pass

        if ping_node is not None:
            try:
                ping_node.destroy_node()
            except (InvalidHandle, RuntimeError):
                pass

        self._ping_node = None
        self._ping_executor = None
        self.ping_cli = None
        try:
            super().destroy_node()
        except (InvalidHandle, RuntimeError):
            pass

    def _has_duplicate_authorizer(self) -> bool:
        """Return ``True`` if another authorizer node is already visible.

        This check is only a user-friendly guard.  The actual uniqueness is still
        enforced by the ROS service name.  The important point is to stop ``__init__``
        immediately when a duplicate is detected; continuing after destroying this
        node creates services on an invalid Jazzy handle.
        """
        try:
            reachable_nodes = self.get_node_names_and_namespaces()
        except (InvalidHandle, RuntimeError):
            return False
        match = [
            nodename
            for nodename, namespace_ in reachable_nodes
            if (nodename == self.NodeName) and (namespace_ == self.Namespace)
        ]
        return len(match) > 1

    def _check_singleton(self) -> None:
        """Backward-compatible singleton check wrapper."""
        if self._has_duplicate_authorizer():
            self.logger.error("Authority server is already running. Destroying this.")
            self.destroy_node()
            raise RuntimeError("Authority server is already running")


def main(args=None) -> None:
    rclpy.init(args=args)
    node = Authorizer()
    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    finally:
        node.destroy_node()
        rclpy.try_shutdown()


if __name__ == "__main__":
    main()
