__all__ = ["Authorizer"]

from typing import Optional

import rclpy
from necst_msgs.srv import AuthoritySrv
from rclpy.callback_groups import MutuallyExclusiveCallbackGroup
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
        self._check_singleton()

        self.__approved: Optional[str] = None

        self.request_srv = service.privilege_request.service(
            self, self._authorize, callback_group=MutuallyExclusiveCallbackGroup()
        )
        self.ping_cli = service.privilege_ping.client(
            self, callback_group=MutuallyExclusiveCallbackGroup()
        )

    @property
    def approved(self) -> Optional[str]:
        """Identity string of currently privileged node."""
        return self.__approved

    def _ping(self, timeout_sec: float = None) -> bool:
        """Assume auth ping server only responds if it has privilege."""
        self.logger.info("Checking status of current privileged node...")
        if not utils.wait_for_server_to_pick_up(self.ping_cli):
            self.logger.info("Ping server on current privileged node is unreachable.")
            return False

        executor = self.executor
        if executor is None:
            executor = rclpy.get_global_executor()
            executor.add_node(self)

        timeout = config.ros_service_timeout_sec if timeout_sec is None else timeout_sec
        request = Empty.Request()
        future = self.ping_cli.call_async(request)
        executor.spin_until_future_complete(future, timeout)
        # NOTE: Using `rclpy.spin_until_future_complete(self, future, self.executor)`
        # will cause deadlock. Reason unknown.
        return future.done()

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

    def _check_singleton(self) -> None:
        """Check if this server node is duplicated.

        If this node isn't singleton, creating service server in ``__init__`` will fail,
        so this method isn't necessarily required. This method just issues error message
        to the console.

        """
        reachable_nodes = self.get_node_names_and_namespaces()
        match = [
            nodename
            for nodename, namespace in reachable_nodes
            if (nodename == self.NodeName) and (namespace == self.Namespace)
        ]
        if len(match) > 1:
            self.logger.error("Authority server is already running. Destroying this...")
            self.destroy_node()


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
