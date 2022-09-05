__all__ = ["Authorizer"]

from typing import Optional

from rclpy.callback_groups import MutuallyExclusiveCallbackGroup
from rclpy.node import Node

from necst import config
from necst_msgs.srv import AuthoritySrv


class Authorizer(Node):

    NodeName = "aithorizer"
    Namespace = f"/necst/{config.observatory}/core/auth"

    def __init__(self, **kwargs) -> None:
        super().__init__(self.NodeName, namespace=self.Namespace, **kwargs)
        self.logger = self.get_logger()
        self._check_singleton()

        self.__approved: Optional[str] = None

        self.srv = self.create_service(
            AuthoritySrv,
            "request",
            self._authorize,
            callback_group=MutuallyExclusiveCallbackGroup(),
        )
        self.cli = None

    @property
    def approved(self) -> Optional[str]:
        return self.__approved

    def _ping(self) -> bool:
        ...

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
        elif self.approved is not None:
            current_privileged_node_is_responsive = self._ping()
            if current_privileged_node_is_responsive:
                self.logger.warning(
                    f"Decline privilege request from '{request.requester}' (other node has privilege)"
                )
                response.privilege = False
            else:
                self.logger.info(f"Privilege is granted for '{request.requester}'")
                self.__approved = request.requester
                response.privilege = True
        else:
            self.logger.info(f"Privilege is granted for '{request.requester}'")
            self.__approved = request.requester
            response.privilege = True

        return response

    def _check_singleton(self) -> None:
        reachable_nodes = self.get_node_names_and_namespaces()
        match = [
            nodename
            for nodename, namespace in reachable_nodes
            if (nodename == self.NodeName) and (namespace == self.Namespace)
        ]
        if len(match) > 1:
            self.logger.error("Authority server is already running. Destroying this...")
            self.destroy_node()
