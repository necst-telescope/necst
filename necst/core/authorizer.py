__all__ = ["Authorizer"]

from typing import Final, Optional

import rclpy
from rclpy.node import Node

from necst import config
from necst_msgs.srv import AuthoritySrv


class Authorizer(Node):

    NodeName: Final[str] = "authorizer"
    Namespace = f"/necst/{config.observatory}/core/auth"

    def __init__(self) -> None:
        super().__init__(self.NodeName, namespace=self.Namespace)
        self.logger = self.get_logger()
        self._check_singleton()

        self.__approved: Optional[str] = None

        self.srv = self.create_service(AuthoritySrv, "request", self._authorize)

    @property
    def approved(self) -> Optional[str]:
        return self.__approved

    def _authorize(
        self, request: AuthoritySrv.Request, response: AuthoritySrv.Response
    ) -> AuthoritySrv.Response:
        anonymous_request = not bool(request.requester)
        request_from_privileged_node = request.requester == self.approved
        removal_request = request.remove

        if anonymous_request:
            self.logger.warning("Got request from anonymous node, ignoring...")
            response.privilege = False
        elif removal_request and request_from_privileged_node:
            self.logger.info(f"Unregistered privileged node {request.requester}")
            self.__approved = None
            response.privilege = False
        elif removal_request:
            self.logger.warning(
                "Got privilege removal request from unprivileged node, ignoring..."
            )
            response.privilege = False
        elif self.approved is not None:
            self.logger.info(f"Decline privilege request from {request.requester}")
            response.privilege = False
        else:
            self.logger.info(f"Privilege is granted for {request.requester}")
            self.__approved = request.requester
            response.privilege = True

        return response

    def _check_singleton(self) -> bool:
        detected_nodes = self.get_node_names_and_namespaces()

        def condition(name, namespace) -> bool:
            same_node_name = name.find(self.NodeName) != -1
            not_me = name != self.get_name()
            same_namespace = namespace == self.Namespace
            return same_node_name and not_me and same_namespace

        match = [
            name for name, namespace in detected_nodes if condition(name, namespace)
        ]
        if len(match) > 0:
            self.logger.error(
                f"Authority server is already running ({match}), destroying this..."
            )
            self.destroy_node()
        return True


def main(args=None) -> None:
    rclpy.init(args=args)
    node = Authorizer()
    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    finally:
        node.destroy_node()

        try:
            rclpy.shutdown()
        except:  # noqa: E722
            pass


if __name__ == "__main__":
    main()
