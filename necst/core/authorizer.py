from typing import Final, Optional

import rclpy
from rclpy.node import Node

from necst import config
from necst_msgs.srv import AuthoritySrv


class Authorizer(Node):

    NodeName: Final[str] = "authorizer"
    NameSpace = f"/necst/{config.observatory}/core/auth"

    def __init__(self) -> None:
        super().__init__(self.NodeName, namespace=self.NameSpace)
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
        if not request.requester:
            self.logger.debug("Unregister privileged node")
            self.__approved = None
            response.approval = True
        elif self.__approved is None:
            self.logger.debug("Approve privilege request")
            self.__approved = request.requester
            response.approval = True
        else:
            self.logger.debug("Deny privilege request")
            response.approval = False
        return response

    def _check_singleton(self) -> bool:
        detected_nodes = self.get_node_names()
        match = [
            name
            for name in detected_nodes
            if name.startswith(self.NodeName) and (name != self.get_name())
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
    finally:
        node.destroy_node()
        rclpy.shutdown()


if __name__ == "__main__":
    main()
