from typing import NoReturn

from rclpy.node import Node

from necst import config
from necst_msgs.msg import AuthoritySrv


class Authorizer(Node):

    NodeName = "authorizer"
    NameSpace = f"/necst/{config.observatory}/core/auth"

    def __init__(self) -> None:
        super().__init__(self.NodeName, namespace=self.NameSpace)
        self.logger = self.get_logger()
        # self.pub = self.create_publisher(...)
        # self.sub = self.create_subscription(..., self.destroy_this)
        # self._check_singleton()

        self.srv = self.create_service(AuthoritySrv, "request", self.authorize)

    def authorize(
        self, request: AuthoritySrv.Request, response: AuthoritySrv.Response
    ) -> AuthoritySrv.Response:
        response.approval = (request.requester)
        return response

    def check_singleton(self):
        ...

    def destroy_this(self, msg) -> NoReturn:
        self.logger.error(f"")
        self.destroy_node()
