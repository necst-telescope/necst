from rclpy.node import Node

from necst import config
from necst_msgs.msg import AuthoritySrv


class Authorizer(Node):

    NodeName = "authorizer"
    NameSpace = f"/necst/{config.observatory}/core"

    def __init__(self) -> None:
        super().__init__(self.NodeName, namespace=self.NameSpace)
        self.srv = self.create_service(AuthoritySrv, "request", self._request_clbk)

    def _request_clbk(
        self, request: AuthoritySrv, response: AuthoritySrv
    ) -> AuthoritySrv:
        response.approval = self.authorize(request.requester)
        return response

    def _check_singleton(self):
        ...

    def authorize(self):
        ...
