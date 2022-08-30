import functools
import uuid
from typing import Any, Callable, Optional

import rclpy
from rclpy.client import Client
from rclpy.node import Node

from necst import config
from necst_msgs.srv import AuthoritySrv


class PrivilegeManager(Node):

    NodeName = "manager"
    NameSpace = f"/necst/{config.observatory}/core/auth"

    def __init__(self, name: str) -> None:
        super().__init__(self.NodeName, namespace=self.NameSpace)
        self.logger = self.get_logger()

        self.cli = self.create_client(AuthoritySrv, "request")
        self.have_privilege: bool = False

        # User-defined ROS node name + universally unique identifier of computer system
        # + memory address of this instance
        self.identity = f"{name}-{uuid.uuid1()}-{id(self)}"

    def _wait_for_server_to_come_up(self, client: Client) -> Optional[Client]:
        self.logger.info("Waiting for authority server to pick up this node...")
        for _ in range(config.ros_system.service_timeout_sec):
            if client.wait_for_service(timeout_sec=1):
                self.logger.debug("Server is now active.")
                return client
        self.logger.error("Couldn't connect to authority server.")

    def request_privilege(self) -> bool:
        request = AuthoritySrv.Request(requester=self.identity)
        result = self._send_request(request)

        if result.approval:
            self.logger.info("Request approved")
            self.have_privilege = True
        else:
            self.logger.info("Request declined")
            self.have_privilege = False
        return self.have_privilege

    def release_privilege(self) -> bool:
        request = AuthoritySrv.Request(requester="")
        result = self._send_request(request)

        if result.approval:
            self.logger.info("Request approved")
            self.have_privilege = False
        else:
            self.logger.info("Request declined")
            self.have_privilege = True
        return self.have_privilege

    def _send_request(self, request: AuthoritySrv.Request) -> AuthoritySrv.Response:
        _ = self._wait_for_server_to_come_up(self.cli)

        self.future = self.cli.call_async(request)
        rclpy.spin_until_future_complete(self, self.future)

        return self.future.result()

    def require_privilege(
        self, callable_obj: Callable[[Any], Any]
    ) -> Callable[[Any], Any]:
        @functools.wraps(callable_obj)
        def run_with_privilege_check(*args, **kwargs) -> Any:
            if self.have_privilege:
                return callable_obj(*args, **kwargs)

            self.logger.error(
                "This operation require privilege, but this node doesn't have one."
            )
            return

        return run_with_privilege_check
