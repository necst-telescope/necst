__all__ = ["PrivilegedNode"]

import functools
import uuid

from typing import Any, Callable, Optional

from rclpy.callback_groups import MutuallyExclusiveCallbackGroup
from rclpy.client import Client
from rclpy.node import Node

from necst import config
from necst_msgs.srv import AuthoritySrv


class PrivilegedNode(Node):

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
        self.have_privilege = False

    def _wait_for_server_to_pick_up(self, client: Client) -> Optional[Client]:
        if client.service_is_ready():
            return client

        self.logger.info("Waiting for authority server to pick up this node...")
        if client.wait_for_service(timeout_sec=config.ros_service_timeout_sec):
            self.logger.info("Server is now active.")
            return client
        self.logger.error("Couldn't connect to authority server.")

    def _send_request(self, request: AuthoritySrv.Request) -> AuthoritySrv.Response:
        _ = self._wait_for_server_to_pick_up(self.cli)

        future = self.cli.call_async(request)
        self.executor.spin_until_future_complete(future)

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
