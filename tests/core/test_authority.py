from typing import Type

import pytest
from rclpy.exceptions import InvalidHandle
from rclpy.executors import Executor

from necst.core import Authorizer, PrivilegedNode, require_privilege
from necst.utils import get_absolute_name, spinning

from ..conftest import TesterNode, destroy, executor_type, temp_config


class TestAuthority(TesterNode):

    NodeName = "test_authority"

    def test_communicable(self):
        auth_server = Authorizer()
        auth_client = PrivilegedNode("test_node")

        alive_nodes = self.node.get_node_names()
        assert auth_server.get_name() in alive_nodes
        assert auth_client.get_name() in alive_nodes

        server_communicate_with = get_absolute_name(
            [s.srv_name for s in auth_server.services], auth_server.get_namespace()
        )
        client_communicate_with = get_absolute_name(
            [c.srv_name for c in auth_client.clients], auth_client.get_namespace()
        )
        intersection = set(server_communicate_with) & set(client_communicate_with)
        assert auth_server.Namespace + "/request" in intersection

        destroy([auth_server, auth_client])

    def test_authorized_execution(self):
        class MyNode(PrivilegedNode):
            def __init__(self):
                super().__init__("test_node")

            @require_privilege
            def some_operation(self, num: int):
                return num

        auth_server = Authorizer()
        my_node = MyNode()

        with spinning(auth_server):
            assert auth_server.approved is None
            assert my_node.get_privilege() is True
            assert auth_server.approved == my_node.identity

            assert my_node.some_operation(100) == 100

        destroy([auth_server, my_node])

    def test_unauthorized_execution(self):
        class MyNode(PrivilegedNode):
            def __init__(self):
                super().__init__("test_node")

            @PrivilegedNode.require_privilege
            def some_operation(self, num: int):
                return num

        auth_server = Authorizer()
        my_node = MyNode()

        with spinning(auth_server):
            assert auth_server.approved is None

            assert my_node.some_operation(100) is None

        destroy([auth_server, my_node])

    def test_privilege_escape_command(self):
        class MyNode(PrivilegedNode):
            def __init__(self):
                super().__init__("test_node")

            @require_privilege(escape_cmd=["?"])
            def operate(self, cmd: str) -> str:
                return cmd

        auth = Authorizer()
        my_node = MyNode()
        with spinning(auth):
            assert my_node.operate("run") is None
            assert my_node.operate("?") == "?"

            my_node.get_privilege()
            assert my_node.operate("run") == "run"
            assert my_node.operate("?") == "?"
            my_node.quit_privilege()
        destroy([auth, my_node])

    def test_reject_privilege_request_when_other_node_has_one(self):
        auth_server = Authorizer()
        initial = PrivilegedNode("test_node")
        secondary = PrivilegedNode("test_node" + "_")

        with spinning(auth_server):
            assert initial.get_privilege() is True
            assert auth_server.approved == initial.identity

            assert secondary.get_privilege() is False
            assert auth_server.approved == initial.identity

        destroy([auth_server, initial, secondary])

    def test_server_responsive_after_rejecting_request(self):
        auth_server = Authorizer()
        initial = PrivilegedNode("test_node")
        secondary = PrivilegedNode("test_node" + "_")

        with spinning(auth_server):
            assert initial.get_privilege() is True
            assert auth_server.approved == initial.identity

            assert secondary.get_privilege() is False
            assert auth_server.approved == initial.identity

            assert initial.quit_privilege() is False
            assert (
                auth_server.approved is None
            )  # This line will catch the non-responsiveness

        destroy([auth_server, initial, secondary])

    def test_quit_privilege(self):
        auth_server = Authorizer()
        auth_client = PrivilegedNode("test_node")

        with spinning(auth_server):
            assert auth_client.get_privilege() is True
            assert auth_server.approved == auth_client.identity

            assert auth_client.quit_privilege() is False
            assert auth_server.approved is None

        destroy([auth_server, auth_client])

    def test_quit_privilege_on_destroy_node(self):
        auth_server = Authorizer()
        auth_client = PrivilegedNode("test_node")

        with spinning(auth_server):
            assert auth_client.get_privilege() is True
            assert auth_server.approved == auth_client.identity

        destroy([auth_server, auth_client])

        assert auth_client.has_privilege is False
        with pytest.raises(AssertionError):
            assert auth_server.approved is None, "Cannot communicate after Ctrl-C"

    def test_ignore_unresponsive_privileged_node(self):
        auth_server = Authorizer()
        initial = PrivilegedNode("initial")
        secondary = PrivilegedNode("secondary")

        with spinning(auth_server):
            assert initial.get_privilege() is True
            assert auth_server.approved == initial.identity

            destroy(initial)

            with temp_config({"ros.service_timeout_sec": 1}):
                assert secondary.get_privilege() is True
            assert auth_server.approved == secondary.identity

        destroy([auth_server, secondary])

    def test_server_singleton(self):
        initial = Authorizer()
        with pytest.raises(InvalidHandle):
            Authorizer()

        with spinning([initial]):
            auth_client = PrivilegedNode("test_node")
            auth_client.get_privilege()

            assert auth_client.has_privilege is True
            assert initial.approved == auth_client.identity

        destroy([initial, auth_client])

    @executor_type
    def test_assigning_executor_is_error(self, executor_type: Type[Executor]):
        auth = Authorizer()
        executor = executor_type()
        with pytest.raises(ValueError):
            executor.add_node(auth)
        executor.shutdown()
        destroy(auth)
