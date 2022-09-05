from typing import Type

import pytest
from rclpy.executors import Executor

from necst.core import Authorizer, PrivilegedNode
from ..conftest import TesterNode, executor_type


class TestAuthority(TesterNode):

    NodeName = "test_authority"

    def test_communication(self):
        auth_server = Authorizer()
        auth_client = PrivilegedNode("test_node")

        alive_nodes = self.node.get_node_names()
        assert auth_server.get_name() in alive_nodes
        assert auth_client.get_name() in alive_nodes

        server_communicate_with = [p.srv_name for p in auth_server.services]
        client_communicate_with = [s.srv_name for s in auth_client.clients]
        intersection = set(server_communicate_with) & set(client_communicate_with)
        assert auth_server.Namespace + "/request" in intersection

        auth_server.destroy_node()
        auth_client.destroy_node()

    def test_authorized_execution(self):
        class MyNode(PrivilegedNode):
            def __init__(self):
                super().__init__("test_node")

            @PrivilegedNode.require_privilege
            def some_operation(self, num: int):
                return num

        auth_server = Authorizer()
        my_node = MyNode()

        assert auth_server.approved is None
        assert my_node.get_privilege() is True
        assert auth_server.approved == my_node.identity

        assert my_node.some_operation(100) == 100

        auth_server.destroy_node()
        my_node.destroy_node()

    def test_unauthorized_execution(self):
        class MyNode(PrivilegedNode):
            def __init__(self):
                super().__init__("test_node")

            @PrivilegedNode.require_privilege
            def some_operation(self, num: int):
                return num

        auth_server = Authorizer()
        my_node = MyNode()

        assert auth_server.approved is None

        assert my_node.some_operation(100) is None

        auth_server.destroy_node()
        my_node.destroy_node()

    def test_not_allow_multiple_privileged_nodes(self):
        auth_server = Authorizer()
        initial = PrivilegedNode("test_node")
        secondary = PrivilegedNode("test_node" + "_")

        assert initial.get_privilege() is True
        assert auth_server.approved == initial.identity

        assert secondary.get_privilege is False
        assert auth_server.approved == initial.identity

        auth_server.destroy_node()
        initial.destroy_node()
        secondary.destroy_node()

    def test_quit_privilege(self):
        auth_server = Authorizer()
        auth_client = PrivilegedNode("test_node")

        assert auth_client.get_privilege() is True
        assert auth_server.approved == auth_client.identity

        assert auth_client.quit_privilege() is False
        assert auth_server.approved is None

        auth_server.destroy_node()
        auth_client.destroy_node()

    def test_quit_privilege_on_destroy_node(self):
        auth_server = Authorizer()
        auth_client = PrivilegedNode("test_node")

        assert auth_client.get_privilege() is True
        assert auth_server.approved == auth_client.identity

        auth_server.destroy_node()
        auth_client.destroy_node()

        assert auth_client.have_privilege is False
        assert auth_server.approved is None

    def test_server_singleton(self):
        initial = Authorizer()
        secondary = Authorizer()

        assert list(initial.services)
        assert not list(secondary.services)

        auth_client = PrivilegedNode("test_node")
        auth_client.get_privilege()

        assert auth_client.have_privilege is True
        assert initial.approved == auth_client.identity
        assert secondary.approved is None

        initial.destroy_node()
        secondary.destroy_node()
        auth_client.destroy_node()

    @pytest.mark.skip
    @executor_type
    def test_no_deadlock_when_server_only(self, executor_type: Type[Executor]):
        auth_server = Authorizer()

        executor = executor_type()
        executor.add_node(auth_server)

        for _ in range(100):
            executor.spin_once(timeout_sec=0)
        assert auth_server.approved is None

        [n.destroy_node() for n in executor.get_nodes()]
        executor.shutdown()

    @pytest.mark.skip
    @executor_type
    def test_no_deadlock_when_client_only(self, executor_type: Type[Executor]):
        auth_client = PrivilegedNode("test_node")

        executor = executor_type()
        executor.add_node(auth_client)

        for _ in range(100):
            executor.spin_once(timeout_sec=0)
        assert auth_client.get_privilege() is False
        assert auth_client.quit_privilege() is False

        [n.destroy_node() for n in executor.get_nodes()]
        executor.shutdown()

    @pytest.mark.skip
    @executor_type
    def test_no_deadlock(self, executor_type: Type[Executor]):
        auth_server = Authorizer()
        auth_client = PrivilegedNode("test_node")

        executor = executor_type(timeout_sec=0)
        executor.add_node(auth_server)
        executor.add_node(auth_client)

        for _ in range(100):
            executor.spin_once(timeout_sec=0)
        assert auth_client.get_privilege() is True
        assert auth_client.quit_privilege() is False

        [n.destroy_node() for n in executor.get_nodes()]
        executor.shutdown()
