from necst.core import Authorizer, PrivilegeManager

from ..conftest import TesterNode


class TestAuthority(TesterNode):

    NodeName = "test_authority"

    def test_communication(self):
        auth_server = Authorizer()
        auth_client = PrivilegeManager(self.NodeName)

        alive_nodes = self.node.get_node_names()
        assert auth_server.get_name() in alive_nodes
        assert auth_client.get_name() in alive_nodes

        server_communicate_with = [p.srv_name for p in auth_server.services]
        client_communicate_with = [s.srv_name for s in auth_client.clients]
        intersection = set(server_communicate_with) & set(client_communicate_with)
        assert auth_server.NameSpace + "/request" in intersection

        auth_server.destroy_node()
        auth_client.destroy_node()

    def test_authorized_execution(self):
        auth_server = Authorizer()
        auth_client = PrivilegeManager(self.NodeName)

        assert auth_server.approved is None
        assert auth_client.request_privilege() is True
        # assert auth_server.approved == auth_client.identity

        # @auth_client.require_privilege
        # def func(num: int) -> int:
        #     return num

        # assert func(100) == 100

        # auth_server.destroy_node()
        # auth_client.destroy_node()

    # def test_unauthorized_execution(self):
    #     auth_server = Authorizer()
    #     auth_client = PrivilegeManager(self.NodeName)

    #     assert auth_server.approved is None

    #     @auth_client.require_privilege
    #     def func(num: int) -> int:
    #         return num

    #     assert func(100) is None

    #     auth_server.destroy_node()
    #     auth_client.destroy_node()

    # def test_not_allow_multiple_privileged_nodes(self):
    #     auth_server = Authorizer()
    #     initial = PrivilegeManager(self.NodeName + "_")
    #     secondary = PrivilegeManager(self.NodeName)

    #     assert initial.request_privilege() is True
    #     assert auth_server.approved == initial.identity

    #     assert secondary.request_privilege is False
    #     assert auth_server.approved == initial.identity

    #     auth_server.destroy_node()
    #     initial.destroy_node()
    #     secondary.destroy_node()

    # def test_release_privilege(self):
    #     auth_server = Authorizer()
    #     auth_client = PrivilegeManager(self.NodeName)

    #     assert auth_client.request_privilege() is True
    #     assert auth_server.approved == auth_client.identity

    #     assert auth_client.release_privilege() is True
    #     assert auth_server.approved is None

    #     auth_server.destroy_node()
    #     auth_client.destroy_node()
