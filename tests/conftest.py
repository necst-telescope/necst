import unittest

import rclpy


class TesterNode(unittest.TestCase):

    NodeName: str

    @classmethod
    def setUpClass(cls) -> None:
        rclpy.init()
        cls.node = rclpy.create_node(cls.NodeName)

    @classmethod
    def tearDownClass(cls) -> None:
        cls.node.destroy_node()
        rclpy.shutdown()
