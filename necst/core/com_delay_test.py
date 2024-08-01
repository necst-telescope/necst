import time

from necst_msgs.srv import ComDelaySrv

from .. import namespace, service
from rclpy.node import Node


class ComDelayTest(Node):
    NodeName = "comdelaytest"
    Namespace = namespace.core

    def __init__(self) -> None:
        super().__init__(self.NodeName, namespace=self.Namespace)

        service.com_delay.service(self, self.get_time)
        self.logger = self.get_logger()

    def get_time(
        self, request: ComDelaySrv.Request, response: ComDelaySrv.Response
    ) -> ComDelaySrv.Response:
        self.logger.info("send get_time request")
        response.input_time = request.time
        response.output_time = time.time()
        return response


def main(args=None):
    import rclpy

    rclpy.init(args=args)
    node = ComDelayTest()
    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    finally:
        node.destroy_node()
        rclpy.try_shutdown()
