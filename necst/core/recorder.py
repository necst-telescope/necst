import importlib
from typing import Any

from neclib.recorders import DBWriter
from rclpy.node import Node

from .. import config, namespace, qos


class Recorder(Node):

    NodeName = "recorder"
    Namespace = namespace.core

    def __init__(self) -> None:
        super().__init__(self.NodeName, namespace=self.Namespace)

        self.recoder = DBWriter(config.record_root)

        self.subscriber = {}

        self.create_timer(3, self.scan_topics)
        self.recoder.start_recording()

    def _get_msg_type(self, path: str) -> Any:
        module_name, msg_name = path.replace("/", ".").rsplit(".", 1)
        module = importlib.import_module(module_name)
        msg = getattr(module, msg_name)
        return msg

    def _get_msg_field_info(self, path: str):
        mapping = {"double": "float64", "float": "float32"}
        msg = self._get_msg_type(path)
        info = []
        for name, type_ in msg.get_fields_and_field_types().items():
            info.append({"key": name, "format": mapping.get(type_, type_)})
        return info

    def scan_topics(self):
        topics = self.get_topic_names_and_types()
        for name, msg_type in topics:
            msg_type = msg_type[0]
            table_name = name.replace("/", "-").strip("-")
            if table_name not in self.recoder.tables.keys():
                header = {"data": self._get_msg_field_info(msg_type)}
                self.get_logger().warning(str(header))
                self.recoder.add_table(table_name, header)

                msg = self._get_msg_type(msg_type)
                self.create_subscription(msg, name, self.append, qos.lowest)

    def append(self, msg) -> None:
        info = msg.get_fields_and_field_types()
        self.recoder.append({k: getattr(msg, k) for k, _ in info})


def main(args=None):
    import rclpy

    rclpy.init(args=args)
    node = Recorder()
    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    finally:
        node.destroy_node()
        rclpy.try_shutdown()
