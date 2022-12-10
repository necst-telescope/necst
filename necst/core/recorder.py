import importlib
import os
import re
from functools import partial
from typing import Any, Union

import neclib
from neclib.recorders import Recorder as LibRecorder
from rclpy.node import Node

from .. import config, namespace, qos


class Recorder(Node):

    NodeName = "recorder"
    Namespace = namespace.core

    def __init__(self, record_dir: Union[str, os.PathLike] = None) -> None:
        super().__init__(self.NodeName, namespace=self.Namespace)

        self.recorder = LibRecorder(config.record_root)

        self.subscriber = {}
        self.recorder.add_writer(
            neclib.recorders.NECSTDBWriter(),
            neclib.recorders.FileWriter(),
            neclib.recorders.ConsoleLogWriter(),
        )

        self.create_timer(config.ros_topic_scan_interval_sec, self.scan_topics)
        self.recorder.start_recording(record_dir)

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
        for name, msg_type_str in topics:
            (msg_type_str,) = msg_type_str  # Extract only element of list
            if name not in self.subscriber.keys():
                msg_type = self._get_msg_type(msg_type_str)
                self.subscriber[name] = self.create_subscription(
                    msg_type,
                    name,
                    partial(self.append, topic_name=name),
                    qos.adaptive(name, self),
                )
                # Callback argument isn't supported in `create_subscription`.
                # The following link can provide a solution, i.e.,
                # `callback=lambda msg: self.append(msg, name)` but argument `name` is
                # just a reference, so the common callback can be called with unexpected
                # argument value.
                # https://answers.ros.org/question/362954/ros2create_subscription-how-to-pass-callback-arguments/?answer=393430#post-id-393430

    def append(self, msg, topic_name: str) -> None:
        fields = msg.get_fields_and_field_types()
        chunk = [
            {"key": name, "type": type_, "value": getattr(msg, name)}
            for name, type_ in fields.items()
        ]
        for _chunk in chunk:
            if "string" in _chunk["type"]:
                _chunk["value"] = _chunk["value"].ljust(
                    int(re.sub(r"\D", "", _chunk["type"]) or len(_chunk["value"]))
                )

        self.recorder.append(topic_name, chunk)

    def destroy_node(self) -> None:
        try:
            # IMPORTANT: If `stop_recording` isn't called, no data will be saved.
            self.recorder.stop_recording()
        except AttributeError:
            pass
        finally:
            super().destroy_node()


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
