import os
import re
from functools import partial
from pathlib import Path

from neclib.recorders import ConsoleLogWriter, FileWriter, NECSTDBWriter, Recorder
from necst_msgs.srv import File, RecordSrv

from .. import config, namespace, qos, service, utils
from .server_node import ServerNode


class RecorderController(ServerNode):

    NodeName = "recorder"
    Namespace = namespace.core

    def __init__(self) -> None:
        super().__init__(self.NodeName, namespace=self.Namespace)

        self.logger = self.get_logger()
        record_root = os.environ.get("NECST_RECORD_ROOT")
        self.recorder = Recorder(record_root or Path.home() / "data")

        self.subscriber = {}
        writers = [NECSTDBWriter(), FileWriter(), ConsoleLogWriter()]
        for writer in writers:
            if writer not in self.recorder.writers:
                self.recorder.add_writer(writer)

        self.create_timer(config.ros.topic_scan_interval_sec, self.scan_topics)

        service.record_path.service(self, self.change_directory)
        service.record_file.service(self, self.write_file)
        self.start_server()

    def change_directory(
        self, request: RecordSrv.Request, response: RecordSrv.Response
    ) -> RecordSrv.Response:
        if request.stop and self.recorder.is_recording:
            self.recorder.stop_recording()
            self.logger.info("Recording has been stopped.")
            response.recording = False
        elif request.stop:
            self.logger.info("Recording has already been stopped.")
            response.recording = False
        elif not self.recorder.is_recording:
            self.recorder.start_recording(request.name or None)
            self.logger.info(f"Recorder started: {self.recorder.recording_path!s}")
            response.recording = True
        elif self.recorder.recording_path != self.recorder.record_root / request.name:
            self.logger.info(f"Stopped recording: {self.recorder.recording_path!s}")
            self.recorder.stop_recording()
            self.recorder.start_recording(request.name)
            self.logger.info(f"Recorder started: {self.recorder.recording_path!s}")
            response.recording = True
        else:
            self.logger.info(f"Continue recording: {self.recorder.recording_path!s}")
            response.recording = True
        return response

    def write_file(
        self, request: File.Request, response: File.Response
    ) -> File.Response:
        if self.recorder.is_recording:
            self.recorder.append(path=request.path, contents=request.data)
            response.success = True
        else:
            msg = f"This recorder hasn't been started, discarding {request.path!r}"
            self.logger.warning(msg)
            response.success = False
        return response

    # def _get_msg_field_info(self, path: str):
    #     mapping = {"double": "float64", "float": "float32"}
    #     msg = utils.import_msg(path)
    #     info = []
    #     for name, type_ in msg.get_fields_and_field_types().items():
    #         info.append({"key": name, "format": mapping.get(type_, type_)})
    #     return info

    def scan_topics(self):
        topics = self.get_topic_names_and_types()
        for name, msg_type_str in topics:
            (msg_type_str,) = msg_type_str  # Extract only element of list
            if name not in self.subscriber.keys():
                msg_type = utils.import_msg(msg_type_str)
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
        if not self.recorder.is_recording:
            msg = f"This recorder hasn't been started, discarding message {msg!r}"
            self.logger.warning(msg[: min(100, len(msg))], throttle_duration_sec=30)
            return

        fields = msg.get_fields_and_field_types()
        chunk = [
            {"key": name, "type": type_, "value": getattr(msg, name)}
            for name, type_ in fields.items()
        ]
        for _chunk in chunk:
            if _chunk["type"].startswith("string"):
                _chunk["value"] = _chunk["value"].ljust(
                    int(re.sub(r"\D", "", _chunk["type"]) or len(_chunk["value"]))
                )

        try:
            self.recorder.append(topic_name, chunk)
        except RuntimeError:
            pass

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
    node = RecorderController()
    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    finally:
        node.destroy_node()
        rclpy.try_shutdown()
