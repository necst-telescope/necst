"""ROS 2 entry point for asynchronous post-observation analysis."""

from __future__ import annotations

import json
import os
from pathlib import Path

import rclpy
from necst_msgs.msg import RecordMsg
from rclpy.node import Node
from std_msgs.msg import String

from .. import namespace, topic
from ..analysis.node import SkyDipAnalysisCoordinator
from ..analysis.skydip import analyzer_from_environment
from ..notification.discord import DiscordNotifier


class AnalysisNode(Node):
    """Observe completion events and schedule SkyDip qlook delivery."""

    def __init__(self) -> None:
        super().__init__("analysis", namespace=namespace.core)
        self.coordinator = SkyDipAnalysisCoordinator(
            analyzer_from_environment(),
            DiscordNotifier.from_environment(),
            Path(os.environ.get("NECST_RECORD_ROOT", Path.home() / "data")),
            logger=self.get_logger(),
        )
        topic.observation_progress.subscription(self, self._on_progress)
        topic.record_status.subscription(self, self._on_record_status)
        self.get_logger().info("SkyDip Analysis Node started")

    def _on_progress(self, msg: String) -> None:
        try:
            payload = json.loads(msg.data)
        except (TypeError, ValueError):
            self.get_logger().warning("Ignoring malformed observation_progress message")
            return
        if isinstance(payload, dict):
            self.coordinator.on_progress(payload)

    def _on_record_status(self, msg: RecordMsg) -> None:
        self.coordinator.on_recorder_status(bool(msg.recording))

    def destroy_node(self) -> None:
        self.coordinator.shutdown()
        super().destroy_node()


def main(args=None) -> None:
    rclpy.init(args=args)
    node = None
    try:
        node = AnalysisNode()
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    finally:
        if node is not None:
            node.destroy_node()
        rclpy.try_shutdown()


if __name__ == "__main__":
    main()
