from pathlib import Path

import pytest
from necst_msgs.msg import ChopperMsg

from necst import topic
from necst.core import Authorizer, RecorderController
from necst.ctrl import (
    AntennaDeviceSimulator,
    AntennaPIDController,
    AntennaTrackingStatus,
    HorizontalCoord,
)
from necst.procedures import Skydip
from necst.utils import spinning

from ..conftest import TesterNode, destroy


@pytest.fixture
def record_root(tmp_path_factory) -> Path:
    return tmp_path_factory.mktemp("data")


class TestSkydip(TesterNode):

    NodeName = "test_skydip"

    def test_skydip(self, record_root):
        auth = Authorizer()
        dev = AntennaDeviceSimulator()
        horizontal = HorizontalCoord()
        pid = AntennaPIDController()
        recorder = RecorderController()
        tracking = AntennaTrackingStatus()
        recorder.recorder.record_root = record_root

        pub = topic.chopper_status.publisher(self.node)

        def update(msg: ChopperMsg):
            pub.publish(msg)

        sub = topic.chopper_cmd.subscription(self.node, update)
        with spinning([self.node, dev, horizontal, pid, tracking, auth], n_thread=5):
            Skydip(2)

        destroy([auth, dev, horizontal, pid, recorder, tracking])
        destroy([sub, pub], node=self.node)
