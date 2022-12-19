from pathlib import Path

import pytest
from necst_msgs.msg import ChopperMsg

from necst import topic
from necst.core import Authorizer, Recorder
from necst.ctrl import AntennaDeviceSimulator, AntennaPIDController, HorizontalCoord
from necst.utils import spinning
from obs.skydip_obs import Skydip

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
        recorder = Recorder()
        recorder.recorder.record_root = record_root

        pub = topic.chopper_status.publisher(self.node)

        def update(msg: ChopperMsg):
            pub.publish(msg)

        sub = topic.chopper_cmd.subscription(self.node, update)
        with spinning([self.node, dev, horizontal, pid]):
            Skydip(2)

        destroy([auth, dev, horizontal, pid, recorder])
        destroy([sub, pub], node=self.node)
