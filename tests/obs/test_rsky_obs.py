import time

from necst import qos, topic
from necst.core import Authorizer
from necst.ctrl import AntennaDeviceSimulator, HorizontalCoord
from necst.utils import spinning
from necst_msgs.msg import ChopperMsg, CoordMsg, TimedAzElFloat64

from obs.rsky_obs import rsky

from ..conftest import TesterNode, destroy


class TestRsky(TesterNode):

    NodeName = "test_rsky"

    def test_rsky(self):
        auth = Authorizer()
        dev = AntennaDeviceSimulator()
        horizontal = HorizontalCoord()

        def update(msg: ChopperMsg):
            nonlocal checked
            if msg.insert:
                checked = True

        sub = topic.chopper_cmd.subscription(self.node, update)
        timelimit = time.time() + 3
        checked = False
        while not checked:
            assert time.time() < timelimit, "Chopper command not published in 3s"
            time.sleep(0.05)

        rsky(1, 2)
