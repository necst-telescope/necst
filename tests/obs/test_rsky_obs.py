from necst import topic
from necst.core import Authorizer
from necst.ctrl import AntennaDeviceSimulator, AntennaPIDController, HorizontalCoord
from necst.utils import spinning
from necst_msgs.msg import ChopperMsg

from obs.rsky_obs import RSky

from ..conftest import TesterNode, destroy


class TestRSky(TesterNode):

    NodeName = "test_rsky"

    def test_rsky(self):
        auth = Authorizer()
        dev = AntennaDeviceSimulator()
        horizontal = HorizontalCoord()
        pid = AntennaPIDController()

        pub = topic.chopper_status.publisher(self.node)

        def update(msg: ChopperMsg):
            pub.publish(msg)

        sub = topic.chopper_cmd.subscription(self.node, update)
        with spinning([self.node, dev, horizontal, pid]):
            RSky(1, 2)

        destroy([auth, dev, horizontal, pid])
        destroy([sub, pub], node=self.node)
