from necst import topic
from necst.core import Authorizer
from necst.ctrl import AntennaDeviceSimulator, AntennaPIDController, HorizontalCoord
from necst.utils import spinning
from necst_msgs.msg import ChopperMsg

from obs.skydip_obs import Skydip

from ..conftest import TesterNode, destroy


class TestSkydip(TesterNode):

    NodeName = "test_skydip"

    def test_skydip(self):
        auth = Authorizer()
        dev = AntennaDeviceSimulator()
        horizontal = HorizontalCoord()
        pid = AntennaPIDController()

        pub = topic.chopper_status.publisher(self.node)

        def update(msg: ChopperMsg):
            pub.publish(msg)

        sub = topic.chopper_cmd.subscription(self.node, update)
        with spinning([self.node, dev, horizontal, pid]):
            Skydip(2)

        destroy([auth, dev, horizontal, pid])
        destroy([sub, pub], node=self.node)
