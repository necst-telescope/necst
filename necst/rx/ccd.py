from neclib.devices import CcdController as CCD_Device
from necst_msgs.msg import CCDMsg

from .. import namespace, topic
from ..core import DeviceNode


class CCDController(DeviceNode):
    NodeName = "ccd"
    Namespace = namespace.rx

    def __init__(self) -> None:
        super().__init__(self.NodeName, namespace=self.Namespace)
        self.logger = self.get_logger()

        self.ccd = CCD_Device()

        topic.capture_cmd.subscription(self, self.capture)

    def capture(self, msg: CCDMsg) -> None:
        if msg.shot:
            self.ccd.capture(msg.savepath)
            self.logger.info("Capturing the target is completed.")
