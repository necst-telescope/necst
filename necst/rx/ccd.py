from neclib.devices import CcdController as CCD_Device

from necst_msgs.srv import CCDCommand

from .. import namespace, service
from ..core import DeviceNode


class CCDController(DeviceNode):
    NodeName = "ccd"
    Namespace = namespace.rx

    def __init__(self) -> None:
        try:
            super().__init__(self.NodeName, namespace=self.Namespace)
            self.logger = self.get_logger()

            self.ccd = CCD_Device()

            service.ccd_cmd.service(self, self.capture)
        except Exception as e:
            self.logger.error(f"{self.NodeName} Node is shutdown due to Exception: {e}")
            self.destroy_node()

    def capture(
        self, request: CCDCommand.Request, response: CCDCommand.Response
    ) -> CCDCommand.Response:
        if request.capture:
            self.ccd.capture(request.savepath)
            self.logger.info("Capturing the target is completed.")
            response.captured = True
            return response
