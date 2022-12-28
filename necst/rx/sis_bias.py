import time

from neclib.devices import BiasReader, BiasSetter
from necst_msgs.msg import BiasMsg

from .. import config, namespace, topic
from ..core import DeviceNode


class SISBias(DeviceNode):

    NodeName = "sis_bias"
    Namespace = namespace.rx

    def __init__(self) -> None:
        super().__init__(self.NodeName, namespace=self.Namespace)
        self.logger = self.get_logger()

        self.reader_io = BiasReader()
        self.setter_io = BiasSetter()

        self.pub = topic.sis_bias.publisher(self)
        topic.sis_bias_cmd.subscription(self, self.set_voltage)

        self.correspondence_table = config.rx_sisbias

        self.create_timer(1, self.stream)

    def stream(self) -> None:
        for id, ch in self.correspondence_table.getter.items():
            # Possibly combined into single message, with redefining the interface.
            current = self.reader_io.get_bias_current(ch).to_value("uA").item()
            voltage = self.reader_io.get_bias_voltage(ch).to_value("mV").item()
            msg = BiasMsg(time=time.time(), current=current, voltage=voltage, id=id)
            self.pub.publish(msg)
            time.sleep(0.01)

    def set_voltage(self, msg: BiasMsg) -> None:
        ch = self.correspondence_table.setter[msg.id]
        self.setter_io.set_voltage(voltage_mV=msg.voltage, ch=ch)
        self.setter_io.apply_voltage()
        self.logger.info(f"Set voltage {msg.voltage} mV for ch {ch}")
        return
