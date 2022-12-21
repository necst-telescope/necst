import time

from neclib.devices import BiasReader, BiasSetter
from necst_msgs.msg import BiasMsg

from .. import config, topic
from ..core import DeviceNode


class SISBias(DeviceNode):
    def __init__(self):
        super().__init__(self.NodeName, namespace=self.Namespace)
        self.reader_io = BiasReader()
        self.setter_io = BiasSetter()

        self.pub = topic.sis_bias.publisher(self)
        topic.sis_bias_cmd.subscription(self, self.set_voltage)

        self.correspondence_table = config.sisbias

        self.create_timer(1, self.stream)

    def stream(self) -> None:
        current = self.reader_io.get_bias_current(...).to_value("uA").item()
        voltage = self.reader_io.get_bias_voltage(...).to_value("mV").item()
        msg = BiasMsg(time=time.time(), current=current, voltage=voltage)
        self.pub.publish(msg)

    def set_voltage(self, msg: BiasMsg) -> None:
        if -8 < msg.voltage < 8:
            ch = getattr(self.correspondence_table.setter, msg.id)
            self.setter_io.set_voltage(voltage_mV=msg.voltage, ch=ch)
        raise ValueError(f"Unsafe voltage: {msg.voltage} mV")
