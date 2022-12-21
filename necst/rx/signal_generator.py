import time

from neclib.devices import SignalGenerator as SG
from necst_msgs.msg import LocalSignal

from .. import namespace, topic
from ..core import DeviceNode


class SignalGenerator(DeviceNode):

    NodeName = "signal_generator"
    Namespace = namespace.rx

    def __init__(self) -> None:
        super().__init__(self.NodeName, namespace=self.Namespace)

        self.logger = self.get_logger()
        self.io = SG()

        self.publisher = topic.lo_signal.publisher(self)
        topic.lo_signal_cmd.subscription(self, self.set_param)

        self.create_timer(1, self.stream)

    def set_param(self, msg: LocalSignal) -> None:
        self.io.set_freq(freq_GHz=msg.freq)
        self.io.set_power(power_dBm=msg.power)
        self.io.start_output()
        self.logger.info(f"Set freq = {msg.freq} GHz, power = {msg.power} dBm")

    def stream(self) -> None:
        freq = self.io.get_freq()
        power = self.io.get_power()
        msg = LocalSignal(time=time.time(), freq=freq, power=power)
        self.publisher.publish(msg)
