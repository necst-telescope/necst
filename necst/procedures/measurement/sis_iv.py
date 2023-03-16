import rclpy
import time

from ..observation_base import Observation
from necst_msgs.msg import SISIV as SISIVMsg
from neclib.devcices import ADConverter, DAConverter


class SIS_IV(Observation):

    observation_type = "SISIV"

    def __init__(self) -> None:
        self.publisher_ = self.create_publisher(SISIVMsg, 'SIS_IVdata', 10)
        self.da_converter = DAConverter()
        self.ad_converter = ADConverter()

    def run(self) -> None:
        msg = SISIVMsg()
        id = 1
        for bias_voltage in range(-80, 81, 1):
            setting_voltage = self.da_converter.set_voltage(bias_voltage / 10.0, id)
            time.sleep(0.1)
            measured_voltage = self.ad_converter.get_voltage(id).to_value("mV").item()
            measured_current = self.ad_converter.get_current(id).to_value("uA").item()
            msg = SISIVMsg(
                time = time.time(), setting_voltage = setting_voltage, measured_voltage = measured_voltage, measured_current = measured_current, id = id
            )
            self.publisher_.publish(msg)
            self.logger.info(f'SIS IV Data: setting voltage={setting_voltage}mV, measured voltage={measured_voltage}mV, measured current={measured_current}mA')

