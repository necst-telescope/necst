import time

# from neclib.devcices import BiasSetter, BiasReader
from necst_msgs.msg import SISIV as SISIVMsg

from ..observation_base import Observation


class SIS_IV(Observation):

    observation_type = "SISIV"

    def __init__(self) -> None:
        self.publisher_ = self.create_publisher(SISIVMsg, "SIS_IVdata", 10)
        # self.da_converter = DAConverter()
        # self.ad_converter = ADConverter()

    def run(
        self,
        sideband: str,
        min_voltage_mV: float = -8.0,
        max_voltage_mV: float = 8.0,
        step_voltage_mV: float = 0.1,
    ) -> None:
        msg = SISIVMsg()
        channels = self.ad_converter.Config.get("bias_setter", "channel")
        if sideband == "USB":
            channel = channels["USB"]
        elif sideband == "LSB":
            channel = channels["LSB"]
        else:
            raise ValueError("sideband must be either 'USB' or 'LSB'.")

        for bias_voltage in range(
            int(round(1000 * min_voltage_mV)),
            int(round(1000 * max_voltage_mV) + 1000 * step_voltage_mV),
            int(round(1000 * step_voltage_mV)),
        ):
            setting_voltage = self.da_converter.set_voltage(
                bias_voltage / 1000, channel
            )
            time.sleep(0.1)
            measured_voltage = (
                self.ad_converter.get_voltage(channel).to_value("mV").item()
            )
            measured_current = (
                self.ad_converter.get_current(channel).to_value("uA").item()
            )
            msg = SISIVMsg(
                time=time.time(),
                setting_voltage=setting_voltage,
                measured_voltage=measured_voltage,
                measured_current=measured_current,
                id=id,
            )
            self.publisher_.publish(msg)
            self.logger.info(
                f"SIS IV Data: setting voltage={setting_voltage}mV, measured voltage="
                f"{measured_voltage}mV, measured current={measured_current}mA"
            )
