import time

from .measurement_base import Measurement


class SIS_IV(Measurement):

    observation_type = "SIS_IV"

    def run(
        self,
        id: str,
        min_voltage_mV: float = -8.0,
        max_voltage_mV: float = 8.0,
        step_voltage_mV: float = 0.1,
    ) -> None:
        for bias_voltage in range(
            int(round(1000 * min_voltage_mV)),
            int(round(1000 * max_voltage_mV) + 1000 * step_voltage_mV),
            int(round(1000 * step_voltage_mV)),
        ):
            self.com.sis_bias("set", bias_voltage / 1000, id=id)
            time.sleep(0.1)
            self.com.sis_bias("?", id=id + "_I")
            self.com.sis_bias("?", id=id + "_V")
