import time

from .measurement_base import Measurement


class SIS_Tuning(Measurement):
    observation_type = "SIS_Tuning"

    def run(
        self,
        id: list,
        min_voltage_mV: float = 5.0,
        max_voltage_mV: float = 8.5,
        step_voltage_mV: float = 0.1,
        min_loatt_mA: float = 4.0,
        max_loatt_mA: float = 7.0,
        step_loatt_mA: float = 0.1,
        interval: float = 5.0,
    ) -> None:
        if interval >= 2.0:
            self.com.chopper("insert")
            for loatt_current in range(
                int(round(1000 * min_loatt_mA)),
                int(round(1000 * max_loatt_mA) + 1000 * step_loatt_mA),
                int(round(1000 * step_loatt_mA)),
            ):
                self.com.local_attenuator("pass", id=id, current=(loatt_current / 1000))
                for bias_voltage in range(
                    int(round(1000 * min_voltage_mV)),
                    int(round(1000 * max_voltage_mV) + 1000 * step_voltage_mV),
                    int(round(1000 * step_voltage_mV)),
                ):
                    self.com.sis_bias("set", mV=(bias_voltage / 1000), id=id)
                    self.com.chopper("insert")
                    time.sleep(interval)
                    self.com.chopper("remove")
                    time.sleep(interval)
            self.com.sis_bias("finalize")
            self.com.local_attenuator("finalize")
        else:
            self.logger.warning("The Measurement interval must be longer than 2.0 sec.")
