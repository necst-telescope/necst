from datetime import datetime
from typing import Optional, Tuple
import time

from neclib import config
from neclib.coordinates.observations import OpticalPointingSpec

from .observation_base import Observation


class OpticalPointing(Observation):
    observation_type = "OpticalPointing"

    def run(
        self,
        file: str,
        magnitude: Tuple[float, float],
        drive_test: bool = False,
        obstime: Optional[datetime] = None,
    ) -> None:
        self.com.record("reduce", nth=60)
        if obstime is None:
            obsdatetime = datetime.now()
        else:
            obsdatetime = obstime
        obsfloattime = obsdatetime.timestamp()
        opt_pointing = OpticalPointingSpec(obsfloattime, "unix")
        sorted_list = opt_pointing.sort(
            catalog_file=file, magnitude=(float(magnitude[0]), float(magnitude[1]))
        )
        if obstime is None:
            self.logger.info(
                f"{len(sorted_list)} stars will be captured. Do you want to start?"
            )
            _input = input("(y/n) ")
            if _input != "y":
                self.logger.info("System ended.")
                return None
        else:
            self.logger.info(f"{len(sorted_list)} stars will be captured.")
            return None
        self.logger.info("Starting Optical Pointing Observation.")
        # TODO: Add time estimator before observing.
        captured_num = 0
        try:
            for opt_target in sorted_list:
                self.com.antenna(
                    "point",
                    target=(float(opt_target[1]), float(opt_target[2]), "fk5"),
                    unit="deg",
                    wait=True,
                )
                time.sleep(3.0)
                if drive_test is False:
                    save_directory = config.ccd_controller.pic_captured_path
                    save_filename = datetime.now().strftime("%Y%m%d_%H%M%S") + ".JPG"
                    save_path = (
                        save_directory
                        / obsdatetime.strftime("%Y%m%d_%H%M%S")
                        / save_filename
                    )
                    self.com.ccd("capture", name=save_path)
                    time.sleep(3.0)
                captured_num += 1
                self.logger.info(
                    f"Target {captured_num}/{len(sorted_list)} is completed."
                )
        except KeyboardInterrupt:
            self.logger.info("Operation was Interrupted. Stopping antenna...")
            self.com.antenna("stop")
        else:
            self.logger.info(
                f"Optical Pointing is completed: {captured_num} stars were captured."
            )
