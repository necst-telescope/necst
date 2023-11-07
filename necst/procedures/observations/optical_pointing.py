from datetime import datetime
from typing import List, Optional, Tuple
import os
import time

from neclib import config
from neclib.coordinates.observations import OpticalPointingSpec

from .observation_base import Observation


class OpticalPointing(Observation):
    observation_type = "OpticalPointing"

    def write_capture_list(
        self,
        directory: str,
        filename: str,
        az: List[float],
        el: List[float],
        pic_filename: List[str],
        ra: List[float],
        dec: List[float],
        time: List[float],
    ) -> None:
        os.makedirs(directory, exist_ok=True)
        with open(filename, mode="w") as file:
            for _az, _el, _pic, _ra, _dec, _time in zip(
                az, el, pic_filename, ra, dec, time
            ):
                file.write(
                    f"{str(_az)} {str(_el)} {_pic} "
                    f"{str(_ra)} {str(_dec)} {str(_time)}\n"
                )

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
        save_directory = config.ccd_controller.pic_captured_path / obsdatetime.strftime(
            "%Y%m%d_%H%M%S"
        )
        cap_az = []
        cap_el = []
        cap_pic_filename = []
        cap_ra = []
        cap_dec = []
        cap_time = []
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
                    save_filename = datetime.now().strftime("%Y%m%d_%H%M%S") + ".JPG"
                    save_path = save_directory / save_filename
                    coord_before = self.com.antenna("?")
                    az_before = coord_before.lon
                    el_before = coord_before.lat
                    time_before = time.time()
                    self.com.ccd("capture", name=save_path)
                    coord_after = self.com.antenna("?")
                    az_after = coord_after.lon
                    el_after = coord_after.lat
                    time_after = time.time()
                    cap_az.append((az_before + az_after) / 2)
                    cap_el.append((el_before + el_after) / 2)
                    cap_pic_filename.append(save_filename)
                    cap_ra.append(float(opt_target[1]))
                    cap_dec.append(float(opt_target[2]))
                    cap_time.append((time_before + time_after) / 2)
                    time.sleep(8.0)
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
        finally:
            save_filename = obsdatetime.strftime("%Y%m%d_%H%M%S") + ".dat"
            self.write_capture_list(
                directory=save_directory,
                filename=save_filename,
                az=cap_az,
                el=cap_el,
                pic_filename=cap_pic_filename,
                ra=cap_ra,
                dec=cap_dec,
                time=cap_time,
            )
