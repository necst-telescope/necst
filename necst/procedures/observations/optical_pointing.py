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
        t_tot = opt_pointing.estimate_time(sorted_list)
        self.logger.info(
            f"Starting Optical Pointing Observation. It takes about {t_tot/60} minutes."
        )
        date = obsdatetime.strftime("%Y%m%d_%H%M%S")
        save_directory = config.ccd_controller.pic_captured_path + "/" + date
        """""
        cap_az = []
        cap_el = []
        cap_pic_filename = []
        cap_ra = []
        cap_dec = []
        cap_time = []
        """
        captured_num = 0

        try:
            for i in range(len(sorted_list)):
                self.com.antenna(
                    "point",
                    target=(
                        float(sorted_list["ra"][i]),
                        float(sorted_list["dec"][i]),
                        "fk5",
                    ),
                    unit="deg",
                    wait=True,
                )
                time.sleep(3.0)
                save_filename = date + "No" + str(i) + ".JPG"
                save_path = save_directory + "/" + save_filename

                coord_before = self.com.antenna("?")
                az_before, el_before, time_before = (
                    coord_before.lon,
                    coord_before.lat,
                    coord_before.time,
                )
                if drive_test is False:
                    self.com.ccd("capture", name=save_path)

                coord_after = self.com.antenna("?")
                az_after, el_after, time_after = (
                    coord_after.lon,
                    coord_after.lat,
                    coord_after.time,
                )

                """""
                cap_az.append((az_before + az_after) / 2)
                cap_el.append((el_before + el_after) / 2)
                cap_pic_filename.append(save_filename)
                cap_ra.append(float(sorted_list["ra"][i]))
                cap_dec.append(float(sorted_list["dec"][i]))
                cap_time.append((time_before + time_after) / 2)
                """
                cap_az = (az_before + az_after) / 2
                cap_el = (el_before + el_after) / 2
                ra = float(sorted_list["ra"][i])
                dec = float(sorted_list["dec"[i]])
                cap_time = (time_before + time_after) / 2
                captured_num += 1
                time.sleep(8.0)
                self.logger.info(f"Target {i+1}/{len(sorted_list)} is completed.")
                data = [cap_az, cap_el, ra, dec]
                self.com.metadata(
                    "set",
                    optical_data=data,
                    position=f"No{i}",
                    id=date,
                    time=cap_time,
                )
        except KeyboardInterrupt:
            self.logger.info("Operation was Interrupted. Stopping antenna...")
            self.com.antenna("stop")
        else:
            self.logger.info(
                f"Optical Pointing is completed: {captured_num} stars were captured."
            )

        """"
        sorted_list["cap_az"] = cap_az
        sorted_list["cap_el"] = cap_el
        sorted_list["cap_time"] = cap_time
        sorted_list["pic_filename"] = cap_pic_filename
        self.com.metadata(
            "set",
            optical_data=sorted_list["cap_az"].values.tolist(),
            position="",
            id="cap_az",
        )
        time.sleep(1.0)
        self.com.metadata(
            "set",
            optical_data=sorted_list["cap_el"].values.tolist(),
            position="",
            id="cap_el",
        )
        time.sleep(1.0)
        self.com.metadata(
            "set", optical_data=sorted_list["ra"].values.tolist(), position="", id="ra"
        )
        time.sleep(1.0)
        self.com.metadata(
            "set",
            optical_data=sorted_list["dec"].values.tolist(),
            position="",
            id="dec",
        )
        time.sleep(1.0)
        self.com.metadata(
            "set",
            optical_data=sorted_list["cap_time"].values.tolist(),
            position="",
            id="cap_time",
        )
        time.sleep(1.0)
        # to check locus
        self.com.metadata(
            "set", optical_data=sorted_list["az"].values.tolist(), position="", id="az"
        )
        time.sleep(1.0)
        self.com.metadata(
            "set", optical_data=sorted_list["el"].values.tolist(), position="", id="el"
        )
        """
