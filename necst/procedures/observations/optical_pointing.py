from datetime import datetime
from typing import Optional, Tuple
import time

from astropy import units as u
from neclib import config
from neclib.coordinates import DriveLimitChecker
from neclib.coordinates.observations import OpticalPointingSpec

from .observation_base import Observation


class OpticalPointing(Observation):
    observation_type = "OpticalPointing"

    @staticmethod
    def _make_plan(file, magnitude, obsdatetime, *, show_graph):
        spec = OpticalPointingSpec(obsdatetime.timestamp(), "unix")
        sorted_list = spec.sort(
            catalog_file=file,
            magnitude=(float(magnitude[0]), float(magnitude[1])),
            show_graph=show_graph,
        )
        if sorted_list.empty:
            raise RuntimeError("No optical-pointing stars are available for this time")
        return spec, spec.resolve_mount_targets(sorted_list)

    def _validate_live_tracking_slew(self, sorted_list, index: int) -> None:
        """Reject a live RA/Dec command that would select a large Az unwrap."""
        current = self.com.antenna("?")
        live_spec = OpticalPointingSpec(time.time(), "unix")
        live_altaz = live_spec.to_altaz(
            target=(
                float(sorted_list["ra"][index]) * u.deg,
                float(sorted_list["dec"][index]) * u.deg,
            ),
            frame="fk5",
        )
        checker = DriveLimitChecker(
            config.antenna_drive_critical_limit_az,
            config.antenna_drive_warning_limit_az,
        )
        selected = checker.optimize(
            current=float(current.lon) * u.deg,
            target=live_altaz.az,
        )
        if selected is None:
            raise RuntimeError(f"Star {index + 1} has no safe live mount-Az solution")
        selected_az = float(selected.to_value(u.deg))
        slew = abs(selected_az - float(current.lon))
        if slew > 180.0:
            raise RuntimeError(
                "Unsafe optical-pointing Az unwrap rejected: "
                f"star={index + 1}, current={float(current.lon):.3f}deg, "
                f"selected={selected_az:.3f}deg, slew={slew:.3f}deg"
            )
        self.logger.debug(
            f"Live Az unwrap check: star={index + 1}, "
            f"selected={selected_az:.3f}deg, slew={slew:.3f}deg"
        )

    def _pin_unwrap_branch(self, sorted_list) -> None:
        """Put the antenna on the 360deg unwrap branch this plan intends.

        The plan sweeps azimuth upward from the lowest-Az stars, so every
        target's intended mount angle is its own true azimuth (branch k=0).
        But the antenna may be parked anywhere - e.g. near 375deg from a
        previous observation. A plain RA/Dec command from there lets
        DriveLimitChecker pick whichever 360deg-equivalent is nearest to that
        parked position, i.e. the k=+1 branch, which then forces a wasted
        ~350deg unwind a star or two later once k=+1 runs past the drive
        limit (see necst-telescope/necst#481).

        Slewing once to the first star's intended mount angle fixes that: from
        then on the antenna is already on the k=0 branch, so for every
        subsequent star the nearest equivalent angle *is* the intended one and
        normal RA/Dec tracking commands stay on the branch by themselves. Only
        this one slew bypasses tracking; all actual pointing and capture runs
        on ordinary RA/Dec tracking commands.
        """
        az = float(sorted_list["mount_az"][0])
        el = float(sorted_list["el"][0])
        self.logger.info(f"Pinning Az unwrap branch: slewing to mount Az={az:.3f}deg")
        cmd_id = self.com.antenna(
            "point",
            target=(az, el, "altaz"),
            unit="deg",
            direct_mode=True,
            az_target_mode="mount",
            wait=False,
        )
        # wait=True can't be used for mount-frame commands: TrackingStatus
        # compares sky azimuth modulo 360, so it would report "arrived" at the
        # wrong branch. wait_mount_point compares the raw mechanical angle.
        self.com.wait_mount_point(az, el, command_id=cmd_id, timeout_sec=420)

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
        opt_pointing, sorted_list = self._make_plan(
            file, magnitude, obsdatetime, show_graph=True
        )
        t_tot = opt_pointing.estimate_time(sorted_list)
        if obstime is None:
            self.logger.info(f"{len(sorted_list)} stars will be captured. ")
            self.logger.info(f"It takes about {round(t_tot/60)} minutes.")
            self.logger.info("Do you want to start?")
            _input = input("(y/n) ")
            if _input != "y":
                self.logger.info("System ended.")
                return None
            # Plot review and operator input can leave the original AltAz plan
            # stale. Rebuild immediately before execution without opening a
            # second plot, then validate each live command at dispatch time.
            preview_count = len(sorted_list)
            obsdatetime = datetime.now()
            opt_pointing, sorted_list = self._make_plan(
                file, magnitude, obsdatetime, show_graph=False
            )
            self.logger.info(
                "Optical-pointing plan refreshed at execution time: "
                f"{preview_count} -> {len(sorted_list)} stars"
            )
        else:
            self.logger.info(f"{len(sorted_list)} stars will be captured.")
            return None
        self.logger.info("Starting Optical Pointing Observation.")
        date = obsdatetime.strftime("%Y%m%d_%H%M%S")
        save_directory = config.ccd_controller.pic_captured_path + "/" + date

        total = len(sorted_list)
        plan = [
            {
                "item_uid": f"opticalpointing:{i:05d}",
                "index0": i,
                "total": total,
                "label": (
                    f"Star {i + 1}/{total} "
                    f"(RA={float(sorted_list['ra'][i]):.3f}, "
                    f"Dec={float(sorted_list['dec'][i]):.3f})"
                ),
                "mode": "POINT",
                "role": "pointing",
                "obs_id": str(i),
                "drive_kind": "point",
                "geometry": {
                    "kind": "point",
                    "frame": "fk5",
                    "unit": "deg",
                    "target": [
                        float(sorted_list["ra"][i]),
                        float(sorted_list["dec"][i]),
                        "fk5",
                    ],
                },
            }
            for i in range(total)
        ]
        self.progress.set_plan(plan, observation_type=self.observation_type)

        captured_num = 0

        try:
            for i in range(total):
                item = plan[i]
                geometry = item["geometry"]
                with self.progress.item(**item):
                    with self.progress.drive(
                        kind="point", stage="moving", geometry=geometry
                    ):
                        if i == 0:
                            self._pin_unwrap_branch(sorted_list)
                        self._validate_live_tracking_slew(sorted_list, i)
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
                        with self.progress.drive(
                            kind="hold", stage="capturing", geometry=geometry
                        ):
                            captured = self.com.ccd("capture", name=save_path)
                            if not captured:
                                raise RuntimeError(
                                    f"CCD capture failed for star {i + 1}: {save_path}"
                                )

                    coord_after = self.com.antenna("?")
                    az_after, el_after, time_after = (
                        coord_after.lon,
                        coord_after.lat,
                        coord_after.time,
                    )

                    cap_az = (az_before + az_after) / 2
                    cap_el = (el_before + el_after) / 2
                    ra = float(sorted_list["ra"][i])
                    dec = float(sorted_list["dec"][i])
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
