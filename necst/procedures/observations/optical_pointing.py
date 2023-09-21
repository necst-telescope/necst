from typing import Union
import time
from .observation_base import Observation
from neclib import config

from neclib.coordinates.observations import OpticalPointingSpec


class OpticalPointing(Observation):
    observation_type = "OpticalPointing"

    def run(self, file: str, magnitude: int) -> None:
        self.com.record("reduce", nth=60)  # 分光計のデータを取りたくない。
        delay = 0.0  # v3 にあった obstimedelay というパラメータ（常に 0.0 となっていた）
        opt_pointing = OpticalPointingSpec(time.time() + delay, "unix")
        # 何かしらのファイル読み込みの関数（Readlineなど）正直neclibに実装してもいい。-> neclib に実装してみた
        sorted_list = opt_pointing.sort(target_list=file, magnitude=magnitude)
        # self.logger.info(  # 天体の個数の表示だけでもいいかも
        #     f"Starting Optical Pointing Observation. Estimated observing time is {estimated_time} min."
        # )
        complete = 0
        # 以下 try: / except KeyboardInterrupt:
        for opt_target in sorted_list:
            self.com.antenna(
                "point",
                target=(opt_target[0], opt_target[1], "fk5"),
                unit="deg",
                wait=True,
            )
            time.sleep(3.0)  # 念のため追尾が落ち着くまで数秒待機？
            save_path = config.ccd_pic_captured_path
            self.com.ccd("capture", name=save_path)
            complete += 1
            self.logger.info(f"Target {complete}/{len(sorted_list)} is completed.")
        self.logger.info(
            f"Optical Pointing is completed: the total pointing number is {complete}."
        )
