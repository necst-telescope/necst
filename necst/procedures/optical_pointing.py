from typing import Union
import time
from .observation_base import Observation

from neclib.coordinates.coordinates import OpticalPointingSort


class OpticalPointing(Observation):
    observation_type = "OpticalPointing"

    def run(self, file: str, magnitude: int) -> None:
        self.com.record("reduce", nth=60)  # 分光計のデータを取りたくない。
        # 何かしらのファイル読み込みの関数（Readlineなど）正直neclibに実装してもいい。
        opt_pointing = OpticalPointingSort(time.time(), "unix")
        sorted_list = opt_pointing.sort(file or list, magnitude)
        # self.logger.info(  # 天体の個数の表示だけでもいいかも
        #     f"Starting Optical Pointing Observation. Estimated observing time is {estimated_time} min."
        # )
        complete = 0
        # 以下 try: / except KeyboardInterrupt:
        for opt_target in sorted_list:
            com.antenna(
                "point",
                target=(opt_target[0], opt_target[1], "fk5"),
                unit="deg",
                wait=True,
            )
            time.sleep(3.0)  # 念のため追尾が落ち着くまで数秒待機？
            com.ccd(
                "capture", name=savepath
            )  # savepath は neclib の config から撮影データの path を読み込んで生成
            complete += 1
            self.logger.info(f"Target {complete}/{len(sorted_list)} is completed.")
        self.logger.info(
            f"Optical Pointing is completed: the total pointing number is {complete}."
        )
