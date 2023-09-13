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
        self.logger.info(
            f"Starting Optical Pointing Observation. Expected observing time is {ex_time} min."
        )
        complete = 0
        for opt_target in len(sorted_list):
            com.antenna(
                "point", target=(opt_target[0], opt_target[1], "fk5"), unit="deg"
            )
            time.sleep(5.0)  # 念のため追尾が落ち着くまで数秒待機？
            com.ccd.capture(savepath)
            complete += 1
            self.logger.info(f"{complete}/{len(sorted_list)} th target is completed.")
        self.logger.info(
            f"Optical Pointing is completed: the total pointing number is {complete}."
        )
        # 以降、ファイルを読み込んだ後の天体を順番に向けて写真を撮ってを繰り返す
