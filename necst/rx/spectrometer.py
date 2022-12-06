import queue
import time as pytime
from collections import defaultdict
from typing import Dict, List, Optional, Tuple

from neclib.data import Resize
from neclib.devices import Spectrometer
from neclib.recorders import Recorder
from necst_msgs.msg import Spectral
from rclpy.node import Node

from .. import config, namespace, topic


class SpectralData(Node):

    NodeName = "spectrometer"
    Namespace = namespace.rx

    def __init__(self) -> None:
        super().__init__(self.NodeName, namespace=self.Namespace)
        integ = 1
        self.resizers = defaultdict(lambda: Resize(integ))
        self.io = Spectrometer()

        self.position = ""
        self.id = ""
        self.data_queue = queue.Queue()

        self.publisher = topic.quick_spectra.publisher(self)
        self.create_timer(integ, self.stream)

        self.last_data = None
        self.recorder = Recorder(config.record_root)
        self.create_timer(0.02, self.record)
        self.create_timer(0.02, self.fetch_data)

        topic.spectra_meta.subscription(self, self.update_metadata)

    def update_metadata(self, msg: Spectral) -> None:
        self.position = msg.position
        self.id = msg.id

    def fetch_spectra(self) -> None:
        if self.io.data_queue.empty():
            return
        self.data_queue.put(self.io.get_spectra())

    def get_data(self) -> Optional[Tuple[float, Dict[int, List[float]]]]:
        if self.data_queue.empty():
            return

        self.last_data = self.data_queue.get()
        timestamp, data = self.last_data
        for board_id, _data in data.items():
            self.resizers[board_id].push(_data, timestamp)
        return self.last_data

    def stream(self) -> None:
        __range = [1, 100]
        for board_id in self.resizers:
            data = self.resizers[board_id].get(__range)
            msg = Spectral(
                data=data,
                time=pytime.time(),
                position=self.position,
                id=str(__range) + self.id,
            )
            self.publisher.publish(msg)

    def record(self) -> None:
        _data = self.get_data()
        if _data is None:
            return

        time, data = _data
        for board_id, spectral_data in data:
            msg = Spectral(
                data=spectral_data, time=time, id=self.id, position=self.position
            )
            fields = msg.get_fields_and_field_types()
            chunk = [
                {"key": name, "type": type_, "value": getattr(msg, name)}
                for name, type_ in fields.items()
            ]

            self.recorder.append(
                f"{self.Namespace}/data/spectral/board{board_id}", chunk
            )