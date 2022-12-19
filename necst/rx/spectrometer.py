import queue
import time as pytime
from collections import defaultdict
from typing import Dict, List, Optional, Tuple

from neclib.data import Resize
from neclib.devices import Spectrometer
from neclib.recorders import NECSTDBWriter, Recorder
from necst_msgs.msg import Spectral
from rclpy.publisher import Publisher

from .. import config, namespace, topic
from ..core import DeviceNode


class SpectralData(DeviceNode):

    NodeName = "spectrometer"
    Namespace = namespace.rx

    def __init__(self) -> None:
        super().__init__(self.NodeName, namespace=self.Namespace)
        self.logger = self.get_logger()

        integ = 1
        self.resizers = defaultdict(lambda: Resize(integ))
        self.io = Spectrometer()

        self.position = ""
        self.id = ""
        self.data_queue = queue.Queue()

        self.publisher: Dict[int, Publisher] = {}
        self.create_timer(integ, self.stream)

        self.last_data = None
        self.recorder = Recorder(config.record_root)
        if not any(isinstance(w, NECSTDBWriter) for w in self.recorder.writers):
            self.recorder.add_writer(NECSTDBWriter())
        self.create_timer(0.02, self.record)
        self.create_timer(0.02, self.fetch_data)

        self.qlook_ch_range = (0, 100)

        topic.spectra_meta.subscription(self, self.update_metadata)
        topic.qlook_meta.subscription(self, self.update_qlook_conf)

    def update_metadata(self, msg: Spectral) -> None:
        self.logger.info(
            "Observation metadata updated : "
            f"position={msg.position}(<-{self.position}), id={msg.id}(<-{self.id})"
        )
        self.position = msg.position
        self.id = msg.id

    def update_qlook_conf(self, msg: Spectral) -> None:
        if len(msg.ch) == 2:
            self.qlook_ch_range = (min(msg.ch), max(msg.ch))
            self.logger.info(
                f"Changed Q-Look configuration: channel = {tuple(self.qlook_ch_range)}"
            )
        else:
            self.logger.warning(f"Cannot parse new Q-Look configuration: {msg}")

        if msg.integ > 0:
            for r in self.resizers.values():
                r.keep_duration = msg.integ

    def fetch_data(self) -> None:
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
        for board_id in self.resizers:
            _id = f"board{board_id}"
            if _id not in self.publisher:
                self.publisher[_id] = topic.quick_spectra[_id].publisher(self)

            data = self.resizers[board_id].get(self.qlook_ch_range, n_samples=100)
            msg = Spectral(
                data=data,
                time=pytime.time(),
                position=self.position,
                id=self.id,
                ch=tuple(map(int, self.qlook_ch_range)),
                integ=float(self.resizers[board_id].keep_duration),
            )
            self.publisher[_id].publish(msg)

    def record(self) -> None:
        _data = self.get_data()
        if _data is None:
            return
        if not self.recorder.is_recording:
            self.logger.warning(
                "Recorder not started, skipping recording", throttle_duration_sec=5
            )
            return

        time, data = _data
        for board_id, spectral_data in data.items():
            msg = Spectral(
                data=spectral_data, time=time, id=self.id, position=self.position
            )
            fields = msg.get_fields_and_field_types()
            chunk = [
                {"key": name, "type": type_, "value": getattr(msg, name)}
                for name, type_ in fields.items()
            ]

            self.recorder.append(f"{namespace.data}/spectral/board{board_id}", chunk)
