import queue
import time as pytime
from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

from neclib.data import Resize
from neclib.devices import Spectrometer
from neclib.recorders import NECSTDBWriter, Recorder
from necst_msgs.msg import Spectral
from rclpy.publisher import Publisher

from .. import config, namespace, topic
from ..core import DeviceNode


class ObservingModeManager:
    @dataclass(frozen=True)
    class ObservingMode:
        time: float
        position: str = ""
        id: str = ""

    def __init__(self) -> None:
        self.mode = []

    def set(
        self, time: float, position: Optional[str] = None, id: Optional[str] = None
    ) -> None:
        last = self.get(time)
        if position is None:
            position = last.position
        if id is None:
            id = last.id
        self.mode.append(self.ObservingMode(position=position, id=id, time=time))
        self.mode.sort(key=lambda x: x.time)

        n_stale_modes = len(list(filter(lambda x: x.time < pytime.time(), self.mode)))
        if n_stale_modes > 1:
            [self.mode.pop(0) for _ in range(n_stale_modes - 1)]

    def get(self, time: float) -> ObservingMode:
        try:
            return tuple(filter(lambda x: x.time < time, self.mode))[-1]
        except IndexError:
            new = self.ObservingMode(time=time)
            self.mode.append(new)
            return new


class SpectralData(DeviceNode):

    NodeName = "spectrometer"
    Namespace = namespace.rx

    def __init__(self) -> None:
        super().__init__(self.NodeName, namespace=self.Namespace)
        self.logger = self.get_logger()

        self.resizers = defaultdict(lambda: Resize(1))
        self.io = Spectrometer()

        self.metadata = ObservingModeManager()
        self.data_queue = queue.Queue()

        self.publisher: Dict[int, Publisher] = {}
        self.create_timer(1, self.stream)

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
        now = pytime.time()
        dt = msg.time - now
        current = self.metadata.get(now)
        logmsg = f"in {dt:.3s}s" if dt > 0 else "immediately"
        self.logger.info(
            f"Observation metadata updated : position={msg.position!r}"
            f"(<-{current.position!r}), id={msg.id!r}(<-{current.id!r}); will take "
            f"effect {logmsg}"
        )
        self.metadata.set(msg.time, msg.position, msg.id)

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
            now = pytime.time()
            metadata = self.metadata.get(now)
            msg = Spectral(
                data=data,
                time=now,
                position=metadata.position,
                id=metadata.id,
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
            metadata = self.metadata.get(time)
            msg = Spectral(
                data=spectral_data,
                time=time,
                id=metadata.id,
                position=metadata.position,
            )
            fields = msg.get_fields_and_field_types()
            chunk = [
                {"key": name, "type": type_, "value": getattr(msg, name)}
                for name, type_ in fields.items()
            ]

            try:
                self.recorder.append(
                    f"{namespace.data}/spectral/board{board_id}", chunk
                )
            except RuntimeError:
                pass
