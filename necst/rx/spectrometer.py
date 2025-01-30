import os
import queue
import re
import time as pytime
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from neclib.data import Resize
from neclib.recorders import NECSTDBWriter, Recorder
from neclib.utils import ConditionChecker
from necst_msgs.msg import Binning, ControlStatus, Sampling, Spectral, TPModeMsg
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
        self._enabled = []

    def enabled(self, time: float) -> bool:
        try:
            past = [tag for tag in self._enabled if tag[1] < time]
            return past[-1][0]
        except IndexError:
            return False

    def set(
        self, time: float, position: Optional[str] = None, id: Optional[str] = None
    ) -> None:
        if (position is None) or (id is None):
            last = self.get(time)
            position = last.position if position is None else position
            id = last.id if id is None else id

        self.mode.append(self.ObservingMode(position=position, id=id, time=time))
        self.mode.sort(key=lambda x: x.time)

        now = pytime.time()
        # Assume we won't get any data taken more than 30 seconds ago.
        n_stale_modes = len(list(filter(lambda x: x.time < now - 30, self.mode)))
        if n_stale_modes > 1:
            # Keep last one observing mode, which will define current status.
            [self.mode.pop(0) for _ in range(n_stale_modes - 1)]

    def get(self, time: float) -> ObservingMode:
        try:
            if self.enabled(time):
                return tuple(filter(lambda x: x.time < time, self.mode))[-1]
            else:
                return self.ObservingMode(time=time)
        except IndexError:
            new = self.ObservingMode(time=time)
            self.mode.append(new)
            return new

    def disable(self, start: float) -> None:
        tag = (False, start)
        if tag in self._enabled:
            return
        self._enabled.append(tag)
        self._enabled.sort(key=lambda x: x[1])

        now = pytime.time()
        n_stale = len([tag for tag in self._enabled if tag[1] < now - 30])
        for _ in range(n_stale):
            self._enabled.pop(0)

    def enable(self, start: float) -> None:
        tag = (True, start)
        if tag in self._enabled:
            return
        self._enabled.append(tag)
        self._enabled.sort(key=lambda x: x[1])

        now = pytime.time()
        n_stale = len([tag for tag in self._enabled if tag[1] < now - 30])
        for _ in range(n_stale):
            self._enabled.pop(0)


class SpectralData(DeviceNode):
    NodeName = "spectrometer"
    Namespace = namespace.rx

    def __init__(self) -> None:
        super().__init__(self.NodeName, namespace=self.Namespace)
        self.logger = self.get_logger()

        try:
            from neclib.devices import Spectrometer
        except ImportError:
            self.logger.error(
                "Configuration for spectrometer not found; "
                "no spectral data would be recorded"
            )
            return

        self.io = Spectrometer()

        self.metadata = ObservingModeManager()

        self.resizers = {}
        self.data_queue = {}
        for key, _ in self.io.items():
            self.data_queue[key] = queue.Queue()
            self.resizers[key] = defaultdict(lambda: Resize(1))

        self.publisher: Dict[int, Publisher] = {}
        self.create_timer(1, self.stream)

        self.last_data = {}
        record_root = os.environ.get("NECST_RECORD_ROOT", None)
        self.recorder = Recorder(record_root or Path.home() / "data")
        if not any(isinstance(w, NECSTDBWriter) for w in self.recorder.writers):
            self.recorder.add_writer(NECSTDBWriter())
        self.record_condition = ConditionChecker(
            config.record_every_n_spectral_data or 1, True
        )
        self.create_timer(0.02, self.record)
        self.create_timer(0.02, self.fetch_data)

        self.qlook_ch_range = (0, 100)

        self.tp_mode = False
        self.tp_range = None

        topic.spectra_meta.subscription(self, self.update_metadata)
        topic.qlook_meta.subscription(self, self.update_qlook_conf)
        topic.antenna_control_status.subscription(self, self.update_control_status)
        topic.spectra_rec.subscription(self, self.change_record_frequency)
        topic.tp_mode.subscription(self, self.tp_mode_func)
        topic.channel_binning.subscription(self, self.change_spec_chan)

    def change_record_frequency(self, msg: Sampling) -> None:
        nth = max(msg.nth, 1)
        if msg.save:
            self.record_condition = ConditionChecker(nth, True)
            self.logger.info(
                f"Record frequency changed; every {nth}th data will be saved"
            )
        else:
            nth = float("inf")
            self.record_condition = ConditionChecker(nth, True)
            self.logger.info("Spectral data will NOT be saved")

    def tp_mode_func(self, msg: TPModeMsg) -> None:
        # tp_range: List[int, int] or None
        self.tp_mode = msg.tp_mode
        self.tp_range = msg.tp_range.tolist()
        if self.tp_mode:
            if self.tp_range:
                self.logger.info(f"Total power will be saved. Range: {self.tp_range}")
            elif self.tp_range == []:
                self.logger.info("Total power will be saved. Range: all channels")

    def change_spec_chan(self, msg: Binning) -> None:
        record_chan = msg.ch
        self.io.change_spec_ch(record_chan)
        self.logger.info(
            f"Record channel number changed; {record_chan} ch data will be saved"
        )

    def update_control_status(self, msg: ControlStatus) -> None:
        if msg.tight:
            self.metadata.enable(start=msg.time)
        else:
            self.metadata.disable(start=msg.time)

    def update_metadata(self, msg: Spectral) -> None:
        now = pytime.time()
        dt = msg.time - now
        current = self.metadata.get(now)
        logmsg = f"in {dt:.3f}s" if dt > 0 else "immediately"
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
            for key, resizers in self.resizers.items():
                for r in resizers.values():
                    r.keep_duration = msg.integ

    def fetch_data(self) -> None:
        for key, io in self.io.items():
            if io.data_queue.empty():
                return
            self.data_queue[key].put(io.get_spectra())

    def get_data(self) -> Optional[Tuple[float, Dict[int, List[float]]]]:
        for key, data_queue in self.data_queue.items():
            if data_queue.empty():
                return

            self.last_data[key] = data_queue.get()
            timestamp, data = self.last_data[key]
            for board_id, _data in data.items():
                self.resizers[key][board_id].push(_data, timestamp)

        return self.last_data

    def stream(self) -> None:
        for key, resizers in self.resizers.items():
            for board_id in resizers:
                _id = f"{key}_board{board_id}"
                if _id not in self.publisher:
                    self.publisher[_id] = topic.quick_spectra[_id].publisher(self)

                data = resizers[board_id].get(self.qlook_ch_range, n_samples=100)
                now = pytime.time()
                metadata = self.metadata.get(now)
                msg = Spectral(
                    data=data,
                    time=now,
                    position=metadata.position,
                    id=metadata.id,
                    ch=tuple(map(int, self.qlook_ch_range)),
                    integ=float(self.resizers[key][board_id].keep_duration),
                )
                self.publisher[_id].publish(msg)

    def record(self) -> None:
        _data_dict = self.get_data()
        if _data_dict is None:
            return
        for key, _data in _data_dict.items():
            if _data is None:
                return

            if not self.record_condition.check(True):  # Skip recording
                return
            else:
                self.record_condition.check(False)

            if not self.recorder.is_recording:
                self.logger.warning(
                    "Recorder not started, skipping recording", throttle_duration_sec=30
                )
                return

            time, data = _data

            if self.tp_mode:
                data = self.io[key].calc_tp(data, self.tp_range)
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
                for _chunk in chunk:
                    if _chunk["type"].startswith("string"):
                        _chunk["value"] = _chunk["value"].ljust(
                            int(
                                re.sub(r"\D", "", _chunk["type"])
                                or len(_chunk["value"])
                            )
                        )

                try:
                    self.recorder.append(
                        f"{namespace.data}/spectral/{key}/board{board_id}", chunk
                    )
                except RuntimeError:
                    pass
