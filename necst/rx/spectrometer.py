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

from .. import config, namespace, service, topic
from ..core import DeviceNode
from .spectral_recording_runtime import (
    SpectralRecordingRuntimeError,
    SpectralRecordingRuntimeState,
    namespace_db_path,
    response_lists,
    is_tp_stream,
    slice_spectrum_for_stream,
    tp_chunk_for_stream,
    spectrum_chunk_for_stream,
    spectrum_extra_chunk,
    legacy_spectral_string_field,
)


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


class ControlSectionManager:
    @dataclass(frozen=True)
    class ControlSection:
        time: float
        kind: str = ""
        label: str = ""
        line_index: int = -1

    def __init__(self) -> None:
        self.section = []

    def set(
        self,
        time: float,
        kind: str = "",
        label: str = "",
        line_index: int = -1,
    ) -> None:
        self.section.append(
            self.ControlSection(
                time=time,
                kind=kind,
                label=label,
                line_index=int(line_index),
            )
        )
        self.section.sort(key=lambda x: x.time)

        now = pytime.time()
        n_stale = len([entry for entry in self.section if entry.time < now - 30])
        if n_stale > 1:
            [self.section.pop(0) for _ in range(n_stale - 1)]

    def get(self, time: float) -> ControlSection:
        try:
            return tuple(filter(lambda x: x.time < time, self.section))[-1]
        except IndexError:
            new = self.ControlSection(time=time)
            self.section.append(new)
            return new


def _fit_string(value: str, limit: int) -> str:
    value = str(value)
    return value[:limit]


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
        self.control_section = ControlSectionManager()

        self.resizers = {}
        self.data_queue = {}
        for key, _ in self.io.items():
            self.data_queue[key] = queue.Queue()
            self.resizers[key] = defaultdict(lambda: Resize(1))

        fetch_limit = int(getattr(config, "spectrometer_fetch_max_packets_per_timer", 8) or 8)
        self._fetch_max_packets_per_timer = fetch_limit if fetch_limit > 0 else 1

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

        self.spectral_recording_runtime = SpectralRecordingRuntimeState()
        self._spectral_recording_apply_srv = service.apply_spectral_recording_setup.service(
            self, self.apply_spectral_recording_setup
        )
        self._spectral_recording_gate_srv = service.set_spectral_recording_gate.service(
            self, self.set_spectral_recording_gate
        )
        self._spectral_recording_clear_srv = service.clear_spectral_recording_setup.service(
            self, self.clear_spectral_recording_setup
        )

        topic.spectra_meta.subscription(self, self.update_metadata)
        topic.qlook_meta.subscription(self, self.update_qlook_conf)
        topic.antenna_control_status.subscription(self, self.update_control_status)
        topic.spectra_rec.subscription(self, self.change_record_frequency)
        topic.tp_mode.subscription(self, self.tp_mode_func)
        topic.channel_binning.subscription(self, self.change_spec_chan)


    def apply_spectral_recording_setup(self, request, response):
        try:
            setup = self.spectral_recording_runtime.apply(
                snapshot_toml=request.snapshot_toml,
                snapshot_sha256=request.snapshot_sha256,
                setup_id=request.setup_id,
                strict=bool(request.strict),
            )
            for stream in setup.streams.values():
                stream["_runtime_db_append_path"] = namespace_db_path(
                    namespace.root, str(stream["db_table_path"])
                )
            lists = response_lists(setup)
            response.success = True
            response.setup_id = setup.setup_id
            response.setup_hash = setup.setup_hash
            response.active_mode_summary = setup.active_mode_summary
            response.warnings = list(setup.warnings)
            response.errors = []
            response.enabled_streams = lists["enabled_streams"]
            response.disabled_streams = lists["disabled_streams"]
            response.spectrum_streams = lists["spectrum_streams"]
            response.tp_streams = lists["tp_streams"]
            self.logger.info(
                "Applied spectral recording setup; gate is closed: "
                + setup.active_mode_summary
            )
        except Exception as exc:
            response.success = False
            response.setup_id = getattr(request, "setup_id", "")
            response.setup_hash = ""
            response.active_mode_summary = ""
            response.warnings = []
            response.errors = [str(exc)]
            response.enabled_streams = []
            response.disabled_streams = []
            response.spectrum_streams = []
            response.tp_streams = []
            self.logger.error(f"Failed to apply spectral recording setup: {exc}")
        return response

    def set_spectral_recording_gate(self, request, response):
        try:
            self.spectral_recording_runtime.set_gate(
                setup_id=request.setup_id,
                setup_hash=request.setup_hash,
                allow_save=bool(request.allow_save),
            )
            response.success = True
            response.warnings = []
            response.errors = []
            state = "open" if request.allow_save else "closed"
            self.logger.info(f"Spectral recording setup gate is now {state}")
        except Exception as exc:
            response.success = False
            response.warnings = []
            response.errors = [str(exc)]
            self.logger.error(f"Failed to set spectral recording setup gate: {exc}")
        return response

    def clear_spectral_recording_setup(self, request, response):
        try:
            warnings = self.spectral_recording_runtime.clear(
                setup_id=getattr(request, "setup_id", ""),
                setup_hash=getattr(request, "setup_hash", ""),
                strict=bool(getattr(request, "strict", True)),
            )
            response.success = True
            response.warnings = list(warnings)
            response.errors = []
            self.logger.info("Cleared spectral recording setup")
        except Exception as exc:
            response.success = False
            response.warnings = []
            response.errors = [str(exc)]
            self.logger.error(f"Failed to clear spectral recording setup: {exc}")
        return response

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

    def tp_mode_func(self, msg: TPModeMsg) -> None:
        if self.spectral_recording_runtime.active:
            try:
                self.spectral_recording_runtime.active_setup.reject_legacy_tp_mode()
            except SpectralRecordingRuntimeError as exc:
                message = str(exc)
                self.logger.error(message)
                self.spectral_recording_runtime.latch_fatal_error(message)
            return

        # tp_range: List[int, int] or None
        self.tp_mode = msg.tp_mode
        self.tp_range = msg.tp_range.tolist()
        if self.tp_mode:
            if self.tp_range:
                self.logger.info(f"Total power will be saved. Range: {self.tp_range}")
            elif self.tp_range == []:
                self.logger.info("Total power will be saved. Range: all channels")

    def change_spec_chan(self, msg: Binning) -> None:
        if self.spectral_recording_runtime.active:
            try:
                self.spectral_recording_runtime.active_setup.reject_legacy_channel_binning()
            except SpectralRecordingRuntimeError as exc:
                message = str(exc)
                self.logger.error(message)
                self.spectral_recording_runtime.latch_fatal_error(message)
            return

        for key, io in self.io.items():
            record_chan = msg.ch
            io.change_spec_ch(record_chan)
            self.logger.info(
                f"{key}'s channel number changed; {record_chan} ch data will be saved"
            )

    def update_control_status(self, msg: ControlStatus) -> None:
        if msg.tight:
            self.metadata.enable(start=msg.time)
        else:
            self.metadata.disable(start=msg.time)
        self.control_section.set(
            msg.time,
            kind=getattr(msg, "section_kind", "") or "",
            label=getattr(msg, "section_label", "") or "",
            line_index=getattr(msg, "line_index", -1),
        )

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

    def _get_spectra_nowait(self, io):
        """Return one spectrometer packet without blocking the ROS timer.

        XFFTS and AC240 already buffer packets in ``io.data_queue`` from their
        own driver threads.  Calling ``io.get_spectra()`` on an empty queue would
        block the spectrometer node timer and can delay reads from other
        spectrometers.  Queue-backed devices are therefore drained with
        ``get_nowait()``.  Non queue-backed simulators or future devices keep the
        old ``get_spectra()`` path.
        """

        device_queue = getattr(io, "data_queue", None)
        if device_queue is None:
            try:
                return io.get_spectra()
            except queue.Empty:
                return None

        try:
            packet = device_queue.get_nowait()
        except queue.Empty:
            return None

        if hasattr(io, "warn"):
            io.warn = True
        return packet

    def fetch_data(self) -> None:
        for key, io in self.io.items():
            device_queue = getattr(io, "data_queue", None)
            max_packets = self._fetch_max_packets_per_timer if device_queue is not None else 1
            for _ in range(max_packets):
                packet = self._get_spectra_nowait(io)
                if packet is None:
                    break
                self.data_queue[key].put(packet)

    def get_data(self) -> Optional[Dict[str, Tuple[float, str, Dict[int, List[float]]]]]:
        if not self.data_queue:
            return None
        if any(data_queue.empty() for data_queue in self.data_queue.values()):
            return None

        batch = {}
        for key, data_queue in self.data_queue.items():
            try:
                batch[key] = data_queue.get_nowait()
            except queue.Empty:
                return None

        for key, packet in batch.items():
            self.last_data[key] = packet
            timestamp, _time_spectrometer, data = packet
            for board_id, _data in data.items():
                self.resizers[key][board_id].push(_data, timestamp)

        return batch

    def stream(self) -> None:
        for key, resizers in self.resizers.items():
            for board_id in resizers:
                _id = f"{key}_board{board_id}"
                if _id not in self.publisher:
                    self.publisher[_id] = topic.quick_spectra[_id].publisher(self)

                data = resizers[board_id].get(self.qlook_ch_range, n_samples=100)
                now = pytime.time()
                metadata = self.metadata.get(now)
                section = self.control_section.get(now)
                line_index = -1
                line_label = ""
                if (metadata.position == "ON") and (section.kind == "line"):
                    line_index = int(section.line_index)
                    line_label = _fit_string(section.label, 64)
                msg = Spectral(
                    data=data,
                    time=now,
                    position=metadata.position,
                    id=_fit_string(metadata.id, 16),
                    line_index=line_index,
                    line_label=line_label,
                    ch=tuple(map(int, self.qlook_ch_range)),
                    integ=float(self.resizers[key][board_id].keep_duration),
                )
                self.publisher[_id].publish(msg)

    def _spectral_chunk(self, msg: Spectral) -> List[Dict[str, object]]:
        fields = msg.get_fields_and_field_types()
        chunk = [
            {"key": name, "type": type_, "value": getattr(msg, name)}
            for name, type_ in fields.items()
        ]
        for _chunk in chunk:
            if _chunk["type"].startswith("string"):
                _chunk["value"] = _fit_string(
                    _chunk["value"],
                    int(re.sub(r"\D", "", _chunk["type"]) or len(_chunk["value"])),
                ).ljust(
                    int(re.sub(r"\D", "", _chunk["type"]) or len(_chunk["value"]))
                )
        return chunk

    def _make_spectral_message(
        self,
        *,
        spectral_data,
        time: float,
        time_spectrometer: str,
        metadata,
        section,
    ) -> Spectral:
        line_index = -1
        line_label = ""
        if (metadata.position == "ON") and (section.kind == "line"):
            line_index = int(section.line_index)
            line_label = _fit_string(section.label, 64)
        return Spectral(
            data=spectral_data,
            time=time,
            id=_fit_string(metadata.id, 16),
            position=metadata.position,
            line_index=line_index,
            line_label=line_label,
            time_spectrometer=time_spectrometer,
        )

    def _legacy_spectrum_chunk(
        self,
        *,
        spectral_data,
        time: float,
        time_spectrometer: str,
        metadata,
        section,
    ) -> List[Dict[str, object]]:
        line_index = -1
        line_label = ""
        if (metadata.position == "ON") and (section.kind == "line"):
            line_index = int(section.line_index)
            line_label = _fit_string(section.label, 64)
        return [
            {"key": "data", "type": "float32", "value": spectral_data},
            legacy_spectral_string_field("position", _fit_string(metadata.position, 8), 8),
            legacy_spectral_string_field("id", _fit_string(metadata.id, 16), 16),
            {"key": "line_index", "type": "int32", "value": int(line_index)},
            legacy_spectral_string_field("line_label", line_label, 64),
            {"key": "time", "type": "float64", "value": float(time)},
            legacy_spectral_string_field(
                "time_spectrometer",
                _fit_string(time_spectrometer, 32),
                32,
            ),
            {"key": "ch", "type": "int32", "value": []},
            {"key": "rfreq", "type": "float64", "value": []},
            {"key": "ifreq", "type": "float64", "value": []},
            {"key": "vlsr", "type": "float64", "value": []},
            {"key": "integ", "type": "float64", "value": 0.0},
        ]

    def _record_legacy_stream(self, key: str, board_id: int, spectral_data, time, time_spectrometer) -> None:
        metadata = self.metadata.get(time)
        section = self.control_section.get(time)
        chunk = self._legacy_spectrum_chunk(
            spectral_data=spectral_data,
            time=time,
            time_spectrometer=time_spectrometer,
            metadata=metadata,
            section=section,
        )
        self.recorder.append(f"{namespace.data}/spectral/{key}/board{board_id}", chunk)

    def _record_active_stream(
        self,
        *,
        key: str,
        board_id: int,
        spectral_data,
        time,
        time_spectrometer,
    ) -> None:
        setup = self.spectral_recording_runtime.active_setup
        if setup is None:
            return
        setup.assert_no_fatal_error()

        streams = setup.streams_for_raw(key, board_id)
        if not streams:
            message = (
                f"No active spectral recording stream for key={key!r}, board_id={board_id}; "
                "active snapshot mode treats unknown raw streams as fatal"
            )
            self.logger.error(message)
            self.spectral_recording_runtime.latch_fatal_error(message)
            return

        metadata = self.metadata.get(time)
        section = self.control_section.get(time)
        line_index = -1
        line_label = ""
        if (metadata.position == "ON") and (section.kind == "line"):
            line_index = int(section.line_index)
            line_label = _fit_string(section.label, 64)

        for stream in streams:
            if is_tp_stream(stream):
                try:
                    chunk = tp_chunk_for_stream(
                        stream,
                        setup,
                        time=time,
                        time_spectrometer=_fit_string(time_spectrometer, 32),
                        position=_fit_string(metadata.position, 8),
                        obs_id=_fit_string(metadata.id, 16),
                        line_index=line_index,
                        line_label=line_label,
                        spectral_data=spectral_data,
                    )
                except SpectralRecordingRuntimeError as exc:
                    self.logger.error(str(exc))
                    self.spectral_recording_runtime.latch_fatal_error(str(exc))
                    return
            else:
                try:
                    saved_data = slice_spectrum_for_stream(stream, spectral_data)
                    chunk = spectrum_chunk_for_stream(
                        stream,
                        setup,
                        time=time,
                        time_spectrometer=_fit_string(time_spectrometer, 32),
                        position=_fit_string(metadata.position, 8),
                        obs_id=_fit_string(metadata.id, 16),
                        line_index=line_index,
                        line_label=line_label,
                        spectral_data=saved_data,
                    )
                except SpectralRecordingRuntimeError as exc:
                    self.logger.error(str(exc))
                    self.spectral_recording_runtime.latch_fatal_error(str(exc))
                    return

            append_path = stream.get("_runtime_db_append_path")
            if not append_path:
                append_path = namespace_db_path(namespace.root, str(stream["db_table_path"]))
            self.recorder.append(append_path, chunk)

    def record(self) -> None:
        _data_dict = self.get_data()
        if _data_dict is None:
            return
        for key, _data in _data_dict.items():
            if _data is None:
                return

            active_setup = self.spectral_recording_runtime.active_setup
            if active_setup is not None:
                try:
                    active_setup.assert_no_fatal_error()
                except SpectralRecordingRuntimeError as exc:
                    self.logger.error(str(exc), throttle_duration_sec=30)
                    return
            if active_setup is not None and not active_setup.check_save_allowed():
                self.logger.warning(
                    "Spectral recording setup gate is closed, skipping recording",
                    throttle_duration_sec=30,
                )
                return

            if not self.recorder.is_recording:
                self.logger.warning(
                    "Recorder not started, skipping recording", throttle_duration_sec=30
                )
                return

            if not self.record_condition.check(True):  # Skip recording
                return
            else:
                self.record_condition.check(False)

            time, time_spectrometer, data = _data

            if active_setup is None and self.tp_mode:
                data = self.io[key].calc_tp(data, self.tp_range)

            for board_id, spectral_data in data.items():
                try:
                    if active_setup is None:
                        self._record_legacy_stream(
                            key, board_id, spectral_data, time, time_spectrometer
                        )
                    else:
                        self._record_active_stream(
                            key=key,
                            board_id=board_id,
                            spectral_data=spectral_data,
                            time=time,
                            time_spectrometer=time_spectrometer,
                        )
                        if active_setup.fatal_error:
                            self.logger.error(
                                "Active spectral recording error latched; stopping this record tick: "
                                + active_setup.fatal_error
                            )
                            return
                except RuntimeError as exc:
                    if active_setup is not None:
                        message = f"Fatal active spectral recording error: {exc}"
                        self.logger.error(message)
                        self.spectral_recording_runtime.latch_fatal_error(message)
                        return
                    pass

