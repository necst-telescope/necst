"""Runtime state and helpers for spectral-recording active setup.

This module is intentionally ROS-free.  ``necst.rx.spectrometer.SpectralData``
uses these helpers for PR4/PR5, while the unit tests exercise them with simple
Python objects.

Implemented PR scope
--------------------
PR4:
  * active snapshot setup state;
  * setup hash validation;
  * setup gate closed-by-default;
  * legacy channel_binning / tp_mode conflict policy.

PR5:
  * spectrum full/slice branch;
  * no double slicing;
  * explicit skip for TP streams until PR6.

PR6:
  * TP direct append chunk builder;
  * all-NaN / zero-valid-channel semantics;
  * TP stream path and metadata helpers.
"""

from __future__ import annotations

from dataclasses import dataclass, field
import hashlib
import json
import math
from typing import Any, Dict, Iterable, List, Mapping, MutableMapping, Optional, Sequence, Tuple

try:
    from .spectral_recording_setup import _toml_loads, validate_snapshot
except Exception:  # pragma: no cover
    from spectral_recording_setup import _toml_loads, validate_snapshot  # type: ignore


class SpectralRecordingRuntimeError(RuntimeError):
    """Base error for runtime spectral-recording setup failures."""


class SpectralRecordingGateError(SpectralRecordingRuntimeError):
    """Raised when setup gate cannot be opened."""


def canonical_snapshot_sha256_from_text(snapshot_toml: str) -> str:
    # The resolver writes canonical_snapshot_sha256 into the TOML itself.  For
    # service validation we compare the raw text delivered by the caller, because
    # this is the exact setup being activated.
    return hashlib.sha256(snapshot_toml.encode("utf-8")).hexdigest()


def normalize_snapshot_hash(snapshot: Mapping[str, Any], raw_text: str = "") -> str:
    value = str(snapshot.get("canonical_snapshot_sha256") or snapshot.get("setup_hash") or "")
    if value:
        return value
    if raw_text:
        return canonical_snapshot_sha256_from_text(raw_text)
    raise SpectralRecordingRuntimeError("Snapshot does not contain canonical_snapshot_sha256.")


def load_snapshot_from_toml_text(snapshot_toml: str) -> Dict[str, Any]:
    snapshot = _toml_loads(snapshot_toml)
    validate_snapshot(snapshot)
    return snapshot


@dataclass
class ActiveSpectralRecordingSetup:
    snapshot: Dict[str, Any]
    setup_id: str
    setup_hash: str
    gate_allow_save: bool = False
    legacy_tp_policy: str = "error"
    warnings: List[str] = field(default_factory=list)
    runtime_errors: List[str] = field(default_factory=list)
    fatal_error: str = ""

    @classmethod
    def from_snapshot_toml(
        cls,
        *,
        snapshot_toml: str,
        snapshot_sha256: str = "",
        setup_id: str = "",
        strict: bool = True,
    ) -> "ActiveSpectralRecordingSetup":
        snapshot = load_snapshot_from_toml_text(snapshot_toml)
        expected_hash = normalize_snapshot_hash(snapshot, snapshot_toml)
        supplied_hash = str(snapshot_sha256 or expected_hash)
        if supplied_hash != expected_hash:
            message = (
                f"snapshot hash mismatch: supplied={supplied_hash}, "
                f"snapshot canonical_snapshot_sha256={expected_hash}"
            )
            if strict:
                raise SpectralRecordingRuntimeError(message)
            warnings = [message]
        else:
            warnings = []

        active_setup_id = str(setup_id or snapshot.get("setup_id") or "")
        if not active_setup_id:
            raise SpectralRecordingRuntimeError("setup_id is required.")

        setup = cls(
            snapshot=dict(snapshot),
            setup_id=active_setup_id,
            setup_hash=expected_hash,
            gate_allow_save=False,
            warnings=warnings,
        )
        validate_setup_chunk_schema(setup)
        return setup

    @property
    def streams(self) -> Dict[str, Dict[str, Any]]:
        return {str(k): dict(v) for k, v in self.snapshot.get("streams", {}).items()}

    @property
    def enabled_stream_ids(self) -> List[str]:
        return list(self.streams.keys())

    @property
    def spectrum_stream_ids(self) -> List[str]:
        return [
            sid
            for sid, stream in self.streams.items()
            if str(stream.get("recording_mode", "spectrum")) == "spectrum"
        ]

    @property
    def tp_stream_ids(self) -> List[str]:
        return [
            sid
            for sid, stream in self.streams.items()
            if str(stream.get("recording_mode", "spectrum")) == "tp"
        ]

    @property
    def active_mode_summary(self) -> str:
        return (
            f"setup_id={self.setup_id}; setup_hash={self.setup_hash}; "
            f"spectrum={len(self.spectrum_stream_ids)}; tp={len(self.tp_stream_ids)}; "
            f"gate={'open' if self.gate_allow_save else 'closed'}"
        )


    def latch_fatal_error(self, message: str) -> None:
        """Record a fatal active-mode error and close the setup gate.

        Active snapshot mode must not silently skip rows. Once a schema/setup
        error is detected, later save attempts remain blocked until the setup is
        explicitly cleared and a new setup is applied.
        """
        text = str(message)
        self.runtime_errors.append(text)
        if not self.fatal_error:
            self.fatal_error = text
        self.gate_allow_save = False

    def assert_no_fatal_error(self) -> None:
        if self.fatal_error:
            raise SpectralRecordingRuntimeError(
                "active spectral recording setup has a latched fatal error: "
                + self.fatal_error
            )

    def set_gate(self, *, setup_id: str, setup_hash: str, allow_save: bool) -> None:
        if str(setup_id) != self.setup_id:
            raise SpectralRecordingGateError(
                f"setup_id mismatch: active={self.setup_id!r}, requested={setup_id!r}"
            )
        if str(setup_hash) != self.setup_hash:
            raise SpectralRecordingGateError(
                f"setup_hash mismatch: active={self.setup_hash!r}, requested={setup_hash!r}"
            )
        if allow_save:
            self.assert_no_fatal_error()
        self.gate_allow_save = bool(allow_save)

    def stream_for_raw(self, spectrometer_key: str, board_id: int) -> Optional[Dict[str, Any]]:
        for stream in self.streams.values():
            if (
                str(stream.get("spectrometer_key")) == str(spectrometer_key)
                and int(stream.get("board_id")) == int(board_id)
            ):
                return dict(stream)
        return None

    def check_save_allowed(self) -> bool:
        return self.gate_allow_save

    def reject_legacy_channel_binning(self) -> None:
        raise SpectralRecordingRuntimeError(
            "legacy channel_binning is disabled while spectral_recording_snapshot is active"
        )

    def reject_legacy_tp_mode(self) -> None:
        if self.legacy_tp_policy == "ignore":
            return
        raise SpectralRecordingRuntimeError(
            "legacy tp_mode is disabled while spectral_recording_snapshot is active"
        )


class SpectralRecordingRuntimeState:
    """Mutable holder used by ``SpectralData``."""

    def __init__(self) -> None:
        self.active_setup: Optional[ActiveSpectralRecordingSetup] = None

    @property
    def active(self) -> bool:
        return self.active_setup is not None

    def apply(self, *, snapshot_toml: str, snapshot_sha256: str, setup_id: str, strict: bool = True) -> ActiveSpectralRecordingSetup:
        if self.active_setup is not None:
            raise SpectralRecordingRuntimeError(
                "A spectral recording setup is already active; clear it before applying another setup."
            )
        self.active_setup = ActiveSpectralRecordingSetup.from_snapshot_toml(
            snapshot_toml=snapshot_toml,
            snapshot_sha256=snapshot_sha256,
            setup_id=setup_id,
            strict=strict,
        )
        return self.active_setup

    def set_gate(self, *, setup_id: str, setup_hash: str, allow_save: bool) -> None:
        if self.active_setup is None:
            raise SpectralRecordingGateError("No active spectral recording setup.")
        self.active_setup.set_gate(
            setup_id=setup_id,
            setup_hash=setup_hash,
            allow_save=allow_save,
        )

    def clear(self, *, setup_id: str = "", setup_hash: str = "", strict: bool = True) -> List[str]:
        warnings: List[str] = []
        setup = self.active_setup
        if setup is None:
            message = "No active spectral recording setup to clear."
            if strict:
                raise SpectralRecordingGateError(message)
            return [message]
        if setup_id and str(setup_id) != setup.setup_id:
            raise SpectralRecordingGateError(
                f"setup_id mismatch: active={setup.setup_id!r}, requested={setup_id!r}"
            )
        if setup_hash and str(setup_hash) != setup.setup_hash:
            raise SpectralRecordingGateError(
                f"setup_hash mismatch: active={setup.setup_hash!r}, requested={setup_hash!r}"
            )
        if setup.gate_allow_save:
            warnings.append("setup gate was open during clear; closing it before deactivation")
            setup.gate_allow_save = False
        self.active_setup = None
        return warnings

    def latch_fatal_error(self, message: str) -> None:
        """Latch a fatal active-mode error without raising from async callbacks.

        ROS subscription callbacks such as legacy TPMode/Binning conflict handlers
        should make the active setup fail-closed, but they should not depend on an
        unhandled exception escaping the callback. Recording paths observe the
        latched state via ``assert_no_fatal_error()`` before any append.
        """
        if self.active_setup is None:
            return
        self.active_setup.latch_fatal_error(str(message))

    def assert_no_fatal_error(self) -> None:
        if self.active_setup is not None:
            self.active_setup.assert_no_fatal_error()

    def stream_for_raw(self, spectrometer_key: str, board_id: int) -> Optional[Dict[str, Any]]:
        if self.active_setup is None:
            return None
        return self.active_setup.stream_for_raw(spectrometer_key, board_id)


def _as_sequence(data: Any) -> Sequence[Any]:
    # Works for lists, tuples, arrays, and numpy arrays without importing numpy.
    if hasattr(data, "__len__") and hasattr(data, "__getitem__"):
        return data
    return list(data)


def slice_spectrum_for_stream(stream: Mapping[str, Any], spectral_data: Any) -> Any:
    """Return full/sliced spectrum for one stream.

    The runtime input is expected to be the full XFFTS spectrum.  The slice is
    applied exactly once using full-channel inclusive/exclusive indices.
    """

    mode = str(stream.get("recording_mode", "spectrum"))
    if mode != "spectrum":
        raise SpectralRecordingRuntimeError(
            f"stream {stream.get('stream_id')} is recording_mode={mode}; PR5 handles only spectrum"
        )

    full_nchan = int(stream.get("full_nchan"))
    start = int(stream.get("saved_ch_start", 0))
    stop = int(stream.get("saved_ch_stop", full_nchan))
    saved_nchan = int(stream.get("saved_nchan", stop - start))

    if start < 0 or stop < start or stop > full_nchan:
        raise SpectralRecordingRuntimeError(
            f"invalid saved channel range for stream {stream.get('stream_id')}: "
            f"start={start}, stop={stop}, full_nchan={full_nchan}"
        )

    data_len = len(spectral_data)
    if data_len == full_nchan:
        sliced = spectral_data[start:stop]
    elif data_len == saved_nchan and start == 0 and stop == saved_nchan:
        # This is effectively a full saved spectrum with no offset.
        sliced = spectral_data
    else:
        raise SpectralRecordingRuntimeError(
            f"input spectrum length mismatch for stream {stream.get('stream_id')}: "
            f"got {data_len}, expected full_nchan={full_nchan}; "
            f"saved range is [{start}:{stop}] with saved_nchan={saved_nchan}"
        )

    if len(sliced) != saved_nchan:
        raise SpectralRecordingRuntimeError(
            f"saved spectrum length mismatch for stream {stream.get('stream_id')}: "
            f"got {len(sliced)}, expected {saved_nchan}"
        )
    return sliced


def spectrum_extra_chunk(stream: Mapping[str, Any], setup: ActiveSpectralRecordingSetup) -> List[Dict[str, Any]]:
    """Extra metadata fields appended to the normal Spectral.msg chunk."""

    fields: List[Tuple[str, str, Any]] = [
        ("setup_id", "string<=64", setup.setup_id),
        ("setup_hash", "string<=64", setup.setup_hash),
        ("stream_id", "string<=64", str(stream.get("stream_id", ""))),
        ("spectrometer_key", "string<=32", str(stream.get("spectrometer_key", ""))),
        ("board_id", "int32", int(stream.get("board_id", -1))),
        ("fdnum", "int32", int(stream.get("fdnum", -1))),
        ("ifnum", "int32", int(stream.get("ifnum", -1))),
        ("plnum", "int32", int(stream.get("plnum", -1))),
        ("polariza", "string<=8", str(stream.get("polariza", ""))),
        ("beam_id", "string<=32", str(stream.get("beam_id", ""))),
        ("full_nchan", "int32", int(stream.get("full_nchan", -1))),
        ("saved_ch_start", "int32", int(stream.get("saved_ch_start", -1))),
        ("saved_ch_stop", "int32", int(stream.get("saved_ch_stop", -1))),
        ("saved_nchan", "int32", int(stream.get("saved_nchan", -1))),
        ("saved_window_policy", "string<=32", str(stream.get("saved_window_policy", ""))),
    ]
    return [runtime_chunk_field(key, typ, value) for key, typ, value in fields]


def namespace_db_path(namespace_root: str, db_table_path: str) -> str:
    """Convert canonical snapshot ``data/...`` path to current NECSTDB append path."""

    path = str(db_table_path)
    if path.startswith("/"):
        return path
    root = namespace_root.rstrip("/")
    if path.startswith("data/"):
        return f"{root}/{path}"
    return f"{root}/data/{path.lstrip('/')}"

def _is_nan(value: Any) -> bool:
    try:
        return math.isnan(float(value))
    except (TypeError, ValueError):
        return False


def _as_float_list(data: Any) -> List[float]:
    return [float(data[i]) for i in range(len(data))]


def _parse_fixed_string_length(type_name: str) -> Optional[int]:
    """Return fixed byte length for a runtime string field type.

    ``NECSTDBWriter`` chooses the actual ``Ns`` struct format from the byte
    length of the value, not from an annotation such as ``string<=64``.
    Therefore runtime helpers must pass bytes already padded to the exact
    schema length whenever fixed-length string columns are required.
    """

    prefix = "string<="
    if not str(type_name).startswith(prefix):
        return None
    try:
        length = int(str(type_name)[len(prefix) :])
    except ValueError as exc:
        raise SpectralRecordingRuntimeError(f"Invalid fixed string type: {type_name!r}") from exc
    if length <= 0:
        raise SpectralRecordingRuntimeError(f"Fixed string length must be positive: {type_name!r}")
    return length


def fixed_string_bytes(value: Any, length: int, *, key: str = "") -> bytes:
    """Encode and NUL-pad a value to exactly ``length`` bytes.

    The check is byte-based, not Python-character-based, because NECSTDB uses
    ``struct`` byte string formats.  Oversized values are rejected rather than
    silently truncated; truncating ``tp_windows_json`` would make the JSON
    invalid and truncating identifiers would make provenance ambiguous.
    """

    raw = value if isinstance(value, bytes) else str(value).encode("utf-8")
    if len(raw) > length:
        label = f" for {key}" if key else ""
        raise SpectralRecordingRuntimeError(
            f"Fixed string value{label} is too long: {len(raw)} bytes > {length} bytes"
        )
    return raw + (b"\x00" * (length - len(raw)))


def runtime_chunk_field(key: str, type_name: str, value: Any) -> Dict[str, Any]:
    """Build one NECSTDB chunk field with fixed-string schema hardening."""

    fixed_len = _parse_fixed_string_length(type_name)
    if fixed_len is not None:
        return {"key": key, "type": "string", "value": fixed_string_bytes(value, fixed_len, key=key)}
    return {"key": key, "type": type_name, "value": value}


def decode_fixed_string_value(value: Any) -> str:
    """Testing/debug helper: decode a fixed string value emitted by this module."""

    raw = value if isinstance(value, bytes) else bytes(value)
    return raw.split(b"\x00", 1)[0].decode("utf-8")


def tp_window_for_stream(stream: Mapping[str, Any]) -> Tuple[int, int]:
    """Return the PR6 contiguous TP window in full-channel indices."""

    mode = str(stream.get("recording_mode", "spectrum"))
    if mode != "tp":
        raise SpectralRecordingRuntimeError(
            f"stream {stream.get('stream_id')} is recording_mode={mode}; "
            "TP branch requires recording_mode='tp'"
        )
    full_nchan = int(stream.get("full_nchan"))
    start = int(stream.get("saved_ch_start", 0))
    stop = int(stream.get("saved_ch_stop", full_nchan))
    if start < 0 or stop < start or stop > full_nchan:
        raise SpectralRecordingRuntimeError(
            f"invalid TP channel range for stream {stream.get('stream_id')}: "
            f"start={start}, stop={stop}, full_nchan={full_nchan}"
        )
    return start, stop


def compute_tp_statistics_for_stream(stream: Mapping[str, Any], spectral_data: Any) -> Dict[str, Any]:
    """Compute TP sum/mean with explicit all-NaN/zero-valid semantics."""

    full_nchan = int(stream.get("full_nchan"))
    data_len = len(spectral_data)
    if data_len != full_nchan:
        raise SpectralRecordingRuntimeError(
            f"input spectrum length mismatch for TP stream {stream.get('stream_id')}: "
            f"got {data_len}, expected full_nchan={full_nchan}"
        )
    start, stop = tp_window_for_stream(stream)
    window = _as_float_list(spectral_data[start:stop])
    finite = [value for value in window if not _is_nan(value)]
    if not finite:
        return {
            "tp_sum": float("nan"),
            "tp_mean": float("nan"),
            "tp_nchan_used": 0,
            "tp_valid": False,
        }
    total = float(sum(finite))
    return {
        "tp_sum": total,
        "tp_mean": total / float(len(finite)),
        "tp_nchan_used": int(len(finite)),
        "tp_valid": True,
    }


def _json_string_for_tp_windows(stream: Mapping[str, Any]) -> str:
    text = str(stream.get("computed_windows_json", "") or "")
    if text:
        return text
    start, stop = tp_window_for_stream(stream)
    return json.dumps(
        [{"kind": "channel", "computed_ch_start": start, "computed_ch_stop": stop}],
        sort_keys=True,
        separators=(",", ":"),
    )


def tp_chunk_for_stream(
    stream: Mapping[str, Any],
    setup: ActiveSpectralRecordingSetup,
    *,
    time: float,
    time_spectrometer: str,
    position: str,
    obs_id: str,
    line_index: int,
    line_label: str,
    spectral_data: Any,
) -> List[Dict[str, Any]]:
    """Build the fixed-schema TP chunk for direct NECSTDB append."""
    stats = compute_tp_statistics_for_stream(stream, spectral_data)
    fields: List[Tuple[str, str, Any]] = [
        ("time", "float64", float(time)),
        ("time_spectrometer", "string<=32", str(time_spectrometer)),
        ("position", "string<=8", str(position)),
        ("id", "string<=16", str(obs_id)),
        ("line_index", "int32", int(line_index)),
        ("line_label", "string<=64", str(line_label)),
        ("setup_id", "string<=64", setup.setup_id),
        ("setup_hash", "string<=64", setup.setup_hash),
        ("stream_id", "string<=64", str(stream.get("stream_id", ""))),
        ("spectrometer_key", "string<=32", str(stream.get("spectrometer_key", ""))),
        ("board_id", "int32", int(stream.get("board_id", -1))),
        ("fdnum", "int32", int(stream.get("fdnum", -1))),
        ("ifnum", "int32", int(stream.get("ifnum", -1))),
        ("plnum", "int32", int(stream.get("plnum", -1))),
        ("polariza", "string<=8", str(stream.get("polariza", ""))),
        ("beam_id", "string<=32", str(stream.get("beam_id", ""))),
        ("tp_sum", "float64", float(stats["tp_sum"])),
        ("tp_mean", "float64", float(stats["tp_mean"])),
        ("tp_nchan_used", "int32", int(stats["tp_nchan_used"])),
        ("tp_valid", "bool", bool(stats["tp_valid"])),
        ("tp_windows_json", "string<=2048", _json_string_for_tp_windows(stream)),
        ("tp_stat", "string<=16", str(stream.get("tp_stat", "sum_mean"))),
    ]
    return [runtime_chunk_field(key, typ, value) for key, typ, value in fields]


def validate_setup_chunk_schema(setup: ActiveSpectralRecordingSetup) -> None:
    """Dry-run fixed-schema metadata chunks before a setup gate can be opened.

    This catches snapshot-derived fixed-string overflows at setup-apply time
    instead of during the first TP/spectrum row. Dynamic observation metadata
    fields that are truncated by ``SpectralData`` are represented by legal
    maximum-length dummy values.
    """

    for stream_id, stream in setup.streams.items():
        if is_tp_stream(stream):
            fields: List[Tuple[str, str, Any]] = [
                ("time_spectrometer", "string<=32", "0" * 32),
                ("position", "string<=8", "ON"),
                ("id", "string<=16", "X" * 16),
                ("line_label", "string<=64", "L" * 64),
                ("setup_id", "string<=64", setup.setup_id),
                ("setup_hash", "string<=64", setup.setup_hash),
                ("stream_id", "string<=64", str(stream.get("stream_id", ""))),
                ("spectrometer_key", "string<=32", str(stream.get("spectrometer_key", ""))),
                ("polariza", "string<=8", str(stream.get("polariza", ""))),
                ("beam_id", "string<=32", str(stream.get("beam_id", ""))),
                ("tp_windows_json", "string<=2048", _json_string_for_tp_windows(stream)),
                ("tp_stat", "string<=16", str(stream.get("tp_stat", "sum_mean"))),
            ]
            for key, typ, value in fields:
                runtime_chunk_field(key, typ, value)
        else:
            spectrum_extra_chunk(stream, setup)


def is_tp_stream(stream: Mapping[str, Any]) -> bool:
    return str(stream.get("recording_mode", "spectrum")) == "tp"


def response_lists(setup: ActiveSpectralRecordingSetup) -> Dict[str, List[str]]:
    return {
        "enabled_streams": setup.enabled_stream_ids,
        "disabled_streams": [],
        "spectrum_streams": setup.spectrum_stream_ids,
        "tp_streams": setup.tp_stream_ids,
    }
