"""Parser/resolver for NECST/XFFTS spectral recording setup snapshots.

This module is intentionally self-contained and depends only on the Python
standard library plus an optional TOML reader (``tomllib``, ``tomli`` or
``toml``).  It implements the PR1 foundation for the spectral recording
redesign:

* parse human-edited ``lo_profile.toml``, ``recording_window_setup.toml`` and
  ``beam_model.toml``;
* expand strict two-level ``stream_groups`` into a normalized stream table;
* resolve per-stream recording mode and channel ranges;
* validate primary keys, references, and DB table paths;
* emit a deterministic ``spectral_recording_snapshot.toml``.

Runtime recording, ROS services, and converter/sunscan adapters are deliberately
left to later PRs.  The classes and functions here are written so they can be
unit-tested without ROS imports.
"""

from __future__ import annotations

import argparse
import copy
import datetime as _datetime
import hashlib
import json
import math
import os
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, MutableMapping, Optional, Sequence, Tuple

try:  # Python >= 3.11
    import tomllib as _toml_reader  # type: ignore[attr-defined]
except ModuleNotFoundError:  # pragma: no cover - depends on runtime Python
    try:
        import tomli as _toml_reader  # type: ignore[no-redef]
    except ModuleNotFoundError:  # pragma: no cover - optional fallback
        try:
            import toml as _toml_legacy  # type: ignore[no-redef]
        except ModuleNotFoundError as _toml_import_error:  # pragma: no cover
            _toml_reader = None  # type: ignore[assignment]
            _TOML_IMPORT_ERROR = _toml_import_error
        else:  # pragma: no cover - only used when tomllib/tomli unavailable
            _toml_reader = _toml_legacy  # type: ignore[assignment]
            _TOML_IMPORT_ERROR = None
    else:
        _TOML_IMPORT_ERROR = None
else:
    _TOML_IMPORT_ERROR = None


C_M_PER_S = 299_792_458.0
C_KM_PER_S = C_M_PER_S / 1000.0
SCHEMA_VERSION = "spectral_recording_snapshot_v1"
SNAPSHOT_VERSION = "v26-aligned-2026-04-26-pr1"

SETUP_OVERRIDE_POLICIES = {"strict", "warn", "force", "legacy"}
SAVED_WINDOW_POLICIES = {"full", "contiguous_envelope", "channel"}
RECORDING_MODES = {"spectrum", "tp"}
VELOCITY_RESOLVER_VERSION = "topocentric_rest_velocity_approx_v1"
VELOCITY_RESOLVER_FRAME = "TOPOCENTRIC_REST_APPROX"
CHANNEL_ORDERS_INCREASING = {"increasing_if", "increasing_if_frequency", "increasing"}
CHANNEL_ORDERS_DECREASING = {"decreasing_if", "decreasing_if_frequency", "decreasing"}

GROUP_REQUIRED_KEYS = {
    "spectrometer_key",
    "frontend",
    "backend",
    "lo_chain",
    "polariza",
    "frequency_axis_id",
}
GROUP_OPTIONAL_KEYS = {
    "default_rest_frequency_hz",
    "use_for_convert",
    "use_for_sunscan",
    "use_for_fit",
    "recording_default_policy",
    "human_label",
}
GROUP_ALLOWED_KEYS = GROUP_REQUIRED_KEYS | GROUP_OPTIONAL_KEYS | {"streams"}
ITEM_ALLOWED_KEYS = {
    "stream_id",
    "board_id",
    "fdnum",
    "ifnum",
    "plnum",
    "beam_id",
    "db_table_path",
    "db_stream_name",
    "db_table_name",  # accepted only as legacy-compatible input alias
    "sampler",
    "human_label",
}
ITEM_REQUIRED_KEYS = {"stream_id", "board_id", "fdnum", "ifnum", "plnum", "beam_id"}


class SpectralRecordingSetupError(ValueError):
    """Raised when a setup file is structurally invalid."""


class SpectralRecordingValidationError(SpectralRecordingSetupError):
    """Raised when references or resolved values fail validation."""


def _toml_loads(text: str) -> Dict[str, Any]:
    if _toml_reader is None:  # pragma: no cover
        raise SpectralRecordingSetupError(
            "No TOML reader is available. Use Python >= 3.11, or install tomli/toml."
        ) from _TOML_IMPORT_ERROR
    if hasattr(_toml_reader, "loads"):
        return _toml_reader.loads(text)  # type: ignore[no-any-return]
    raise SpectralRecordingSetupError("Configured TOML reader has no loads() function")


def read_toml(path: os.PathLike[str] | str) -> Dict[str, Any]:
    p = Path(path)
    try:
        raw = p.read_bytes()
    except OSError as exc:
        raise SpectralRecordingSetupError(f"Cannot read TOML file: {p}") from exc
    try:
        return _toml_loads(raw.decode("utf-8"))
    except Exception as exc:
        if isinstance(exc, SpectralRecordingSetupError):
            raise
        raise SpectralRecordingSetupError(f"Cannot parse TOML file: {p}: {exc}") from exc


def sha256_file(path: os.PathLike[str] | str) -> str:
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _as_bool(value: Any, *, field: str) -> bool:
    if isinstance(value, bool):
        return value
    raise SpectralRecordingValidationError(f"{field} must be bool, got {type(value).__name__}")


def _as_int(value: Any, *, field: str) -> int:
    if isinstance(value, bool):
        raise SpectralRecordingValidationError(f"{field} must be int, got bool")
    try:
        return int(value)
    except Exception as exc:
        raise SpectralRecordingValidationError(f"{field} must be int, got {value!r}") from exc


def _as_float(value: Any, *, field: str) -> float:
    if isinstance(value, bool):
        raise SpectralRecordingValidationError(f"{field} must be float, got bool")
    try:
        return float(value)
    except Exception as exc:
        raise SpectralRecordingValidationError(f"{field} must be float, got {value!r}") from exc


def _as_str(value: Any, *, field: str) -> str:
    if isinstance(value, str):
        return value
    raise SpectralRecordingValidationError(f"{field} must be string, got {type(value).__name__}")


def _as_str_list(value: Any, *, field: str) -> List[str]:
    if not isinstance(value, list):
        raise SpectralRecordingValidationError(f"{field} must be a list of strings")
    out: List[str] = []
    for i, item in enumerate(value):
        if not isinstance(item, str):
            raise SpectralRecordingValidationError(f"{field}[{i}] must be string")
        out.append(item)
    return out


def _canonical_table_name(s: str) -> str:
    return "".join(ch if (ch.isalnum() or ch in "_.-/") else "_" for ch in str(s))


def _default_db_table_path(spectrometer_key: str, board_id: int, *, kind: str = "spectral") -> str:
    if kind == "tp":
        return f"data/tp/{spectrometer_key}/board{board_id}"
    return f"data/spectral/{spectrometer_key}/board{board_id}"


def _deepcopy_dict(value: Mapping[str, Any]) -> Dict[str, Any]:
    return copy.deepcopy(dict(value))


def _sorted_dict(value: Mapping[str, Any]) -> Dict[str, Any]:
    return {k: value[k] for k in sorted(value)}


def _format_float(value: float) -> str:
    if math.isnan(value):
        return "nan"
    if math.isinf(value):
        return "inf" if value > 0 else "-inf"
    return format(float(value), ".17g")


def _toml_scalar(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int) and not isinstance(value, bool):
        return str(value)
    if isinstance(value, float):
        return _format_float(value)
    if isinstance(value, str):
        return json.dumps(value, ensure_ascii=False)
    if value is None:
        return json.dumps("")
    if isinstance(value, list):
        return "[" + ", ".join(_toml_scalar(v) for v in value) + "]"
    raise TypeError(f"Unsupported TOML scalar value: {value!r}")


def dumps_toml(data: Mapping[str, Any]) -> str:
    """Write a deterministic, minimal TOML representation.

    The writer supports scalars, lists of scalars, nested tables, and arrays of
    tables.  It is deliberately small so that canonical hashing is independent
    of third-party TOML writer formatting.
    """

    lines: List[str] = []

    def emit_table(path: List[str], table: Mapping[str, Any]) -> None:
        scalar_keys: List[str] = []
        table_keys: List[str] = []
        array_table_keys: List[str] = []
        for key in sorted(table):
            value = table[key]
            if isinstance(value, Mapping):
                table_keys.append(key)
            elif isinstance(value, list) and all(isinstance(x, Mapping) for x in value):
                array_table_keys.append(key)
            else:
                scalar_keys.append(key)
        if path:
            if lines and lines[-1] != "":
                lines.append("")
            lines.append("[" + ".".join(path) + "]")
        for key in scalar_keys:
            lines.append(f"{key} = {_toml_scalar(table[key])}")
        for key in table_keys:
            emit_table(path + [key], table[key])  # type: ignore[arg-type]
        for key in array_table_keys:
            for item in table[key]:
                if lines and lines[-1] != "":
                    lines.append("")
                lines.append("[[" + ".".join(path + [key]) + "]]")
                emit_array_item(path + [key], item)

    def emit_array_item(path: List[str], table: Mapping[str, Any]) -> None:
        scalar_keys = []
        table_keys = []
        for key in sorted(table):
            value = table[key]
            if isinstance(value, Mapping):
                table_keys.append(key)
            else:
                scalar_keys.append(key)
        for key in scalar_keys:
            lines.append(f"{key} = {_toml_scalar(table[key])}")
        for key in table_keys:
            emit_table(path + [key], table[key])  # type: ignore[arg-type]

    emit_table([], data)
    return "\n".join(lines).rstrip() + "\n"


def _ensure_mapping(value: Any, *, field: str) -> Dict[str, Any]:
    if not isinstance(value, Mapping):
        raise SpectralRecordingValidationError(f"{field} must be a table")
    return dict(value)


def _resolve_lo_roles(lo_profile: Mapping[str, Any]) -> Dict[str, Dict[str, Any]]:
    sg_devices = _ensure_mapping(lo_profile.get("sg_devices", {}), field="sg_devices")
    lo_roles = _ensure_mapping(lo_profile.get("lo_roles", {}), field="lo_roles")
    if not lo_roles:
        raise SpectralRecordingValidationError("lo_roles must not be empty")

    resolved: Dict[str, Dict[str, Any]] = {}
    for role_id, raw_role in lo_roles.items():
        role = _ensure_mapping(raw_role, field=f"lo_roles.{role_id}")
        source = str(role.get("source", "sg_device"))
        entry: Dict[str, Any] = {"lo_role_id": str(role_id), "source": source}
        if source == "sg_device":
            sg_id = _as_str(role.get("sg_id"), field=f"lo_roles.{role_id}.sg_id")
            if sg_id not in sg_devices:
                raise SpectralRecordingValidationError(
                    f"lo_roles.{role_id}.sg_id={sg_id!r} is not defined in sg_devices"
                )
            sg = _ensure_mapping(sg_devices[sg_id], field=f"sg_devices.{sg_id}")
            sg_set_frequency_hz = _as_float(
                sg.get("sg_set_frequency_hz"), field=f"sg_devices.{sg_id}.sg_set_frequency_hz"
            )
            multiplier = _as_float(role.get("multiplier", 1.0), field=f"lo_roles.{role_id}.multiplier")
            physical_lo_frequency_hz = sg_set_frequency_hz * multiplier
            entry.update(
                {
                    "sg_id": sg_id,
                    "multiplier": multiplier,
                    "sg_set_frequency_hz": sg_set_frequency_hz,
                    "physical_lo_frequency_hz": physical_lo_frequency_hz,
                }
            )
            if "expected_lo_frequency_hz" in role:
                expected = _as_float(
                    role["expected_lo_frequency_hz"],
                    field=f"lo_roles.{role_id}.expected_lo_frequency_hz",
                )
                entry["expected_lo_frequency_hz"] = expected
                tol = _as_float(
                    role.get("expected_lo_tolerance_hz", sg.get("frequency_tolerance_hz", 10.0)),
                    field=f"lo_roles.{role_id}.expected_lo_tolerance_hz",
                )
                if abs(physical_lo_frequency_hz - expected) > tol:
                    raise SpectralRecordingValidationError(
                        f"lo_roles.{role_id}: computed physical LO {physical_lo_frequency_hz} Hz "
                        f"does not match expected {expected} Hz within {tol} Hz"
                    )
        elif source == "fixed":
            fixed = _as_float(
                role.get("fixed_lo_frequency_hz"), field=f"lo_roles.{role_id}.fixed_lo_frequency_hz"
            )
            entry["physical_lo_frequency_hz"] = fixed
        else:
            raise SpectralRecordingValidationError(
                f"lo_roles.{role_id}.source must be 'sg_device' or 'fixed', got {source!r}"
            )
        resolved[str(role_id)] = entry
    return resolved


def _chain_roles_and_signs(chain_id: str, chain: Mapping[str, Any]) -> Tuple[List[str], List[int]]:
    if "lo_roles" in chain:
        roles = _as_str_list(chain["lo_roles"], field=f"lo_chains.{chain_id}.lo_roles")
    else:
        roles = []
        for key in ("lo1_role", "lo2_role", "lo3_role", "lo4_role"):
            if key in chain:
                roles.append(_as_str(chain[key], field=f"lo_chains.{chain_id}.{key}"))
    if not roles:
        raise SpectralRecordingValidationError(
            f"lo_chains.{chain_id} must define lo_roles or loN_role keys"
        )

    if "sideband_signs" in chain:
        raw_signs = chain["sideband_signs"]
        sign_field = "sideband_signs"
    elif "signs" in chain:
        raw_signs = chain["signs"]
        sign_field = "signs"
    else:
        raw_signs = [1 for _ in roles]
        sign_field = "signs"
    if not isinstance(raw_signs, list):
        raise SpectralRecordingValidationError(f"lo_chains.{chain_id}.{sign_field} must be list")
    signs = []
    for i, sign in enumerate(raw_signs):
        s = _as_int(sign, field=f"lo_chains.{chain_id}.{sign_field}[{i}]")
        if s not in (-1, 1):
            raise SpectralRecordingValidationError(
                f"lo_chains.{chain_id}.{sign_field}[{i}] must be +1 or -1"
            )
        signs.append(s)
    if len(signs) != len(roles):
        raise SpectralRecordingValidationError(
            f"lo_chains.{chain_id}: number of roles ({len(roles)}) and signs ({len(signs)}) differs"
        )
    return roles, signs


def _resolve_lo_chains(lo_profile: Mapping[str, Any], resolved_roles: Mapping[str, Mapping[str, Any]]) -> Dict[str, Dict[str, Any]]:
    lo_chains = _ensure_mapping(lo_profile.get("lo_chains", {}), field="lo_chains")
    if not lo_chains:
        raise SpectralRecordingValidationError("lo_chains must not be empty")
    resolved: Dict[str, Dict[str, Any]] = {}
    for chain_id, raw_chain in lo_chains.items():
        chain = _ensure_mapping(raw_chain, field=f"lo_chains.{chain_id}")
        roles, signs = _chain_roles_and_signs(str(chain_id), chain)
        missing = [role for role in roles if role not in resolved_roles]
        if missing:
            raise SpectralRecordingValidationError(
                f"lo_chains.{chain_id} references unknown lo_roles: {missing}"
            )
        physical = [float(resolved_roles[role]["physical_lo_frequency_hz"]) for role in roles]
        signed_sum = sum(sign * freq for sign, freq in zip(signs, physical))
        resolved[str(chain_id)] = {
            "lo_chain_id": str(chain_id),
            "formula_version": str(chain.get("formula_version", "signed_lo_sum_plus_if_v1")),
            "lo_roles": roles,
            "signs": signs,
            "physical_lo_frequencies_hz": physical,
            "signed_lo_sum_hz": signed_sum,
        }
    return resolved


def _normalize_frequency_axes(lo_profile: Mapping[str, Any]) -> Dict[str, Dict[str, Any]]:
    axes = _ensure_mapping(lo_profile.get("frequency_axes", {}), field="frequency_axes")
    if not axes:
        raise SpectralRecordingValidationError("frequency_axes must not be empty")
    out: Dict[str, Dict[str, Any]] = {}
    for axis_id, raw_axis in axes.items():
        axis = _ensure_mapping(raw_axis, field=f"frequency_axes.{axis_id}")
        full_nchan = _as_int(axis.get("full_nchan"), field=f"frequency_axes.{axis_id}.full_nchan")
        if full_nchan <= 0:
            raise SpectralRecordingValidationError(f"frequency_axes.{axis_id}.full_nchan must be positive")
        if0 = _as_float(
            axis.get("if_freq_at_full_ch0_hz"), field=f"frequency_axes.{axis_id}.if_freq_at_full_ch0_hz"
        )
        step = _as_float(axis.get("if_freq_step_hz"), field=f"frequency_axes.{axis_id}.if_freq_step_hz")
        if step == 0.0:
            raise SpectralRecordingValidationError(f"frequency_axes.{axis_id}.if_freq_step_hz must be non-zero")
        order = str(axis.get("channel_order", "increasing_if"))
        if order not in CHANNEL_ORDERS_INCREASING and order not in CHANNEL_ORDERS_DECREASING:
            raise SpectralRecordingValidationError(
                f"frequency_axes.{axis_id}.channel_order={order!r} is not recognized"
            )
        if order in CHANNEL_ORDERS_INCREASING and step < 0:
            raise SpectralRecordingValidationError(
                f"frequency_axes.{axis_id}.channel_order={order!r} requires if_freq_step_hz > 0, got {step}"
            )
        if order in CHANNEL_ORDERS_DECREASING and step > 0:
            raise SpectralRecordingValidationError(
                f"frequency_axes.{axis_id}.channel_order={order!r} requires if_freq_step_hz < 0, got {step}"
            )
        out[str(axis_id)] = {
            "frequency_axis_id": str(axis_id),
            "full_nchan": full_nchan,
            "if_freq_at_full_ch0_hz": if0,
            "if_freq_step_hz": step,
            "channel_order": order,
        }
        for optional in ("bandwidth_hz", "sky_freq_at_full_ch0_hz", "sky_freq_step_hz"):
            if optional in axis:
                out[str(axis_id)][optional] = _as_float(axis[optional], field=f"frequency_axes.{axis_id}.{optional}")
    return out


def _expand_stream_groups(
    lo_profile: Mapping[str, Any],
    *,
    frequency_axes: Mapping[str, Mapping[str, Any]],
    lo_chains: Mapping[str, Mapping[str, Any]],
) -> Dict[str, Dict[str, Any]]:
    groups = _ensure_mapping(lo_profile.get("stream_groups", {}), field="stream_groups")
    if not groups:
        raise SpectralRecordingValidationError("stream_groups must not be empty")
    streams: Dict[str, Dict[str, Any]] = {}
    for group_id, raw_group in groups.items():
        group = _ensure_mapping(raw_group, field=f"stream_groups.{group_id}")
        unknown_group_keys = set(group) - GROUP_ALLOWED_KEYS
        if unknown_group_keys:
            raise SpectralRecordingValidationError(
                f"stream_groups.{group_id} has unknown/disallowed group keys: {sorted(unknown_group_keys)}"
            )
        missing_group_keys = GROUP_REQUIRED_KEYS - set(group)
        if missing_group_keys:
            raise SpectralRecordingValidationError(
                f"stream_groups.{group_id} is missing required group keys: {sorted(missing_group_keys)}"
            )
        lo_chain = _as_str(group["lo_chain"], field=f"stream_groups.{group_id}.lo_chain")
        if lo_chain not in lo_chains:
            raise SpectralRecordingValidationError(
                f"stream_groups.{group_id}.lo_chain={lo_chain!r} is not defined"
            )
        axis_id = _as_str(group["frequency_axis_id"], field=f"stream_groups.{group_id}.frequency_axis_id")
        if axis_id not in frequency_axes:
            raise SpectralRecordingValidationError(
                f"stream_groups.{group_id}.frequency_axis_id={axis_id!r} is not defined"
            )
        raw_streams = group.get("streams")
        if not isinstance(raw_streams, list) or not raw_streams:
            raise SpectralRecordingValidationError(f"stream_groups.{group_id}.streams must be a non-empty array")
        group_values = {k: group[k] for k in sorted(group) if k != "streams"}
        for i, raw_item in enumerate(raw_streams):
            item = _ensure_mapping(raw_item, field=f"stream_groups.{group_id}.streams[{i}]")
            unknown_item_keys = set(item) - ITEM_ALLOWED_KEYS
            if unknown_item_keys:
                raise SpectralRecordingValidationError(
                    f"stream_groups.{group_id}.streams[{i}] has disallowed item keys: {sorted(unknown_item_keys)}"
                )
            missing_item_keys = ITEM_REQUIRED_KEYS - set(item)
            if missing_item_keys:
                raise SpectralRecordingValidationError(
                    f"stream_groups.{group_id}.streams[{i}] is missing required item keys: {sorted(missing_item_keys)}"
                )
            stream_id = _as_str(item["stream_id"], field=f"stream_groups.{group_id}.streams[{i}].stream_id")
            if stream_id in streams:
                raise SpectralRecordingValidationError(f"Duplicate stream_id: {stream_id}")
            entry = _deepcopy_dict(group_values)
            entry.update(_deepcopy_dict(item))
            entry["stream_id"] = stream_id
            entry["stream_group_id"] = str(group_id)
            entry["board_id"] = _as_int(entry["board_id"], field=f"streams.{stream_id}.board_id")
            entry["fdnum"] = _as_int(entry["fdnum"], field=f"streams.{stream_id}.fdnum")
            entry["ifnum"] = _as_int(entry["ifnum"], field=f"streams.{stream_id}.ifnum")
            entry["plnum"] = _as_int(entry["plnum"], field=f"streams.{stream_id}.plnum")
            for flag in ("use_for_convert", "use_for_sunscan", "use_for_fit"):
                if flag in entry:
                    entry[flag] = _as_bool(entry[flag], field=f"streams.{stream_id}.{flag}")
                else:
                    entry[flag] = True
            if "db_table_name" in entry and "db_stream_name" not in entry:
                # db_table_name is accepted only as a legacy-compatible alias.
                entry["db_stream_name"] = str(entry.pop("db_table_name"))
            entry.setdefault("db_stream_name", stream_id)
            entry.setdefault("raw_input_key", str(entry["spectrometer_key"]))
            entry.setdefault("raw_board_id", int(entry["board_id"]))
            streams[stream_id] = entry
    return streams


def _validate_beam_ids(streams: Mapping[str, Mapping[str, Any]], beam_model: Mapping[str, Any]) -> Dict[str, Dict[str, Any]]:
    beams = _ensure_mapping(beam_model.get("beams", {}), field="beam_model.beams") if beam_model else {}
    if not beams:
        raise SpectralRecordingValidationError("beam_model.toml must define [beams.<beam_id>] tables")
    out: Dict[str, Dict[str, Any]] = {}
    for beam_id, raw_beam in beams.items():
        beam = _ensure_mapping(raw_beam, field=f"beam_model.beams.{beam_id}")
        out[str(beam_id)] = _deepcopy_dict(beam)
        out[str(beam_id)].setdefault("beam_id", str(beam_id))
    missing = sorted({str(s["beam_id"]) for s in streams.values()} - set(out))
    if missing:
        raise SpectralRecordingValidationError(f"beam_id(s) referenced by streams are missing in beam_model: {missing}")
    return out


def _recording_groups(recording_setup: Optional[Mapping[str, Any]]) -> Dict[str, Dict[str, Any]]:
    if recording_setup is None:
        return {}
    return _ensure_mapping(recording_setup.get("recording_groups", {}), field="recording_groups")


def _frequency_array_for_stream(
    stream: Mapping[str, Any],
    *,
    frequency_axes: Mapping[str, Mapping[str, Any]],
    lo_chains: Mapping[str, Mapping[str, Any]],
) -> List[float]:
    axis = frequency_axes[str(stream["frequency_axis_id"])]
    full_nchan = int(axis["full_nchan"])
    if "sky_freq_at_full_ch0_hz" in axis and "sky_freq_step_hz" in axis:
        start = float(axis["sky_freq_at_full_ch0_hz"])
        step = float(axis["sky_freq_step_hz"])
    else:
        # Minimal deterministic fallback.  This is not a replacement for the full
        # instrument-specific converter; it gives the resolver a well-defined
        # monotonic axis when a profile supplies only IF axis plus signed LO chain.
        chain = lo_chains[str(stream["lo_chain"])]
        start = float(chain["signed_lo_sum_hz"]) + float(axis["if_freq_at_full_ch0_hz"])
        step = float(axis["if_freq_step_hz"])
    return [start + i * step for i in range(full_nchan)]


def _radio_velocity_to_frequency(rest_frequency_hz: float, velocity_kms: float) -> float:
    return rest_frequency_hz * (1.0 - velocity_kms / C_KM_PER_S)


def _optical_velocity_to_frequency(rest_frequency_hz: float, velocity_kms: float) -> float:
    return rest_frequency_hz / (1.0 + velocity_kms / C_KM_PER_S)


def _relativistic_velocity_to_frequency(rest_frequency_hz: float, velocity_kms: float) -> float:
    beta = velocity_kms / C_KM_PER_S
    if abs(beta) >= 1.0:
        raise SpectralRecordingValidationError("relativistic velocity must satisfy |v| < c")
    return rest_frequency_hz * math.sqrt((1.0 - beta) / (1.0 + beta))


def velocity_to_frequency_hz(rest_frequency_hz: float, velocity_kms: float, definition: str) -> float:
    definition = definition.lower()
    if definition == "radio":
        return _radio_velocity_to_frequency(rest_frequency_hz, velocity_kms)
    if definition == "optical":
        return _optical_velocity_to_frequency(rest_frequency_hz, velocity_kms)
    if definition == "relativistic":
        return _relativistic_velocity_to_frequency(rest_frequency_hz, velocity_kms)
    raise SpectralRecordingValidationError(
        f"velocity_definition must be radio, optical, or relativistic; got {definition!r}"
    )


def resolve_velocity_window_to_channels(
    *,
    rest_frequency_hz: float,
    velocity_definition: str,
    vmin_kms: float,
    vmax_kms: float,
    margin_kms: float,
    frequency_hz: Sequence[float],
    clip_policy: str = "strict",
) -> Dict[str, Any]:
    """Resolve a velocity window to an inclusive/exclusive full-channel range.

    Direction definitions:
    - full channel index is zero-based;
    - output ``computed_ch_start`` is inclusive;
    - output ``computed_ch_stop`` is exclusive;
    - any channel whose center falls within the effective frequency interval is
      included.  The result is always sorted by full channel index, independent
      of increasing/decreasing frequency order.
    """

    if vmax_kms < vmin_kms:
        raise SpectralRecordingValidationError("vmax_kms must be >= vmin_kms")
    if margin_kms < 0:
        raise SpectralRecordingValidationError("margin_kms must be >= 0")
    effective_vmin = vmin_kms - margin_kms
    effective_vmax = vmax_kms + margin_kms
    f1 = velocity_to_frequency_hz(rest_frequency_hz, effective_vmin, velocity_definition)
    f2 = velocity_to_frequency_hz(rest_frequency_hz, effective_vmax, velocity_definition)
    f_low = min(f1, f2)
    f_high = max(f1, f2)
    indices = [i for i, freq in enumerate(frequency_hz) if f_low <= float(freq) <= f_high]
    warnings: List[str] = []
    clipped = False
    if not indices:
        axis_low = min(float(x) for x in frequency_hz)
        axis_high = max(float(x) for x in frequency_hz)
        outside = f_high < axis_low or f_low > axis_high
        if clip_policy == "strict" or outside:
            raise SpectralRecordingValidationError(
                "velocity window does not overlap the available frequency axis "
                f"({f_low}..{f_high} Hz vs axis {axis_low}..{axis_high} Hz)"
            )
        raise SpectralRecordingValidationError("velocity window could not be resolved to channels")
    ch_start = min(indices)
    ch_stop = max(indices) + 1
    if indices[0] == 0 or indices[-1] == len(frequency_hz) - 1:
        # This may simply be a legitimate edge channel.  Mark it as clipped only
        # when the requested interval extends beyond the axis.
        axis_low = min(float(x) for x in frequency_hz)
        axis_high = max(float(x) for x in frequency_hz)
        if f_low < axis_low or f_high > axis_high:
            if clip_policy == "strict":
                raise SpectralRecordingValidationError(
                    "velocity window extends beyond the available frequency axis"
                )
            clipped = True
            warnings.append("velocity window clipped to available frequency axis")
    return {
        "requested_vmin_kms": float(vmin_kms),
        "requested_vmax_kms": float(vmax_kms),
        "effective_vmin_kms": float(effective_vmin),
        "effective_vmax_kms": float(effective_vmax),
        "computed_ch_start": int(ch_start),
        "computed_ch_stop": int(ch_stop),
        "clipped_to_axis": bool(clipped),
        "resolver_warnings": warnings,
        "frequency_low_hz": float(f_low),
        "frequency_high_hz": float(f_high),
    }


def _resolve_recording_for_stream(
    stream: Mapping[str, Any],
    recording_group: Optional[Mapping[str, Any]],
    *,
    frequency_axes: Mapping[str, Mapping[str, Any]],
    lo_chains: Mapping[str, Mapping[str, Any]],
    setup_override_policy: str,
) -> Dict[str, Any]:
    axis = frequency_axes[str(stream["frequency_axis_id"])]
    full_nchan = int(axis["full_nchan"])
    if recording_group is None:
        mode = "spectrum"
        policy = "full"
        group_id = "__default_full_spectrum__"
        rec: Dict[str, Any] = {}
    else:
        rec = dict(recording_group)
        mode = str(rec.get("mode", "spectrum"))
        policy = str(rec.get("saved_window_policy", "full"))
        group_id = str(rec.get("recording_group_id", ""))
    if mode not in RECORDING_MODES:
        raise SpectralRecordingValidationError(f"recording mode must be spectrum or tp, got {mode!r}")
    if policy not in SAVED_WINDOW_POLICIES:
        raise SpectralRecordingValidationError(
            f"saved_window_policy must be one of {sorted(SAVED_WINDOW_POLICIES)}, got {policy!r}"
        )

    out: Dict[str, Any] = {
        "recording_group_id": group_id,
        "recording_mode": mode,
        "saved_window_policy": policy,
        "full_nchan": full_nchan,
        "input_data_nchan": full_nchan,
    }
    if mode == "tp":
        out["recording_table_kind"] = "tp"
    else:
        out["recording_table_kind"] = "spectral"

    clip_policy = "strict" if setup_override_policy == "strict" else "clip"
    if policy == "full":
        ch_start = 0
        ch_stop = full_nchan
        computed_windows: List[Dict[str, Any]] = [
            {"kind": "channel", "computed_ch_start": ch_start, "computed_ch_stop": ch_stop}
        ]
    elif policy == "channel":
        ch_start = _as_int(rec.get("saved_ch_start"), field=f"recording_groups.{group_id}.saved_ch_start")
        ch_stop = _as_int(rec.get("saved_ch_stop"), field=f"recording_groups.{group_id}.saved_ch_stop")
        if ch_start < 0 or ch_stop > full_nchan or ch_start >= ch_stop:
            raise SpectralRecordingValidationError(
                f"recording_groups.{group_id}: invalid channel slice [{ch_start}:{ch_stop}] "
                f"for full_nchan={full_nchan}"
            )
        computed_windows = [
            {"kind": "channel", "computed_ch_start": ch_start, "computed_ch_stop": ch_stop}
        ]
    else:  # contiguous_envelope
        rest_frequency_hz = _as_float(
            rec.get("rest_frequency_hz", stream.get("default_rest_frequency_hz")),
            field=f"recording_groups.{group_id}.rest_frequency_hz",
        )
        velocity_definition = str(rec.get("velocity_definition", "radio"))
        vmin_kms = _as_float(rec.get("vmin_kms"), field=f"recording_groups.{group_id}.vmin_kms")
        vmax_kms = _as_float(rec.get("vmax_kms"), field=f"recording_groups.{group_id}.vmax_kms")
        margin_kms = _as_float(rec.get("margin_kms", 0.0), field=f"recording_groups.{group_id}.margin_kms")
        freq = _frequency_array_for_stream(stream, frequency_axes=frequency_axes, lo_chains=lo_chains)
        resolved = resolve_velocity_window_to_channels(
            rest_frequency_hz=rest_frequency_hz,
            velocity_definition=velocity_definition,
            vmin_kms=vmin_kms,
            vmax_kms=vmax_kms,
            margin_kms=margin_kms,
            frequency_hz=freq,
            clip_policy=clip_policy,
        )
        ch_start = int(resolved["computed_ch_start"])
        ch_stop = int(resolved["computed_ch_stop"])
        requested_velocity_frame = str(rec.get("velocity_frame", "LSRK"))
        resolver_warnings = list(resolved.get("resolver_warnings", []))
        resolver_warnings.append(
            "velocity resolver is topocentric_rest_velocity_approx_v1: "
            "source coordinate, site, reference_time_utc, and LSRK correction are not applied"
        )
        resolved = dict(resolved)
        resolved["resolver_warnings"] = resolver_warnings
        computed_windows = [
            {
                "kind": "velocity",
                "line_name": str(rec.get("line_name", "")),
                "rest_frequency_hz": rest_frequency_hz,
                "requested_velocity_frame": requested_velocity_frame,
                "velocity_frame": VELOCITY_RESOLVER_FRAME,
                "velocity_resolver_version": VELOCITY_RESOLVER_VERSION,
                "velocity_definition": velocity_definition,
                **resolved,
            }
        ]
    saved_nchan = ch_stop - ch_start
    out.update(
        {
            "saved_ch_start": int(ch_start),
            "saved_ch_stop": int(ch_stop),
            "saved_nchan": int(saved_nchan),
            "computed_windows": computed_windows,
            "computed_windows_json": json.dumps(computed_windows, sort_keys=True, separators=(",", ":")),
        }
    )
    if mode == "tp":
        out.setdefault("tp_stat", str(rec.get("tp_stat", "sum_mean")))
        out["tp_nchan_configured"] = int(saved_nchan)
    return out


def _assign_recording_groups(
    streams: Mapping[str, Mapping[str, Any]],
    recording_setup: Optional[Mapping[str, Any]],
    *,
    frequency_axes: Mapping[str, Mapping[str, Any]],
    lo_chains: Mapping[str, Mapping[str, Any]],
    setup_override_policy: str,
) -> Dict[str, Dict[str, Any]]:
    groups = _recording_groups(recording_setup)
    assignments: Dict[str, Dict[str, Any]] = {}
    for group_id, raw_group in groups.items():
        group = _ensure_mapping(raw_group, field=f"recording_groups.{group_id}")
        group["recording_group_id"] = str(group_id)
        stream_ids = _as_str_list(group.get("streams", []), field=f"recording_groups.{group_id}.streams")
        if not stream_ids:
            raise SpectralRecordingValidationError(f"recording_groups.{group_id}.streams must not be empty")
        for stream_id in stream_ids:
            if stream_id not in streams:
                raise SpectralRecordingValidationError(
                    f"recording_groups.{group_id} references unknown stream_id {stream_id!r}"
                )
            if stream_id in assignments:
                raise SpectralRecordingValidationError(
                    f"stream_id {stream_id!r} is assigned to multiple recording_groups"
                )
            assignments[stream_id] = group

    resolved: Dict[str, Dict[str, Any]] = {}
    for stream_id, stream in streams.items():
        resolved[stream_id] = _resolve_recording_for_stream(
            stream,
            assignments.get(stream_id),
            frequency_axes=frequency_axes,
            lo_chains=lo_chains,
            setup_override_policy=setup_override_policy,
        )
    return resolved



def _utf8_len(value: Any) -> int:
    return len(str(value).encode("utf-8"))


def _require_fixed_string_limit(value: Any, limit: int, *, field: str) -> None:
    length = _utf8_len(value)
    if length > limit:
        raise SpectralRecordingValidationError(
            f"{field} is too long for fixed schema: {length} bytes > {limit} bytes"
        )


def _validate_snapshot_fixed_schema(snapshot: Mapping[str, Any]) -> None:
    setup_id = str(snapshot.get("setup_id", ""))
    setup_hash = str(snapshot.get("canonical_snapshot_sha256", "") or snapshot.get("setup_hash", ""))
    _require_fixed_string_limit(setup_id, 64, field="snapshot.setup_id")
    if setup_hash:
        _require_fixed_string_limit(setup_hash, 64, field="snapshot.canonical_snapshot_sha256")
    streams = _ensure_mapping(snapshot.get("streams", {}), field="snapshot.streams")
    for stream_id, raw_stream in streams.items():
        stream = _ensure_mapping(raw_stream, field=f"snapshot.streams.{stream_id}")
        _require_fixed_string_limit(str(stream.get("stream_id", stream_id)), 64, field=f"snapshot.streams.{stream_id}.stream_id")
        _require_fixed_string_limit(str(stream.get("spectrometer_key", "")), 32, field=f"snapshot.streams.{stream_id}.spectrometer_key")
        _require_fixed_string_limit(str(stream.get("polariza", "")), 8, field=f"snapshot.streams.{stream_id}.polariza")
        _require_fixed_string_limit(str(stream.get("beam_id", "")), 32, field=f"snapshot.streams.{stream_id}.beam_id")
        _require_fixed_string_limit(str(stream.get("saved_window_policy", "")), 32, field=f"snapshot.streams.{stream_id}.saved_window_policy")
        if str(stream.get("recording_mode", "spectrum")) == "tp":
            _require_fixed_string_limit(str(stream.get("tp_stat", "sum_mean")), 16, field=f"snapshot.streams.{stream_id}.tp_stat")
            windows_json = str(stream.get("computed_windows_json", "") or "")
            if not windows_json:
                start = int(stream.get("saved_ch_start", 0))
                stop = int(stream.get("saved_ch_stop", 0))
                windows_json = json.dumps(
                    [{"kind": "channel", "computed_ch_start": start, "computed_ch_stop": stop}],
                    sort_keys=True,
                    separators=(",", ":"),
                )
            _require_fixed_string_limit(windows_json, 2048, field=f"snapshot.streams.{stream_id}.tp_windows_json")


def _validate_db_table_path_kind(stream_id: str, stream: Mapping[str, Any]) -> None:
    kind = str(stream.get("recording_table_kind", "spectral"))
    path = str(stream.get("db_table_path", ""))
    if kind == "tp":
        if not path.startswith("data/tp/"):
            raise SpectralRecordingValidationError(
                f"streams.{stream_id}: recording_table_kind='tp' requires db_table_path under 'data/tp/', got {path!r}"
            )
    elif kind == "spectral":
        if not path.startswith("data/spectral/"):
            raise SpectralRecordingValidationError(
                f"streams.{stream_id}: recording_table_kind='spectral' requires db_table_path under 'data/spectral/', got {path!r}"
            )
    else:
        raise SpectralRecordingValidationError(
            f"streams.{stream_id}: recording_table_kind must be 'spectral' or 'tp', got {kind!r}"
        )

def _validate_unique_stream_keys(streams: Mapping[str, Mapping[str, Any]]) -> None:
    seen_raw: Dict[Tuple[str, int], str] = {}
    seen_db: Dict[str, str] = {}
    seen_sdfits: Dict[Tuple[int, int, int, str], str] = {}
    for stream_id, stream in streams.items():
        _validate_db_table_path_kind(str(stream_id), stream)
        raw_key = (str(stream["spectrometer_key"]), int(stream["board_id"]))
        if raw_key in seen_raw:
            raise SpectralRecordingValidationError(
                f"Duplicate raw input key {raw_key}: {seen_raw[raw_key]} and {stream_id}"
            )
        seen_raw[raw_key] = stream_id
        db_path = str(stream["db_table_path"])
        if db_path in seen_db:
            raise SpectralRecordingValidationError(
                f"Duplicate db_table_path {db_path!r}: {seen_db[db_path]} and {stream_id}"
            )
        seen_db[db_path] = stream_id
        sdfits_key = (int(stream["fdnum"]), int(stream["ifnum"]), int(stream["plnum"]), str(stream["polariza"]))
        if sdfits_key in seen_sdfits:
            raise SpectralRecordingValidationError(
                f"Duplicate SDFITS key {sdfits_key}: {seen_sdfits[sdfits_key]} and {stream_id}"
            )
        seen_sdfits[sdfits_key] = stream_id


def resolve_spectral_recording_setup(
    *,
    lo_profile: Mapping[str, Any],
    recording_setup: Optional[Mapping[str, Any]],
    beam_model: Mapping[str, Any],
    setup_id: str,
    input_paths: Optional[Mapping[str, os.PathLike[str] | str]] = None,
    setup_override_policy: str = "strict",
    created_utc: Optional[str] = None,
) -> Dict[str, Any]:
    if setup_override_policy not in SETUP_OVERRIDE_POLICIES:
        raise SpectralRecordingValidationError(
            f"setup_override_policy must be one of {sorted(SETUP_OVERRIDE_POLICIES)}, got {setup_override_policy!r}"
        )
    if not setup_id:
        raise SpectralRecordingValidationError("setup_id must not be empty")
    created_utc = created_utc or _datetime.datetime.now(_datetime.timezone.utc).isoformat().replace("+00:00", "Z")

    resolved_roles = _resolve_lo_roles(lo_profile)
    resolved_chains = _resolve_lo_chains(lo_profile, resolved_roles)
    frequency_axes = _normalize_frequency_axes(lo_profile)
    streams = _expand_stream_groups(lo_profile, frequency_axes=frequency_axes, lo_chains=resolved_chains)
    beams = _validate_beam_ids(streams, beam_model)
    recording = _assign_recording_groups(
        streams,
        recording_setup,
        frequency_axes=frequency_axes,
        lo_chains=resolved_chains,
        setup_override_policy=setup_override_policy,
    )

    normalized_streams: Dict[str, Dict[str, Any]] = {}
    for stream_id in sorted(streams):
        stream = _deepcopy_dict(streams[stream_id])
        rec = recording[stream_id]
        stream.update(rec)
        stream["recording_table_kind"] = "tp" if stream["recording_mode"] == "tp" else "spectral"
        if "db_table_path" not in stream:
            stream["db_table_path"] = _default_db_table_path(
                str(stream["spectrometer_key"]), int(stream["board_id"]), kind=str(stream["recording_table_kind"])
            )
        if "db_stream_name" not in stream:
            stream["db_stream_name"] = str(stream_id)
        stream["raw_input_key"] = str(stream.get("raw_input_key", stream["spectrometer_key"]))
        stream["raw_board_id"] = int(stream.get("raw_board_id", stream["board_id"]))
        normalized_streams[stream_id] = _sorted_dict(stream)

    _validate_unique_stream_keys(normalized_streams)

    inputs: Dict[str, Dict[str, Any]] = {}
    if input_paths:
        for key, raw_path in sorted(input_paths.items()):
            path = Path(raw_path)
            if path.exists():
                inputs[key] = {
                    "path": str(path),
                    "input_file_sha256": sha256_file(path),
                }
            else:
                inputs[key] = {"path": str(path), "input_file_sha256": ""}

    snapshot: Dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "snapshot_version": SNAPSHOT_VERSION,
        "setup_id": setup_id,
        "created_utc": created_utc,
        "setup_override_policy": setup_override_policy,
        "canonical_snapshot_sha256": "",
        "inputs": inputs,
        "lo_roles": _sorted_dict(resolved_roles),
        "lo_chains": _sorted_dict(resolved_chains),
        "frequency_axes": _sorted_dict(frequency_axes),
        "beams": _sorted_dict(beams),
        "streams": normalized_streams,
    }
    _validate_snapshot_fixed_schema(snapshot)
    canonical_without_hash = copy.deepcopy(snapshot)
    canonical_without_hash["canonical_snapshot_sha256"] = ""
    snapshot_hash = sha256_text(dumps_toml(canonical_without_hash))
    snapshot["canonical_snapshot_sha256"] = snapshot_hash
    return snapshot


def load_and_resolve_spectral_recording_setup(
    *,
    lo_profile_path: os.PathLike[str] | str,
    recording_window_setup_path: Optional[os.PathLike[str] | str],
    beam_model_path: os.PathLike[str] | str,
    setup_id: str,
    setup_override_policy: str = "strict",
) -> Dict[str, Any]:
    lo_path = Path(lo_profile_path)
    rec_path = Path(recording_window_setup_path) if recording_window_setup_path else None
    beam_path = Path(beam_model_path)
    lo_profile = read_toml(lo_path)
    recording_setup = read_toml(rec_path) if rec_path else None
    beam_model = read_toml(beam_path)
    input_paths: Dict[str, Path] = {"lo_profile": lo_path, "beam_model": beam_path}
    if rec_path:
        input_paths["recording_window_setup"] = rec_path
    return resolve_spectral_recording_setup(
        lo_profile=lo_profile,
        recording_setup=recording_setup,
        beam_model=beam_model,
        setup_id=setup_id,
        input_paths=input_paths,
        setup_override_policy=setup_override_policy,
    )


def validate_snapshot(snapshot: Mapping[str, Any]) -> None:
    if snapshot.get("schema_version") != SCHEMA_VERSION:
        raise SpectralRecordingValidationError(
            f"Unsupported schema_version: {snapshot.get('schema_version')!r}"
        )
    streams = _ensure_mapping(snapshot.get("streams", {}), field="snapshot.streams")
    if not streams:
        raise SpectralRecordingValidationError("snapshot.streams must not be empty")
    for stream_id, raw_stream in streams.items():
        stream = _ensure_mapping(raw_stream, field=f"snapshot.streams.{stream_id}")
        for key in (
            "stream_id",
            "spectrometer_key",
            "board_id",
            "db_table_path",
            "db_stream_name",
            "fdnum",
            "ifnum",
            "plnum",
            "polariza",
            "beam_id",
            "recording_mode",
            "recording_table_kind",
            "saved_ch_start",
            "saved_ch_stop",
            "saved_nchan",
            "full_nchan",
        ):
            if key not in stream:
                raise SpectralRecordingValidationError(f"snapshot.streams.{stream_id} missing {key}")
        if str(stream["stream_id"]) != str(stream_id):
            raise SpectralRecordingValidationError(
                f"snapshot stream table key {stream_id!r} does not match stream_id field {stream['stream_id']!r}"
            )
        ch_start = _as_int(stream["saved_ch_start"], field=f"snapshot.streams.{stream_id}.saved_ch_start")
        ch_stop = _as_int(stream["saved_ch_stop"], field=f"snapshot.streams.{stream_id}.saved_ch_stop")
        saved_nchan = _as_int(stream["saved_nchan"], field=f"snapshot.streams.{stream_id}.saved_nchan")
        full_nchan = _as_int(stream["full_nchan"], field=f"snapshot.streams.{stream_id}.full_nchan")
        if not (0 <= ch_start < ch_stop <= full_nchan):
            raise SpectralRecordingValidationError(
                f"snapshot.streams.{stream_id}: invalid saved range [{ch_start}:{ch_stop}] for full_nchan={full_nchan}"
            )
        if saved_nchan != ch_stop - ch_start:
            raise SpectralRecordingValidationError(
                f"snapshot.streams.{stream_id}: saved_nchan={saved_nchan} does not match range [{ch_start}:{ch_stop}]"
            )
    _validate_unique_stream_keys(streams)
    _validate_snapshot_fixed_schema(snapshot)


def local_channel_to_full_channel(saved_ch_start: int, c_local: int) -> int:
    if c_local < 0:
        raise SpectralRecordingValidationError("c_local must be >= 0")
    return int(saved_ch_start) + int(c_local)


def if_frequency_at_full_channel(axis: Mapping[str, Any], c_full: int) -> float:
    return float(axis["if_freq_at_full_ch0_hz"]) + int(c_full) * float(axis["if_freq_step_hz"])


def if_frequency_at_saved_local_channel(axis: Mapping[str, Any], saved_ch_start: int, c_local: int) -> float:
    return if_frequency_at_full_channel(axis, local_channel_to_full_channel(saved_ch_start, c_local))


def write_snapshot(path: os.PathLike[str] | str, snapshot: Mapping[str, Any]) -> None:
    Path(path).write_text(dumps_toml(snapshot), encoding="utf-8")


def _build_resolve_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="necst-spectral-resolve",
        description="Resolve NECST/XFFTS spectral recording setup files into a canonical snapshot TOML.",
    )
    parser.add_argument("--lo-profile", required=True, help="Path to lo_profile.toml")
    parser.add_argument(
        "--recording-window-setup",
        default=None,
        help="Path to recording_window_setup.toml. If omitted, all streams default to full-spectrum recording.",
    )
    parser.add_argument("--beam-model", required=True, help="Path to beam_model.toml")
    parser.add_argument("--setup-id", required=True, help="Human-readable setup id stored in the snapshot")
    parser.add_argument("--output", required=True, help="Output spectral_recording_snapshot.toml")
    parser.add_argument(
        "--setup-override-policy",
        default="strict",
        choices=sorted(SETUP_OVERRIDE_POLICIES),
        help="Conflict policy for resolver-side override handling. Default: strict.",
    )
    return parser


def main_resolve(argv: Optional[Sequence[str]] = None) -> int:
    parser = _build_resolve_parser()
    args = parser.parse_args(argv)
    try:
        snapshot = load_and_resolve_spectral_recording_setup(
            lo_profile_path=args.lo_profile,
            recording_window_setup_path=args.recording_window_setup,
            beam_model_path=args.beam_model,
            setup_id=args.setup_id,
            setup_override_policy=args.setup_override_policy,
        )
        validate_snapshot(snapshot)
        write_snapshot(args.output, snapshot)
    except SpectralRecordingSetupError as exc:
        parser.error(str(exc))
        return 2
    return 0


def _build_validate_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="necst-spectral-validate",
        description="Validate a resolved spectral_recording_snapshot.toml.",
    )
    parser.add_argument("snapshot", help="Path to spectral_recording_snapshot.toml")
    return parser


def main_validate(argv: Optional[Sequence[str]] = None) -> int:
    parser = _build_validate_parser()
    args = parser.parse_args(argv)
    try:
        snapshot = read_toml(args.snapshot)
        validate_snapshot(snapshot)
    except SpectralRecordingSetupError as exc:
        parser.error(str(exc))
        return 2
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main_resolve())
