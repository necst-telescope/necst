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
import re
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
SAVED_WINDOW_POLICIES = {"full", "contiguous_envelope", "channel", "multi_window"}
RECORDING_MODES = {"spectrum", "tp"}
VELOCITY_RESOLVER_VERSION = "topocentric_rest_velocity_approx_v1"
VELOCITY_RESOLVER_FRAME = "TOPOCENTRIC_REST_APPROX"
VLSRK_RESOLVER_VERSION = "astropy_lsrk_reference_time_v1"
VLSRK_RESOLVER_FRAME = "LSRK"
CHANNEL_ORDERS_INCREASING = {"increasing_if", "increasing_if_frequency", "increasing"}
CHANNEL_ORDERS_DECREASING = {"decreasing_if", "decreasing_if_frequency", "decreasing"}

# ``stream_groups`` provide defaults; each stream item may override the
# instrument-defining fields.  Stream-local LO/frequency/backend settings are
# first-class because different XFFTS boards/streams can have different
# spectrometers, LO chains, IF axes, rest frequencies, and DB bindings.
STREAM_TRUTH_KEYS = {
    "spectrometer_key",
    "frontend",
    "backend",
    "sampler",
    "lo_chain",
    "polariza",
    "frequency_axis_id",
    "rest_frequency_hz",
    "rest_frequency_ghz",
    "rest_frequency_mhz",
    "restfreq_hz",
    "restfreq_ghz",
    "restfreq_mhz",
    "default_rest_frequency_hz",
    "default_rest_frequency_ghz",
    "default_rest_frequency_mhz",
    "beam_id",
}
REST_FREQUENCY_KEYS = {
    "rest_frequency_hz",
    "rest_frequency_ghz",
    "rest_frequency_mhz",
    "restfreq_hz",
    "restfreq_ghz",
    "restfreq_mhz",
    "default_rest_frequency_hz",
    "default_rest_frequency_ghz",
    "default_rest_frequency_mhz",
}

GROUP_REQUIRED_KEYS: set[str] = set()
GROUP_OPTIONAL_KEYS = {
    *STREAM_TRUTH_KEYS,
    # PR8g: analysis-time stream selection no longer belongs to the
    # observation-time registry.  The legacy use_for_* keys are still accepted
    # as backward-compatible analysis-default hints and are canonicalized into
    # streams.<stream_id>.analysis_defaults, not top-level truth fields.
    "use_for_convert",
    "use_for_sunscan",
    "use_for_fit",
    "analysis_defaults",
    "recording_default_policy",
    "human_label",
}
GROUP_ALLOWED_KEYS = GROUP_OPTIONAL_KEYS | {"streams"}
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
    "raw_input_key",
    "raw_board_id",
    "human_label",
    *STREAM_TRUTH_KEYS,
}
ITEM_REQUIRED_KEYS = {"stream_id", "board_id", "fdnum", "ifnum", "plnum"}
RESOLVED_STREAM_REQUIRED_KEYS = {
    "spectrometer_key",
    "frontend",
    "backend",
    "lo_chain",
    "polariza",
    "frequency_axis_id",
}


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


def _frequency_alias_hz(
    cfg: Mapping[str, Any],
    *,
    field_base: str,
    canonical: str,
    aliases: Mapping[str, float],
    required: bool = False,
) -> Optional[float]:
    """Return a frequency-like value in Hz from *_hz/*_ghz/*_mhz aliases.

    ``canonical`` is the preferred *_hz key.  ``aliases`` maps accepted input
    keys to their multiplier to Hz.  When more than one alias is supplied, all
    supplied values must agree to tight floating precision.  This keeps short
    GHz/MHz operator-facing configs safe without allowing silent unit mistakes.
    """
    values: List[Tuple[str, float]] = []
    for key, factor in aliases.items():
        if key in cfg and cfg.get(key) is not None:
            value_hz = _as_float(cfg[key], field=f"{field_base}.{key}") * factor
            values.append((key, value_hz))
    if not values:
        if required:
            keys = ", ".join(sorted(aliases))
            raise SpectralRecordingValidationError(f"{field_base} must define one of: {keys}")
        return None
    ref_key, ref_value = values[0]
    if not math.isfinite(ref_value) or ref_value <= 0.0:
        raise SpectralRecordingValidationError(f"{field_base}.{ref_key} must be positive finite")
    for key, value in values[1:]:
        if not math.isfinite(value) or value <= 0.0:
            raise SpectralRecordingValidationError(f"{field_base}.{key} must be positive finite")
        tol = max(1.0e-6, abs(ref_value) * 1.0e-12)
        if abs(value - ref_value) > tol:
            raise SpectralRecordingValidationError(
                f"{field_base} has inconsistent frequency aliases: "
                f"{ref_key}={ref_value} Hz but {key}={value} Hz"
            )
    return float(ref_value)


def _rest_frequency_hz_from_stream(entry: Mapping[str, Any], *, field_base: str) -> Optional[float]:
    return _frequency_alias_hz(
        entry,
        field_base=field_base,
        canonical="rest_frequency_hz",
        aliases={
            "rest_frequency_hz": 1.0,
            "restfreq_hz": 1.0,
            "default_rest_frequency_hz": 1.0,
            "rest_frequency_ghz": 1.0e9,
            "restfreq_ghz": 1.0e9,
            "default_rest_frequency_ghz": 1.0e9,
            "rest_frequency_mhz": 1.0e6,
            "restfreq_mhz": 1.0e6,
            "default_rest_frequency_mhz": 1.0e6,
        },
        required=False,
    )


def _copy_frequency_alias_canonical(
    entry: MutableMapping[str, Any],
    *,
    field_base: str,
    canonical: str,
    aliases: Mapping[str, float],
    required: bool = False,
) -> Optional[float]:
    value = _frequency_alias_hz(entry, field_base=field_base, canonical=canonical, aliases=aliases, required=required)
    if value is not None:
        entry[canonical] = value
        # Remove non-canonical unit aliases from the resolved truth so the
        # snapshot has exactly one unit convention.
        for key in aliases:
            if key != canonical:
                entry.pop(key, None)
    return value


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


WINDOW_ID_RE = re.compile(r"^[A-Za-z0-9_+-]+$")


def _safe_window_id(value: Any, *, field: str) -> str:
    window_id = _as_str(value, field=field).strip()
    if not window_id:
        raise SpectralRecordingValidationError(f"{field} must not be empty")
    if not WINDOW_ID_RE.match(window_id):
        raise SpectralRecordingValidationError(
            f"{field}={window_id!r} contains invalid characters; allowed: A-Z a-z 0-9 _ + -"
        )
    return window_id


def _rest_frequency_hz_from_window(entry: Mapping[str, Any], *, field_base: str) -> Optional[float]:
    return _frequency_alias_hz(
        entry,
        field_base=field_base,
        canonical="rest_frequency_hz",
        aliases={
            "rest_frequency_hz": 1.0,
            "restfreq_hz": 1.0,
            "default_rest_frequency_hz": 1.0,
            "rest_frequency_ghz": 1.0e9,
            "restfreq_ghz": 1.0e9,
            "default_rest_frequency_ghz": 1.0e9,
            "rest_frequency_mhz": 1.0e6,
            "restfreq_mhz": 1.0e6,
            "default_rest_frequency_mhz": 1.0e6,
        },
        required=False,
    )


def _window_list_from_recording_group(rec: Mapping[str, Any], *, group_id: str) -> List[Dict[str, Any]]:
    """Return explicit window definitions from a recording group.

    ``windows`` is the canonical name.  ``velocity_windows`` is accepted as a
    backwards-compatible alias used while the multi-line envelope design was
    being drafted.  The two keys must not be supplied together.
    """
    has_windows = "windows" in rec and rec.get("windows") is not None
    has_velocity_windows = "velocity_windows" in rec and rec.get("velocity_windows") is not None
    if has_windows and has_velocity_windows:
        raise SpectralRecordingValidationError(
            f"recording_groups.{group_id}: use either windows or velocity_windows, not both"
        )
    raw = rec.get("windows") if has_windows else rec.get("velocity_windows")
    if raw is None:
        return []
    if not isinstance(raw, list):
        raise SpectralRecordingValidationError(f"recording_groups.{group_id}.windows must be a list of tables")
    out: List[Dict[str, Any]] = []
    seen: set[str] = set()
    for i, item in enumerate(raw):
        if not isinstance(item, Mapping):
            raise SpectralRecordingValidationError(f"recording_groups.{group_id}.windows[{i}] must be a table")
        w = dict(item)
        window_id = _safe_window_id(
            w.get("window_id", w.get("line_name", f"window{i}")),
            field=f"recording_groups.{group_id}.windows[{i}].window_id",
        )
        if window_id in seen:
            raise SpectralRecordingValidationError(f"recording_groups.{group_id}: duplicate window_id {window_id!r}")
        seen.add(window_id)
        w["window_id"] = window_id
        out.append(w)
    return out


def _recorded_stream_id(source_stream_id: str, window_id: str) -> str:
    return f"{source_stream_id}__{window_id}"


def _recorded_db_stream_name(source_db_stream_name: str, window_id: str) -> str:
    return f"{source_db_stream_name}__{window_id}"


def _apply_recorded_window_metadata_overrides(
    product: Dict[str, Any],
    window: Mapping[str, Any],
    *,
    group_id: str,
    window_index: int,
) -> None:
    """Apply optional per-window SDFITS metadata overrides to a saved product.

    Multi-window recording splits one source stream into multiple saved products.
    The source stream has only one FDNUM/IFNUM/PLNUM, but the saved products may
    need distinct SDFITS numbering, for example 13CO and C18O windows cut from the
    same XFFTS board.  The override is intentionally product-local and does not
    modify the source stream.
    """

    for key in ("fdnum", "ifnum", "plnum"):
        if key in window:
            value = _as_int(window.get(key), field=f"recording_groups.{group_id}.windows[{window_index}].{key}")
            if value < 0:
                raise SpectralRecordingValidationError(
                    f"recording_groups.{group_id}.windows[{window_index}].{key} must be non-negative"
                )
            product[key] = value

    for key in ("polariza", "beam_id"):
        if key in window:
            product[key] = str(window[key])


def _db_table_path_for_recorded_product(stream: Mapping[str, Any], recorded_db_stream_name: str, *, mode: str) -> str:
    kind = "tp" if mode == "tp" else "spectral"
    spectrometer_key = str(stream.get("spectrometer_key", ""))
    safe = _canonical_table_name(_recorded_product_table_stem(stream, recorded_db_stream_name)).strip("/")
    if kind == "tp":
        return f"data/tp/{spectrometer_key}/{safe}"
    return f"data/spectral/{spectrometer_key}/{safe}"


def _recorded_product_table_stem(stream: Mapping[str, Any], recorded_db_stream_name: str) -> str:
    """Return the final table name used below ``data/<kind>/<spectrometer>/``.

    ``recorded_db_stream_name`` intentionally preserves the legacy DB stream
    alias, for example ``xffts-board2__13CO_J2_1``.  If we use that alias as the
    final table name under ``data/spectral/xffts/``, NECSTDB file names become
    visually redundant (``...-data-spectral-xffts-xffts-board2__...data``).

    For the actual table path, prefer the canonical source table leaf from
    ``db_table_path`` (``board2`` for ``data/spectral/xffts/board2``) and append
    the window suffix.  This keeps the path short while retaining the legacy
    alias in ``db_stream_name``/``recorded_db_stream_name`` for compatibility and
    traceability.
    """
    recorded = _canonical_table_name(recorded_db_stream_name).strip("/")
    source_path = str(stream.get("db_table_path", "") or "").strip("/")
    if source_path:
        source_leaf = _canonical_table_name(source_path.split("/")[-1]).strip("/")
    else:
        source_leaf = _canonical_table_name(str(stream.get("db_stream_name", stream.get("stream_id", "")))).strip("/")
        prefix = f"{stream.get('spectrometer_key', '')}-"
        if prefix != "-" and source_leaf.startswith(prefix):
            source_leaf = source_leaf[len(prefix):]
    source_alias = _canonical_table_name(str(stream.get("db_stream_name", stream.get("stream_id", "")))).strip("/")
    suffix = recorded
    for prefix in (source_alias + "__", source_leaf + "__"):
        if suffix.startswith(prefix):
            suffix = suffix[len(prefix):]
            break
    if source_leaf and suffix and suffix != recorded:
        return f"{source_leaf}__{suffix}"
    return recorded


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
    # Role-based profiles define lo_roles + lo_chains.  Legacy-compatible
    # stream-by-stream LO chains can be self-contained loN_hz/sbN entries and
    # therefore do not need any lo_roles at all.
    if not lo_roles:
        return {}

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
            sg_set_frequency_hz = _frequency_alias_hz(
                sg,
                field_base=f"sg_devices.{sg_id}",
                canonical="sg_set_frequency_hz",
                aliases={
                    "sg_set_frequency_hz": 1.0,
                    "sg_set_frequency_ghz": 1.0e9,
                    "sg_set_frequency_mhz": 1.0e6,
                },
                required=True,
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
            if any(k in role for k in ("expected_lo_frequency_hz", "expected_lo_frequency_ghz", "expected_lo_frequency_mhz")):
                expected = _frequency_alias_hz(
                    role,
                    field_base=f"lo_roles.{role_id}",
                    canonical="expected_lo_frequency_hz",
                    aliases={
                        "expected_lo_frequency_hz": 1.0,
                        "expected_lo_frequency_ghz": 1.0e9,
                        "expected_lo_frequency_mhz": 1.0e6,
                    },
                    required=True,
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
            fixed = _frequency_alias_hz(
                role,
                field_base=f"lo_roles.{role_id}",
                canonical="fixed_lo_frequency_hz",
                aliases={
                    "fixed_lo_frequency_hz": 1.0,
                    "fixed_lo_frequency_ghz": 1.0e9,
                    "fixed_lo_frequency_mhz": 1.0e6,
                },
                required=True,
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


def _legacy_local_oscillator_stages(chain_id: str, chain: Mapping[str, Any]) -> Tuple[List[Tuple[float, str]], Dict[str, Any]]:
    stages: List[Tuple[float, str]] = []
    local_oscillators: Dict[str, Any] = {}
    for i in range(1, 5):
        sb_key = f"sb{i}"
        aliases = {
            f"lo{i}_hz": 1.0,
            f"lo{i}_ghz": 1.0e9,
            f"lo{i}_mhz": 1.0e6,
        }
        lo_present = any(key in chain and chain.get(key) is not None for key in aliases)
        sb_present = sb_key in chain and chain.get(sb_key) is not None
        if not lo_present and not sb_present:
            continue
        if not lo_present or not sb_present:
            alias_names = "/".join(aliases)
            raise SpectralRecordingValidationError(
                f"lo_chains.{chain_id} legacy LO stage requires one of {alias_names} and {sb_key}"
            )
        lo_hz = _frequency_alias_hz(
            chain,
            field_base=f"lo_chains.{chain_id}",
            canonical=f"lo{i}_hz",
            aliases=aliases,
            required=True,
        )
        assert lo_hz is not None
        sb = str(_as_str(chain.get(sb_key), field=f"lo_chains.{chain_id}.{sb_key}")).strip().upper()
        if sb not in {"USB", "LSB"}:
            raise SpectralRecordingValidationError(f"lo_chains.{chain_id}.{sb_key} must be USB or LSB, got {sb!r}")
        stages.append((lo_hz, sb))
        local_oscillators[f"lo{i}_hz"] = lo_hz
        local_oscillators[sb_key] = sb
    for optional in ("obsfreq_hz", "imagfreq_hz"):
        if optional in chain:
            local_oscillators[optional] = _as_float(chain[optional], field=f"lo_chains.{chain_id}.{optional}")
    if "store_freq_column" in chain:
        local_oscillators["store_freq_column"] = chain["store_freq_column"]
    return stages, local_oscillators


def _resolve_legacy_lo_chain(chain_id: str, chain: Mapping[str, Any]) -> Optional[Dict[str, Any]]:
    """Resolve old converter-style lo1_hz/sb1/lo2_hz/sb2 chains.

    The converter's historical frequency formula applies the conversion stages
    in reverse order:

        out = IF
        for stage in reversed(stages):
            out = lo + sign(sb) * out

    Therefore the final sky frequency is

        sky = signed_lo_sum_hz + if_frequency_sign * IF

    where ``if_frequency_sign`` can be -1 for an odd number of LSB stages.
    """

    stages, local_oscillators = _legacy_local_oscillator_stages(chain_id, chain)
    if not stages:
        return None
    signed_sum = 0.0
    if_sign = 1.0
    for lo_hz, sb in reversed(stages):
        sign = 1.0 if sb == "USB" else -1.0
        signed_sum = lo_hz + sign * signed_sum
        if_sign = sign * if_sign
    return {
        "lo_chain_id": str(chain_id),
        "formula_version": str(chain.get("formula_version", "legacy_local_oscillators_v1")),
        "legacy_local_oscillators": local_oscillators,
        "lo_stage_order": [f"lo{i + 1}" for i in range(len(stages))],
        "lo_frequencies_hz": [float(lo) for lo, _ in stages],
        "sidebands": [str(sb) for _, sb in stages],
        "signed_lo_sum_hz": float(signed_sum),
        "if_frequency_sign": float(if_sign),
    }


def _resolve_lo_chains(lo_profile: Mapping[str, Any], resolved_roles: Mapping[str, Mapping[str, Any]]) -> Dict[str, Dict[str, Any]]:
    lo_chains = _ensure_mapping(lo_profile.get("lo_chains", {}), field="lo_chains")
    if not lo_chains:
        raise SpectralRecordingValidationError("lo_chains must not be empty")
    resolved: Dict[str, Dict[str, Any]] = {}
    for chain_id, raw_chain in lo_chains.items():
        chain = _ensure_mapping(raw_chain, field=f"lo_chains.{chain_id}")

        legacy_chain = _resolve_legacy_lo_chain(str(chain_id), chain)
        if legacy_chain is not None:
            # A legacy inline chain is self-contained.  It may coexist with
            # lo_roles elsewhere in the profile, but this particular chain must
            # not also define role references because that would make the IF sign
            # convention ambiguous.
            if "lo_roles" in chain or any(k in chain for k in ("lo1_role", "lo2_role", "lo3_role", "lo4_role")):
                raise SpectralRecordingValidationError(
                    f"lo_chains.{chain_id} must not mix legacy loN_hz/sbN keys with lo_roles/loN_role keys"
                )
            resolved[str(chain_id)] = legacy_chain
            continue

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
            "if_frequency_sign": float(chain.get("if_frequency_sign", 1.0)),
        }
    return resolved

def _normalize_channel_order_from_step(step: float) -> str:
    return "increasing_if" if float(step) > 0 else "decreasing_if"


def _normalize_frequency_axis(axis_id: str, raw_axis: Mapping[str, Any]) -> Dict[str, Any]:
    axis = _ensure_mapping(raw_axis, field=f"frequency_axes.{axis_id}")
    mode = str(axis.get("definition_mode", "first_center_and_delta")).strip().lower()
    mode_aliases = {
        "first_center_and_delta": "first_center_and_delta",
        "first_center_delta": "first_center_and_delta",
        "explicit_wcs": "explicit_wcs",
        "band_start_stop": "band_start_stop",
        "band_edges": "band_start_stop",
    }
    if mode not in mode_aliases:
        raise SpectralRecordingValidationError(
            f"frequency_axes.{axis_id}.definition_mode must be first_center_and_delta, explicit_wcs, or band_start_stop; got {mode!r}"
        )
    mode = mode_aliases[mode]

    nchan_raw = axis.get("full_nchan", axis.get("nchan"))
    full_nchan = _as_int(nchan_raw, field=f"frequency_axes.{axis_id}.full_nchan/nchan")
    if full_nchan <= 0:
        raise SpectralRecordingValidationError(f"frequency_axes.{axis_id}.full_nchan/nchan must be positive")

    out: Dict[str, Any] = {
        "frequency_axis_id": str(axis_id),
        "definition_mode": mode,
        "full_nchan": full_nchan,
        "nchan": full_nchan,
    }

    if mode == "explicit_wcs":
        crval1 = _as_float(axis.get("crval1_hz"), field=f"frequency_axes.{axis_id}.crval1_hz")
        cdelt1 = _as_float(axis.get("cdelt1_hz"), field=f"frequency_axes.{axis_id}.cdelt1_hz")
        crpix1 = _as_float(axis.get("crpix1", 1.0), field=f"frequency_axes.{axis_id}.crpix1")
        if cdelt1 == 0.0:
            raise SpectralRecordingValidationError(f"frequency_axes.{axis_id}.cdelt1_hz must be non-zero")
        if0 = crval1 + (1.0 - crpix1) * cdelt1
        step = cdelt1
        out.update({"crval1_hz": crval1, "cdelt1_hz": cdelt1, "crpix1": crpix1})
    elif mode == "first_center_and_delta":
        if "if_freq_at_full_ch0_hz" in axis:
            if0 = _as_float(axis.get("if_freq_at_full_ch0_hz"), field=f"frequency_axes.{axis_id}.if_freq_at_full_ch0_hz")
        else:
            if0 = _as_float(axis.get("first_channel_center_hz"), field=f"frequency_axes.{axis_id}.first_channel_center_hz")
        if "if_freq_step_hz" in axis:
            step = _as_float(axis.get("if_freq_step_hz"), field=f"frequency_axes.{axis_id}.if_freq_step_hz")
        else:
            step = _as_float(axis.get("channel_spacing_hz"), field=f"frequency_axes.{axis_id}.channel_spacing_hz")
        if step == 0.0:
            raise SpectralRecordingValidationError(f"frequency_axes.{axis_id}.if_freq_step_hz/channel_spacing_hz must be non-zero")
        out.update({
            "first_channel_center_hz": if0,
            "channel_spacing_hz": step,
        })
    else:
        band_start = _as_float(axis.get("band_start_hz"), field=f"frequency_axes.{axis_id}.band_start_hz")
        band_stop = _as_float(axis.get("band_stop_hz"), field=f"frequency_axes.{axis_id}.band_stop_hz")
        origin = str(axis.get("channel_origin", "center")).strip().lower()
        if origin not in {"center", "edge"}:
            raise SpectralRecordingValidationError(
                f"frequency_axes.{axis_id}.channel_origin must be 'center' or 'edge', got {origin!r}"
            )
        if origin == "center":
            if full_nchan == 1:
                if abs(band_stop - band_start) > max(1.0e-6, max(abs(band_start), abs(band_stop)) * 1.0e-12):
                    raise SpectralRecordingValidationError(
                        f"frequency_axes.{axis_id}: nchan=1 with channel_origin='center' requires band_start_hz == band_stop_hz"
                    )
                step0 = 0.0
            else:
                step0 = (band_stop - band_start) / float(full_nchan - 1)
            first_center = band_start
            center_span_hz = abs(band_stop - band_start)
            edge_bandwidth_hz = abs(step0) * full_nchan if full_nchan > 1 else 0.0
        else:
            step0 = (band_stop - band_start) / float(full_nchan)
            first_center = band_start + 0.5 * step0
            center_span_hz = abs(step0) * (full_nchan - 1) if full_nchan > 1 else 0.0
            edge_bandwidth_hz = abs(band_stop - band_start)
        reverse = _as_bool(axis.get("reverse", False), field=f"frequency_axes.{axis_id}.reverse")
        if reverse:
            if0 = first_center + (full_nchan - 1) * step0
            step = -step0
        else:
            if0 = first_center
            step = step0
        if step == 0.0 and full_nchan > 1:
            raise SpectralRecordingValidationError(f"frequency_axes.{axis_id} has zero channel spacing")
        out.update({
            "band_start_hz": band_start,
            "band_stop_hz": band_stop,
            "channel_origin": origin,
            "reverse": bool(reverse),
            "center_span_hz": float(center_span_hz),
            "edge_bandwidth_hz": float(edge_bandwidth_hz),
        })

    if step == 0.0 and full_nchan > 1:
        raise SpectralRecordingValidationError(f"frequency_axes.{axis_id}.if_freq_step_hz must be non-zero")

    order = str(axis.get("channel_order", _normalize_channel_order_from_step(step))).strip().lower()
    if order not in CHANNEL_ORDERS_INCREASING and order not in CHANNEL_ORDERS_DECREASING:
        raise SpectralRecordingValidationError(
            f"frequency_axes.{axis_id}.channel_order={order!r} is not recognized"
        )
    if order in CHANNEL_ORDERS_INCREASING and step < 0:
        raise SpectralRecordingValidationError(
            f"frequency_axes.{axis_id}.channel_order={order!r} requires positive frequency step after reverse handling; got {step}"
        )
    if order in CHANNEL_ORDERS_DECREASING and step > 0:
        raise SpectralRecordingValidationError(
            f"frequency_axes.{axis_id}.channel_order={order!r} requires negative frequency step after reverse handling; got {step}"
        )

    out.update({
        "if_freq_at_full_ch0_hz": float(if0),
        "if_freq_step_hz": float(step),
        "channel_order": order,
    })

    # Preserve converter/WCS metadata and rest-frequency conventions.  restfreq_ghz
    # is accepted as a compatibility alias but canonicalized to restfreq_hz.
    for optional in ("ctype1", "cunit1", "specsys", "veldef", "store_freq_column"):
        if optional in axis:
            out[optional] = axis[optional]
    rest_hz = axis.get("restfreq_hz", axis.get("rest_frequency_hz"))
    rest_ghz = axis.get("restfreq_ghz")
    if rest_hz is not None:
        rest_hz_f = _as_float(rest_hz, field=f"frequency_axes.{axis_id}.restfreq_hz")
        if rest_hz_f <= 0.0 or not math.isfinite(rest_hz_f):
            raise SpectralRecordingValidationError(f"frequency_axes.{axis_id}.restfreq_hz must be positive finite")
        out["restfreq_hz"] = rest_hz_f
        out["rest_frequency_hz"] = rest_hz_f
    if rest_ghz is not None:
        rest_ghz_f = _as_float(rest_ghz, field=f"frequency_axes.{axis_id}.restfreq_ghz")
        rest_from_ghz = rest_ghz_f * 1.0e9
        if "restfreq_hz" in out and abs(float(out["restfreq_hz"]) - rest_from_ghz) > max(1.0e-6, abs(rest_from_ghz) * 1.0e-12):
            raise SpectralRecordingValidationError(f"frequency_axes.{axis_id} has inconsistent restfreq_hz/restfreq_ghz")
        out["restfreq_hz"] = rest_from_ghz
        out["rest_frequency_hz"] = rest_from_ghz

    if "bandwidth_hz" in axis:
        bandwidth = _as_float(axis["bandwidth_hz"], field=f"frequency_axes.{axis_id}.bandwidth_hz")
        if bandwidth <= 0.0 or not math.isfinite(bandwidth):
            raise SpectralRecordingValidationError(f"frequency_axes.{axis_id}.bandwidth_hz must be positive finite")
        expected = abs(float(step)) * full_nchan
        if abs(bandwidth - expected) > max(1.0e-6, expected * 1.0e-9):
            raise SpectralRecordingValidationError(
                f"frequency_axes.{axis_id}.bandwidth_hz={bandwidth} is inconsistent with "
                f"full_nchan*abs(if_freq_step_hz)={expected}"
            )
        out["bandwidth_hz"] = bandwidth
    elif out.get("channel_origin") == "edge" and "edge_bandwidth_hz" in out:
        out["bandwidth_hz"] = float(out["edge_bandwidth_hz"])

    if "sky_freq_at_full_ch0_hz" in axis:
        out["sky_freq_at_full_ch0_hz"] = _as_float(axis["sky_freq_at_full_ch0_hz"], field=f"frequency_axes.{axis_id}.sky_freq_at_full_ch0_hz")
    if "sky_freq_step_hz" in axis:
        out["sky_freq_step_hz"] = _as_float(axis["sky_freq_step_hz"], field=f"frequency_axes.{axis_id}.sky_freq_step_hz")
    return out


def _normalize_frequency_axes(lo_profile: Mapping[str, Any]) -> Dict[str, Dict[str, Any]]:
    axes = _ensure_mapping(lo_profile.get("frequency_axes", {}), field="frequency_axes")
    if not axes:
        raise SpectralRecordingValidationError("frequency_axes must not be empty")
    return {str(axis_id): _normalize_frequency_axis(str(axis_id), raw_axis) for axis_id, raw_axis in axes.items()}

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
        raw_streams = group.get("streams")
        if not isinstance(raw_streams, list) or not raw_streams:
            raise SpectralRecordingValidationError(f"stream_groups.{group_id}.streams must be a non-empty array")
        analysis_default_raw = _ensure_mapping(group.get("analysis_defaults", {}), field=f"stream_groups.{group_id}.analysis_defaults") if group.get("analysis_defaults") is not None else {}
        analysis_defaults: Dict[str, Any] = {}
        for key, value in analysis_default_raw.items():
            analysis_defaults[str(key)] = _as_bool(value, field=f"stream_groups.{group_id}.analysis_defaults.{key}")
        legacy_analysis_key_map = {
            "use_for_convert": "convert",
            "use_for_sunscan": "sunscan_extract",
            "use_for_fit": "sunscan_fit",
        }
        for legacy_key, canonical_key in legacy_analysis_key_map.items():
            if legacy_key in group:
                analysis_defaults.setdefault(
                    canonical_key,
                    _as_bool(group[legacy_key], field=f"stream_groups.{group_id}.{legacy_key}"),
                )
        group_values = {
            k: group[k]
            for k in sorted(group)
            if k not in {"streams", "analysis_defaults", "use_for_convert", "use_for_sunscan", "use_for_fit"}
        }
        if analysis_defaults:
            group_values["analysis_defaults"] = analysis_defaults
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
            item_values = _deepcopy_dict(item)
            # If a stream item supplies any rest-frequency alias, it replaces the
            # group-level rest-frequency default as a set.  Otherwise mixed alias
            # names such as group restfreq_ghz + item rest_frequency_ghz would look
            # like a conflicting double specification rather than an override.
            if any(k in item_values for k in REST_FREQUENCY_KEYS):
                for rest_key in REST_FREQUENCY_KEYS:
                    entry.pop(rest_key, None)
            entry.update(item_values)
            entry["stream_id"] = stream_id
            entry["stream_group_id"] = str(group_id)
            # If no explicit beam is given, default to the boresight/center
            # beam.  This keeps single-beam observations simple while still
            # making the resolved stream truth explicit for downstream tools.
            entry.setdefault("beam_id", "B00")

            missing_resolved = sorted(k for k in RESOLVED_STREAM_REQUIRED_KEYS if k not in entry or entry[k] is None)
            if missing_resolved:
                raise SpectralRecordingValidationError(
                    f"stream {stream_id!r} is missing required resolved stream truth keys: {missing_resolved}. "
                    "Set them either on the stream group or on the stream item."
                )
            lo_chain = _as_str(entry["lo_chain"], field=f"streams.{stream_id}.lo_chain")
            if lo_chain not in lo_chains:
                raise SpectralRecordingValidationError(f"streams.{stream_id}.lo_chain={lo_chain!r} is not defined")
            axis_id = _as_str(entry["frequency_axis_id"], field=f"streams.{stream_id}.frequency_axis_id")
            if axis_id not in frequency_axes:
                raise SpectralRecordingValidationError(f"streams.{stream_id}.frequency_axis_id={axis_id!r} is not defined")

            entry["spectrometer_key"] = _as_str(entry["spectrometer_key"], field=f"streams.{stream_id}.spectrometer_key")
            entry["frontend"] = _as_str(entry["frontend"], field=f"streams.{stream_id}.frontend")
            entry["backend"] = _as_str(entry["backend"], field=f"streams.{stream_id}.backend")
            entry["lo_chain"] = lo_chain
            entry["polariza"] = _as_str(entry["polariza"], field=f"streams.{stream_id}.polariza").upper()
            entry["frequency_axis_id"] = axis_id
            entry["board_id"] = _as_int(entry["board_id"], field=f"streams.{stream_id}.board_id")
            entry["fdnum"] = _as_int(entry["fdnum"], field=f"streams.{stream_id}.fdnum")
            entry["ifnum"] = _as_int(entry["ifnum"], field=f"streams.{stream_id}.ifnum")
            entry["plnum"] = _as_int(entry["plnum"], field=f"streams.{stream_id}.plnum")
            rest_frequency_hz = _rest_frequency_hz_from_stream(entry, field_base=f"streams.{stream_id}")
            if rest_frequency_hz is not None:
                entry["rest_frequency_hz"] = rest_frequency_hz
                # Keep the old name as a compatibility mirror for older code paths
                # that still ask for default_rest_frequency_hz.
                entry["default_rest_frequency_hz"] = rest_frequency_hz
                for alias in (
                    "rest_frequency_ghz",
                    "rest_frequency_mhz",
                    "restfreq_hz",
                    "restfreq_ghz",
                    "restfreq_mhz",
                    "default_rest_frequency_ghz",
                    "default_rest_frequency_mhz",
                ):
                    entry.pop(alias, None)
            # PR8g: use_for_* are analysis-time choices, not observation-time
            # stream-registry facts.  Do not synthesize top-level defaults here.
            if "db_table_name" in entry and "db_stream_name" not in entry:
                # db_table_name is accepted only as a legacy-compatible alias.
                entry["db_stream_name"] = str(entry.pop("db_table_name"))
            entry.setdefault("db_stream_name", stream_id)
            entry.setdefault("raw_input_key", str(entry["spectrometer_key"]))
            entry.setdefault("raw_board_id", int(entry["board_id"]))
            streams[stream_id] = entry
    return streams

def _default_boresight_beam_model() -> Dict[str, Dict[str, Any]]:
    return {
        "B00": {
            "beam_id": "B00",
            "beam_model_version": "default_boresight_v1",
            "model": "legacy",
            "rotation_mode": "none",
            "az_offset_arcsec": 0.0,
            "el_offset_arcsec": 0.0,
            "reference_angle_deg": 0.0,
            "rotation_sign": 1.0,
            "rotation_slope_deg_per_deg": None,
            "dewar_angle_deg": 0.0,
            "pure_rotation_offset_x_el0_arcsec": None,
            "pure_rotation_offset_y_el0_arcsec": None,
            "pure_rotation_sign": None,
        }
    }


def _validate_beam_ids(streams: Mapping[str, Mapping[str, Any]], beam_model: Optional[Mapping[str, Any]]) -> Dict[str, Dict[str, Any]]:
    referenced = {str(s.get("beam_id", "B00")) for s in streams.values()}
    if not beam_model:
        missing_non_default = sorted(referenced - {"B00"})
        if missing_non_default:
            raise SpectralRecordingValidationError(
                "beam_model.toml is required when streams reference non-B00 beam_id(s): "
                f"{missing_non_default}. Without beam_model, only the default boresight beam B00=(0,0) is defined."
            )
        return _default_boresight_beam_model()

    beams = _ensure_mapping(beam_model.get("beams", {}), field="beam_model.beams")
    if not beams:
        missing_non_default = sorted(referenced - {"B00"})
        if missing_non_default:
            raise SpectralRecordingValidationError(
                "beam_model.toml must define [beams.<beam_id>] tables for non-B00 stream beam_id(s): "
                f"{missing_non_default}"
            )
        return _default_boresight_beam_model()
    out: Dict[str, Dict[str, Any]] = {}
    for beam_id, raw_beam in beams.items():
        beam = _ensure_mapping(raw_beam, field=f"beam_model.beams.{beam_id}")
        out[str(beam_id)] = _deepcopy_dict(beam)
        out[str(beam_id)].setdefault("beam_id", str(beam_id))
    missing = sorted(referenced - set(out))
    if missing:
        raise SpectralRecordingValidationError(f"beam_id(s) referenced by streams are missing in beam_model: {missing}")
    return out

def _recording_groups(recording_setup: Optional[Mapping[str, Any]]) -> Dict[str, Dict[str, Any]]:
    if recording_setup is None:
        return {}
    return _ensure_mapping(recording_setup.get("recording_groups", {}), field="recording_groups")


def _linear_sky_axis_for_stream(
    stream: Mapping[str, Any],
    *,
    frequency_axes: Mapping[str, Mapping[str, Any]],
    lo_chains: Mapping[str, Mapping[str, Any]],
) -> Tuple[float, float, int]:
    """Return ``(sky_freq_ch0_hz, sky_freq_step_hz, full_nchan)``.

    Velocity-window resolution only needs the linear WCS, not a materialized
    list of every channel center.  Keeping this as a small helper avoids
    rebuilding 32768-element arrays repeatedly for multi-window setups.
    """

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
        if_sign = float(chain.get("if_frequency_sign", 1.0))
        start = float(chain["signed_lo_sum_hz"]) + if_sign * float(axis["if_freq_at_full_ch0_hz"])
        step = if_sign * float(axis["if_freq_step_hz"])
    return float(start), float(step), int(full_nchan)


def _frequency_array_for_stream(
    stream: Mapping[str, Any],
    *,
    frequency_axes: Mapping[str, Mapping[str, Any]],
    lo_chains: Mapping[str, Mapping[str, Any]],
) -> List[float]:
    start, step, full_nchan = _linear_sky_axis_for_stream(
        stream,
        frequency_axes=frequency_axes,
        lo_chains=lo_chains,
    )
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


def _relativistic_doppler_factor_from_kms(v_kms: float) -> float:
    beta = float(v_kms) / C_KM_PER_S
    if not math.isfinite(beta) or abs(beta) >= 1.0:
        raise SpectralRecordingValidationError(f"invalid LSRK correction velocity: {v_kms!r} km/s")
    return math.sqrt((1.0 + beta) / (1.0 - beta))


def _lsrk_correction_kms_from_context(context: Optional[Mapping[str, Any]]) -> Optional[float]:
    """Return topocentric-to-LSRK correction projected to the reference direction.

    The returned value follows the same convention as the converter:
    if the topocentric frequency is ``freq_topo``, the LSRK-frame frequency is
    ``freq_lsrk = freq_topo / doppler_factor(v_corr_kms)``.
    Therefore, when resolving a desired LSRK velocity window against a
    topocentric spectrometer axis, the requested LSRK frequency interval is
    multiplied by ``doppler_factor(v_corr_kms)`` before channel selection.
    """

    if not context:
        return None
    try:
        ra_deg = float(context["ra_deg"])
        dec_deg = float(context["dec_deg"])
        reference_time_utc = str(context["reference_time_utc"])
        lat_deg = float(context["site_lat_deg"])
        lon_deg = float(context["site_lon_deg"])
        elev_m = float(context.get("site_elev_m", 0.0))
    except Exception:
        return None
    try:
        from astropy.time import Time  # type: ignore
        from astropy.coordinates import EarthLocation, SkyCoord  # type: ignore
        import astropy.coordinates as coord  # type: ignore
        from astropy import units as u  # type: ignore
    except Exception as exc:
        raise SpectralRecordingValidationError(
            "velocity_frame='LSRK' requires astropy to compute the LSRK correction"
        ) from exc

    try:
        loc = EarthLocation(lat=lat_deg * u.deg, lon=lon_deg * u.deg, height=elev_m * u.m)
        tobs = Time(reference_time_utc, scale="utc")
        target = SkyCoord(ra_deg * u.deg, dec_deg * u.deg, frame="icrs", obstime=tobs, location=loc)
        vobs = SkyCoord(loc.get_gcrs(tobs)).transform_to(coord.LSRK()).velocity
        ra_rad = target.icrs.ra.to_value(u.rad)
        dec_rad = target.icrs.dec.to_value(u.rad)
        v_proj = (
            vobs.d_x.to_value(u.km / u.s) * math.cos(dec_rad) * math.cos(ra_rad)
            + vobs.d_y.to_value(u.km / u.s) * math.cos(dec_rad) * math.sin(ra_rad)
            + vobs.d_z.to_value(u.km / u.s) * math.sin(dec_rad)
        )
        return float(v_proj)
    except Exception as exc:
        raise SpectralRecordingValidationError(
            "failed to compute LSRK correction from velocity_reference_context"
        ) from exc


def resolve_velocity_window_to_channels(
    *,
    rest_frequency_hz: float,
    velocity_definition: str,
    vmin_kms: float,
    vmax_kms: float,
    margin_kms: float,
    frequency_hz: Sequence[float],
    clip_policy: str = "strict",
    requested_velocity_frame: str = "LSRK",
    vlsrk_correction_kms: Optional[float] = None,
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
    frame_norm = str(requested_velocity_frame or "LSRK").strip().upper()
    correction_applied = False
    if frame_norm in {"LSRK", "VLSRK"} and vlsrk_correction_kms is not None:
        k_corr = _relativistic_doppler_factor_from_kms(float(vlsrk_correction_kms))
        # Convert the requested LSRK frequency interval back to the topocentric
        # spectrometer frame before selecting channels.
        f1 *= k_corr
        f2 *= k_corr
        correction_applied = True
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
        "requested_velocity_frame": str(requested_velocity_frame),
        "vlsrk_correction_applied": bool(correction_applied),
        "vlsrk_correction_kms": None if vlsrk_correction_kms is None else float(vlsrk_correction_kms),
    }


def _channel_range_for_linear_frequency_axis(
    *,
    f_low: float,
    f_high: float,
    start_hz: float,
    step_hz: float,
    nchan: int,
    clip_policy: str = "strict",
) -> Tuple[int, int, bool, List[str]]:
    """Return inclusive/exclusive channel range for a linear channel-center axis.

    Channels are selected when their *center* frequency lies within
    ``[f_low, f_high]``.  The result is sorted by channel index and works for both
    increasing and decreasing frequency axes without materializing the full
    frequency array.
    """

    if nchan <= 0:
        raise SpectralRecordingValidationError("linear frequency axis nchan must be positive")
    if nchan == 1:
        freq = float(start_hz)
        if min(f_low, f_high) <= freq <= max(f_low, f_high):
            return 0, 1, False, []
        axis_low = axis_high = freq
        outside = max(f_low, f_high) < axis_low or min(f_low, f_high) > axis_high
        if clip_policy == "strict" or outside:
            raise SpectralRecordingValidationError(
                "velocity window does not overlap the available frequency axis "
                f"({f_low}..{f_high} Hz vs axis {axis_low}..{axis_high} Hz)"
            )
        raise SpectralRecordingValidationError("velocity window could not be resolved to channels")
    if step_hz == 0.0:
        raise SpectralRecordingValidationError("linear frequency axis step must be non-zero")

    f_low = float(min(f_low, f_high))
    f_high = float(max(f_low, f_high))
    start = float(start_hz)
    step = float(step_hz)
    eps = 1.0e-12

    if step > 0.0:
        raw_i_min = (f_low - start) / step
        raw_i_max = (f_high - start) / step
    else:
        raw_i_min = (f_high - start) / step
        raw_i_max = (f_low - start) / step

    i_min = math.ceil(raw_i_min - eps)
    i_max = math.floor(raw_i_max + eps)

    axis_end = start + (nchan - 1) * step
    axis_low = min(start, axis_end)
    axis_high = max(start, axis_end)
    outside = f_high < axis_low or f_low > axis_high
    if i_max < 0 or i_min > nchan - 1 or i_min > i_max:
        if clip_policy == "strict" or outside:
            raise SpectralRecordingValidationError(
                "velocity window does not overlap the available frequency axis "
                f"({f_low}..{f_high} Hz vs axis {axis_low}..{axis_high} Hz)"
            )
        raise SpectralRecordingValidationError("velocity window could not be resolved to channels")

    clipped = False
    warnings: List[str] = []
    if f_low < axis_low or f_high > axis_high:
        if clip_policy == "strict":
            raise SpectralRecordingValidationError("velocity window extends beyond the available frequency axis")
        clipped = True
        warnings.append("velocity window clipped to available frequency axis")

    ch_start = max(0, int(i_min))
    ch_stop = min(nchan, int(i_max) + 1)
    if ch_start >= ch_stop:
        raise SpectralRecordingValidationError("velocity window could not be resolved to channels")
    return ch_start, ch_stop, clipped, warnings


def resolve_velocity_window_to_channels_linear(
    *,
    rest_frequency_hz: float,
    velocity_definition: str,
    vmin_kms: float,
    vmax_kms: float,
    margin_kms: float,
    start_hz: float,
    step_hz: float,
    nchan: int,
    clip_policy: str = "strict",
    requested_velocity_frame: str = "LSRK",
    vlsrk_correction_kms: Optional[float] = None,
) -> Dict[str, Any]:
    """Resolve a velocity window on a linear WCS without allocating an array."""

    if vmax_kms < vmin_kms:
        raise SpectralRecordingValidationError("vmax_kms must be >= vmin_kms")
    if margin_kms < 0:
        raise SpectralRecordingValidationError("margin_kms must be >= 0")
    effective_vmin = vmin_kms - margin_kms
    effective_vmax = vmax_kms + margin_kms
    f1 = velocity_to_frequency_hz(rest_frequency_hz, effective_vmin, velocity_definition)
    f2 = velocity_to_frequency_hz(rest_frequency_hz, effective_vmax, velocity_definition)
    frame_norm = str(requested_velocity_frame or "LSRK").strip().upper()
    correction_applied = False
    if frame_norm in {"LSRK", "VLSRK"} and vlsrk_correction_kms is not None:
        k_corr = _relativistic_doppler_factor_from_kms(float(vlsrk_correction_kms))
        f1 *= k_corr
        f2 *= k_corr
        correction_applied = True
    f_low = min(f1, f2)
    f_high = max(f1, f2)
    ch_start, ch_stop, clipped, warnings = _channel_range_for_linear_frequency_axis(
        f_low=f_low,
        f_high=f_high,
        start_hz=float(start_hz),
        step_hz=float(step_hz),
        nchan=int(nchan),
        clip_policy=clip_policy,
    )
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
        "requested_velocity_frame": str(requested_velocity_frame),
        "vlsrk_correction_applied": bool(correction_applied),
        "vlsrk_correction_kms": None if vlsrk_correction_kms is None else float(vlsrk_correction_kms),
    }


def _resolve_velocity_window_record(
    *,
    stream: Mapping[str, Any],
    window: Mapping[str, Any],
    group_id: str,
    window_index: int,
    frequency_axes: Mapping[str, Mapping[str, Any]],
    lo_chains: Mapping[str, Mapping[str, Any]],
    setup_override_policy: str,
    default_velocity_definition: str = "radio",
    default_vmin_kms: Optional[float] = None,
    default_vmax_kms: Optional[float] = None,
    default_margin_kms: float = 0.0,
    default_velocity_frame: str = "LSRK",
    velocity_reference_context: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    rest_frequency_hz = _rest_frequency_hz_from_window(
        window,
        field_base=f"recording_groups.{group_id}.windows[{window_index}]",
    )
    if rest_frequency_hz is None:
        rest_frequency_hz = _rest_frequency_hz_from_stream(
            stream,
            field_base=f"streams.{stream.get('stream_id', '')}",
        )
    if rest_frequency_hz is None:
        raise SpectralRecordingValidationError(
            f"recording_groups.{group_id}.windows[{window_index}] needs rest_frequency_hz/ghz/mhz "
            "or the source stream must define rest_frequency_hz"
        )
    velocity_definition = str(window.get("velocity_definition", default_velocity_definition))
    vmin_value = window.get("vmin_kms", default_vmin_kms)
    vmax_value = window.get("vmax_kms", default_vmax_kms)
    if vmin_value is None or vmax_value is None:
        raise SpectralRecordingValidationError(
            f"recording_groups.{group_id}.windows[{window_index}] needs vmin_kms and vmax_kms"
        )
    vmin_kms = _as_float(vmin_value, field=f"recording_groups.{group_id}.windows[{window_index}].vmin_kms")
    vmax_kms = _as_float(vmax_value, field=f"recording_groups.{group_id}.windows[{window_index}].vmax_kms")
    margin_kms = _as_float(
        window.get("margin_kms", default_margin_kms),
        field=f"recording_groups.{group_id}.windows[{window_index}].margin_kms",
    )
    clip_policy = "strict" if setup_override_policy == "strict" else "clip"
    axis_start_hz, axis_step_hz, axis_nchan = _linear_sky_axis_for_stream(
        stream,
        frequency_axes=frequency_axes,
        lo_chains=lo_chains,
    )
    requested_velocity_frame = str(window.get("velocity_frame", default_velocity_frame))
    frame_norm = requested_velocity_frame.strip().upper()
    vlsrk_correction_kms = None
    resolver_frame = VELOCITY_RESOLVER_FRAME
    resolver_version = VELOCITY_RESOLVER_VERSION
    if frame_norm in {"LSRK", "VLSRK"}:
        vlsrk_correction_kms = _lsrk_correction_kms_from_context(velocity_reference_context)
        if vlsrk_correction_kms is None:
            raise SpectralRecordingValidationError(
                "velocity_frame='LSRK' requires a velocity_reference_context with "
                "ra_deg, dec_deg, reference_time_utc, site_lat_deg, and site_lon_deg. "
                "Specify velocity_reference_* parameters or use velocity_frame='TOPOCENTRIC' "
                "for an explicitly topocentric approximate window."
            )
        resolver_frame = VLSRK_RESOLVER_FRAME
        resolver_version = VLSRK_RESOLVER_VERSION
    resolved = resolve_velocity_window_to_channels_linear(
        rest_frequency_hz=rest_frequency_hz,
        velocity_definition=velocity_definition,
        vmin_kms=vmin_kms,
        vmax_kms=vmax_kms,
        margin_kms=margin_kms,
        start_hz=axis_start_hz,
        step_hz=axis_step_hz,
        nchan=axis_nchan,
        clip_policy=clip_policy,
        requested_velocity_frame=requested_velocity_frame,
        vlsrk_correction_kms=vlsrk_correction_kms,
    )
    resolver_warnings = list(resolved.get("resolver_warnings", []))
    out = dict(resolved)
    out["resolver_warnings"] = resolver_warnings
    out.update(
        {
            "kind": "velocity",
            "window_id": str(window.get("window_id", "")),
            "line_name": str(window.get("line_name", window.get("window_id", ""))),
            "rest_frequency_hz": float(rest_frequency_hz),
            "requested_velocity_frame": requested_velocity_frame,
            "velocity_frame": resolver_frame,
            "velocity_resolver_version": resolver_version,
            "velocity_definition": velocity_definition,
            "vlsrk_correction_kms": None if vlsrk_correction_kms is None else float(vlsrk_correction_kms),
            "velocity_reference_context": {} if velocity_reference_context is None else dict(velocity_reference_context),
        }
    )
    return out


def _single_channel_window_record(*, ch_start: int, ch_stop: int, window_id: str = "", line_name: str = "") -> Dict[str, Any]:
    return {
        "kind": "channel",
        "window_id": window_id,
        "line_name": line_name,
        "computed_ch_start": int(ch_start),
        "computed_ch_stop": int(ch_stop),
    }


def _resolve_recording_for_stream(
    stream: Mapping[str, Any],
    recording_group: Optional[Mapping[str, Any]],
    *,
    frequency_axes: Mapping[str, Mapping[str, Any]],
    lo_chains: Mapping[str, Mapping[str, Any]],
    setup_override_policy: str,
    velocity_reference_context: Optional[Mapping[str, Any]] = None,
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
    if mode == "tp" and policy == "multi_window":
        raise SpectralRecordingValidationError(f"recording_groups.{group_id}: multi_window is only valid for spectrum mode")

    out: Dict[str, Any] = {
        "recording_group_id": group_id,
        "recording_mode": mode,
        "saved_window_policy": policy,
        "full_nchan": full_nchan,
        "input_data_nchan": full_nchan,
        "recording_table_kind": "tp" if mode == "tp" else "spectral",
    }

    if policy == "multi_window":
        windows = _window_list_from_recording_group(rec, group_id=group_id)
        if not windows:
            raise SpectralRecordingValidationError(f"recording_groups.{group_id}: multi_window requires windows")
        default_velocity_definition = str(rec.get("velocity_definition", "radio"))
        default_vmin = rec.get("vmin_kms")
        default_vmax = rec.get("vmax_kms")
        default_margin = _as_float(rec.get("margin_kms", 0.0), field=f"recording_groups.{group_id}.margin_kms")
        default_velocity_frame = str(rec.get("velocity_frame", "LSRK"))
        products: List[Dict[str, Any]] = []
        for i, window in enumerate(windows):
            w = dict(window)
            window_id = _safe_window_id(w.get("window_id"), field=f"recording_groups.{group_id}.windows[{i}].window_id")
            if "saved_ch_start" in w or "saved_ch_stop" in w:
                ch_start = _as_int(w.get("saved_ch_start"), field=f"recording_groups.{group_id}.windows[{i}].saved_ch_start")
                ch_stop = _as_int(w.get("saved_ch_stop"), field=f"recording_groups.{group_id}.windows[{i}].saved_ch_stop")
                if ch_start < 0 or ch_stop > full_nchan or ch_start >= ch_stop:
                    raise SpectralRecordingValidationError(
                        f"recording_groups.{group_id}.windows[{i}]: invalid channel slice "
                        f"[{ch_start}:{ch_stop}] for full_nchan={full_nchan}"
                    )
                computed = _single_channel_window_record(
                    ch_start=ch_start,
                    ch_stop=ch_stop,
                    window_id=window_id,
                    line_name=str(w.get("line_name", window_id)),
                )
                rest = _rest_frequency_hz_from_window(w, field_base=f"recording_groups.{group_id}.windows[{i}]")
                if rest is not None:
                    computed["rest_frequency_hz"] = float(rest)
            else:
                computed = _resolve_velocity_window_record(
                    stream=stream,
                    window=w,
                    group_id=group_id,
                    window_index=i,
                    frequency_axes=frequency_axes,
                    lo_chains=lo_chains,
                    setup_override_policy=setup_override_policy,
                    default_velocity_definition=default_velocity_definition,
                    default_vmin_kms=None if default_vmin is None else _as_float(default_vmin, field=f"recording_groups.{group_id}.vmin_kms"),
                    default_vmax_kms=None if default_vmax is None else _as_float(default_vmax, field=f"recording_groups.{group_id}.vmax_kms"),
                    default_margin_kms=default_margin,
                    default_velocity_frame=default_velocity_frame,
                    velocity_reference_context=velocity_reference_context,
                )
            ch_start = int(computed["computed_ch_start"])
            ch_stop = int(computed["computed_ch_stop"])
            if ch_start < 0 or ch_stop > full_nchan or ch_start >= ch_stop:
                raise SpectralRecordingValidationError(
                    f"recording_groups.{group_id}.windows[{i}]: invalid computed channel slice "
                    f"[{ch_start}:{ch_stop}] for full_nchan={full_nchan}"
                )
            source_db_stream_name = str(stream.get("db_stream_name", stream.get("stream_id", "")))
            recorded_stream_id = str(w.get("recorded_stream_id", _recorded_stream_id(str(stream["stream_id"]), window_id)))
            recorded_db_stream_name = str(w.get("recorded_db_stream_name", _recorded_db_stream_name(source_db_stream_name, window_id)))
            product = {
                "window_id": window_id,
                "line_name": str(w.get("line_name", window_id)),
                "recorded_stream_id": recorded_stream_id,
                "recorded_db_stream_name": recorded_db_stream_name,
                "source_stream_id": str(stream["stream_id"]),
                "source_db_stream_name": source_db_stream_name,
                "recording_mode": mode,
                "recording_table_kind": "spectral",
                "saved_window_policy": policy,
                "full_nchan": int(full_nchan),
                "input_data_nchan": int(full_nchan),
                "saved_ch_start": ch_start,
                "saved_ch_stop": ch_stop,
                "saved_nchan": int(ch_stop - ch_start),
                "computed_windows": [computed],
                "computed_windows_json": json.dumps([computed], sort_keys=True, separators=(",", ":")),
            }
            _apply_recorded_window_metadata_overrides(
                product,
                w,
                group_id=group_id,
                window_index=i,
            )
            product_rest = computed.get("rest_frequency_hz", stream.get("rest_frequency_hz", stream.get("default_rest_frequency_hz")))
            if product_rest is not None:
                product["rest_frequency_hz"] = float(product_rest)
            products.append(product)
        out["recorded_products"] = products
        # The source stream itself is not a saved product in multi_window mode.
        # For callers that still inspect the source recording entry, expose the
        # minimal envelope as diagnostic metadata only.
        out["saved_ch_start"] = min(int(p["saved_ch_start"]) for p in products)
        out["saved_ch_stop"] = max(int(p["saved_ch_stop"]) for p in products)
        out["saved_nchan"] = int(out["saved_ch_stop"] - out["saved_ch_start"])
        out["computed_windows"] = [cw for p in products for cw in p["computed_windows"]]
        out["computed_windows_json"] = json.dumps(out["computed_windows"], sort_keys=True, separators=(",", ":"))
        return out

    # All non-multi policies produce one saved product per source stream.
    if policy == "full":
        ch_start = 0
        ch_stop = full_nchan
        computed_windows: List[Dict[str, Any]] = [
            _single_channel_window_record(ch_start=ch_start, ch_stop=ch_stop)
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
            _single_channel_window_record(ch_start=ch_start, ch_stop=ch_stop)
        ]
    else:  # contiguous_envelope
        explicit_windows = _window_list_from_recording_group(rec, group_id=group_id)
        if explicit_windows:
            resolved_windows: List[Dict[str, Any]] = []
            default_velocity_definition = str(rec.get("velocity_definition", "radio"))
            default_vmin = rec.get("vmin_kms")
            default_vmax = rec.get("vmax_kms")
            default_margin = _as_float(rec.get("margin_kms", 0.0), field=f"recording_groups.{group_id}.margin_kms")
            default_velocity_frame = str(rec.get("velocity_frame", "LSRK"))
            for i, window in enumerate(explicit_windows):
                resolved_windows.append(
                    _resolve_velocity_window_record(
                        stream=stream,
                        window=window,
                        group_id=group_id,
                        window_index=i,
                        frequency_axes=frequency_axes,
                        lo_chains=lo_chains,
                        setup_override_policy=setup_override_policy,
                        default_velocity_definition=default_velocity_definition,
                        default_vmin_kms=None if default_vmin is None else _as_float(default_vmin, field=f"recording_groups.{group_id}.vmin_kms"),
                        default_vmax_kms=None if default_vmax is None else _as_float(default_vmax, field=f"recording_groups.{group_id}.vmax_kms"),
                        default_margin_kms=default_margin,
                        default_velocity_frame=default_velocity_frame,
                        velocity_reference_context=velocity_reference_context,
                    )
                )
            ch_start = min(int(w["computed_ch_start"]) for w in resolved_windows)
            ch_stop = max(int(w["computed_ch_stop"]) for w in resolved_windows)
            computed_windows = resolved_windows + [
                {
                    "kind": "contiguous_envelope",
                    "computed_ch_start": int(ch_start),
                    "computed_ch_stop": int(ch_stop),
                    "source_window_ids": [str(w.get("window_id", "")) for w in resolved_windows],
                }
            ]
        else:
            single_window = dict(rec)
            single_window["window_id"] = str(rec.get("window_id", rec.get("line_name", "velocity_window")))
            resolved = _resolve_velocity_window_record(
                stream=stream,
                window=single_window,
                group_id=group_id,
                window_index=0,
                frequency_axes=frequency_axes,
                lo_chains=lo_chains,
                setup_override_policy=setup_override_policy,
                default_velocity_definition=str(rec.get("velocity_definition", "radio")),
                default_vmin_kms=_as_float(rec.get("vmin_kms"), field=f"recording_groups.{group_id}.vmin_kms"),
                default_vmax_kms=_as_float(rec.get("vmax_kms"), field=f"recording_groups.{group_id}.vmax_kms"),
                default_margin_kms=_as_float(rec.get("margin_kms", 0.0), field=f"recording_groups.{group_id}.margin_kms"),
                default_velocity_frame=str(rec.get("velocity_frame", "LSRK")),
                velocity_reference_context=velocity_reference_context,
            )
            ch_start = int(resolved["computed_ch_start"])
            ch_stop = int(resolved["computed_ch_stop"])
            computed_windows = [resolved]

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
    # Promote single-window metadata to the stream top level.  This keeps
    # single-line contiguous_envelope streams such as board1/board3 as explicit
    # as multi_window products, while leaving true multi-line envelopes
    # unambiguous.
    source_windows = [w for w in computed_windows if str(w.get("kind", "")) != "contiguous_envelope"]
    if len(source_windows) == 1:
        w0 = source_windows[0]
        if w0.get("window_id") not in (None, ""):
            out["window_id"] = str(w0.get("window_id"))
        if w0.get("line_name") not in (None, ""):
            out["line_name"] = str(w0.get("line_name"))
        if w0.get("rest_frequency_hz") is not None:
            out["rest_frequency_hz"] = float(w0.get("rest_frequency_hz"))
    elif len(source_windows) > 1:
        out["source_window_ids"] = [str(w.get("window_id", "")) for w in source_windows]
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
    velocity_reference_context: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Dict[str, Any]]:
    groups = _recording_groups(recording_setup)
    defaults = (
        _ensure_mapping(recording_setup.get("defaults", {}), field="defaults")
        if recording_setup is not None
        else {}
    )
    assignments: Dict[str, Dict[str, Any]] = {}
    for group_id, raw_group in groups.items():
        group = dict(defaults)
        group.update(_ensure_mapping(raw_group, field=f"recording_groups.{group_id}"))
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
            velocity_reference_context=velocity_reference_context,
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
        is_recorded_product = bool(stream.get("source_stream_id")) or bool(stream.get("recorded_stream_id"))
        if raw_key in seen_raw and not is_recorded_product:
            raise SpectralRecordingValidationError(
                f"Duplicate raw input key {raw_key}: {seen_raw[raw_key]} and {stream_id}"
            )
        seen_raw.setdefault(raw_key, stream_id)
        db_path = str(stream["db_table_path"])
        if db_path in seen_db:
            raise SpectralRecordingValidationError(
                f"Duplicate db_table_path {db_path!r}: {seen_db[db_path]} and {stream_id}"
            )
        seen_db[db_path] = stream_id
        sdfits_key = (int(stream["fdnum"]), int(stream["ifnum"]), int(stream["plnum"]), str(stream["polariza"]))
        if sdfits_key in seen_sdfits and not is_recorded_product:
            raise SpectralRecordingValidationError(
                f"Duplicate SDFITS key {sdfits_key}: {seen_sdfits[sdfits_key]} and {stream_id}"
            )
        seen_sdfits.setdefault(sdfits_key, stream_id)


def resolve_spectral_recording_setup(
    *,
    lo_profile: Mapping[str, Any],
    recording_setup: Optional[Mapping[str, Any]],
    beam_model: Optional[Mapping[str, Any]],
    setup_id: str,
    input_paths: Optional[Mapping[str, os.PathLike[str] | str]] = None,
    setup_override_policy: str = "strict",
    created_utc: Optional[str] = None,
    velocity_reference_context: Optional[Mapping[str, Any]] = None,
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
        velocity_reference_context=velocity_reference_context,
    )

    source_streams: Dict[str, Dict[str, Any]] = {}
    normalized_streams: Dict[str, Dict[str, Any]] = {}
    recorded_streams: Dict[str, Dict[str, Any]] = {}
    for stream_id in sorted(streams):
        source = _deepcopy_dict(streams[stream_id])
        if "db_stream_name" not in source:
            source["db_stream_name"] = str(stream_id)
        source["raw_input_key"] = str(source.get("raw_input_key", source["spectrometer_key"]))
        source["raw_board_id"] = int(source.get("raw_board_id", source["board_id"]))
        source_streams[stream_id] = _sorted_dict(source)

        rec = recording[stream_id]
        if "recorded_products" in rec:
            for product in rec["recorded_products"]:
                stream = _deepcopy_dict(source)
                recorded_stream_id = str(product["recorded_stream_id"])
                recorded_db_stream_name = str(product["recorded_db_stream_name"])
                if product.get("rest_frequency_hz") is not None:
                    for rest_key in REST_FREQUENCY_KEYS:
                        stream.pop(rest_key, None)
                stream.update(product)
                stream["stream_id"] = recorded_stream_id
                stream["db_stream_name"] = recorded_db_stream_name
                stream["recorded_db_stream_name"] = recorded_db_stream_name
                stream["recording_table_kind"] = "spectral"
                stream["raw_input_key"] = str(source.get("raw_input_key", source["spectrometer_key"]))
                stream["raw_board_id"] = int(source.get("raw_board_id", source["board_id"]))
                stream["db_table_path"] = str(
                    product.get(
                        "db_table_path",
                        _db_table_path_for_recorded_product(source, recorded_db_stream_name, mode=str(product.get("recording_mode", "spectrum"))),
                    )
                )
                normalized_streams[recorded_stream_id] = _sorted_dict(stream)
                recorded_streams[recorded_stream_id] = _sorted_dict(stream)
        else:
            stream = _deepcopy_dict(source)
            stream.update(rec)
            stream["recording_table_kind"] = "tp" if stream["recording_mode"] == "tp" else "spectral"
            if "db_table_path" not in stream:
                stream["db_table_path"] = _default_db_table_path(
                    str(stream["spectrometer_key"]), int(stream["board_id"]), kind=str(stream["recording_table_kind"])
                )
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
        "source_streams": source_streams,
        "streams": normalized_streams,
    }
    if recorded_streams:
        snapshot["recorded_streams"] = recorded_streams
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
    beam_model_path: Optional[os.PathLike[str] | str],
    setup_id: str,
    setup_override_policy: str = "strict",
    velocity_reference_context: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    lo_path = Path(lo_profile_path)
    rec_path = Path(recording_window_setup_path) if recording_window_setup_path else None
    beam_path = Path(beam_model_path) if beam_model_path else None
    lo_profile = read_toml(lo_path)
    recording_setup = read_toml(rec_path) if rec_path else None
    beam_model = read_toml(beam_path) if beam_path else None
    input_paths: Dict[str, Path] = {"lo_profile": lo_path}
    if beam_path:
        input_paths["beam_model"] = beam_path
    if rec_path:
        input_paths["recording_window_setup"] = rec_path
    return resolve_spectral_recording_setup(
        lo_profile=lo_profile,
        recording_setup=recording_setup,
        beam_model=beam_model,
        setup_id=setup_id,
        input_paths=input_paths,
        setup_override_policy=setup_override_policy,
        velocity_reference_context=velocity_reference_context,
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
    parser.add_argument("--beam-model", default=None, help="Path to beam_model.toml. If omitted, only B00=(0,0) is available.")
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
