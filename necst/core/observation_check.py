"""Static observation-file check and dry-run planning helpers.

The functions in this module are intentionally read-only.  They parse and
inspect observation parameter files, but they never acquire NECST privilege,
move the antenna, move the chopper, touch recorder/gate state, or apply SG
settings.  They are shared by CLI-like operator actions and the web Operator
Console so that Check / Dry run semantics stay consistent.
"""

from __future__ import annotations

import builtins
import math
import os
import re
import tomllib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, Mapping, Optional


class ObservationCheckError(ValueError):
    """Raised when an observation file cannot pass static validation."""


@dataclass(frozen=True)
class ObservationCheckReport:
    """Machine-readable report returned by Check / Dry run."""

    ok: bool
    mode: str
    file: str
    channel: Optional[int]
    summary: str
    data: Dict[str, Any] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "ok": self.ok,
            "mode": self.mode,
            "file": self.file,
            "channel": self.channel,
            "summary": self.summary,
            "warnings": list(self.warnings),
            "errors": list(self.errors),
            **dict(self.data),
        }


_ALLOWED_MODES = {
    "otf": "OTF",
    "psw": "PSW",
    "grid": "Grid",
    "radio_pointing": "Radio Pointing",
    "radio-pointing": "Radio Pointing",
    "radiopointing": "Radio Pointing",
}

_V16_DEFAULTS: Dict[str, Any] = {
    "cos_correction": True,
    "use_scan_block": True,
    "sg_preflight_policy": "verify_only",
    "sg_preflight_scope": "active_lo_chains",
}

_REFERENCE_FILE_KEYS = (
    "lo_profile",
    "lo_profile_path",
    "recording_window_setup",
    "recording_window_setup_path",
    "beam_model",
    "beam_model_path",
    "analysis_stream_selection",
    "analysis_stream_selection_path",
    "converter_analysis",
    "converter_analysis_path",
    "recording_window",
    "recording_window_path",
    "pointing_param",
    "pointing_param_path",
)

_DURATION_RE = re.compile(r"^\s*([+-]?\d+(?:\.\d*)?|[+-]?\.\d+)\s*([a-zA-Z]+)?\s*$")
_ANGLE_RE = re.compile(r"^\s*([+-]?\d+(?:\.\d*)?|[+-]?\.\d+)\s*([a-zA-Z]+)?\s*$")


def normalize_observation_mode(mode: Any) -> str:
    normalized = str(mode or "").strip().lower().replace(" ", "_")
    if normalized not in _ALLOWED_MODES:
        raise ObservationCheckError(
            "observation mode must be OTF, PSW, Grid, or Radio Pointing"
        )
    return normalized


def optional_positive_int(value: Any, *, name: str) -> Optional[int]:
    if value in (None, ""):
        return None
    try:
        number = int(value)
    except Exception as exc:
        raise ObservationCheckError(f"{name} must be an integer") from exc
    if number <= 0:
        raise ObservationCheckError(f"{name} must be positive")
    return number


def validate_obs_file_path(file_path: Any, *, require_exists: bool) -> Path:
    text = str(file_path or "").strip()
    if not text:
        raise ObservationCheckError("obs file path/name is empty")
    path = Path(text).expanduser()
    if path.suffix.lower() not in {".obs", ".toml"}:
        raise ObservationCheckError("obs file extension must be .obs or .toml")
    if require_exists:
        if not path.exists():
            raise ObservationCheckError(f"obs file does not exist on this computer: {path}")
        if not path.is_file():
            raise ObservationCheckError(f"obs file is not a regular file: {path}")
        if not os.access(path, os.R_OK):
            raise ObservationCheckError(f"obs file is not readable: {path}")
    return path


def _read_toml(path: Path) -> Dict[str, Any]:
    try:
        with path.open("rb") as fh:
            data = tomllib.load(fh)
    except tomllib.TOMLDecodeError as exc:
        raise ObservationCheckError(f"obs file TOML parse failed: {exc}") from exc
    except OSError as exc:
        raise ObservationCheckError(f"obs file cannot be read: {exc}") from exc
    if not isinstance(data, dict):
        raise ObservationCheckError("obs file did not parse to a TOML table")
    return data


def _mapping(value: Any) -> Dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _params_from_toml(data: Mapping[str, Any]) -> Dict[str, Any]:
    params = _mapping(data.get("parameters"))
    # Older observation files often put observation/scanning keys directly in
    # dedicated sections rather than [parameters].  For static checking we keep
    # [parameters] as the canonical default-expansion target, but later summary
    # functions look at all sections.
    return params


def _apply_v16_defaults(params: Mapping[str, Any]) -> tuple[Dict[str, Any], Dict[str, bool]]:
    expanded = dict(params)
    explicit: Dict[str, bool] = {}
    for key, value in _V16_DEFAULTS.items():
        explicit[key] = key in expanded
        expanded.setdefault(key, value)
    return expanded, explicit


def _format_default_summary(explicit: Mapping[str, bool]) -> str:
    parts = []
    for key, value in _V16_DEFAULTS.items():
        source = "explicit" if explicit.get(key) else "default"
        parts.append(f"{key}={value!r} ({source})")
    return ", ".join(parts)


def _iter_reference_files(params: Mapping[str, Any]) -> Iterable[tuple[str, str]]:
    for key in _REFERENCE_FILE_KEYS:
        value = params.get(key)
        if value in (None, ""):
            continue
        if isinstance(value, (str, os.PathLike)):
            yield key, os.fspath(value)


def _resolve_reference_path(obs_path: Path, ref: str) -> Path:
    p = Path(ref).expanduser()
    if not p.is_absolute():
        p = obs_path.parent / p
    return p


def _check_reference_files(obs_path: Path, params: Mapping[str, Any]) -> list[Dict[str, Any]]:
    refs = []
    for key, value in _iter_reference_files(params):
        resolved = _resolve_reference_path(obs_path, value)
        exists = resolved.exists()
        readable = exists and resolved.is_file() and os.access(resolved, os.R_OK)
        refs.append(
            {
                "key": key,
                "value": value,
                "path": str(resolved),
                "exists": bool(exists),
                "readable": bool(readable),
            }
        )
    return refs


def _parse_duration_sec(value: Any) -> Optional[float]:
    if value in (None, "", {}):
        return None
    if isinstance(value, (int, float)) and math.isfinite(float(value)):
        return float(value)
    match = _DURATION_RE.match(str(value))
    if not match:
        return None
    number = float(match.group(1))
    unit = (match.group(2) or "s").strip().lower()
    if unit in {"s", "sec", "secs", "second", "seconds"}:
        return number
    if unit in {"m", "min", "mins", "minute", "minutes"}:
        return number * 60.0
    if unit in {"h", "hr", "hour", "hours"}:
        return number * 3600.0
    return None


def _parse_angle_arcsec(value: Any) -> Optional[float]:
    if value in (None, "", {}):
        return None
    if isinstance(value, (int, float)) and math.isfinite(float(value)):
        # Bare numeric values in observation TOML are rare; treat as degrees
        # rather than arcsec because neclib coordinate specs generally use
        # explicit strings for angular units.
        return float(value) * 3600.0
    match = _ANGLE_RE.match(str(value))
    if not match:
        return None
    number = float(match.group(1))
    unit = (match.group(2) or "deg").strip().lower()
    if unit in {"arcsec", "asec", "as", "second", "seconds"}:
        return number
    if unit in {"arcmin", "amin", "am", "minute", "minutes"}:
        return number * 60.0
    if unit in {"deg", "degree", "degrees", "d"}:
        return number * 3600.0
    return None


def _first_value(sections: Iterable[Mapping[str, Any]], *names: str) -> Any:
    lower_names = {n.lower(): n for n in names}
    for section in sections:
        if not isinstance(section, Mapping):
            continue
        for key, value in section.items():
            if str(key).lower() in lower_names:
                return value
    return None


def _summarize_observation_properties(data: Mapping[str, Any]) -> Dict[str, Any]:
    obs = _mapping(data.get("observation_property"))
    coord = _mapping(data.get("coordinate"))
    return {
        "observer": obs.get("OBSERVER") or obs.get("observer"),
        "object": obs.get("OBJECT") or obs.get("object") or obs.get("target"),
        "molecule_1": obs.get("MOLECULE_1") or obs.get("molecule_1"),
        "coord_sys": coord.get("COORD_SYS") or coord.get("coord_sys"),
        "relative": coord.get("RELATIVE") if "RELATIVE" in coord else coord.get("relative"),
    }


def _mode_required_sections(mode: str) -> tuple[str, ...]:
    if mode in {"otf", "grid", "psw"}:
        return ("observation_property", "coordinate", "calibration")
    if mode in {"radio_pointing", "radio-pointing", "radiopointing"}:
        return ("observation_property", "coordinate", "pointing_property", "calibration")
    return ("observation_property", "coordinate")


def _section_names(data: Mapping[str, Any]) -> list[str]:
    return [str(k) for k, v in data.items() if isinstance(v, Mapping)]


def _static_section_warnings(mode: str, data: Mapping[str, Any]) -> list[str]:
    warnings: list[str] = []
    for section in _mode_required_sections(mode):
        if section not in data:
            warnings.append(f"section [{section}] is not present")
    if mode == "otf" and "scan_property" not in data:
        warnings.append("OTF usually needs [scan_property]")
    if mode == "radio_pointing" and "pointing_property" not in data:
        warnings.append("Radio Pointing usually needs [pointing_property]")
    return warnings


def _estimate_plan(mode: str, data: Mapping[str, Any], expanded_params: Mapping[str, Any]) -> Dict[str, Any]:
    scan = _mapping(data.get("scan_property"))
    pointing = _mapping(data.get("pointing_property"))
    calib = _mapping(data.get("calibration"))
    params = _mapping(expanded_params)
    sections = (params, scan, pointing, calib)

    n_value = _first_value(sections, "n", "N", "num", "repeat", "n_scan", "nscan")
    try:
        n = int(n_value) if n_value not in (None, "", {}) else None
    except Exception:
        n = None
    if n is not None and n <= 0:
        n = None

    integ_on = _parse_duration_sec(_first_value(sections, "integ_on", "integ"))
    integ_off = _parse_duration_sec(_first_value((calib, params), "integ_off"))
    integ_hot = _parse_duration_sec(_first_value((calib, params), "integ_hot"))
    off_interval = _first_value((calib, params), "off_interval")
    load_interval = _first_value((calib, params), "load_interval")

    scan_spacing_arcsec = _parse_angle_arcsec(_first_value((scan, params), "scan_spacing"))
    scan_length_arcsec = _parse_angle_arcsec(_first_value((scan, params), "scan_length"))
    scan_velocity_arcsec_s = _parse_angle_arcsec(_first_value((scan, params), "scan_velocity"))
    scan_direction = _first_value((scan, params), "SCAN_DIRECTION", "scan_direction")

    estimated_scan_sec = None
    if scan_length_arcsec is not None and scan_velocity_arcsec_s not in (None, 0):
        estimated_scan_sec = abs(scan_length_arcsec / scan_velocity_arcsec_s)
    elif integ_on is not None:
        estimated_scan_sec = integ_on

    estimated_total_sec = None
    if n is not None and estimated_scan_sec is not None:
        estimated_total_sec = n * estimated_scan_sec
        if integ_off is not None:
            estimated_total_sec += integ_off
        if integ_hot is not None:
            estimated_total_sec += integ_hot

    if mode == "radio_pointing":
        plan_label = "radio pointing cross/grid plan"
    elif mode == "grid":
        plan_label = "grid point sequence"
    elif mode == "psw":
        plan_label = "position-switching sequence"
    else:
        plan_label = "OTF scan sequence"

    return {
        "plan_label": plan_label,
        "n": n,
        "scan_direction": scan_direction,
        "scan_spacing_arcsec": scan_spacing_arcsec,
        "scan_length_arcsec": scan_length_arcsec,
        "scan_velocity_arcsec_s": scan_velocity_arcsec_s,
        "integ_on_sec": integ_on,
        "integ_off_sec": integ_off,
        "integ_hot_sec": integ_hot,
        "off_interval": off_interval,
        "load_interval": load_interval,
        "estimated_scan_sec": estimated_scan_sec,
        "estimated_total_sec": estimated_total_sec,
        "use_scan_block": bool(expanded_params.get("use_scan_block", True)),
        "cos_correction": bool(expanded_params.get("cos_correction", True)),
        "sg_preflight_policy": expanded_params.get("sg_preflight_policy"),
        "sg_preflight_scope": expanded_params.get("sg_preflight_scope"),
    }


def _try_neclib_parser(mode: str, path: Path) -> Dict[str, Any]:
    """Try the real neclib observation parser when dependencies are available."""

    spec_names = {
        "otf": "OTFSpec",
        "psw": "PSWSpec",
        "grid": "GridSpec",
        "radio_pointing": "RadioPointingSpec",
        "radio-pointing": "RadioPointingSpec",
        "radiopointing": "RadioPointingSpec",
    }
    spec_name = spec_names.get(mode)
    if spec_name is None:
        return {"available": False, "ok": False, "reason": "unsupported mode"}
    try:
        module = __import__("neclib.coordinates.observations", fromlist=[spec_name])
        spec_cls = getattr(module, spec_name)
        spec = spec_cls.from_file(str(path))
        target = getattr(spec, "target", None)
        params = getattr(spec, "parameters", None)
        return {
            "available": True,
            "ok": True,
            "spec_class": spec_name,
            "target": target,
            "parameter_keys": sorted(params.keys()) if isinstance(params, Mapping) else [],
        }
    except Exception as exc:
        return {
            "available": False,
            "ok": False,
            "spec_class": spec_name,
            "reason": str(exc),
            "exception_type": exc.__class__.__name__,
        }


def check_observation_file(
    mode: Any,
    file_path: Any,
    *,
    channel: Any = None,
    require_exists: bool = True,
    use_neclib_parser: bool = True,
) -> ObservationCheckReport:
    """Perform read-only static observation-file validation."""

    normalized_mode = normalize_observation_mode(mode)
    ch = optional_positive_int(channel, name="channel override")
    path = validate_obs_file_path(file_path, require_exists=require_exists)
    if not require_exists and not path.exists():
        return ObservationCheckReport(
            ok=True,
            mode=normalized_mode,
            file=str(path),
            channel=ch,
            summary=(
                f"Check OK for launcher selection only: mode={_ALLOWED_MODES[normalized_mode]}, "
                f"file={path}. File was not parsed because it does not exist here."
            ),
            data={
                "exists": False,
                "parsed": False,
                "v16_defaults": dict(_V16_DEFAULTS),
                "default_sources": {k: "default" for k in _V16_DEFAULTS},
            },
            warnings=["obs file was not present; structural parse was skipped"],
        )

    data = _read_toml(path)
    params = _params_from_toml(data)
    expanded_params, explicit = _apply_v16_defaults(params)
    references = _check_reference_files(path, expanded_params)
    warnings = _static_section_warnings(normalized_mode, data)
    missing_refs = [r for r in references if not r["readable"]]
    for ref in missing_refs:
        warnings.append(f"reference file for {ref['key']} is not readable: {ref['path']}")

    neclib_parse = _try_neclib_parser(normalized_mode, path) if use_neclib_parser else {
        "available": False,
        "ok": False,
        "reason": "disabled",
    }
    if use_neclib_parser and not neclib_parse.get("ok"):
        warnings.append(
            "neclib mode-specific parser was not available or did not complete; "
            "TOML/static checks were still performed"
        )

    plan = _estimate_plan(normalized_mode, data, expanded_params)
    obs_summary = _summarize_observation_properties(data)
    line_count = len(path.read_text(encoding="utf-8", errors="replace").splitlines())
    byte_size = path.stat().st_size
    default_sources = {
        key: "explicit" if explicit.get(key) else "default"
        for key in _V16_DEFAULTS
    }
    summary = (
        f"Check OK: mode={_ALLOWED_MODES[normalized_mode]}, file={path}, "
        f"channel={ch if ch is not None else 'obs file/default'}, "
        f"sections={len(_section_names(data))}, refs={len(references)}, "
        f"v16 defaults: {_format_default_summary(explicit)}"
    )
    return ObservationCheckReport(
        ok=True,
        mode=normalized_mode,
        file=str(path),
        channel=ch,
        summary=summary,
        data={
            "exists": True,
            "parsed": True,
            "size_bytes": byte_size,
            "line_count": line_count,
            "sections": _section_names(data),
            "observation": obs_summary,
            "parameters": dict(params),
            "expanded_parameters": dict(expanded_params),
            "v16_defaults": dict(_V16_DEFAULTS),
            "default_sources": default_sources,
            "reference_files": references,
            "missing_reference_files": missing_refs,
            "neclib_parse": neclib_parse,
            "plan_preview": plan,
        },
        warnings=warnings,
    )


def dry_run_observation_plan(
    mode: Any,
    file_path: Any,
    *,
    channel: Any = None,
    require_exists: bool = True,
    use_neclib_parser: bool = True,
) -> ObservationCheckReport:
    """Build a read-only execution-plan preview for a file-based observation."""

    report = check_observation_file(
        mode,
        file_path,
        channel=channel,
        require_exists=require_exists,
        use_neclib_parser=use_neclib_parser,
    )
    plan = dict(report.data.get("plan_preview") or {})
    refs = report.data.get("reference_files") or []
    missing_refs = report.data.get("missing_reference_files") or []
    duration = plan.get("estimated_total_sec")
    duration_text = "unknown"
    if isinstance(duration, (int, float)) and math.isfinite(float(duration)):
        duration_text = f"{float(duration):.1f}s"
    summary = (
        f"Dry run OK: {_ALLOWED_MODES[report.mode]} plan={plan.get('plan_label')}, "
        f"file={report.file}, channel={report.channel if report.channel is not None else 'obs file/default'}, "
        f"estimated_total={duration_text}, refs={len(refs)}, missing_refs={len(missing_refs)}. "
        "No mount/chopper/recording/gate/SG command was sent."
    )
    data = dict(report.data)
    data["dry_run_semantics"] = {
        "moves_antenna": False,
        "moves_chopper": False,
        "starts_recording": False,
        "opens_gate": False,
        "applies_sg": False,
        "queues_observation": False,
    }
    return ObservationCheckReport(
        ok=report.ok,
        mode=report.mode,
        file=report.file,
        channel=report.channel,
        summary=summary,
        data=data,
        warnings=list(report.warnings),
        errors=list(report.errors),
    )
