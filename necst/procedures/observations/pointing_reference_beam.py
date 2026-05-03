"""Pointing reference-beam support for file-based observations.

This module implements the observation-control side of
``pointing_reference_beam_id``.  The coordinate convention is intentionally
explicit:

* ``x_arcsec = dAz * cos(El)`` is positive toward increasing Az.
* ``y_arcsec = dEl`` is positive toward increasing El.
* A beam center is reconstructed as ``beam = boresight + offset``.
* To put a selected beam on a requested target, the antenna boresight command is
  ``boresight = target - offset``.

The implementation is deliberately dependency-light and does not import the
converter/sunscan implementation.  It accepts the same beam-model fields needed
by the observation program and fails closed for unsupported cases instead of
silently applying a geometrically ambiguous correction.
"""

from __future__ import annotations

from dataclasses import dataclass
import copy
import hashlib
import math
from pathlib import Path
from typing import Any, Dict, Mapping, MutableMapping, Optional, Tuple

try:  # Python >= 3.11
    import tomllib as _toml_reader
except ModuleNotFoundError:  # pragma: no cover
    try:
        import tomli as _toml_reader  # type: ignore[no-redef]
    except ModuleNotFoundError:  # pragma: no cover
        _toml_reader = None  # type: ignore[assignment]


PURE_ROTATION_MODEL = "pure_rotation_v1"

POLICY_TARGET_EL = "target_el"
POLICY_BORE_EL_ITER1 = "boresight_el_iter1"
POLICY_EXACT = "exact"
DEFAULT_POINTING_REFERENCE_BEAM_POLICY = POLICY_EXACT
SUPPORTED_POLICIES = {POLICY_TARGET_EL, POLICY_BORE_EL_ITER1, POLICY_EXACT}
# Backward-compatible alias for older tests/scripts that imported this symbol.
SUPPORTED_POLICY = DEFAULT_POINTING_REFERENCE_BEAM_POLICY
DEFAULT_REFERENCE_BEAM_ID = "B00"
ALTAZ_FRAMES = {"altaz", "azel", "az_el", "az-el"}


class PointingReferenceBeamError(ValueError):
    """Raised when pointing-reference beam correction cannot be applied safely."""


@dataclass(frozen=True)
class BeamModelDocument:
    beams: Dict[str, Dict[str, Any]]
    source_path: Optional[Path] = None
    input_file_sha256: Optional[str] = None
    beam_model_version: Optional[str] = None


@dataclass(frozen=True)
class PointingReferenceBeamContext:
    pointing_reference_beam_id: str
    pointing_reference_beam_policy: str = DEFAULT_POINTING_REFERENCE_BEAM_POLICY
    beam_model: Optional[BeamModelDocument] = None

    @property
    def active(self) -> bool:
        return bool(self.pointing_reference_beam_id) and self.pointing_reference_beam_id != DEFAULT_REFERENCE_BEAM_ID


def _optional_str(params: Mapping[str, Any], *keys: str) -> Optional[str]:
    for key in keys:
        if key in params and params[key] not in (None, ""):
            return str(params[key]).strip()
    return None


def _obs_base_dir(obs_file: Any) -> Path:
    try:
        path = Path(obs_file).expanduser()
    except TypeError:
        return Path.cwd()
    if path.parent == Path(""):
        return Path.cwd()
    return path.resolve().parent


def _resolve_path(path_text: Optional[str], *, base_dir: Path) -> Optional[Path]:
    if not path_text:
        return None
    path = Path(path_text).expanduser()
    if not path.is_absolute():
        path = base_dir / path
    return path.resolve()


def _read_toml(path: Path) -> Dict[str, Any]:
    if _toml_reader is None:  # pragma: no cover
        raise PointingReferenceBeamError("No TOML reader is available. Use Python >= 3.11, or install tomli.")
    try:
        raw = path.read_bytes()
    except OSError as exc:
        raise PointingReferenceBeamError(f"Cannot read beam_model TOML file: {path}") from exc
    try:
        return _toml_reader.loads(raw.decode("utf-8"))  # type: ignore[no-any-return]
    except Exception as exc:
        raise PointingReferenceBeamError(f"Cannot parse beam_model TOML file: {path}: {exc}") from exc


def _sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _as_float(value: Any, *, key: str, default: Optional[float] = None) -> float:
    if value is None:
        if default is None:
            raise PointingReferenceBeamError(f"beam field {key!r} is required")
        return float(default)
    try:
        out = float(value)
    except Exception as exc:
        raise PointingReferenceBeamError(f"beam field {key!r} must be numeric, got {value!r}") from exc
    if not math.isfinite(out):
        raise PointingReferenceBeamError(f"beam field {key!r} must be finite, got {value!r}")
    return out


def _normalize_rotation_mode(value: Any) -> str:
    text = str(value if value is not None else "none").strip().lower()
    if text in {"", "legacy"}:
        return "none"
    if text not in {"none", "elevation", PURE_ROTATION_MODEL}:
        raise PointingReferenceBeamError(
            f"rotation_mode must be none, elevation, or {PURE_ROTATION_MODEL!r}; got {value!r}"
        )
    return text


def _normalize_model(value: Any, rotation_mode: str) -> str:
    text = str(value if value is not None else "legacy").strip().lower()
    if text in {"", "none"}:
        text = "legacy"
    if rotation_mode == PURE_ROTATION_MODEL:
        text = PURE_ROTATION_MODEL
    return text


def _normalize_sign(value: Any, *, key: str = "rotation_sign") -> float:
    sign = _as_float(value, key=key)
    if sign not in (-1.0, 1.0):
        raise PointingReferenceBeamError(f"{key} must be +/-1, got {value!r}")
    return sign


def _first_consistent_float(beam_id: str, key: str, *values: Any) -> Any:
    present = [v for v in values if v is not None]
    if not present:
        return None
    first = _as_float(present[0], key=key)
    for value in present[1:]:
        other = _as_float(value, key=key)
        if abs(other - first) > 1e-9:
            raise PointingReferenceBeamError(
                f"beam {beam_id!r}: inconsistent {key} values in flat/nested pure_rotation schema: {present!r}"
            )
    return first


def _normalize_beam_block(beam_id: str, raw_block: Mapping[str, Any]) -> Dict[str, Any]:
    block = dict(raw_block or {})
    rotation_mode = _normalize_rotation_mode(block.get("rotation_mode", "none"))
    model = _normalize_model(block.get("model", None), rotation_mode)
    version = block.get("beam_model_version", block.get("version", None))
    version_s = None if version is None else str(version).strip()

    if model == PURE_ROTATION_MODEL:
        nested = dict(block.get("pure_rotation", {}) or {})
        sign = _first_consistent_float(
            beam_id,
            "pure_rotation_sign/rotation_sign",
            block.get("pure_rotation_sign"),
            block.get("rotation_sign"),
            nested.get("rotation_sign"),
        )
        x0 = _first_consistent_float(
            beam_id,
            "pure_rotation_offset_x_el0_arcsec",
            block.get("pure_rotation_offset_x_el0_arcsec"),
            block.get("offset_x_el0_arcsec"),
            nested.get("offset_x_el0_arcsec"),
        )
        y0 = _first_consistent_float(
            beam_id,
            "pure_rotation_offset_y_el0_arcsec",
            block.get("pure_rotation_offset_y_el0_arcsec"),
            block.get("offset_y_el0_arcsec"),
            nested.get("offset_y_el0_arcsec"),
        )
        # Do not silently accept legacy non-zero offsets mixed into pure_rotation_v1.
        conflicts = []
        for key in ("az_offset_arcsec", "el_offset_arcsec", "reference_angle_deg", "reference_el_deg"):
            if key in block and block.get(key) is not None and abs(_as_float(block.get(key), key=key)) > 0.0:
                conflicts.append(key)
        if "rotation_slope_deg_per_deg" in block and block.get("rotation_slope_deg_per_deg") is not None:
            if abs(_as_float(block.get("rotation_slope_deg_per_deg"), key="rotation_slope_deg_per_deg")) > 0.0:
                conflicts.append("rotation_slope_deg_per_deg")
        if conflicts:
            raise PointingReferenceBeamError(
                f"beam {beam_id!r}: {PURE_ROTATION_MODEL} cannot be mixed with non-zero legacy fields: "
                f"{sorted(set(conflicts))}"
            )
        return {
            "beam_id": str(beam_id),
            "model": PURE_ROTATION_MODEL,
            "rotation_mode": PURE_ROTATION_MODEL,
            "beam_model_version": version_s,
            "az_offset_arcsec": 0.0,
            "el_offset_arcsec": 0.0,
            "reference_angle_deg": 0.0,
            "rotation_sign": _normalize_sign(sign, key="pure_rotation_sign/rotation_sign"),
            "rotation_slope_deg_per_deg": None,
            "dewar_angle_deg": _as_float(block.get("dewar_angle_deg", 0.0), key="dewar_angle_deg", default=0.0),
            "pure_rotation_offset_x_el0_arcsec": _as_float(x0, key="pure_rotation_offset_x_el0_arcsec"),
            "pure_rotation_offset_y_el0_arcsec": _as_float(y0, key="pure_rotation_offset_y_el0_arcsec"),
            "pure_rotation_sign": _normalize_sign(sign, key="pure_rotation_sign/rotation_sign"),
        }

    return {
        "beam_id": str(beam_id),
        "model": model,
        "rotation_mode": rotation_mode,
        "beam_model_version": version_s,
        "az_offset_arcsec": _as_float(block.get("az_offset_arcsec", 0.0), key="az_offset_arcsec", default=0.0),
        "el_offset_arcsec": _as_float(block.get("el_offset_arcsec", 0.0), key="el_offset_arcsec", default=0.0),
        "reference_angle_deg": _as_float(
            block.get("reference_angle_deg", block.get("reference_el_deg", 0.0)),
            key="reference_angle_deg",
            default=0.0,
        ),
        "rotation_sign": _as_float(block.get("rotation_sign", 1.0), key="rotation_sign", default=1.0),
        "rotation_slope_deg_per_deg": (
            _as_float(block.get("rotation_slope_deg_per_deg"), key="rotation_slope_deg_per_deg")
            if block.get("rotation_slope_deg_per_deg") is not None
            else None
        ),
        "dewar_angle_deg": _as_float(block.get("dewar_angle_deg", 0.0), key="dewar_angle_deg", default=0.0),
        "pure_rotation_offset_x_el0_arcsec": None,
        "pure_rotation_offset_y_el0_arcsec": None,
        "pure_rotation_sign": None,
    }


def normalize_beam_model(raw: Mapping[str, Any], *, source_path: Optional[Path] = None, input_file_sha256: Optional[str] = None) -> BeamModelDocument:
    raw = dict(raw or {})
    beams_raw = raw.get("beams", {})
    if not isinstance(beams_raw, Mapping) or not beams_raw:
        raise PointingReferenceBeamError("beam model contains no [beams.<beam_id>] entries")
    beams = {
        str(beam_id): _normalize_beam_block(str(beam_id), block)
        for beam_id, block in dict(beams_raw).items()
    }
    versions = sorted({str(b.get("beam_model_version")) for b in beams.values() if b.get("beam_model_version")})
    version = str(raw.get("beam_model_version", raw.get("version", ""))).strip() or (versions[0] if len(versions) == 1 else None)
    return BeamModelDocument(
        beams=beams,
        source_path=source_path,
        input_file_sha256=input_file_sha256,
        beam_model_version=version,
    )


def load_beam_model(path: Path) -> BeamModelDocument:
    path = path.expanduser().resolve()
    if not path.exists():
        raise PointingReferenceBeamError(f"beam_model file does not exist: {path}")
    return normalize_beam_model(_read_toml(path), source_path=path, input_file_sha256=_sha256_file(path))


def build_pointing_reference_context(
    params: Mapping[str, Any],
    *,
    obs_file: Any,
) -> Optional[PointingReferenceBeamContext]:
    """Build pointing-reference context from `.obs` parameters.

    ``point_reference_beam_id`` is rejected intentionally; the supported
    observation-control name is ``pointing_reference_beam_id``.
    """

    params = dict(params or {})
    if _optional_str(params, "point_reference_beam_id") is not None:
        raise PointingReferenceBeamError(
            "Unsupported parameter 'point_reference_beam_id'. Use 'pointing_reference_beam_id'."
        )

    ref_id = _optional_str(params, "pointing_reference_beam_id")
    if ref_id is None or ref_id == "":
        return None
    policy = _optional_str(params, "pointing_reference_beam_policy") or DEFAULT_POINTING_REFERENCE_BEAM_POLICY
    if policy not in SUPPORTED_POLICIES:
        raise PointingReferenceBeamError(
            f"Unsupported pointing_reference_beam_policy={policy!r}; "
            f"supported={sorted(SUPPORTED_POLICIES)!r}."
        )

    ref_id = str(ref_id)
    base_dir = _obs_base_dir(obs_file)
    beam_ref = _optional_str(params, "beam_model", "beam_model_path")
    beam_path = _resolve_path(beam_ref, base_dir=base_dir)
    if beam_path is None:
        raise PointingReferenceBeamError(
            "pointing_reference_beam_id was specified, so beam_model or beam_model_path is required. "
            "Omit pointing_reference_beam_id to use the default boresight B00=(0,0) observation."
        )
    beam_model = load_beam_model(beam_path)
    if ref_id not in beam_model.beams:
        raise PointingReferenceBeamError(
            f"pointing_reference_beam_id={ref_id!r} is not present in beam_model; "
            f"available={sorted(beam_model.beams)}"
        )
    return PointingReferenceBeamContext(
        pointing_reference_beam_id=ref_id,
        pointing_reference_beam_policy=policy,
        beam_model=beam_model,
    )


def _rotate_offset_arcsec(x0_arcsec: float, y0_arcsec: float, theta_deg: float) -> Tuple[float, float]:
    theta = math.radians(float(theta_deg))
    x = float(x0_arcsec) * math.cos(theta) - float(y0_arcsec) * math.sin(theta)
    y = float(x0_arcsec) * math.sin(theta) + float(y0_arcsec) * math.cos(theta)
    return x, y


def evaluate_beam_offset_arcsec(
    beam_model: BeamModelDocument | Mapping[str, Any],
    beam_id: str,
    az_deg: float,
    el_deg: float,
) -> Tuple[float, float]:
    """Evaluate beam offset in the tangent-plane convention.

    Parameters
    ----------
    beam_model
        Normalized document or raw mapping containing ``beams``.
    beam_id
        Beam to evaluate.
    az_deg, el_deg
        Target Az/El in degrees.  The current implemented models depend on El;
        Az is accepted to keep the API explicit and future-proof.

    Returns
    -------
    tuple
        ``(x_arcsec, y_arcsec)`` where ``x = dAz*cos(El)`` and ``y = dEl``.
    """

    _ = float(az_deg)  # kept for signature symmetry and future models
    el = float(el_deg)
    if not math.isfinite(el):
        raise PointingReferenceBeamError(f"el_deg must be finite, got {el_deg!r}")

    bm = beam_model if isinstance(beam_model, BeamModelDocument) else normalize_beam_model(beam_model)
    beam_id = str(beam_id)
    if beam_id not in bm.beams:
        raise PointingReferenceBeamError(f"beam_id={beam_id!r} is not present in beam_model")
    beam = bm.beams[beam_id]
    model = str(beam.get("model", "legacy") or "legacy").strip().lower()
    rotation_mode = str(beam.get("rotation_mode", "none") or "none").strip().lower()

    if model == PURE_ROTATION_MODEL or rotation_mode == PURE_ROTATION_MODEL:
        sign = float(beam.get("pure_rotation_sign", beam.get("rotation_sign", 1.0)))
        x0 = float(beam["pure_rotation_offset_x_el0_arcsec"])
        y0 = float(beam["pure_rotation_offset_y_el0_arcsec"])
        x, y = _rotate_offset_arcsec(x0, y0, sign * el)
    else:
        if rotation_mode == "none":
            theta = float(beam.get("dewar_angle_deg", 0.0))
        elif rotation_mode == "elevation":
            slope_raw = beam.get("rotation_slope_deg_per_deg")
            slope = float(slope_raw) if slope_raw is not None else float(beam.get("rotation_sign", 1.0))
            theta = slope * (el - float(beam.get("reference_angle_deg", 0.0))) + float(beam.get("dewar_angle_deg", 0.0))
        else:
            raise PointingReferenceBeamError(f"Unsupported rotation_mode={rotation_mode!r}")
        x, y = _rotate_offset_arcsec(
            float(beam.get("az_offset_arcsec", 0.0)),
            float(beam.get("el_offset_arcsec", 0.0)),
            theta,
        )

    if not (math.isfinite(x) and math.isfinite(y)):
        raise PointingReferenceBeamError(
            f"Non-finite beam offset for beam_id={beam_id!r}: x={x!r}, y={y!r}"
        )
    return x, y


def _check_cos_el(el_deg: float, *, label: str) -> float:
    cos_el = math.cos(math.radians(el_deg))
    min_cos = math.cos(math.radians(89.9))
    if abs(cos_el) < min_cos:
        raise PointingReferenceBeamError(
            f"Cannot apply pointing_reference_beam near the pole/high elevation: "
            f"{label}={el_deg} deg, cos(el)={cos_el}"
        )
    return cos_el


def _solve_exact_boresight_el_deg(
    target_az_deg: float,
    target_el_deg: float,
    context: PointingReferenceBeamContext,
    *,
    max_iter: int = 20,
    tol_arcsec: float = 1.0e-8,
) -> float:
    """Solve C_el + y(C_el)/3600 = T_el for the boresight elevation.

    Current supported beam models depend on elevation but not azimuth.  A
    fixed-point solve is sufficient because |dy/dEl|/3600 is very small for
    realistic multibeam offsets.  The residual is checked explicitly so that
    a future non-contractive model cannot silently pass.
    """

    if context.beam_model is None:
        raise PointingReferenceBeamError("Active pointing_reference_beam_id requires a beam model")

    c_el = float(target_el_deg)
    for _ in range(max_iter):
        _, y_arcsec = evaluate_beam_offset_arcsec(
            context.beam_model,
            context.pointing_reference_beam_id,
            target_az_deg,
            c_el,
        )
        next_c_el = float(target_el_deg) - y_arcsec / 3600.0
        if abs(next_c_el - c_el) * 3600.0 <= tol_arcsec:
            c_el = next_c_el
            break
        c_el = next_c_el

    _, y_arcsec = evaluate_beam_offset_arcsec(
        context.beam_model,
        context.pointing_reference_beam_id,
        target_az_deg,
        c_el,
    )
    residual_arcsec = (c_el + y_arcsec / 3600.0 - float(target_el_deg)) * 3600.0
    if abs(residual_arcsec) > max(10.0 * tol_arcsec, 1.0e-6):
        raise PointingReferenceBeamError(
            f"Failed to solve exact pointing_reference_beam elevation: "
            f"target_el={target_el_deg!r}, residual={residual_arcsec:.6g} arcsec"
        )
    return c_el


def apply_pointing_reference_beam(
    target_az_deg: float,
    target_el_deg: float,
    context: Optional[PointingReferenceBeamContext],
    policy: Optional[str] = None,
) -> Tuple[float, float]:
    """Convert requested target Az/El into boresight Az/El.

    The sign convention is always ``beam = boresight + offset`` and therefore
    ``boresight = target - offset``.  The policy controls which elevation is
    used to evaluate the offset:

    * ``target_el`` evaluates the offset at the requested target elevation.
    * ``boresight_el_iter1`` evaluates once at the target elevation and once at
      the resulting boresight elevation.
    * ``exact`` solves ``C_el + y(C_el)/3600 = T_el`` and then evaluates
      ``x(C_el)``.  This is the recommended policy for science observations
      because it is exactly consistent with converter/sunscan reconstruction.
    """

    if context is None or not context.active:
        return float(target_az_deg), float(target_el_deg)
    effective_policy = policy or context.pointing_reference_beam_policy or DEFAULT_POINTING_REFERENCE_BEAM_POLICY
    if effective_policy not in SUPPORTED_POLICIES:
        raise PointingReferenceBeamError(
            f"Unsupported pointing_reference_beam_policy={effective_policy!r}; "
            f"supported={sorted(SUPPORTED_POLICIES)!r}."
        )
    if context.beam_model is None:
        raise PointingReferenceBeamError("Active pointing_reference_beam_id requires a beam model")

    target_az = float(target_az_deg)
    target_el = float(target_el_deg)
    if not (math.isfinite(target_az) and math.isfinite(target_el)):
        raise PointingReferenceBeamError(
            f"target Az/El must be finite, got ({target_az_deg!r}, {target_el_deg!r})"
        )

    if effective_policy == POLICY_TARGET_EL:
        eval_el = target_el
    elif effective_policy == POLICY_BORE_EL_ITER1:
        _, y0_arcsec = evaluate_beam_offset_arcsec(
            context.beam_model,
            context.pointing_reference_beam_id,
            target_az,
            target_el,
        )
        eval_el = target_el - y0_arcsec / 3600.0
    elif effective_policy == POLICY_EXACT:
        eval_el = _solve_exact_boresight_el_deg(target_az, target_el, context)
    else:  # pragma: no cover - guarded above
        raise PointingReferenceBeamError(f"Internal unsupported policy: {effective_policy!r}")

    x_arcsec, y_arcsec = evaluate_beam_offset_arcsec(
        context.beam_model,
        context.pointing_reference_beam_id,
        target_az,
        eval_el,
    )
    if effective_policy == POLICY_EXACT:
        boresight_el = eval_el
    else:
        boresight_el = target_el - y_arcsec / 3600.0

    cos_el = _check_cos_el(boresight_el if effective_policy == POLICY_EXACT else eval_el, label="boresight_el")
    boresight_az = target_az - x_arcsec / (3600.0 * cos_el)
    if not (math.isfinite(boresight_az) and math.isfinite(boresight_el)):
        raise PointingReferenceBeamError(
            f"Computed non-finite boresight Az/El: ({boresight_az!r}, {boresight_el!r})"
        )
    return boresight_az, boresight_el


def _is_altaz_frame(frame: Any) -> bool:
    return str(frame or "").strip().lower() in ALTAZ_FRAMES


def _shift_altaz_tuple(
    value: Tuple[Any, Any, Any],
    context: Optional[PointingReferenceBeamContext],
    *,
    label: str,
) -> Tuple[float, float, str]:
    if context is None or not context.active:
        return (float(value[0]), float(value[1]), str(value[2]))
    if len(value) != 3:
        raise PointingReferenceBeamError(f"{label} must be a 3-tuple (az, el, frame)")
    if not _is_altaz_frame(value[2]):
        raise PointingReferenceBeamError(
            f"pointing_reference_beam_id can only be applied to altaz/azel coordinates at this layer; "
            f"{label} frame is {value[2]!r}."
        )
    az, el = apply_pointing_reference_beam(
        float(value[0]),
        float(value[1]),
        context,
        policy=context.pointing_reference_beam_policy,
    )
    return (az, el, str(value[2]))


def apply_pointing_reference_to_point_kwargs(
    kwargs: Mapping[str, Any],
    context: Optional[PointingReferenceBeamContext],
) -> Dict[str, Any]:
    """Apply reference-beam correction to a Commander.antenna('point') kwargs dict."""

    out = copy.deepcopy(dict(kwargs))
    if context is None or not context.active:
        return out
    if "name" in out:
        raise PointingReferenceBeamError(
            "pointing_reference_beam_id is not supported for name-based point commands in this layer; "
            "use explicit altaz/azel target coordinates or implement a lower-level resolver."
        )
    if "target" in out and out["target"] is not None:
        out["target"] = _shift_altaz_tuple(out["target"], context, label="target")
    elif "reference" in out and out["reference"] is not None:
        out["reference"] = _shift_altaz_tuple(out["reference"], context, label="reference")
    else:
        raise PointingReferenceBeamError("pointing_reference_beam_id point command requires target/reference")
    return out


def apply_pointing_reference_to_scan_kwargs(
    kwargs: Mapping[str, Any],
    context: Optional[PointingReferenceBeamContext],
) -> Dict[str, Any]:
    """Apply reference-beam correction to a Commander.antenna('scan') kwargs dict."""

    out = copy.deepcopy(dict(kwargs))
    if context is None or not context.active:
        return out
    if "name" in out:
        raise PointingReferenceBeamError(
            "pointing_reference_beam_id is not supported for name-based scan commands in this layer; "
            "use explicit altaz/azel reference coordinates or implement a lower-level resolver."
        )
    if "target" in out and out["target"] is not None:
        out["target"] = _shift_altaz_tuple(out["target"], context, label="target")
        return out
    if "reference" in out and out["reference"] is not None:
        out["reference"] = _shift_altaz_tuple(out["reference"], context, label="reference")
        return out

    scan_frame = out.get("scan_frame")
    if not _is_altaz_frame(scan_frame):
        raise PointingReferenceBeamError(
            "pointing_reference_beam_id scan without target/reference requires altaz/azel scan_frame."
        )
    if "start" not in out or "stop" not in out:
        raise PointingReferenceBeamError("scan command requires start/stop")
    start = out["start"]
    stop = out["stop"]
    mid_az = 0.5 * (float(start[0]) + float(stop[0]))
    mid_el = 0.5 * (float(start[1]) + float(stop[1]))
    bore_mid_az, bore_mid_el = apply_pointing_reference_beam(
        mid_az,
        mid_el,
        context,
        policy=context.pointing_reference_beam_policy,
    )
    daz = bore_mid_az - mid_az
    del_ = bore_mid_el - mid_el
    out["start"] = (float(start[0]) + daz, float(start[1]) + del_)
    out["stop"] = (float(stop[0]) + daz, float(stop[1]) + del_)
    return out


def apply_pointing_reference_to_scan_block_kwargs(
    kwargs: Mapping[str, Any],
    context: Optional[PointingReferenceBeamContext],
) -> Dict[str, Any]:
    """Apply reference-beam correction to a Commander.scan_block kwargs dict."""

    out = copy.deepcopy(dict(kwargs))
    if context is None or not context.active:
        return out
    if "name" in out:
        raise PointingReferenceBeamError(
            "pointing_reference_beam_id is not supported for name-based scan_block commands in this layer; "
            "use explicit altaz/azel target/reference coordinates or implement a lower-level resolver."
        )
    if "target" in out and out["target"] is not None:
        out["target"] = _shift_altaz_tuple(out["target"], context, label="target")
        return out
    if "reference" in out and out["reference"] is not None:
        out["reference"] = _shift_altaz_tuple(out["reference"], context, label="reference")
        return out
    raise PointingReferenceBeamError(
        "pointing_reference_beam_id scan_block requires an explicit altaz/azel target/reference."
    )


def pointing_reference_snapshot_record(context: Optional[PointingReferenceBeamContext]) -> Dict[str, Any]:
    if context is None:
        return {
            "pointing_reference_beam_id": DEFAULT_REFERENCE_BEAM_ID,
            "pointing_reference_beam_policy": DEFAULT_POINTING_REFERENCE_BEAM_POLICY,
        }
    record: Dict[str, Any] = {
        "pointing_reference_beam_id": context.pointing_reference_beam_id or DEFAULT_REFERENCE_BEAM_ID,
        "pointing_reference_beam_policy": context.pointing_reference_beam_policy,
    }
    if context.beam_model is not None:
        if context.beam_model.source_path is not None:
            record["beam_model_path"] = str(context.beam_model.source_path)
        if context.beam_model.input_file_sha256:
            record["beam_model_hash"] = context.beam_model.input_file_sha256
        if context.beam_model.beam_model_version:
            record["beam_model_version"] = context.beam_model.beam_model_version
    return record


def attach_pointing_reference_to_snapshot(
    snapshot: Mapping[str, Any],
    context: Optional[PointingReferenceBeamContext],
    *,
    dumps_toml_func: Any,
    sha256_text_func: Any,
) -> Dict[str, Any]:
    """Attach pointing-reference provenance and recompute the canonical hash."""

    out = copy.deepcopy(dict(snapshot))
    out["pointing_reference_beam"] = pointing_reference_snapshot_record(context)
    out["canonical_snapshot_sha256"] = ""
    out["canonical_snapshot_sha256"] = sha256_text_func(dumps_toml_func(out))
    return out
