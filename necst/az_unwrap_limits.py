"""Safety helpers for Az commands when absolute Az unwrap is disabled.

This module intentionally lives at the top level of :mod:`necst`, not under
``necst.ctrl.antenna``.  Core modules such as ``commander.py`` are imported
while ``necst.core`` is still being initialised; importing ``necst.ctrl`` from
there pulls in antenna controller modules and can create a circular import.
The helpers here depend only on the already-loaded NECST configuration.
"""

from __future__ import annotations

import math
from typing import Any, Mapping, Optional

from . import config


def _cfg_get(obj: Any, key: str, default: Any = None) -> Any:
    if obj is None:
        return default
    try:
        if isinstance(obj, Mapping) and key in obj:
            return obj[key]
    except Exception:
        pass
    try:
        value = getattr(obj, key)
    except Exception:
        return default
    return default if value is None else value


def _to_float(value: Any, unit: str = "deg", default: Optional[float] = None) -> Optional[float]:
    if value is None:
        return default
    if hasattr(value, "to_value"):
        try:
            return float(value.to_value(unit))
        except Exception:
            try:
                return float(value.to_value(""))
            except Exception:
                return default
    try:
        return float(value)
    except Exception:
        return default


def _to_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on", "enabled"}
    return bool(value)


def _az_unwrap_az_section() -> Optional[Any]:
    """Return the configured Az unwrap subsection, if any exists.

    A missing ``antenna_encoder_unwrap`` section means this telescope is not
    configured for absolute-modulo Az unwrap and must keep the legacy behavior.
    A present section with ``enabled=false`` means an absolute-modulo encoder is
    configured but the software unwrap is disabled; in that state commands must
    not exceed the raw absolute encoder range.
    """

    root = getattr(config, "antenna_encoder_unwrap", None)
    if root is None:
        return None
    az = _cfg_get(root, "az", None)
    return root if az is None else az


def load_unwrap_disabled_mount_az_limit() -> Optional[dict]:
    """Return raw-Az command limits when absolute unwrap is configured off.

    When an absolute-modulo Az encoder is configured but
    ``antenna_encoder_unwrap.az.enabled`` is false, the control system has no
    branch information.  In that state Az commands must be confined to the raw
    absolute encoder range (normally 0..360 deg).  If no unwrap section exists,
    return ``None`` so NANTEN2/incremental-encoder configurations are unchanged.
    """

    az = _az_unwrap_az_section()
    if az is None:
        return None
    enabled = _to_bool(_cfg_get(az, "enabled", False), False)
    if enabled:
        return None
    mode = str(_cfg_get(az, "mode", "absolute_modulo") or "").strip().lower()
    if mode not in {"absolute_modulo", "absolute-modulo"}:
        return None
    raw_min = float(
        _to_float(
            _cfg_get(az, "raw_min", _cfg_get(az, "raw_min_deg", 0.0)),
            "deg",
            0.0,
        )
    )
    raw_max = float(
        _to_float(
            _cfg_get(az, "raw_max", _cfg_get(az, "raw_max_deg", 360.0)),
            "deg",
            360.0,
        )
    )
    if not (math.isfinite(raw_min) and math.isfinite(raw_max)) or raw_min >= raw_max:
        raise ValueError(
            "antenna_encoder_unwrap.az raw_min/raw_max are invalid: "
            f"raw_min={raw_min!r}, raw_max={raw_max!r}"
        )
    return {
        "enabled": False,
        "mode": mode,
        "raw_min_deg": raw_min,
        "raw_max_deg": raw_max,
    }


def _flatten_degree_values(value: Any) -> list[float]:
    if hasattr(value, "to_value"):
        value = value.to_value("deg")
    if hasattr(value, "tolist"):
        value = value.tolist()
    if isinstance(value, (str, bytes)):
        return [float(value)]
    try:
        iterator = iter(value)
    except TypeError:
        return [float(value)]
    values: list[float] = []
    for item in iterator:
        values.extend(_flatten_degree_values(item))
    return values


def assert_mount_az_allowed_when_unwrap_disabled(
    az_deg: Any, *, action_label: str = "Az target"
) -> Optional[dict]:
    """Reject Az commands outside raw range when unwrap is configured off.

    This is a fail-closed guard for absolute-modulo encoders.  It intentionally
    uses the configured raw absolute encoder range, not the wider continuous
    drive range, because without unwrap the controller cannot distinguish e.g.
    10 deg from 370 deg.
    """

    limit = load_unwrap_disabled_mount_az_limit()
    if limit is None:
        return None
    raw_min = float(limit["raw_min_deg"])
    raw_max = float(limit["raw_max_deg"])
    values = _flatten_degree_values(az_deg)
    for value in values:
        if not math.isfinite(value) or value < raw_min or value > raw_max:
            raise ValueError(
                f"{action_label}: Az={value:g} deg is outside raw absolute "
                f"encoder range {raw_min:g}..{raw_max:g} deg while "
                "antenna_encoder_unwrap.az.enabled=false; enable Az unwrap "
                "or keep Az commands within raw_min/raw_max."
            )
    checked = dict(limit)
    checked["checked_value_count"] = len(values)
    return checked
