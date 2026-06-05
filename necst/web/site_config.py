"""Site-configuration summary for the NECST Operator Console.

The Operator Console must not hard-code drive limits or chopper endpoints.  This
module resolves the active configuration into a small, dependency-light summary
that browser status and server-side validation can both use.

Resolution order
----------------
1. Explicit TOML path passed by ``bin/console.py --site-config``.
2. Active ``necst.config`` / neclib configuration, when importable.
3. CLI fallback values for mount limits only.

No telescope command is sent from this module.
"""

from __future__ import annotations

import math
import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Mapping, Optional, Tuple

try:  # Python 3.11+
    import tomllib  # type: ignore[attr-defined]
except Exception:  # pragma: no cover - old Python fallback if tomli is installed
    tomllib = None  # type: ignore[assignment]


JsonDict = Dict[str, Any]


@dataclass(frozen=True)
class SiteConfigSummary:
    """Small, serializable view of active site/operator settings."""

    source: str
    source_path: Optional[str] = None
    observatory: Optional[str] = None
    mount_limits: Dict[str, float] = field(default_factory=dict)
    mount_warning_limits: Dict[str, float] = field(default_factory=dict)
    mount_drive_range: Dict[str, float] = field(default_factory=dict)
    chopper: Dict[str, Any] = field(default_factory=dict)
    capabilities: Dict[str, bool] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)

    def to_dict(self) -> JsonDict:
        return {
            "source": self.source,
            "source_path": self.source_path,
            "observatory": self.observatory,
            "mount_limits": dict(self.mount_limits),
            "mount_warning_limits": dict(self.mount_warning_limits),
            "mount_drive_range": dict(self.mount_drive_range),
            "chopper": dict(self.chopper),
            "capabilities": dict(self.capabilities),
            "warnings": list(self.warnings),
        }


def _finite_float(value: Any) -> Optional[float]:
    try:
        number = float(value)
    except Exception:
        return None
    return number if math.isfinite(number) else None


def _as_mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


_QUANTITY_RE = re.compile(
    r"^\s*([+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?)\s*([A-Za-z/_*\-0-9]*)\s*$"
)


def _quantity_to_deg(value: Any) -> Optional[float]:
    """Convert a config quantity-like object to degrees.

    Numeric values are interpreted as degrees because neclib has already parsed
    the site TOML or because the console CLI fallback explicitly uses degrees.
    String values are accepted only for common angular units.
    """

    if value is None:
        return None
    try:
        return float(value.to_value("deg"))  # astropy Quantity
    except Exception:
        pass
    try:
        raw = getattr(value, "value")
        unit = str(getattr(value, "unit", "deg") or "deg").lower()
        number = float(raw)
        if unit in {"deg", "degree", "degrees"}:
            return number
        if unit in {"arcmin", "arcminute", "arcminutes"}:
            return number / 60.0
        if unit in {"arcsec", "arcsecond", "arcseconds"}:
            return number / 3600.0
        if unit in {"rad", "radian", "radians"}:
            return math.degrees(number)
    except Exception:
        pass
    if isinstance(value, (int, float)):
        number = _finite_float(value)
        return number
    if isinstance(value, str):
        m = _QUANTITY_RE.match(value)
        if not m:
            return None
        number = _finite_float(m.group(1))
        if number is None:
            return None
        unit = (m.group(2) or "deg").lower().replace(" ", "")
        if unit in {"", "deg", "degree", "degrees"}:
            return number
        if unit in {"arcmin", "arcminute", "arcminutes"}:
            return number / 60.0
        if unit in {"arcsec", "arcsecond", "arcseconds"}:
            return number / 3600.0
        if unit in {"rad", "radian", "radians"}:
            return math.degrees(number)
    return None


def _range_to_pair(obj: Any) -> Optional[Tuple[float, float]]:
    """Return ``(min_deg, max_deg)`` from neclib ValueRange or TOML list."""

    if obj is None:
        return None
    try:
        values = list(obj.map(lambda x: x.to_value("deg")))
    except Exception:
        try:
            values = list(obj)
        except Exception:
            return None
    if len(values) < 2:
        return None
    lo = _quantity_to_deg(values[0])
    hi = _quantity_to_deg(values[1])
    if lo is None or hi is None:
        return None
    if not (math.isfinite(lo) and math.isfinite(hi)):
        return None
    return float(lo), float(hi)


def _dict_from_range_pair(pair: Optional[Tuple[float, float]], prefix: str) -> Dict[str, float]:
    if pair is None:
        return {}
    return {f"{prefix}_min": float(pair[0]), f"{prefix}_max": float(pair[1])}


def _mount_dict(
    az_obj: Any,
    el_obj: Any,
    *,
    az_prefix: str = "az",
    el_prefix: str = "el",
) -> Dict[str, float]:
    out: Dict[str, float] = {}
    out.update(_dict_from_range_pair(_range_to_pair(az_obj), az_prefix))
    out.update(_dict_from_range_pair(_range_to_pair(el_obj), el_prefix))
    return out


def _normalize_observatory(value: Any) -> Optional[str]:
    text = str(value or "").strip()
    return text or None


def _maintenance_supported(observatory: Optional[str]) -> bool:
    """Return whether chopper maintenance services should be exposed.

    The current confirmed maintenance path is for the OMU 1.85 m system.  For
    NANTEN2/NANTEN2SA these service buttons must not send commands from the GUI.
    Unknown sites are kept conservative and treated as unsupported.
    """

    key = (observatory or "").replace("-", "").replace("_", "").upper()
    return key in {"OMU1P85M", "OMU185M", "ALAB"}


def _capabilities_from_summary(
    *,
    observatory: Optional[str],
    mount_limits: Mapping[str, Any],
    chopper: Mapping[str, Any],
) -> Dict[str, bool]:
    has_mount_limits = all(k in mount_limits for k in ("az_min", "az_max", "el_min", "el_max"))
    has_chopper_positions = chopper.get("insert_position") is not None and chopper.get("remove_position") is not None
    return {
        "mount_move": bool(has_mount_limits),
        "antenna_stop": True,
        "abort_observation": True,
        "observation_start": True,
        "rsky": True,
        "skydip": True,
        "target_tracking": True,
        "chopper": bool(has_chopper_positions),
        "chopper_move": bool(has_chopper_positions),
        "chopper_status": bool(has_chopper_positions),
        "chopper_maintenance": bool(has_chopper_positions and _maintenance_supported(observatory)),
    }


def _summary_from_necst_config(config: Any) -> SiteConfigSummary:
    observatory = _normalize_observatory(getattr(config, "observatory", None))
    mount_limits = _mount_dict(
        getattr(config, "antenna_drive_critical_limit_az", None),
        getattr(config, "antenna_drive_critical_limit_el", None),
    )
    warning_limits = _mount_dict(
        getattr(config, "antenna_drive_warning_limit_az", None),
        getattr(config, "antenna_drive_warning_limit_el", None),
    )
    drive_range = _mount_dict(
        getattr(config, "antenna_drive_range_az", None),
        getattr(config, "antenna_drive_range_el", None),
    )

    chopper_position = _as_mapping(getattr(config, "chopper_motor_position", None))
    insert_position = _finite_float(chopper_position.get("insert"))
    remove_position = _finite_float(chopper_position.get("remove"))
    chopper = {
        "available": insert_position is not None and remove_position is not None,
        "insert_position": int(insert_position) if insert_position is not None else None,
        "remove_position": int(remove_position) if remove_position is not None else None,
        "maintenance_supported": bool(_maintenance_supported(observatory)),
    }
    capabilities = _capabilities_from_summary(
        observatory=observatory,
        mount_limits=mount_limits,
        chopper=chopper,
    )
    warnings: list[str] = []
    if not mount_limits:
        warnings.append("antenna drive critical limits are unavailable from necst.config")
    if not chopper["available"]:
        warnings.append("chopper motor insert/remove positions are unavailable from necst.config")
    return SiteConfigSummary(
        source="necst.config",
        source_path=None,
        observatory=observatory,
        mount_limits=mount_limits,
        mount_warning_limits=warning_limits,
        mount_drive_range=drive_range,
        chopper=chopper,
        capabilities=capabilities,
        warnings=warnings,
    )


def _read_toml(path: Path) -> Mapping[str, Any]:
    if tomllib is None:
        raise RuntimeError("tomllib/tomli is unavailable; cannot read site TOML")
    with path.open("rb") as fh:
        data = tomllib.load(fh)  # type: ignore[union-attr]
    return data if isinstance(data, Mapping) else {}


def _strip_inline_comment(line: str) -> str:
    in_single = False
    in_double = False
    out: list[str] = []
    prev = ""
    for ch in line:
        if ch == "'" and not in_double:
            in_single = not in_single
        elif ch == '"' and not in_single and prev != "\\":
            in_double = not in_double
        if ch == "#" and not in_single and not in_double:
            break
        out.append(ch)
        prev = ch
    return "".join(out).strip()


def _parse_toml_array_value(value: str) -> list[str]:
    m = re.search(r"\[(.*)\]", value)
    if not m:
        return []
    body = m.group(1)
    items: list[str] = []
    token: list[str] = []
    in_single = False
    in_double = False
    prev = ""
    for ch in body:
        if ch == "'" and not in_double:
            in_single = not in_single
            continue
        if ch == '"' and not in_single and prev != "\\":
            in_double = not in_double
            continue
        if ch == "," and not in_single and not in_double:
            item = "".join(token).strip()
            if item:
                items.append(item)
            token = []
        else:
            token.append(ch)
        prev = ch
    item = "".join(token).strip()
    if item:
        items.append(item)
    return items


def _parse_inline_position(value: str) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for key in ("insert", "remove"):
        m = re.search(rf"\b{key}\s*=\s*([+-]?(?:\d+(?:\.\d*)?|\.\d+))", value)
        if m:
            out[key] = float(m.group(1))
    return out


def _summary_from_lenient_toml(path: Path, *, parse_error: Exception) -> SiteConfigSummary:
    """Extract console-critical fields even if unrelated TOML sections fail.

    Some development/default configuration files may contain duplicate sections
    outside the fields needed by the console.  For operator safety we still read
    only the top-level observatory, antenna drive limits, and chopper endpoints;
    if these cannot be extracted, the corresponding capability stays disabled.
    """

    section = ""
    observatory: Optional[str] = None
    antenna: Dict[str, Any] = {}
    chopper_position: Dict[str, Any] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = _strip_inline_comment(raw_line)
        if not line:
            continue
        if line.startswith("[") and line.endswith("]"):
            section = line.strip("[]").strip()
            continue
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip().strip('"').strip("'")
        value = value.strip()
        if section == "" and key == "observatory":
            observatory = _normalize_observatory(value.strip().strip('"').strip("'"))
        elif section == "antenna" and key in {
            "drive_critical_limit_az",
            "drive_critical_limit_el",
            "drive_warning_limit_az",
            "drive_warning_limit_el",
            "drive_range_az",
            "drive_range_el",
        }:
            antenna[key] = _parse_toml_array_value(value)
        elif section == "chopper_motor" and key == "position":
            chopper_position.update(_parse_inline_position(value))

    mount_limits = _mount_dict(
        antenna.get("drive_critical_limit_az"),
        antenna.get("drive_critical_limit_el"),
    )
    warning_limits = _mount_dict(
        antenna.get("drive_warning_limit_az"),
        antenna.get("drive_warning_limit_el"),
    )
    drive_range = _mount_dict(
        antenna.get("drive_range_az"),
        antenna.get("drive_range_el"),
    )
    insert_position = _finite_float(chopper_position.get("insert"))
    remove_position = _finite_float(chopper_position.get("remove"))
    chopper = {
        "available": insert_position is not None and remove_position is not None,
        "insert_position": int(insert_position) if insert_position is not None else None,
        "remove_position": int(remove_position) if remove_position is not None else None,
        "maintenance_supported": bool(_maintenance_supported(observatory)),
    }
    capabilities = _capabilities_from_summary(
        observatory=observatory,
        mount_limits=mount_limits,
        chopper=chopper,
    )
    warnings = [
        "standard TOML parser failed; used lenient field scan for console-critical site settings: "
        f"{parse_error}"
    ]
    if not mount_limits:
        warnings.append(f"antenna drive critical limits are unavailable in {path}")
    if not chopper["available"]:
        warnings.append(f"chopper motor insert/remove positions are unavailable in {path}")
    return SiteConfigSummary(
        source="toml-lenient",
        source_path=str(path),
        observatory=observatory,
        mount_limits=mount_limits,
        mount_warning_limits=warning_limits,
        mount_drive_range=drive_range,
        chopper=chopper,
        capabilities=capabilities,
        warnings=warnings,
    )


def _summary_from_toml(path: Path) -> SiteConfigSummary:
    try:
        raw = _read_toml(path)
    except Exception as exc:
        return _summary_from_lenient_toml(path, parse_error=exc)
    observatory = _normalize_observatory(raw.get("observatory"))
    antenna = _as_mapping(raw.get("antenna"))
    chopper_motor = _as_mapping(raw.get("chopper_motor"))
    position = _as_mapping(chopper_motor.get("position"))

    mount_limits = _mount_dict(
        antenna.get("drive_critical_limit_az"),
        antenna.get("drive_critical_limit_el"),
    )
    warning_limits = _mount_dict(
        antenna.get("drive_warning_limit_az"),
        antenna.get("drive_warning_limit_el"),
    )
    drive_range = _mount_dict(
        antenna.get("drive_range_az"),
        antenna.get("drive_range_el"),
    )
    insert_position = _finite_float(position.get("insert"))
    remove_position = _finite_float(position.get("remove"))
    chopper = {
        "available": insert_position is not None and remove_position is not None,
        "insert_position": int(insert_position) if insert_position is not None else None,
        "remove_position": int(remove_position) if remove_position is not None else None,
        "maintenance_supported": bool(_maintenance_supported(observatory)),
    }
    capabilities = _capabilities_from_summary(
        observatory=observatory,
        mount_limits=mount_limits,
        chopper=chopper,
    )
    warnings: list[str] = []
    if not mount_limits:
        warnings.append(f"antenna drive critical limits are unavailable in {path}")
    if not chopper["available"]:
        warnings.append(f"chopper motor insert/remove positions are unavailable in {path}")
    return SiteConfigSummary(
        source="toml",
        source_path=str(path),
        observatory=observatory,
        mount_limits=mount_limits,
        mount_warning_limits=warning_limits,
        mount_drive_range=drive_range,
        chopper=chopper,
        capabilities=capabilities,
        warnings=warnings,
    )


def _try_active_necst_config() -> Optional[SiteConfigSummary]:
    try:
        from necst import config as necst_config  # type: ignore
    except Exception:
        return None
    try:
        return _summary_from_necst_config(necst_config)
    except Exception as exc:
        return SiteConfigSummary(
            source="necst.config",
            warnings=[f"failed to summarize necst.config: {exc}"],
            capabilities=_capabilities_from_summary(
                observatory=None,
                mount_limits={},
                chopper={},
            ),
        )


def _apply_mount_fallbacks(
    summary: SiteConfigSummary,
    *,
    az_min: Optional[float] = None,
    az_max: Optional[float] = None,
    el_min: Optional[float] = None,
    el_max: Optional[float] = None,
) -> SiteConfigSummary:
    mount_limits = dict(summary.mount_limits)
    fallback = {
        "az_min": az_min,
        "az_max": az_max,
        "el_min": el_min,
        "el_max": el_max,
    }
    used: list[str] = []
    for key, value in fallback.items():
        if key not in mount_limits and value is not None:
            number = _finite_float(value)
            if number is not None:
                mount_limits[key] = float(number)
                used.append(key)
    capabilities = _capabilities_from_summary(
        observatory=summary.observatory,
        mount_limits=mount_limits,
        chopper=summary.chopper,
    )
    warnings = list(summary.warnings)
    if used:
        warnings.append("using CLI fallback mount limit(s): " + ", ".join(sorted(used)))
    return SiteConfigSummary(
        source=summary.source,
        source_path=summary.source_path,
        observatory=summary.observatory,
        mount_limits=mount_limits,
        mount_warning_limits=dict(summary.mount_warning_limits),
        mount_drive_range=dict(summary.mount_drive_range),
        chopper=dict(summary.chopper),
        capabilities=capabilities,
        warnings=warnings,
    )


def resolve_site_config(
    *,
    site_config_path: Optional[os.PathLike[str] | str] = None,
    az_min: Optional[float] = None,
    az_max: Optional[float] = None,
    el_min: Optional[float] = None,
    el_max: Optional[float] = None,
) -> SiteConfigSummary:
    """Resolve active site configuration for the Operator Console."""

    warnings: list[str] = []
    if site_config_path:
        path = Path(site_config_path).expanduser()
        try:
            summary = _summary_from_toml(path)
        except Exception as exc:
            summary = SiteConfigSummary(
                source="toml",
                source_path=str(path),
                warnings=[f"failed to read site config TOML {path}: {exc}"],
                capabilities=_capabilities_from_summary(
                    observatory=None,
                    mount_limits={},
                    chopper={},
                ),
            )
        return _apply_mount_fallbacks(
            summary,
            az_min=az_min,
            az_max=az_max,
            el_min=el_min,
            el_max=el_max,
        )

    active = _try_active_necst_config()
    if active is None:
        warnings.append("necst.config is unavailable; site TOML was not explicitly provided")
        active = SiteConfigSummary(
            source="fallback",
            warnings=warnings,
            capabilities=_capabilities_from_summary(
                observatory=None,
                mount_limits={},
                chopper={},
            ),
        )
    return _apply_mount_fallbacks(
        active,
        az_min=az_min,
        az_max=az_max,
        el_min=el_min,
        el_max=el_max,
    )
