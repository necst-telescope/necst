"""Site configuration summary for the NECST operator console."""

from __future__ import annotations

import os
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional

from . import node_health

try:
    import tomllib  # Python 3.11+
except Exception:  # pragma: no cover
    tomllib = None  # type: ignore


_DEFAULT_CAPABILITIES: Dict[str, bool] = {
    "mount_move": True,
    "target_tracking": True,
    "observation_start": True,
    "rsky": True,
    "skydip": True,
    "chopper_status": True,
    "chopper_move": True,
    "chopper_maintenance": True,
}


@dataclass
class SiteConfigSummary:
    source: str = "defaults"
    source_path: Optional[str] = None
    observatory: Optional[str] = None
    mount_limits: Dict[str, float] = field(default_factory=dict)
    capabilities: Dict[str, bool] = field(default_factory=lambda: dict(_DEFAULT_CAPABILITIES))
    chopper: Dict[str, Any] = field(default_factory=dict)
    observation_log: Dict[str, Any] = field(default_factory=dict)
    health: node_health.NodeHealthConfig = field(default_factory=node_health.NodeHealthConfig)
    warnings: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "source": self.source,
            "source_path": self.source_path,
            "observatory": self.observatory,
            "mount_limits": dict(self.mount_limits),
            "capabilities": dict(self.capabilities),
            "chopper": dict(self.chopper),
            "observation_log": dict(self.observation_log),
            "health": self.health.to_dict(),
            "warnings": list(self.warnings),
        }


def _parse_deg(value: Any) -> Optional[float]:
    if value in (None, ""):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    match = re.search(r"[-+]?\d+(?:\.\d*)?(?:[eE][-+]?\d+)?", text)
    if not match:
        return None
    return float(match.group(0))


def _read_toml(path: Path) -> Dict[str, Any]:
    if tomllib is None:
        raise RuntimeError("tomllib is unavailable")
    with path.open("rb") as fh:
        payload = tomllib.load(fh)
    return payload if isinstance(payload, dict) else {}


def _telescope_config_filename() -> str:
    telescope = str(os.environ.get("TELESCOPE", os.environ.get("NECST_TELESCOPE", ""))).strip()
    return f"{telescope}_config.toml" if telescope else "config.toml"


def _candidate_config_paths(site_config_path: Optional[os.PathLike[str] | str]) -> List[Path]:
    raw: List[str] = []
    if site_config_path not in (None, ""):
        raw.append(str(site_config_path))

    # Explicit overrides first.  These are console-specific escape hatches and
    # keep working even if neclib is not importable in a reduced smoke-test env.
    for env_name in ("NECST_SITE_CONFIG", "NECST_CONFIG", "NECLIB_CONFIG", "NECST_CONFIG_PATH"):
        value = str(os.environ.get(env_name, "")).strip()
        if value:
            raw.append(value)

    # Match neclib's normal configuration lookup before falling back to bundled
    # defaults.  The actual telescope site TOML is usually
    #   $NECST_ROOT/${TELESCOPE}_config.toml
    # or
    #   ~/.necst/${TELESCOPE}_config.toml
    # so omitting these paths makes console-only additions such as
    # [console.health] invisible on the real telescope.
    config_file_name = _telescope_config_filename()
    necst_root = str(os.environ.get("NECST_ROOT", "")).strip()
    if necst_root:
        raw.append(str(Path(necst_root).expanduser() / config_file_name))
    raw.append(str(Path.home() / ".necst" / config_file_name))

    try:
        from neclib.core.configuration import find_config  # type: ignore

        raw.append(str(find_config()))
    except Exception:
        pass

    telescope_key = str(os.environ.get("TELESCOPE", os.environ.get("NECST_TELESCOPE", ""))).strip().lower()
    default_dirs: List[Path] = []
    source_tree_defaults = Path(__file__).resolve().parents[3] / "neclib-main" / "neclib" / "defaults"
    default_dirs.append(source_tree_defaults)
    try:
        import neclib  # type: ignore

        default_dirs.append(Path(neclib.__file__).resolve().parent / "defaults")
    except Exception:
        pass
    # In source-tree fallback mode, source_tree_defaults works.  In an installed
    # ROS/colcon environment, the importable neclib defaults path is preferred if
    # available.  Duplicates are removed below.
    for defaults_dir in default_dirs:
        if telescope_key:
            raw.append(str(defaults_dir / config_file_name))
            if "nanten2" in telescope_key:
                raw.append(str(defaults_dir / "NANTEN2_config.toml"))
            elif "1p85" in telescope_key or "1.85" in telescope_key or "omu" in telescope_key:
                raw.append(str(defaults_dir / "OMU1p85m_config.toml"))
        raw.append(str(defaults_dir / "config.toml"))
    paths: List[Path] = []
    seen = set()
    for item in raw:
        try:
            path = Path(item).expanduser()
        except Exception:
            continue
        key = str(path)
        if key not in seen:
            seen.add(key)
            paths.append(path)
    return paths


def _limits_from_config(config: Mapping[str, Any]) -> Dict[str, float]:
    antenna = config.get("antenna") if isinstance(config.get("antenna"), Mapping) else {}
    limits: Dict[str, float] = {}
    az_range = antenna.get("drive_range_az") if isinstance(antenna, Mapping) else None
    el_range = antenna.get("drive_range_el") if isinstance(antenna, Mapping) else None
    if isinstance(az_range, (list, tuple)) and len(az_range) >= 2:
        az_min = _parse_deg(az_range[0])
        az_max = _parse_deg(az_range[1])
        if az_min is not None and az_max is not None:
            limits["az_min"] = az_min
            limits["az_max"] = az_max
    if isinstance(el_range, (list, tuple)) and len(el_range) >= 2:
        el_min = _parse_deg(el_range[0])
        el_max = _parse_deg(el_range[1])
        if el_min is not None and el_max is not None:
            limits["el_min"] = el_min
            limits["el_max"] = el_max
    return limits


def _chopper_from_config(config: Mapping[str, Any]) -> Dict[str, Any]:
    motor = config.get("chopper_motor") if isinstance(config.get("chopper_motor"), Mapping) else {}
    pos = motor.get("position") if isinstance(motor, Mapping) and isinstance(motor.get("position"), Mapping) else {}
    insert = pos.get("insert") if isinstance(pos, Mapping) else None
    remove = pos.get("remove") if isinstance(pos, Mapping) else None
    available = bool(motor) and insert not in (None, "") and remove not in (None, "")
    return {
        "available": available,
        "insert_position": insert,
        "remove_position": remove,
        "maintenance_supported": available,
    }


def _capabilities_from_config(config: Mapping[str, Any], chopper: Mapping[str, Any]) -> Dict[str, bool]:
    caps = dict(_DEFAULT_CAPABILITIES)
    custom = config.get("operator_console") if isinstance(config.get("operator_console"), Mapping) else {}
    custom_caps = custom.get("capabilities") if isinstance(custom.get("capabilities"), Mapping) else {}
    for key, value in custom_caps.items():
        caps[str(key)] = bool(value)
    if not bool(chopper.get("available")):
        caps["chopper_move"] = False
        caps["chopper_maintenance"] = False
    return caps




def _observation_log_from_config(config: Mapping[str, Any]) -> Dict[str, Any]:
    """Return console observation-log settings from site TOML.

    Supported forms are intentionally small and console-local.  ``directory`` is
    the preferred key because the observation CSV must be written by the
    operator-console process, not necessarily by the recorder/spectrometer PC.
    """
    console = config.get("console") if isinstance(config.get("console"), Mapping) else {}
    raw = {}
    if isinstance(console, Mapping):
        for key in ("observation_log", "obslog"):
            value = console.get(key)
            if isinstance(value, Mapping):
                raw = dict(value)
                break
    out: Dict[str, Any] = {}
    warnings: List[str] = []

    for key in ("directory", "dir", "log_dir", "path"):
        value = raw.get(key)
        if value not in (None, ""):
            out["directory"] = str(value).strip()
            break

    for key in ("prefix", "filename_prefix"):
        value = raw.get(key)
        if value not in (None, ""):
            out["prefix"] = str(value).strip()
            break

    for key in ("user", "observer", "operator"):
        value = raw.get(key)
        if value not in (None, ""):
            out["user"] = str(value).strip()
            break

    record_roots: List[str] = []
    for key in ("record_root", "record_roots"):
        value = raw.get(key)
        if value in (None, ""):
            continue
        if isinstance(value, (list, tuple)):
            record_roots.extend(str(item).strip() for item in value if str(item or "").strip())
        else:
            record_roots.append(str(value).strip())
    if record_roots:
        out["record_roots"] = record_roots
        warnings.append(
            "console.observation_log.record_root is only a fallback candidate for <record_root>/obslogs; "
            "prefer console.observation_log.directory when the recorder root is on another PC or is not mounted"
        )

    if warnings:
        out["warnings"] = warnings
    return out

def resolve_site_config(
    *,
    site_config_path: Optional[os.PathLike[str] | str] = None,
    az_min: Optional[float] = None,
    az_max: Optional[float] = None,
    el_min: Optional[float] = None,
    el_max: Optional[float] = None,
) -> SiteConfigSummary:
    warnings: List[str] = []
    config: Dict[str, Any] = {}
    source = "unresolved"
    source_path: Optional[str] = None
    for path in _candidate_config_paths(site_config_path):
        if not path.exists():
            continue
        try:
            config = _read_toml(path)
            source = "site_config_path" if site_config_path not in (None, "") and Path(site_config_path).expanduser() == path else "default_config"
            source_path = str(path)
            break
        except Exception as exc:
            warnings.append(f"failed to read site config {path}: {exc}")
    if not config:
        warnings.append("no site TOML config found; using conservative console defaults")
    limits = _limits_from_config(config)
    overrides = {"az_min": az_min, "az_max": az_max, "el_min": el_min, "el_max": el_max}
    for key, value in overrides.items():
        if value is not None:
            try:
                limits[key] = float(value)
            except Exception:
                warnings.append(f"invalid CLI mount limit {key}={value!r}")
    chopper = _chopper_from_config(config)
    caps = _capabilities_from_config(config, chopper)
    obslog = _observation_log_from_config(config)
    health = node_health.config_from_mapping(config)
    observatory = config.get("observatory") if isinstance(config, Mapping) else None
    return SiteConfigSummary(
        source=source,
        source_path=source_path,
        observatory=str(observatory) if observatory not in (None, "") else None,
        mount_limits=limits,
        capabilities=caps,
        chopper=chopper,
        observation_log=obslog,
        health=health,
        warnings=warnings + list(obslog.get("warnings", [])) + list(health.warnings),
    )
