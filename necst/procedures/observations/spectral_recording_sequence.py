"""Observation-flow helper for spectral-recording setup sidecars and gate control."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
import datetime as _datetime
import math
from typing import Any, Dict, Mapping, Optional, Tuple

from ... import config
from ...rx.spectral_recording_setup import (
    dumps_toml,
    load_and_resolve_spectral_recording_setup,
    read_toml,
    sha256_text,
    validate_snapshot,
)
from .pointing_reference_beam import (
    attach_pointing_reference_to_snapshot,
    build_pointing_reference_context,
)

_CANONICAL_SIDECAR_NAMES = {
    "snapshot": "spectral_recording_snapshot.toml",
    "lo_profile": "lo_profile.toml",
    "recording_window_setup": "recording_window_setup.toml",
    "beam_model": "beam_model.toml",
}


@dataclass(frozen=True)
class SpectralRecordingObservationSetup:
    setup_id: str
    setup_hash: str
    snapshot_toml: str
    sidecars: Tuple[Tuple[str, str], ...] = field(default_factory=tuple)
    setup_override_policy: str = "strict"

    @property
    def strict(self) -> bool:
        return self.setup_override_policy == "strict"


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "true", "yes", "on", "enable", "enabled"}


def _optional_str(params: Mapping[str, Any], *keys: str) -> Optional[str]:
    for key in keys:
        if key in params and params[key] not in (None, ""):
            return str(params[key])
    return None


def _resolve_path(path_text: Optional[str], *, base_dir: Path) -> Optional[Path]:
    if not path_text:
        return None
    path = Path(path_text).expanduser()
    if not path.is_absolute():
        path = base_dir / path
    return path.resolve()


def _quantity_to_float(value: Any, unit: str) -> Optional[float]:
    if value is None:
        return None
    try:
        if hasattr(value, "to_value"):
            return float(value.to_value(unit))
    except Exception:
        pass
    try:
        return float(value)
    except Exception:
        return None


def _site_from_params_or_config(params: Mapping[str, Any]) -> Dict[str, float]:
    lat = _quantity_to_float(params.get("velocity_reference_site_lat_deg", params.get("site_lat_deg")), "deg")
    lon = _quantity_to_float(params.get("velocity_reference_site_lon_deg", params.get("site_lon_deg")), "deg")
    elev = _quantity_to_float(params.get("velocity_reference_site_elev_m", params.get("site_elev_m")), "m")
    if lat is None or lon is None:
        loc = getattr(config, "location", None)
        lat = lat if lat is not None else _quantity_to_float(getattr(loc, "lat", None), "deg")
        lon = lon if lon is not None else _quantity_to_float(getattr(loc, "lon", None), "deg")
        elev = elev if elev is not None else _quantity_to_float(getattr(loc, "height", None), "m")
    out: Dict[str, float] = {}
    if lat is not None:
        out["site_lat_deg"] = float(lat)
    if lon is not None:
        out["site_lon_deg"] = float(lon)
    out["site_elev_m"] = float(0.0 if elev is None else elev)
    return out



_SOLAR_SYSTEM_TARGETS = {
    "sun": "sun",
    "moon": "moon",
    "mercury": "mercury",
    "venus": "venus",
    "mars": "mars",
    "jupiter": "jupiter",
    "saturn": "saturn",
    "uranus": "uranus",
    "neptune": "neptune",
}


def _site_to_earth_location(site: Mapping[str, float]) -> Any:
    """Return an Astropy EarthLocation from a site dict, or None."""

    if not {"site_lat_deg", "site_lon_deg"} <= set(site):
        return None
    try:
        from astropy.coordinates import EarthLocation  # type: ignore
        from astropy import units as u  # type: ignore

        return EarthLocation(
            lat=float(site["site_lat_deg"]) * u.deg,
            lon=float(site["site_lon_deg"]) * u.deg,
            height=float(site.get("site_elev_m", 0.0)) * u.m,
        )
    except Exception:
        return None


def _skycoord_to_icrs_deg(coord: Any) -> Dict[str, float]:
    try:
        from astropy import units as u  # type: ignore

        icrs = coord.icrs
        return {"ra_deg": float(icrs.ra.to_value(u.deg)), "dec_deg": float(icrs.dec.to_value(u.deg))}
    except Exception:
        return {}


def _icrs_from_lon_lat_frame(
    lon_deg: float,
    lat_deg: float,
    frame: str,
    *,
    reference_time_utc: Optional[str] = None,
    site: Optional[Mapping[str, float]] = None,
) -> Dict[str, float]:
    """Convert explicit longitude/latitude in a known frame to ICRS."""

    frame_l = str(frame).strip().lower()
    try:
        from astropy.coordinates import AltAz, SkyCoord  # type: ignore
        from astropy.time import Time  # type: ignore
        from astropy import units as u  # type: ignore

        if frame_l in {"fk5", "icrs", "j2000", "radec", "equatorial"}:
            astropy_frame = "icrs" if frame_l in {"icrs", "j2000", "radec", "equatorial"} else "fk5"
            return _skycoord_to_icrs_deg(SkyCoord(float(lon_deg) * u.deg, float(lat_deg) * u.deg, frame=astropy_frame))
        if frame_l in {"galactic", "gal"}:
            return _skycoord_to_icrs_deg(SkyCoord(l=float(lon_deg) * u.deg, b=float(lat_deg) * u.deg, frame="galactic"))
        if frame_l in {"altaz", "azel", "az_el"}:
            location = _site_to_earth_location(site or {})
            if location is None or not reference_time_utc:
                return {}
            obstime = Time(str(reference_time_utc).replace("Z", "+00:00"))
            altaz = AltAz(
                az=float(lon_deg) * u.deg,
                alt=float(lat_deg) * u.deg,
                obstime=obstime,
                location=location,
            )
            return _skycoord_to_icrs_deg(SkyCoord(altaz))
    except Exception:
        if frame_l in {"fk5", "icrs", "j2000", "radec", "equatorial"}:
            return {"ra_deg": float(lon_deg), "dec_deg": float(lat_deg)}
    return {}


def _reference_icrs_from_params_or_obsspec(
    params: Mapping[str, Any],
    obsspec: Any,
    *,
    reference_time_utc: Optional[str] = None,
    site: Optional[Mapping[str, float]] = None,
) -> Dict[str, float]:
    """Resolve the velocity-reference direction to ICRS degrees.

    This helper is intentionally deterministic:

    - Explicit RA/Dec parameters are used directly.
    - Explicit lon/lat/frame parameters are converted with Astropy when needed.
    - Explicit `.obs` reference coordinates are used when available, including
      Galactic coordinates.
    - Solar-system body names are resolved with Astropy's built-in ephemeris.
    - Arbitrary target names are not sent to network/Sesame resolvers.
    """

    ra = _quantity_to_float(
        params.get("velocity_reference_ra_deg", params.get("vlsr_reference_ra_deg")),
        "deg",
    )
    dec = _quantity_to_float(
        params.get("velocity_reference_dec_deg", params.get("vlsr_reference_dec_deg")),
        "deg",
    )
    if ra is not None and dec is not None:
        return {"ra_deg": float(ra), "dec_deg": float(dec)}

    l_deg = _quantity_to_float(params.get("velocity_reference_l_deg", params.get("vlsr_reference_l_deg")), "deg")
    b_deg = _quantity_to_float(params.get("velocity_reference_b_deg", params.get("vlsr_reference_b_deg")), "deg")
    if l_deg is not None and b_deg is not None:
        converted = _icrs_from_lon_lat_frame(
            float(l_deg),
            float(b_deg),
            "galactic",
            reference_time_utc=reference_time_utc,
            site=site,
        )
        if converted:
            return converted

    lon = _quantity_to_float(
        params.get("velocity_reference_lon_deg", params.get("vlsr_reference_lon_deg")),
        "deg",
    )
    lat = _quantity_to_float(
        params.get("velocity_reference_lat_deg", params.get("vlsr_reference_lat_deg")),
        "deg",
    )
    frame = params.get("velocity_reference_frame", params.get("vlsr_reference_frame"))
    if lon is not None and lat is not None and frame:
        converted = _icrs_from_lon_lat_frame(
            float(lon),
            float(lat),
            str(frame),
            reference_time_utc=reference_time_utc,
            site=site,
        )
        if converted:
            return converted

    reference = None
    try:
        reference = getattr(obsspec, "_reference")
    except Exception:
        reference = None
    if isinstance(reference, (tuple, list)) and len(reference) >= 3:
        ref_lon = _quantity_to_float(reference[0], "deg")
        ref_lat = _quantity_to_float(reference[1], "deg")
        ref_frame = str(reference[2]).strip().lower()
        if ref_lon is not None and ref_lat is not None:
            converted = _icrs_from_lon_lat_frame(
                float(ref_lon),
                float(ref_lat),
                ref_frame,
                reference_time_utc=reference_time_utc,
                site=site,
            )
            if converted:
                return converted

    target_name = (
        params.get("velocity_reference_target")
        or params.get("vlsr_reference_target")
        or getattr(obsspec, "target", None)
    )
    if target_name is not None:
        key = str(target_name).strip().lower().replace(" ", "")
        if key in _SOLAR_SYSTEM_TARGETS and reference_time_utc:
            try:
                from astropy.coordinates import get_body, get_sun  # type: ignore
                from astropy.time import Time  # type: ignore

                obstime = Time(str(reference_time_utc).replace("Z", "+00:00"))
                if key == "sun":
                    return _skycoord_to_icrs_deg(get_sun(obstime))
                location = _site_to_earth_location(site or {})
                return _skycoord_to_icrs_deg(get_body(_SOLAR_SYSTEM_TARGETS[key], obstime, location))
            except Exception:
                return {}

    # Do not silently resolve arbitrary target names here.  Network/Sesame name
    # resolution is intentionally avoided in the observation-time setup resolver.
    return {}


def _build_velocity_reference_context(params: Mapping[str, Any], obsspec: Any) -> Optional[Dict[str, Any]]:
    """Build the reference context used by observation-time LSRK window resolution.

    The reference time is the setup resolution time, i.e. the command-time before
    the recorder starts.  The reference direction is determined in this order:

    1. explicit velocity_reference_ra_deg/velocity_reference_dec_deg,
    2. explicit velocity_reference_l_deg/velocity_reference_b_deg or
       velocity_reference_lon_deg/lat_deg/frame,
    3. explicit `.obs` reference coordinates, including Galactic coordinates,
    4. deterministic solar-system body names such as Sun or Moon.

    Arbitrary target names are not resolved via network catalogs.
    """

    now = _datetime.datetime.now(_datetime.timezone.utc).isoformat().replace("+00:00", "Z")
    reference_time_utc = str(params.get("velocity_reference_time_utc", params.get("vlsr_reference_time_utc", now)))
    site = _site_from_params_or_config(params)
    coords = _reference_icrs_from_params_or_obsspec(
        params,
        obsspec,
        reference_time_utc=reference_time_utc,
        site=site,
    )
    if not {"ra_deg", "dec_deg"} <= set(coords) or not {"site_lat_deg", "site_lon_deg"} <= set(site):
        return None
    out: Dict[str, Any] = {}
    out.update(coords)
    out.update(site)
    out["reference_time_utc"] = reference_time_utc
    out["reference_time_policy"] = "setup_resolve_time_utc"
    out["reference_coordinate_policy"] = "explicit_or_obs_reference_or_solar_system_body"
    return out


def _obs_base_dir(obs_file: Any) -> Path:
    try:
        path = Path(obs_file).expanduser()
    except TypeError:
        return Path.cwd()
    if path.parent == Path(""):
        return Path.cwd()
    return path.resolve().parent


def build_spectral_recording_observation_setup(
    *,
    params: Mapping[str, Any],
    obs_file: Any,
    default_setup_id: str,
    obsspec: Any = None,
) -> Optional[SpectralRecordingObservationSetup]:
    """Build a spectral-recording setup from `.obs` parameters.

    If no spectral-recording parameter is present, return None and preserve the
    legacy observation flow.
    """

    params = dict(params or {})
    snapshot_ref = _optional_str(params, "spectral_recording_snapshot", "spectral_recording_snapshot_path")
    lo_ref = _optional_str(params, "lo_profile", "lo_profile_path")
    rec_ref = _optional_str(params, "recording_window_setup", "recording_window_setup_path")
    beam_ref = _optional_str(params, "beam_model", "beam_model_path")
    pointing_ref = _optional_str(params, "pointing_reference_beam_id")
    explicit_enable = _truthy(params.get("spectral_recording", False)) or _truthy(
        params.get("use_spectral_recording_setup", False)
    )

    # ``beam_model`` can be used only for pointing-reference-beam correction or
    # as optional stream geometry for spectral recording.  By itself it does not
    # define a spectral setup.  A lo_profile or an existing snapshot is the
    # actual stream-truth input; when no beam_model is supplied, spectral setup
    # resolution falls back to B00=(0,0) and rejects non-B00 stream beams.
    if not explicit_enable and not any((snapshot_ref, lo_ref, rec_ref)):
        return None

    base_dir = _obs_base_dir(obs_file)
    setup_id = _optional_str(params, "spectral_recording_setup_id", "setup_id") or str(default_setup_id)
    policy = _optional_str(params, "setup_override_policy", "spectral_recording_setup_override_policy") or "strict"
    if policy not in {"strict", "warn", "force", "legacy"}:
        raise ValueError(f"Unsupported setup_override_policy: {policy!r}")
    if policy == "legacy":
        return None

    pointing_reference_context = build_pointing_reference_context(
        params,
        obs_file=obs_file,
    )
    velocity_reference_context = _build_velocity_reference_context(params, obsspec)

    sidecars: Dict[str, str] = {}
    snapshot_path = _resolve_path(snapshot_ref, base_dir=base_dir)
    if snapshot_path is not None:
        snapshot_toml = snapshot_path.read_text(encoding="utf-8")
        snapshot = read_toml(snapshot_path)
        validate_snapshot(snapshot)
        beam_path = _resolve_path(beam_ref, base_dir=base_dir)
        if beam_path is not None:
            sidecars[_CANONICAL_SIDECAR_NAMES["beam_model"]] = beam_path.read_text(encoding="utf-8")
    else:
        lo_path = _resolve_path(lo_ref, base_dir=base_dir)
        beam_path = _resolve_path(beam_ref, base_dir=base_dir)
        rec_path = _resolve_path(rec_ref, base_dir=base_dir)
        if lo_path is None:
            raise ValueError(
                "Spectral recording setup requires either spectral_recording_snapshot "
                "or lo_profile. beam_model is optional only for B00=(0,0) stream geometry."
            )
        snapshot = load_and_resolve_spectral_recording_setup(
            lo_profile_path=lo_path,
            recording_window_setup_path=rec_path,
            beam_model_path=beam_path,
            setup_id=setup_id,
            setup_override_policy=policy,
            velocity_reference_context=velocity_reference_context,
        )
        validate_snapshot(snapshot)
        snapshot_toml = dumps_toml(snapshot)
        sidecars[_CANONICAL_SIDECAR_NAMES["lo_profile"]] = lo_path.read_text(encoding="utf-8")
        if rec_path is not None:
            sidecars[_CANONICAL_SIDECAR_NAMES["recording_window_setup"]] = rec_path.read_text(encoding="utf-8")
        if beam_path is not None:
            sidecars[_CANONICAL_SIDECAR_NAMES["beam_model"]] = beam_path.read_text(encoding="utf-8")

    if pointing_reference_context is not None:
        snapshot = attach_pointing_reference_to_snapshot(
            snapshot,
            pointing_reference_context,
            dumps_toml_func=dumps_toml,
            sha256_text_func=sha256_text,
        )
        validate_snapshot(snapshot)
        snapshot_toml = dumps_toml(snapshot)

    sidecars[_CANONICAL_SIDECAR_NAMES["snapshot"]] = snapshot_toml
    ordered_sidecars = tuple((name, sidecars[name]) for name in sorted(sidecars))
    setup_hash = str(snapshot.get("canonical_snapshot_sha256") or "")
    if not setup_hash:
        raise ValueError("Resolved snapshot does not contain canonical_snapshot_sha256.")
    return SpectralRecordingObservationSetup(
        setup_id=setup_id,
        setup_hash=setup_hash,
        snapshot_toml=snapshot_toml,
        sidecars=ordered_sidecars,
        setup_override_policy=policy,
    )


def reject_legacy_recording_kwargs_for_setup(kwargs: Mapping[str, Any], setup: Optional[SpectralRecordingObservationSetup]) -> None:
    """Reject active legacy per-observation spectral controls in setup mode.

    In setup mode, stream-local saved channel windows and TP policy are already
    encoded in the resolved snapshot. Accepting legacy ``ch``/``tp_mode``/
    ``tp_range`` controls would be ambiguous and can appear to succeed while
    being ignored by SpectralData.

    Command-line wrappers often pass inactive defaults such as ``ch=None`` even
    when the user did not request legacy binning.  Those inert defaults must not
    disable spectral-recording setup mode.  This function therefore rejects only
    values that would actually trigger the legacy controls in
    :meth:`Observation.execute`.
    """
    if setup is None:
        return

    conflicts = []
    if kwargs.get("ch") is not None:
        conflicts.append("ch")
    if bool(kwargs.get("tp_mode", False)):
        conflicts.append("tp_mode")
    tp_range = kwargs.get("tp_range", None)
    if tp_range is not None and len(tp_range) > 0:
        conflicts.append("tp_range")

    if conflicts:
        raise ValueError(
            "spectral recording setup cannot be combined with legacy observation kwargs: "
            + ", ".join(conflicts)
        )

def apply_setup_with_commander(com: Any, setup: SpectralRecordingObservationSetup) -> Any:
    response = com.apply_spectral_recording_setup(
        snapshot_toml=setup.snapshot_toml,
        snapshot_sha256=setup.setup_hash,
        setup_id=setup.setup_id,
        strict=setup.strict,
    )
    if response is not None and hasattr(response, "success") and not bool(response.success):
        errors = getattr(response, "errors", []) or []
        raise RuntimeError("Failed to apply spectral recording setup: " + "; ".join(map(str, errors)))
    return response


def save_sidecars_with_commander(com: Any, setup: SpectralRecordingObservationSetup) -> None:
    for name, content in setup.sidecars:
        response = com.record("file", name=name, content=content)
        if response is not None and hasattr(response, "success") and not bool(response.success):
            raise RuntimeError(f"Failed to save spectral recording sidecar {name!r}")


def set_gate_with_commander(com: Any, setup: SpectralRecordingObservationSetup, *, allow_save: bool) -> Any:
    response = com.set_spectral_recording_gate(
        setup_id=setup.setup_id,
        setup_hash=setup.setup_hash,
        allow_save=bool(allow_save),
    )
    if response is not None and hasattr(response, "success") and not bool(response.success):
        errors = getattr(response, "errors", []) or []
        state = "open" if allow_save else "close"
        raise RuntimeError(f"Failed to {state} spectral recording setup gate: " + "; ".join(map(str, errors)))
    return response


def clear_setup_with_commander(com: Any, setup: SpectralRecordingObservationSetup, *, strict: bool = True) -> Any:
    response = com.clear_spectral_recording_setup(
        setup_id=setup.setup_id,
        setup_hash=setup.setup_hash,
        strict=bool(strict),
    )
    if response is not None and hasattr(response, "success") and not bool(response.success):
        errors = getattr(response, "errors", []) or []
        raise RuntimeError("Failed to clear spectral recording setup: " + "; ".join(map(str, errors)))
    return response
