"""Observation-flow helper for spectral-recording setup sidecars and gate control."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Mapping, Optional, Tuple

from ...rx.spectral_recording_setup import (
    dumps_toml,
    load_and_resolve_spectral_recording_setup,
    read_toml,
    validate_snapshot,
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
    explicit_enable = _truthy(params.get("spectral_recording", False)) or _truthy(
        params.get("use_spectral_recording_setup", False)
    )

    if not explicit_enable and not any((snapshot_ref, lo_ref, rec_ref, beam_ref)):
        return None

    base_dir = _obs_base_dir(obs_file)
    setup_id = _optional_str(params, "spectral_recording_setup_id", "setup_id") or str(default_setup_id)
    policy = _optional_str(params, "setup_override_policy", "spectral_recording_setup_override_policy") or "strict"
    if policy not in {"strict", "warn", "force", "legacy"}:
        raise ValueError(f"Unsupported setup_override_policy: {policy!r}")
    if policy == "legacy":
        return None

    sidecars: Dict[str, str] = {}
    snapshot_path = _resolve_path(snapshot_ref, base_dir=base_dir)
    if snapshot_path is not None:
        snapshot_toml = snapshot_path.read_text(encoding="utf-8")
        snapshot = read_toml(snapshot_path)
        validate_snapshot(snapshot)
    else:
        lo_path = _resolve_path(lo_ref, base_dir=base_dir)
        beam_path = _resolve_path(beam_ref, base_dir=base_dir)
        rec_path = _resolve_path(rec_ref, base_dir=base_dir)
        if lo_path is None or beam_path is None:
            raise ValueError(
                "Spectral recording setup requires either spectral_recording_snapshot "
                "or both lo_profile and beam_model."
            )
        snapshot = load_and_resolve_spectral_recording_setup(
            lo_profile_path=lo_path,
            recording_window_setup_path=rec_path,
            beam_model_path=beam_path,
            setup_id=setup_id,
            setup_override_policy=policy,
        )
        validate_snapshot(snapshot)
        snapshot_toml = dumps_toml(snapshot)
        sidecars[_CANONICAL_SIDECAR_NAMES["lo_profile"]] = lo_path.read_text(encoding="utf-8")
        if rec_path is not None:
            sidecars[_CANONICAL_SIDECAR_NAMES["recording_window_setup"]] = rec_path.read_text(encoding="utf-8")
        sidecars[_CANONICAL_SIDECAR_NAMES["beam_model"]] = beam_path.read_text(encoding="utf-8")

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
    """Reject legacy per-observation spectral controls when a setup is active.

    In setup mode, stream-local saved channel windows and TP policy are already
    encoded in the resolved snapshot. Accepting legacy ``ch``/``tp_mode``/
    ``tp_range`` kwargs would be ambiguous and can appear to succeed while being
    ignored by SpectralData.
    """
    if setup is None:
        return
    conflicts = [key for key in ("ch", "tp_mode", "tp_range") if key in kwargs]
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
