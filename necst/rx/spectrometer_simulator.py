import math
from typing import Any, Dict, Mapping, Sequence, Tuple

import numpy as np
from necst_msgs.msg import ChopperMsg, Spectral

from .. import config, topic
from .spectral_recording_setup import (
    C_KM_PER_S,
    _linear_sky_axis_for_stream,
    _relativistic_doppler_factor_from_kms,
)
from .spectrometer import SpectralData


_ON_LINE_CONFIG_PREFIX = "spectrometer.simulator.on_line."
_ON_LINE_FIELDS = {"v_center_kms", "fwhm_kms", "t_line_peak_K"}


def parse_on_line_settings(
    config_items: Sequence[Tuple[str, Any]],
) -> Dict[str, Dict[str, float]]:
    """Parse window-keyed simulator signal settings from flattened NECST config."""
    raw: Dict[str, Dict[str, Any]] = {}
    for key, value in config_items:
        key = str(key)
        if not key.startswith(_ON_LINE_CONFIG_PREFIX):
            continue
        suffix = key[len(_ON_LINE_CONFIG_PREFIX) :]
        if "." not in suffix:
            raise ValueError(
                f"Simulator ON-line config key must include window_id and field: {key!r}"
            )
        window_id, field = suffix.rsplit(".", 1)
        if not window_id:
            raise ValueError(f"Simulator ON-line config has empty window_id: {key!r}")
        if field not in _ON_LINE_FIELDS:
            raise ValueError(
                f"Unknown simulator ON-line field {field!r} for window {window_id!r}"
            )
        raw.setdefault(window_id, {})[field] = value

    out: Dict[str, Dict[str, float]] = {}
    for window_id, values in raw.items():
        missing = _ON_LINE_FIELDS - set(values)
        if missing:
            raise ValueError(
                f"Simulator ON-line window {window_id!r} is missing fields: {sorted(missing)}"
            )
        center = float(values["v_center_kms"])
        fwhm = float(values["fwhm_kms"])
        peak = float(values["t_line_peak_K"])
        if not math.isfinite(center):
            raise ValueError(f"{window_id}.v_center_kms must be finite")
        if not math.isfinite(fwhm) or fwhm <= 0:
            raise ValueError(f"{window_id}.fwhm_kms must be positive finite")
        if not math.isfinite(peak) or peak < 0:
            raise ValueError(f"{window_id}.t_line_peak_K must be non-negative finite")
        out[window_id] = {
            "v_center_kms": center,
            "fwhm_kms": fwhm,
            "t_line_peak_K": peak,
        }
    return out


def _frequency_to_velocity_kms(
    rest_frequency_hz: float,
    frequency_hz: np.ndarray,
    definition: str,
) -> np.ndarray:
    """Inverse of the spectral setup velocity-to-frequency definitions."""
    rest = float(rest_frequency_hz)
    if rest <= 0:
        raise ValueError("rest_frequency_hz must be positive")
    ratio = np.asarray(frequency_hz, dtype=float) / rest
    definition = str(definition).strip().lower()
    if definition == "radio":
        return C_KM_PER_S * (1.0 - ratio)
    if definition == "optical":
        if np.any(ratio <= 0):
            raise ValueError("optical velocity conversion requires positive frequency")
        return C_KM_PER_S * (1.0 / ratio - 1.0)
    if definition == "relativistic":
        ratio2 = ratio * ratio
        return C_KM_PER_S * (1.0 - ratio2) / (1.0 + ratio2)
    raise ValueError(
        f"velocity_definition must be radio, optical, or relativistic; got {definition!r}"
    )


def velocity_axis_for_window(
    snapshot: Mapping[str, Any],
    stream: Mapping[str, Any],
    window: Mapping[str, Any],
) -> np.ndarray:
    """Return requested-frame velocity for every full channel of one window."""
    if str(window.get("kind", "")) != "velocity":
        raise ValueError(
            f"window {window.get('window_id', '')!r} is not velocity-resolved"
        )
    rest_frequency_hz = float(window["rest_frequency_hz"])
    start_hz, step_hz, full_nchan = _linear_sky_axis_for_stream(
        stream,
        frequency_axes=snapshot["frequency_axes"],
        lo_chains=snapshot["lo_chains"],
    )
    frequency_topocentric_hz = start_hz + np.arange(full_nchan, dtype=float) * step_hz

    requested_frame = str(
        window.get("requested_velocity_frame", "TOPOCENTRIC")
    ).strip().upper()
    if requested_frame in {"LSRK", "VLSRK"}:
        correction = window.get("vlsrk_correction_kms")
        if correction is None:
            raise ValueError(
                f"window {window.get('window_id', '')!r} is LSRK but has no vlsrk correction"
            )
        frequency_requested_hz = frequency_topocentric_hz / (
            _relativistic_doppler_factor_from_kms(float(correction))
        )
    else:
        frequency_requested_hz = frequency_topocentric_hz

    return _frequency_to_velocity_kms(
        rest_frequency_hz,
        frequency_requested_hz,
        str(window.get("velocity_definition", "radio")),
    )


def build_on_line_components(
    snapshot: Mapping[str, Any],
    line_settings: Mapping[str, Mapping[str, float]],
) -> Tuple[
    Dict[str, Dict[int, list]],
    Tuple[str, ...],
]:
    """Resolve window-keyed signal config to simulator device/board components."""
    components: Dict[str, Dict[int, list]] = {}
    matched = set()
    seen = set()

    for stream in snapshot.get("streams", {}).values():
        spectrometer_key = str(
            stream.get("raw_input_key", stream.get("spectrometer_key", ""))
        )
        board_id = int(stream.get("raw_board_id", stream.get("board_id", 0)))
        for window in stream.get("computed_windows", []):
            if str(window.get("kind", "")) == "contiguous_envelope":
                continue
            window_id = str(window.get("window_id", ""))
            if not window_id or window_id not in line_settings:
                continue
            identity = (spectrometer_key, board_id, window_id)
            if identity in seen:
                continue
            if str(window.get("kind", "")) != "velocity":
                raise ValueError(
                    f"Simulator ON-line window {window_id!r} must be a velocity window"
                )

            setting = line_settings[window_id]
            velocity_axis = velocity_axis_for_window(snapshot, stream, window)
            component = {
                "velocity_axis_kms": velocity_axis,
                "v_center_kms": float(setting["v_center_kms"]),
                "fwhm_kms": float(setting["fwhm_kms"]),
                "t_line_peak_K": float(setting["t_line_peak_K"]),
            }
            components.setdefault(spectrometer_key, {}).setdefault(board_id, []).append(
                component
            )
            matched.add(window_id)
            seen.add(identity)

    unmatched = tuple(sorted(set(line_settings) - matched))
    return components, unmatched


class SimulatedSpectralData(SpectralData):
    """SpectralData extension used only when NECST runs in simulator mode.

    Simulator-only observing-state effects belong here so the real spectrometer
    path never contains synthetic signal-generation branches.
    """

    def __init__(self) -> None:
        super().__init__()

        simulated = []
        for key, io in self.io.items():
            if not getattr(io, "is_simulator", False):
                raise RuntimeError(
                    "SimulatedSpectralData requires simulator spectrometers; "
                    f"{key!r} is not a simulator"
                )
            for method in ("set_hot", "set_on_source", "set_on_line_components"):
                if not callable(getattr(io, method, None)):
                    raise RuntimeError(
                        "Configured spectrometer simulator does not support simulator "
                        f"state API; {key!r} has no {method}()"
                    )
            simulated.append(io)

        self._simulated_spectrometers = tuple(simulated)
        topic.chopper_status.subscription(self, self.update_chopper_status)

    def _set_on_source_from_position(self, position: str) -> None:
        # Observation.sky() uses SKY and Observation.off() uses OFF.  In
        # particular, Skydip integrations are SKY/HOT and must never acquire an
        # astronomical Gaussian merely because they are source-like integrations.
        enabled = str(position or "").strip().upper() == "ON"
        for io in self._simulated_spectrometers:
            io.set_on_source(enabled)

    def update_metadata(self, msg: Spectral) -> None:
        """Mirror regular metadata handling, then apply simulator ON/OFF state."""
        super().update_metadata(msg)
        self._set_on_source_from_position(msg.position)

    def update_chopper_status(self, msg: ChopperMsg) -> None:
        """Apply HOT only after the chopper reaches the configured insert position."""
        hot = msg.position == config.chopper_motor_position["insert"]
        for io in self._simulated_spectrometers:
            io.set_hot(hot)

    def _configure_on_line_components(self) -> None:
        setup = self.spectral_recording_runtime.active_setup
        if setup is None:
            for io in self._simulated_spectrometers:
                io.set_on_line_components({})
            return

        line_settings = parse_on_line_settings(tuple(config.items()))
        by_spectrometer, unmatched = build_on_line_components(
            setup.snapshot, line_settings
        )
        for key, io in self.io.items():
            io.set_on_line_components(by_spectrometer.get(str(key), {}))
            io.set_on_source(False)
        if unmatched:
            self.logger.warning(
                "Simulator ON-line config window_id(s) are not present in the active "
                f"spectral setup and will be ignored: {list(unmatched)}"
            )

    def apply_spectral_recording_setup(self, request, response):
        response = super().apply_spectral_recording_setup(request, response)
        if not response.success:
            return response
        try:
            self._configure_on_line_components()
        except Exception as exc:
            try:
                self.spectral_recording_runtime.clear(strict=False)
            except Exception:
                pass
            response.success = False
            response.errors = [f"Simulator ON-line setup failed: {exc}"]
            self.logger.error(response.errors[0])
        return response

    def clear_spectral_recording_setup(self, request, response):
        response = super().clear_spectral_recording_setup(request, response)
        if response.success:
            for io in self._simulated_spectrometers:
                io.set_on_source(False)
                io.set_on_line_components({})
        return response
