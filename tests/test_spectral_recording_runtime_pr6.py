"""PR6 tests for TP direct append runtime helpers."""

from __future__ import annotations

import importlib.util
import math
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RX = ROOT / "necst" / "rx"
sys.path.insert(0, str(RX))

SETUP_SPEC = importlib.util.spec_from_file_location(
    "spectral_recording_setup", RX / "spectral_recording_setup.py"
)
assert SETUP_SPEC and SETUP_SPEC.loader
srs = importlib.util.module_from_spec(SETUP_SPEC)
sys.modules["spectral_recording_setup"] = srs
SETUP_SPEC.loader.exec_module(srs)

RUNTIME_SPEC = importlib.util.spec_from_file_location(
    "spectral_recording_runtime", RX / "spectral_recording_runtime.py"
)
assert RUNTIME_SPEC and RUNTIME_SPEC.loader
rt = importlib.util.module_from_spec(RUNTIME_SPEC)
sys.modules["spectral_recording_runtime"] = rt
RUNTIME_SPEC.loader.exec_module(rt)


def _lo_profile():
    return {
        "sg_devices": {
            "sg_lsb": {
                "control_adapter": "necst_commander_signal_generator_set_v1",
                "sg_set_frequency_hz": 4.0e9,
                "sg_set_power_dbm": 15.0,
                "frequency_tolerance_hz": 10.0,
            }
        },
        "lo_roles": {
            "lo1": {"source": "fixed", "fixed_lo_frequency_hz": 230.0e9},
            "lo2": {
                "source": "sg_device",
                "sg_id": "sg_lsb",
                "multiplier": 3,
                "expected_lo_frequency_hz": 12.0e9,
            },
        },
        "lo_chains": {
            "band6_lsb": {
                "formula_version": "signed_lo_sum_plus_if_v1",
                "lo_roles": ["lo1", "lo2"],
                "sideband_signs": [1, -1],
            }
        },
        "frequency_axes": {
            "axis_small": {
                "full_nchan": 16,
                "if_freq_at_full_ch0_hz": 0.0,
                "if_freq_step_hz": 1.0e6,
                "channel_order": "increasing_if",
            }
        },
        "stream_groups": {
            "g": {
                "spectrometer_key": "xffts",
                "frontend": "nanten2_band6",
                "backend": "xffts",
                "lo_chain": "band6_lsb",
                "polariza": "XX",
                "frequency_axis_id": "axis_small",
                "use_for_convert": True,
                "use_for_sunscan": True,
                "use_for_fit": True,
                "streams": [
                    {
                        "stream_id": "xffts_board4",
                        "board_id": 4,
                        "fdnum": 0,
                        "ifnum": 1,
                        "plnum": 0,
                        "beam_id": "B00",
                    },
                ],
            }
        },
    }


def _beam_model():
    return {
        "beams": {
            "B00": {
                "beam_model_version": "2026-04-26",
                "az_offset_arcsec": 0.0,
                "el_offset_arcsec": 0.0,
                "rotation_mode": "elevation",
                "reference_angle_deg": 0.0,
                "rotation_sign": 1,
                "rotation_slope_deg_per_deg": 1.0,
                "dewar_angle_deg": 0.0,
            }
        }
    }


def _setup_and_stream():
    rec = {
        "recording_groups": {
            "tp": {
                "mode": "tp",
                "saved_window_policy": "channel",
                "saved_ch_start": 8,
                "saved_ch_stop": 12,
                "streams": ["xffts_board4"],
            }
        }
    }
    snap = srs.resolve_spectral_recording_setup(
        lo_profile=_lo_profile(),
        recording_setup=rec,
        beam_model=_beam_model(),
        setup_id="tp_runtime_test",
        created_utc="2026-04-26T00:00:00Z",
    )
    state = rt.SpectralRecordingRuntimeState()
    setup = state.apply(
        snapshot_toml=srs.dumps_toml(snap),
        snapshot_sha256=snap["canonical_snapshot_sha256"],
        setup_id="tp_runtime_test",
        strict=True,
    )
    return setup, setup.stream_for_raw("xffts", 4)


def test_tp_statistics_use_selected_window_and_ignore_nan():
    setup, stream = _setup_and_stream()
    data = list(range(16))
    data[9] = float("nan")
    stats = rt.compute_tp_statistics_for_stream(stream, data)
    assert stats["tp_valid"] is True
    assert stats["tp_nchan_used"] == 3
    assert stats["tp_sum"] == 8.0 + 10.0 + 11.0
    assert stats["tp_mean"] == (8.0 + 10.0 + 11.0) / 3.0


def test_tp_all_nan_is_invalid_not_zero():
    setup, stream = _setup_and_stream()
    data = list(range(16))
    for idx in range(8, 12):
        data[idx] = float("nan")
    stats = rt.compute_tp_statistics_for_stream(stream, data)
    assert stats["tp_valid"] is False
    assert stats["tp_nchan_used"] == 0
    assert math.isnan(stats["tp_sum"])
    assert math.isnan(stats["tp_mean"])


def test_tp_chunk_schema_and_metadata():
    setup, stream = _setup_and_stream()
    data = list(range(16))
    chunk = rt.tp_chunk_for_stream(
        stream,
        setup,
        time=123.5,
        time_spectrometer="xffts-time",
        position="ON",
        obs_id="orion",
        line_index=7,
        line_label="scan-line",
        spectral_data=data,
    )
    table = {entry["key"]: entry for entry in chunk}
    assert rt.decode_fixed_string_value(table["setup_id"]["value"]) == "tp_runtime_test"
    assert len(table["setup_id"]["value"]) == 64
    assert rt.decode_fixed_string_value(table["stream_id"]["value"]) == "xffts_board4"
    assert len(table["stream_id"]["value"]) == 64
    assert rt.decode_fixed_string_value(table["spectrometer_key"]["value"]) == "xffts"
    assert len(table["spectrometer_key"]["value"]) == 32
    assert table["board_id"]["value"] == 4
    assert table["tp_sum"]["value"] == 8.0 + 9.0 + 10.0 + 11.0
    assert table["tp_mean"]["value"] == (8.0 + 9.0 + 10.0 + 11.0) / 4.0
    assert table["tp_nchan_used"]["value"] == 4
    assert table["tp_valid"]["value"] is True
    assert table["tp_windows_json"]["type"] == "string"
    assert len(table["tp_windows_json"]["value"]) == 2048
    assert "computed_ch_start" in rt.decode_fixed_string_value(table["tp_windows_json"]["value"])
    assert rt.decode_fixed_string_value(table["tp_stat"]["value"]) == "sum_mean"
    assert len(table["tp_stat"]["value"]) == 16


def test_tp_rejects_pre_sliced_input_to_avoid_ambiguous_indices():
    setup, stream = _setup_and_stream()
    try:
        rt.compute_tp_statistics_for_stream(stream, [8.0, 9.0, 10.0, 11.0])
    except rt.SpectralRecordingRuntimeError as exc:
        assert "input spectrum length mismatch" in str(exc)
    else:  # pragma: no cover
        raise AssertionError("Expected full_nchan mismatch")


def test_tp_fixed_string_rejects_oversized_values():
    setup, stream = _setup_and_stream()
    long_line_label = "x" * 65
    try:
        rt.tp_chunk_for_stream(
            stream,
            setup,
            time=123.5,
            time_spectrometer="xffts-time",
            position="ON",
            obs_id="orion",
            line_index=7,
            line_label=long_line_label,
            spectral_data=list(range(16)),
        )
    except rt.SpectralRecordingRuntimeError as exc:
        assert "line_label" in str(exc)
        assert "too long" in str(exc)
    else:  # pragma: no cover
        raise AssertionError("Expected fixed string overflow error")


if __name__ == "__main__":
    test_tp_statistics_use_selected_window_and_ignore_nan()
    test_tp_all_nan_is_invalid_not_zero()
    test_tp_chunk_schema_and_metadata()
    test_tp_rejects_pre_sliced_input_to_avoid_ambiguous_indices()
    test_tp_fixed_string_rejects_oversized_values()
    print("all spectral_recording_runtime PR6 tests passed")
