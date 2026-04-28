"""Unit tests for spectral_recording_setup PR1.

These tests intentionally load the module from its file path so they can run in a
minimal environment without importing ROS/neclib through ``necst.__init__``.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "necst" / "rx" / "spectral_recording_setup.py"
SPEC = importlib.util.spec_from_file_location("spectral_recording_setup", MODULE_PATH)
assert SPEC and SPEC.loader
srs = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(srs)


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
                # Explicit sky axis for deterministic velocity-window tests.
                "sky_freq_at_full_ch0_hz": 230.542e9,
                "sky_freq_step_hz": -1.0e6,
            }
        },
        "stream_groups": {
            "g_usb_xx": {
                "spectrometer_key": "xffts",
                "frontend": "nanten2_band6",
                "backend": "xffts",
                "lo_chain": "band6_lsb",
                "polariza": "XX",
                "frequency_axis_id": "axis_small",
                "default_rest_frequency_hz": 230.538e9,
                "use_for_convert": True,
                "use_for_sunscan": True,
                "use_for_fit": True,
                "streams": [
                    {
                        "stream_id": "xffts_board3",
                        "board_id": 3,
                        "fdnum": 0,
                        "ifnum": 0,
                        "plnum": 0,
                        "beam_id": "B00",
                    },
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


def test_default_full_spectrum_resolution():
    snapshot = srs.resolve_spectral_recording_setup(
        lo_profile=_lo_profile(),
        recording_setup=None,
        beam_model=_beam_model(),
        setup_id="default_full",
        created_utc="2026-04-26T00:00:00Z",
    )
    srs.validate_snapshot(snapshot)
    streams = snapshot["streams"]
    assert streams["xffts_board3"]["recording_mode"] == "spectrum"
    assert streams["xffts_board3"]["saved_ch_start"] == 0
    assert streams["xffts_board3"]["saved_ch_stop"] == 16
    assert streams["xffts_board3"]["db_table_path"] == "data/spectral/xffts/board3"
    assert snapshot["canonical_snapshot_sha256"]


def test_channel_slice_and_tp_resolution():
    rec = {
        "recording_groups": {
            "slice": {
                "mode": "spectrum",
                "saved_window_policy": "channel",
                "saved_ch_start": 2,
                "saved_ch_stop": 7,
                "streams": ["xffts_board3"],
            },
            "tp": {
                "mode": "tp",
                "saved_window_policy": "channel",
                "saved_ch_start": 8,
                "saved_ch_stop": 11,
                "tp_stat": "sum_mean",
                "streams": ["xffts_board4"],
            },
        }
    }
    snapshot = srs.resolve_spectral_recording_setup(
        lo_profile=_lo_profile(),
        recording_setup=rec,
        beam_model=_beam_model(),
        setup_id="slice_tp",
        created_utc="2026-04-26T00:00:00Z",
    )
    srs.validate_snapshot(snapshot)
    s3 = snapshot["streams"]["xffts_board3"]
    s4 = snapshot["streams"]["xffts_board4"]
    assert (s3["saved_ch_start"], s3["saved_ch_stop"], s3["saved_nchan"]) == (2, 7, 5)
    assert s3["recording_table_kind"] == "spectral"
    assert (s4["saved_ch_start"], s4["saved_ch_stop"], s4["saved_nchan"]) == (8, 11, 3)
    assert s4["recording_table_kind"] == "tp"
    assert s4["db_table_path"] == "data/tp/xffts/board4"


def test_velocity_window_resolution_with_explicit_sky_axis():
    rec = {
        "recording_groups": {
            "co": {
                "mode": "spectrum",
                "saved_window_policy": "contiguous_envelope",
                "line_name": "12CO",
                "rest_frequency_hz": 230.538e9,
                "velocity_definition": "radio",
                "velocity_frame": "LSRK",
                "vmin_kms": 0.0,
                "vmax_kms": 0.0,
                "margin_kms": 2.0,
                "streams": ["xffts_board3"],
            }
        }
    }
    snapshot = srs.resolve_spectral_recording_setup(
        lo_profile=_lo_profile(),
        recording_setup=rec,
        beam_model=_beam_model(),
        setup_id="velocity",
        setup_override_policy="warn",
        created_utc="2026-04-26T00:00:00Z",
    )
    srs.validate_snapshot(snapshot)
    s3 = snapshot["streams"]["xffts_board3"]
    assert s3["saved_window_policy"] == "contiguous_envelope"
    assert 0 <= s3["saved_ch_start"] < s3["saved_ch_stop"] <= 16
    # Unassigned streams stay full-spectrum by default.
    assert snapshot["streams"]["xffts_board4"]["saved_ch_start"] == 0
    assert snapshot["streams"]["xffts_board4"]["saved_ch_stop"] == 16


def test_disallowed_item_override_is_error():
    lo = _lo_profile()
    lo["stream_groups"]["g_usb_xx"]["streams"][0]["polariza"] = "YY"
    try:
        srs.resolve_spectral_recording_setup(
            lo_profile=lo,
            recording_setup=None,
            beam_model=_beam_model(),
            setup_id="bad",
            created_utc="2026-04-26T00:00:00Z",
        )
    except srs.SpectralRecordingValidationError as exc:
        assert "disallowed item keys" in str(exc)
    else:  # pragma: no cover
        raise AssertionError("Expected disallowed item override to fail")


def test_duplicate_db_path_is_error():
    lo = _lo_profile()
    lo["stream_groups"]["g_usb_xx"]["streams"][0]["db_table_path"] = "data/spectral/xffts/shared"
    lo["stream_groups"]["g_usb_xx"]["streams"][1]["db_table_path"] = "data/spectral/xffts/shared"
    try:
        srs.resolve_spectral_recording_setup(
            lo_profile=lo,
            recording_setup=None,
            beam_model=_beam_model(),
            setup_id="bad_db",
            created_utc="2026-04-26T00:00:00Z",
        )
    except srs.SpectralRecordingValidationError as exc:
        assert "Duplicate db_table_path" in str(exc)
    else:  # pragma: no cover
        raise AssertionError("Expected duplicate db_table_path to fail")


def test_saved_local_channel_frequency_matches_full_channel():
    axis = _lo_profile()["frequency_axes"]["axis_small"]
    saved_ch_start = 10
    assert srs.local_channel_to_full_channel(saved_ch_start, 0) == 10
    assert srs.if_frequency_at_saved_local_channel(axis, saved_ch_start, 0) == srs.if_frequency_at_full_channel(axis, 10)


def test_toml_roundtrip_snapshot(tmp_path):
    snapshot = srs.resolve_spectral_recording_setup(
        lo_profile=_lo_profile(),
        recording_setup=None,
        beam_model=_beam_model(),
        setup_id="roundtrip",
        created_utc="2026-04-26T00:00:00Z",
    )
    out = tmp_path / "snapshot.toml"
    srs.write_snapshot(out, snapshot)
    loaded = srs.read_toml(out)
    srs.validate_snapshot(loaded)
    assert loaded["canonical_snapshot_sha256"] == snapshot["canonical_snapshot_sha256"]


def test_db_table_path_kind_mismatch_is_error():
    lo = _lo_profile()
    rec = {
        "recording_groups": {
            "tp": {
                "mode": "tp",
                "saved_window_policy": "channel",
                "saved_ch_start": 1,
                "saved_ch_stop": 3,
                "streams": ["xffts_board4"],
            }
        }
    }
    lo["stream_groups"]["g_usb_xx"]["streams"][1]["db_table_path"] = "data/spectral/xffts/board4"
    try:
        srs.resolve_spectral_recording_setup(
            lo_profile=lo,
            recording_setup=rec,
            beam_model=_beam_model(),
            setup_id="bad_path_kind",
            created_utc="2026-04-27T00:00:00Z",
        )
    except srs.SpectralRecordingValidationError as exc:
        assert "recording_table_kind='tp'" in str(exc)
        assert "data/tp/" in str(exc)
    else:  # pragma: no cover
        raise AssertionError("Expected TP db_table_path kind mismatch to fail")


def test_velocity_window_is_marked_topocentric_approximation():
    rec = {
        "recording_groups": {
            "co": {
                "mode": "spectrum",
                "saved_window_policy": "contiguous_envelope",
                "line_name": "12CO",
                "rest_frequency_hz": 230.538e9,
                "velocity_definition": "radio",
                "velocity_frame": "LSRK",
                "vmin_kms": 0.0,
                "vmax_kms": 0.0,
                "margin_kms": 2.0,
                "streams": ["xffts_board3"],
            }
        }
    }
    snapshot = srs.resolve_spectral_recording_setup(
        lo_profile=_lo_profile(),
        recording_setup=rec,
        beam_model=_beam_model(),
        setup_id="velocity_approx",
        setup_override_policy="warn",
        created_utc="2026-04-27T00:00:00Z",
    )
    window = snapshot["streams"]["xffts_board3"]["computed_windows"][0]
    assert window["requested_velocity_frame"] == "LSRK"
    assert window["velocity_frame"] == "TOPOCENTRIC_REST_APPROX"
    assert window["velocity_resolver_version"] == "topocentric_rest_velocity_approx_v1"
    assert any("LSRK correction" in w for w in window["resolver_warnings"])


def test_channel_order_must_match_if_step_sign():
    lo = _lo_profile()
    lo["frequency_axes"]["axis_small"]["if_freq_step_hz"] = -1.0e6
    lo["frequency_axes"]["axis_small"]["channel_order"] = "increasing_if"
    try:
        srs.resolve_spectral_recording_setup(
            lo_profile=lo,
            recording_setup=None,
            beam_model=_beam_model(),
            setup_id="bad_channel_order",
            created_utc="2026-04-27T00:00:00Z",
        )
    except srs.SpectralRecordingValidationError as exc:
        assert "requires if_freq_step_hz > 0" in str(exc)
    else:  # pragma: no cover
        raise AssertionError("Expected channel_order/sign mismatch to fail")


def test_fixed_schema_limits_are_validated_before_snapshot_use():
    lo = _lo_profile()
    lo["stream_groups"]["g_usb_xx"]["streams"][1]["stream_id"] = "x" * 65
    try:
        srs.resolve_spectral_recording_setup(
            lo_profile=lo,
            recording_setup={
                "recording_groups": {
                    "tp": {
                        "mode": "tp",
                        "saved_window_policy": "channel",
                        "saved_ch_start": 8,
                        "saved_ch_stop": 11,
                        "streams": ["x" * 65],
                    }
                }
            },
            beam_model=_beam_model(),
            setup_id="fixed_schema_limit",
            created_utc="2026-04-27T00:00:00Z",
        )
    except srs.SpectralRecordingValidationError as exc:
        assert "stream_id" in str(exc)
        assert "64 bytes" in str(exc)
    else:  # pragma: no cover
        raise AssertionError("Expected fixed-string stream_id overflow to fail")


def test_tp_windows_json_limit_is_validated_in_snapshot_validator():
    snapshot = srs.resolve_spectral_recording_setup(
        lo_profile=_lo_profile(),
        recording_setup={
            "recording_groups": {
                "tp": {
                    "mode": "tp",
                    "saved_window_policy": "channel",
                    "saved_ch_start": 8,
                    "saved_ch_stop": 11,
                    "streams": ["xffts_board4"],
                }
            }
        },
        beam_model=_beam_model(),
        setup_id="tp_json_limit",
        created_utc="2026-04-27T00:00:00Z",
    )
    snapshot["streams"]["xffts_board4"]["computed_windows_json"] = "x" * 2049
    try:
        srs.validate_snapshot(snapshot)
    except srs.SpectralRecordingValidationError as exc:
        assert "tp_windows_json" in str(exc)
        assert "2048 bytes" in str(exc)
    else:  # pragma: no cover
        raise AssertionError("Expected overlong tp_windows_json to fail")


if __name__ == "__main__":
    import tempfile

    class _TmpPath:
        def __truediv__(self, name):
            return Path(tempfile.mkdtemp()) / name

    test_default_full_spectrum_resolution()
    test_channel_slice_and_tp_resolution()
    test_velocity_window_resolution_with_explicit_sky_axis()
    test_disallowed_item_override_is_error()
    test_duplicate_db_path_is_error()
    test_saved_local_channel_frequency_matches_full_channel()
    test_toml_roundtrip_snapshot(_TmpPath())
    test_db_table_path_kind_mismatch_is_error()
    test_velocity_window_is_marked_topocentric_approximation()
    test_channel_order_must_match_if_step_sign()
    test_fixed_schema_limits_are_validated_before_snapshot_use()
    test_tp_windows_json_limit_is_validated_in_snapshot_validator()
    print("all spectral_recording_setup tests passed")
