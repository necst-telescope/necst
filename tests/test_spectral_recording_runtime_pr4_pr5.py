"""PR4/PR5 tests for runtime spectral-recording active setup and slicing."""

from __future__ import annotations

import importlib.util
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


def _snapshot_toml():
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
                "streams": ["xffts_board4"],
            },
        }
    }
    snap = srs.resolve_spectral_recording_setup(
        lo_profile=_lo_profile(),
        recording_setup=rec,
        beam_model=_beam_model(),
        setup_id="runtime_test",
        created_utc="2026-04-26T00:00:00Z",
    )
    return srs.dumps_toml(snap), snap


def test_apply_setup_gate_closed_by_default_and_lists():
    toml, snap = _snapshot_toml()
    state = rt.SpectralRecordingRuntimeState()
    setup = state.apply(
        snapshot_toml=toml,
        snapshot_sha256=snap["canonical_snapshot_sha256"],
        setup_id="runtime_test",
        strict=True,
    )
    assert setup.setup_hash == snap["canonical_snapshot_sha256"]
    assert not setup.gate_allow_save
    assert setup.spectrum_stream_ids == ["xffts_board3"]
    assert setup.tp_stream_ids == ["xffts_board4"]
    assert setup.stream_for_raw("xffts", 3)["stream_id"] == "xffts_board3"


def test_gate_requires_matching_setup_id_and_hash():
    toml, snap = _snapshot_toml()
    state = rt.SpectralRecordingRuntimeState()
    setup = state.apply(
        snapshot_toml=toml,
        snapshot_sha256=snap["canonical_snapshot_sha256"],
        setup_id="runtime_test",
        strict=True,
    )
    try:
        state.set_gate(setup_id="bad", setup_hash=setup.setup_hash, allow_save=True)
    except rt.SpectralRecordingGateError as exc:
        assert "setup_id mismatch" in str(exc)
    else:  # pragma: no cover
        raise AssertionError("Expected setup_id mismatch")

    try:
        state.set_gate(setup_id="runtime_test", setup_hash="bad", allow_save=True)
    except rt.SpectralRecordingGateError as exc:
        assert "setup_hash mismatch" in str(exc)
    else:  # pragma: no cover
        raise AssertionError("Expected setup_hash mismatch")

    state.set_gate(setup_id="runtime_test", setup_hash=setup.setup_hash, allow_save=True)
    assert setup.gate_allow_save


def test_legacy_conflicts_are_rejected_when_active():
    toml, snap = _snapshot_toml()
    state = rt.SpectralRecordingRuntimeState()
    setup = state.apply(
        snapshot_toml=toml,
        snapshot_sha256=snap["canonical_snapshot_sha256"],
        setup_id="runtime_test",
        strict=True,
    )
    for func in (setup.reject_legacy_channel_binning, setup.reject_legacy_tp_mode):
        try:
            func()
        except rt.SpectralRecordingRuntimeError:
            pass
        else:  # pragma: no cover
            raise AssertionError("Expected legacy conflict rejection")


def test_slice_spectrum_for_stream_uses_full_channel_indices_once():
    toml, snap = _snapshot_toml()
    state = rt.SpectralRecordingRuntimeState()
    setup = state.apply(
        snapshot_toml=toml,
        snapshot_sha256=snap["canonical_snapshot_sha256"],
        setup_id="runtime_test",
        strict=True,
    )
    stream = setup.stream_for_raw("xffts", 3)
    data = list(range(16))
    assert rt.slice_spectrum_for_stream(stream, data) == [2, 3, 4, 5, 6]

    try:
        rt.slice_spectrum_for_stream(stream, list(range(5)))
    except rt.SpectralRecordingRuntimeError as exc:
        assert "input spectrum length mismatch" in str(exc)
    else:  # pragma: no cover
        raise AssertionError("Expected full/saved double-slice mismatch")


def test_tp_stream_is_detected_after_pr6_and_namespace_path():
    toml, snap = _snapshot_toml()
    state = rt.SpectralRecordingRuntimeState()
    setup = state.apply(
        snapshot_toml=toml,
        snapshot_sha256=snap["canonical_snapshot_sha256"],
        setup_id="runtime_test",
        strict=True,
    )
    tp_stream = setup.stream_for_raw("xffts", 4)
    assert rt.is_tp_stream(tp_stream)
    assert rt.namespace_db_path("/necst/OMU", "data/spectral/xffts/board3") == "/necst/OMU/data/spectral/xffts/board3"


def test_extra_chunk_contains_channel_metadata():
    toml, snap = _snapshot_toml()
    state = rt.SpectralRecordingRuntimeState()
    setup = state.apply(
        snapshot_toml=toml,
        snapshot_sha256=snap["canonical_snapshot_sha256"],
        setup_id="runtime_test",
        strict=True,
    )
    stream = setup.stream_for_raw("xffts", 3)
    chunk = {c["key"]: c["value"] for c in rt.spectrum_extra_chunk(stream, setup)}
    assert rt.decode_fixed_string_value(chunk["setup_id"]) == "runtime_test"
    assert len(chunk["setup_id"]) == 64
    assert rt.decode_fixed_string_value(chunk["stream_id"]) == "xffts_board3"
    assert len(chunk["stream_id"]) == 64
    assert chunk["saved_ch_start"] == 2
    assert chunk["saved_ch_stop"] == 7
    assert chunk["saved_nchan"] == 5


def test_clear_reapply_and_fatal_latch_pr8b():
    toml, snap = _snapshot_toml()
    state = rt.SpectralRecordingRuntimeState()
    setup = state.apply(
        snapshot_toml=toml,
        snapshot_sha256=snap["canonical_snapshot_sha256"],
        setup_id="runtime_test",
        strict=True,
    )

    try:
        state.apply(
            snapshot_toml=toml,
            snapshot_sha256=snap["canonical_snapshot_sha256"],
            setup_id="runtime_test",
            strict=True,
        )
    except rt.SpectralRecordingRuntimeError as exc:
        assert "already active" in str(exc)
    else:  # pragma: no cover
        raise AssertionError("Expected reapply rejection while setup is active")

    state.set_gate(setup_id="runtime_test", setup_hash=setup.setup_hash, allow_save=True)
    # PR8d: fatal latch must fail closed without raising out of async callbacks.
    state.latch_fatal_error("unknown raw stream")
    assert setup.fatal_error == "unknown raw stream"
    assert not setup.gate_allow_save

    try:
        state.set_gate(setup_id="runtime_test", setup_hash=setup.setup_hash, allow_save=True)
    except rt.SpectralRecordingRuntimeError as exc:
        assert "latched fatal error" in str(exc)
    else:  # pragma: no cover
        raise AssertionError("Expected gate-open rejection after fatal error")

    warnings = state.clear(setup_id="runtime_test", setup_hash=setup.setup_hash)
    assert warnings == []
    assert state.active_setup is None

    setup2 = state.apply(
        snapshot_toml=toml,
        snapshot_sha256=snap["canonical_snapshot_sha256"],
        setup_id="runtime_test",
        strict=True,
    )
    assert setup2.setup_hash == snap["canonical_snapshot_sha256"]


if __name__ == "__main__":
    test_apply_setup_gate_closed_by_default_and_lists()
    test_gate_requires_matching_setup_id_and_hash()
    test_legacy_conflicts_are_rejected_when_active()
    test_slice_spectrum_for_stream_uses_full_channel_indices_once()
    test_tp_stream_is_detected_after_pr6_and_namespace_path()
    test_extra_chunk_contains_channel_metadata()
    test_clear_reapply_and_fatal_latch_pr8b()
    print("all spectral_recording_runtime PR4/PR5 tests passed")
