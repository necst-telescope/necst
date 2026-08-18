import math

import numpy as np

from necst.rx.spectral_recording_setup import C_KM_PER_S
from necst.rx.spectrometer_simulator import (
    SimulatedSpectralData,
    build_on_line_components,
    parse_on_line_settings,
    velocity_axis_for_window,
)


class _FakeSpectrometerSimulator:
    def __init__(self) -> None:
        self.on_states = []
        self.line_components = []

    def set_on_source(self, enabled: bool) -> None:
        self.on_states.append(bool(enabled))

    def set_on_line_components(self, components) -> None:
        self.line_components.append(components)


def _node_with_fake_simulator():
    node = SimulatedSpectralData.__new__(SimulatedSpectralData)
    simulator = _FakeSpectrometerSimulator()
    node._simulated_spectrometers = (simulator,)
    node._source_state_schedule = []
    node._current_on_source = None
    return node, simulator


def _topocentric_snapshot():
    rest = 230.538e9
    return {
        "frequency_axes": {
            "axis": {
                "full_nchan": 9,
                "sky_freq_at_full_ch0_hz": rest + 4.0e6,
                "sky_freq_step_hz": -1.0e6,
            }
        },
        "lo_chains": {},
        "streams": {
            "xffts_board1_12co21": {
                "spectrometer_key": "xffts",
                "raw_input_key": "xffts",
                "board_id": 1,
                "raw_board_id": 1,
                "frequency_axis_id": "axis",
                "lo_chain": "unused",
                "computed_windows": [
                    {
                        "kind": "velocity",
                        "window_id": "12CO_J2_1",
                        "rest_frequency_hz": rest,
                        "velocity_definition": "radio",
                        "requested_velocity_frame": "TOPOCENTRIC",
                        "vlsrk_correction_kms": None,
                    }
                ],
            }
        },
    }


def test_parse_on_line_settings_uses_window_id_as_key():
    settings = parse_on_line_settings(
        (
            (
                "spectrometer.simulator.on_line.12CO_J2_1.v_center_kms",
                -7.0,
            ),
            ("spectrometer.simulator.on_line.12CO_J2_1.fwhm_kms", 3.0),
            (
                "spectrometer.simulator.on_line.12CO_J2_1.t_line_peak_K",
                20.0,
            ),
            (
                "spectrometer.simulator.on_line.13CO_J2_1.v_center_kms",
                -7.0,
            ),
            ("spectrometer.simulator.on_line.13CO_J2_1.fwhm_kms", 2.0),
            (
                "spectrometer.simulator.on_line.13CO_J2_1.t_line_peak_K",
                8.0,
            ),
        )
    )

    assert settings == {
        "12CO_J2_1": {
            "v_center_kms": -7.0,
            "fwhm_kms": 3.0,
            "t_line_peak_K": 20.0,
        },
        "13CO_J2_1": {
            "v_center_kms": -7.0,
            "fwhm_kms": 2.0,
            "t_line_peak_K": 8.0,
        },
    }


def test_velocity_axis_uses_window_rest_frequency_and_raw_board_axis():
    snapshot = _topocentric_snapshot()
    stream = snapshot["streams"]["xffts_board1_12co21"]
    window = stream["computed_windows"][0]

    velocity = velocity_axis_for_window(snapshot, stream, window)

    assert len(velocity) == 9
    assert np.isclose(velocity[4], 0.0, atol=1e-10)
    assert velocity[0] < 0
    assert velocity[-1] > 0


def test_velocity_axis_applies_lsrk_correction_from_snapshot():
    rest = 230.538e9
    correction_kms = 10.0
    beta = correction_kms / C_KM_PER_S
    doppler = math.sqrt((1.0 + beta) / (1.0 - beta))
    snapshot = {
        "frequency_axes": {
            "axis": {
                "full_nchan": 1,
                "sky_freq_at_full_ch0_hz": rest * doppler,
                "sky_freq_step_hz": 1.0,
            }
        },
        "lo_chains": {},
    }
    stream = {
        "frequency_axis_id": "axis",
        "lo_chain": "unused",
    }
    window = {
        "kind": "velocity",
        "window_id": "12CO_J2_1",
        "rest_frequency_hz": rest,
        "velocity_definition": "radio",
        "requested_velocity_frame": "LSRK",
        "vlsrk_correction_kms": correction_kms,
    }

    velocity = velocity_axis_for_window(snapshot, stream, window)

    assert np.isclose(velocity[0], 0.0, atol=1e-7)


def test_build_on_line_components_maps_window_to_raw_board():
    snapshot = _topocentric_snapshot()
    settings = {
        "12CO_J2_1": {
            "v_center_kms": -7.0,
            "fwhm_kms": 3.0,
            "t_line_peak_K": 20.0,
        }
    }

    by_spectrometer, unmatched = build_on_line_components(snapshot, settings)

    assert unmatched == ()
    assert list(by_spectrometer) == ["xffts"]
    assert list(by_spectrometer["xffts"]) == [1]
    component = by_spectrometer["xffts"][1][0]
    assert component["v_center_kms"] == -7.0
    assert component["fwhm_kms"] == 3.0
    assert component["t_line_peak_K"] == 20.0
    assert len(component["velocity_axis_kms"]) == 9


def test_build_on_line_components_supports_multiple_windows_on_same_board():
    snapshot = _topocentric_snapshot()
    stream = snapshot["streams"]["xffts_board1_12co21"]
    stream["computed_windows"].append(
        {
            "kind": "velocity",
            "window_id": "13CO_J2_1",
            "rest_frequency_hz": 230.536e9,
            "velocity_definition": "radio",
            "requested_velocity_frame": "TOPOCENTRIC",
            "vlsrk_correction_kms": None,
        }
    )
    settings = {
        "12CO_J2_1": {
            "v_center_kms": -7.0,
            "fwhm_kms": 3.0,
            "t_line_peak_K": 20.0,
        },
        "13CO_J2_1": {
            "v_center_kms": -7.0,
            "fwhm_kms": 2.0,
            "t_line_peak_K": 8.0,
        },
    }

    by_spectrometer, unmatched = build_on_line_components(snapshot, settings)

    assert unmatched == ()
    assert len(by_spectrometer["xffts"][1]) == 2


def test_on_metadata_enables_line_and_off_disables_it():
    node, simulator = _node_with_fake_simulator()

    node._set_on_source_from_position("ON")
    node._set_on_source_from_position("OFF")

    assert simulator.on_states == [True, False]


def test_future_metadata_does_not_change_signal_before_effective_time():
    node, simulator = _node_with_fake_simulator()
    future = 1.0e20

    node._schedule_source_state(future, "ON")

    assert simulator.on_states == []
    node._apply_due_source_state(now=future)
    assert simulator.on_states == [True]


def test_scheduled_on_off_changes_follow_effective_time_order():
    node, simulator = _node_with_fake_simulator()
    future = 1.0e20

    node._schedule_source_state(future + 1.0, "OFF")
    node._schedule_source_state(future, "ON")

    node._apply_due_source_state(now=future)
    assert simulator.on_states == [True]
    node._apply_due_source_state(now=future + 1.0)
    assert simulator.on_states == [True, False]


def test_skydip_sky_and_hot_positions_never_enable_gaussian():
    node, simulator = _node_with_fake_simulator()

    for position in ("SKY", "HOT", ""):
        node._set_on_source_from_position(position)

    assert simulator.on_states == [False]
