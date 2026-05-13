# OMU 1.85 m 4-board current setup example

This directory contains the current OMU 1.85 m 4-board spectral-recording setup for:

- 12CO J=2-1
- 13CO J=2-1
- C18O J=2-1
- 12CO J=1-0
- 13CO J=1-0
- C18O J=1-0

## Files

- `lo_profile_OMU1P85M_4board_12co_13co_c18o_current.toml`
- `recording_window_setup_OMU1P85M_4board_12co_13co_c18o_current.toml`
- `parameter_update_report.md`
- `converter_analysis_OMU1P85M_4board_12co_13co_c18o_current.toml`
- `sunscan_analysis_OMU1P85M_4board_12co_13co_c18o_current.toml`

## SDFITS numbering policy

The current OMU 1.85 m system separates the 100 GHz and 200 GHz beams by a quasi-optical filter.  Therefore the SDFITS feed number is assigned by frequency path, not by XFFTS board.

| Frequency path | fdnum |
|---|---:|
| 200 GHz path | 1 |
| 100 GHz path | 2 |

`ifnum` is local within each `fdnum`; the product identity is the tuple `(fdnum, ifnum, plnum)`.

| stream | line group | fdnum | ifnum | plnum |
|---|---|---:|---:|---:|
| `xffts_board1__12CO_J2_1` | 12CO J=2-1 | 1 | 0 | 0 |
| `xffts_board2__13CO_J2_1` | 13CO J=2-1 | 1 | 1 | 0 |
| `xffts_board2__C18O_J2_1` | C18O J=2-1 | 1 | 2 | 0 |
| `xffts_board3__12CO_J1_0` | 12CO J=1-0 | 2 | 0 | 0 |
| `xffts_board4__13CO_J1_0` | 13CO J=1-0 | 2 | 1 | 0 |
| `xffts_board4__C18O_J1_0` | C18O J=1-0 | 2 | 2 | 0 |

## 200 GHz first local

The 200 GHz first local is represented as a fixed 225 GHz LO:

```toml
[lo_roles.band6_1st]
source = "fixed"
fixed_lo_frequency_ghz = 225.0
```

The hardware may be generated internally by x12 from 18.75 GHz, but it is not controlled by this `lo_profile.toml`.

## Validation

The TOML files parse successfully.  The six line centers fall within the 0--2.5 GHz XFFTS baseband using the configured LO chains.

For multi-window groups, `fdnum`, `ifnum`, and `plnum` are assigned per window in `recording_window_setup` so that the saved products have distinct SDFITS IF numbers.

Use `multi_window` even for single-line boards so that the `window_id` is reflected consistently in the saved product stream name and DB table path.

## Analysis files

`converter_analysis_OMU1P85M_4board_12co_13co_c18o_current.toml` contains analysis-time settings only. The spectral truth comes from `lo_profile`, `recording_window_setup`, or the generated snapshot.

`sunscan_analysis_OMU1P85M_4board_12co_13co_c18o_current.toml` inherits common table/timing/weather settings through `[analysis_base]` and keeps chopper-wheel settings in `[sunscan_analysis]`.


## Sun/sunscan TP setup

For solar continuum scans, use the TP recording setup instead of the line/velocity recording setup:

```text
recording_window_setup_sun_tp_OMU1P85M_4board_current.toml
sunscan_analysis_sun_tp_OMU1P85M_4board_current.toml
```

The TP setup uses `mode = "tp"` and `saved_window_policy = "channel"` for each stream.  The default channel range is `8192:24576`, the central half of the 32768-channel band.  This is intentionally channel-based, not velocity-based, because Sun continuum work does not distinguish 12CO/13CO/C18O lines and should avoid known spurious channels directly.
