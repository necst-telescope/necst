# 12co-NANTEN2-multi_260331TO legacy-config equivalent split setup

Generated: 2026-04-29Tupdate11-docs

This directory is an example split-configuration equivalent of:

`12co-NANTEN2-multi_260331TO.conf3(3).txt`

## Files

- `lo_profile_12co_NANTEN2_multi_260331TO.toml`
  - stream truth, shared frequency axis, shared LO chains.
  - `rest_frequency_ghz` is stream-level, not frequency-axis-level.
  - `lo1_ghz` and `lo2_mhz` are used to avoid large Hz literals.
- `beam_model_12co_NANTEN2_multi_260331TO.toml`
  - standalone beam geometry for B01--B05 plus a zero B00 fallback.
- `recording_window_12co_NANTEN2_multi_260331TO.toml`
  - 230/115 GHz: VLSRK -150..+150 km/s with 10 km/s margin.
  - 110 GHz: `multi_window`, separate 13CO and C18O recorded products, each -100..+100 km/s with 10 km/s margin.
- `analysis_stream_selection_12co_NANTEN2_multi_260331TO.toml`
  - legacy `enabled`, `use_for_convert`, `use_for_sunscan`, `use_for_fit`, `beam_fit_use`.
- `converter_analysis_12co_NANTEN2_multi_260331TO.toml`
  - converter timing, DB, encoder, meteorology, chopper-related analysis parameters from `[global]`.
- `sunscan_analysis_12co_NANTEN2_multi_260331TO.toml`
  - sunscan chopper/ripple/edge/trim parameters from `[global]`.
- `example_obs_parameters_fragment_12co_NANTEN2_multi_260331TO.obs`
  - snippet to paste into the `.obs` `[parameters]` section.

## Stream grouping

The source streams are grouped by common line/LO settings:

- `nanten2_xffts_12co115`: 8 streams, default `rest_frequency_ghz = 115.271`, LO2 = 9500 MHz.
- `nanten2_xffts_110ghz`: 7 streams, default `rest_frequency_ghz = 110.201353`, LO2 = 4000 MHz.
- `nanten2_xffts_12co230`: 1 stream, default `rest_frequency_ghz = 230.538`, LO1 = 225.635994 GHz, LO2 = 4500 MHz.

All groups share:

```toml
frequency_axis_id = "xffts_2GHz_32768ch"
```

## Frequency-axis convention

The legacy configuration used:

```toml
definition_mode = "band_start_stop"
nchan = 32768
band_start_hz = 0.0
band_stop_hz = 2000000000.0
channel_origin = "center"
reverse = false
```

Because `channel_origin = "center"`, the frequency step is:

```text
delta_hz = (band_stop_hz - band_start_hz) / (nchan - 1)
         = 2000000000.0 / 32767
         = 61037.01895199438 Hz
```

This is **not** `2000000000.0 / 32768`.

## Multi-window naming

For 110 GHz streams, the recording setup creates separate recorded products:

```text
2LL__13CO_J1_0  -> xffts-board2__13CO_J1_0
2LL__C18O_J1_0  -> xffts-board2__C18O_J1_0
...
```

Use converter selection as:

```bash
--stream-id 2LL --window-id 13CO_J1_0
```

or directly:

```bash
--recorded-stream-id 2LL__13CO_J1_0
```

## Counts

- source streams: 16
- 230 GHz streams: 1
- 115 GHz streams: 8
- 110 GHz source streams: 7
- 110 GHz recorded products: 14
- expected total recorded products: 23

## update10 note: VLSRK reference direction

The recording window file uses `velocity_frame = "LSRK"`.  In update10 this is fail-closed:
the resolver must receive a velocity-reference direction, site, and reference time.  During
normal NECST observation flow these are supplied from `[parameters]` and/or the parsed
`.obs` reference coordinate.  For standalone validation, either provide explicit
`velocity_reference_l_deg` / `velocity_reference_b_deg` / site parameters, or change
`velocity_frame` to `"TOPOCENTRIC"` only when an explicitly topocentric approximate
window is intended.

## Converter analysis as the timing/base file for sunscan

The normal operational file is `converter_analysis_12co_NANTEN2_multi_260331TO.toml`.
It contains the dataset-wide common analysis environment, including `[spectral_time]`
and `[encoder_time]`.  These timing sections describe how spectral row times and
encoder times are interpreted for this RawData/setup family, not a sunscan-only
choice.

`sunscan_analysis_12co_NANTEN2_multi_260331TO.toml` therefore does not repeat
those common settings.  Instead it contains:

```toml
[analysis_base]
path = "converter_analysis_12co_NANTEN2_multi_260331TO.toml"

[sunscan_analysis]
# sunscan-specific fitting/trimming/report parameters only
```

When this sunscan file is read, the converter-analysis file is used as a
base/master for common scalar settings.  Converter-only stream export slices are
not inherited by sunscan.  If a sunscan file inherits an `analysis_base`, it
should not define `[spectral_time]`, `[encoder_time]`, or timing keys inside
`[sunscan_analysis]`; doing so is treated as a configuration conflict unless
`[analysis_base].allow_timing_override = true` is explicitly set.

