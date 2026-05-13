# NANTEN2 multibeam example set: `NANTEN2_multibeam_260331TO`

This directory contains the organized update14 example set for a NANTEN2-style multibeam spectral-recording workflow.

## Files

- `lo_profile_12co_NANTEN2_multi_260331TO.toml`  
  LO/SG/frequency-axis/stream truth definition.
- `recording_window_12co_NANTEN2_multi_260331TO.toml`  
  Velocity or channel windows to record.
- `beam_model_12co_NANTEN2_multi_260331TO.toml`  
  Beam geometry used by snapshot resolving and pointing-reference-beam logic.
- `analysis_stream_selection_12co_NANTEN2_multi_260331TO.toml`  
  Optional analysis-time stream selection.
- `converter_analysis_12co_NANTEN2_multi_260331TO.toml`  
  Converter analysis settings.  Do not use `spectral_name` for snapshot stream selection; use `--recorded-stream-id` or `[analysis_selection]`.
- `sunscan_analysis_12co_NANTEN2_multi_260331TO.toml`  
  Sunscan-specific settings.  It uses `[analysis_base]` to inherit common timing/table settings from the converter analysis file.
- `example_obs_parameters_fragment_12co_NANTEN2_multi_260331TO.obs`  
  Observation-file fragment showing how to refer to the setup files.

## Recommended use

Generate a snapshot at observation/setup time from `lo_profile`, `recording_window`, and `beam_model`.  Converter and sunscan should then read the generated `spectral_recording_snapshot.toml`.  For single-product conversion, prefer explicit recorded stream IDs:

```bash
necst_v4_sdfits_converter RAWDATA_DIR \
  --converter-analysis-config converter_analysis_12co_NANTEN2_multi_260331TO.toml \
  --recorded-stream-id xffts_board1
```

## Notes

The historical directory `old_config_260331TO_update11/` is retained only as a migration record.  Use this directory for new examples.

## Recording-window metadata overrides

The recording-window example now uses `saved_window_policy = "multi_window"` for all groups so that `window_id` is reflected in recorded product names. It demonstrates window-level `ifnum` overrides while leaving `fdnum` and `plnum` inherited from each source stream. This is important for multibeam/polarization data where `fdnum` and `plnum` differ across streams inside the same recording group.


## Sun/sunscan TP setup

For solar continuum scans, use:

```text
recording_window_sun_tp_NANTEN2_multi_260331TO.toml
sunscan_analysis_sun_tp_NANTEN2_multi_260331TO.toml
```

The TP setup uses `mode = "tp"` and `saved_window_policy = "channel"`.  The default range is `8192:24576`, the central half of the 32768-channel band.  `fdnum`, `ifnum`, and `plnum` are inherited from the source streams in `lo_profile`; no line-specific 13CO/C18O product split is made for Sun continuum work.
