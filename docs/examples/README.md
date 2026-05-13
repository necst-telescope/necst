# update14 examples

The examples are organized by telescope/use case.

## Recommended directories

- `NANTEN2_multibeam_260331TO/`  
  NANTEN2-style multibeam setup examples.
- `OMU1P85M_4board_12co_13co_c18o_current/`  
  Current OMU 1.85 m 4-board 12CO/13CO/C18O setup, including lo_profile, recording_window_setup, converter_analysis, and sunscan_analysis.

## Historical directories

The following directories are retained only as migration/history records and should not be used as the first reference for new setup files.

- `old_config_260331TO_update11/`
- `legacy_converter_20260503/`

New documentation and examples should point to the recommended directories above.

## Spectral time examples

Current examples use `spectral_time_source = "auto"` by default.  `host-time` and `xffts-timestamp` may be selected explicitly when needed.  `spectrometer_time_offset_sec` is applied only when the selected source is host-time.


See `OBSOLETE_EXAMPLE_PATHS_TO_REMOVE.md` for local cleanup of removed duplicate example paths.
