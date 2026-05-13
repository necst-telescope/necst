# update14 SG frequency alias fix validation (2026-05-03)

## Issue

`lo_profile.toml` documentation allowed `sg_set_frequency_hz`, `sg_set_frequency_mhz`, and `sg_set_frequency_ghz` for `[sg_devices.<sg_id>]`, but `necst/rx/spectral_recording_sg.py` only read `sg_set_frequency_hz` in the `necst-lo-profile apply/verify` path.

As a result, a profile using only `sg_set_frequency_ghz = 4.0` was normalized to the missing default `0.0` Hz in the SG apply plan, and `Commander.signal_generator(..., GHz=0.0, ...)` could reach the SG node. This explains the observed frequency-range error from the SG node when using the GHz key.

## Fix

`build_sg_apply_plan()` now resolves exactly one of:

- `sg_set_frequency_hz`
- `sg_set_frequency_mhz`
- `sg_set_frequency_ghz`

and normalizes the result to `sg_set_frequency_hz` internally. Ambiguous multiple unit keys are rejected. Missing SG frequency is rejected instead of silently becoming 0 Hz.

## Tests / checks performed

- `python -m py_compile necst/rx/spectral_recording_sg.py tests/test_spectral_recording_sg_pr3.py`: OK
- `python tests/test_spectral_recording_sg_pr3.py`: OK
- direct minimal apply-plan simulation:
  - input: `sg_set_frequency_ghz = 4.0`
  - normalized: `sg_set_frequency_hz = 4000000000.0`
  - command call: `Commander.signal_generator("set", GHz=4.0, dBm=10.0, id="band3_1st")`
- zip integrity checked with `zipfile.testzip()`: OK

## Operational note

Before this fix is applied and rebuilt, profiles using `sg_set_frequency_ghz` or `sg_set_frequency_mhz` can fail because the SG apply path sees 0 Hz. The immediate workaround for an unpatched tree is to use `sg_set_frequency_hz` explicitly. After this fix, all three unit-suffixed keys are valid, but only one should be present for a given SG device.
