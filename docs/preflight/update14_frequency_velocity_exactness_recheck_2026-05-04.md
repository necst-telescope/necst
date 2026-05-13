# update14 SD FITS frequency/velocity exactness recheck

Date: 2026-05-04

## Scope

This recheck used the integrated tree made from the original zip files plus all update14 patches in this chat, including the SD FITS writer RESTFREQ/VELDEF fix.  The uploaded real snapshot `spectral_recording_snapshot.toml` from `necst_otf_20260503_114555_test` was used as the numerical truth.

Checked items:

1. LO-chain sign convention.
2. LSRK velocity window to topocentric channel range.
3. Inclusive/exclusive channel boundary handling.
4. Runtime spectrum slicing with `saved_ch_start:saved_ch_stop`.
5. Snapshot-to-converter explicit WCS propagation.
6. SD FITS writer row-level `CRVAL1`, `CDELT1`, `CRPIX1`, `RESTFREQ`, `SPECSYS`, `VELDEF` propagation.
7. Converter snapshot loading with embedded beam optional blank fields.

## Definitions

- Full XFFTS channel index: zero-based `ch_full`.
- Saved local channel index: zero-based `ch_local`.
- Saved range: `[saved_ch_start, saved_ch_stop)`, i.e. start inclusive and stop exclusive.
- Local-to-full mapping: `ch_full = saved_ch_start + ch_local`.
- IF center frequency:
  `IF(ch_full) = if_freq_at_full_ch0_hz + ch_full * if_freq_step_hz`.
- Sky frequency:
  `sky(ch_full) = signed_lo_sum_hz + if_frequency_sign * IF(ch_full)`.
- Converter SD FITS local WCS:
  `CRPIX1 = 1`,
  `CRVAL1 = sky(saved_ch_start)`,
  `CDELT1 = if_frequency_sign * if_freq_step_hz`.

## Snapshot constants

- `full_nchan = 32768`
- `if_freq_at_full_ch0_hz = 0.000000`
- `if_freq_step_hz = 76296.273689992988`
- `CTYPE1 = FREQ`
- `CUNIT1 = Hz`
- `SPECSYS = TOPOCENT`
- `VELDEF = RADIO`

## Numerical channel-boundary check

For each velocity window, I recomputed the channel range from the stored `frequency_low_hz` and `frequency_high_hz`, using the same center-in-window rule:

- include a channel if its center frequency is inside `[frequency_low_hz, frequency_high_hz]`;
- output `ch_start = min(included)`;
- output `ch_stop = max(included) + 1`.

Result: all six products reproduce the snapshot `computed_ch_start` and `computed_ch_stop` exactly.  In every case, the first and last saved channels are inside the requested effective velocity/frequency window, while the immediate outside neighbors `start-1` and `stop` are outside.  This rules out a one-channel shift in the snapshot window resolution.

| stream | line | ch_start:ch_stop | N | CDELT1 Hz | v(start) km/s | v(stop-1) km/s | neighbor check |
|---|---|---:|---:|---:|---:|---:|---|
| `xffts_board1` | 12CO J=2-1 | 18185:21410 | 3225 | 76296.273681641 | 159.923665 | -159.986850 | OK: start-1/stop outside |
| `xffts_board2__13CO_J2_1` | 13CO J=2-1 | 21037:24120 | 3083 | 76296.273681641 | 159.956690 | -159.932097 | OK: start-1/stop outside |
| `xffts_board2__C18O_J2_1` | C18O J=2-1 | 10052:13124 | 3072 | 76296.273681641 | 159.983090 | -159.981480 | OK: start-1/stop outside |
| `xffts_board3` | 12CO J=1-0 | 10241:11853 | 1612 | -76296.273681641 | -159.819267 | 159.887245 | OK: start-1/stop outside |
| `xffts_board4__13CO_J1_0` | 13CO J=1-0 | 7909:9450 | 1541 | -76296.273696899 | -159.870168 | 159.806682 | OK: start-1/stop outside |
| `xffts_board4__C18O_J1_0` | C18O J=1-0 | 13401:14936 | 1535 | -76296.273696899 | -159.793871 | 159.852298 | OK: start-1/stop outside |

## Converter WCS propagation check

The snapshot adapter materializes each saved stream as an explicit local WCS.  The local channel 0 WCS is exactly the full-channel `saved_ch_start` frequency, not full channel 0.  The checked WCS values are:

| stream | CRVAL1 Hz | CDELT1 Hz | CRPIX1 | RESTFREQ Hz |
|---|---:|---:|---:|---:|
| `xffts_board1` | 230387447737.052521 | 76296.273681641 | 1.0 | 230538000000.0 |
| `xffts_board2__13CO_J2_1` | 220255044709.616394 | 76296.273681641 | 1.0 | 220399000000.0 |
| `xffts_board2__C18O_J2_1` | 219416930143.131805 | 76296.273681641 | 1.0 | 219560357000.0 |
| `xffts_board3` | 115318649861.140778 | -76296.273681641 | 1.0 | 115271000000.0 |
| `xffts_board4__13CO_J1_0` | 110246572771.385849 | -76296.273696899 | 1.0 | 110201000000.0 |
| `xffts_board4__C18O_J1_0` | 109827553636.280411 | -76296.273696899 | 1.0 | 109782182000.0 |

The last local-channel WCS value differs from the direct full-channel formula by less than 0.03 Hz in the manual double-precision recomputation.  This is only floating-point roundoff at ~1e-13 relative level, far below one channel spacing of 76296.273690 Hz.

## Runtime slicing check

`necst/rx/spectral_recording_runtime.py` slices full spectra as:

```python
sliced = spectral_data[start:stop]
```

where `start = saved_ch_start`, `stop = saved_ch_stop`.  I simulated a full spectrum with `data[ch] = ch` and verified for all six streams:

- saved length equals `saved_ch_stop - saved_ch_start`;
- first saved datum equals `saved_ch_start`;
- last saved datum equals `saved_ch_stop - 1`.

Therefore the recorded data array and the converter WCS local channel 0 refer to the same full XFFTS channel.

## Old converter config consistency

The uploaded legacy converter configs imply:

- 12CO(1-0): `104.5 + 11.6 - IF`;
- 13CO/C18O(1-0): `104.5 + 6.35 - IF`;
- 12CO(2-1): `225.0 + 4.0 + IF`;
- 13CO/C18O(2-1): `225.0 - 6.35 + IF`.

The snapshot LO chains reproduce these formulas:

- `rx100_12co10_usb_lsb`: `signed_lo_sum_hz = 116.1 GHz`, `if_frequency_sign = -1`;
- `rx100_13co_c18o10_usb_lsb`: `signed_lo_sum_hz = 110.85 GHz`, `if_frequency_sign = -1`;
- `rx200_12co21_usb_usb`: `signed_lo_sum_hz = 229.0 GHz`, `if_frequency_sign = +1`;
- `rx200_13co_c18o21_lsb_lsb`: `signed_lo_sum_hz = 218.65 GHz`, `if_frequency_sign = +1`.

## Found and fixed issue unrelated to the arithmetic

During the converter snapshot-load check, I found that the embedded snapshot beam fields use empty strings for optional fields such as `rotation_slope_deg_per_deg` and `pure_rotation_offset_x_el0_arcsec`.  The converter-side beam parser treated `""` as a numeric value and could fail before reaching the spectral conversion stage.

Fix: treat blank strings in optional beam fields as missing values.  This does not change frequency arithmetic, but it is required for the converter to load the actual real snapshot reliably.

Added regression test:

- `tests/test_update14_snapshot_frequency_wcs_exactness.py`

This test checks:

1. embedded beam blank optional fields do not break snapshot bundle loading;
2. `saved_ch_start:saved_ch_stop` materializes to local explicit WCS with no off-by-one shift.

## SD FITS writer status

The previous writer patch is still required.  It makes topocentric FREQ rows emit row-level `RESTFREQ` and `VELDEF` when line context is present.  This is important for a merged SD FITS table containing 12CO/13CO/C18O rows with different rest frequencies.

The added writer test is:

- `tests/test_sdfits_writer_multiline_restfreq_columns.py`

In this environment, astropy is not installed, so the FITS write/read test is skipped here.  The writer and converter files passed `py_compile`, and the no-astropy snapshot/WCS tests passed.

## Final judgement

No one-channel shift was found in the snapshot window resolution, runtime slicing, or converter WCS propagation.  The frequency sign convention is consistent with the old converter configs.  The SD FITS row arguments receive the correct per-stream `CRVAL1`, `CDELT1`, `CRPIX1`, `RESTFREQ`, `SPECSYS`, and `VELDEF`.

Required patch from this recheck:

- converter beam parser blank-optional-field fix;
- regression test for snapshot saved-channel WCS exactness.

