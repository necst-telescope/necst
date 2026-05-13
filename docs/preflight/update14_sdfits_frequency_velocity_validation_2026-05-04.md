# update14 SD FITS frequency/velocity validation

Date: 2026-05-04

## Scope

This check uses the cumulative update14 tree as of 2026-05-03 and the real
`spectral_recording_snapshot.toml` from `necst_otf_20260503_114555_test`.

Definitions:

- Full XFFTS IF axis: `IF(ch) = if_freq_at_full_ch0_hz + ch * if_freq_step_hz`.
- Snapshot sky axis: `sky(ch) = signed_lo_sum_hz + if_frequency_sign * IF(ch)`.
- Converter SD FITS WCS for a saved product:
  - `CRPIX1 = 1.0`
  - `CRVAL1 = sky(saved_ch_start)`
  - `CDELT1 = if_frequency_sign * if_freq_step_hz`
  - `NCHAN = saved_nchan = saved_ch_stop - saved_ch_start`
- Radio velocity from a topocentric frequency axis:
  - `v_radio = c * (1 - f / RESTFREQ)`

## Result

The frequency-axis reconstruction is internally consistent for all six saved
products.  The line center lies inside the saved channel range in every case.
The sign of `CDELT1` correctly follows `if_frequency_sign`.

| stream | line | saved_ch | NCHAN | REST GHz | CRVAL1 GHz | last GHz | CDELT1 Hz | line IF GHz | line full ch | topocentric radio velocity range km/s |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `xffts_board1` | 12CO J=2-1 | 18185–21410 | 3225 | 230.538000 | 230.387447737 | 230.633426923 | 76296.273690 | 1.538000 | 20158.258 | 195.779 → -124.094 |
| `xffts_board2__13CO_J2_1` | 13CO J=2-1 | 21037–24120 | 3083 | 220.399000 | 220.255044710 | 220.490189825 | 76296.273690 | 1.749000 | 22923.793 | 195.812 → -124.039 |
| `xffts_board2__C18O_J2_1` | C18O J=2-1 | 10052–13124 | 3072 | 219.560357 | 219.416930143 | 219.651236000 | 76296.273690 | 0.910357 | 11931.867 | 195.838 → -124.088 |
| `xffts_board3` | 12CO J=1-0 | 10241–11853 | 1612 | 115.271000 | 115.318649861 | 115.195736564 | -76296.273690 | 0.829000 | 10865.537 | -123.926 → 195.742 |
| `xffts_board4__13CO_J1_0` | 13CO J=1-0 | 7909–9450 | 1541 | 110.201000 | 110.246572771 | 110.129076510 | -76296.273690 | 0.649000 | 8506.313 | -123.977 → 195.662 |
| `xffts_board4__C18O_J1_0` | C18O J=1-0 | 13401–14936 | 1535 | 109.782182 | 109.827553636 | 109.710515152 | -76296.273690 | 1.067818 | 13995.677 | -123.901 → 195.707 |

The small differences between the saved frequency endpoints and the requested
window edges are below one channel and are expected from integer channel
rounding.

## Finding fixed in this patch

The converter passes row-specific `restfreq_hz`, `crval1_hz`, `cdelt1_hz`,
`crpix1`, `ctype1`, `cunit1`, `specsys`, and `veldef` to the SDFITS writer.
However, the writer only emitted the `RESTFREQ` and `VELDEF` columns when the
dataset had explicit velocity context such as `SPECSYS='LSRK'`, non-`FREQ`
`CTYPE1`, or `VFRAME`.

For snapshot-driven update14 data, the output normally uses a topocentric
frequency axis:

- `CTYPE1 = 'FREQ'`
- `SPECSYS = 'TOPOCENT'`
- `VELDEF = 'RADIO'`
- one row/product may have a different `RESTFREQ`

Therefore the old writer behavior could omit row-level `RESTFREQ` / `VELDEF`
columns and leave only a primary-header default.  That is unsafe for multi-line
outputs because the primary header can represent only one default rest
frequency.

The writer is now changed so that any meaningful row-level `RESTFREQ` activates
line context and emits both `RESTFREQ` and `VELDEF` columns in the SINGLE DISH
table.

## Important interpretation

The converter output is a topocentric frequency-axis SD FITS unless `SPECSYS`
is explicitly changed to `LSRK` and a frequency column is written.  The recording
window was selected using the LSRK velocity request at snapshot creation time,
but the saved data themselves are not resampled to a common LSRK grid by this
converter step.  LSRK coadd/regridding remains a later processing step.
