# update14 follow-up: converter/sunscan handling of shortened snapshot `db_table_path`

## Summary

The recording-side cleanup changed split-window products from paths such as

```text
data/spectral/xffts/xffts-board2__13CO_J2_1
```

to shorter canonical paths such as

```text
data/spectral/xffts/board2__13CO_J2_1
```

This is the right direction for the recording metadata, but converter and sunscan
must treat snapshot `db_table_path` as the authoritative DB binding and convert it
back to the NECSTDB table filename stem before calling `db.open_table()`.

## Problem found

The converter already preserved `db_table_path` in `stream.db_table_name`, but
`_resolve_spec_table_name()` returned it literally.  Since NECSTDB stores topic
names as hyphen-normalized table stems, a literal path like
`data/spectral/xffts/board2__13CO_J2_1` is not directly accepted by
`db.open_table()`.

Sunscan had the same issue in a different form: snapshot materialization carried
`db_table_name`, but the multibeam and singlebeam runners still used
`db_stream_name` as the spectral table selector.  After the recording-side path
cleanup, this would point sunscan to the old redundant table name.

## Fix

### Converter

`necst_v4_sdfits_converter.py` now resolves snapshot paths as follows:

```text
stream.db_table_name = data/spectral/xffts/board2__13CO_J2_1
  -> necst-OMU1P85M-data-spectral-xffts-board2__13CO_J2_1
```

Already-qualified legacy table names are preserved.

### Sunscan

`sunscan_legacy_compat.py` now accepts either:

```text
xffts-board1
```

or

```text
data/spectral/xffts/board2__13CO_J2_1
```

or an already-qualified NECSTDB table stem.

`sunscan_extract_multibeam.py` and `sunscan_singlebeam.py` now prefer
`stream.db_table_name` over `stream.db_stream_name` when a snapshot-derived stream
is selected.  This makes sunscan follow the same authoritative binding as the
converter.

## Compatibility

Legacy config operation remains unchanged:

```text
xffts-board1
  -> necst-OMU1P85M-data-spectral-xffts-board1
```

Snapshot-aware operation now correctly follows the new short path:

```text
data/spectral/xffts/board2__13CO_J2_1
  -> necst-OMU1P85M-data-spectral-xffts-board2__13CO_J2_1
```

## Tests

Added/extended tests:

```text
tests/test_converter_spectral_time_controls_p16g.py
tests/test_sunscan_db_table_path_resolution.py
```

Validated with:

```bash
PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 python3 -m pytest -q \
  tests/test_converter_spectral_time_controls_p16g.py \
  tests/test_sunscan_db_table_path_resolution.py
```

Result:

```text
12 passed
```
