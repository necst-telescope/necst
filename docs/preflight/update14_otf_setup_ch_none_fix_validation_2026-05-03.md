# update14 OTF setup `ch=None` compatibility fix validation (2026-05-03)

## Problem

`necst otf -f otf_lo_test.toml` failed after applying a spectral recording setup with:

```text
ValueError: spectral recording setup cannot be combined with legacy observation kwargs: ch
```

The command-line wrapper `bin/otf.py` always constructs the observation as:

```python
obs = OTF(file=args.file, ch=args.channel)
```

When `-c/--channel` is not specified, `args.channel` is `None`.  Therefore the
observation kwargs contain the key `ch`, but the value is inert and would not
trigger legacy binning in `Observation.execute()`.

The update14 setup-mode guard previously rejected the presence of the key itself:

```python
conflicts = [key for key in ("ch", "tp_mode", "tp_range") if key in kwargs]
```

That made normal `necst otf -f ...` fail whenever a new spectral recording setup
was present.

## Fix

`reject_legacy_recording_kwargs_for_setup()` now rejects only active legacy
controls:

- `ch` only when `ch is not None`.
- `tp_mode` only when truthy.
- `tp_range` only when non-empty.

Inactive CLI defaults such as `ch=None`, `tp_mode=False`, and `tp_range=[]` are
accepted in setup mode.

## Files changed

- `necst-second_OTF_branch/necst/procedures/observations/spectral_recording_sequence.py`
- `necst-second_OTF_branch/tests/test_spectral_recording_sequence_pr7.py`

## Validation

Commands run in the patched tree:

```bash
python3 -m py_compile \
  necst-second_OTF_branch/necst/procedures/observations/spectral_recording_sequence.py \
  necst-second_OTF_branch/tests/test_spectral_recording_sequence_pr7.py

cd /mnt/data/test_ch_none_full/necst-second_OTF_branch
PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 python3 -m pytest -q tests/test_spectral_recording_sequence_pr7.py
```

Result:

```text
7 passed
```

