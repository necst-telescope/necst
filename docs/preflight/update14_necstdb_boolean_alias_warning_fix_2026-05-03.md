# update14 NECSTDB boolean alias warning fix — 2026-05-03

## Symptom

During real observation recording, the recorder node repeatedly emitted:

```text
Unsupported NECSTDB field type: 'boolean'
```

## Cause

ROS 2 IDL exposes boolean message fields as `boolean`.  The NECSTDB writer supported `bool` but update14 changed the writer to exact type lookup for safer and faster packing.  As a result, `boolean` was no longer accepted and those fields were skipped.

This does not explain the spectral data files themselves; spectral arrays are written through explicit `float32` chunks.  The warning most likely comes from normal status/control topics that contain boolean flags.

## Fix

Add an explicit `boolean` alias to `NECSTDBWriter.DTypeConverters`, mapping it to the same `?` struct format as `bool`.

## Validation

Performed before packaging:

- `necstdb_writer.py` py_compile: OK
- `test_necstdb_writer.py` py_compile: OK
- direct `_parse_field({type="boolean"})` check: OK
- direct `_parse_field({type="boolean", value=[True, False]})` check: OK

No msg/service files are changed.
