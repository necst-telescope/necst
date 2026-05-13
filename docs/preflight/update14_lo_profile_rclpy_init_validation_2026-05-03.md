# update14 lo-profile CLI rclpy init validation (2026-05-03)

## Issue

`necst-lo-profile apply lo_profile.toml --id band3_1st` failed before creating Commander:

```text
rclpy.exceptions.NotInitializedException: ('rclpy.init() has not been called', 'cannot create node')
```

## Cause

`main_lo_profile()` imported and instantiated `necst.core.Commander` directly for `apply`/`verify`, but did not initialize the ROS2 Python client library.  This was missed because the helper-level SG tests do not require ROS.

The `summary` subcommand remains ROS-free.

## Fix

For `apply`/`verify` only, `main_lo_profile()` now:

1. imports `rclpy` lazily,
2. checks `rclpy.ok()`,
3. calls `rclpy.init()` only when needed,
4. creates `Commander()`,
5. destroys the Commander node on exit,
6. calls `rclpy.shutdown()` only if this CLI initialized rclpy itself.

This matches the pattern used by observation/measurement runners while keeping pure helper functions and `summary` free of ROS runtime requirements.

## Validation

- `py_compile` passed for:
  - `necst/rx/spectral_recording_sg.py`
  - `tests/test_spectral_recording_sg_pr3.py`
- Direct SG helper test runner passed.
- Added tests verify:
  - `rclpy.init()` is called before `Commander()` when no context exists,
  - `rclpy.shutdown()` is called only when this CLI initialized rclpy,
  - an existing rclpy context is reused and not shut down by the CLI.

## Operational note

After applying this patch to the source tree, rebuild/reinstall the `necst` Python package or rerun the command from an overlay that points to the patched source.  The traceback path in the failing run was under `/root/ros2_ws/build/necst/...`, so a stale installed/build copy may continue to show the old behavior until rebuilt.
