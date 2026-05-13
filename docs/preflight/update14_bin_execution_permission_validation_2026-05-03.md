# update14 bin execution permission validation (2026-05-03)

## Purpose

This patch addresses the source-tree execution-permission problem for the update14 helper CLIs:

- `necst-lo-profile`
- `necst-spectral-resolve`
- `necst-spectral-validate`

The original NECST source-tree workflow uses the executable `bin/necst` wrapper, which calls `bin/*.py` files through `python3`.  Therefore the individual `.py` files do not need executable permissions.  In contrast, the update14 standalone helper scripts are executed directly when `src/necst/bin` is on `PATH`, so their executable bit matters.

## Implemented compatibility layer

The following `.py` aliases were added:

```text
necst-second_OTF_branch/bin/lo_profile.py
necst-second_OTF_branch/bin/spectral_resolve.py
necst-second_OTF_branch/bin/spectral_validate.py
```

These allow the permission-stable source-tree form:

```bash
necst lo_profile summary lo_profile.toml
necst lo_profile apply lo_profile.toml --id sg_lsb_2nd --verify --timeout-sec 10
necst spectral_resolve --lo-profile lo_profile.toml --recording-window-setup recording_window_setup.toml --beam-model beam_model.toml --setup-id orikl_NANTEN2_multi_12co_260331TO --output spectral_recording_snapshot.toml
necst spectral_validate spectral_recording_snapshot.toml
```

The existing standalone wrappers are also kept and packaged with executable mode:

```text
necst-second_OTF_branch/bin/necst-lo-profile        0755
necst-second_OTF_branch/bin/necst-spectral-resolve  0755
necst-second_OTF_branch/bin/necst-spectral-validate 0755
```

`bin/necst` is also included with executable mode `0755` so that the source-tree wrapper itself remains executable when the patch is applied with a permission-preserving unzip tool.

## Boundary condition

Linux `unzip` preserves the executable bits stored in this patch archive.  Some Python-based extraction methods do not preserve zip permission metadata.  If such an extraction method is used, the robust source-tree command is still `necst lo_profile ...` as long as the original `bin/necst` wrapper remains executable.  The existing operational NECST workflow already depends on `bin/necst` being executable.

## Validation performed

- Added `.py` aliases compile with `py_compile`.
- Standalone wrapper contents are unchanged except for executable mode in the patch archive.
- The patch archive records executable permissions for `bin/necst` and the three standalone helper CLIs.
- Documentation was updated to describe both invocation styles.
