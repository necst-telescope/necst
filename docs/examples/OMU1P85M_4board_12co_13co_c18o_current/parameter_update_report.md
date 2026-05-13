# OMU1P85M 4-board current parameter update

## SDFITS product numbering policy

- 200 GHz quasi-optical path: `fdnum = 1`
- 100 GHz quasi-optical path: `fdnum = 2`
- `ifnum` is local within each `fdnum` and is assigned as `0,1,2` per frequency path
- `plnum = 0` for all products

| product | line | rest frequency [GHz] | fdnum | ifnum | plnum |
|---|---|---:|---:|---:|---:|
| `xffts_board1__12CO_J2_1` | 12CO J=2-1 | 230.5380000 | 1 | 0 | 0 |
| `xffts_board2__13CO_J2_1` | 13CO J=2-1 | 220.3986842 | 1 | 1 | 0 |
| `xffts_board2__C18O_J2_1` | C18O J=2-1 | 219.5603541 | 1 | 2 | 0 |
| `xffts_board3__12CO_J1_0` | 12CO J=1-0 | 115.2712018 | 2 | 0 | 0 |
| `xffts_board4__13CO_J1_0` | 13CO J=1-0 | 110.2013543 | 2 | 1 | 0 |
| `xffts_board4__C18O_J1_0` | C18O J=1-0 | 109.7821734 | 2 | 2 | 0 |

## Implementation note

`recording_groups.*.windows[]` supports optional `fdnum`, `ifnum`, and `plnum` overrides.  These overrides apply only to the saved product stream and do not modify the source XFFTS board stream.

All recording groups use `saved_window_policy = "multi_window"`, including single-line boards, so that `window_id` is reflected in `recorded_stream_id` and `db_table_path`.
