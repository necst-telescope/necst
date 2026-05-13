# update14 recording metadata/path cleanup preflight

## 目的

2026-05-03の実観測テストで、snapshotとNECSTDB出力に以下の2点が確認された。

1. single-window `contiguous_envelope` stream（例: board1/board3）では、`computed_windows` 内には `line_name` / `window_id` があるが、stream top-levelには無かった。
2. `multi_window` streamの `db_table_path` が `data/spectral/xffts/xffts-board2__13CO_J2_1` のようになり、NECSTDB実ファイル名では `xffts` が二重に見える。

## 修正方針

### single-window contiguous_envelope

`computed_windows` から、実windowが1つだけの場合に限り、以下をstream top-levelへ昇格する。

- `window_id`
- `line_name`
- `rest_frequency_hz`

複数windowを1つのenvelopeにまとめる場合は、1つのline名に畳み込むと誤解を招くため、top-level `line_name` は付けず、`source_window_ids` を保持する。

### multi_window db_table_path

互換性のため、`db_stream_name` / `recorded_db_stream_name` は従来どおり `xffts-board2__13CO_J2_1` 形式を保持する。

一方、実際にNECSTDBへ保存する `db_table_path` は、source streamのcanonical table leafを使い、以下のようにする。

```text
old: data/spectral/xffts/xffts-board2__13CO_J2_1
new: data/spectral/xffts/board2__13CO_J2_1
```

これにより、実ファイル名は `necst-OMU1P85M-data-spectral-xffts-board2__13CO_J2_1.data` となり、`xffts-xffts-board2` の見た目の重複を避ける。

## 検証

直接importによる最小検証を行った。

- multi-window stream:
  - `recorded_db_stream_name = xffts-board2__13CO_J1_0` は保持。
  - `db_table_path = data/spectral/xffts/board2__13CO_J1_0` へ短縮。
  - snapshot validation passed.
- single-window contiguous_envelope stream:
  - top-level `window_id = 12CO_J1_0`
  - top-level `line_name = 12CO J=1-0`
  - top-level `rest_frequency_hz = 115271000000.0`
  - snapshot validation passed.

`pytest` はこのコンテナに `rclpy` が無いためconftest importで起動できなかった。該当ロジックは `spectral_recording_setup.py` を直接importして検証した。
