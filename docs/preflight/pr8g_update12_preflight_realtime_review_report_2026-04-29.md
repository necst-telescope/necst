# update12 実機前レビュー・高速化副作用確認レポート

作成日: 2026-04-29

対象:
- base ZIP群: neclib, necst-msgs, necst, necstdb, sd-radio-spectral-fits, xfftspy
- 入力変更ZIP: pr8g_update11_with_docs.zip
- 追加修正: `necst-second_OTF_branch/necst/rx/spectral_recording_runtime.py`

## 結論

update11を元ZIP群へ再適用して確認した結果、実機投入前に潰すべき軽微な速度改善点が1つ残っていたため修正した。
`streams_for_raw()` が cached stream list から毎回 `list(...)` を作っていたため、高頻度callbackで不要なlist allocationが残っていた。
update12では、setup apply時に raw input -> recorded products のtuple indexを作り、callbackではtuple参照を返す。

この変更は、出力されるstream順序、slice範囲、metadata chunk、multi-window recorded productの対応を変えない。

## 確認項目

### ZIP / file integrity

- update11 with docs ZIP: testzip OK
- update12 full ZIP: testzip OK
- update12 patch ZIP: testzip OK
- `__pycache__`, `.pyc`, `.pyo`: 混入なし
- update12 full ZIP収録ファイルと統合tree実ファイル: 一致

### Python構文

- 統合tree内 Python 594 files: `py_compile` OK
- 変更対象 Python: `ast.parse` / `py_compile` OK

### 旧conf同等性

添付旧conf `12co-NANTEN2-multi_260331TO.conf3(3).txt` 相当の新分離ファイルを読み、topocentric test用recording windowでsnapshot解決した。

確認:
- source streams: 16
- recorded products: 23
- multi-window products: 14
- stream field一致: `fdnum`, `ifnum`, `plnum`, `polariza`, `beam_id`, `frontend`, `backend`, `sampler`, `db_stream_name`
- rest frequency一致
- legacy local_oscillators一致: `lo1_hz`, `lo2_hz`, `sb1`, `sb2`
- 差分: 0

### velocity window

- `TOPOCENTRIC`: snapshot resolve OK
- `LSRK` without reference context: 明示エラー OK
- 線形WCSの直接channel範囲計算と旧array方式: random 1000 casesで一致

注意:
- このコンテナにはAstropyが無いため、実際のLSRK補正値計算は未実行。
- 実機で `velocity_frame="LSRK"` を使う場合はAstropyが必要。

### runtime / multi-window

synthetic old-conf-equivalent snapshotで確認:
- raw `('xffts', 2)` -> `2LL__13CO_J1_0`, `2LL__C18O_J1_0`
- `slice_spectrum_for_stream()` の出力長とfirst/last indexは `saved_ch_start/saved_ch_stop` と一致
- static `_runtime_spectrum_extra_chunk` と従来の動的 `spectrum_extra_chunk()` が一致
- stream mappingは `MappingProxyType` でread-only
- unknown raw boardは空tupleを返す

### converter stream selection

converterの `_select_streams_for_convert()` 相当を実コードsnippetで検証:
- `--recorded-stream-id 2LL__13CO_J1_0`: OK
- `--stream-id 2LL --window-id C18O_J1_0`: OK
- `--window-id 13CO_J1_0`: OK
- `--stream-id 1LU`: OK
- `--stream-id 2LL` のみ: ambiguous error OK

### 性能

この環境での参考値:

`streams_for_raw('xffts', 2)` 500,000 calls median:
- update11: 約 0.187 s
- update12: 約 0.101 s
- 約 1.8 倍高速

callback側の意味:
- update11ではtuple化済みでなく、lookupごとにlist allocationが残っていた
- update12ではtupleをそのまま返す
- for-loop利用、len、indexアクセスの互換性は維持

## 未実施

- ROS2 `colcon build`
- ROS2 service code generation
- 実XFFTS stream入力
- 実NECSTDBへのrecording
- Astropyあり環境での実LSRK補正値確認
- 実converter/sunscan実DB処理

## 実機投入前チェック

1. `colcon build` を実行する。
2. `ApplySpectralRecordingSetup.srv` 等のservice生成を確認する。
3. `necst-lo-profile summary/apply/verify` でSG readbackを確認する。
4. `velocity_frame="LSRK"` を使う場合、Astropyがimportできることを確認する。
5. 最初は `recording_window_topo.toml` またはfull spectrumで短時間dry-runする。
6. multi-window使用時は、DB tableが `xffts-board2__13CO_J1_0` などに分かれることを確認する。
7. queue size / append latency / first row table creation latencyをlogで見る。
