# update13 priority 1-5 実装レポート

Date: 2026-04-30  
Base: `pr8g_update13_reviewed_fixed.zip`  
Output: `pr8g_update13_priority1_5.*`

## 実装対象

update13 deep reviewで挙げた優先改良候補 1-5 を実装した。

```text
1. sunscan複数RawData入力で、DBごとにsnapshotを完全独立自動検出する
2. sidecar discovery結果を表示するdry-run CLIを追加する
3. converter/sunscanの起動時に、実際に使ったconfig sourceをmanifestへ保存する
4. duplicate snapshot時のエラー文をさらに親切にする
5. 小さい疑似RawData fixtureでCLIに近い回帰試験を追加する
```

## 1. sunscan multibeam 複数RawData

`sunscan_extract_multibeam.config_from_args()` は、複数RawData入力時に最初のDBのsnapshotを `args.spectral_recording_snapshot` へ束縛しないようにした。

`run_extract_many()` は、明示snapshotがない場合、各 `run_extract()` に `spectral_recording_snapshot=None` を渡す。これにより、各run内で `discover_default_spectral_recording_snapshot(rawdata_path)` が実行され、RawDataごとに独立した自動検出になる。

明示snapshotがある場合は、従来通りそのsnapshotを全runに使う。これは自動検出ではなく、ユーザーが明示した共通設定として扱う。

## 2. sidecar dry-run CLI

追加CLI:

```bash
python -m tools.necst.necst_v4_sdfits_converter RAWDATA_DIR --inspect-sidecars
```

```bash
python -m tools.necst.multibeam_beam_measurement.sunscan_multibeam inspect-sidecars RAWDATA_001 RAWDATA_002
```

```bash
python -m tools.necst.multibeam_beam_measurement.sunscan_multibeam extract RAWDATA_001 RAWDATA_002 --inspect-sidecars
```

```bash
python -m tools.necst.multibeam_beam_measurement.sunscan_singlebeam RAWDATA_DIR --inspect-sidecars
```

dry-runはJSONを出力し、解析本体は実行しない。

## 3. config source manifest

converter:

```text
OUTPUT.fits.config_manifest.json
```

を出力する。FITS HISTORYにも以下を追加する。

```text
config_source_kind
config_source_path
config_source_auto
config_source_explicit
config_manifest
```

sunscan multibeam:

```text
sunscan_multibeam_manifest_*.csv
sunscan_multibeam_run_table_*.csv
analysis_config_snapshot_*.json
```

に `config_source_*` 情報を残す。

## 4. duplicate snapshot error

`discover_db_sidecar_files(..., strict_duplicates=True)` で複数canonical sidecarを見つけた場合、候補pathと推奨対処を含む例外を出す。

`inspect_db_sidecars()` は `strict_duplicates=False` で走査し、以下をJSONで返す。

```text
layout_kind = "DUPLICATE_CANONICAL_SIDECARS"
duplicate_canonical_paths
recommended_action
```

## 5. 回帰試験

追加:

```text
sd-radio-spectral-fits-main/tests/test_necst_update13_priority1_5.py
```

確認:

```text
- duplicate snapshot dry-run report
- sunscan multibeam run_extract_many の暗黙snapshot独立性
- run tableへのconfig_source記録
- 複数RawData時にconfig_from_argsが最初のDB snapshotを束縛しないこと
```

## 確認結果

この環境で確認した内容:

```text
py_compile:
  spectral_recording_snapshot.py OK
  necst_v4_sdfits_converter.py OK
  sunscan_extract_multibeam.py OK
  sunscan_multibeam.py OK
  sunscan_singlebeam.py OK
  test_necst_update13_priority1_5.py OK

pytest:
  tests/test_necst_update13_priority1_5.py
  3 passed

dry-run simulation:
  LEGACY_DB
  NEW_DB_WITH_SNAPSHOT
  DUPLICATE_CANONICAL_SIDECARS
  INCOMPLETE_NEW_DB
```

## 未実施

```text
ROS2 colcon build
実necstdb RawDataでのconverter実行
実sunscan extract
実XFFTS/実観測DBでの確認
```

