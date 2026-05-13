# update13 snapshot autodiscovery patch report

## Purpose

Connect the existing `spectral_recording_snapshot.toml` DB sidecar discovery helper to converter/sunscan default paths.

## Behavior after this patch

### converter

If neither `--spectral-recording-snapshot` nor `--spectrometer-config` is specified, the converter searches the input `RAWDATA_DIR` for a unique `spectral_recording_snapshot.toml`.

- If exactly one snapshot is found, it is used automatically through the snapshot adapter.
- If no snapshot and no new setup sidecars are found, the historical legacy single-stream fallback remains unchanged.
- If setup sidecars such as `lo_profile.toml`, `recording_window_setup.toml`, or `beam_model.toml` exist but `spectral_recording_snapshot.toml` is absent, conversion fails explicitly instead of silently falling back to legacy.
- If multiple canonical snapshots are found, it fails explicitly.

### sunscan extract / singlebeam

If neither `--spectral-recording-snapshot` nor `--spectrometer-config` is specified, sunscan searches the RawData directory for a unique `spectral_recording_snapshot.toml`.

- If found, it is used automatically.
- If no snapshot is found and the DB is legacy, the previous explicit-config requirement remains.
- If new spectral setup sidecars exist without a snapshot, it fails explicitly. Plain `.obs` files and generic `config.toml` / `*_config.toml` files are not by themselves treated as new spectral setup sidecars.

### sunscan_multibeam wrapper

The wrapper now forwards `--spectral-recording-snapshot`, `--beam-model`, and `--allow-beam-model-override` to the public API/extract path.

## Modified files

- `sd-radio-spectral-fits-main/src/tools/necst/necst_v4_sdfits_converter.py`
- `sd-radio-spectral-fits-main/src/tools/necst/multibeam_beam_measurement/config_io.py`
- `sd-radio-spectral-fits-main/src/tools/necst/multibeam_beam_measurement/sunscan_extract_multibeam.py`
- `sd-radio-spectral-fits-main/src/tools/necst/multibeam_beam_measurement/sunscan_singlebeam.py`
- `sd-radio-spectral-fits-main/src/tools/necst/multibeam_beam_measurement/public_api.py`
- `sd-radio-spectral-fits-main/src/tools/necst/multibeam_beam_measurement/sunscan_multibeam.py`

## Verification

- `py_compile` passed for all 594 Python files in the fresh tree built from the base ZIPs plus update12 plus this patch.
- Sidecar autodiscovery simulation passed:
  - nested snapshot sidecar is found
  - duplicate snapshots raise an error
  - incomplete new spectral setup sidecars without snapshot raise an error
  - legacy directory without sidecars returns `None`

## Additional review on 2026-04-30

単一 `RAWDATA_DIR` に対する converter、sunscan extract、sunscan singlebeam の snapshot自動検出は、update13の実ソースで接続済みである。

通常運用では、次のように `--spectral-recording-snapshot` を省略できる。

```bash
python -m tools.necst.necst_v4_sdfits_converter \
  RAWDATA_DIR \
  --converter-analysis-config converter_analysis.toml
```

```bash
python -m tools.necst.multibeam_beam_measurement.sunscan_multibeam extract \
  RAWDATA_DIR \
  --sunscan-analysis-config sunscan_analysis.toml
```

```bash
python -m tools.necst.multibeam_beam_measurement.sunscan_singlebeam \
  RAWDATA_DIR \
  --sunscan-analysis-config sunscan_analysis.toml
```

## 実装上の優先順位

```text
1. 明示 --spectral-recording-snapshot
2. 明示 --spectrometer-config
3. RAWDATA_DIR 内の一意な spectral_recording_snapshot.toml
4. legacy動作
```

converterでは、snapshotもspectrometer_configも無く、new spectral setup sidecarも無い場合に限り、従来の legacy single-stream fallback を維持する。sunscanでは、legacy DBに対して従来どおり明示configを要求する。

## 異常系

```text
複数snapshot:
  エラー。勝手に1つを選ばない。

new setup sidecarがあるがsnapshotが無い:
  incomplete new DB としてエラー。
  converterでもlegacyへ黙ってfallbackしない。
```

## 直接確認したファイル

```text
sd-radio-spectral-fits-main/src/tools/necst/necst_v4_sdfits_converter.py
sd-radio-spectral-fits-main/src/tools/necst/multibeam_beam_measurement/config_io.py
sd-radio-spectral-fits-main/src/tools/necst/multibeam_beam_measurement/sunscan_extract_multibeam.py
sd-radio-spectral-fits-main/src/tools/necst/multibeam_beam_measurement/sunscan_singlebeam.py
sd-radio-spectral-fits-main/src/tools/necst/multibeam_beam_measurement/public_api.py
sd-radio-spectral-fits-main/src/tools/necst/multibeam_beam_measurement/sunscan_multibeam.py
```

## 確認したシミュレーション

```text
legacy DB:
  spectral setup sidecar無し（`.obs` / `config.toml` のみを含む） -> None

flat snapshot:
  RAWDATA_DIR/spectral_recording_snapshot.toml -> 検出

nested snapshot:
  RAWDATA_DIR/metadata/config/spectral_recording_snapshot.toml -> 検出

duplicate snapshot:
  複数の spectral_recording_snapshot.toml -> error

incomplete new DB:
  lo_profile.toml 等はあるが snapshot無し -> error
```

## 注意

sunscanの複数 `RAWDATA_DIR` 入力では、同じ観測設定のDB群をまとめる運用を前提にする。異なるsnapshotを持つDBを一度に混ぜる場合は、DBごとに分けて実行するか、共通の明示 config/snapshot を指定する。

## 検証

```text
統合tree Python:
  594 files py_compile OK

軽量import:
  tools.necst.spectral_recording_snapshot OK
  tools.necst.multibeam_beam_measurement.config_io OK

未実施:
  ROS2 colcon build
  service/msg生成
  実RawData converter/sunscan
  実XFFTS/NECSTDB recording
```


## 追加確認: legacy DB と `.obs` / `config.toml` の扱い

`.obs` ファイルや一般の `config.toml` / `*_config.toml` は、RawData内に存在していても、それだけでは new spectral setup sidecar とは扱わない。これらは旧RawDataにも存在し得るためである。snapshotが無い状態で incomplete new DB として扱うのは、`lo_profile.toml`、`recording_window_setup.toml`、`beam_model.toml`、`pointing_param.toml` のような spectral recording setup sidecar がある場合である。
