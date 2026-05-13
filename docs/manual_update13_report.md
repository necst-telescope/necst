# update13 snapshot autodiscovery review report

Version: 2026-04-30  
対象: `pr8g_update13.zip` を元ZIP群へ重ねた統合tree

## 結論

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


update13 priority 1-5 の実装詳細は `docs/update13_priority1_5_implementation_report_2026-04-30.md` を参照してください。


## update13 p16: 分光計取得ホットパス確認

p16では、分光計データ取得からNECSTDB書き込みまでの中枢経路を見直した。主な修正は、`fetch_data()`の途中return廃止、`get_data()`の部分消費防止、active spectrum branchのdirect chunk化、append pathのprecompute、NECSTDBWriterのburst drainである。

確認した疑似シミュレーション:

```text
first spectrometer empty / second ready:
  second queueを読み出す

A ready / B empty:
  Aを部分消費しない

single spectrometer:
  従来通り1 packetを取得してresizerへpush

active multi-window:
  1 raw spectrumから複数recorded productへ正しくsliceしてappend

writer:
  spectrum chunkのheader schemaが期待通り
```

詳細は `docs/update13_realtime_hotpath_performance_report_2026-04-30.md` を参照してください。

---

## update13 p16g: XFFTS timestamp診断とconverter時刻解釈override

p16gでは、IRIG-B投入後のXFFTS timestampを安全に検証するため、converterへ時刻診断モードと明示的な時刻解釈overrideを追加した。

### 追加CLI

```bash
python -m tools.necst.necst_v4_sdfits_converter RAWDATA_DIR \
  --inspect-spectral-time
```

このモードはSDFITSを作らず、RawData内の分光テーブルから以下を読み出してJSONで比較する。

```text
time_spectrometer / timestamp : XFFTS header timestamp
time                         : NECST host receive time
recorded_time                : NECSTDB writer write time
```

時刻基準とtimestamp本文の解釈は次で指定できる。

```text
--spectral-time-source auto
--spectral-time-source host-time
--spectral-time-source xffts-timestamp

--xffts-timestamp-scale auto
--xffts-timestamp-scale utc
--xffts-timestamp-scale gps
--xffts-timestamp-scale tai
```

`auto` は従来互換であり、`UTC/GPS/TAI` suffixならtimestampを使い、`PC`/unknown/空ならhost receive timeへfallbackする。  
`xffts-timestamp` はtimestampを必須にするため、IRIG-B時刻を本観測に使う時の安全装置になる。  
`--xffts-timestamp-scale utc/gps/tai` は、XFFTSのliteral suffixではなく、timestamp本文をどの時刻系として解釈するかを明示する。

UTC入力なのにsuffixがGPSになる疑いがある場合は、まず以下で診断する。

```bash
python -m tools.necst.necst_v4_sdfits_converter RAWDATA_DIR \
  --inspect-spectral-time \
  --spectral-time-source xffts-timestamp \
  --xffts-timestamp-scale utc
```

本変換でtimestamp本文をUTCとして使う場合は以下のように明示する。

```bash
python -m tools.necst.necst_v4_sdfits_converter RAWDATA_DIR \
  --spectral-time-source xffts-timestamp \
  --xffts-timestamp-scale utc
```

timestampが怪しい場合は従来のhost time基準へ明示的に戻せる。

```bash
python -m tools.necst.necst_v4_sdfits_converter RAWDATA_DIR \
  --spectral-time-source host-time
```

詳細は `docs/update13_p16g_spectral_time_diagnostics_report_2026-04-30.md` を参照する。



---

## update14 time consistency: XFFTS/IRIG-B 時刻仕様

update14では、converter と sunscan の時刻設定を統一した。XFFTS `time_spectrometer` / `timestamp` は積分中心時刻として扱う。IRIG-B ONのOMU/XFFTS実測では literal suffix は `GPS` だが本文はUTC相当であるため、既定では `xffts_gps_suffix_means = "utc"` とする。

```toml
[spectral_time]
spectral_time_source = "auto"
xffts_timestamp_scale = "auto"
xffts_gps_suffix_means = "utc"
spectrometer_time_offset_sec = 0.0

[encoder_time]
encoder_shift_sec = 0.0
encoder_az_time_offset_sec = 0.0
encoder_el_time_offset_sec = 0.0
```

`PC` suffix は自動ではtimestampとして採用せず、host-time fallbackを使う。`spectrometer_time_offset_sec` はhost-time fallback時だけ適用し、XFFTS timestamp採用時には適用しない。`encoder_shift_sec`, `encoder_az_time_offset_sec`, `encoder_el_time_offset_sec` はspectral time選択とは独立にencoder時刻側へ適用する。

観測時保存では、NECSTDB spectral tableの `time` はXFFTS header 64 byteを受け取った直後の `header["received_time"]` とする。`recorded_time` はwriter書き込み時刻であり、取得時刻ではない。

