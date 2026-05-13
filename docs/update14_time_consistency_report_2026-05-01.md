# update14_time_consistency 実装・確認レポート

Version: update14_time_consistency_2026-05-01  
Base: original zip群へ、update13 / p16f / p17a / p16g / ti 相当を統合した cumulative tree

## 結論

この版では、XFFTS時刻、host受信時刻、converter、sunscan の時刻仕様を揃えた。

- XFFTS `header["received_time"]` を NECSTDB spectral table の `time` として使う。
- XFFTS `time_spectrometer` / `timestamp` は積分中心時刻として扱う。
- `GPS` suffix は OMU/XFFTS IRIG-B 実測に合わせ、既定では timestamp本文を UTC として解釈する。
- `spectrometer_time_offset_sec` は host-time fallback 時だけ適用する。
- XFFTS timestamp 採用時には `spectrometer_time_offset_sec` を適用しない。
- converter と sunscan は同じ CLI/TOML 設定名を使う。
- `encoder_shift_sec`, `encoder_az_time_offset_sec`, `encoder_el_time_offset_sec` は spectral time 選択とは独立に encoder 側へ適用する。

## 時刻定義

```text
t_xffts_text
  XFFTS header timestamp string.
  NECSTDBでは time_spectrometer。

t_header_received
  xfftspy.data_consumer._receive_header() が 64 byte header を受け取った直後の time.time().
  header["received_time"].

t_packet_received
  receive_once() が payload まで読み終えた後の時刻。
  今回の仕様では spectral table の標準 time には使わない。

t_recorded
  NECSTDBWriter が書き込んだ時刻。
  recorded_time.

t_spec
  converter / sunscan が解析に使う分光データ時刻。
```

NECSTDB spectral table の保存仕様:

```text
time              = t_header_received
time_spectrometer = t_xffts_text
recorded_time     = t_recorded
```

## 共通CLI

converter と sunscan で同じ名前を使う。

```bash
--spectral-time-source auto|xffts-timestamp|host-time
--xffts-timestamp-scale auto|utc|gps|tai
--xffts-gps-suffix-means utc|gps
--spectrometer-time-offset-sec FLOAT
--encoder-shift-sec FLOAT
--encoder-az-time-offset-sec FLOAT
--encoder-el-time-offset-sec FLOAT
```

既定値:

```text
spectral_time_source = auto
xffts_timestamp_scale = auto
xffts_gps_suffix_means = utc
spectrometer_time_offset_sec = 0.0
```

## 共通TOML

`converter_analysis.toml` と `sunscan_analysis.toml` で同じ形を受け付ける。

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

これらの値は `config_dict["global"]` へ展開され、従来の `[converter_analysis]` / `[sunscan_analysis]` 内の同名scalar値とも互換にした。

## 時刻選択規則

```text
spectral_time_source = auto

suffix = UTC:
  timestamp本文をUTCとして使う。

suffix = GPS:
  xffts_gps_suffix_means に従う。
  OMU既定では utc なので、timestamp本文をUTCとして使う。

suffix = TAI:
  timestamp本文をTAIとしてUTCへ変換する。

suffix = PC:
  timestampは自動採用しない。
  host-time fallbackを使う。

parse失敗:
  host-time fallbackを使う。
```

```text
spectral_time_source = xffts-timestamp

timestampを必ず使う。
PC suffixでも --xffts-timestamp-scale utc/gps/tai が明示されれば使える。
使えない場合はエラーにする。
```

```text
spectral_time_source = host-time

timestampを使わず、NECSTDB spectral table の time を使う。
spectrometer_time_offset_sec を適用する。
```

## offset適用規則

```text
if selected_time_source == "xffts-timestamp":
    t_spec = timestamp-derived UTC unix seconds
    spectrometer_time_offset_sec is NOT applied

if selected_time_source == "host-time":
    t_spec = time + spectrometer_time_offset_sec
```

XFFTS timestamp は積分中心時刻として扱うため、timestamp採用時に積分時間/2の補正はしない。

encoder側:

```text
t_enc_common = t_encoder + encoder_shift_sec
t_enc_az     = t_enc_common + encoder_az_time_offset_sec
t_enc_el     = t_enc_common + encoder_el_time_offset_sec

Az(t_spec) = interp(t_enc_az, encoder_az)
El(t_spec) = interp(t_enc_el, encoder_el)
```

これは converter と sunscan の両方で同じ考え方にした。

## 実測との対応

IRIG-B OFF の古い診断では `PC` suffix の本文がJSTローカル時刻風だったため、
UTC仮定では `timestamp_as_utc_minus_host_sec` が約 `32400 s` になった。
現在PCをUTC運用に変えた場合、この値は約0秒近くになり得る。
ただし仕様としては `PC` suffix は自動採用しない。

IRIG-B ON の診断では、literal suffix は `GPS` だが、本文をUTCとして読むとhost時刻に自然に近く、
GPSとして読むと18秒ずれる。したがってOMU/XFFTS既定は `xffts_gps_suffix_means = "utc"` とした。

## 変更ファイルの主な内訳

- `neclib/.../xffts.py`
  - `header["received_time"]` を spectral row の `time` として渡す。
- `sd-radio-spectral-fits/.../necst_v4_sdfits_converter.py`
  - spectral time共通設定、GPS suffix policy、host-time offset条件付き適用、encoder az/el offsetを追加。
- `sd-radio-spectral-fits/.../sunscan_legacy_compat.py`
  - converterと同じ時刻選択規則を実装。
  - `spectrometer_time_offset_sec` を host-time fallback時だけ適用。
- `sd-radio-spectral-fits/.../sunscan_config.py`
  - spectral time設定をInputConfigに追加。
- `sd-radio-spectral-fits/.../sunscan_extract_multibeam.py`
  - CLI/TOML反映、stream override対応。
- `sd-radio-spectral-fits/.../sunscan_singlebeam.py`
  - CLI/TOML/stream override対応。
- `sd-radio-spectral-fits/.../sunscan_io.py`
  - SunScanAnalysisConfigからbuild_dataframeへ新設定を渡す。
- `config_separation_model.py`
  - `[spectral_time]` と `[encoder_time]` を analysis config の共通sectionとして受ける。

## 確認項目

- 変更Pythonファイルの `py_compile`: OK
- integrated tree 全Pythonファイルの `py_compile`: OK
- `header["received_time"]` 使用箇所の文字列確認: OK
- converter/sunscanのGPS suffix policy存在確認: OK
- `spectrometer_time_offset_sec` がsunscan timestamp採用経路で無条件加算されないことを文字列確認: OK
- IRIG-B ON/OFF相当の時刻選択シミュレーション: OK

## 未実施

この環境では以下は未実施。

- ROS2 colcon build
- 実XFFTS入力
- 実NECSTDB recording
- 実RawDataに対するconverter/sunscanの最終実行
- Astropyあり環境での実timestamp変換の再確認

実機側では、次を確認すること。

```bash
uv run necst_v4_sdfits_converter RAWDATA_DIR --inspect-spectral-time
```

期待:

- IRIG-B ON:
  - `literal_suffix = GPS`
  - `timestamp_scale_used = UTC`
  - `selected_minus_host_sec` が約 -0.1秒程度
  - `spectrometer_time_offset_applied = false`
- IRIG-B OFF:
  - `literal_suffix = PC`
  - host-time fallback
  - `spectrometer_time_offset_applied` は offset が非ゼロなら true
