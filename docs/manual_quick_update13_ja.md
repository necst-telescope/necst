# NECST/XFFTS spectral recording update13 簡易マニュアル

Version: update13 / 日本語版 / 2026-04-30  
対象実装: update13 tree (`pr8g_update13.zip` を直接確認し、snapshot自動検出を反映)

この簡易版は、実際に観測・受信機調整・converter/sunscanを走らせる人向けの手順書である。詳細な仕様、全パラメータ表、旧config相当の完全例は `manual_full_update13_ja.md` を参照する。

---

## 0. update13で追加されたsnapshot自動検出

通常運用では、converter/sunscanに `--spectral-recording-snapshot` を明示しなくても、`RAWDATA_DIR` 内の標準 sidecar を自動検出する。

優先順位:

```text
1. 明示 --spectral-recording-snapshot
2. 明示 --spectrometer-config
3. RAWDATA_DIR 内の一意な spectral_recording_snapshot.toml
4. legacy動作
```

異常系:

```text
複数snapshot:
  エラー。勝手に1つを選ばない。

lo_profile.toml / recording_window_setup.toml / beam_model.toml があるが snapshot無し:
  incomplete new DB としてエラー。

legacy DB:
  converterは従来の single-stream fallback を維持。
  sunscanは従来どおり明示 config を要求。
```


## 1. 何が変わったか

旧all-in-one `spectrometer_config` は、現在仕様では以下へ分離する。

| 目的 | ファイル |
|---|---|
| streamごとの分光計・LO・backend・beam・rest frequency | `lo_profile.toml` |
| 観測時に保存するchannel/velocity/window | `recording_window_setup.toml` |
| beam offset / rotation | `beam_model.toml` |
| 観測時に実際に使った解決済みtruth | `spectral_recording_snapshot.toml` |
| converter/sunscan対象stream選択 | `analysis_stream_selection.toml` |
| converter解析条件 | `converter_analysis.toml` |
| sunscan解析条件 | `sunscan_analysis.toml` |

`.obs` では `[parameters]` に参照を書く。

```toml
[parameters]
lo_profile = "lo_profile_12co_NANTEN2_multi_260331TO.toml"
recording_window_setup = "recording_window_12co_NANTEN2_multi_260331TO.toml"
beam_model = "beam_model_12co_NANTEN2_multi_260331TO.toml"
setup_id = "orikl_NANTEN2_multi_12co_260331TO"
```

何も書かなければlegacy観測のまま動く。

---

## 2. 受信機・SG設定

SGのIP addressやportはNECST `config.toml` に書く。`lo_profile.toml` には、そのSGを何GHz/何dBmで使うかを書く。

確認:

```bash
necst-lo-profile summary lo_profile.toml
```

SGを1台だけ設定・検証:

```bash
necst-lo-profile apply lo_profile.toml --id sg_lsb_2nd --verify --timeout-sec 10
```

全SGを設定:

```bash
necst-lo-profile apply lo_profile.toml --verify --timeout-sec 10
```

重要:

```text
lo_profile.toml の [sg_devices.<sg_id>]
  == config.toml の [signal_generator.<sg_id>]
```

`sg_set_frequency_ghz` はSGへ直接設定する周波数であり、逓倍後のphysical LOではない。

---

## 3. lo_profileの基本

共通周波数軸を作り、streamから参照する。

```toml
[frequency_axes.xffts_2GHz_32768ch]
definition_mode = "band_start_stop"
nchan = 32768
band_start_hz = 0.0
band_stop_hz = 2000000000.0
channel_origin = "center"
reverse = false
```

`channel_origin="center"` の式:

```text
delta_hz = (band_stop_hz - band_start_hz) / (nchan - 1)
```

LOはGHz/MHzで書ける。

```toml
[lo_chains.rx115_12co_usb_usb]
formula_version = "legacy_two_stage_sideband_v1"
lo1_ghz = 104.67120198
sb1 = "USB"
lo2_mhz = 9500
sb2 = "USB"
```

rest frequencyは分光計軸ではなくstreamまたはwindowに書く。

```toml
rest_frequency_ghz = 115.271
```

---

## 4. beam_model

`beam_model` を省略すると、内部的には `B00=(0,0)` のみを使う。`B01`以降を使う場合や `pointing_reference_beam_id` を使う場合は、必ず `beam_model.toml` を指定する。

```toml
[beams.B03]
rotation_mode = "pure_rotation_v1"
pure_rotation_offset_x_el0_arcsec = -286.1060256157315
pure_rotation_offset_y_el0_arcsec = 23.90550553606971
pure_rotation_sign = -1.0
```

`pointing_reference_beam_id` は `.obs [parameters]` に書く。

```toml
pointing_reference_beam_id = "B03"
pointing_reference_beam_policy = "exact"
```

本観測では `exact` を推奨する。`center_beam_id` はsunscan fit解析用概念で、観測制御には使わない。

---

## 5. recording_window

230/115 GHzを1本の速度windowで保存する例:

```toml
[recording_groups.co115]
mode = "spectrum"
saved_window_policy = "contiguous_envelope"
velocity_frame = "LSRK"
velocity_definition = "radio"
streams = ["2LU", "2RU"]
windows = [
  { window_id = "12CO_J1_0", rest_frequency_ghz = 115.271, vmin_kms = -150.0, vmax_kms = 150.0, margin_kms = 10.0 },
]
```

110 GHzで13CO/C18Oを別DB productとして保存する例:

```toml
[recording_groups.co110_multi]
mode = "spectrum"
saved_window_policy = "multi_window"
velocity_frame = "LSRK"
velocity_definition = "radio"
streams = ["2LL", "2RL"]
windows = [
  { window_id = "13CO_J1_0", rest_frequency_ghz = 110.201353, vmin_kms = -100.0, vmax_kms = 100.0, margin_kms = 10.0 },
  { window_id = "C18O_J1_0", rest_frequency_ghz = 109.7821734, vmin_kms = -100.0, vmax_kms = 100.0, margin_kms = 10.0 },
]
```

この場合、`2LL` から次が作られる。

```text
2LL__13CO_J1_0  / xffts-board2__13CO_J1_0
2LL__C18O_J1_0  / xffts-board2__C18O_J1_0
```

---

## 6. VLSRK

`velocity_frame="LSRK"` または `"VLSRK"` では、参照座標、site、時刻が必要。不足時はエラーであり、TOPOCENTRIC近似へ黙ってfallbackしない。

`.obs [parameters]` 例:

```toml
velocity_reference_l_deg = 0.0
velocity_reference_b_deg = 0.0
velocity_reference_site_lat_deg = -22.969956
velocity_reference_site_lon_deg = -67.703081
velocity_reference_site_elev_m = 4865.0
```

基準時刻は既定でsetup resolve時刻。明示する場合:

```toml
velocity_reference_time_utc = "2026-04-29T12:34:56Z"
```

太陽は次のように指定できる。

```toml
velocity_reference_target = "Sun"
```

任意の天体名をネットワークで名前解決しない。

---

## 7. snapshot生成と観測

事前snapshotを作る。

```bash
necst-spectral-resolve   --lo-profile lo_profile.toml   --recording-window-setup recording_window_setup.toml   --beam-model beam_model.toml   --setup-id orikl_NANTEN2_multi_12co_260331TO   --output spectral_recording_snapshot.toml
```

検証:

```bash
necst-spectral-validate spectral_recording_snapshot.toml
```

`.obs` でsnapshotを使う。

```toml
[parameters]
spectral_recording_snapshot = "spectral_recording_snapshot.toml"
setup_id = "orikl_NANTEN2_multi_12co_260331TO"
```

---

## 8. converter

DB内snapshotを自動検出する通常運用では、`--spectral-recording-snapshot` は不要。snapshotを使う場合、`--config-loader adapter` も不要。

```bash
python -m tools.necst.necst_v4_sdfits_converter \
  RAWDATA_DIR \
  --converter-analysis-config converter_analysis.toml
```

DB外のsnapshotを使う検証時だけ明示する。

```bash
python -m tools.necst.necst_v4_sdfits_converter \
  RAWDATA_DIR \
  --spectral-recording-snapshot path/to/spectral_recording_snapshot.toml \
  --converter-analysis-config converter_analysis.toml
```

multi-window productを選ぶ。

```bash
--recorded-stream-id 2LL__13CO_J1_0
```

または、

```bash
--stream-id 2LL --window-id 13CO_J1_0
```

`--stream-id 2LL` だけで複数productがある場合はエラーになる。

converterの `channel_slice` は保存済みlocal channelで指定する。観測時full channel番号ではない。

---

## 9. sunscan

extract例。DB内snapshotを自動検出する通常運用では、`--spectral-recording-snapshot` は不要。

```bash
python -m tools.necst.multibeam_beam_measurement.sunscan_multibeam extract \
  RAWDATA_DIR \
  --sunscan-analysis-config sunscan_analysis.toml \
  --analysis-stream-selection analysis_stream_selection.toml \
  --outdir sunscan_out
```

singlebeam例。

```bash
python -m tools.necst.multibeam_beam_measurement.sunscan_singlebeam \
  RAWDATA_DIR \
  --sunscan-analysis-config sunscan_analysis.toml \
  --analysis-stream-selection analysis_stream_selection.toml \
  --stream-name 1LU \
  --outdir sunscan_single
```

複数RawDataを一度に与える場合は、同じ観測設定のDB群をまとめる運用を基本にする。異なるsnapshotを混在させる場合は、DBごとに分けて実行するか、共通の明示 config/snapshot を指定する。

fitではstandalone `beam_model.toml` を出力する。旧all-in-one config更新は互換出力扱い。

---

## 10. 実機前チェック

```text
1. zip適用後、変更Pythonをpy_compileする。
2. colcon buildを実機環境で実行する。
3. necst-lo-profile summary/apply/verifyを実SGで確認する。
4. snapshot resolve/validateを通す。
5. LSRKを使う場合、Astropy、site、参照座標、時刻が揃っていることを確認する。
6. 短時間recordingでsidecar、gate、recorded product、DB table名を確認する。
7. converterで --recorded-stream-id または --stream-id + --window-id を確認する。
8. sunscan extract/fitで新beam_model.tomlを確認する。
```

---

## 11. 高速化と注意

update12では、高頻度callbackで毎回listを作らないようにし、raw input -> recorded productsをtuple cacheとして返す。static metadata chunkもsetup apply時に事前生成する。

ただし、multi-windowは1 raw inputから複数recorded productsへappendするため、DB table/file数とappend回数は増える。高cadence観測では `contiguous_envelope` の方が軽い場合がある。実機ではwriter queue、append latency、最初のrowでのtable作成時間を確認する。


---

## 12. update13確認結果

単一 `RAWDATA_DIR` の通常経路では、converter、sunscan extract、sunscan singlebeamがDB内 `spectral_recording_snapshot.toml` を自動検出する実装になっていることを確認した。

確認した異常系:

```text
sidecar無しのlegacy DB:
  converter helperは None を返す。

flat snapshot:
  RAWDATA_DIR/spectral_recording_snapshot.toml を検出。

nested snapshot:
  RAWDATA_DIR/metadata/config/spectral_recording_snapshot.toml を検出。

duplicate snapshot:
  エラー。

incomplete new DB:
  lo_profile.toml 等があるのに snapshot無しならエラー。
```


## 付記: legacy DB 判定の境界

`.obs` / `config.toml` だけでは incomplete new DB にならない。これらは旧DBにも存在し得るため、snapshot無しでエラーにする根拠にはしない。`lo_profile.toml`、`recording_window_setup.toml`、`beam_model.toml`、`pointing_param.toml` があるのに `spectral_recording_snapshot.toml` が無い場合だけ incomplete new DB として扱う。


---

## update13 priority 1-5 追補: snapshot自動検出の確認と複数RawData

### sidecar dry-run

変換やsunscanを実行する前に、DB内のsnapshot検出状態だけを確認できます。

```bash
python -m tools.necst.necst_v4_sdfits_converter RAWDATA_DIR --inspect-sidecars
```

```bash
python -m tools.necst.multibeam_beam_measurement.sunscan_multibeam inspect-sidecars RAWDATA_001 RAWDATA_002
```

```bash
python -m tools.necst.multibeam_beam_measurement.sunscan_singlebeam RAWDATA_DIR --inspect-sidecars
```

見るべき項目は `layout_kind`, `snapshot_path`, `duplicate_canonical_paths`, `recommended_action` です。

### sunscan multibeam extract

複数RawDataを渡し、かつ `--spectral-recording-snapshot` を明示しない場合、各RawDataの中で独立にsnapshotを自動検出します。

```bash
python -m tools.necst.multibeam_beam_measurement.sunscan_multibeam extract \
  RAWDATA_001 RAWDATA_002 \
  --sunscan-analysis-config sunscan_analysis.toml
```

明示snapshotを指定した場合は、その1つのsnapshotを全RawDataに使う明示運用になります。

### manifest

converterは `OUTPUT.fits.config_manifest.json` を出力します。sunscanはmanifest CSV/run table/config snapshot JSONに、実際に使った設定源を記録します。

主な項目:

```text
config_source_kind
config_source_path
config_source_auto_detected
config_source_explicit
config_loader
```

### duplicate snapshot

1つのDB内に複数の `spectral_recording_snapshot.toml` がある場合、自動選択せず停止します。`--inspect-sidecars` で候補pathを確認し、DB内に1つだけ残すか、意図したsnapshotを明示指定してください。



## update13 p16: 分光計取得ホットパス

p16では、観測中の分光計データ取りこぼしを避けるため、`fetch_data()`、`get_data()`、active spectrum chunk生成、NECSTDB writer drainを高速化しました。

実機確認で特に見るべきwarningは以下です。

```text
Dropping the data due to low readout frequency.
Too many data waiting for being dumped.
```

前者は分光計driver queue側、後者はDB writer queue側が詰まり気味であることを示します。詳細は `docs/update13_realtime_hotpath_performance_report_2026-04-30.md` を参照してください。

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

