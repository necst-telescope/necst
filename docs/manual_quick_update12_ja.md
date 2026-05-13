# NECST/XFFTS spectral recording update12 簡易マニュアル

Version: update12 / 日本語版 / 2026-04-29  
対象実装: update12 tree (`pr8g_update12_preflight_verified_with_docs.zip` または update11 + `u12p.zip`)

この簡易版は、実際に観測・受信機調整・converter/sunscanを走らせる人向けの手順書である。詳細な仕様、全パラメータ表、旧config相当の完全例は `manual_full_update12_ja.md` を参照する。

---

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

snapshotを使う場合、`--config-loader adapter` は不要。

```bash
python -m tools.necst.necst_v4_sdfits_converter   RAWDATA_DIR   --spectral-recording-snapshot RAWDATA_DIR/spectral_recording_snapshot.toml   --converter-analysis-config converter_analysis.toml
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

extract例:

```bash
python -m tools.necst.multibeam_beam_measurement.sunscan_multibeam extract   RAWDATA_DIR   --spectral-recording-snapshot spectral_recording_snapshot.toml   --sunscan-analysis-config sunscan_analysis.toml   --analysis-stream-selection analysis_stream_selection.toml   --outdir sunscan_out
```

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
