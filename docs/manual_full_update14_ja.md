# NECST/XFFTS spectral recording・split configuration・multi-window・sunscan 累積詳細取扱説明書 update14

Version: update14 / 日本語版 / 2026-05-03  
対象: original zip群に、update14本体、および2026-05-03時点までにこのチャット内で確認・修正した全patchを適用したtree。  
方針: update13までの詳細情報は削らず保持し、update14と実機preflightで判明した修正・注意点を先頭に統合する。

---

## 0. 2026-05-03時点の最重要更新

この章は、update13 manual本文の上に重ねて読む。古い説明と矛盾する場合は、この章を優先する。

### 0.1 実機preflightで通った範囲

`necst otf -f otf_lo_test.toml` により、少なくとも以下は実機で動作確認された。

- OTF wrapperの起動。
- privilege取得と解放。
- `spectral_recording_setup` を使った観測開始前setup処理。
- LO/window設定から `spectral_recording_snapshot.toml` を作成。
- OFFからONまでの観測シーケンス開始。
- XFFTS boardごとのwindow切り出し保存。
- board2, board4のmulti-window分割保存。
- `data/spectral/xffts/...` namespaceへのspectral data保存。
- `rx/quick_spectra` のboard別保存。

観測ログでは、保存サイズがおおむね `saved_nchan` に比例していた。これはXFFTS dataのwindow切り出しが動いていることを示す重要な確認である。

### 0.2 LO profile apply/verifyのrclpy初期化

`necst lo_profile apply ...` / `necst-lo-profile apply ...` で `Commander()` を作る前に `rclpy.init()` していなかった問題を修正した。修正後は、CLI自身がROS contextを開始した場合だけ終了時に `rclpy.shutdown()` する。

また、`Commander.get_privilege()` を使うapply/verify経路で、終了時に `quit_privilege()` を呼ぶようにした。実機運用では、LO apply失敗後にprivilegeが残ることを避けるために重要である。

### 0.3 `sg_set_frequency_ghz/mhz/hz`

`lo_profile.toml` の `[sg_devices.<id>]` では、以下のいずれか1つを使える。

```toml
sg_set_frequency_hz = 4000000000.0
```

```toml
sg_set_frequency_mhz = 4000.0
```

```toml
sg_set_frequency_ghz = 4.0
```

複数を同時に書くと曖昧なのでエラーにする。内部ではHzに正規化し、SG nodeへはGHz単位で渡す。

### 0.4 fixed LOのキー

`source = "fixed"` のLO roleでは、実装が読むキーは以下である。

```toml
fixed_lo_frequency_hz
fixed_lo_frequency_mhz
fixed_lo_frequency_ghz
```

したがって、200 GHz帯1st localを225 GHz固定として扱う場合は以下のように書く。

```toml
[lo_roles.band6_1st]
source = "fixed"
fixed_lo_frequency_ghz = 225.0
```

`frequency_ghz = 225.0` はfixed LOのキーとしては読まない。これはエラーになる。

### 0.5 `sg_devices.<id>` と `lo_roles.<role_id>` の関係

現実装では、`source = "sg_device"` の場合、`sg_id` を明示する。

```toml
[sg_devices.band3_1st]
sg_set_frequency_ghz = 17.416666666666668

[lo_roles.band3_1st]
source = "sg_device"
sg_id = "band3_1st"
multiplier = 6
expected_lo_frequency_ghz = 104.5
```

`sg_devices.<id>` と `lo_roles.<role_id>` が1対1で同名のときは冗長に見えるが、現時点では省略しない。物理SG名と論理LO名を分ける設計を維持しているためである。将来的には `sg_id` 省略時に `sg_id = lo_roles.<role_id>` とみなす拡張はあり得るが、この版には入れていない。

### 0.6 200 GHz帯1st localが固定の場合

旧converter設定を再現する例では、100 GHz帯1stは6逓倍、200 GHz帯1stは225 GHz fixed LO、2nd localは逓倍なしとして表現する。200 GHz帯がハードウェア内部で12逓倍されていても、観測制御でSGを触らないなら `source = "fixed"` を使う。

```toml
[lo_roles.band6_1st]
source = "fixed"
fixed_lo_frequency_ghz = 225.0
```

### 0.7 executable permission対策

zip更新時に `bin/necst-lo-profile` などの実行ビットが落ちる場合がある。これを避けるため、元々の `necst otf` と同じ形式で呼べるwrapperを追加した。

```bash
necst lo_profile summary lo_profile.toml
necst lo_profile apply lo_profile.toml --id band3_1st --verify --timeout-sec 10
necst spectral_resolve ...
necst spectral_validate ...
```

この形式では、個別の `bin/lo_profile.py` 等に実行権限は不要である。standalone wrapperも0755でzipに入れるが、環境によっては展開時に落ちるため、基本は `necst lo_profile ...` 形式を推奨する。

### 0.8 spectral setupとlegacy kwargsの競合判定

`OTF`, `RadioPointing`, `PSW`, `Grid` などfile-based観測では、CLI wrapperが `ch=None`, `tp_mode=False`, `tp_range=[]` を渡すことがある。これは「未指定」を意味し、legacy spectral制御を使う意図ではない。

修正後は以下を許可する。

```python
ch = None
tp_mode = False
tp_range = None
tp_range = []
```

一方で、以下はspectral recording setupとは併用不可としてエラーにする。

```python
ch = 1024
tp_mode = True
tp_range = [1000, 2000]
```

また、setup mode時には未指定相当のlegacy keysを消費し、旧式 `record("tp_mode", False)` 等を送らないようにした。

### 0.9 single-window contiguous envelopeのmetadata

`saved_window_policy = "contiguous_envelope"` でも、元windowが1本だけなら、top-level streamに以下を昇格して記録する。

```toml
line_name = "12CO J=2-1"
window_id = "12CO_J2_1"
rest_frequency_hz = 230538000000
```

元windowが複数ある場合に1つのline名へ潰すのは危険なので、その場合は `source_window_ids` に元window一覧を保持する。

### 0.10 multi-window streamのDB path短縮

従来、board2の13CO split streamは以下のようなpathになっていた。

```text
data/spectral/xffts/xffts-board2__13CO_J2_1
```

これは実ファイル名で `xffts-xffts-board2__...` と見え、冗長であった。修正後は実保存pathを短くする。

```text
data/spectral/xffts/board2__13CO_J2_1
```

ただし互換性のため、metadataには元のlogical名を保持する。

```toml
db_stream_name = "xffts-board2__13CO_J2_1"
recorded_db_stream_name = "xffts-board2__13CO_J2_1"
db_table_path = "data/spectral/xffts/board2__13CO_J2_1"
```

この変更は設定ファイルの変更を必要としない。`lo_profile.toml` や `recording_window_setup.toml` は同じでよい。

### 0.11 converter/sunscanのDB path追従

DB path短縮に合わせ、converter/sunscanもsnapshotの `db_table_path` / `db_table_name` を優先して読むように修正した。これにより、以下の新pathを読める。

```text
data/spectral/xffts/board2__13CO_J2_1
```

legacyの `xffts-board2` や、旧snapshotの `data/spectral/xffts/xffts-board2__...` も考慮する。

### 0.12 NECSTDB boolean warning

record nodeで以下のwarningが出る問題を修正した。

```text
Unsupported NECSTDB field type: 'boolean'
```

ROS 2 IDL由来のboolean fieldは `boolean` として来るが、writer側が `bool` しか扱っていなかったためである。修正後は以下を同じNECSTDB format `?` として扱う。

```python
"bool"
"boolean"
```

spectral array本体はfloat32 hot pathなので、このwarningがXFFTS spectrum本体を壊していた可能性は低い。ただしstatus/control topicのboolean field保存が落ちていた可能性があるため、修正は必要である。

### 0.13 build方針

msg/srvを変更していないpatchでは、`necst_msgs` の再buildは不要である。ただし、tracebackが `/root/ros2_ws/build/necst/...` を読んでいる場合、source treeだけを置き換えても反映されない。

最小で安全な反映は以下。

```bash
cd ~/ros2_ws
colcon build --packages-select necst --symlink-install
source install/setup.bash
```

`neclib` を変更した場合は、環境に応じて `neclib` の反映も必要である。`necstdb` や `sd-radio-spectral-fits` 側のconverter/sunscan変更は、それぞれ実行環境がどのsource/installを読んでいるかを確認する。

### 0.14 実観測前の最小preflight

```bash
necst lo_profile summary lo_profile.toml
necst lo_profile apply lo_profile.toml --id band3_1st --verify --timeout-sec 10
necst otf -f otf_lo_test.toml
```

観測後は、snapshotと実ファイルを確認する。

```bash
du -h *data
```

期待される兆候は、`saved_nchan` に比例したspectral dataサイズである。multi-window streamでは `board2__13CO...` と `board2__C18O...` が別々に保存される。

### 0.15 converter/sunscan確認

新snapshotでは、converter/sunscanはDB内 `spectral_recording_snapshot.toml` を自動検出し、`db_table_path` に従ってstreamを読む。

```bash
necst_v4_sdfits_converter <RawDataDir> --analysis-config converter_analysis_...toml --inspect-spectral-time
```

sunscanは頻繁に行わない想定なので、sunscan analysis TOMLはconverter analysis TOMLをbaseにして差分だけを書く。

```toml
[analysis_base]
path = "converter_analysis_12co_NANTEN2_multi_260331TO.toml"

[sunscan_analysis]
# sunscan固有項目のみ
```

---

## 1. 2026-05-03追加確認から得た注意点

### 1.1 仮座標とVLSRK

setup解決時のVLSRK windowは、snapshot作成時の座標・時刻・サイトで決まる。実機preflightで天体が沈んでいるため仮座標を入れた場合、その仮座標に対するVLSRK補正がsnapshotに入る。本観測では、必ず本当の天体座標でsnapshotを再生成する。仮座標snapshotを本観測に流用しない。

### 1.2 quick_spectra

`rx/quick_spectra` は元board単位で保存される。multi-window後のsplit stream名ではなく、`xffts_board1` などのboard名で出るのは自然である。

### 1.3 DB file名

新仕様では、multi-window streamの実ファイル名は以下のようになる。

```text
necst-OMU1P85M-data-spectral-xffts-board2__13CO_J2_1.data
```

旧仕様では `xffts-xffts-board2__...` のように見えた。新しいconverter/sunscan patchを必ず併用する。

---

## 2. 旧converter configからの4 board例

`docs/examples/legacy_converter_20260503/` に、旧converter config 6個をもとにした例を追加した。

- `lo_profile_legacy_converter_4board_12co_13co_c18o_20260503.toml`
- `recording_window_setup_legacy_converter_4board_12co_13co_c18o_20260503.toml`
- `README_legacy_converter_20260503.md`

この例では以下を再現する。

| line | LO式 |
|---|---|
| 12CO J=1-0 | `sky = 104.5 + 11.6 - IF = 116.1 - IF` |
| 13CO/C18O J=1-0 | `sky = 104.5 + 6.35 - IF = 110.85 - IF` |
| 12CO J=2-1 | `sky = 225.0 + 4.0 + IF = 229.0 + IF` |
| 13CO/C18O J=2-1 | `sky = 225.0 - 6.35 + IF = 218.65 + IF` |

200 GHz帯1stは `source = "fixed"` とし、`fixed_lo_frequency_ghz = 225.0` とする。

---

## 3. ここから下はupdate13までの詳細情報を保持した累積本文

以下の本文は、update13までの詳細説明を削らず保持し、上のupdate14/2026-05-03章と合わせて読むために収録している。古い呼び方・古いpath例が下位章に残っている場合、実運用では上位章の最新仕様を優先する。



# NECST/XFFTS spectral recording・split configuration・multi-window・sunscan 累積詳細取扱説明書 update14

Version: update14 / 日本語版 / 2026-05-02

対象実装: original zip群にupdate14 changed filesを上書きしたtree。update12/update13の詳細仕様を削らず保持し、update14の時刻仕様・example整理・sunscan analysis baseを統合する。

## A. このmanualの対象と読み方

このmanualは、original zip群に対してupdate14までに加えた変更を累積的に説明する。update14の時刻差分だけを抜き出したものではなく、update12/update13で導入したsplit configuration、multi-window、snapshot自動検出、converter/sunscan連携、XFFTS hot pathに、update14で追加した時刻整合、`header["received_time"]`、example整理、sunscan `[analysis_base]`、test修正を統合した文書である。

対象original zip群は次である。

```text
neclib-second_OTF_neclib-5(2).zip
necst-msgs-second_OTF_msg-5(2).zip
necst-second_OTF_branch-5(2).zip
necstdb-master-5(2).zip
sd-radio-spectral-fits-main-5(2).zip
xfftspy-master-5(2).zip
```

このmanualの「現在の仕様」はupdate14適用後を意味する。後半にはupdate13までの詳細manualを情報を削らず収録している。ただし、update14で明示的に変わった次の事項は、このA-L章を優先する。

- XFFTS timestamp / IRIG-B literal suffixの解釈
- DB spectral row `time`, XFFTS `time_spectrometer`, writer `recorded_time` の意味
- converter/sunscanの `[spectral_time]` / `[encoder_time]` の置き場所
- sunscan `[analysis_base]` によるconverter analysis TOMLの継承
- example TOMLの重複排除
- `test_converter_spectral_time_controls_p16g.py` の期待値修正

## B. update14までの大きな変更一覧

| 区分 | 内容 |
|---|---|
| split configuration | `lo_profile.toml`, `recording_window_setup.toml`, `beam_model.toml`, `analysis_stream_selection.toml` に分離 |
| `.obs`連携 | `[parameters]` でsplit config/snapshot/pointing reference/velocity referenceを指定 |
| SG/LO CLI | `necst-lo-profile`, `necst-spectral-resolve`, `necst-spectral-validate` を追加 |
| stream/product | source streamとrecorded productを分離し、multi-window保存に対応 |
| snapshot | 観測時のresolved setupをDB内 `spectral_recording_snapshot.toml` に保存 |
| update13 | converter/sunscanがRawData内snapshotを自動検出、sidecar discovery dry-run、manifest記録、duplicate検出を追加 |
| hot path | XFFTS payloadをndarrayで保持し、NECSTDB writerで`append_packed()`へ流す |
| update14 | XFFTS/IRIG-B時刻仕様、`header["received_time"]`、GPS suffix default UTC扱い、host-time fallback補正、encoder時刻補正を整理 |
| example整理 | converter analysis TOMLへ共通項目を集約し、sunscan analysis TOMLは`[analysis_base]`差分形式へ変更 |

## C. 用語・変数・時刻定義

| 変数 | 実装名 | 単位 | 意味 |
|---|---|---:|---|
| `t_xffts` | `time_spectrometer` / `timestamp` | s | XFFTS timestamp。update14では積分中心時刻として扱う |
| `t_recv` | `header["received_time"]`, spectral row `time` | s | XFFTS 64 byte headerをhostが受け取った直後の時刻 |
| `t_recorded` | `recorded_time` | s | writerがDBへappendした時刻 |
| `t_spec` | converter/sunscanのselected spectral time | s | 後処理でencoder補間に使うspectral time |
| `t_enc` | encoder table `time` | s | antenna encoderの時刻 |

`recorded_time`は取得時刻ではない。座標付けに使うべきではない。`recorded_time - time`はwriter遅延やqueue滞留を見る診断量である。

## D. update14 XFFTS/IRIG-B時刻仕様

通常のOMU/XFFTSでは、XFFTS timestampのliteral suffixが`GPS`でも本文はUTC相当として扱う。したがってdefaultは次である。

```toml
[spectral_time]
spectral_time_source = "auto"
xffts_timestamp_scale = "auto"
xffts_gps_suffix_means = "utc"
# host-time fallback時だけ適用
spectrometer_time_offset_sec = 0.0
```

`xffts_gps_suffix_means = "gps"` を明示した場合だけ、literal `GPS` を真のGPS時刻としてUTCへ変換する。その場合は現在のGPS-UTC差の分だけ、UTC解釈と約18秒ずれる。これは通常defaultではない。

`PC` suffixはautoでは採用しない。`spectral_time_source = "auto"` ではhost-time fallbackへ回す。`spectral_time_source = "xffts-timestamp"` を明示し、timestampが`PC` suffixしかない場合は失敗させる。

`spectrometer_time_offset_sec` はhost-time fallback時だけ適用する。XFFTS timestamp採用時には適用しない。`encoder_shift_sec`, `encoder_az_time_offset_sec`, `encoder_el_time_offset_sec` はspectral time選択とは独立したencoder時刻補正である。

## E. XFFTS受信からDB書き込みまで

```text
XFFTS TCP packet
  ↓
xfftspy data_consumer
  - 64 byte headerを読む
  - header受信直後に header["received_time"] = time.time()
  - spectral payloadは np.frombuffer(..., dtype="<f4")
  ↓
neclib XFFTS device
  - received_timeをspectral row timeとして渡す
  - time_spectrometer/timestampも別フィールドで保持
  ↓
necst spectrometer runtime
  - chunk内に time と time_spectrometer を保持
  ↓
NECSTDBWriter
  - timeは取得側時刻
  - recorded_timeはwriter書込時刻
  - ndarrayはraw bytes / append_packed 経路
```

主な実装箇所は以下である。

```text
xfftspy-master/xfftspy/data_consumer.py
neclib-second_OTF_neclib/neclib/devices/spectrometer/xffts.py
necst-second_OTF_branch/necst/rx/spectrometer.py
necst-second_OTF_branch/necst/rx/spectral_recording_runtime.py
neclib-second_OTF_neclib/neclib/recorders/necstdb_writer.py
necstdb-master/necstdb/core.py
```

実機に近い疑似シミュレーションでは、8 boards、32768 channels、0.1 s cadence、1 packet約1.049 MB、必要入力率約10.49 MB/s、必要writer rate約80 rows/sを想定した。ndarray pathは十分速い一方、tuple fallbackは0.1 s cadenceに近く危険である。通常経路で`spectral_data`がnumpy ndarrayのまま流れていることを確認するのが重要である。

| 経路 | 測定値 | 判断 |
|---|---:|---|
| XFFTS parse ndarray path | median 5.936 ms/packet, 176.7 MB/s | 余裕あり |
| XFFTS parse tuple fallback | median 91.062 ms/packet, 11.5 MB/s | 危険寄り |
| NECSTDB direct write ndarray+append_packed | 202.1 rows/s | 必要80 rows/sを上回る |
| NECSTDB queue write ndarray+background drain | 740.2 rows/s, 97.1 MB/s | 十分な余裕 |

## F. converter/sunscan解析TOMLの最新方針

converter analysis TOMLをbase/masterとして扱う。ここに、converter固有設定だけでなく、そのRawData/setup familyに共通するDB/source/table/chopper/weather/timing設定を置く。sunscan analysis TOMLは`[analysis_base]`でconverter analysis TOMLを参照し、sunscan固有項目だけを書く。

### F.1 converter analysis TOML

```toml
schema_version = "converter_analysis_v1"
analysis_id = "12co_NANTEN2_multi_260331TO_converter"

# Common timing policy for this RawData/setup family.
# Converter uses this file directly.  Sunscan may inherit these common values via
# [analysis_base] in sunscan_analysis_12co_NANTEN2_multi_260331TO.toml.
#
# For modern XFFTS data, XFFTS timestamps are treated as integration-center
# times.  In the OMU/XFFTS IRIG-B configuration, the literal suffix may be GPS,
# but the timestamp body is UTC unless xffts_gps_suffix_means = "gps" is
# explicitly selected.
[spectral_time]
spectral_time_source = "auto"
xffts_timestamp_scale = "auto"
xffts_gps_suffix_means = "utc"
# Applied only when the analysis falls back to host-time spectral rows.
spectrometer_time_offset_sec = -0.145

[encoder_time]
encoder_shift_sec = 0.0
encoder_az_time_offset_sec = 0.0
encoder_el_time_offset_sec = 0.0

[converter_analysis]
db_namespace = "necst"
telescope = "NANTEN2"
planet = "Ori-KL"
spectral_name = "xffts-board1"
boresight_source = "encoder"
boresight_correction_apply = "add"
skycoord_method = "azel"
output_azel_source = "beam"
encoder_time_col = "time"
altaz_time_col = "time"
encoder_table_suffix = "ctrl-antenna-encoder"
altaz_table_suffix = "ctrl-antenna-altaz"
weather_inside_table_suffix = "weather-ambient-in"
weather_outside_table_suffix = "weather-ambient-out"
weather_inside_time_col = "time"
weather_outside_time_col = "time"
met_source = "auto"
thot_default_k = 273.15
thot_min_k = 250.0
thot_max_k = 330.0
tamb_default_k = 270.0
tamb_min_k = 230.0
tamb_max_k = 330.0
outside_default_temperature_c = 0.0
outside_default_pressure_hpa = 520.0
outside_default_humidity_pct = 30.0
outside_temperature_min_c = -50.0
outside_temperature_max_c = 50.0
outside_pressure_min_hpa = 400.0
outside_pressure_max_hpa = 1100.0
outside_humidity_min_pct = 0.0
outside_humidity_max_pct = 100.0
chopper_wheel = true
chopper_win_sec = 5.0
chopper_stat = "median"
output_layout = "merged"
time_sort = true

```

### F.2 sunscan analysis TOML

```toml
schema_version = "sunscan_analysis_v1"
analysis_id = "12co_NANTEN2_multi_260331TO_sunscan"

# Reuse the usual converter-analysis file as the master/base for common settings:
# db/telescope/source names, encoder/weather table names, chopper-wheel settings,
# and [spectral_time]/[encoder_time].  The sunscan section below therefore
# contains only sunscan-specific parameters.
[analysis_base]
path = "converter_analysis_12co_NANTEN2_multi_260331TO.toml"

[sunscan_analysis]
profile_xlim_deg = 1.0
ripple_preset = "auto"
ripple_model = "auto"
ripple_target_hz = 1.2
ripple_search_hz = 0.3
edge_fit_win_deg = 0.15
edge_fit_threshold = 0.2
hpbw_init_arcsec = 324.0
edge_fit_plot_max_scans = 3
trim_vfrac = 0.2
trim_vmin = 0.0001
trim_gap = 10
trim_min_samples = 100
trim_axis_ratio_min = 3.0
trim_vpercentile = 95.0
trim_scan_speed_min_arcsec = 20.0
trim_xwin_factor = 1.2
trim_cross_offset_max_deg = 0.5
trim_steady_cv_max = 0.8
dish_diameter_m = 4.0
hpbw_factor = 0.5

```

sunscanはbaseから共通スカラー設定を継承する。ただし、converter固有のchannel/export sliceはsunscanへ継承しない。sunscan TOML側で`[spectral_time]`または`[encoder_time]`を再定義した場合、既定ではエラーにする。例外的に必要なときだけ次のように明示する。

```toml
[analysis_base]
path = "converter_analysis_12co_NANTEN2_multi_260331TO.toml"
allow_timing_override = true
```

## G. 採用しない・避ける書き方

次は採用しない。

```toml
[converter_analysis]
spectrometer_time_offset_sec = -0.145
encoder_shift_sec = 0.0

[sunscan_analysis]
spectrometer_time_offset_sec = -0.145
encoder_shift_sec = 0.0
```

時刻設定はトップレベルの`[spectral_time]`/`[encoder_time]`に置く。sunscanでは原則としてbaseから継承する。また、`GPS` suffixを無条件に真GPSとして読む運用、`recorded_time`を取得時刻として使う運用、snapshotがあるのに古いsidecarを手で指定する運用も避ける。

## H. snapshot自動検出とanalysis baseの違い

| 仕組み | 対象 | 主な情報 |
|---|---|---|
| `spectral_recording_snapshot.toml` | 観測時setup | stream mapping, frequency axis, recorded products, beam model, recording windows |
| converter analysis TOML | 後処理の共通解析環境 | DB namespace, table名, chopper/weather, timing policy, converter設定 |
| sunscan analysis TOML | sunscan差分 | fit/trimming/plot/report, analysis base参照 |

snapshot自動検出は観測時setupを復元する仕組みであり、analysis baseは後処理パラメータの重複を避ける仕組みである。

## I. example READMEとobs fragment

### I.1 README_mapping.md

```markdown
# 12co-NANTEN2-multi_260331TO legacy-config equivalent split setup

Generated: 2026-04-29Tupdate11-docs

This directory is an example split-configuration equivalent of:

`12co-NANTEN2-multi_260331TO.conf3(3).txt`

## Files

- `lo_profile_12co_NANTEN2_multi_260331TO.toml`
  - stream truth, shared frequency axis, shared LO chains.
  - `rest_frequency_ghz` is stream-level, not frequency-axis-level.
  - `lo1_ghz` and `lo2_mhz` are used to avoid large Hz literals.
- `beam_model_12co_NANTEN2_multi_260331TO.toml`
  - standalone beam geometry for B01--B05 plus a zero B00 fallback.
- `recording_window_12co_NANTEN2_multi_260331TO.toml`
  - 230/115 GHz: VLSRK -150..+150 km/s with 10 km/s margin.
  - 110 GHz: `multi_window`, separate 13CO and C18O recorded products, each -100..+100 km/s with 10 km/s margin.
- `analysis_stream_selection_12co_NANTEN2_multi_260331TO.toml`
  - legacy `enabled`, `use_for_convert`, `use_for_sunscan`, `use_for_fit`, `beam_fit_use`.
- `converter_analysis_12co_NANTEN2_multi_260331TO.toml`
  - converter timing, DB, encoder, meteorology, chopper-related analysis parameters from `[global]`.
- `sunscan_analysis_12co_NANTEN2_multi_260331TO.toml`
  - sunscan chopper/ripple/edge/trim parameters from `[global]`.
- `example_obs_parameters_fragment_12co_NANTEN2_multi_260331TO.obs`
  - snippet to paste into the `.obs` `[parameters]` section.

## Stream grouping

The source streams are grouped by common line/LO settings:

- `nanten2_xffts_12co115`: 8 streams, default `rest_frequency_ghz = 115.271`, LO2 = 9500 MHz.
- `nanten2_xffts_110ghz`: 7 streams, default `rest_frequency_ghz = 110.201353`, LO2 = 4000 MHz.
- `nanten2_xffts_12co230`: 1 stream, default `rest_frequency_ghz = 230.538`, LO1 = 225.635994 GHz, LO2 = 4500 MHz.

All groups share:

```toml
frequency_axis_id = "xffts_2GHz_32768ch"
```

## Frequency-axis convention

The legacy configuration used:

```toml
definition_mode = "band_start_stop"
nchan = 32768
band_start_hz = 0.0
band_stop_hz = 2000000000.0
channel_origin = "center"
reverse = false
```

Because `channel_origin = "center"`, the frequency step is:

```text
delta_hz = (band_stop_hz - band_start_hz) / (nchan - 1)
         = 2000000000.0 / 32767
         = 61037.01895199438 Hz
```

This is **not** `2000000000.0 / 32768`.

## Multi-window naming

> 2026-05-03 metadata/path cleanup:
> `recorded_db_stream_name` は互換性のため `xffts-board2__13CO_J2_1` のような旧aliasを保持する。
> 一方、実際の `db_table_path` は `data/spectral/xffts/board2__13CO_J2_1` のように、
> spectrometer namespace配下では `boardN__window_id` を使う。これによりNECSTDBの実ファイル名が
> `...-data-spectral-xffts-xffts-board2__...data` のように `xffts` を二重に含むことを避ける。
> single-windowの `contiguous_envelope` では、`computed_windows` 内だけでなくtop-levelにも
> `line_name`, `window_id`, `rest_frequency_hz` を昇格して保持する。


For 110 GHz streams, the recording setup creates separate recorded products:

```text
2LL__13CO_J1_0  -> xffts-board2__13CO_J1_0
2LL__C18O_J1_0  -> xffts-board2__C18O_J1_0
...
```

Use converter selection as:

```bash
--stream-id 2LL --window-id 13CO_J1_0
```

or directly:

```bash
--recorded-stream-id 2LL__13CO_J1_0
```

## Counts

- source streams: 16
- 230 GHz streams: 1
- 115 GHz streams: 8
- 110 GHz source streams: 7
- 110 GHz recorded products: 14
- expected total recorded products: 23

## update10 note: VLSRK reference direction

The recording window file uses `velocity_frame = "LSRK"`.  In update10 this is fail-closed:
the resolver must receive a velocity-reference direction, site, and reference time.  During
normal NECST observation flow these are supplied from `[parameters]` and/or the parsed
`.obs` reference coordinate.  For standalone validation, either provide explicit
`velocity_reference_l_deg` / `velocity_reference_b_deg` / site parameters, or change
`velocity_frame` to `"TOPOCENTRIC"` only when an explicitly topocentric approximate
window is intended.

## Converter analysis as the timing/base file for sunscan

The normal operational file is `converter_analysis_12co_NANTEN2_multi_260331TO.toml`.
It contains the dataset-wide common analysis environment, including `[spectral_time]`
and `[encoder_time]`.  These timing sections describe how spectral row times and
encoder times are interpreted for this RawData/setup family, not a sunscan-only
choice.

`sunscan_analysis_12co_NANTEN2_multi_260331TO.toml` therefore does not repeat
those common settings.  Instead it contains:

```toml
[analysis_base]
path = "converter_analysis_12co_NANTEN2_multi_260331TO.toml"

[sunscan_analysis]
# sunscan-specific fitting/trimming/report parameters only
```

When this sunscan file is read, the converter-analysis file is used as a
base/master for common scalar settings.  Converter-only stream export slices are
not inherited by sunscan.  If a sunscan file inherits an `analysis_base`, it
should not define `[spectral_time]`, `[encoder_time]`, or timing keys inside
`[sunscan_analysis]`; doing so is treated as a configuration conflict unless
`[analysis_base].allow_timing_override = true` is explicitly set.


```

### I.2 example obs parameters fragment

```toml
# Add these keys to the [parameters] section of the .obs file.
# Paths are normally resolved relative to the .obs file directory.
[parameters]
cos_correction = true
lo_profile = "lo_profile_12co_NANTEN2_multi_260331TO.toml"
recording_window_setup = "recording_window_12co_NANTEN2_multi_260331TO.toml"
beam_model = "beam_model_12co_NANTEN2_multi_260331TO.toml"
setup_id = "orikl_NANTEN2_multi_260331TO_update11_windows"
# velocity_reference_time_utc = "2026-04-29T12:34:56Z"
# pointing_reference_beam_id = "B01"
# pointing_reference_beam_policy = "exact"

# VLSRK recording windows require a deterministic reference direction and site.
# If your .obs parser provides the [coordinate] reference as obsspec._reference,
# these explicit fields may be omitted.  Otherwise set them explicitly.
# Example for Galactic Center:
# velocity_reference_l_deg = 0.0
# velocity_reference_b_deg = 0.0
# velocity_reference_site_lat_deg = -22.969956
# velocity_reference_site_lon_deg = -67.703081
# velocity_reference_site_elev_m = 4865.0
# velocity_reference_time_utc = "2026-04-29T12:34:56Z"


```

## J. 検証済み項目

- example TOML parse
- active timing keyの重複なし
- sunscan `[analysis_base]` からconverter analysis TOMLを読むこと
- timing overrideは既定で拒否、`allow_timing_override = true`でのみ許可
- `GPS` suffix default UTC扱い
- true GPS明示時の18秒差
- `PC` suffix auto fallback
- `spectrometer_time_offset_sec` がhost-time fallback時のみ適用されること
- selected pytest: `14 passed`

実機XFFTS、実NECSTDB長時間書き込み、ROS2 `colcon build`、実RawData full runは未確認であり、完了扱いしない。

## K. 運用チェックリスト

1. `.obs [parameters]` でsplit configまたはsnapshotを指定する。
2. VLSRK windowを使う場合はreference direction, site, timeを一意にする。
3. 観測後、DB内に`spectral_recording_snapshot.toml`が1つだけあることを確認する。
4. converterはconverter analysis TOMLを指定する。
5. sunscanはsunscan analysis TOMLを指定し、共通項目は`[analysis_base]`から継承する。
6. 時刻に疑義がある場合は`--inspect-spectral-time`でselected source, suffix, fallback理由を見る。
7. writer遅延に疑義がある場合は`recorded_time - time`と`spectral_data`の型を確認する。

## L. original zipからの変更ファイル概要

| カテゴリ | ファイル数 |
|---|---:|
| ROS msg/srv | 4 |
| converter/sunscan/tests | 20 |
| docs/examples/reports | 35 |
| neclib spectrometer/writer | 2 |
| necst runtime/CLI | 23 |
| necstdb storage | 2 |
| xfftspy receive path | 2 |

### L.1 changed files manifest一覧

- `docs/examples/old_config_260331TO_update11/README_mapping.md` (added, 4531 bytes)
- `docs/examples/old_config_260331TO_update11/analysis_stream_selection_12co_NANTEN2_multi_260331TO.toml` (added, 2004 bytes)
- `docs/examples/old_config_260331TO_update11/beam_model_12co_NANTEN2_multi_260331TO.toml` (added, 1571 bytes)
- `docs/examples/old_config_260331TO_update11/converter_analysis_12co_NANTEN2_multi_260331TO.toml` (added, 1983 bytes)
- `docs/examples/old_config_260331TO_update11/example_obs_parameters_fragment_12co_NANTEN2_multi_260331TO.obs` (added, 1077 bytes)
- `docs/examples/old_config_260331TO_update11/lo_profile_12co_NANTEN2_multi_260331TO.toml` (added, 4777 bytes)
- `docs/examples/old_config_260331TO_update11/recording_window_12co_NANTEN2_multi_260331TO.toml` (added, 1520 bytes)
- `docs/examples/old_config_260331TO_update11/recording_window_topo.toml` (added, 1527 bytes)
- `docs/examples/old_config_260331TO_update11/sunscan_analysis_12co_NANTEN2_multi_260331TO.toml` (added, 966 bytes)
- `docs/examples/old_config_260331TO_update11/validation_summary.json` (added, 2471 bytes)
- `docs/manual_full_update12_ja.md` (added, 86259 bytes)
- `docs/manual_full_update13_ja.md` (added, 110542 bytes)
- `docs/manual_full_update14_ja.md` (added, 37596 bytes)
- `docs/manual_quick_update12_ja.md` (added, 8106 bytes)
- `docs/manual_quick_update13_ja.md` (added, 16534 bytes)
- `docs/manual_quick_update14_ja.md` (added, 10031 bytes)
- `docs/manual_update12_report.md` (added, 798 bytes)
- `docs/manual_update13_report.md` (added, 8106 bytes)
- `docs/manual_update14_report.md` (added, 1658 bytes)
- `docs/preflight/pr8g_update12_preflight_realtime_review_report_2026-04-29.md` (added, 4103 bytes)
- `docs/preflight/update13_snapshot_autodiscovery_report.md` (added, 6161 bytes)
- `docs/preflight/update13_snapshot_autodiscovery_review_2026-04-30.md` (added, 3675 bytes)
- `docs/preflight/update14_final_validation_2026-05-02.json` (added, 1930 bytes)
- `docs/preflight/update14_final_validation_2026-05-02.md` (added, 1543 bytes)
- `docs/preflight/update14_realistic_time_and_writer_simulation_2026-05-01.json` (added, 1874 bytes)
- `docs/preflight/update14_realistic_time_and_writer_simulation_2026-05-01.md` (added, 3521 bytes)
- `docs/preflight/update14_realistic_time_and_writer_simulation_2026-05-01.py` (added, 12142 bytes)
- `docs/u13_report.md` (added, 6161 bytes)
- `docs/update13_deep_review_report_2026-04-30.md` (added, 8770 bytes)
- `docs/update13_p16g_spectral_time_diagnostics_report_2026-04-30.md` (added, 5094 bytes)
- `docs/update13_p17ag_integrated_time_and_numpy_report_2026-04-30.md` (added, 4261 bytes)
- `docs/update13_priority1_5_implementation_report_2026-04-30.md` (added, 3653 bytes)
- `docs/update13_realtime_hotpath_p16_deep_verification_2026-04-30.md` (added, 6079 bytes)
- `docs/update13_realtime_hotpath_performance_report_2026-04-30.md` (added, 10873 bytes)
- `docs/update14_time_consistency_report_2026-05-01.md` (added, 7057 bytes)
- `neclib-second_OTF_neclib/neclib/devices/spectrometer/xffts.py` (modified, 5643 bytes)
- `neclib-second_OTF_neclib/neclib/recorders/necstdb_writer.py` (modified, 13465 bytes)
- `necst-msgs-second_OTF_msg/CMakeLists.txt` (modified, 1989 bytes)
- `necst-msgs-second_OTF_msg/srv/ApplySpectralRecordingSetup.srv` (added, 281 bytes)
- `necst-msgs-second_OTF_msg/srv/ClearSpectralRecordingSetup.srv` (added, 97 bytes)
- `necst-msgs-second_OTF_msg/srv/SetSpectralRecordingGate.srv` (added, 101 bytes)
- `necst-second_OTF_branch/bin/necst-lo-profile` (added, 118 bytes)
- `necst-second_OTF_branch/bin/necst-spectral-resolve` (added, 115 bytes)
- `necst-second_OTF_branch/bin/necst-spectral-validate` (added, 117 bytes)
- `necst-second_OTF_branch/necst/core/commander.py` (modified, 58047 bytes)
- `necst-second_OTF_branch/necst/definitions.py` (modified, 12286 bytes)
- `necst-second_OTF_branch/necst/procedures/observations/file_based.py` (modified, 23526 bytes)
- `necst-second_OTF_branch/necst/procedures/observations/observation_base.py` (modified, 12892 bytes)
- `necst-second_OTF_branch/necst/procedures/observations/pointing_reference_beam.py` (added, 27873 bytes)
- `necst-second_OTF_branch/necst/procedures/observations/spectral_recording_sequence.py` (added, 19519 bytes)
- `necst-second_OTF_branch/necst/rx/spectral_recording_runtime.py` (added, 28359 bytes)
- `necst-second_OTF_branch/necst/rx/spectral_recording_setup.py` (added, 99134 bytes)
- `necst-second_OTF_branch/necst/rx/spectral_recording_sg.py` (added, 14963 bytes)
- `necst-second_OTF_branch/necst/rx/spectrometer.py` (modified, 26346 bytes)
- `necst-second_OTF_branch/setup.py` (modified, 1778 bytes)
- `necst-second_OTF_branch/tests/test_pointing_reference_beam.py` (added, 6563 bytes)
- `necst-second_OTF_branch/tests/test_spectral_recording_hotpath_performance.py` (added, 11214 bytes)
- `necst-second_OTF_branch/tests/test_spectral_recording_runtime_pr4_pr5.py` (added, 10776 bytes)
- `necst-second_OTF_branch/tests/test_spectral_recording_runtime_pr6.py` (added, 9674 bytes)
- `necst-second_OTF_branch/tests/test_spectral_recording_sequence_pr7.py` (added, 7610 bytes)
- `necst-second_OTF_branch/tests/test_spectral_recording_setup.py` (added, 14549 bytes)
- `necst-second_OTF_branch/tests/test_spectral_recording_sg_pr3.py` (added, 6274 bytes)
- `necst-second_OTF_branch/tests/test_spectral_recording_stream_truth_frequency.py` (added, 22224 bytes)
- `necst-second_OTF_branch/tests/test_spectral_recording_writer_pr8a.py` (added, 7832 bytes)
- `necstdb-master/necstdb/necstdb.py` (modified, 18394 bytes)
- `necstdb-master/tests/test_append_packed.py` (added, 1036 bytes)
- `sd-radio-spectral-fits-main/src/tools/necst/analysis_stream_selection.py` (added, 8068 bytes)
- `sd-radio-spectral-fits-main/src/tools/necst/beam_model.py` (added, 19770 bytes)
- `sd-radio-spectral-fits-main/src/tools/necst/multibeam_beam_measurement/config_io.py` (modified, 37690 bytes)
- `sd-radio-spectral-fits-main/src/tools/necst/multibeam_beam_measurement/config_separation_model.py` (added, 70748 bytes)
- `sd-radio-spectral-fits-main/src/tools/necst/multibeam_beam_measurement/public_api.py` (modified, 11295 bytes)
- `sd-radio-spectral-fits-main/src/tools/necst/multibeam_beam_measurement/sunscan_config.py` (modified, 10851 bytes)
- `sd-radio-spectral-fits-main/src/tools/necst/multibeam_beam_measurement/sunscan_extract_multibeam.py` (modified, 79288 bytes)
- `sd-radio-spectral-fits-main/src/tools/necst/multibeam_beam_measurement/sunscan_fit_multibeam.py` (modified, 15565 bytes)
- `sd-radio-spectral-fits-main/src/tools/necst/multibeam_beam_measurement/sunscan_io.py` (modified, 3705 bytes)
- `sd-radio-spectral-fits-main/src/tools/necst/multibeam_beam_measurement/sunscan_legacy_compat.py` (modified, 187055 bytes)
- `sd-radio-spectral-fits-main/src/tools/necst/multibeam_beam_measurement/sunscan_multibeam.py` (modified, 9417 bytes)
- `sd-radio-spectral-fits-main/src/tools/necst/multibeam_beam_measurement/sunscan_singlebeam.py` (modified, 48141 bytes)
- `sd-radio-spectral-fits-main/src/tools/necst/multibeam_beam_measurement/synthetic_multibeam.py` (modified, 10247 bytes)
- `sd-radio-spectral-fits-main/src/tools/necst/necst_v4_sdfits_converter.py` (modified, 233307 bytes)
- `sd-radio-spectral-fits-main/src/tools/necst/spectral_recording_snapshot.py` (added, 38297 bytes)
- `sd-radio-spectral-fits-main/tests/test_converter_spectral_time_controls_p16g.py` (added, 12731 bytes)
- `sd-radio-spectral-fits-main/tests/test_necst_config_separation_snapshot_bundle.py` (added, 6486 bytes)
- `sd-radio-spectral-fits-main/tests/test_necst_snapshot_stream_truth_beam_override.py` (added, 2031 bytes)
- `sd-radio-spectral-fits-main/tests/test_necst_update13_priority1_5.py` (added, 7256 bytes)
- `sd-radio-spectral-fits-main/tests/test_sunscan_analysis_base_config.py` (added, 3966 bytes)
- `xfftspy-master/tests/test_data_consumer_numpy.py` (added, 1331 bytes)
- `xfftspy-master/xfftspy/data_consumer.py` (modified, 2942 bytes)

---

## M. update13までの累積仕様詳細（情報を削らず収録）

以下はupdate13までの詳細manualを、情報を削らず収録した部分である。update14で明示的に更新された時刻設定とsunscan analysis TOMLについては、上のA-L章を優先する。

## NECST/XFFTS spectral recording・split configuration・multi-window・sunscan 詳細取扱説明書 update13版

Version: update13 / 日本語版 / 2026-04-30  
対象実装: `pr8g_update13.zip` を直接確認し、DB内 `spectral_recording_snapshot.toml` 自動検出経路を反映した update13 tree  
対象者: 観測者、受信機・LO担当者、コミッショニング担当者、SDFITS converter利用者、sunscan/multibeam解析利用者、実装保守担当者

この文書は、NECST/XFFTS spectral recording の現行仕様を説明する詳細版である。旧all-in-one `spectrometer_config` で混在していた観測時truth、保存window、beam geometry、converter解析条件、sunscan解析条件を分離して扱う。過去の経緯ではなく、現在の仕様・運用・パラメータ・例を中心に記述する。

---

### 0. update13での位置づけと読み方

この詳細版は、`manual_full_update12_ja.md` の情報を削らずに、update13 の実装確認結果を追記した版である。update12までの split configuration、multi-window、VLSRK、`pointing_reference_beam_id`、高速化、converter/sunscan adapter 経路は維持し、その上に DB 内 `spectral_recording_snapshot.toml` の自動検出を追加して扱う。

update13で追加・確認した主な点は次である。

```text
1. converter の default 経路
   --spectral-recording-snapshot と --spectrometer-config の両方が未指定の場合、
   RAWDATA_DIR 配下から一意な spectral_recording_snapshot.toml を探索する。

2. sunscan extract / singlebeam の default 経路
   明示 snapshot/config が未指定の場合、RawData 内 snapshot を探索する。

3. 優先順位
   明示 --spectral-recording-snapshot > 明示 --spectrometer-config >
   RawData 内 snapshot 自動検出 > legacy fallback または legacy explicit-config 要求。

4. 異常系
   複数 snapshot はエラー。
   lo_profile.toml / recording_window_setup.toml / beam_model.toml などの
   new spectral setup sidecar があるのに snapshot が無いDBは incomplete new DB としてエラー。ただし `.obs` や一般の `config.toml` / `*_config.toml` だけでは new spectral setup sidecar とは判定しない。
   snapshot が無い legacy DB では、converter は従来の single-stream fallback を維持し、
   sunscan は従来どおり明示 config を要求する。

5. 注意
   sunscan の複数 RawData 入力では、同じ観測設定のDB群をまとめる運用を前提にする。
   異なる snapshot を持つDBを一度に混ぜる場合は、DBごとに分けて実行するか、
   共通の明示 config/snapshot を指定して意図を明確にする。
```

本書の読み方は次の通りである。


実装上の重要な境界条件として、`.obs` ファイルや一般の `config.toml` / `*_config.toml` は、RawData内に存在していても、それだけでは「新しいspectral recording setup sidecarがある」とは判定しない。これらは旧RawDataにも存在し得るためである。`spectral_recording_snapshot.toml` が無い状態で incomplete new DB として扱うのは、`lo_profile.toml`、`recording_window_setup.toml`、`beam_model.toml`、`pointing_param.toml` のように、spectral recording の新setupを示すsidecarが存在する場合である。

```text
通常converter運用:
  10章、特に10.1と10.5を読む。

sunscan運用:
  11章、特に11.1, 11.2, 11.5を読む。

観測時snapshot生成:
  9章、12章、13章を読む。

実装・保守:
  14章以降と25章、26章を含めて読む。
```


### 0A. update12までの位置づけと読み方

この詳細版は、添付された `spectral_recording_update11_full_ja` を正本として、情報を削らずに update12 の実装・運用へ合わせたものである。章構成は大きくは維持し、obsoleteな版名や中間版名は update12 へ置換した。旧all-in-one `spectrometer_config` の情報を落とさず、現在の split configuration、multi-window、VLSRK、`pointing_reference_beam_id`、converter/sunscan、SG設定CLI、実機前確認を一体として扱う。

update12で実装上追加された主な点は次である。

```text
1. 高頻度callback経路の高速化
   streams_for_raw() が毎回listを生成しないようにし、
   raw input -> recorded products をtuple cacheとして返す。

2. update11高速化の副作用確認
   raw input -> recorded product対応、multi-window順序、slice結果、
   static metadata chunk、unknown raw board挙動を確認済み。

3. 実機前preflight位置づけ
   Python構文・py_compile・旧conf相当変換・multi-window runtime・converter selection・
   velocity window algebraを確認済み。
   ただし、ROS2 colcon build、service生成、実XFFTS入力、実NECSTDB recording、
   Astropyあり環境での実LSRK補正値、実RawData converter/sunscanは実機環境で要確認。
```

本書の読み方は次の通りである。

```text
受信機・SG調整:
  5A章、12章、13章を読む。

観測者:
  3章、4章、8章、12章、13章を読む。

converter/sunscan解析:
  10章、11章、13章を読む。

実装・保守:
  1章、2章、5章、6章、7章、8章、9章、14章以降を含めて読む。
```


---

### 1. 設計の全体像

#### 1.1 分離するもの

旧configでは、1つのファイルに以下が混在していた。

```text
stream identity
DB raw stream名
fdnum / ifnum / plnum / polariza
分光計周波数軸
LO設定
beam geometry
保存範囲
converterのDB/時刻/weather設定
sunscanのchopper/ripple/trim/edge-fit設定
解析対象stream選択
```

新仕様では、次のように分離する。

| 分類 | ファイル | 主な所有者 | 役割 |
|---|---|---|---|
| stream truth | `lo_profile.toml` | 受信機・backend担当者 | streamごとの分光計、LO、backend/sampler、beam_id、rest frequency |
| 保存window | `recording_window_setup.toml` | 観測者・コミッショニング担当者 | full/channel/velocity/multi-window保存範囲 |
| beam geometry | `beam_model.toml` | beam較正・sunscan担当者 | B00/B01/... のoffsetとrotation |
| 解決済みtruth | `spectral_recording_snapshot.toml` | 自動生成 | 観測時に実際に使った設定 |
| 解析stream選択 | `analysis_stream_selection.toml` | 解析者 | converter/sunscan/fitの対象選択 |
| converter解析条件 | `converter_analysis.toml` | 解析者 | DB名、時刻補正、weather、出力slice |
| sunscan解析条件 | `sunscan_analysis.toml` | 解析者 | chopper、ripple、trim、edge-fit、HPBW |

#### 1.2 最重要原則

1. `lo_profile.toml` は観測時truthである。
2. `converter_analysis.toml` と `sunscan_analysis.toml` は解析条件であり、観測時truthを再定義しない。
3. `rest_frequency_*` は分光計の仕様ではなく、streamまたはwindowのline設定である。
4. `db_stream_name` はraw入力stream名であり、multi-window保存後のDB名は `recorded_db_stream_name` である。
5. snapshotを使うconverter/sunscanでは、`--config-loader adapter` は不要である。
6. `pointing_reference_beam_id` は観測制御に効くbeam IDであり、`center_beam_id` とは別概念である。

---

### 2. 用語

| 用語 | 意味 |
|---|---|
| source stream | XFFTS等から来る元stream。例: `stream_id="2LL"`, `db_stream_name="xffts-board2"` |
| recorded product | source streamから切り出して保存される成果物。multi-windowでは1 source streamから複数生成される |
| stream_id | 人間・解析用の論理stream名 |
| db_stream_name | raw DB stream名。multi-windowでもraw入力名として固定 |
| recorded_stream_id | 保存後productの論理名。例: `2LL__13CO_J1_0` |
| recorded_db_stream_name | 保存後productのDB名。例: `xffts-board2__13CO_J1_0` |
| window_id | 保存windowの識別子。DB pathに入るため安全な文字だけ使う |
| board_id | XFFTS等backend内の物理・論理board番号 |
| frequency_axis_id | 分光計のIF周波数軸定義への参照 |
| lo_chain | LO chain定義への参照 |
| beam_id | beam geometry定義への参照 |
| pointing_reference_beam_id | そのbeamがtargetへ向くようboresightを補正する観測制御パラメータ |
| rest_frequency_hz/ghz/mhz | velocity変換やRESTFRQに使うline rest frequency |

---

### 3. `.obs` での使い方

#### 3.1 書く場所

spectral recording関連のkeyは、`.obs` の `[parameters]` に書く。

```toml
[parameters]
cos_correction = true

lo_profile = "lo_profile_12co_NANTEN2_multi_260331TO.toml"
recording_window_setup = "recording_window_12co_NANTEN2_multi_260331TO.toml"
beam_model = "beam_model_12co_NANTEN2_multi_260331TO.toml"
setup_id = "orikl_NANTEN2_multi_260331TO"
```

相対パスは `.obs` のあるディレクトリを基準に解決される。

#### 3.2 何も書かなければlegacy

`lo_profile`、`spectral_recording_snapshot`、`recording_window_setup` などを書かなければ、legacy観測として動く。新setupはapplyされず、snapshot sidecarも保存されない。

#### 3.3 事前snapshotを使う場合

```toml
[parameters]
spectral_recording_snapshot = "spectral_recording_snapshot.toml"
setup_id = "orikl_snapshot"
```

この場合、stream truthはsnapshotから来る。外部 `beam_model` を解析・pointing用に渡す場合は、pointing reference beamに関わるbeamの一致検査に注意する。

#### 3.4 `.obs [parameters]` の全体表

| key | 置き場所 | 型 | 意味 | 既定・注意 |
|---|---|---:|---|---|
| `lo_profile` / `lo_profile_path` | `.obs [parameters]` | path | `lo_profile.toml` への参照。stream truth の本体 | 相対パスは `.obs` 基準 |
| `recording_window_setup` / `recording_window_setup_path` | `.obs [parameters]` | path | `recording_window_setup.toml` への参照 | 省略時は全stream full-spectrum |
| `beam_model` / `beam_model_path` | `.obs [parameters]` | path | `beam_model.toml` への参照 | 省略時は `B00=(0,0)` のみ |
| `spectral_recording_snapshot` / `spectral_recording_snapshot_path` | `.obs [parameters]` | path | 事前生成snapshotを使う | `lo_profile` 方式と排他的に扱う |
| `setup_id` / `spectral_recording_setup_id` | `.obs [parameters]` | string | 人間が読むsetup名 | DB/log/provenance用 |
| `setup_override_policy` / `spectral_recording_setup_override_policy` | `.obs [parameters]` | enum | conflict policy | `strict`, `warn`, `force`, `legacy` |
| `spectral_recording` | `.obs [parameters]` | bool | 明示的に新setupを有効化 | 通常は `lo_profile` 指定で十分 |
| `use_spectral_recording_setup` | `.obs [parameters]` | bool | 同上の互換名 |  |
| `pointing_reference_beam_id` | `.obs [parameters]` | string | 指定beamをtargetへ向ける | `center_beam_id` は使わない |
| `pointing_reference_beam_policy` | `.obs [parameters]` | enum | pointing reference補正方式 | 本観測推奨 `exact` |
| `velocity_reference_ra_deg`, `velocity_reference_dec_deg` | `.obs [parameters]` | deg | VLSRK計算用ICRS座標 | LSRK/VLSRK windowで必要 |
| `velocity_reference_l_deg`, `velocity_reference_b_deg` | `.obs [parameters]` | deg | VLSRK計算用Galactic座標 | ICRSへ変換される |
| `velocity_reference_lon_deg`, `velocity_reference_lat_deg`, `velocity_reference_frame` | `.obs [parameters]` | deg/string | 任意frameの参照座標 | AstropyでICRSへ変換 |
| `velocity_reference_target` | `.obs [parameters]` | string | 太陽系天体名 | `Sun`, `Moon`, planets。任意名のネットワーク解決はしない |
| `velocity_reference_site_lat_deg`, `velocity_reference_site_lon_deg`, `velocity_reference_site_elev_m` | `.obs [parameters]` | deg/deg/m | VLSRK計算用site | 省略時はNECST config locationを試す |
| `site_lat_deg`, `site_lon_deg`, `site_elev_m` | `.obs [parameters]` | deg/deg/m | site指定の短縮名 | `velocity_reference_site_*` が優先 |
| `velocity_reference_time_utc` | `.obs [parameters]` | ISO time | VLSRK基準時刻 | 省略時は `setup_resolve_time_utc` |


---

### 4. 観測時シーケンス

新setupを使う場合、概念的には次の順序で進む。

```text
record開始前:
  .obs [parameters] を読む
  lo_profile / recording_window_setup / beam_model をresolve
  spectral_recording_snapshot.toml を作る
  SpectralDataへsetupをapply
  setup gateはまだ閉じたまま

record開始後:
  sidecarとして設定ファイルとsnapshotをDBに保存
  setup gateを開く
  spectral/TP row保存開始

record停止前:
  setup gateを閉じる

record停止後:
  recorder stop完了を待つ
  active setupをclear
```

この順序により、sidecar保存前に本体data rowが保存されることを避ける。

#### 4.1 cleanup時の安全側動作

終了時に失敗があっても、原則として以下を試みる。

```text
gate close
recorder stop
active setup clear
antenna stop / privilege release
```

active setupが残っている可能性がある場合、legacy TP/binning resetを不用意に送らない。

---

### 5. `lo_profile.toml`

#### 5.1 役割

`lo_profile.toml` は、観測時truthの中心ファイルである。ここには以下を書く。

```text
SG device設定
LO roles
LO chains
frequency axes
stream groups
streamごとのbackend/sampler/LO/frequency_axis/rest frequency/beam_id
```

#### 5.2 全パラメータ表

| key | 場所 | 型 | 意味 |
|---|---|---:|---|
| `schema_version` | root | string | 例: `lo_profile_v2` |
| `profile_id` | root | string | profileの識別名 |
| `sg_devices.<sg_id>.control_adapter` | `[sg_devices.*]` | string | SG制御adapter |
| `sg_devices.<sg_id>.sg_set_frequency_hz/ghz/mhz` | `[sg_devices.*]` | float | SGへ設定する周波数。Hzへ正規化 |
| `sg_devices.<sg_id>.sg_set_power_dbm` | `[sg_devices.*]` | float | SG出力power |
| `sg_devices.<sg_id>.frequency_tolerance_hz` | `[sg_devices.*]` | float | readback許容差 |
| `sg_devices.<sg_id>.power_tolerance_db` | `[sg_devices.*]` | float | power readback許容差 |
| `sg_devices.<sg_id>.output_required` | `[sg_devices.*]` | bool | SG output状態を要求するか |
| `sg_devices.<sg_id>.output_policy` | `[sg_devices.*]` | string | output ON/OFF方針 |
| `lo_roles.<role>.source` | `[lo_roles.*]` | enum | `fixed` または `sg_device` |
| `lo_roles.<role>.sg_id` | `[lo_roles.*]` | string | `source="sg_device"` のSG ID |
| `lo_roles.<role>.multiplier` | `[lo_roles.*]` | float | SG周波数からphysical LOへの倍率 |
| `lo_roles.<role>.fixed_lo_frequency_hz/ghz/mhz` | `[lo_roles.*]` | float | fixed LO周波数 |
| `lo_roles.<role>.expected_lo_frequency_hz/ghz/mhz` | `[lo_roles.*]` | float | 検査用expected physical LO |
| `lo_chains.<id>.formula_version` | `[lo_chains.*]` | string | `legacy_two_stage_sideband_v1`, `signed_lo_sum_plus_if_v1` など |
| `lo_chains.<id>.lo1_hz/ghz/mhz`, `lo2_hz/ghz/mhz` | `[lo_chains.*]` | float | 旧config互換の2段LO |
| `lo_chains.<id>.sb1`, `sb2` | `[lo_chains.*]` | enum | `USB` / `LSB` |
| `lo_chains.<id>.lo_roles` | `[lo_chains.*]` | array | role型LO chainのrole列 |
| `lo_chains.<id>.sideband_signs` | `[lo_chains.*]` | array | roleごとの符号 |
| `frequency_axes.<id>.definition_mode` | `[frequency_axes.*]` | enum | `band_start_stop`, `first_center_and_delta`, `explicit_wcs` |
| `frequency_axes.<id>.nchan` / `full_nchan` | `[frequency_axes.*]` | int | full channel数 |
| `frequency_axes.<id>.band_start_hz`, `band_stop_hz` | `[frequency_axes.*]` | float | band_start_stop用 |
| `frequency_axes.<id>.channel_origin` | `[frequency_axes.*]` | enum | `center` / `edge` |
| `frequency_axes.<id>.reverse` | `[frequency_axes.*]` | bool | trueなら最終周波数軸を反転 |
| `frequency_axes.<id>.if_freq_at_full_ch0_hz` | `[frequency_axes.*]` | float | full channel 0中心IF周波数 |
| `frequency_axes.<id>.if_freq_step_hz` | `[frequency_axes.*]` | float | channel間隔 |
| `frequency_axes.<id>.channel_order` | `[frequency_axes.*]` | enum | `increasing_if`, `decreasing_if` |
| `frequency_axes.<id>.ctype1`, `cunit1`, `specsys`, `veldef` | `[frequency_axes.*]` | string | FITS/WCS metadata |
| `frequency_axes.<id>.store_freq_column` | `[frequency_axes.*]` | bool | 周波数列保存方針 |
| `stream_groups.<group>` | `[stream_groups.*]` | table | stream default群 |
| `stream_groups.<group>.streams` | `[[...streams]]` | array | stream item配列 |
| `stream_id` | stream item | string | 論理stream名 |
| `db_stream_name` | stream item | string | raw DB stream名 |
| `board_id` | stream item | int | XFFTS等のboard番号 |
| `fdnum`, `ifnum`, `plnum`, `polariza` | stream item | int/string | SDFITS識別 |
| `spectrometer_key` | group/stream | string | raw入力key |
| `frontend`, `backend`, `sampler` | group/stream | string | 受信機・backend・sampler |
| `frequency_axis_id` | group/stream | string | 参照するfrequency axis |
| `lo_chain` | group/stream | string | 参照するLO chain |
| `beam_id` | group/stream | string | 参照するbeam |
| `rest_frequency_hz/ghz/mhz` | group/stream | float | 輝線rest frequency。stream側がcanonical |
| `restfreq_hz/ghz/mhz` | group/stream | float | `rest_frequency_*` の互換alias |
| `default_rest_frequency_hz/ghz/mhz` | group/stream | float | 互換alias。出力ではcanonicalへ正規化 |
| `raw_input_key`, `raw_board_id` | stream item | string/int | runtime raw callback binding。通常は自動 |


#### 5.3 frequency axis

##### 5.3.1 `definition_mode = "band_start_stop"`

旧config互換で最も重要な形式である。

```toml
[frequency_axes.xffts_2GHz_32768ch]
definition_mode = "band_start_stop"
nchan = 32768
band_start_hz = 0.0
band_stop_hz = 2000000000.0
channel_origin = "center"
reverse = false
ctype1 = "FREQ"
cunit1 = "Hz"
specsys = "TOPOCENT"
veldef = "RADIO"
store_freq_column = false
```

`channel_origin="center"` の場合:

```text
N = nchan
c = 0, 1, ..., N-1

delta_hz = (band_stop_hz - band_start_hz) / (N - 1)
if_freq_hz(c) = band_start_hz + c * delta_hz
```

この場合、`band_start_hz` と `band_stop_hz` はchannel中心周波数である。`abs(band_stop_hz-band_start_hz)` はfirst-to-last center spanであり、edge-to-edge帯域幅ではない。

`channel_origin="edge"` の場合:

```text
N = nchan
c = 0, 1, ..., N-1

delta_hz = (band_stop_hz - band_start_hz) / N
if_freq_hz(c) = band_start_hz + (c + 0.5) * delta_hz
```

この場合、`band_start_hz` と `band_stop_hz` はband edgeであり、`abs(band_stop_hz-band_start_hz)` がedge-to-edge bandwidthである。

##### 5.3.2 `definition_mode = "first_center_and_delta"`

```toml
[frequency_axes.axis1]
definition_mode = "first_center_and_delta"
full_nchan = 32768
if_freq_at_full_ch0_hz = 0.0
if_freq_step_hz = 61037.01895199438
channel_order = "increasing_if"
```

式は次。

```text
if_freq_hz(c) = if_freq_at_full_ch0_hz + c * if_freq_step_hz
```

##### 5.3.3 `definition_mode = "explicit_wcs"`

snapshotや保存済みlocal channel用の明示WCSである。

```toml
definition_mode = "explicit_wcs"
nchan = 8000
crval1_hz = 112000000000.0
cdelt1_hz = 61037.0
crpix1 = 1.0
```

#### 5.4 rest frequency

`rest_frequency_*` はfrequency axisではなく、streamまたはrecording windowに置く。

```toml
rest_frequency_ghz = 115.271
```

以下はaliasとして読める。

```text
rest_frequency_hz / rest_frequency_ghz / rest_frequency_mhz
restfreq_hz / restfreq_ghz / restfreq_mhz
default_rest_frequency_hz / default_rest_frequency_ghz / default_rest_frequency_mhz
```

snapshot内ではHzへ正規化される。

#### 5.5 LO chain

旧config互換の2段LO例:

```toml
[lo_chains.rx115_12co_usb_usb]
formula_version = "legacy_two_stage_sideband_v1"
lo1_ghz = 104.67120198
sb1 = "USB"
lo2_mhz = 9500.0
sb2 = "USB"
```

role型LO chain例:

```toml
[sg_devices.sg_lsb_2nd]
control_adapter = "necst_commander_signal_generator_set_v1"
sg_set_frequency_ghz = 4.0
sg_set_power_dbm = 15.0
frequency_tolerance_hz = 10.0
power_tolerance_db = 0.5
output_required = true
output_policy = "require_off"

[lo_roles.lo2]
source = "sg_device"
sg_id = "sg_lsb_2nd"
multiplier = 3
expected_lo_frequency_ghz = 12.0

[lo_chains.band6_lsb]
formula_version = "signed_lo_sum_plus_if_v1"
lo_roles = ["lo2"]
sideband_signs = [-1]
```

Hz/GHz/MHz aliasはHzへ正規化される。同じ階層で矛盾した値を二重指定した場合はエラーにする。`necst-lo-profile apply` でも同じalias解決を使い、例えば `sg_set_frequency_ghz = 4.0` は内部で `4.0e9` Hzに正規化された後、`Commander.signal_generator(..., GHz=4.0)` としてSG nodeへ渡される。

#### 5.6 stream group

現行実装では `stream_groups` が必要である。groupは省略記法であり、stream itemが第一級の最終truthへ展開される。

```toml
[stream_groups.nanten2_xffts_12co115]
spectrometer_key = "xffts"
backend = "XFFTS"
sampler = "XFFTS1"
frequency_axis_id = "xffts_2GHz_32768ch"
lo_chain = "rx115_12co_usb_usb"
rest_frequency_ghz = 115.271

  [[stream_groups.nanten2_xffts_12co115.streams]]
  stream_id = "2LU"
  db_stream_name = "xffts-board1"
  board_id = 1
  fdnum = 2
  ifnum = 0
  plnum = 1
  polariza = "LL"
  frontend = "RX115"
  beam_id = "B02"
```

groupに書いたkeyはdefaultであり、stream itemに同じkeyを書けばstream側が上書きする。例えば115 GHz groupと110 GHz groupを分け、rest frequencyやLO chainをgroup単位で共通化できる。

---


### 5A. 受信機設定時の SG/LO CLI

#### 5A.1 `config.toml` と `lo_profile.toml` の分担

SGのIP address、port、機種、device idはNECSTの `config.toml` に置く。

```toml
[signal_generator.sg_lsb_2nd]
_ = "FSW0010"
host = "192.168.100.53"
port = 10001
```

`lo_profile.toml` には、そのdevice idのSGを観測ごとにどの周波数・何dBmで使うかを書く。周波数は `sg_set_frequency_hz` / `sg_set_frequency_mhz` / `sg_set_frequency_ghz` のいずれか1つで指定する。内部標準はHzだが、観測者がGHzで管理した方が自然なSG設定では `sg_set_frequency_ghz` を使ってよい。

```toml
[sg_devices.sg_lsb_2nd]
control_adapter = "necst_commander_signal_generator_set_v1"
sg_set_frequency_ghz = 4.0
sg_set_power_dbm = 15.0
frequency_tolerance_hz = 10.0
power_tolerance_db = 0.5
output_required = true
output_policy = "require_off"
```

重要な規則:

```text
lo_profile.toml の [sg_devices.<sg_id>]
  == NECST config.toml の [signal_generator.<sg_id>]
```

`lo_profile.toml` 内で別名 `control_id` を用いてconfig側device idへ変換する運用は、現行標準仕様では使わない。

#### 5A.2 SG設定一覧の確認

```bash
necst-lo-profile summary lo_profile.toml
```

確認する項目:

```text
sg_id
SGへ直接設定する周波数
power_dBm
output_required
output_policy
frequency_tolerance_hz
power_tolerance_db
```

`summary` の `sg_id` が実際の `config.toml` に存在する `signal_generator` device idと一致していることを、受信機設定時に確認する。

#### 5A.2.1 実行権限に依存しないsource tree運用

update14では、SG/LOとsnapshot生成のために以下のCLIを追加している。

```text
necst-lo-profile
necst-spectral-resolve
necst-spectral-validate
```

これらは `setup.py` の `console_scripts` に登録されているため、`colcon build` 後に `install/setup.bash` をsourceして使う通常のROS2/ament運用では、install tree側の実行可能scriptとして起動できる。

一方で、実機では `/root/ros2_ws/src/necst/bin` のようなsource treeの `bin` がPATHに入っていることがある。この場合、zip更新やpatch適用の方法によっては、新規追加した `bin/necst-lo-profile` などの実行ビットが落ち、更新のたびに `chmod +x` が必要になることがある。

この問題を避けるため、元からある `bin/necst` wrapperと同じ方式で使える `.py` aliasも用意する。

```text
bin/lo_profile.py
bin/spectral_resolve.py
bin/spectral_validate.py
```

`bin/necst` は `bin/*.py` を `python3` で起動する設計なので、これらの `.py` alias自体には実行ビットは不要である。source tree運用では、次の形式を使えば、個別CLI wrapperの実行権限に依存しない。

```bash
necst lo_profile summary lo_profile.toml
necst lo_profile apply lo_profile.toml --id sg_lsb_2nd --verify --timeout-sec 10
necst spectral_resolve \
  --lo-profile lo_profile.toml \
  --recording-window-setup recording_window_setup.toml \
  --beam-model beam_model.toml \
  --setup-id orikl_NANTEN2_multi_12co_260331TO \
  --output spectral_recording_snapshot.toml
necst spectral_validate spectral_recording_snapshot.toml
```

standalone wrapperも残している。Linuxの `unzip` など権限を保持する展開方法では、以下もそのまま使える。

```bash
necst-lo-profile summary lo_profile.toml
necst-spectral-resolve --help
necst-spectral-validate --help
```

ただし、source treeの `bin` がinstall treeよりPATHの前にある場合は、どちらの実体が実行されているかを確認する。

```bash
which necst
which necst-lo-profile
ls -l $(which necst-lo-profile)
```

実行権限の問題で止まる場合は、恒久的には今回の `.py` alias経由、すなわち `necst lo_profile ...` 形式を使うのが安全である。

#### 5A.3 SG apply

特定SGだけ設定する。

```bash
necst-lo-profile apply lo_profile.toml --id sg_lsb_2nd
```

全SGを設定する。

```bash
necst-lo-profile apply lo_profile.toml
```

受信機立ち上げ時は、1台ずつapplyして、実際のSG表示やreadbackと照合する方が安全である。

#### 5A.4 SG apply + verify

設定後にreadback検証する。

```bash
necst-lo-profile apply lo_profile.toml --id sg_lsb_2nd --verify --timeout-sec 10
```

既に設定済みのSGを検証する。

```bash
necst-lo-profile verify lo_profile.toml --id sg_lsb_2nd --timeout-sec 10
```

通常のコミッショニングでは、可能な限り `--allow-command-echo` なしで通す。実device readbackが得られず、command echoしか返らない環境では、明示的に以下を使う。

```bash
necst-lo-profile verify lo_profile.toml --id sg_lsb_2nd --allow-command-echo
```

#### 5A.5 stale readbackを成功扱いしない

verifyは、古いreadbackを成功扱いしない。

```text
verify開始時刻を記録
期限 = now + timeout_sec
loop:
  Commander.signal_generator("?", id=sg_id) を読む
  readback idが一致しなければ拒否
  verify開始より古いreadbackならstaleとして拒否
  frequency / power toleranceを満たせば成功
  timeoutまでfreshな一致readbackがなければ失敗
```

これは、直前にbufferに残っていた古いreadbackで成功したように見えることを避けるためである。

#### 5A.6 SG frequency と physical LO frequency

`sg_set_frequency_*` はSGへ直接設定する周波数である。`*` は `hz` / `mhz` / `ghz` のいずれかで、TOML内では1つだけ指定する。converterやWCSが必要とするLO周波数は、逓倍やsidebandを反映したphysical LOである。

例:

```toml
[sg_devices.sg_lsb_2nd]
sg_set_frequency_ghz = 4.0

[lo_roles.lo2]
source = "sg_device"
sg_id = "sg_lsb_2nd"
multiplier = 3
expected_lo_frequency_ghz = 12.0
```

この例では、SG設定値は4 GHz、physical LOは12 GHzである。ここを混同すると、周波数軸が大きくずれる。

#### 5A.7 受信機調整時の標準手順

```bash
## 1. SG設定一覧
necst-lo-profile summary lo_profile.toml

## 2. SGを1台ずつ設定・検証
necst-lo-profile apply lo_profile.toml --id sg_lsb_2nd --verify --timeout-sec 10
necst-lo-profile apply lo_profile.toml --id sg_usb_2nd --verify --timeout-sec 10

## 3. snapshot resolve
necst-spectral-resolve \
  --lo-profile lo_profile.toml \
  --recording-window-setup recording_window_setup.toml \
  --beam-model beam_model.toml \
  --setup-id receiver_checkout \
  --output spectral_recording_snapshot.toml

## 4. snapshot validate
necst-spectral-validate spectral_recording_snapshot.toml
```

確認ポイント:

```text
SG device idがconfig.tomlと一致しているか
SG設定周波数とphysical LOを混同していないか
lo_chainがstreamごとに正しいか
frequency_axis_idがstreamごとに正しいか
board_idとdb_stream_nameが実機対応と一致するか
beam_idがbeam_model.tomlに存在するか
recording_windowがfull_nchan範囲内か
```


### 6. `beam_model.toml`

#### 6.1 役割

`beam_model.toml` はbeam geometryのファイルである。観測時にstreamの `beam_id` が実在するかを検査し、後段converter/sunscanでも同じbeam geometryを使えるようにする。

#### 6.2 全パラメータ表

| key | 場所 | 型 | 意味 |
|---|---|---:|---|
| `schema_version` | root | string | 例: `beam_model_v2` |
| `beam_model_id` | root | string | beam model識別名 |
| `beams.<beam_id>.beam_model_version` | beam | string | beam model version |
| `beams.<beam_id>.rotation_mode` | beam | enum | `fixed`, `none`, `pure_rotation_v1` など |
| `az_offset_arcsec`, `el_offset_arcsec` | beam | float | fixed offset。`x=ΔAz cos El`, `y=ΔEl` |
| `dewar_angle_deg` | beam | float | dewar角 |
| `pure_rotation_offset_x_el0_arcsec` | beam | float | El=0でのx offset |
| `pure_rotation_offset_y_el0_arcsec` | beam | float | El=0でのy offset |
| `pure_rotation_sign` | beam | float | El依存回転の符号 |


#### 6.3 B00の扱い

`beam_model.toml` を省略した場合、内部的には `B00=(0,0)` だけが定義される。streamに `beam_id` が無い場合は `B00` へ解決される。streamが `B01` 以降を参照する場合、`beam_model.toml` は必須である。

#### 6.4 fixed beam

```toml
[beams.B00]
beam_model_version = "default_zero_boresight"
rotation_mode = "fixed"
az_offset_arcsec = 0.0
el_offset_arcsec = 0.0
dewar_angle_deg = 0.0
```

#### 6.5 pure_rotation_v1

```toml
[beams.B03]
beam_model_version = "sunscan_multibeam_pure_rotation_v1"
rotation_mode = "pure_rotation_v1"
pure_rotation_offset_x_el0_arcsec = -286.1060256157315
pure_rotation_offset_y_el0_arcsec = 23.90550553606971
pure_rotation_sign = -1.0
dewar_angle_deg = 0.0
```

接平面規約は次である。

```text
x = ΔAz cos(El) [arcsec]
y = ΔEl         [arcsec]
```

---

### 7. `pointing_reference_beam_id`

#### 7.1 意味

`pointing_reference_beam_id` は、中心boresightではなく、任意のbeamが観測者の指定targetへ向くようにboresightをずらす観測制御パラメータである。

```toml
[parameters]
beam_model = "beam_model_multibeam.toml"
pointing_reference_beam_id = "B03"
pointing_reference_beam_policy = "exact"
```

`center_beam_id` はsunscan fit等の解析時概念と衝突するため、観測制御には使わない。

#### 7.2 exact policy

指定beamのoffsetを `x(C_el), y(C_el)` とする。converter/sunscan側は通常、

```text
beam_az = C_az + x(C_el) / cos(C_el) / 3600
beam_el = C_el + y(C_el) / 3600
```

でbeam位置を再構成する。したがって、観測時に指定beamをtargetへ向けるには、

```text
T_el = C_el + y(C_el) / 3600
T_az = C_az + x(C_el) / cos(C_el) / 3600
```

を満たす `C_az, C_el` を使う。`exact` policyではこの条件を満たすように解く。

#### 7.3 snapshot beamと外部beam_modelの一致規則

`pointing_reference_beam_id` が指定されている場合、観測時boresight補正に効くのはそのbeamだけである。したがって、snapshot内beamsと外部 `beam_model.toml` を比較する場合、全beam一致ではなく、少なくとも `pointing_reference_beam_id` のbeam定義が一致している必要がある。

```text
pointing_reference_beam_id = B03:
  snapshot.beams.B03 と external beam_model.beams.B03 は一致必須
  B04など他beamの更新は許容。ただしprovenanceに残す
```

`pointing_reference_beam_id` が未指定またはB00で、B00が `(0,0)` として一致しているなら、後処理用に他beamをsunscan結果で更新してよい。

---

### 8. `recording_window_setup.toml`

#### 8.1 役割

観測時にDBへ保存するchannel範囲を定義する。channel指定はfull channelであり、converterの `--channel-slice` はsnapshot使用時には保存済みlocal channelである。

#### 8.2 全パラメータ表

| key | 場所 | 型 | 意味 |
|---|---|---:|---|
| `schema_version` | root | string | 例: `recording_window_setup_v2` |
| `setup_id` | root | string | window setup識別名 |
| `[defaults].mode` | defaults/group | enum | `spectrum` / `tp` |
| `[defaults].saved_window_policy` | defaults/group | enum | `full`, `channel`, `contiguous_envelope`, `multi_window` |
| `[defaults].velocity_frame` | defaults/group/window | enum | `LSRK`, `VLSRK`, `TOPOCENTRIC` |
| `[defaults].velocity_definition` | defaults/group/window | enum | `radio` |
| `[defaults].reference_time_policy` | defaults/group | string | 現在は `setup_resolve_time` が実用 |
| `recording_groups.<id>.streams` | group | array | 対象source stream |
| `recording_groups.<id>.saved_ch_start`, `saved_ch_stop` | group/window | int | full channel範囲。start inclusive, stop exclusive |
| `recording_groups.<id>.windows` | group | array | 複数window定義 |
| `recording_groups.<id>.velocity_windows` | group | array | `windows` の互換alias。同時指定不可 |
| `windows[].window_id` | window | string | 保存window識別子。DB pathに使うため安全文字のみ |
| `windows[].line_name` | window | string | 人間向けline名 |
| `windows[].rest_frequency_hz/ghz/mhz` | window | float | window固有rest frequency |
| `windows[].vmin_kms`, `vmax_kms` | window | float | 速度範囲 |
| `windows[].margin_kms` | window | float | 両側margin |
| `windows[].recorded_stream_id` | window | string | 省略時は `<source_stream_id>__<window_id>` |
| `windows[].recorded_db_stream_name` | window | string | 省略時は `<source_db_stream_name>__<window_id>` |
| `tp_stat` | group | string | TP保存時の統計 |
| `tp_windows` | group | array | TP channel window |


#### 8.3 `saved_window_policy`

##### `full`

full spectrumを保存する。

```toml
[recording_groups.full_all]
mode = "spectrum"
saved_window_policy = "full"
streams = ["2LU", "2RU"]
```

##### `channel`

full channel indexで保存範囲を直接指定する。

```toml
[recording_groups.slice]
mode = "spectrum"
saved_window_policy = "channel"
streams = ["2LU"]
saved_ch_start = 10000
saved_ch_stop = 18000
```

`start` はinclusive、`stop` はexclusiveである。

##### `contiguous_envelope`

1つまたは複数windowを含む最小連続範囲を1本で保存する。複数lineを1本の保存vectorで残したい場合に使う。

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

##### `multi_window`

1つのsource streamから複数recorded productを作り、DB table/fileを分ける。

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

この場合、`2LL`から以下が作られる。

```text
source stream:
  2LL / xffts-board2

recorded products:
  2LL__13CO_J1_0 / xffts-board2__13CO_J1_0
  2LL__C18O_J1_0 / xffts-board2__C18O_J1_0
```

#### 8.4 velocity frame

`velocity_frame = "LSRK"` または `"VLSRK"` の場合、VLSRK補正を計算する。参照座標・site・時刻が無い場合はエラーであり、topocentric近似には戻さない。

`velocity_frame = "TOPOCENTRIC"` の場合だけ、rest周波数まわりの近似windowとして扱う。

#### 8.5 VLSRK基準時刻と座標

基準時刻は既定で `setup_resolve_time_utc`、つまりrecord開始前にsetupをresolveした時刻である。最初の分光データ時刻ではない。保存channel数とDB schemaをrecord開始前に決める必要があるためである。

参照座標の優先順位:

```text
1. velocity_reference_ra_deg / velocity_reference_dec_deg
2. velocity_reference_l_deg / velocity_reference_b_deg
3. velocity_reference_lon_deg / velocity_reference_lat_deg / velocity_reference_frame
4. .obs parserが提供するreference座標
5. Sun / Moon / planets
```

任意のtarget名をネットワークで名前解決しない。

---

### 9. snapshot

#### 9.1 役割

`spectral_recording_snapshot.toml` は観測時truthの解決済み結果である。手編集しない。

snapshotには概念的に以下が入る。

```text
frequency_axes
lo_chains
beams
source_streams
streams
recorded_streams
inputs
provenance
```

`streams` は実際に保存されるstream/productを表す。multi-windowではsource streamではなくrecorded productが `streams` に入る。source streamは `source_streams` に残る。

#### 9.3 DB内snapshot sidecarの自動検出

update13では、converter/sunscan の通常運用で `--spectral-recording-snapshot` を毎回指定しなくてもよいように、RawData/DBディレクトリ内の標準 sidecar を探索する経路を追加した。

探索対象の標準名は次である。

```text
spectral_recording_snapshot.toml
```

探索は `RAWDATA_DIR` 配下を再帰的に見る。したがって、snapshot がDB直下ではなく、DB内の `metadata/` や `config/` 相当のサブディレクトリへ保存されている場合も、標準名で一意であれば検出できる。

優先順位は次である。

```text
1. CLIで明示した --spectral-recording-snapshot
2. CLIで明示した --spectrometer-config
3. RAWDATA_DIR 配下の一意な spectral_recording_snapshot.toml
4. legacy動作
```

ただし、4の扱いは tool により異なる。

```text
converter:
  snapshotもspectrometer_configも無く、new spectral setup sidecarも無い場合は、
  従来の legacy single-stream CLI fallback を維持する。

sunscan extract / singlebeam:
  snapshotもspectrometer_configも無い legacy DB では、
  従来どおり明示 config が必要である。
```

異常系は次のように扱う。

```text
複数の spectral_recording_snapshot.toml が見つかる:
  エラー。勝手に1つを選ばない。

lo_profile.toml / recording_window_setup.toml / beam_model.toml があるが、
spectral_recording_snapshot.toml が無い:
  incomplete new DB としてエラー。
  converterでも legacy fallback へ黙って落とさない。
```

この設計により、通常運用では次のように短く実行できる。

```bash
python -m tools.necst.necst_v4_sdfits_converter \
  RAWDATA_DIR \
  --converter-analysis-config converter_analysis.toml
```

```bash
python -m tools.necst.multibeam_beam_measurement.sunscan_multibeam extract \
  RAWDATA_DIR \
  --sunscan-analysis-config sunscan_analysis.toml \
  --analysis-stream-selection analysis_stream_selection.toml \
  --outdir sunscan_out
```

明示指定は、DB外のsnapshotを使う検証、古いDBへの手動対応、または意図的な再解析でのみ使う。


#### 9.4 channel mapping

観測時channel保存:

```text
c_full = 0 ... full_nchan - 1
saved_ch_start: inclusive
saved_ch_stop : exclusive
saved_nchan = saved_ch_stop - saved_ch_start
```

保存済みlocal channel:

```text
c_local = 0 ... saved_nchan - 1
c_full = saved_ch_start + c_local
```

snapshot bridgeは、保存済みlocal channel 0がfull channel `saved_ch_start` の周波数になるようWCSを作る。

---

### 10. converter

#### 10.1 明示snapshotを使う

```bash
python -m tools.necst.necst_v4_sdfits_converter \
  RAWDATA_DIR \
  --spectral-recording-snapshot RAWDATA_DIR/spectral_recording_snapshot.toml \
  --converter-analysis-config converter_analysis.toml
```

snapshotを使う場合、`--config-loader adapter` は不要である。`--config-loader adapter` は旧 `spectrometer_config.toml` を分離モデルへ通すためのloader指定である。

#### 10.5 update13の自動snapshot検出を使う通常converter運用

観測DBの中に標準名の `spectral_recording_snapshot.toml` が保存されている場合、通常は snapshot path を明示しない。

```bash
python -m tools.necst.necst_v4_sdfits_converter \
  RAWDATA_DIR \
  --converter-analysis-config converter_analysis.toml
```

この場合、converter は次を行う。

```text
1. RAWDATA_DIR を絶対パス化する。
2. --spectral-recording-snapshot と --spectrometer-config が未指定であることを確認する。
3. RAWDATA_DIR 配下の spectral_recording_snapshot.toml を再帰的に探索する。
4. 一意に見つかったら snapshot_adapter 経路で読み込む。
5. converter_analysis.toml と analysis_stream_selection.toml を解析条件として重ねる。
```

multi-window productを選ぶ場合も、DB内snapshotを使う通常運用では snapshot path を明示しない。

```bash
python -m tools.necst.necst_v4_sdfits_converter \
  RAWDATA_DIR \
  --converter-analysis-config converter_analysis.toml \
  --recorded-stream-id 2LL__13CO_J1_0
```

または、

```bash
python -m tools.necst.necst_v4_sdfits_converter \
  RAWDATA_DIR \
  --converter-analysis-config converter_analysis.toml \
  --stream-id 2LL \
  --window-id 13CO_J1_0
```

DB外のsnapshotを使いたい場合だけ、従来どおり明示する。

```bash
python -m tools.necst.necst_v4_sdfits_converter \
  RAWDATA_DIR \
  --spectral-recording-snapshot path/to/spectral_recording_snapshot.toml \
  --converter-analysis-config converter_analysis.toml
```

`--beam-model` は snapshot 経路の解析時overrideである。明示snapshotでも自動検出snapshotでも使用できるが、snapshotが見つからない場合はエラーになる。overrideを使う場合は、`--allow-beam-model-override` で意図を明示する。



#### 10.2 multi-windowの選択

recorded productを直接指定:

```bash
python -m tools.necst.necst_v4_sdfits_converter \
  RAWDATA_DIR \
  --spectral-recording-snapshot RAWDATA_DIR/spectral_recording_snapshot.toml \
  --recorded-stream-id 2LL__13CO_J1_0
```

source stream + windowで指定:

```bash
python -m tools.necst.necst_v4_sdfits_converter \
  RAWDATA_DIR \
  --spectral-recording-snapshot RAWDATA_DIR/spectral_recording_snapshot.toml \
  --stream-id 2LL \
  --window-id 13CO_J1_0
```

全streamの特定windowを処理したい場合:

```bash
python -m tools.necst.necst_v4_sdfits_converter \
  RAWDATA_DIR \
  --spectral-recording-snapshot RAWDATA_DIR/spectral_recording_snapshot.toml \
  --window-id 13CO_J1_0
```

`--stream-id 2LL` だけで複数recorded productがある場合は曖昧なのでエラーにする。

#### 10.3 channel slice

converter側の `--channel-slice` は、snapshot使用時には保存済みlocal channelで指定する。

観測時に full channel `[10000,18000)` を保存した場合、converter local channel 0はfull channel 10000である。converterで `--channel-slice "[100,200)"` とすると、full channel 10100--10199を出す。

#### 10.4 converter CLI主要オプション

| option | 意味 |
|---|---|
| `--spectrometer-config` | 旧configを使う |
| `--spectral-recording-snapshot`, `--snapshot` | snapshotを使う |
| `--beam-model` | snapshot使用時の解析用beam_model override |
| `--allow-beam-model-override` | beam_model overrideを明示許可 |
| `--config-loader legacy/adapter` | 旧config loader。snapshot時は不要 |
| `--converter-analysis-config`, `--converter-analysis` | converter解析条件 |
| `--analysis-stream-selection` | stream選択 |
| `--stream-name`, `--stream-id` | source stream選択 |
| `--window-id` | multi-windowのwindow選択 |
| `--recorded-stream-id` | recorded productを直接選択 |
| `--restfreq-ghz` | RESTFREQをCLIで上書き |
| `--channel-slice` | 保存済みlocal channel slice |
| `--vlsrk-kms-slice` | converter時のVLSRK velocity slice |

---

### 11. sunscan

#### 11.1 extract

```bash
python -m tools.necst.multibeam_beam_measurement.sunscan_multibeam extract \
  RAWDATA_DIR \
  --spectral-recording-snapshot RAWDATA_DIR/spectral_recording_snapshot.toml \
  --sunscan-analysis-config sunscan_analysis.toml \
  --analysis-stream-selection analysis_stream_selection.toml \
  --outdir sunscan_out
```

#### 11.2 singlebeam

```bash
python -m tools.necst.multibeam_beam_measurement.sunscan_singlebeam \
  RAWDATA_DIR \
  --spectral-recording-snapshot RAWDATA_DIR/spectral_recording_snapshot.toml \
  --sunscan-analysis-config sunscan_analysis.toml \
  --analysis-stream-selection analysis_stream_selection.toml \
  --stream-name 1LU \
  --outdir sunscan_single
```

#### 11.5 update13の自動snapshot検出を使うsunscan通常運用

観測DB内に `spectral_recording_snapshot.toml` が保存されている場合、sunscan extract でも snapshot path を明示しない。

```bash
python -m tools.necst.multibeam_beam_measurement.sunscan_multibeam extract \
  RAWDATA_DIR \
  --sunscan-analysis-config sunscan_analysis.toml \
  --analysis-stream-selection analysis_stream_selection.toml \
  --outdir sunscan_out
```

singlebeamでも同じである。

```bash
python -m tools.necst.multibeam_beam_measurement.sunscan_singlebeam \
  RAWDATA_DIR \
  --sunscan-analysis-config sunscan_analysis.toml \
  --analysis-stream-selection analysis_stream_selection.toml \
  --stream-name 1LU \
  --outdir sunscan_single
```

sunscanの優先順位は次である。

```text
1. 明示 --spectral-recording-snapshot
2. 明示 --spectrometer-config
3. RawData内の一意な spectral_recording_snapshot.toml
4. legacy DBでは明示config要求
```

converterと違い、sunscanは snapshot/config が無い legacy DB に対して自動的にsingle-stream configを組み立てない。これは従来挙動を維持するためである。

複数 RawData を一度に与える場合は、同じ観測設定・同じstream構造のDB群をまとめる運用を基本にする。異なる snapshot を持つDBを混在させる場合は、DBごとに分けて実行するか、共通の明示 config/snapshot を指定する。これは、multi-runの集約出力では同じ解析条件で比較することが前提になるためである。


#### 11.3 fit

sunscan fitは、現行では `--spectrometer-config` を要求する経路が残っているが、fit結果のbeam geometryは新仕様の standalone `beam_model.toml` として出力する。旧all-in-one config形式は `--write-legacy-beam-model` を付けた時だけ互換出力する。

```bash
python -m tools.necst.multibeam_beam_measurement.sunscan_fit_multibeam \
  sunscan_out/all_stream_summary.csv \
  --spectrometer-config legacy_or_adapter_config.toml \
  --config-loader adapter \
  --analysis-stream-selection analysis_stream_selection.toml \
  --center-beam-id B01 \
  --outdir fit_out
```

`center_beam_id` は解析時のfit基準であり、観測時の `pointing_reference_beam_id` とは別である。

#### 11.4 sunscan解析パラメータ

| ファイル | key | 意味 |
|---|---|---|
| `analysis_stream_selection.toml` | `streams.<id>.enabled` | 解析対象として有効か |
| 同上 | `use_for_convert` | converter対象 |
| 同上 | `use_for_sunscan` | sunscan extract対象 |
| 同上 | `use_for_fit` | sunscan fit対象 |
| 同上 | `beam_fit_use` | beam fittingで使うか |
| `converter_analysis.toml` | `db_namespace`, `telescope`, `planet` | DB名・望遠鏡・対象名 |
| 同上 | `boresight_source`, `boresight_correction_apply`, `skycoord_method`, `output_azel_source` | 座標・boresight処理 |
| 同上 | `spectrometer_time_offset_sec`, `encoder_shift_sec`, `encoder_az_time_offset_sec`, `encoder_el_time_offset_sec` | 時刻補正 |
| 同上 | `encoder_table_suffix`, `altaz_table_suffix`, `encoder_time_col`, `altaz_time_col` | encoder/altaz table |
| 同上 | `weather_inside_table_suffix`, `weather_outside_table_suffix`, `met_source` | weather/meteo |
| 同上 | `thot_default_k`, `tamb_default_k`, min/max類 | 温度fallbackとQC |
| 同上 | `channel_slice`, `export_slices_by_stream_id` | converter出力時slice。保存済みlocal channel |
| `sunscan_analysis.toml` | `chopper_wheel`, `chopper_win_sec`, `chopper_stat` | chopper wheel処理 |
| 同上 | `ripple_preset`, `ripple_model`, `ripple_target_hz`, `ripple_search_hz`, `ripple_*` | ripple除去 |
| 同上 | `edge_fit_win_deg`, `edge_fit_threshold`, `hpbw_init_arcsec`, `edge_fit_plot_max_scans` | edge fit |
| 同上 | `trim_vfrac`, `trim_vmin`, `trim_gap`, `trim_min_samples`, `trim_axis_ratio_min`, `trim_vpercentile`, `trim_scan_speed_min_arcsec`, `trim_xwin_factor`, `trim_cross_offset_max_deg`, `trim_steady_cv_max` | scan trim |
| 同上 | `dish_diameter_m`, `hpbw_factor`, `profile_xlim_deg` | sunscan profile/HPBW |


---

### 12. 受信機調整・コミッショニング・本観測 cookbook

#### 12.1 受信機調整: full spectrumで保存する

目的: まず全帯域を保存し、spurious、bandpass、LO設定、backend stream対応を確認する。

`recording_window_setup.toml`:

```toml
schema_version = "recording_window_setup_v2"
setup_id = "rx_tuning_full"

[defaults]
mode = "spectrum"
saved_window_policy = "full"
```

`.obs [parameters]`:

```toml
lo_profile = "lo_profile_rx_tuning.toml"
recording_window_setup = "recording_full.toml"
beam_model = "beam_model_single_or_multi.toml"
setup_id = "rx_tuning_full_YYYYMMDD"
```

#### 12.2 コミッショニング: 速度範囲を広めに保存する

目的: LSRK計算・stream/beam/LO対応・保存windowの安全性を確認する。

```toml
[recording_groups.co115]
mode = "spectrum"
saved_window_policy = "contiguous_envelope"
velocity_frame = "LSRK"
velocity_definition = "radio"
streams = ["2LU", "2RU"]
windows = [
  { window_id = "12CO_J1_0", rest_frequency_ghz = 115.271, vmin_kms = -200.0, vmax_kms = 200.0, margin_kms = 30.0 },
]
```

#### 12.3 通常観測: 必要な範囲だけ保存する

目的: DBサイズを減らしつつ、必要な速度範囲を取り逃さない。

```toml
[recording_groups.co115]
mode = "spectrum"
saved_window_policy = "contiguous_envelope"
velocity_frame = "LSRK"
velocity_definition = "radio"
streams = ["2LU", "2RU", "3LU", "3RU"]
windows = [
  { window_id = "12CO_J1_0", rest_frequency_ghz = 115.271, vmin_kms = -150.0, vmax_kms = 150.0, margin_kms = 10.0 },
]
```

#### 12.4 110 GHzで13CO/C18Oを別product保存する

```toml
[recording_groups.co110_multi]
mode = "spectrum"
saved_window_policy = "multi_window"
velocity_frame = "LSRK"
velocity_definition = "radio"
streams = ["2LL", "2RL", "3RL", "4LL", "4RL", "5LL", "5RL"]

windows = [
  { window_id = "13CO_J1_0", line_name = "13CO J=1-0", rest_frequency_ghz = 110.201353, vmin_kms = -100.0, vmax_kms = 100.0, margin_kms = 10.0 },
  { window_id = "C18O_J1_0", line_name = "C18O J=1-0", rest_frequency_ghz = 109.7821734, vmin_kms = -100.0, vmax_kms = 100.0, margin_kms = 10.0 },
]
```

#### 12.5 sunscanでbeam_modelを作る

1. fullまたは十分広いwindowでsunscan観測する。
2. `sunscan_multibeam extract` でstreamごとのprofileを出す。
3. `sunscan_fit_multibeam` でfitし、standalone `beam_model.toml` を得る。
4. 本観測の `.obs [parameters]` でその `beam_model.toml` を参照する。
5. `pointing_reference_beam_id` を使った観測で外部beam_modelをoverrideする場合は、そのbeam IDの定義がsnapshotと一致することを確認する。

---

### 13. 旧config相当のNANTEN2/XFFTS例

本節は、旧config `12co-NANTEN2-multi_260331TO.conf3` と同等の新分離ファイル例である。230/115 GHzは `contiguous_envelope`、110 GHzは `multi_window` を使う。

#### 13.1 `lo_profile_12co_NANTEN2_multi_260331TO.toml`

```toml
## Auto-generated from 12co-NANTEN2-multi_260331TO.conf3.txt
## New split configuration example: stream truth + shared frequency/LO definitions.
schema_version = "lo_profile_v2"
profile_id = "12co_NANTEN2_multi_260331TO_equivalent_update12"

[frequency_axes.xffts_2GHz_32768ch]
definition_mode = "band_start_stop"
nchan = 32768
## Current resolver key is *_hz. Equivalent: 0.0 MHz to 2000.0 MHz.
band_start_hz = 0.0
band_stop_hz = 2000000000.0
channel_origin = "center"
reverse = false
ctype1 = "FREQ"
cunit1 = "Hz"
specsys = "TOPOCENT"
veldef = "RADIO"
store_freq_column = false

[lo_chains.rx115_12co_usb_usb]
formula_version = "legacy_two_stage_sideband_v1"
lo1_ghz = 104.67120198
sb1 = "USB"
lo2_mhz = 9500
sb2 = "USB"

[lo_chains.rx115_110_usb_usb]
formula_version = "legacy_two_stage_sideband_v1"
lo1_ghz = 104.67120198
sb1 = "USB"
lo2_mhz = 4000
sb2 = "USB"

[lo_chains.rx230_12co_usb_usb]
formula_version = "legacy_two_stage_sideband_v1"
lo1_ghz = 225.635994
sb1 = "USB"
lo2_mhz = 4500
sb2 = "USB"

[stream_groups.nanten2_xffts_12co115]
spectrometer_key = "xffts"
backend = "XFFTS"
sampler = "XFFTS1"
frequency_axis_id = "xffts_2GHz_32768ch"
frontend = "RX115"
lo_chain = "rx115_12co_usb_usb"
rest_frequency_ghz = 115.271

  [[stream_groups.nanten2_xffts_12co115.streams]]
  stream_id = "2LU"
  db_stream_name = "xffts-board1"
  board_id = 1
  fdnum = 2
  ifnum = 0
  plnum = 1
  polariza = "LL"
  beam_id = "B02"

  [[stream_groups.nanten2_xffts_12co115.streams]]
  stream_id = "2RU"
  db_stream_name = "xffts-board3"
  board_id = 3
  fdnum = 2
  ifnum = 0
  plnum = 0
  polariza = "RR"
  beam_id = "B02"

  [[stream_groups.nanten2_xffts_12co115.streams]]
  stream_id = "3LU"
  db_stream_name = "xffts-board5"
  board_id = 5
  fdnum = 3
  ifnum = 0
  plnum = 1
  polariza = "LL"
  beam_id = "B03"

  [[stream_groups.nanten2_xffts_12co115.streams]]
  stream_id = "3RU"
  db_stream_name = "xffts-board7"
  board_id = 7
  fdnum = 3
  ifnum = 0
  plnum = 0
  polariza = "RR"
  beam_id = "B03"

  [[stream_groups.nanten2_xffts_12co115.streams]]
  stream_id = "4LU"
  db_stream_name = "xffts-board9"
  board_id = 9
  fdnum = 4
  ifnum = 0
  plnum = 1
  polariza = "LL"
  beam_id = "B04"

  [[stream_groups.nanten2_xffts_12co115.streams]]
  stream_id = "4RU"
  db_stream_name = "xffts-board11"
  board_id = 11
  fdnum = 4
  ifnum = 0
  plnum = 0
  polariza = "RR"
  beam_id = "B04"

  [[stream_groups.nanten2_xffts_12co115.streams]]
  stream_id = "5LU"
  db_stream_name = "xffts-board13"
  board_id = 13
  fdnum = 5
  ifnum = 0
  plnum = 1
  polariza = "LL"
  beam_id = "B05"

  [[stream_groups.nanten2_xffts_12co115.streams]]
  stream_id = "5RU"
  db_stream_name = "xffts-board15"
  board_id = 15
  fdnum = 5
  ifnum = 0
  plnum = 0
  polariza = "RR"
  beam_id = "B05"

[stream_groups.nanten2_xffts_110ghz]
spectrometer_key = "xffts"
backend = "XFFTS"
sampler = "XFFTS1"
frequency_axis_id = "xffts_2GHz_32768ch"
frontend = "RX115"
lo_chain = "rx115_110_usb_usb"
rest_frequency_ghz = 110.201353

  [[stream_groups.nanten2_xffts_110ghz.streams]]
  stream_id = "2LL"
  db_stream_name = "xffts-board2"
  board_id = 2
  fdnum = 2
  ifnum = 1
  plnum = 1
  polariza = "LL"
  frontend = "RX230"
  beam_id = "B02"

  [[stream_groups.nanten2_xffts_110ghz.streams]]
  stream_id = "2RL"
  db_stream_name = "xffts-board4"
  board_id = 4
  fdnum = 2
  ifnum = 1
  plnum = 0
  polariza = "RR"
  beam_id = "B02"

  [[stream_groups.nanten2_xffts_110ghz.streams]]
  stream_id = "3RL"
  db_stream_name = "xffts-board8"
  board_id = 8
  fdnum = 3
  ifnum = 1
  plnum = 0
  polariza = "RR"
  beam_id = "B03"

  [[stream_groups.nanten2_xffts_110ghz.streams]]
  stream_id = "4LL"
  db_stream_name = "xffts-board10"
  board_id = 10
  fdnum = 4
  ifnum = 1
  plnum = 1
  polariza = "LL"
  beam_id = "B04"

  [[stream_groups.nanten2_xffts_110ghz.streams]]
  stream_id = "4RL"
  db_stream_name = "xffts-board12"
  board_id = 12
  fdnum = 4
  ifnum = 1
  plnum = 0
  polariza = "RR"
  beam_id = "B04"

  [[stream_groups.nanten2_xffts_110ghz.streams]]
  stream_id = "5LL"
  db_stream_name = "xffts-board14"
  board_id = 14
  fdnum = 5
  ifnum = 1
  plnum = 1
  polariza = "LL"
  beam_id = "B05"

  [[stream_groups.nanten2_xffts_110ghz.streams]]
  stream_id = "5RL"
  db_stream_name = "xffts-board16"
  board_id = 16
  fdnum = 5
  ifnum = 1
  plnum = 0
  polariza = "RR"
  beam_id = "B05"

[stream_groups.nanten2_xffts_12co230]
spectrometer_key = "xffts"
backend = "XFFTS"
sampler = "XFFTS1"
frequency_axis_id = "xffts_2GHz_32768ch"
frontend = "RX230"
lo_chain = "rx230_12co_usb_usb"
rest_frequency_ghz = 230.538

  [[stream_groups.nanten2_xffts_12co230.streams]]
  stream_id = "1LU"
  db_stream_name = "xffts-board6"
  board_id = 6
  fdnum = 1
  ifnum = 0
  plnum = 1
  polariza = "LL"
  beam_id = "B01"
```

#### 13.2 `beam_model_12co_NANTEN2_multi_260331TO.toml`

```toml
## Auto-generated beam_model.toml from legacy spectrometer beam entries.
schema_version = "beam_model_v2"
beam_model_id = "12co_NANTEN2_multi_260331TO_equivalent"

[beams.B00]
beam_model_version = "default_zero_boresight"
rotation_mode = "fixed"
az_offset_arcsec = 0.0
el_offset_arcsec = 0.0
dewar_angle_deg = 0.0

[beams.B01]
beam_model_version = "sunscan_multibeam_pure_rotation_v1"
rotation_mode = "pure_rotation_v1"
pure_rotation_offset_x_el0_arcsec = 0
pure_rotation_offset_y_el0_arcsec = 0
pure_rotation_sign = -1
dewar_angle_deg = 0.0

[beams.B02]
beam_model_version = "sunscan_multibeam_pure_rotation_v1"
rotation_mode = "pure_rotation_v1"
pure_rotation_offset_x_el0_arcsec = 35.20277101014
pure_rotation_offset_y_el0_arcsec = 295.06639448179
pure_rotation_sign = -1
dewar_angle_deg = 0.0

[beams.B03]
beam_model_version = "sunscan_multibeam_pure_rotation_v1"
rotation_mode = "pure_rotation_v1"
pure_rotation_offset_x_el0_arcsec = -286.106025615731
pure_rotation_offset_y_el0_arcsec = 23.9055055360697
pure_rotation_sign = -1
dewar_angle_deg = 0.0

[beams.B04]
beam_model_version = "sunscan_multibeam_pure_rotation_v1"
rotation_mode = "pure_rotation_v1"
pure_rotation_offset_x_el0_arcsec = 68.379403972865
pure_rotation_offset_y_el0_arcsec = -303.605491064541
pure_rotation_sign = -1
dewar_angle_deg = 0.0

[beams.B05]
beam_model_version = "sunscan_multibeam_pure_rotation_v1"
rotation_mode = "pure_rotation_v1"
pure_rotation_offset_x_el0_arcsec = 330.962642142925
pure_rotation_offset_y_el0_arcsec = -17.272265827714
pure_rotation_sign = -1
dewar_angle_deg = 0.0
```

#### 13.3 `recording_window_12co_NANTEN2_multi_260331TO.toml`

```toml
## Recording windows for 12co-NANTEN2-multi_260331TO equivalent setup.
## 230/115 GHz: VLSRK -150..+150 km/s with 10 km/s margin.
## 110 GHz: multi_window products for 13CO and C18O, each -100..+100 km/s with 10 km/s margin.
schema_version = "recording_window_setup_v2"
setup_id = "12co_NANTEN2_multi_260331TO_windows_update12"

[defaults]
mode = "spectrum"
velocity_frame = "LSRK"
velocity_definition = "radio"
reference_time_policy = "setup_resolve_time"

[recording_groups.co230_12CO_J2_1]
mode = "spectrum"
saved_window_policy = "contiguous_envelope"
streams = ["1LU"]
windows = [
  { window_id = "12CO_J2_1", line_name = "12CO J=2-1", rest_frequency_ghz = 230.538, vmin_kms = -150.0, vmax_kms = 150.0, margin_kms = 10.0 },
]

[recording_groups.co115_12CO_J1_0]
mode = "spectrum"
saved_window_policy = "contiguous_envelope"
streams = ["2LU", "2RU", "3LU", "3RU", "4LU", "4RU", "5LU", "5RU"]
windows = [
  { window_id = "12CO_J1_0", line_name = "12CO J=1-0", rest_frequency_ghz = 115.271, vmin_kms = -150.0, vmax_kms = 150.0, margin_kms = 10.0 },
]

[recording_groups.co110_13CO_C18O_multi_window]
mode = "spectrum"
saved_window_policy = "multi_window"
streams = ["2LL", "2RL", "3RL", "4LL", "4RL", "5LL", "5RL"]
windows = [
  { window_id = "13CO_J1_0", line_name = "13CO J=1-0", rest_frequency_ghz = 110.201353, vmin_kms = -100.0, vmax_kms = 100.0, margin_kms = 10.0 },
  { window_id = "C18O_J1_0", line_name = "C18O J=1-0", rest_frequency_ghz = 109.7821734, vmin_kms = -100.0, vmax_kms = 100.0, margin_kms = 10.0 },
]
```

#### 13.4 `analysis_stream_selection_12co_NANTEN2_multi_260331TO.toml`

```toml
schema_version = "analysis_stream_selection_v1"
selection_id = "12co_NANTEN2_multi_260331TO_legacy_selection"

[streams."2LU"]
enabled = true
use_for_convert = true
use_for_sunscan = true
use_for_fit = true
beam_fit_use = true

[streams."2LL"]
enabled = true
use_for_convert = true
use_for_sunscan = true
use_for_fit = false
beam_fit_use = false

[streams."2RU"]
enabled = true
use_for_convert = true
use_for_sunscan = true
use_for_fit = true
beam_fit_use = true

[streams."2RL"]
enabled = true
use_for_convert = true
use_for_sunscan = true
use_for_fit = false
beam_fit_use = false

[streams."3LU"]
enabled = true
use_for_convert = true
use_for_sunscan = true
use_for_fit = false
beam_fit_use = false

[streams."1LU"]
enabled = true
use_for_convert = true
use_for_sunscan = true
use_for_fit = true
beam_fit_use = true

[streams."3RU"]
enabled = true
use_for_convert = true
use_for_sunscan = true
use_for_fit = false
beam_fit_use = false

[streams."3RL"]
enabled = true
use_for_convert = true
use_for_sunscan = true
use_for_fit = false
beam_fit_use = false

[streams."4LU"]
enabled = true
use_for_convert = true
use_for_sunscan = true
use_for_fit = false
beam_fit_use = false

[streams."4LL"]
enabled = true
use_for_convert = true
use_for_sunscan = true
use_for_fit = false
beam_fit_use = false

[streams."4RU"]
enabled = true
use_for_convert = true
use_for_sunscan = true
use_for_fit = true
beam_fit_use = true

[streams."4RL"]
enabled = true
use_for_convert = true
use_for_sunscan = true
use_for_fit = false
beam_fit_use = false

[streams."5LU"]
enabled = true
use_for_convert = true
use_for_sunscan = true
use_for_fit = false
beam_fit_use = false

[streams."5LL"]
enabled = true
use_for_convert = true
use_for_sunscan = true
use_for_fit = true
beam_fit_use = true

[streams."5RU"]
enabled = true
use_for_convert = true
use_for_sunscan = true
use_for_fit = false
beam_fit_use = false

[streams."5RL"]
enabled = true
use_for_convert = true
use_for_sunscan = true
use_for_fit = false
beam_fit_use = false
```

#### 13.5 `converter_analysis_12co_NANTEN2_multi_260331TO.toml`

```toml
schema_version = "converter_analysis_v1"
analysis_id = "12co_NANTEN2_multi_260331TO_converter"

[converter_analysis]
db_namespace = "necst"
telescope = "NANTEN2"
planet = "Ori-KL"
spectral_name = "xffts-board1"
boresight_source = "encoder"
boresight_correction_apply = "add"
skycoord_method = "azel"
output_azel_source = "beam"
spectrometer_time_offset_sec = -0.145
encoder_shift_sec = 0.0
encoder_az_time_offset_sec = 0.0
encoder_el_time_offset_sec = 0.0
encoder_time_col = "time"
altaz_time_col = "time"
encoder_table_suffix = "ctrl-antenna-encoder"
altaz_table_suffix = "ctrl-antenna-altaz"
weather_inside_table_suffix = "weather-ambient-in"
weather_outside_table_suffix = "weather-ambient-out"
weather_inside_time_col = "time"
weather_outside_time_col = "time"
met_source = "auto"
thot_default_k = 273.15
thot_min_k = 250.0
thot_max_k = 330.0
tamb_default_k = 270.0
tamb_min_k = 230.0
tamb_max_k = 330.0
outside_default_temperature_c = 0.0
outside_default_pressure_hpa = 520.0
outside_default_humidity_pct = 30.0
outside_temperature_min_c = -50.0
outside_temperature_max_c = 50.0
outside_pressure_min_hpa = 400.0
outside_pressure_max_hpa = 1100.0
outside_humidity_min_pct = 0.0
outside_humidity_max_pct = 100.0
chopper_wheel = true
chopper_win_sec = 5.0
chopper_stat = "median"
output_layout = "merged"
time_sort = true
```

#### 13.6 `sunscan_analysis_12co_NANTEN2_multi_260331TO.toml`

```toml
schema_version = "sunscan_analysis_v1"
analysis_id = "12co_NANTEN2_multi_260331TO_sunscan"

[sunscan_analysis]
db_namespace = "necst"
telescope = "NANTEN2"
planet = "Ori-KL"
spectral_name = "xffts-board1"
boresight_source = "encoder"
boresight_correction_apply = "add"
skycoord_method = "azel"
output_azel_source = "beam"
spectrometer_time_offset_sec = -0.145
encoder_shift_sec = 0.0
encoder_az_time_offset_sec = 0.0
encoder_el_time_offset_sec = 0.0
encoder_time_col = "time"
altaz_time_col = "time"
encoder_table_suffix = "ctrl-antenna-encoder"
altaz_table_suffix = "ctrl-antenna-altaz"
weather_inside_table_suffix = "weather-ambient-in"
weather_outside_table_suffix = "weather-ambient-out"
weather_inside_time_col = "time"
weather_outside_time_col = "time"
met_source = "auto"
thot_default_k = 273.15
thot_min_k = 250.0
thot_max_k = 330.0
tamb_default_k = 270.0
tamb_min_k = 230.0
tamb_max_k = 330.0
chopper_wheel = true
chopper_win_sec = 5.0
chopper_stat = "median"
profile_xlim_deg = 1.0
ripple_preset = "auto"
ripple_model = "auto"
ripple_target_hz = 1.2
ripple_search_hz = 0.3
edge_fit_win_deg = 0.15
edge_fit_threshold = 0.2
hpbw_init_arcsec = 324.0
edge_fit_plot_max_scans = 3
trim_vfrac = 0.2
trim_vmin = 0.0001
trim_gap = 10
trim_min_samples = 100
trim_axis_ratio_min = 3.0
trim_vpercentile = 95.0
trim_scan_speed_min_arcsec = 20.0
trim_xwin_factor = 1.2
trim_cross_offset_max_deg = 0.5
trim_steady_cv_max = 0.8
dish_diameter_m = 4.0
hpbw_factor = 0.5
output_layout = "merged"
time_sort = true
```

#### 13.7 `.obs [parameters]` fragment

```toml
## Add these keys to the [parameters] section of the .obs file.
## Paths are normally resolved relative to the .obs file directory.
[parameters]
cos_correction = true
lo_profile = "lo_profile_12co_NANTEN2_multi_260331TO.toml"
recording_window_setup = "recording_window_12co_NANTEN2_multi_260331TO.toml"
beam_model = "beam_model_12co_NANTEN2_multi_260331TO.toml"
setup_id = "orikl_NANTEN2_multi_260331TO_update12_windows"
## velocity_reference_time_utc = "2026-04-29T12:34:56Z"
## pointing_reference_beam_id = "B01"
## pointing_reference_beam_policy = "exact"

## VLSRK recording windows require a deterministic reference direction and site.
## If your .obs parser provides the [coordinate] reference as obsspec._reference,
## these explicit fields may be omitted.  Otherwise set them explicitly.
## Example for Galactic Center:
## velocity_reference_l_deg = 0.0
## velocity_reference_b_deg = 0.0
## velocity_reference_site_lat_deg = -22.969956
## velocity_reference_site_lon_deg = -67.703081
## velocity_reference_site_elev_m = 4865.0
## velocity_reference_time_utc = "2026-04-29T12:34:56Z"
```

---

### 14. ファイル間依存関係と検査

| 依存元 | 依存先 | 検査 |
|---|---|---|
| `.obs: lo_profile` | `lo_profile.toml` | file存在、TOML parse、stream/LO/frequency axisが解決可能 |
| `.obs: recording_window_setup` | `lo_profile.stream_id` | group内streamsが実在 |
| `.obs: beam_model` | stream `beam_id` | non-B00 beamが存在 |
| `.obs: pointing_reference_beam_id` | `beam_model.beams` | 指定beamが存在 |
| snapshot beams vs external `beam_model.toml` | pointing reference beam | 指定beamだけ一致必須 |
| stream `frequency_axis_id` | `frequency_axes` | axisが存在 |
| stream `lo_chain` | `lo_chains` | chainが存在 |
| LO roles | SG devices | SG IDが存在 |
| `saved_ch_start/stop` | `full_nchan` | `0 <= start < stop <= full_nchan` |
| converter `channel_slice` | saved local nchan | 範囲内 |
| `window_id` | DB path | 安全文字、group内一意 |
| `recorded_stream_id` | snapshot streams | snapshot内一意 |
| `recorded_db_stream_name` | DB table/file | snapshot内一意 |

---

### 15. 性能・リアルタイム性

#### 15.1 setup resolve側

TOML parse、VLSRK計算、velocity window計算、beam_model hash、snapshot生成はrecord開始前に行う。raw callbackに入れてはいけない。

update12では、velocity window解決時に32768 channel配列を毎回作らず、線形WCSから直接channel範囲を計算する。

#### 15.2 runtime callback側

raw callbackでは、raw入力 `(raw_input_key, raw_board_id)` から保存対象recorded productを高速に引く必要がある。update12ではsetup apply時にindexを作る。

避けるべき処理:

```text
TOML parse
全stream走査
VLSRK補正計算
周波数配列生成
beam_model parse
hash計算
```

#### 15.3 multi-window時のDB構造

1 recorded product = 1固定長schema = 1 DB table/file とする。同じDB tableにchannel数やRESTFRQの異なるwindowを混ぜない。

---

### 16. よくある間違い

#### 16.1 `rest_frequency_ghz` を `frequency_axes` に書く

`rest_frequency_ghz` はstreamまたはwindowに書く。frequency axisは分光計のIF軸であり、line rest frequencyではない。

#### 16.2 `--channel-slice` にfull channelを入れる

snapshot使用時のconverter `--channel-slice` は保存済みlocal channelである。観測時のfull channelと混同しない。

#### 16.3 `stream_id` だけでmulti-window productを選ぶ

multi-windowで1 source streamに複数recorded productがある場合、`--stream-id` だけでは曖昧である。`--window-id` または `--recorded-stream-id` を指定する。

#### 16.4 `beam_model` だけで新setupが起動すると思う

`beam_model` だけではspectral recording setupは起動しない。stream truthの本体は `lo_profile` または `spectral_recording_snapshot` である。

#### 16.5 LSRKに必要な参照情報を省略する

`velocity_frame="LSRK"` では、参照座標・site・基準時刻が必要である。情報不足ならエラーにする。近似で進めたい場合は明示的に `velocity_frame="TOPOCENTRIC"` とする。

---

### 17. 既知の未確認項目

この環境で確認済み:

```text
TOML parse
snapshot resolve simulation
旧config相当ファイル生成
multi-window snapshot構造
converter selection logic
構文チェック
速度面のpure-Python測定
```

この環境で未実施:

```text
colcon build
ROS2 service生成
実SG readback
実NECSTDB recording
実raw DBに対するconverter/sunscan完全実行
Astropyを用いた実天球VLSRK値の実測
```

実機投入前には、実環境でこれらを確認する。



| option | 型 | 説明 |
|---|---:|---|
| `RAWDATA_DIR` | path | NECST RawData directory。複数指定可 |
| `--out` / 出力系オプション | path | SDFITS等の出力先。実CLIの既存指定に従う |
| `--spectrometer-config` | path | 旧all-in-one configを使う場合 |
| `--spectral-recording-snapshot`, `--snapshot` | path | 観測時snapshotを使う場合 |
| `--beam-model` | path | snapshot内beam geometryの解析時override |
| `--allow-beam-model-override` | flag | `--beam-model` overrideを許可 |
| `--config-loader` | enum | `legacy` / `adapter`。旧config用。snapshot使用時は不要 |
| `--converter-analysis-config`, `--converter-analysis` | path | converter解析条件TOML |
| `--analysis-stream-selection` | path | 解析stream選択TOML。複数指定可 |
| `--stream-name`, `--convert-stream-name`, `--stream-id` | string | source streamを選ぶ。multi-windowでは`--window-id`が必要なことがある |
| `--window-id` | string | multi-window productをwindowで選ぶ |
| `--recorded-stream-id` | string | recorded productを直接選ぶ |
| `--restfreq-ghz` | float | RESTFREQをGHzで上書き |
| `--channel-slice` | string | converter出力時channel slice。snapshot使用時は保存済みlocal channel |
| `--vlsrk-kms-slice` | string | converter時VLSRK速度slice |


| command | 主なoption | 説明 |
|---|---|---|
| `sunscan_multibeam extract` | `--spectral-recording-snapshot` | snapshotからstream truthを読む |
| 同上 | `--sunscan-analysis-config` / `--sunscan-analysis` | sunscan解析条件 |
| 同上 | `--analysis-stream-selection` | extract/fit対象選択 |
| 同上 | `--beam-model`, `--allow-beam-model-override` | 解析時beam model override |
| 同上 | `--spectrometer-config`, `--config-loader adapter` | 旧config互換 |
| `sunscan_singlebeam` | `--stream-name` | single stream指定 |
| `sunscan_fit_multibeam` | `--center-beam-id` | fitで中心beamとして扱うbeam。観測制御ではない |
| 同上 | `--fit-stream-name` | fit対象stream |
| 同上 | `--write-legacy-beam-model` | 旧all-in-one config型beam出力も作る |


### 18. トラブルシュート

#### 18.1 `stream_groups must not be empty`

現行実装では、`lo_profile.toml` に `[stream_groups.<group>]` と `[[stream_groups.<group>.streams]]` が必要である。将来 `[streams.<id>]` 直書き形式を追加する余地はあるが、update12では `stream_groups` を使う。

#### 18.2 `beam_model.toml is required when streams reference non-B00`

`beam_model` を省略したときに使えるのは `B00=(0,0)` だけである。`B01` 以降をstreamが参照する場合は `beam_model.toml` を指定する。

#### 18.3 `velocity_frame='LSRK' requires a velocity_reference_context`

LSRK/VLSRK保存windowでは、参照座標・site・基準時刻が必要である。`.obs` parserがreferenceを渡さない場合は `[parameters]` に `velocity_reference_l_deg` / `velocity_reference_b_deg` などを明示する。

#### 18.4 `source stream has multiple recorded products`

multi-windowで `--stream-id 2LL` だけ指定すると、`2LL__13CO_J1_0` と `2LL__C18O_J1_0` のどちらを読むか曖昧である。`--window-id` または `--recorded-stream-id` を追加する。

#### 18.5 期待した速度範囲が保存されていない

確認順序:

1. `recording_window_setup.toml` の `velocity_frame` を確認する。
2. `velocity_reference_*` が正しいか確認する。
3. snapshotの `computed_windows_json` を確認する。
4. `saved_ch_start/saved_ch_stop` がfull channelとして妥当か確認する。
5. converterの `--channel-slice` をfull channelと勘違いしていないか確認する。

#### 18.6 rest frequencyの値が大きすぎて読みにくい

`rest_frequency_ghz`、`restfreq_ghz`、`lo1_ghz`、`lo2_mhz` などを使う。snapshot内ではHzへ正規化される。

#### 18.7 sunscan fitで新beam_modelが見つからない

fitの既定出力はstandalone `beam_model.toml` 形式である。旧all-in-one config形式が必要なら `--write-legacy-beam-model` を付ける。



### 19. 実機投入前チェックリスト

#### 19.1 ファイル整合

- `.obs [parameters]` の相対パスが `.obs` から正しく辿れる。
- `lo_profile.toml` の `stream_groups` が空でない。
- すべてのstreamに `stream_id`, `board_id`, `fdnum`, `ifnum`, `plnum`, `polariza` がある。
- すべてのstreamが `frequency_axis_id` と `lo_chain` を解決できる。
- すべてのnon-B00 `beam_id` が `beam_model.toml` に存在する。
- `recording_window_setup.toml` の `streams` が実在する。
- `window_id` が安全文字だけで構成されている。
- LSRK/VLSRK使用時は参照座標・site・基準時刻が確定している。

#### 19.2 周波数軸確認

- `channel_origin=center` の場合、`delta = (stop-start)/(N-1)`。
- `channel_origin=edge` の場合、`delta = (stop-start)/N`。
- `reverse=true` の場合、最終 `if_freq_step_hz` の符号が意図通りか。
- `rest_frequency_ghz` がfrequency axisではなくstream/windowにある。
- 110 GHzの複数lineは `multi_window` か `contiguous_envelope` のどちらかを意図して選んでいる。

#### 19.3 runtime確認

- setup apply時にfixed schema検査が通る。
- gate open前にsidecar保存が行われる。
- raw callbackで例外が出ない。
- multi-window時にrecorded productごとのDB table/fileが分かれている。
- `setup_hash` がログ・snapshot・DB rowで追える。

#### 19.4 後処理確認

- converterはsnapshotを使う。
- multi-windowでは `--recorded-stream-id` または `--stream-id + --window-id` を使う。
- sunscan extractは `--sunscan-analysis-config` と `--analysis-stream-selection` を使う。
- beam_model override時はpointing reference beamの一致を確認する。


### 20. 旧config `[global]` の新ファイル対応表

旧configの `[global]` にあった主な解析パラメータは、観測時truthではなく `converter_analysis.toml` と `sunscan_analysis.toml` へ移す。旧config例の値と移行先は以下である。

| 旧global key | 旧値例 | 新ファイル |
|---|---:|---|
| `db_namespace` | `'necst'` | converter_analysis/sunscan_analysis |
| `telescope` | `'NANTEN2'` | converter_analysis/sunscan_analysis |
| `planet` | `'Ori-KL'` | converter_analysis/sunscan_analysis |
| `spectral_name` | `'xffts-board1'` | converter_analysis/sunscan_analysis |
| `boresight_source` | `'encoder'` | converter_analysis/sunscan_analysis |
| `boresight_correction_apply` | `'add'` | converter_analysis/sunscan_analysis |
| `skycoord_method` | `'azel'` | converter_analysis/sunscan_analysis |
| `output_azel_source` | `'beam'` | converter_analysis/sunscan_analysis |
| `spectrometer_time_offset_sec` | `-0.145` | converter_analysis/sunscan_analysis |
| `encoder_shift_sec` | `0.0` | converter_analysis/sunscan_analysis |
| `encoder_az_time_offset_sec` | `0.0` | converter_analysis/sunscan_analysis |
| `encoder_el_time_offset_sec` | `0.0` | converter_analysis/sunscan_analysis |
| `encoder_time_col` | `'time'` | converter_analysis/sunscan_analysis |
| `altaz_time_col` | `'time'` | converter_analysis/sunscan_analysis |
| `encoder_table_suffix` | `'ctrl-antenna-encoder'` | converter_analysis/sunscan_analysis |
| `altaz_table_suffix` | `'ctrl-antenna-altaz'` | converter_analysis/sunscan_analysis |
| `weather_inside_table_suffix` | `'weather-ambient-in'` | converter_analysis/sunscan_analysis |
| `weather_outside_table_suffix` | `'weather-ambient-out'` | converter_analysis/sunscan_analysis |
| `weather_inside_time_col` | `'time'` | converter_analysis/sunscan_analysis |
| `weather_outside_time_col` | `'time'` | converter_analysis/sunscan_analysis |
| `met_source` | `'auto'` | converter_analysis/sunscan_analysis |
| `thot_default_k` | `273.15` | converter_analysis/sunscan_analysis |
| `thot_min_k` | `250.0` | converter_analysis/sunscan_analysis |
| `thot_max_k` | `330.0` | converter_analysis/sunscan_analysis |
| `tamb_default_k` | `270.0` | converter_analysis/sunscan_analysis |
| `tamb_min_k` | `230.0` | converter_analysis/sunscan_analysis |
| `tamb_max_k` | `330.0` | converter_analysis/sunscan_analysis |
| `outside_default_temperature_c` | `0.0` | converter_analysis |
| `outside_default_pressure_hpa` | `520.0` | converter_analysis |
| `outside_default_humidity_pct` | `30.0` | converter_analysis |
| `outside_temperature_min_c` | `-50.0` | converter_analysis |
| `outside_temperature_max_c` | `50.0` | converter_analysis |
| `outside_pressure_min_hpa` | `400.0` | converter_analysis |
| `outside_pressure_max_hpa` | `1100.0` | converter_analysis |
| `outside_humidity_min_pct` | `0.0` | converter_analysis |
| `outside_humidity_max_pct` | `100.0` | converter_analysis |
| `chopper_wheel` | `True` | sunscan_analysis |
| `chopper_win_sec` | `5.0` | sunscan_analysis |
| `chopper_stat` | `'median'` | sunscan_analysis |
| `profile_xlim_deg` | `1.0` | sunscan_analysis |
| `ripple_preset` | `'auto'` | sunscan_analysis |
| `ripple_model` | `'auto'` | sunscan_analysis |
| `ripple_target_hz` | `1.2` | sunscan_analysis |
| `ripple_search_hz` | `0.3` | sunscan_analysis |
| `edge_fit_win_deg` | `0.15` | sunscan_analysis |
| `edge_fit_threshold` | `0.2` | sunscan_analysis |
| `hpbw_init_arcsec` | `324.0` | sunscan_analysis |
| `edge_fit_plot_max_scans` | `3` | sunscan_analysis |
| `trim_vfrac` | `0.2` | sunscan_analysis |
| `trim_vmin` | `0.0001` | sunscan_analysis |
| `trim_gap` | `10` | sunscan_analysis |
| `trim_min_samples` | `100` | sunscan_analysis |
| `trim_axis_ratio_min` | `3.0` | sunscan_analysis |
| `trim_vpercentile` | `95.0` | sunscan_analysis |
| `trim_scan_speed_min_arcsec` | `20.0` | sunscan_analysis |
| `trim_xwin_factor` | `1.2` | sunscan_analysis |
| `trim_cross_offset_max_deg` | `0.5` | sunscan_analysis |
| `trim_steady_cv_max` | `0.8` | sunscan_analysis |
| `dish_diameter_m` | `4.0` | sunscan_analysis |
| `hpbw_factor` | `0.5` | sunscan_analysis |
| `output_layout` | `'merged'` | converter_analysis/sunscan_analysis |
| `time_sort` | `True` | converter_analysis/sunscan_analysis |


### 21. snapshotに残る主なフィールド

| field | 意味 |
|---|---|
| `schema_version` | snapshot schema |
| `setup_id` | 人間向けsetup名 |
| `setup_hash` / `canonical_snapshot_sha256` | snapshot同一性確認 |
| `created_utc` | snapshot生成時刻 |
| `inputs` | 元設定ファイルのpath/hash |
| `frequency_axes` | 解決済みfrequency axis |
| `lo_chains` | 解決済みLO chain |
| `beams` | 解決済みbeam geometry |
| `source_streams` | raw入力source stream |
| `streams` | 実際に保存されるstream/product |
| `recorded_streams` | multi-windowで生成されたrecorded product |
| `computed_windows` | channel/velocity windowの計算結果 |
| `computed_windows_json` | DB固定長metadata用JSON文字列 |
| `raw_input_key`, `raw_board_id` | runtime callback binding |
| `db_stream_name` | rawまたはrecorded DB stream名 |
| `recorded_db_stream_name` | recorded product保存名 |
| `source_stream_id` | recorded productの元stream |
| `source_db_stream_name` | recorded productの元DB stream |
| `saved_ch_start`, `saved_ch_stop`, `saved_nchan` | 保存範囲 |
| `full_nchan` | 元full channel数 |
| `rest_frequency_hz` | stream/productのRESTFRQ |

### 22. TOPOCENTRIC fallback用の明示例

LSRK/VLSRKに必要な参照座標・siteがまだ確定できないコミッショニングでは、近似であることを明示したうえで `velocity_frame = "TOPOCENTRIC"` を使える。この場合だけ、rest周波数まわりの近似windowとして扱う。

```toml
## Recording windows for 12co-NANTEN2-multi_260331TO equivalent setup.
## 230/115 GHz: VLSRK -150..+150 km/s with 10 km/s margin.
## 110 GHz: multi_window products for 13CO and C18O, each -100..+100 km/s with 10 km/s margin.
schema_version = "recording_window_setup_v2"
setup_id = "12co_NANTEN2_multi_260331TO_windows_update12"

[defaults]
mode = "spectrum"
velocity_frame = "TOPOCENTRIC"
velocity_definition = "radio"
reference_time_policy = "setup_resolve_time"

[recording_groups.co230_12CO_J2_1]
mode = "spectrum"
saved_window_policy = "contiguous_envelope"
streams = ["1LU"]
windows = [
  { window_id = "12CO_J2_1", line_name = "12CO J=2-1", rest_frequency_ghz = 230.538, vmin_kms = -150.0, vmax_kms = 150.0, margin_kms = 10.0 },
]

[recording_groups.co115_12CO_J1_0]
mode = "spectrum"
saved_window_policy = "contiguous_envelope"
streams = ["2LU", "2RU", "3LU", "3RU", "4LU", "4RU", "5LU", "5RU"]
windows = [
  { window_id = "12CO_J1_0", line_name = "12CO J=1-0", rest_frequency_ghz = 115.271, vmin_kms = -150.0, vmax_kms = 150.0, margin_kms = 10.0 },
]

[recording_groups.co110_13CO_C18O_multi_window]
mode = "spectrum"
saved_window_policy = "multi_window"
streams = ["2LL", "2RL", "3RL", "4LL", "4RL", "5LL", "5RL"]
windows = [
  { window_id = "13CO_J1_0", line_name = "13CO J=1-0", rest_frequency_ghz = 110.201353, vmin_kms = -100.0, vmax_kms = 100.0, margin_kms = 10.0 },
  { window_id = "C18O_J1_0", line_name = "C18O J=1-0", rest_frequency_ghz = 109.7821734, vmin_kms = -100.0, vmax_kms = 100.0, margin_kms = 10.0 },
]
```

### 23. 旧config相当ファイル群のREADME

以下は、旧config相当ファイル群に含めたREADMEである。運用時には、このREADMEを観測ディレクトリに置き、どのファイルが何を担うかを確認する。

```markdown
## 12co-NANTEN2-multi_260331TO legacy-config equivalent split setup

Generated: 2026-04-29T10:02:42

This directory is an example split-configuration equivalent of:

`12co-NANTEN2-multi_260331TO.conf3(3).txt`

### Files

- `lo_profile_12co_NANTEN2_multi_260331TO.toml`
  - stream truth, shared frequency axis, shared LO chains.
  - `rest_frequency_ghz` is stream-level, not frequency-axis-level.
  - `lo1_ghz` and `lo2_mhz` are used to avoid large Hz literals.
- `beam_model_12co_NANTEN2_multi_260331TO.toml`
  - standalone beam geometry for B01--B05 plus a zero B00 fallback.
- `recording_window_12co_NANTEN2_multi_260331TO.toml`
  - 230/115 GHz: VLSRK -150..+150 km/s with 10 km/s margin.
  - 110 GHz: `multi_window`, separate 13CO and C18O recorded products, each -100..+100 km/s with 10 km/s margin.
- `analysis_stream_selection_12co_NANTEN2_multi_260331TO.toml`
  - legacy `enabled`, `use_for_convert`, `use_for_sunscan`, `use_for_fit`, `beam_fit_use`.
- `converter_analysis_12co_NANTEN2_multi_260331TO.toml`
  - converter timing, DB, encoder, meteorology, chopper-related analysis parameters from `[global]`.
- `sunscan_analysis_12co_NANTEN2_multi_260331TO.toml`
  - sunscan chopper/ripple/edge/trim parameters from `[global]`.
- `example_obs_parameters_fragment_12co_NANTEN2_multi_260331TO.obs`
  - snippet to paste into the `.obs` `[parameters]` section.

### Stream grouping

The source streams are grouped by common line/LO settings:

- `nanten2_xffts_12co115`: 8 streams, default `rest_frequency_ghz = 115.271`, LO2 = 9500 MHz.
- `nanten2_xffts_110ghz`: 7 streams, default `rest_frequency_ghz = 110.201353`, LO2 = 4000 MHz.
- `nanten2_xffts_12co230`: 1 stream, default `rest_frequency_ghz = 230.538`, LO1 = 225.635994 GHz, LO2 = 4500 MHz.

All groups share:

```toml
frequency_axis_id = "xffts_2GHz_32768ch"
```

### Frequency-axis convention

The legacy configuration used:

```toml
definition_mode = "band_start_stop"
nchan = 32768
band_start_hz = 0.0
band_stop_hz = 2000000000.0
channel_origin = "center"
reverse = false
```

Because `channel_origin = "center"`, the frequency step is:

```text
delta_hz = (band_stop_hz - band_start_hz) / (nchan - 1)
         = 2000000000.0 / 32767
         = 61037.01895199438 Hz
```

This is **not** `2000000000.0 / 32768`.

### Multi-window naming

For 110 GHz streams, the recording setup creates separate recorded products:

```text
2LL__13CO_J1_0  -> xffts-board2__13CO_J1_0
2LL__C18O_J1_0  -> xffts-board2__C18O_J1_0
...
```

Use converter selection as:

```bash
--stream-id 2LL --window-id 13CO_J1_0
```

or directly:

```bash
--recorded-stream-id 2LL__13CO_J1_0
```

### Counts

- source streams: 16
- 230 GHz streams: 1
- 115 GHz streams: 8
- 110 GHz source streams: 7
- 110 GHz recorded products: 14
- expected total recorded products: 23

### update12 note: VLSRK reference direction

The recording window file uses `velocity_frame = "LSRK"`.  In update12 this is fail-closed:
the resolver must receive a velocity-reference direction, site, and reference time.  During
normal NECST observation flow these are supplied from `[parameters]` and/or the parsed
`.obs` reference coordinate.  For standalone validation, either provide explicit
`velocity_reference_l_deg` / `velocity_reference_b_deg` / site parameters, or change
`velocity_frame` to `"TOPOCENTRIC"` only when an explicitly topocentric approximate
window is intended.
```

### 24. 旧config相当ファイル群の検証サマリ

```json
{
  "source_config": "/mnt/data/12co-NANTEN2-multi_260331TO.conf3(3).txt",
  "generated_at": "2026-04-29T10:02:42",
  "source_stream_count": 16,
  "groups": {
    "nanten2_xffts_12co115": [
      "2LU",
      "2RU",
      "3LU",
      "3RU",
      "4LU",
      "4RU",
      "5LU",
      "5RU"
    ],
    "nanten2_xffts_110ghz": [
      "2LL",
      "2RL",
      "3RL",
      "4LL",
      "4RL",
      "5LL",
      "5RL"
    ],
    "nanten2_xffts_12co230": [
      "1LU"
    ]
  },
  "beam_ids": [
    "B01",
    "B02",
    "B03",
    "B04",
    "B05"
  ],
  "lo_chain_ids": [
    "rx115_110_usb_usb",
    "rx115_12co_usb_usb",
    "rx230_12co_usb_usb"
  ],
  "recording": {
    "co230": {
      "streams": [
        "1LU"
      ],
      "vmin_kms": -150.0,
      "vmax_kms": 150.0,
      "margin_kms": 10.0,
      "rest_frequency_ghz": 230.538
    },
    "co115": {
      "streams": [
        "2LU",
        "2RU",
        "3LU",
        "3RU",
        "4LU",
        "4RU",
        "5LU",
        "5RU"
      ],
      "vmin_kms": -150.0,
      "vmax_kms": 150.0,
      "margin_kms": 10.0,
      "rest_frequency_ghz": 115.271
    },
    "co110_multi_window": {
      "streams": [
        "2LL",
        "2RL",
        "3RL",
        "4LL",
        "4RL",
        "5LL",
        "5RL"
      ],
      "vmin_kms": -100.0,
      "vmax_kms": 100.0,
      "margin_kms": 10.0,
      "windows": [
        {
          "window_id": "13CO_J1_0",
          "rest_frequency_ghz": 110.201353
        },
        {
          "window_id": "C18O_J1_0",
          "rest_frequency_ghz": 109.7821734
        }
      ]
    }
  },
  "expected_recorded_products": 23,
  "frequency_axis": {
    "definition_mode": "band_start_stop",
    "nchan": 32768,
    "band_start_hz": 0.0,
    "band_stop_hz": 2000000000.0,
    "channel_origin": "center",
    "reverse": false,
    "if_freq_step_hz": 61037.01895199438
  },
  "toml_parse_results": {
    "lo_profile_12co_NANTEN2_multi_260331TO.toml": "OK",
    "beam_model_12co_NANTEN2_multi_260331TO.toml": "OK",
    "recording_window_12co_NANTEN2_multi_260331TO.toml": "OK",
    "analysis_stream_selection_12co_NANTEN2_multi_260331TO.toml": "OK",
    "converter_analysis_12co_NANTEN2_multi_260331TO.toml": "OK",
    "sunscan_analysis_12co_NANTEN2_multi_260331TO.toml": "OK"
  },
  "update12_notes": {
    "vlsrk_fail_closed": true,
    "requires_velocity_reference_context_for_lsrk": true,
    "explicit_obs_fragment_fields_added_as_comments": true
  }
}
```

### 25. 仕様上の推奨値

| 項目 | 推奨 |
|---|---|
| 本観測のpointing reference | `pointing_reference_beam_policy = "exact"` |
| 本観測のvelocity frame | `LSRK` または `VLSRK`。参照情報不足ならエラー |
| 試験用近似window | `velocity_frame = "TOPOCENTRIC"` を明示 |
| 110 GHzの13CO/C18O | 保存量重視なら `multi_window`、安全第一なら `contiguous_envelope` |
| 旧config互換周波数軸 | `definition_mode = "band_start_stop"` |
| `channel_origin=center` | `N-1` 分割 |
| `channel_origin=edge` | `N` 分割 |
| `rest_frequency` | streamまたはwindowに置く |
| `frequency_axis` | 分光計・IF軸だけを書く |
| converter | snapshot + `converter_analysis.toml` |
| sunscan extract | snapshot + `sunscan_analysis.toml` + `analysis_stream_selection.toml` |

### 26. 本文中で特に誤解しやすい境界

#### 26.1 観測時保存windowとconverter出力slice

観測時の `recording_window_setup.toml` は、DBへ保存する範囲を決める。これはfull channelに対する指定である。converterの `--channel-slice` は保存済みlocal channelに対する指定である。両者は同じ番号空間ではない。

#### 26.2 source streamとrecorded product

`stream_id="2LL"` はsource streamであり、multi-window保存後には `2LL__13CO_J1_0` のようなrecorded productが保存される。converterでline別に処理するときはrecorded productを選ぶ。

#### 26.3 rest frequencyとfrequency axis

周波数軸は分光計のchannelからIF周波数への対応である。rest frequencyは、そのstream/productをどの輝線として速度変換するかである。110 GHzのように同じ分光計軸で複数lineを扱う場合、frequency axisは共通で、windowごとにrest frequencyを持てる。

#### 26.4 beam geometryとpointing reference

`beam_model.toml` はbeam geometryである。`pointing_reference_beam_id` はその中の1つを観測制御の基準に使うだけである。後処理用に他beamのgeometryを更新しても、pointing reference beam自体が一致していれば観測時boresight再現性は保たれる。

#### 26.5 target名とVLSRK

通常の観測制御でtarget名を使うことと、velocity window resolverが任意名をネットワーク解決することは別である。velocity window resolverは明示座標、`.obs` reference、太陽系天体名だけをdeterministicに扱う。


---

### 25. update12 リアルタイム高速化と副作用確認

update12では、分光データ取得後からDB appendまでのリアルタイム経路を再確認し、以下を修正した。

```text
streams_for_raw:
  callbackごとの全stream copy / 全走査を避ける。
  setup apply時に raw input -> recorded products indexを作る。

spectrum_extra_chunk:
  rowごとの静的metadata生成を避ける。
  setup apply時に事前生成したmetadata chunkを使う。

NECSTDBWriter.append:
  validation時の一時list生成を避ける。
  1回のfor loopで検査する。
```

副作用防止のため、以下をsimulationで確認する。

```text
stream truthが変わらないこと
slice結果が変わらないこと
static metadata chunkが従来の動的生成結果と一致すること
streams_for_rawの順序が維持されること
read-only mappingを誤って変更できないこと
multi-windowでraw streamから複数recorded productへ正しく分岐すること
```

性能上の注意:

```text
multi_windowは保存量を減らせるが、append先product数は増える。
高cadenceでwriterが追いつかない場合は、contiguous_envelopeの方が軽いことがある。
初回rowではDB table/file作成が集中する可能性がある。
将来はgate open時のtable pre-createやwriter queue監視を入れるとよい。
```

---

### 26. 現行仕様でobsolete扱いする情報

以下は互換のため読める場合があっても、現行標準仕様としては推奨しない。

```text
restfreqをfrequency_axesに置く
multi-windowでdb_stream_name自体を分岐後名にする
LSRK指定時にtopocentric近似へwarningだけでfallbackする
snapshotなしで観測時slice済みDBをconverterへ読ませる
sunscan fitで旧all-in-one spectrometer_configだけを更新する
converter_analysis.toml / sunscan_analysis.toml を stream truth の置き場として使う
```

現行標準では、stream truthは `lo_profile.toml` とsnapshot、解析条件は `converter_analysis.toml` / `sunscan_analysis.toml` に分離する。

---

### 付録A. update12 実機前preflight確認項目

update12を実機へ入れる前に、最低限次を確認する。

```text
1. zip展開・適用
   元ZIP群へ update12 全変更ZIPまたは update11 + u12p.zip を適用する。
   変更対象以外を誤って上書きしない。

2. Python構文
   変更Pythonファイルに対して py_compile を通す。

3. ROS2 build
   colcon build を実機環境で実行する。
   service/msg生成が通ることを確認する。

4. SG設定
   necst-lo-profile summary/apply/verify を実SGで実行する。
   sg_id が config.toml の signal_generator device id と一致していることを確認する。

5. snapshot resolve
   lo_profile / recording_window_setup / beam_model から snapshot を生成し、
   validateを通す。

6. VLSRK
   velocity_frame="LSRK" を使う場合、Astropy import、site、参照座標、基準時刻が揃っていることを確認する。
   参照情報不足でTOPOCENTRICへ黙ってfallbackしないことを確認する。

7. recording
   短時間の実XFFTS recordingで、
   sidecar保存、gate open/close、recorded product生成、DB table名を確認する。

8. converter
   snapshot指定でconverterを実行し、multi-windowでは
   --recorded-stream-id または --stream-id + --window-id で目的lineを選べることを確認する。

9. sunscan
   snapshot + sunscan_analysis + analysis_stream_selection でextractし、
   beam_model出力が新standalone beam_model.tomlとして読めることを確認する。
```

### 付録B. update12 高速化の確認と注意

分光計データ取得後の高頻度経路では、余分なobject生成や全stream走査が遅延要因になる。update12では次を確認・修正している。

```text
変更点:
  _raw_index_cache は raw input -> tuple(recorded stream mappings) とする。
  streams_for_raw() は呼び出しごとにlistを新規生成しない。
  static spectrum metadata chunkはsetup apply時に事前生成する。
  writer validationでは不要な一時list生成を避ける。

期待される効果:
  multi-windowでrecorded product数が増えても、
  callbackごとの全stream走査やlist生成を避けられる。

副作用確認:
  raw input -> recorded product の対応
  multi-window時のstream順序
  slice結果
  static metadata chunk
  unknown raw boardの扱い
  read-only性
```

ただし、multi-windowは1 raw inputから複数recorded productsへ書くため、保存量は減ってもappend回数やDB table/file数は増える。高cadence観測では、必要に応じて `contiguous_envelope` の方が軽い場合がある。実機ではwriter queue、append latency、最初のrowでのtable作成時間を必ず確認する。

### 付録C. manual更新時の禁止事項

この詳細版は運用・実装双方の正本に近い扱いをする。今後更新する場合は、以下を守る。

```text
1. 章を整理してよいが、パラメータ表・cookbook・注意事項を削らない。
2. 旧config相当例は、少なくともNANTEN2/XFFTS 16 stream構成を保持する。
3. SG apply/verify、config.tomlとlo_profile.tomlの分担を削らない。
4. streamごとのLO、frequency_axis、rest_frequency、beam_idを第一級の仕様として残す。
5. multi-windowの source stream / recorded product / db_stream_name の区別を崩さない。
6. VLSRKの参照座標・site・時刻・Astropy依存を曖昧にしない。
7. `center_beam_id` と `pointing_reference_beam_id` を混同しない。
```

---

### 27. update13 snapshot自動検出の実装確認結果

#### 27.1 確認した入力・出力・優先順位

入力は次である。

```text
RAWDATA_DIR:
  NECST RawData/DB directory

CLI explicit snapshot:
  --spectral-recording-snapshot path/to/spectral_recording_snapshot.toml

CLI explicit legacy config:
  --spectrometer-config path/to/spectrometer_config.toml

解析overlay:
  --converter-analysis-config
  --sunscan-analysis-config
  --analysis-stream-selection
```

出力は、converterではSDFITS、sunscanでは summary CSV / plots / manifest / config snapshot である。snapshot自動検出は、観測時truthを読むための入力選択だけを変更し、解析overlayの意味は変えない。

実装上の優先順位は次である。

```text
converter:
  explicit snapshot and explicit spectrometer_config together -> error
  explicit snapshot -> snapshot_adapter
  explicit spectrometer_config -> legacy loader or adapter loader
  no explicit config -> RAWDATA_DIR sidecar discovery
  sidecar found -> snapshot_adapter
  no spectral setup sidecar and legacy DB -> legacy single-stream fallback
  incomplete new DB -> error

sunscan extract/singlebeam:
  explicit snapshot and explicit spectrometer_config together -> error
  explicit snapshot -> snapshot_adapter
  explicit spectrometer_config -> legacy loader or adapter loader
  no explicit config -> RAWDATA_DIR sidecar discovery
  sidecar found -> snapshot_adapter
  no spectral setup sidecar and legacy DB -> explicit config required
  incomplete new DB -> error
```

#### 27.2 直接確認した実装箇所

```text
converter:
  sd-radio-spectral-fits-main/src/tools/necst/necst_v4_sdfits_converter.py
  _discover_default_spectral_recording_snapshot()
  main() の config load 前

sunscan共通:
  sd-radio-spectral-fits-main/src/tools/necst/multibeam_beam_measurement/config_io.py
  discover_default_spectral_recording_snapshot()

sunscan extract:
  sd-radio-spectral-fits-main/src/tools/necst/multibeam_beam_measurement/sunscan_extract_multibeam.py
  config_from_args()
  run_extract()

sunscan singlebeam:
  sd-radio-spectral-fits-main/src/tools/necst/multibeam_beam_measurement/sunscan_singlebeam.py
  config_from_args()

sunscan wrapper/API:
  sd-radio-spectral-fits-main/src/tools/necst/multibeam_beam_measurement/sunscan_multibeam.py
  public_api.py
```

#### 27.3 シミュレーションで確認した異常系

Python上で一時RawData風ディレクトリを作成し、DB sidecar探索だけを直接確認した。

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

#### 27.4 注意点

単一 `RAWDATA_DIR` の通常運用については、converter、sunscan extract、sunscan singlebeamの自動検出経路は接続済みである。

一方、sunscanの複数 `RAWDATA_DIR` 入力では、同じ観測設定のDB群をまとめて扱う運用を前提にする。異なるsnapshotを持つDBを混ぜる場合は、DBごとに分けて実行するか、共通の明示 config/snapshot を指定する。これは、multi-run集約では解析条件・stream選択・出力表の意味を揃える必要があるためである。



---

### update13 priority 1-5: snapshot自動検出まわりの追加改良

この節は、update13 review後に実装した優先改良候補 1-5 を反映する追補である。既存の仕様を置き換えるものではなく、`spectral_recording_snapshot.toml` の自動検出を、複数RawData、dry-run、provenance、異常系、回帰試験の観点で補強する。

#### 1. sunscan multibeam extract の複数RawData入力

対象コマンドは以下である。

```bash
python -m tools.necst.multibeam_beam_measurement.sunscan_multibeam extract \
  RAWDATA_001 RAWDATA_002 RAWDATA_003 \
  --sunscan-analysis-config sunscan_analysis.toml
```

明示的な `--spectral-recording-snapshot` も `--spectrometer-config` も指定しない場合、複数RawData入力では、最初のRawDataで見つかったsnapshotを全runに使い回してはいけない。update13 priority 1-5では、この点を修正し、`run_extract_many()` から各 `run_extract()` へ `spectral_recording_snapshot=None` を渡すことで、各RawDataの内部で独立に自動検出を行う。

つまり、論理的な挙動は以下である。

```text
RAWDATA_001 -> RAWDATA_001 内の spectral_recording_snapshot.toml を自動検出
RAWDATA_002 -> RAWDATA_002 内の spectral_recording_snapshot.toml を自動検出
RAWDATA_003 -> RAWDATA_003 内の spectral_recording_snapshot.toml を自動検出
```

明示的に

```bash
--spectral-recording-snapshot path/to/spectral_recording_snapshot.toml
```

を指定した場合は、従来どおりその1つのsnapshotを全入力に対して使う。これは「複数RawDataを同一の明示設定で解析する」という明確な運用であり、自動検出とは区別される。

#### 2. sidecar discovery dry-run

自動検出が何を見ているかを、変換・解析を実行せずに確認するためのdry-run経路を追加した。

converter:

```bash
python -m tools.necst.necst_v4_sdfits_converter RAWDATA_DIR --inspect-sidecars
```

sunscan multibeam:

```bash
python -m tools.necst.multibeam_beam_measurement.sunscan_multibeam inspect-sidecars \
  RAWDATA_001 RAWDATA_002
```

または extract subcommand のオプションとして

```bash
python -m tools.necst.multibeam_beam_measurement.sunscan_multibeam extract \
  RAWDATA_001 RAWDATA_002 \
  --inspect-sidecars
```

sunscan singlebeam:

```bash
python -m tools.necst.multibeam_beam_measurement.sunscan_singlebeam RAWDATA_DIR --inspect-sidecars
```

dry-run出力はJSONで、主な項目は以下である。

```text
db_dir
snapshot_path
lo_profile_path
recording_window_setup_path
beam_model_path
pointing_param_path
obs_paths
config_paths
extra_toml_paths
duplicate_canonical_paths
warnings
status
layout_kind
has_snapshot
has_new_sidecars
recommended_action
```

`status="ok"` で `layout_kind="NEW_DB_WITH_SNAPSHOT"` の場合は、明示configなしの通常実行でsnapshot自動検出できる。`layout_kind="LEGACY_DB"` の場合は、新snapshot sidecarは無く、converterではlegacy fallbackが許される。`layout_kind="INCOMPLETE_NEW_DB"` または `layout_kind="DUPLICATE_CANONICAL_SIDECARS"` の場合は、自動選択せずエラー扱いにする。

#### 3. 実際に使った設定源のmanifest記録

converterでは、出力FITSと同じprefixで以下のJSON manifestを作成する。

```text
OUTPUT.fits.config_manifest.json
```

このJSONには以下が入る。

```text
rawdata_path
output_fits
manifest_path
config_source_kind
config_source_path
config_loader
explicit
explicit_spectrometer_config
explicit_spectral_recording_snapshot
auto_detected
beam_model_path
allow_beam_model_override
converter_analysis_config
analysis_stream_selection
config_name
provenance
```

また、FITS HISTORYにも以下を追加する。

```text
config_source_kind
config_source_path
config_source_auto
config_source_explicit
config_manifest
```

sunscan multibeam extractでは、stream manifest CSV、run table CSV、analysis config snapshot JSONに設定源情報を記録する。代表的な列は以下である。

```text
config_source_kind
config_source_path
config_loader
config_source_explicit
config_source_auto_detected
explicit_spectrometer_config
explicit_spectral_recording_snapshot
beam_model_path
allow_beam_model_override
sunscan_analysis_config
analysis_stream_selection
```

これにより、後から「この解析がDB内snapshotを自動検出したのか」「明示configを使ったのか」「どのsnapshot pathを読んだのか」を確認できる。

#### 4. duplicate snapshot時の扱い

1つのRawData/DB内に複数の `spectral_recording_snapshot.toml` が見つかった場合、converter/sunscanは自動選択しない。候補をソートして先頭を選ぶことは、観測時truthを取り違える危険があるため禁止する。

dry-runでは以下のような情報を出す。

```text
status = "error"
layout_kind = "DUPLICATE_CANONICAL_SIDECARS"
duplicate_canonical_paths.snapshot = [
  ".../metadata/spectral_recording_snapshot.toml",
  ".../backup/spectral_recording_snapshot.toml"
]
recommended_action = "keep exactly one ... or pass --spectral-recording-snapshot explicitly"
```

通常実行では、重複検出時に例外を出して停止する。対処は以下のどちらかである。

```text
1. RawData/DB内に残す canonical snapshot を1つだけにする。
2. 意図したsnapshotを --spectral-recording-snapshot で明示指定する。
```

#### 5. 回帰試験

追加した軽量回帰試験は以下である。

```text
sd-radio-spectral-fits-main/tests/test_necst_update13_priority1_5.py
```

確認内容は以下である。

```text
- duplicate snapshot dry-runが候補pathとrecommended_actionを返すこと。
- sunscan multibeamの複数RawData実行で、暗黙snapshotが最初のDBに固定されず、各runへ None として渡されること。
- run tableに config_source_path / config_source_auto_detected が残ること。
- config_from_args が複数RawData入力時に最初のDBのsnapshotを args.spectral_recording_snapshot へ勝手に束縛しないこと。
```

#### singlebeam複数RawDataについての注意

`sunscan_singlebeam` は、構造上「1つのresolved analysis configを複数runへ適用する」古い運用を強く引きずっている。したがって、DBごとに異なるsnapshot truthを自動で使い分けたい場合は、原則として `sunscan_multibeam extract` を使う。

`sunscan_singlebeam` には `--inspect-sidecars` を追加しているので、解析前に各DBの状態を確認できる。ただし、複数RawDataでDBごとに異なるstream truthを使う用途では、singlebeamを1回でまとめず、DBごとに個別実行するか、multibeam extractを使用する。



---

### 追補 update13 p16: 分光計データ取得ホットパスの高速化と安全性確認

p16では、snapshot自動検出とは別に、分光計データ取得からNECSTDB書き込みまでのホットパスを見直した。目的は、XFFTS/AC240 driver thread側のqueueを詰まらせず、次の分光計データを取りこぼさないようにすることである。

主な変更は以下である。

```text
1. SpectralData.fetch_data()
   - ある分光計queueが空でも、そこで関数全体をreturnしない。
   - 後続分光計queueを必ず確認する。
   - queue-backed deviceではget_nowait()で非blocking取得する。
   - 1 timer tickで最大8 packetまでdrainする。

2. SpectralData.get_data()
   - 全分光計の内部queueが揃う前に一部だけ消費しない。
   - partial consumeによる内部同期崩れを避ける。

3. active snapshot spectrum branch
   - rowごとのROS Spectral message生成とfield introspectionを避ける。
   - spectrum_chunk_for_stream()でNECSTDB chunkを直接作る。
   - base column orderはnecst_msgs/Spectralに合わせる。

4. append path
   - setup apply時に_runtime_db_append_pathをprecomputeする。
   - rowごとのnamespace_db_path()再計算を避ける。

5. NECSTDBWriter
   - background threadで最大64 chunkまでburst drainする。
   - table liveliness scanを1秒間隔に抑える。
   - append validationの一時set生成を避ける。
```

疑似シミュレーションでは、以下を確認した。

```text
- 先頭分光計queueが空でも、後続分光計queueを読み出す。
- 全queueが揃わない場合、内部queueを部分消費しない。
- single spectrometerの通常取得は従来通り動く。
- active multi-windowでslice結果とappend pathが正しい。
- spectrum direct chunkをNECSTDBWriterへ渡したときのheader schemaが期待通り。
```

詳細は `docs/update13_realtime_hotpath_performance_report_2026-04-30.md` を参照すること。

---

### update13 p16g: XFFTS timestamp診断とconverter時刻解釈override

p16gでは、IRIG-B投入後のXFFTS timestampを安全に検証するため、converterへ時刻診断モードと明示的な時刻解釈overrideを追加した。

#### 追加CLI

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

---

# 2026-05-04 追補: SD FITS周波数・速度変換と1 channel境界確認


この追補は、2026-05-03までの累積manualに対し、SD FITS出力時の周波数軸・速度窓・RESTFREQ/VELDEF伝播を追加確認した結果を統合する。

## 追加確認した定義

- `IF(ch) = if_freq_at_full_ch0_hz + ch * if_freq_step_hz`
- `sky(ch) = signed_lo_sum_hz + if_frequency_sign * IF(ch)`
- `saved_ch_start:saved_ch_stop` は Python slicing と同じ半開区間である。
- SD FITS出力では保存済み配列の先頭を1-originのWCSへ写し、
  - `CRPIX1 = 1.0`
  - `CRVAL1 = sky(saved_ch_start)`
  - `CDELT1 = if_frequency_sign * if_freq_step_hz`
  - `NCHAN = saved_ch_stop - saved_ch_start`
  とする。

## 追加修正

1. SD FITS writerで、topocentric frequency axisの場合でも、rowごとに有効な `RESTFREQ` がある場合は `RESTFREQ` と `VELDEF` をSINGLE DISH tableへ出力する。
2. converterが実機snapshotを読むとき、beam model optional fieldに空文字 `""` があっても未指定として扱う。
3. 実機snapshotに対して、全6 streamの start/stop境界を再計算し、1 channelずれがないことを確認するテストを追加した。

## 注意

snapshot作成時のLSRK velocity windowは、保存するtopocentric frequency channel範囲を決めるために使う。converterの基本出力はTOPOCENTのFREQ軸であり、LSRK共通速度格子への再gridは後段処理で行う。


## 詳細検証ログ

# update14 SD FITS frequency/velocity validation

Date: 2026-05-04

## Scope

This check uses the cumulative update14 tree as of 2026-05-03 and the real
`spectral_recording_snapshot.toml` from `necst_otf_20260503_114555_test`.

Definitions:

- Full XFFTS IF axis: `IF(ch) = if_freq_at_full_ch0_hz + ch * if_freq_step_hz`.
- Snapshot sky axis: `sky(ch) = signed_lo_sum_hz + if_frequency_sign * IF(ch)`.
- Converter SD FITS WCS for a saved product:
  - `CRPIX1 = 1.0`
  - `CRVAL1 = sky(saved_ch_start)`
  - `CDELT1 = if_frequency_sign * if_freq_step_hz`
  - `NCHAN = saved_nchan = saved_ch_stop - saved_ch_start`
- Radio velocity from a topocentric frequency axis:
  - `v_radio = c * (1 - f / RESTFREQ)`

## Result

The frequency-axis reconstruction is internally consistent for all six saved
products.  The line center lies inside the saved channel range in every case.
The sign of `CDELT1` correctly follows `if_frequency_sign`.

| stream | line | saved_ch | NCHAN | REST GHz | CRVAL1 GHz | last GHz | CDELT1 Hz | line IF GHz | line full ch | topocentric radio velocity range km/s |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `xffts_board1` | 12CO J=2-1 | 18185–21410 | 3225 | 230.538000 | 230.387447737 | 230.633426923 | 76296.273690 | 1.538000 | 20158.258 | 195.779 → -124.094 |
| `xffts_board2__13CO_J2_1` | 13CO J=2-1 | 21037–24120 | 3083 | 220.399000 | 220.255044710 | 220.490189825 | 76296.273690 | 1.749000 | 22923.793 | 195.812 → -124.039 |
| `xffts_board2__C18O_J2_1` | C18O J=2-1 | 10052–13124 | 3072 | 219.560357 | 219.416930143 | 219.651236000 | 76296.273690 | 0.910357 | 11931.867 | 195.838 → -124.088 |
| `xffts_board3` | 12CO J=1-0 | 10241–11853 | 1612 | 115.271000 | 115.318649861 | 115.195736564 | -76296.273690 | 0.829000 | 10865.537 | -123.926 → 195.742 |
| `xffts_board4__13CO_J1_0` | 13CO J=1-0 | 7909–9450 | 1541 | 110.201000 | 110.246572771 | 110.129076510 | -76296.273690 | 0.649000 | 8506.313 | -123.977 → 195.662 |
| `xffts_board4__C18O_J1_0` | C18O J=1-0 | 13401–14936 | 1535 | 109.782182 | 109.827553636 | 109.710515152 | -76296.273690 | 1.067818 | 13995.677 | -123.901 → 195.707 |

The small differences between the saved frequency endpoints and the requested
window edges are below one channel and are expected from integer channel
rounding.

## Finding fixed in this patch

The converter passes row-specific `restfreq_hz`, `crval1_hz`, `cdelt1_hz`,
`crpix1`, `ctype1`, `cunit1`, `specsys`, and `veldef` to the SDFITS writer.
However, the writer only emitted the `RESTFREQ` and `VELDEF` columns when the
dataset had explicit velocity context such as `SPECSYS='LSRK'`, non-`FREQ`
`CTYPE1`, or `VFRAME`.

For snapshot-driven update14 data, the output normally uses a topocentric
frequency axis:

- `CTYPE1 = 'FREQ'`
- `SPECSYS = 'TOPOCENT'`
- `VELDEF = 'RADIO'`
- one row/product may have a different `RESTFREQ`

Therefore the old writer behavior could omit row-level `RESTFREQ` / `VELDEF`
columns and leave only a primary-header default.  That is unsafe for multi-line
outputs because the primary header can represent only one default rest
frequency.

The writer is now changed so that any meaningful row-level `RESTFREQ` activates
line context and emits both `RESTFREQ` and `VELDEF` columns in the SINGLE DISH
table.

## Important interpretation

The converter output is a topocentric frequency-axis SD FITS unless `SPECSYS`
is explicitly changed to `LSRK` and a frequency column is written.  The recording
window was selected using the LSRK velocity request at snapshot creation time,
but the saved data themselves are not resampled to a common LSRK grid by this
converter step.  LSRK coadd/regridding remains a later processing step.


---

# update14 SD FITS frequency/velocity exactness recheck

Date: 2026-05-04

## Scope

This recheck used the integrated tree made from the original zip files plus all update14 patches in this chat, including the SD FITS writer RESTFREQ/VELDEF fix.  The uploaded real snapshot `spectral_recording_snapshot.toml` from `necst_otf_20260503_114555_test` was used as the numerical truth.

Checked items:

1. LO-chain sign convention.
2. LSRK velocity window to topocentric channel range.
3. Inclusive/exclusive channel boundary handling.
4. Runtime spectrum slicing with `saved_ch_start:saved_ch_stop`.
5. Snapshot-to-converter explicit WCS propagation.
6. SD FITS writer row-level `CRVAL1`, `CDELT1`, `CRPIX1`, `RESTFREQ`, `SPECSYS`, `VELDEF` propagation.
7. Converter snapshot loading with embedded beam optional blank fields.

## Definitions

- Full XFFTS channel index: zero-based `ch_full`.
- Saved local channel index: zero-based `ch_local`.
- Saved range: `[saved_ch_start, saved_ch_stop)`, i.e. start inclusive and stop exclusive.
- Local-to-full mapping: `ch_full = saved_ch_start + ch_local`.
- IF center frequency:
  `IF(ch_full) = if_freq_at_full_ch0_hz + ch_full * if_freq_step_hz`.
- Sky frequency:
  `sky(ch_full) = signed_lo_sum_hz + if_frequency_sign * IF(ch_full)`.
- Converter SD FITS local WCS:
  `CRPIX1 = 1`,
  `CRVAL1 = sky(saved_ch_start)`,
  `CDELT1 = if_frequency_sign * if_freq_step_hz`.

## Snapshot constants

- `full_nchan = 32768`
- `if_freq_at_full_ch0_hz = 0.000000`
- `if_freq_step_hz = 76296.273689992988`
- `CTYPE1 = FREQ`
- `CUNIT1 = Hz`
- `SPECSYS = TOPOCENT`
- `VELDEF = RADIO`

## Numerical channel-boundary check

For each velocity window, I recomputed the channel range from the stored `frequency_low_hz` and `frequency_high_hz`, using the same center-in-window rule:

- include a channel if its center frequency is inside `[frequency_low_hz, frequency_high_hz]`;
- output `ch_start = min(included)`;
- output `ch_stop = max(included) + 1`.

Result: all six products reproduce the snapshot `computed_ch_start` and `computed_ch_stop` exactly.  In every case, the first and last saved channels are inside the requested effective velocity/frequency window, while the immediate outside neighbors `start-1` and `stop` are outside.  This rules out a one-channel shift in the snapshot window resolution.

| stream | line | ch_start:ch_stop | N | CDELT1 Hz | v(start) km/s | v(stop-1) km/s | neighbor check |
|---|---|---:|---:|---:|---:|---:|---|
| `xffts_board1` | 12CO J=2-1 | 18185:21410 | 3225 | 76296.273681641 | 159.923665 | -159.986850 | OK: start-1/stop outside |
| `xffts_board2__13CO_J2_1` | 13CO J=2-1 | 21037:24120 | 3083 | 76296.273681641 | 159.956690 | -159.932097 | OK: start-1/stop outside |
| `xffts_board2__C18O_J2_1` | C18O J=2-1 | 10052:13124 | 3072 | 76296.273681641 | 159.983090 | -159.981480 | OK: start-1/stop outside |
| `xffts_board3` | 12CO J=1-0 | 10241:11853 | 1612 | -76296.273681641 | -159.819267 | 159.887245 | OK: start-1/stop outside |
| `xffts_board4__13CO_J1_0` | 13CO J=1-0 | 7909:9450 | 1541 | -76296.273696899 | -159.870168 | 159.806682 | OK: start-1/stop outside |
| `xffts_board4__C18O_J1_0` | C18O J=1-0 | 13401:14936 | 1535 | -76296.273696899 | -159.793871 | 159.852298 | OK: start-1/stop outside |

## Converter WCS propagation check

The snapshot adapter materializes each saved stream as an explicit local WCS.  The local channel 0 WCS is exactly the full-channel `saved_ch_start` frequency, not full channel 0.  The checked WCS values are:

| stream | CRVAL1 Hz | CDELT1 Hz | CRPIX1 | RESTFREQ Hz |
|---|---:|---:|---:|---:|
| `xffts_board1` | 230387447737.052521 | 76296.273681641 | 1.0 | 230538000000.0 |
| `xffts_board2__13CO_J2_1` | 220255044709.616394 | 76296.273681641 | 1.0 | 220399000000.0 |
| `xffts_board2__C18O_J2_1` | 219416930143.131805 | 76296.273681641 | 1.0 | 219560357000.0 |
| `xffts_board3` | 115318649861.140778 | -76296.273681641 | 1.0 | 115271000000.0 |
| `xffts_board4__13CO_J1_0` | 110246572771.385849 | -76296.273696899 | 1.0 | 110201000000.0 |
| `xffts_board4__C18O_J1_0` | 109827553636.280411 | -76296.273696899 | 1.0 | 109782182000.0 |

The last local-channel WCS value differs from the direct full-channel formula by less than 0.03 Hz in the manual double-precision recomputation.  This is only floating-point roundoff at ~1e-13 relative level, far below one channel spacing of 76296.273690 Hz.

## Runtime slicing check

`necst/rx/spectral_recording_runtime.py` slices full spectra as:

```python
sliced = spectral_data[start:stop]
```

where `start = saved_ch_start`, `stop = saved_ch_stop`.  I simulated a full spectrum with `data[ch] = ch` and verified for all six streams:

- saved length equals `saved_ch_stop - saved_ch_start`;
- first saved datum equals `saved_ch_start`;
- last saved datum equals `saved_ch_stop - 1`.

Therefore the recorded data array and the converter WCS local channel 0 refer to the same full XFFTS channel.

## Old converter config consistency

The uploaded legacy converter configs imply:

- 12CO(1-0): `104.5 + 11.6 - IF`;
- 13CO/C18O(1-0): `104.5 + 6.35 - IF`;
- 12CO(2-1): `225.0 + 4.0 + IF`;
- 13CO/C18O(2-1): `225.0 - 6.35 + IF`.

The snapshot LO chains reproduce these formulas:

- `rx100_12co10_usb_lsb`: `signed_lo_sum_hz = 116.1 GHz`, `if_frequency_sign = -1`;
- `rx100_13co_c18o10_usb_lsb`: `signed_lo_sum_hz = 110.85 GHz`, `if_frequency_sign = -1`;
- `rx200_12co21_usb_usb`: `signed_lo_sum_hz = 229.0 GHz`, `if_frequency_sign = +1`;
- `rx200_13co_c18o21_lsb_lsb`: `signed_lo_sum_hz = 218.65 GHz`, `if_frequency_sign = +1`.

## Found and fixed issue unrelated to the arithmetic

During the converter snapshot-load check, I found that the embedded snapshot beam fields use empty strings for optional fields such as `rotation_slope_deg_per_deg` and `pure_rotation_offset_x_el0_arcsec`.  The converter-side beam parser treated `""` as a numeric value and could fail before reaching the spectral conversion stage.

Fix: treat blank strings in optional beam fields as missing values.  This does not change frequency arithmetic, but it is required for the converter to load the actual real snapshot reliably.

Added regression test:

- `tests/test_update14_snapshot_frequency_wcs_exactness.py`

This test checks:

1. embedded beam blank optional fields do not break snapshot bundle loading;
2. `saved_ch_start:saved_ch_stop` materializes to local explicit WCS with no off-by-one shift.

## SD FITS writer status

The previous writer patch is still required.  It makes topocentric FREQ rows emit row-level `RESTFREQ` and `VELDEF` when line context is present.  This is important for a merged SD FITS table containing 12CO/13CO/C18O rows with different rest frequencies.

The added writer test is:

- `tests/test_sdfits_writer_multiline_restfreq_columns.py`

In this environment, astropy is not installed, so the FITS write/read test is skipped here.  The writer and converter files passed `py_compile`, and the no-astropy snapshot/WCS tests passed.

## Final judgement

No one-channel shift was found in the snapshot window resolution, runtime slicing, or converter WCS propagation.  The frequency sign convention is consistent with the old converter configs.  The SD FITS row arguments receive the correct per-stream `CRVAL1`, `CDELT1`, `CRPIX1`, `RESTFREQ`, `SPECSYS`, and `VELDEF`.

Required patch from this recheck:

- converter beam parser blank-optional-field fix;
- regression test for snapshot saved-channel WCS exactness.

---

# 2026-05-04 追補: VELDEF のSDFITS出力表現

## 結論

converter/writer内部の入力意味としては、速度定義は `RADIO`、周波数参照系は `TOPOCENT` である。一方、SDFITS table の `VELDEF` 列へ出力するときは、writer がこれをSDFITS/AIPS系の表現へ正規化し、`RADI-OBS` として保存する。

つまり、次の対応で扱う。

```text
入力・内部意味:
  veldef = "RADIO"
  specsys = "TOPOCENT"

SDFITS出力:
  VELDEF = "RADI-OBS"
  SPECSYS = "TOPOCENT"
```

ここで `RADI-OBS` は、radio velocity definition と observer/topocentric frame を組み合わせたSDFITS互換の表現である。今回のconverter出力は基本的に `CTYPE1 = "FREQ"`, `CUNIT1 = "Hz"`, `SPECSYS = "TOPOCENT"` の周波数軸であり、`VELDEF = "RADI-OBS"` は、`RESTFREQ` と合わせて速度換算する場合の補助情報として使う。

## test修正

`tests/test_sdfits_writer_multiline_restfreq_columns.py` では、当初 `VELDEF = "RADIO"` を期待していたが、これはwriterのSDFITS正規化後の値としては不適切だった。正しくは、row-level `VELDEF` が存在し、その値が `RADI-OBS` になることを確認する。

確認すべき本質は、topocentric FREQ axisであっても、複数lineのSD FITS出力で rowごとの `RESTFREQ` / `VELDEF` が落ちないことである。

---

# 2026-05-04 追補: chopper設定の配置

## 結論

`chopper_wheel`, `chopper_stat`, `chopper_win_sec` は現在の `necst_v4_sdfits_converter.py` では使われない。converter example の `[converter_analysis]` に active key として置くと、「converterがchopper-wheel較正を行う」と誤解されるため、active設定から外す。

現在の役割分担は以下とする。

```text
converter:
  snapshot/db_table_path/WCS/RESTFREQ/VELDEFを使ってSD FITSへ出力する。
  chopper_wheel/chopper_stat/chopper_win_secは使わない。

sunscan:
  vertical-axis signalをTa*化するためにchopper-wheel較正を使う。
  chopper_wheel/chopper_statは [sunscan_analysis] に置く。

chopper_win_sec:
  legacy compatibility option。
  現在の1-load実装はHOT/OFFをscan idまたは連続blockでまとめ、時刻補間する。
  time-window smoothingには使わないため、通常はコメントアウトする。
```

したがって、converter analysis TOMLをsunscanの `[analysis_base]` として参照する場合でも、chopper設定はbase側ではなく `sunscan_analysis.toml` の `[sunscan_analysis]` に置く。

---

# 2026-05-04 追補: examples ディレクトリの整理

## 結論

`docs/examples` は、用途別に以下のディレクトリを正として使う。

```text
docs/examples/NANTEN2_multibeam_260331TO/
docs/examples/OMU1P85M_single115_xx/
```

旧ディレクトリ

```text
docs/examples/old_config_260331TO_update11/
docs/examples/legacy_converter_20260503/
```

は履歴・移行記録として残すが、新しい設定例の参照先にはしない。

## NANTEN2 multibeam

`NANTEN2_multibeam_260331TO/` には、LO profile、recording window、beam model、analysis stream selection、converter analysis、sunscan analysis、obs fragment をまとめる。

converterで特定streamだけを選ぶ場合、`spectral_name` ではなく、CLIの `--recorded-stream-id` または `[analysis_selection] convert_stream_ids` を使う。

## OMU 1.85 m single115

`OMU1P85M_single115_xx/` には、添付された1.85 m 12CO single115 analysis例を整理して入れる。添付converter analysisは、legacy host-time offset `spectrometer_time_offset_sec = -0.070` を保持する意図があるため、active例では `spectral_time_source = "host-time"` とする。modern XFFTS/IRIG-B timestampを使う場合だけ、`auto` または `xffts-timestamp` へ変更する。

sunscan analysisは `[analysis_base]` でconverter analysisからdb/telescope/table/weather/timing設定を継承する。ただし、chopper-wheel設定はconverterでは使わないため、`[sunscan_analysis]` 側に置く。

## chopper設定

converter本体は `chopper_wheel`, `chopper_stat`, `chopper_win_sec` を使わない。これらはsunscanでTa*化が必要な場合に `[sunscan_analysis]` に置く。

---

# 2026-05-04 追補: spectral_time_source と関連パラメータ

## 推奨値

現行examplesでは、通常の既定値を次のようにする。

```toml
[spectral_time]
spectral_time_source = "auto"
xffts_timestamp_scale = "auto"
xffts_gps_suffix_means = "utc"
spectrometer_time_offset_sec = ...
```

## `spectral_time_source`

取り得る値は以下である。

```text
"auto"
  time_spectrometer 由来の信頼できるXFFTS timestampを優先する。
  使えない場合だけ host-time にfallbackする。

"xffts-timestamp"
  XFFTS timestampを必須にする。
  timestampが使えない場合はhost-timeへfallbackせず、エラーにする。

"host-time"
  XFFTS header受信直後に付けたhost-side received_timeを使う。
  legacy host-time reductionやtimestamp異常時の明示fallbackに使う。
```

## `spectrometer_time_offset_sec`

この値は、選ばれたspectral timeがhost-timeのときだけ適用する。

```text
適用される:
  spectral_time_source = "host-time"
  spectral_time_source = "auto" かつ auto が host-time にfallbackした場合

適用されない:
  spectral_time_source = "xffts-timestamp"
  spectral_time_source = "auto" かつ auto が XFFTS timestamp を選んだ場合
```

したがって、`auto`運用でXFFTS/IRIG-B timestampが正常なデータでは、legacy host-time offset値を設定ファイルに残していても、解析時刻には加算されない。

## `xffts_timestamp_scale`

取り得る値は以下である。

```text
"auto"
  XFFTS timestamp文字列のsuffixから解釈する。

"utc"
  UTCとして解釈する。

"gps"
  GPS timeとして解釈する。

"tai"
  TAIとして解釈する。
```

## `xffts_gps_suffix_means`

取り得る値は以下である。

```text
"utc"
  literal suffixがGPSでも、実体はUTC相当として扱う。
  OMU/XFFTS運用ではこれを推奨する。

"gps"
  literal suffixを真のGPS timeとして扱う。
```

## `encoder_time`

`encoder_shift_sec`, `encoder_az_time_offset_sec`, `encoder_el_time_offset_sec` は秒単位のencoder時刻補正であり、`spectral_time_source`とは独立である。`encoder_shift_sec` はAz/El共通、`encoder_az_time_offset_sec` と `encoder_el_time_offset_sec` は軸別の追加補正である。

---

# 2026-05-12 追補: OMU 1.85 m 4-board current setup example

## 追加した現行example

現在のOMU 1.85 m 4-board setupを、以下に整理して追加した。

```text
docs/examples/OMU1P85M_4board_12co_13co_c18o_current/
```

含まれる主なファイルは以下である。

```text
lo_profile_OMU1P85M_4board_12co_13co_c18o_current.toml
recording_window_setup_OMU1P85M_4board_12co_13co_c18o_current.toml
parameter_update_report.md
```

## SDFITS番号付け

100 GHz帯と200 GHz帯は準光学フィルターでビームが分かれているため、`fdnum` はboardごとではなく周波数pathごとに割り当てる。

```text
200 GHz path: fdnum = 1
100 GHz path: fdnum = 2
```

`ifnum` は4つのXFFTS boardで重ならないように割り当てる。

```text
xffts_board1: fdnum=1, ifnum=0, plnum=0
xffts_board2: fdnum=1, ifnum=1, plnum=0
xffts_board3: fdnum=2, ifnum=2, plnum=0
xffts_board4: fdnum=2, ifnum=3, plnum=0
```

`plnum` は現行setupでは全streamで `0` とする。

## 200 GHz first local

200 GHz帯 first local は software controlled SG ではなく fixed LO として扱う。

```toml
[lo_roles.band6_1st]
source = "fixed"
fixed_lo_frequency_ghz = 225.0
```

ハードウェア内部では18.75 GHzの12逓倍で生成される可能性があるが、この `lo_profile.toml` ではその発振器を直接制御しない。

---

# 2026-05-12 追補: multi_window product別 SDFITS番号override

## 問題

`multi_window` は1つのsource streamを複数のsaved productへ分割する。従来はsource streamの `fdnum/ifnum/plnum` を全productが継承していたため、同じXFFTS boardから切り出した13CO/C18Oのようなproductに、異なる `ifnum` を割り当てられなかった。

## 修正

`recording_groups.*.windows[]` の各windowに、任意で以下を書けるようにした。

```toml
fdnum = ...
ifnum = ...
plnum = ...
```

これらはsaved product streamにだけ適用され、source streamのmetadataは変更しない。

## OMU 1.85 m current setup

現行4-board exampleでは、6つの保存productに対して以下を割り当てる。

```text
12CO J=2-1:  fdnum=1, ifnum=0, plnum=0
13CO J=2-1:  fdnum=1, ifnum=1, plnum=0
C18O J=2-1:  fdnum=1, ifnum=2, plnum=0
12CO J=1-0:  fdnum=2, ifnum=3, plnum=0
13CO J=1-0:  fdnum=2, ifnum=4, plnum=0
C18O J=1-0:  fdnum=2, ifnum=5, plnum=0
```

---

# 2026-05-12 追補: IFNUM local numbering, all-products multi_window, and adopted rest frequencies

## FDNUM/IFNUM/PLNUM 方針の更新

SDFITS上のIF番号は、`FDNUM`ごとに独立して数える方針に更新した。したがって、現行OMU 1.85 m 4-board setupでは以下のproduct番号を使う。

```text
200 GHz path:
  12CO J=2-1   fdnum=1, ifnum=0, plnum=0
  13CO J=2-1   fdnum=1, ifnum=1, plnum=0
  C18O J=2-1   fdnum=1, ifnum=2, plnum=0

100 GHz path:
  12CO J=1-0   fdnum=2, ifnum=0, plnum=0
  13CO J=1-0   fdnum=2, ifnum=1, plnum=0
  C18O J=1-0   fdnum=2, ifnum=2, plnum=0
```

つまり、`IFNUM` はglobal uniqueではなく、`(FDNUM, IFNUM, PLNUM)` の組でproductを識別する。

## `saved_window_policy`

single-line boardでも、`window_id` をrecorded stream名とDB table pathへ反映させるため、現行OMU 1.85 m 4-board exampleでは全recording groupを

```toml
saved_window_policy = "multi_window"
```

に揃えた。これにより、12CO単独boardも以下のようなproduct名になる。

```text
xffts_board1__12CO_J2_1
xffts_board3__12CO_J1_0
```

従来の `contiguous_envelope` は、複数windowを1つの連続channel範囲としてまとめる用途には有効である。ただし、line/window名をsaved product名へ明示的に反映したい場合は、single windowでも `multi_window` を使う方が一貫する。

## 採用静止周波数

現行OMU 1.85 m 4-board exampleでは以下を採用する。

```text
12CO J=1-0   115.2712018 GHz
12CO J=2-1   230.5380000 GHz
13CO J=1-0   110.2013543 GHz
13CO J=2-1   220.3986842 GHz
C18O J=1-0   109.7821734 GHz
C18O J=2-1   219.5603541 GHz
```

---

# 2026-05-12 追補: OMU 1.85 m examples統合とNANTEN2 recording override更新

## OMU 1.85 m examples統合

`OMU1P85M_single115_xx/` と `OMU1P85M_4board_12co_13co_c18o_current/` は内容が重複していたため、現行exampleを以下の1ディレクトリへ統合した。

```text
docs/examples/OMU1P85M_4board_12co_13co_c18o_current/
```

旧 `OMU1P85M_single115_xx/` は削除対象である。既存checkoutへ上書き適用する場合は、手動で以下を削除する。

```bash
rm -rf docs/examples/OMU1P85M_single115_xx
```

旧single115側にあったanalysis TOMLは、以下の名前でcurrent 4-board directoryへ移動した。

```text
converter_analysis_OMU1P85M_4board_12co_13co_c18o_current.toml
sunscan_analysis_OMU1P85M_4board_12co_13co_c18o_current.toml
```

`sunscan_analysis` の `[analysis_base]` は、統合後のconverter analysis fileを参照する。

## NANTEN2 recording override

NANTEN2 multibeam exampleでも、recording window側にwindow-level `ifnum` override例を追加した。ただしNANTEN2は同じrecording group内に複数beam/polを含むため、`fdnum` と `plnum` はsource streamから継承する。window側で固定してはいけない。

```toml
# 12CO windows
ifnum = 0

# 13CO window
ifnum = 1

# C18O window
ifnum = 2
```

NANTEN2 recording exampleも `saved_window_policy = "multi_window"` に揃え、window_idがrecorded product名に反映されるようにした。

---

# 2026-05-12 追補: Sun/sunscan TP recording examples

## 方針

太陽のsunscan観測は連続波TPとして扱う。したがって、13CO/C18Oなどのline windowを速度で切り出す必要はない。各XFFTS streamごとに、スプリアスを避けたchannel範囲を指定してTP値を保存する。

推奨するrecording setupは以下である。

```toml
[defaults]
mode = "tp"
saved_window_policy = "channel"
tp_stat = "sum_mean"
```

`tp_stat = "sum_mean"` はruntimeが `tp_sum` と `tp_mean` の両方を保存することを示すラベルである。sunscan解析では通常 `tp_mean` を1-channel信号として読む。

## channel範囲

exampleでは、32768 channel bandの中央半分を使う。

```toml
saved_ch_start = 8192
saved_ch_stop = 24576
```

`start:stop` はPython sliceと同じ半開区間である。既知のspuriousがこの範囲にある場合は、観測前にchannel範囲を変更する。

## 追加したexample

```text
docs/examples/OMU1P85M_4board_12co_13co_c18o_current/
  recording_window_setup_sun_tp_OMU1P85M_4board_current.toml
  sunscan_analysis_sun_tp_OMU1P85M_4board_current.toml

docs/examples/NANTEN2_multibeam_260331TO/
  recording_window_sun_tp_NANTEN2_multi_260331TO.toml
  sunscan_analysis_sun_tp_NANTEN2_multi_260331TO.toml
```

## sunscan実行時

TP recording setupから生成された `spectral_recording_snapshot.toml` をsunscanへ渡す。snapshot内のstreamは `recording_table_kind = "tp"` となり、`data/tp/...` のtableを読む。

概念的には以下のように実行する。

```bash
sunscan_extract_multibeam RAWDATA_DIR \
  --sunscan-analysis-config sunscan_analysis_sun_tp_...toml \
  --spectral-recording-snapshot RAWDATA_DIR/spectral_recording_snapshot.toml
```

実装側では、TP-only snapshotをsunscan用streamとしてmaterializeできるようにし、TP tableの `tp_mean` または `tp_sum` を1-channel信号として読めるようにした。

---

# 2026-05-12 追補: focused test function name fix

per-window `fdnum/ifnum/plnum` override の追加テストで、存在しない `resolve_spectral_recording_snapshot` を呼んでいたため、正しい `resolve_spectral_recording_setup` へ修正した。実装本体の挙動ではなく、追加テスト側の関数名ミスである。

---

# 2026-05-12 追補: NANTEN2 converter base cleanup

NANTEN2 converter analysis exampleに、旧設定由来の以下がactiveで残っていたため、コメントアウトした。

```toml
planet = "Ori-KL"
spectral_name = "xffts-board1"
```

`planet` はSun/sunscanのようなmoving-body target用であり、固定天体Ori-KLには使わない。また、sunscanが `[analysis_base]` でconverter analysisを継承するため、converter base側に誤った `planet` があるとSun解析へ混入し得る。

`spectral_name` はsnapshot-based update14 conversionのstream selectionとしては使わない。streamを選ぶ場合は、CLIの `--recorded-stream-id` または `[analysis_selection]` を使う。

Sun TP用sunscan analysisには、意図を明確にするため、`[sunscan_analysis] planet = "sun"` を明示した。

