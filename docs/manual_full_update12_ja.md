# NECST/XFFTS spectral recording・split configuration・multi-window・sunscan 詳細取扱説明書

Version: update12 / 日本語版 / 2026-04-29  
対象実装: `pr8g_update12_preflight_verified_with_docs.zip` および `u12p.zip` 適用後の update12 tree  
対象者: 観測者、受信機・LO担当者、コミッショニング担当者、SDFITS converter利用者、sunscan/multibeam解析利用者、実装保守担当者

この文書は、NECST/XFFTS spectral recording の現行仕様を説明する詳細版である。旧all-in-one `spectrometer_config` で混在していた観測時truth、保存window、beam geometry、converter解析条件、sunscan解析条件を分離して扱う。過去の経緯ではなく、現在の仕様・運用・パラメータ・例を中心に記述する。

---

## 0. update12での位置づけと読み方

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

## 1. 設計の全体像

### 1.1 分離するもの

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

### 1.2 最重要原則

1. `lo_profile.toml` は観測時truthである。
2. `converter_analysis.toml` と `sunscan_analysis.toml` は解析条件であり、観測時truthを再定義しない。
3. `rest_frequency_*` は分光計の仕様ではなく、streamまたはwindowのline設定である。
4. `db_stream_name` はraw入力stream名であり、multi-window保存後のDB名は `recorded_db_stream_name` である。
5. snapshotを使うconverter/sunscanでは、`--config-loader adapter` は不要である。
6. `pointing_reference_beam_id` は観測制御に効くbeam IDであり、`center_beam_id` とは別概念である。

---

## 2. 用語

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

## 3. `.obs` での使い方

### 3.1 書く場所

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

### 3.2 何も書かなければlegacy

`lo_profile`、`spectral_recording_snapshot`、`recording_window_setup` などを書かなければ、legacy観測として動く。新setupはapplyされず、snapshot sidecarも保存されない。

### 3.3 事前snapshotを使う場合

```toml
[parameters]
spectral_recording_snapshot = "spectral_recording_snapshot.toml"
setup_id = "orikl_snapshot"
```

この場合、stream truthはsnapshotから来る。外部 `beam_model` を解析・pointing用に渡す場合は、pointing reference beamに関わるbeamの一致検査に注意する。

### 3.4 `.obs [parameters]` の全体表

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

## 4. 観測時シーケンス

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

### 4.1 cleanup時の安全側動作

終了時に失敗があっても、原則として以下を試みる。

```text
gate close
recorder stop
active setup clear
antenna stop / privilege release
```

active setupが残っている可能性がある場合、legacy TP/binning resetを不用意に送らない。

---

## 5. `lo_profile.toml`

### 5.1 役割

`lo_profile.toml` は、観測時truthの中心ファイルである。ここには以下を書く。

```text
SG device設定
LO roles
LO chains
frequency axes
stream groups
streamごとのbackend/sampler/LO/frequency_axis/rest frequency/beam_id
```

### 5.2 全パラメータ表

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


### 5.3 frequency axis

#### 5.3.1 `definition_mode = "band_start_stop"`

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

#### 5.3.2 `definition_mode = "first_center_and_delta"`

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

#### 5.3.3 `definition_mode = "explicit_wcs"`

snapshotや保存済みlocal channel用の明示WCSである。

```toml
definition_mode = "explicit_wcs"
nchan = 8000
crval1_hz = 112000000000.0
cdelt1_hz = 61037.0
crpix1 = 1.0
```

### 5.4 rest frequency

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

### 5.5 LO chain

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

Hz/GHz/MHz aliasはHzへ正規化される。同じ階層で矛盾した値を二重指定した場合はエラーにする。

### 5.6 stream group

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


## 5A. 受信機設定時の SG/LO CLI

### 5A.1 `config.toml` と `lo_profile.toml` の分担

SGのIP address、port、機種、device idはNECSTの `config.toml` に置く。

```toml
[signal_generator.sg_lsb_2nd]
_ = "FSW0010"
host = "192.168.100.53"
port = 10001
```

`lo_profile.toml` には、そのdevice idのSGを観測ごとに何Hz/何dBmで使うかを書く。

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

### 5A.2 SG設定一覧の確認

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

### 5A.3 SG apply

特定SGだけ設定する。

```bash
necst-lo-profile apply lo_profile.toml --id sg_lsb_2nd
```

全SGを設定する。

```bash
necst-lo-profile apply lo_profile.toml
```

受信機立ち上げ時は、1台ずつapplyして、実際のSG表示やreadbackと照合する方が安全である。

### 5A.4 SG apply + verify

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

### 5A.5 stale readbackを成功扱いしない

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

### 5A.6 SG frequency と physical LO frequency

`sg_set_frequency_*` はSGへ直接設定する周波数である。converterやWCSが必要とするLO周波数は、逓倍やsidebandを反映したphysical LOである。

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

### 5A.7 受信機調整時の標準手順

```bash
# 1. SG設定一覧
necst-lo-profile summary lo_profile.toml

# 2. SGを1台ずつ設定・検証
necst-lo-profile apply lo_profile.toml --id sg_lsb_2nd --verify --timeout-sec 10
necst-lo-profile apply lo_profile.toml --id sg_usb_2nd --verify --timeout-sec 10

# 3. snapshot resolve
necst-spectral-resolve \
  --lo-profile lo_profile.toml \
  --recording-window-setup recording_window_setup.toml \
  --beam-model beam_model.toml \
  --setup-id receiver_checkout \
  --output spectral_recording_snapshot.toml

# 4. snapshot validate
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


## 6. `beam_model.toml`

### 6.1 役割

`beam_model.toml` はbeam geometryのファイルである。観測時にstreamの `beam_id` が実在するかを検査し、後段converter/sunscanでも同じbeam geometryを使えるようにする。

### 6.2 全パラメータ表

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


### 6.3 B00の扱い

`beam_model.toml` を省略した場合、内部的には `B00=(0,0)` だけが定義される。streamに `beam_id` が無い場合は `B00` へ解決される。streamが `B01` 以降を参照する場合、`beam_model.toml` は必須である。

### 6.4 fixed beam

```toml
[beams.B00]
beam_model_version = "default_zero_boresight"
rotation_mode = "fixed"
az_offset_arcsec = 0.0
el_offset_arcsec = 0.0
dewar_angle_deg = 0.0
```

### 6.5 pure_rotation_v1

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

## 7. `pointing_reference_beam_id`

### 7.1 意味

`pointing_reference_beam_id` は、中心boresightではなく、任意のbeamが観測者の指定targetへ向くようにboresightをずらす観測制御パラメータである。

```toml
[parameters]
beam_model = "beam_model_multibeam.toml"
pointing_reference_beam_id = "B03"
pointing_reference_beam_policy = "exact"
```

`center_beam_id` はsunscan fit等の解析時概念と衝突するため、観測制御には使わない。

### 7.2 exact policy

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

### 7.3 snapshot beamと外部beam_modelの一致規則

`pointing_reference_beam_id` が指定されている場合、観測時boresight補正に効くのはそのbeamだけである。したがって、snapshot内beamsと外部 `beam_model.toml` を比較する場合、全beam一致ではなく、少なくとも `pointing_reference_beam_id` のbeam定義が一致している必要がある。

```text
pointing_reference_beam_id = B03:
  snapshot.beams.B03 と external beam_model.beams.B03 は一致必須
  B04など他beamの更新は許容。ただしprovenanceに残す
```

`pointing_reference_beam_id` が未指定またはB00で、B00が `(0,0)` として一致しているなら、後処理用に他beamをsunscan結果で更新してよい。

---

## 8. `recording_window_setup.toml`

### 8.1 役割

観測時にDBへ保存するchannel範囲を定義する。channel指定はfull channelであり、converterの `--channel-slice` はsnapshot使用時には保存済みlocal channelである。

### 8.2 全パラメータ表

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


### 8.3 `saved_window_policy`

#### `full`

full spectrumを保存する。

```toml
[recording_groups.full_all]
mode = "spectrum"
saved_window_policy = "full"
streams = ["2LU", "2RU"]
```

#### `channel`

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

#### `contiguous_envelope`

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

#### `multi_window`

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

### 8.4 velocity frame

`velocity_frame = "LSRK"` または `"VLSRK"` の場合、VLSRK補正を計算する。参照座標・site・時刻が無い場合はエラーであり、topocentric近似には戻さない。

`velocity_frame = "TOPOCENTRIC"` の場合だけ、rest周波数まわりの近似windowとして扱う。

### 8.5 VLSRK基準時刻と座標

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

## 9. snapshot

### 9.1 役割

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

### 9.2 channel mapping

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

## 10. converter

### 10.1 snapshotを使う

```bash
python -m tools.necst.necst_v4_sdfits_converter \
  RAWDATA_DIR \
  --spectral-recording-snapshot RAWDATA_DIR/spectral_recording_snapshot.toml \
  --converter-analysis-config converter_analysis.toml
```

snapshotを使う場合、`--config-loader adapter` は不要である。`--config-loader adapter` は旧 `spectrometer_config.toml` を分離モデルへ通すためのloader指定である。

### 10.2 multi-windowの選択

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

### 10.3 channel slice

converter側の `--channel-slice` は、snapshot使用時には保存済みlocal channelで指定する。

観測時に full channel `[10000,18000)` を保存した場合、converter local channel 0はfull channel 10000である。converterで `--channel-slice "[100,200)"` とすると、full channel 10100--10199を出す。

### 10.4 converter CLI主要オプション

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

## 11. sunscan

### 11.1 extract

```bash
python -m tools.necst.multibeam_beam_measurement.sunscan_multibeam extract \
  RAWDATA_DIR \
  --spectral-recording-snapshot RAWDATA_DIR/spectral_recording_snapshot.toml \
  --sunscan-analysis-config sunscan_analysis.toml \
  --analysis-stream-selection analysis_stream_selection.toml \
  --outdir sunscan_out
```

### 11.2 singlebeam

```bash
python -m tools.necst.multibeam_beam_measurement.sunscan_singlebeam \
  RAWDATA_DIR \
  --spectral-recording-snapshot RAWDATA_DIR/spectral_recording_snapshot.toml \
  --sunscan-analysis-config sunscan_analysis.toml \
  --analysis-stream-selection analysis_stream_selection.toml \
  --stream-name 1LU \
  --outdir sunscan_single
```

### 11.3 fit

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

### 11.4 sunscan解析パラメータ

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

## 12. 受信機調整・コミッショニング・本観測 cookbook

### 12.1 受信機調整: full spectrumで保存する

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

### 12.2 コミッショニング: 速度範囲を広めに保存する

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

### 12.3 通常観測: 必要な範囲だけ保存する

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

### 12.4 110 GHzで13CO/C18Oを別product保存する

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

### 12.5 sunscanでbeam_modelを作る

1. fullまたは十分広いwindowでsunscan観測する。
2. `sunscan_multibeam extract` でstreamごとのprofileを出す。
3. `sunscan_fit_multibeam` でfitし、standalone `beam_model.toml` を得る。
4. 本観測の `.obs [parameters]` でその `beam_model.toml` を参照する。
5. `pointing_reference_beam_id` を使った観測で外部beam_modelをoverrideする場合は、そのbeam IDの定義がsnapshotと一致することを確認する。

---

## 13. 旧config相当のNANTEN2/XFFTS例

本節は、旧config `12co-NANTEN2-multi_260331TO.conf3` と同等の新分離ファイル例である。230/115 GHzは `contiguous_envelope`、110 GHzは `multi_window` を使う。

### 13.1 `lo_profile_12co_NANTEN2_multi_260331TO.toml`

```toml
# Auto-generated from 12co-NANTEN2-multi_260331TO.conf3.txt
# New split configuration example: stream truth + shared frequency/LO definitions.
schema_version = "lo_profile_v2"
profile_id = "12co_NANTEN2_multi_260331TO_equivalent_update12"

[frequency_axes.xffts_2GHz_32768ch]
definition_mode = "band_start_stop"
nchan = 32768
# Current resolver key is *_hz. Equivalent: 0.0 MHz to 2000.0 MHz.
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

### 13.2 `beam_model_12co_NANTEN2_multi_260331TO.toml`

```toml
# Auto-generated beam_model.toml from legacy spectrometer beam entries.
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

### 13.3 `recording_window_12co_NANTEN2_multi_260331TO.toml`

```toml
# Recording windows for 12co-NANTEN2-multi_260331TO equivalent setup.
# 230/115 GHz: VLSRK -150..+150 km/s with 10 km/s margin.
# 110 GHz: multi_window products for 13CO and C18O, each -100..+100 km/s with 10 km/s margin.
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

### 13.4 `analysis_stream_selection_12co_NANTEN2_multi_260331TO.toml`

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

### 13.5 `converter_analysis_12co_NANTEN2_multi_260331TO.toml`

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

### 13.6 `sunscan_analysis_12co_NANTEN2_multi_260331TO.toml`

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

### 13.7 `.obs [parameters]` fragment

```toml
# Add these keys to the [parameters] section of the .obs file.
# Paths are normally resolved relative to the .obs file directory.
[parameters]
cos_correction = true
lo_profile = "lo_profile_12co_NANTEN2_multi_260331TO.toml"
recording_window_setup = "recording_window_12co_NANTEN2_multi_260331TO.toml"
beam_model = "beam_model_12co_NANTEN2_multi_260331TO.toml"
setup_id = "orikl_NANTEN2_multi_260331TO_update12_windows"
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

---

## 14. ファイル間依存関係と検査

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

## 15. 性能・リアルタイム性

### 15.1 setup resolve側

TOML parse、VLSRK計算、velocity window計算、beam_model hash、snapshot生成はrecord開始前に行う。raw callbackに入れてはいけない。

update12では、velocity window解決時に32768 channel配列を毎回作らず、線形WCSから直接channel範囲を計算する。

### 15.2 runtime callback側

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

### 15.3 multi-window時のDB構造

1 recorded product = 1固定長schema = 1 DB table/file とする。同じDB tableにchannel数やRESTFRQの異なるwindowを混ぜない。

---

## 16. よくある間違い

### 16.1 `rest_frequency_ghz` を `frequency_axes` に書く

`rest_frequency_ghz` はstreamまたはwindowに書く。frequency axisは分光計のIF軸であり、line rest frequencyではない。

### 16.2 `--channel-slice` にfull channelを入れる

snapshot使用時のconverter `--channel-slice` は保存済みlocal channelである。観測時のfull channelと混同しない。

### 16.3 `stream_id` だけでmulti-window productを選ぶ

multi-windowで1 source streamに複数recorded productがある場合、`--stream-id` だけでは曖昧である。`--window-id` または `--recorded-stream-id` を指定する。

### 16.4 `beam_model` だけで新setupが起動すると思う

`beam_model` だけではspectral recording setupは起動しない。stream truthの本体は `lo_profile` または `spectral_recording_snapshot` である。

### 16.5 LSRKに必要な参照情報を省略する

`velocity_frame="LSRK"` では、参照座標・site・基準時刻が必要である。情報不足ならエラーにする。近似で進めたい場合は明示的に `velocity_frame="TOPOCENTRIC"` とする。

---

## 17. 既知の未確認項目

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


## 18. トラブルシュート

### 18.1 `stream_groups must not be empty`

現行実装では、`lo_profile.toml` に `[stream_groups.<group>]` と `[[stream_groups.<group>.streams]]` が必要である。将来 `[streams.<id>]` 直書き形式を追加する余地はあるが、update12では `stream_groups` を使う。

### 18.2 `beam_model.toml is required when streams reference non-B00`

`beam_model` を省略したときに使えるのは `B00=(0,0)` だけである。`B01` 以降をstreamが参照する場合は `beam_model.toml` を指定する。

### 18.3 `velocity_frame='LSRK' requires a velocity_reference_context`

LSRK/VLSRK保存windowでは、参照座標・site・基準時刻が必要である。`.obs` parserがreferenceを渡さない場合は `[parameters]` に `velocity_reference_l_deg` / `velocity_reference_b_deg` などを明示する。

### 18.4 `source stream has multiple recorded products`

multi-windowで `--stream-id 2LL` だけ指定すると、`2LL__13CO_J1_0` と `2LL__C18O_J1_0` のどちらを読むか曖昧である。`--window-id` または `--recorded-stream-id` を追加する。

### 18.5 期待した速度範囲が保存されていない

確認順序:

1. `recording_window_setup.toml` の `velocity_frame` を確認する。
2. `velocity_reference_*` が正しいか確認する。
3. snapshotの `computed_windows_json` を確認する。
4. `saved_ch_start/saved_ch_stop` がfull channelとして妥当か確認する。
5. converterの `--channel-slice` をfull channelと勘違いしていないか確認する。

### 18.6 rest frequencyの値が大きすぎて読みにくい

`rest_frequency_ghz`、`restfreq_ghz`、`lo1_ghz`、`lo2_mhz` などを使う。snapshot内ではHzへ正規化される。

### 18.7 sunscan fitで新beam_modelが見つからない

fitの既定出力はstandalone `beam_model.toml` 形式である。旧all-in-one config形式が必要なら `--write-legacy-beam-model` を付ける。



## 19. 実機投入前チェックリスト

### 19.1 ファイル整合

- `.obs [parameters]` の相対パスが `.obs` から正しく辿れる。
- `lo_profile.toml` の `stream_groups` が空でない。
- すべてのstreamに `stream_id`, `board_id`, `fdnum`, `ifnum`, `plnum`, `polariza` がある。
- すべてのstreamが `frequency_axis_id` と `lo_chain` を解決できる。
- すべてのnon-B00 `beam_id` が `beam_model.toml` に存在する。
- `recording_window_setup.toml` の `streams` が実在する。
- `window_id` が安全文字だけで構成されている。
- LSRK/VLSRK使用時は参照座標・site・基準時刻が確定している。

### 19.2 周波数軸確認

- `channel_origin=center` の場合、`delta = (stop-start)/(N-1)`。
- `channel_origin=edge` の場合、`delta = (stop-start)/N`。
- `reverse=true` の場合、最終 `if_freq_step_hz` の符号が意図通りか。
- `rest_frequency_ghz` がfrequency axisではなくstream/windowにある。
- 110 GHzの複数lineは `multi_window` か `contiguous_envelope` のどちらかを意図して選んでいる。

### 19.3 runtime確認

- setup apply時にfixed schema検査が通る。
- gate open前にsidecar保存が行われる。
- raw callbackで例外が出ない。
- multi-window時にrecorded productごとのDB table/fileが分かれている。
- `setup_hash` がログ・snapshot・DB rowで追える。

### 19.4 後処理確認

- converterはsnapshotを使う。
- multi-windowでは `--recorded-stream-id` または `--stream-id + --window-id` を使う。
- sunscan extractは `--sunscan-analysis-config` と `--analysis-stream-selection` を使う。
- beam_model override時はpointing reference beamの一致を確認する。


## 20. 旧config `[global]` の新ファイル対応表

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


## 21. snapshotに残る主なフィールド

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

## 22. TOPOCENTRIC fallback用の明示例

LSRK/VLSRKに必要な参照座標・siteがまだ確定できないコミッショニングでは、近似であることを明示したうえで `velocity_frame = "TOPOCENTRIC"` を使える。この場合だけ、rest周波数まわりの近似windowとして扱う。

```toml
# Recording windows for 12co-NANTEN2-multi_260331TO equivalent setup.
# 230/115 GHz: VLSRK -150..+150 km/s with 10 km/s margin.
# 110 GHz: multi_window products for 13CO and C18O, each -100..+100 km/s with 10 km/s margin.
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

## 23. 旧config相当ファイル群のREADME

以下は、旧config相当ファイル群に含めたREADMEである。運用時には、このREADMEを観測ディレクトリに置き、どのファイルが何を担うかを確認する。

```markdown
# 12co-NANTEN2-multi_260331TO legacy-config equivalent split setup

Generated: 2026-04-29T10:02:42

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

## update12 note: VLSRK reference direction

The recording window file uses `velocity_frame = "LSRK"`.  In update12 this is fail-closed:
the resolver must receive a velocity-reference direction, site, and reference time.  During
normal NECST observation flow these are supplied from `[parameters]` and/or the parsed
`.obs` reference coordinate.  For standalone validation, either provide explicit
`velocity_reference_l_deg` / `velocity_reference_b_deg` / site parameters, or change
`velocity_frame` to `"TOPOCENTRIC"` only when an explicitly topocentric approximate
window is intended.
```

## 24. 旧config相当ファイル群の検証サマリ

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

## 25. 仕様上の推奨値

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

## 26. 本文中で特に誤解しやすい境界

### 26.1 観測時保存windowとconverter出力slice

観測時の `recording_window_setup.toml` は、DBへ保存する範囲を決める。これはfull channelに対する指定である。converterの `--channel-slice` は保存済みlocal channelに対する指定である。両者は同じ番号空間ではない。

### 26.2 source streamとrecorded product

`stream_id="2LL"` はsource streamであり、multi-window保存後には `2LL__13CO_J1_0` のようなrecorded productが保存される。converterでline別に処理するときはrecorded productを選ぶ。

### 26.3 rest frequencyとfrequency axis

周波数軸は分光計のchannelからIF周波数への対応である。rest frequencyは、そのstream/productをどの輝線として速度変換するかである。110 GHzのように同じ分光計軸で複数lineを扱う場合、frequency axisは共通で、windowごとにrest frequencyを持てる。

### 26.4 beam geometryとpointing reference

`beam_model.toml` はbeam geometryである。`pointing_reference_beam_id` はその中の1つを観測制御の基準に使うだけである。後処理用に他beamのgeometryを更新しても、pointing reference beam自体が一致していれば観測時boresight再現性は保たれる。

### 26.5 target名とVLSRK

通常の観測制御でtarget名を使うことと、velocity window resolverが任意名をネットワーク解決することは別である。velocity window resolverは明示座標、`.obs` reference、太陽系天体名だけをdeterministicに扱う。


---

## 25. update12 リアルタイム高速化と副作用確認

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

## 26. 現行仕様でobsolete扱いする情報

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

## 付録A. update12 実機前preflight確認項目

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

## 付録B. update12 高速化の確認と注意

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

## 付録C. manual更新時の禁止事項

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
