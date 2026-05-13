# update13 p16 分光計取得ホットパス再検証報告

Date: 2026-04-30  
Input checked: `p16.zip`  
Output after correction: `p16f.zip`

## 結論

`p16.zip` の主要な狙いである、分光計 driver queue を詰まらせにくくする修正は妥当である。
特に以下の2点は、既存実装より安全側である。

1. `fetch_data()` は、先頭の分光計 queue が空でも後続分光計 queue を読む。
2. `get_data()` は、全分光計 queue が揃う前に一部だけ消費しない。

ただし、再検証で active spectrum fast path の基礎文字列列に互換性差分を見つけた。
`position`, `id`, `line_label`, `time_spectrometer` が旧 `Spectral.msg` 経路の空白 padding ではなく NUL padding になり得たため、
既存 reader 互換性を優先して修正した。

修正後は、active spectrum fast path の base columns が旧 `_spectral_chunk(Spectral.msg)` と一致することを直接比較した。

## 修正した互換性差分

### 問題

`p16.zip` では、`spectrum_chunk_for_stream()` が base string fields に `runtime_chunk_field()` を使っていた。
この helper は固定長文字列を NUL padding bytes にする。

一方、従来の active spectrum 経路は、

```text
_make_spectral_message()
  -> _spectral_chunk()
```

を通り、`string<=N` fields は空白 padding の Python string として writer へ渡っていた。

### 修正

`spectrum_chunk_for_stream()` 内の base Spectral columns については、
`SpectralData._spectral_chunk()` と同じ `string<=N` + 空白 padding の表現へ戻した。

対象 field:

```text
position
id
line_label
time_spectrometer
```

active setup provenance fields は、従来通り fixed-byte runtime helper を使う。

## 直接比較結果

同じ入力に対して、以下を比較した。

```text
legacy:
  _make_spectral_message()
  _spectral_chunk()

fast:
  spectrum_chunk_for_stream()
```

base columns:

```text
data
position
id
line_index
line_label
time
time_spectrometer
ch
rfreq
ifreq
vlsr
integ
```

結果:

```text
all base columns: identical
position: "ON      "
id: "orion           "
time_spectrometer: "xffts-time                      "
```

## 分光計 queue シミュレーション

Timer model:

```text
fetch timer:  0.02 s
record timer: 0.02 s
driver queue max size: 10
fetch drain limit: 8 packets/tick
```

### 通常 XFFTS 相当 10 Hz

```text
produced per key: 200
recorded batches: 200
driver drops: 0
max driver queue: 1
max internal queue: 1
final backlog: 0
```

### 50 Hz 入力

```text
produced per key: 1000
recorded batches: 1000
driver drops: 0
max driver queue: 1
max internal queue: 1
final backlog: 0
```

### 10 Hz, record 1秒停止

```text
produced per key: 200
recorded batches: 200
driver drops: 0
max driver queue: 1
max internal queue: 11
final backlog: 0
```

### 100 Hz 入力、catch-upなし

```text
produced per key: 2000
recorded during run: 1000
driver drops: 0
max driver queue: 2
max internal queue: 1001
final internal backlog: 1000
```

この場合、driver queue からは取りこぼさないが、record 側の消費が追いつかず internal queue が増える。
XFFTS の通常設定 synctime_us >= 100000 us ならこの状況にはなりにくいが、
将来 50 Hz を超える取得周期にする場合は、record 側も bounded burst 化する検討が必要である。

### 100 Hz 入力、観測後catch-upあり

```text
produced per key: 2000
recorded total: 2000
driver drops: 0
final backlog: 0
```

## p15 と p16 の差分確認

### fetch_data: 先頭queueが空、後続queueがready

p15相当:

```text
A internal: 0
B internal: 0
B driver:   5
```

p16修正後:

```text
A internal: 0
B internal: 5
B driver:   0
```

p16は後続分光計のdriver queueを読むため、後続queueの詰まりを避ける。

### get_data: Aだけinternal queueにある、Bは空

p15相当:

```text
return: None
A queue after: 4
A resizer pushes: 1
```

p16修正後:

```text
return: None
A queue after: 5
A resizer pushes: 0
```

p16は全queueが揃うまで一部queueを消費しない。

## active multi-window simulation

Input:

```text
1 raw spectrum from xffts board2
2 recorded products:
  s1: channels [2:6]
  s2: channels [8:12]
```

10 Hz for 10 seconds:

```text
input spectra: 100
expected append rows: 200
actual append rows: 200
driver drops: 0
internal backlog: 0
fatal_error: none
```

## unknown raw input

active snapshot mode で snapshot に無い raw board が来た場合:

```text
append rows: 0
fatal_error: latched
gate_allow_save: false
```

これは silent skip を避けるための安全側動作であり、従来仕様通りである。

## NECSTDBWriter drain simulation

1000 queued chunks を `_drain_burst()` で処理した。

```text
written chunks: 1000
remaining queue: 0
number of bursts: 16
```

`DrainBurstSize=64` が効いていることを確認した。

## 実施した機械的確認

```text
changed Python files: py_compile OK
hotpath tests: PASS
direct simulations: PASS
ZIP testzip: OK
```

## 未実施

```text
ROS2 colcon build
実XFFTS入力
実AC240入力
実NECSTDB recording
長時間連続 recording
```

## 実機投入時に見るべきもの

```text
Dropping the data due to low readout frequency.
Too many data waiting for being dumped.
row count
time_spectrometer monotonicity / continuity
recorded table names
active setup fatal_error
```

## 今後の改良候補

1. internal queue size / backlog をログ・diagnostic topicへ出す。
2. record 側にも bounded burst mode を入れる。ただし record callback が長くなり fetch timer を阻害しない設計が必要。
3. driver queue から internal queue へ移した後の backlog 上限を設定し、過大時は fatal/error として観測者に明示する。
4. `time_spectrometer` の連続性 checker を追加し、recorded rows に欠番または逆行がないか自動検査する。
5. 実NECSTDB fixture または fake NECSTDB で end-to-end regression を追加する。
