# update13 p16: 分光計データ取得ホットパス再検証・修正報告

Version: update13_p16_verified_2026-04-30  
Base: `p15.zip` / `pr8g_update13_priority1_5`  
目的: 分光計データ取得 callback から NECSTDB 書き込みまでの経路を、観測中の取りこぼしを避ける観点で再確認し、既存正常動作を壊さない範囲でホットパスを高速化する。

---

## 1. 確認対象のデータ経路

今回確認したホットパスは以下である。

```text
XFFTS / AC240 driver thread
  -> device.data_queue
  -> SpectralData.fetch_data()
  -> SpectralData.data_queue
  -> SpectralData.get_data()
  -> SpectralData.record()
  -> legacy / active snapshot recording branch
  -> Recorder.append()
  -> NECSTDBWriter._data_queue
  -> NECSTDBWriter background thread
  -> necstdb table append
```

ここで最も重要なのは、driver thread 側の `device.data_queue` を詰まらせないことである。`device.data_queue` が満杯になると、XFFTS / AC240 側では古いデータが捨てられ、`Dropping the data due to low readout frequency.` が出る可能性がある。

---

## 2. 見つけた問題点

### 2.1 `fetch_data()` の途中 return

p15時点では、`SpectralData.fetch_data()` は次の構造だった。

```text
for key, io in self.io.items():
    if io.data_queue.empty():
        return
    self.data_queue[key].put(io.get_spectra())
```

このため、複数分光計がある場合、先に見た分光計のqueueが空だと、その時点で関数全体を抜け、後続分光計のqueueを読まない。後続分光計にデータが溜まっている場合、後続側のdriver queueが詰まりやすくなる。

修正後は、空の分光計は `continue` 相当に扱い、後続分光計を必ず確認する。また、queue-backed device では `get_nowait()` を使い、ROS timer callback が分光計queueでblockしないようにした。

### 2.2 `get_data()` の部分消費

p15時点では、`get_data()` は各内部queueを順に確認し、前半のqueueを消費した後で後半queueが空だと `None` を返す可能性があった。

2台構成で、

```text
A: dataあり
B: dataなし
```

の場合、Aだけ消費してから `None` を返すため、以後の同期が崩れ得る。修正後は、全queueに少なくとも1 packetあることを確認してから、まとめて消費する。全queueが揃わない場合は何も消費しない。

### 2.3 active snapshot spectrum branch のrowごとのROS message生成

active snapshot mode の spectrum branch では、保存する各rowごとに `Spectral` messageを作り、`get_fields_and_field_types()` でfield introspectionし、chunkを作っていた。これは通常のROS publishには自然だが、NECSTDBへ保存するだけのホットパスでは余分である。

修正後は `spectrum_chunk_for_stream()` を追加し、`necst_msgs/Spectral` と同じbase columnを直接chunkとして生成する。TP branchで既に行っている direct chunk 方式に合わせた。

### 2.4 append path のrowごとの再計算

active modeではrowごとに `namespace_db_path(namespace.root, stream["db_table_path"])` を呼んでいた。これは軽い処理ではあるが、multi-windowで1 raw inputから複数 recorded product が出る場合は、rowごと・productごとに繰り返される。

修正後は setup apply 時に `_runtime_db_append_path` をstreamへ入れ、recording時はそれを使う。存在しない場合のみfallbackで再計算する。

### 2.5 NECSTDBWriter background thread のcatch-up効率

p15時点では writer thread は1 loopで1 chunkだけ書き、毎loopでtable livelinessを確認していた。queueが一時的に溜まった後のcatch-up効率が弱い。

修正後は、

```text
- 最大64 chunkまでburst drain
- liveliness scanは1秒間隔
- queue getはtimeout/get_nowaitで制御
- append validationで毎fieldごとの一時set生成を避ける
```

とした。

---

## 3. 既存正常動作への影響確認

### 3.1 single spectrometer

1台のqueue-backed spectrometerで1 packetだけある場合、

```text
fetch_data()
get_data()
```

により従来どおり1 packetが取得され、resizerへpushされることを確認した。

### 3.2 複数spectrometerで先頭queueが空の場合

疑似条件:

```text
A: empty
B: 5 packets
```

結果:

```text
p15相当:
  Bから0 packetしか読まない

p16:
  Bから5 packetを読む
```

これにより、片方が一時的に空でも他方のdriver queueを詰まらせにくくなる。

### 3.3 複数spectrometerで全queueが揃わない場合

疑似条件:

```text
A: 1 packet
B: empty
```

結果:

```text
p15相当:
  Aを消費してから None を返す

p16:
  Aを消費せず None を返す
```

したがって、部分消費による内部同期崩れを避ける。

### 3.4 multi-window active recording

1つのraw spectrumから2つのrecorded productを作る疑似設定で確認した。

```text
raw input:
  key="xffts", board_id=2, spectrum length=16

recorded product 1:
  saved_ch_start=2, saved_ch_stop=6

recorded product 2:
  saved_ch_start=10, saved_ch_stop=14
```

結果:

```text
/necst/data/spectral/xffts/board2__13CO
  data = [2, 3, 4, 5]

/necst/data/spectral/xffts/board2__C18O
  data = [10, 11, 12, 13]
```

append path とslice結果は仕様通りである。

### 3.5 NECSTDB writer schema

`Spectrum` direct chunkを実際に `NECSTDBWriter` へ渡し、header schemaを確認した。

```text
position:
  8s

id:
  16s

time_spectrometer:
  32s

setup_id:
  64s

data:
  Nf
```

固定長文字列とspectrum arrayのschemaは期待通りである。

---

## 4. 性能確認

### 4.1 active spectrum chunk生成

疑似 benchmark では、active spectrum chunk生成について以下の傾向だった。

```text
old path:
  Spectral message生成 + field introspection + extra chunk

new path:
  direct spectrum_chunk_for_stream()
```

測定例:

```text
1000 rows:
  old median = 0.188 s
  new median = 0.092 s
  ratio      = 2.05x

5000 rows:
  old median = 0.705 s
  new median = 0.399 s
  ratio      = 1.77x
```

この測定は疑似環境であり、実機の総処理時間を直接表すものではない。ただし、Python側のrowごとの無駄を減らす方向としては有効である。

### 4.2 fetch_data backlog drain

疑似条件:

```text
1 spectrometer queueに20 packets backlog
fetch limit = 8
```

結果:

```text
1回目のfetch_data:
  8 packets drain, remaining 12

2回目のfetch_data:
  さらに8 packets drain, remaining 4
```

短時間のPython/ROS側stall後に、driver queueへ追いつきやすくなる。

---

## 5. 変更ファイル

```text
necst-second_OTF_branch/necst/rx/spectrometer.py
necst-second_OTF_branch/necst/rx/spectral_recording_runtime.py
neclib-second_OTF_neclib/neclib/recorders/necstdb_writer.py
necst-second_OTF_branch/tests/test_spectral_recording_hotpath_performance.py
docs/manual_full_update13_ja.md
docs/manual_quick_update13_ja.md
docs/manual_update13_report.md
docs/update13_realtime_hotpath_performance_report_2026-04-30.md
```

---

## 6. 検証結果

```text
changed Python files:
  py_compile OK

統合tree:
  596 Python files py_compile OK

直接シミュレーション:
  fetch_data first-empty/later-ready case OK
  get_data partial-consume prevention OK
  single-spectrometer normal case OK
  active multi-window slicing + append path OK
  spectrum direct chunk schema OK
  NECSTDBWriter burst drain OK
  NECSTDBWriter spectrum chunk header schema OK

Markdown:
  code fence OK
  制御文字なし

ZIP:
  testzip OK
  再展開後hash一致 OK
  __pycache__ / .pyc / .pyo / .pytest_cache / .DS_Store 混入なし
```

この環境では、repository側の `tests/conftest.py` が `rclpy` を要求するため、通常の `pytest` による全体試験は実施していない。代わりに、今回変更した関数を最小stub環境で直接読み込み、上記の疑似シミュレーションを行った。

---

## 7. 未実施・実機で必ず確認すること

以下はこの環境では未実施である。

```text
ROS2 colcon build
実XFFTS入力
実AC240入力
実NECSTDB recording
実観測DBに対するconverter/sunscan
長時間連続recording
```

実機では少なくとも以下を確認する。

```text
1. row数が期待cadenceと合うこと。
2. time_spectrometerが単調に進むこと。
3. XFFTS/AC240側で Dropping the data due to low readout frequency. が出ないこと。
4. NECSTDBWriter側で Too many data waiting for being dumped. が出ないこと。
5. multi-window時に各recorded productのrow数が一致すること。
6. SSD使用時とHDD/遅い媒体使用時でwriter queue warningの有無を見ること。
```

---

## 8. 今後の追加改良候補

### 8.1 writer queue depthの定期ログ

現在は閾値超過時にwarningするだけである。実機評価では、1秒または10秒ごとに最大queue depthを記録できると、取りこぼし直前の兆候を見つけやすい。

### 8.2 per-spectrometer fetch/drain統計

`fetch_data()` で何packet drainしたかを、低頻度でログまたはdiagnostic topicへ出せるとよい。

### 8.3 internal data_queueの上限

`SpectralData.data_queue` は現在無制限である。片方の分光計が停止した場合、もう片方の内部queueが増え続ける可能性がある。実機運用では、上限とdrop policyを明示する余地がある。

### 8.4 pre-create NECSTDB tables

初回rowでtable作成が走ると、その瞬間だけwrite latencyが増える可能性がある。setup apply時またはrecord開始時に、予想されるtableをpre-createする設計は有効である。

### 8.5 writer threadのI/O affinity / priority

SSD使用で通常問題ないとしても、OS負荷が大きい場合はwriter threadやdriver threadのpriority/affinityを検討する価値がある。ただし、Python threadだけで完結しないため、実測に基づいて行うべきである。

---

## 9. 判断

p16は観測中枢に触る修正であるため、実機投入前には必ず短時間recordingで確認する必要がある。ただし、今回の修正は以下の理由で、p15より安全側である。

```text
- 空queueで関数全体をreturnしないため、後続分光計queueを放置しない。
- 全queueが揃うまで内部queueを部分消費しない。
- active spectrum branchのrowごとのROS message生成を避けるが、base column orderは維持している。
- writerは1 chunkずつではなくburst drainするため、短時間backlogに追いつきやすい。
- legacy branchは基本的に従来の _make_spectral_message + _spectral_chunk を維持している。
```
