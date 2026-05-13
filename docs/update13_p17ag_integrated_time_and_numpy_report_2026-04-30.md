# update13 p17ag integrated report: p17a numpy fast path + p16g spectral-time diagnostics

Date: 2026-04-30

## 結論

`p17ag` は、`p16g` を基準に、`p17a` の高速化方針を統合した版である。

- `p16g` は converter の時刻診断・時刻解釈 override を追加した。
- `p17a` の高速化は XFFTS parser、NECSTDBWriter、necstdb table、legacy/active spectral save path を対象にし、converter 本体には触れない。
- したがって、論理的には統合してよい。
- 本版では、converter ファイルは `p16g` から変更していない。

## 統合した内容

### 1. XFFTS parser: `numpy.frombuffer`

NECST の XFFTS device からは、

```python
xfftspy.data_consumer(host, port, return_numpy=True)
```

を使う。

これにより、XFFTS payload の `32768 float32 × board数` を Python float の tuple に展開せず、`np.ndarray(dtype="<f4")` として受け取る。

外部で `xfftspy` を直接使う既存コードの互換性を保つため、既定値は

```python
return_numpy=False
```

のままである。

### 2. necstdb table: `append_packed(record_bytes)`

既存の `append(*data)` は変更しない。

新たに、

```python
table.append_packed(record_bytes)
```

を追加した。これは既存 NECSTDB の `.header` / `.data` 形式を変えず、1 record分のbytesを直接追記するための低レベルAPIである。

### 3. NECSTDBWriter: ndarray/raw-bytes fast path

`NECSTDBWriter` は `np.ndarray` を受け取った場合、`list()` や `tolist()` へ戻さず、

```python
array.astype("<f4", copy=False).tobytes()
```

相当の raw bytes として record に詰める。

tuple/list入力も従来通り受け付ける。tuple入力とndarray入力で、`recorded_time` 以外の `.data` bytes が一致することを確認した。

### 4. legacy path の保護

snapshotを使わない従来保存経路でも `np.ndarray` が流れ得るため、legacy spectral save path は ROS `Spectral` message constructor に依存せず、NECSTDB chunkを直接作るようにした。

これにより、

- snapshotあり active path
- snapshotなし legacy path

の両方で ndarray fast path を安全に使える。

### 5. full-range slice no-copy

active snapshot path で full spectrum を保存する場合は、`spectral_data[0:full_nchan]` を作らず、元 object をそのまま渡す。

tupleの場合は不要なcopyを避け、ndarrayの場合は zero-copy を維持する。

## 仕様上の注意

本版は raw spool mode ではない。既存 NECSTDB 形式のまま、Python object 生成と struct argument 展開を減らす高速化である。

観測中のDB構造、converterの入力形式、既存readerが読む `.header` / `.data` 形式は変えない。

## 確認したこと

### converterの差分

`sd-radio-spectral-fits-main/src/tools/necst/necst_v4_sdfits_converter.py` は `p16g` から変更していない。

つまり、時刻診断・時刻解釈 override は `p16g` のまま維持され、p17a高速化でconverterへの追加変更はない。

### シミュレーション

以下を確認した。

```text
XFFTS parser:
  16 board × 32768 channel の疑似payloadで、
  tuple path と ndarray path の値が一致。

necstdb append_packed:
  append(*data) と append_packed(record_bytes) の .data bytes が一致。

NECSTDBWriter:
  tuple spectrum 入力と ndarray spectrum 入力で、
  .header JSON が一致。
  .data payload は recorded_time 以外一致。
```

### 構文確認

変更Pythonファイルを `py_compile` で確認した。

## 実機確認項目

実機では以下を見る。

```text
1. Dropping the data due to low readout frequency. が出ないこと
2. Too many data waiting for being dumped. が出ないこと
3. 16 board の row 数が期待値と一致すること
4. time_spectrometer に欠番・重複・逆行がないこと
5. snapshotなし legacy 保存が動くこと
6. snapshotあり active 保存が動くこと
7. converter --inspect-spectral-time が動くこと
8. 旧readerでNECSTDBを読めること
```

## 未実施

```text
ROS2 colcon build
実XFFTS入力
実AC240入力
実NECSTDB recording
長時間連続 recording
HDD/SSD 比較
```
