# update13 p16g: XFFTS spectral time diagnostics and timestamp-scale override

## 結論

この更新は `p16f` を基準に、converter へ以下を追加する。

1. `--inspect-spectral-time`  
   SDFITSを作らず、RawData内の分光テーブルから `time_spectrometer` / `timestamp`、host receive time (`time`)、DB write time (`recorded_time`) を読み、UTC/GPS/TAI の3通りで比較する診断モード。

2. `--spectral-time-source`  
   converterが実際の分光データ時刻として使う基準を明示する。
   - `auto`: 従来互換。`UTC/GPS/TAI` suffixならtimestampを使い、`PC`/unknown/空ならnumeric host timeへfallback。
   - `host-time`: 常にnumeric host receive timeを使う。
   - `xffts-timestamp`: XFFTS timestampを必須にする。使えない場合はエラー。

3. `--xffts-timestamp-scale`  
   XFFTS timestamp本文をどの時刻系として解釈するかを明示する。
   - `auto`: timestamp末尾のsuffixを信じる。
   - `utc`: suffixに関係なく本文をUTCとして解釈する。
   - `gps`: suffixに関係なく本文をGPSとして解釈する。
   - `tai`: suffixに関係なく本文をTAIとして解釈する。

## 用語と列の定義

| 名称 | NECSTDB上の列 | 意味 |
|---|---|---|
| XFFTS timestamp | `time_spectrometer` または `timestamp` | XFFTS header由来の文字列時刻。末尾に `UTC` / `GPS` / `TAI` / `PC` が付き得る。 |
| host receive time | `time` | NECST側がXFFTS packetを受け取った直後の `time.time()`。 |
| DB write time | `recorded_time` | NECSTDB writer threadがDBへ実際にappendした時刻。取得時刻ではない。 |

## 追加CLI

suffixを信じる自動診断:

```bash
python -m tools.necst.necst_v4_sdfits_converter RAWDATA_DIR \
  --inspect-spectral-time
```

UTC入力なのにXFFTSのliteral suffixがGPSになる疑いがある場合:

```bash
python -m tools.necst.necst_v4_sdfits_converter RAWDATA_DIR \
  --inspect-spectral-time \
  --spectral-time-source xffts-timestamp \
  --xffts-timestamp-scale utc
```

従来のhost receive time基準を明示する場合:

```bash
python -m tools.necst.necst_v4_sdfits_converter RAWDATA_DIR \
  --spectral-time-source host-time
```

## 実装上の重要点

`_select_spectral_time_from_structured()` に `spectral_time_source` と `xffts_timestamp_scale` を通した。通常のSDFITS変換、VLSRK channel slice計算、診断モードが同じ時刻選択関数を使う。

また、literal suffixとinterpretation scaleを分離した。

```text
timestamp文字列:
  2026-04-30T12:00:00.000000GPS

--xffts-timestamp-scale auto:
  本文をGPS時刻としてUTC unix秒へ変換

--xffts-timestamp-scale utc:
  末尾GPSは無視し、本文をUTCとしてUTC unix秒へ変換
```

## 診断出力の見方

`--inspect-spectral-time` はJSONを出力する。

主な項目:

```text
streams[].timestamp.first_values
streams[].timestamp.first_suffix
streams[].comparisons.timestamp_as_utc_minus_host_sec
streams[].comparisons.timestamp_as_gps_minus_host_sec
streams[].comparisons.timestamp_as_tai_minus_host_sec
streams[].comparisons.recorded_minus_host_sec
streams[].selected.meta
streams[].selected.selected_minus_host_sec
```

判断例:

```text
timestamp_as_utc_minus_host_sec がほぼ一定で0秒付近:
  timestamp本文はUTC相当の可能性が高い。

timestamp_as_gps_minus_host_sec がほぼ一定で0秒付近:
  timestamp本文はGPS相当の可能性が高い。

suffixがGPSなのに timestamp_as_utc_minus_host_sec が0秒付近:
  XFFTSはsuffixとしてGPSを付けているが、本文はUTC相当の可能性がある。
  converterでは --xffts-timestamp-scale utc を使う候補になる。

recorded_minus_host_sec が大きい、または時間とともに増える:
  DB writer / I/O が遅れている可能性がある。
```

## 既存動作との互換性

何も指定しない場合は以下のままである。

```text
--spectral-time-source auto
--xffts-timestamp-scale auto
```

このため、既存のconverter実行では以下の従来挙動を維持する。

- suffixが `UTC/GPS/TAI` ならtimestampを使う。
- suffixが `PC`、unknown、空ならnumeric host timeへfallbackする。
- `--spectral-time-source xffts-timestamp` を指定した場合だけ、timestampが使えない時にエラーで止める。
- `--xffts-timestamp-scale utc/gps/tai` を明示した時だけ、suffixと異なる解釈を行う。

## 明日の実機試験案

1. XFFTSにIRIG-Bを入力する。
2. NECST通常観測経路で10〜30秒程度の短時間RawDataを取得する。
3. `--inspect-spectral-time` で `time_spectrometer`, `time`, `recorded_time` を比較する。
4. UTC/GPS/TAIのどの解釈がhost receive timeに最も安定して近いかを見る。
5. 本変換では確認済みの解釈を `--spectral-time-source` と `--xffts-timestamp-scale` で明示する。

## 未実施

- 実XFFTS IRIG-B入力時のsuffix確認。
- 実RawDataでの診断モード実行。
- 実SDFITS変換後の絶対時刻確認。
