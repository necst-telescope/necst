# update14 realistic spectral-time and writer-path simulation (2026-05-01)

## 結論

update14の通常経路、すなわち `xfftspy.data_consumer(return_numpy=True)` → `neclib XFFTS` queue → `NECSTDBWriter.append_packed()` では、8 boards × 32768 channels × 0.1 s cadence を想定しても、この環境の最小シミュレーション上は書き込み側に十分な余裕がありました。

一方で、`return_numpy=True` が使えず tuple fallback になる場合は、XFFTS packet parse の時点で余裕がかなり小さくなります。したがって実機投入前の重点確認点は、**実行環境で本当に numpy ndarray のまま SpectralData.record() まで届いていること**です。

## 定義

- `packet`: 1回のXFFTS TCP受信単位。ここでは8 boardsを1 packetに含める。
- `row`: NECSTDB上の1 boardぶんの1 spectral row。
- `nboard`: 8
- `nchan`: 32768 channels per board
- `packet_MB`: 1.048704 MB
- 想定cadence: 0.1 s per packet
- 入力データ率: 10.487 MB/s
- 必要row rate: 80.0 rows/s

## 測定結果

| 項目 | 条件 | 結果 |
|---|---|---:|
| XFFTS受信parse | numpy ndarray path | median 5.936 ms/packet |
| XFFTS受信parse | numpy ndarray path | median 176.7 MB/s |
| XFFTS受信parse | tuple fallback | median 91.062 ms/packet |
| XFFTS受信parse | tuple fallback | median 11.5 MB/s |
| NECSTDB direct write | ndarray + append_packed | 202.1 rows/s |
| NECSTDB direct write | ndarray + append_packed | 26.5 MB/s |
| NECSTDB queue write | ndarray + background drain | 740.2 rows/s |
| NECSTDB queue write | ndarray + background drain | 97.1 MB/s |

## 解釈

通常経路の受信parse中央値は入力データ率に対して約 16.8 倍、queue込みのwriter throughputは必要row rateに対して約 9.3 倍でした。したがって、少なくともこの診断では、分光計データからNECSTDB書き込みまでの通常経路は 0.1 s cadence を処理できます。

ただし、tuple fallbackでは受信parseの中央値が 91.1 ms/packet となり、0.1 s cadenceにかなり近づきます。これはPython float tupleを作るためで、実機ではOS jitter、他node、ディスクI/Oが重なるため危険です。

## 実装上の確認点

- `xfftspy-master/xfftspy/data_consumer.py` は `return_numpy=True` のとき `np.frombuffer(rawdata, dtype='<f4', ...)` を使う。
- `neclib-second_OTF_neclib/neclib/devices/spectrometer/xffts.py` は `data_consumer(..., return_numpy=True)` を要求する。
- `necst-second_OTF_branch/necst/rx/spectral_recording_runtime.py` の full-spectrum save は元の `spectral_data` object を保持する。
- `neclib-second_OTF_neclib/neclib/recorders/necstdb_writer.py` は numpy 1D array を raw bytes に変換し、`append_packed()` があればPython float展開を避ける。

## 未確認・注意

この診断は実TCPソケット、実ROS2 timer、実XFFTS、実観測中の他process負荷を含みません。実機では、最初の数分だけでも以下をログ確認するのが安全です。

- `type(spectral_data)` が `numpy.ndarray` であること。
- writer queue size が増え続けないこと。
- `recorded_time - time` の中央値・p84が観測中に単調増加しないこと。
- `time_spectrometer` と `time` の差が9時間や18秒の異常値になっていないこと。

## 生データ

詳細な数値は `docs/preflight/update14_realistic_time_and_writer_simulation_2026-05-01.json` に保存しました。
