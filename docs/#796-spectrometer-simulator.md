# Spectrometer simulatorによる分光記録の検証

関連Issue: `necst-telescope/neclib#796`

関連実装PR: `necst-telescope/neclib#802`

## 目的

実機XFFTSを接続していない開発環境でも、NECSTの分光計ノードからRecorderまでの処理を確認できるようにする。

`neclib` の `SpectrometerSimulator` は32768 chの疑似スペクトルを生成する。天体やON/OFF位置に応じた物理モデルではなく、分光データ取得・Quick Look・記録経路を動作確認するためのデモデータである。

## 分光計packet

NECSTの分光計ノードは、分光計driverから次の3要素を受け取る。

```python
(timestamp, time_spectrometer, data)
```

- `timestamp`: NECST側で扱うUNIX時刻
- `time_spectrometer`: 分光計側時刻を表す文字列
- `data`: `{board_id: spectrum}` の辞書

XFFTS、AC240、SpectrometerSimulatorでこのpacket形式を統一する。

## Simulator設定

Simulatorでは空Modelを直接指定せず、実機Modelを指定した上で `simulator = true` によるdriver差し替えを利用する。

```toml
simulator = true

[spectrometer.xffts]
_ = "XFFTS"
host = "localhost"
data_port = 25144
cmd_port = 16210
synctime_us = 100000
bw_MHz = { 1 = 2500 }
max_ch = 32768
record_quesize = 2
```

`neclib/defaults/simulator_config.toml` にはこの設定を含める。

## データフロー

```text
SpectrometerSimulator
  -> synthetic 32768 ch spectrum
  -> necst/rx/spectrometer
  -> Quick Look / spectral recording
  -> Recorder / NECSTDB
```

legacy channel binningが要求された場合は、実機XFFTSと同じ `change_spec_ch()` の呼び出しをSimulatorでも受け付け、指定channel数に切り詰めた疑似スペクトルを返す。

Simulatorのスペクトルは実際のXFFTS通信をエミュレートするものではない。XFFTSのUDP/TCP通信やfirmware動作を検証する場合は実機または別のprotocol simulatorが必要になる。

## 確認

Simulator用configを読み込んだ環境で次を起動する。

```bash
ros2 run necst record
```

従来発生していた次の例外が発生せず、分光計ノードが疑似スペクトルを継続的に取得できることを確認する。

```text
ValueError: not enough values to unpack (expected 3, got 2)
```
