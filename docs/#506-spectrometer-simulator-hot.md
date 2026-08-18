# #506 Spectrometer simulator HOT load

## 目的

NECSTを `simulator = true` で動かしたとき、chopperがHOT load位置へinsertされたことを疑似分光データへ反映する。

実機XFFTSの処理経路にはSimulator用の信号生成ロジックを入れない。

## 実機とSimulatorの分離

record executorは起動時に一度だけ分光データノードを選択する。

- `simulator = false`: `SpectralData`
- `simulator = true`: `SimulatedSpectralData`

`SpectralData` はHOT生成を知らない。Simulator固有のchopper購読と疑似HOT状態の設定は `SimulatedSpectralData` にのみ実装する。

## HOT判定

`ChopperMsg.insert` は互換性のため中間位置でもtrueになり得るため、HOT判定には使用しない。

```python
hot = msg.position == config.chopper_motor_position["insert"]
```

chopperがinsert位置に到達した場合のみneclibの `SpectrometerSimulator.set_hot(True)` を呼ぶ。removeまたは移動途中では `False` とする。

## HOT強度

疑似スペクトルの通常状態をSKY基準とし、neclibのSimulator専用設定

```toml
[spectrometer.xffts]
simulator_t_rx_K = 150.0
simulator_t_sky_K = 70.0
simulator_t_hot_K = 293.0
```

から

```text
HOT / SKY = (T_rx + T_hot) / (T_rx + T_sky)
```

を計算して広帯域の強度をスケールする。上記の既定値では約2.01倍になる。

## 今回含めないもの

- ON/OFFラベルに応じたGaussian line
- `recording_window_setup.toml` / LO profileを使ったvelocity-to-channel変換
- Skydipの大気放射モデル

将来ON信号を追加する際は、ONラベルだけを根拠にGaussianを生成しない。SkydipでもON相当のラベルが使われるため、観測種別と天体ON-sourceを区別する。
