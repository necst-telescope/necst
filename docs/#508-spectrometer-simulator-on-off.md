# Spectrometer Simulator ON/OFF signal

## 目的

Spectrometer Simulatorで通常のON/OFF観測を行ったとき、ONだけに設定したGaussian lineを重畳する。

実機処理とSimulator処理は分離する。実機用 `necst/rx/spectrometer.py::SpectralData` や実機XFFTS driverには疑似信号生成処理を追加しない。

## レイヤー分離

```text
NECST simulator layer
  - 観測状態 ON/OFF/SKY/HOT を判断
  - recording window_id を active spectral snapshotから検索
  - rest frequency / LO chain / frequency axis / LSRK補正を使用
  - 各full channelの速度軸を生成
            |
            v
neclib SpectrometerSimulator
  - 渡された速度軸上でGaussianを評価
  - ON時のみ T_line(v) を加算
  - OFF/SKYではlineなし
  - HOTではHOT loadを優先しlineなし
```

NECSTが「どの観測状態か」「設定した分子線がどのboard/channelに対応するか」を解決し、neclibは観測やROS、LO設定を知らずに疑似スペクトルだけを生成する。

## Signal設定

Signalは `recording_window_setup.toml` の `window_id` と同じ名前で指定する。

```toml
[spectrometer.simulator.on_line.12CO_J2_1]
v_center_kms = -7.0
fwhm_kms = 3.0
t_line_peak_K = 20.0

[spectrometer.simulator.on_line.13CO_J2_1]
v_center_kms = -7.0
fwhm_kms = 2.0
t_line_peak_K = 8.0
```

`[[...]]` のarray-of-tablesは使用しない。

- `v_center_kms`: Gaussian中心速度
- `fwhm_kms`: Gaussian FWHM
- `t_line_peak_K`: line peak temperature

Board番号、rest frequency、LO周波数、channel番号はsignal設定に重複して指定しない。これらはactive spectral recording snapshotから解決する。

同一boardに複数のwindowが存在する場合も、それぞれの `window_id` について別々の速度軸を作り、同じraw board spectrumへ複数Gaussianを加算できる。

## Gaussian

Gaussianはchannel番号ではなく速度空間で直接評価する。

```text
T_line(v) = T_peak * exp[-4 ln(2) * ((v - v_center) / FWHM)^2]
```

各windowについてsnapshotからfull-channelのsky frequency axisを復元し、rest frequencyとvelocity definitionを使ってvelocity axisへ変換する。LSRK windowではsnapshotに保存された `vlsrk_correction_kms` も適用する。

## 状態ごとの疑似データ

```text
OFF / SKY
  T_rx + T_sky

ON
  T_rx + T_sky + T_line(v)

HOT
  T_rx + T_hot
```

HOT loadがinsertされている間は空を遮るため、ON stateが残っていてもGaussianは重畳しない。

## Skydip

現在のSkydipは `SKY` と `HOT` metadataを使用するためGaussianは生成されない。

SimulatorのGaussian有効化条件は `metadata position == "ON"` のみとし、`SKY`、`HOT`、`OFF`、空状態では必ず無効にする。将来Skydip側のラベル表現を変更する場合も、SkydipでGaussianを生成しない制約を維持すること。

## 関連

- NECST #508: Spectrometer simulatorでON/OFF疑似スペクトルを再現する
- NECST #506 / PR #507: HOT load simulator
- neclib #803: SpectrometerSimulator Gaussian generation
- neclib #796 / PR #802: Spectrometer simulator recording support
