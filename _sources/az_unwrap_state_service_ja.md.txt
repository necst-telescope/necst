# Az unwrap state service 運用メモ

## 結論

`antenna_az_unwrap_state.json` は、encoder_readout が読んでいる `state_path` に保存されなければ意味がない。
Docker 内の `~/.necst` が永続化されていない場合、`state_path = "~/.necst/antenna_az_unwrap_state.json"` のまま運用すると、container 再作成で branch state が消える危険がある。

本パッチでは、制御PCから直接 local file を書かず、encoder_readout node に ROS service call で依頼する経路を追加した。
実際に JSON を書くのは encoder_readout process なので、実行端末の `~/.necst` ではなく、encoder node の `state_path` に書かれる。

## 推奨設定

Docker 内の `~` に依存しないように、観測所 TOML では永続 bind mount された絶対パスを使う。

```toml
[antenna_encoder_unwrap.az]
enabled = true
state_path = "/necst_state/antenna_az_unwrap_state.json"
```

Docker 起動時には host 側の永続 directory を bind mount する。

```bash
-v /persistent/necst_state:/necst_state
```

## 通常操作

encoder_readout が起動している状態で、制御PCから次を実行する。

```bash
necst az-unwrap-state status
necst az-unwrap-state set --raw-az 8.997253 --continuous-az 368.997253
```

または ROS executable として直接呼ぶ。

```bash
ros2 run necst necst-antenna-az-unwrap-state status
ros2 run necst necst-antenna-az-unwrap-state set --raw-az 8.997253 --continuous-az 368.997253
```

これらは default では encoder_readout の service を呼ぶ。

## local fallback

encoder node が起動していない非常時だけ、encoder PC/container 内で明示的に `--local` を付けて使う。

```bash
python3 -m necst.ctrl.antenna.az_unwrap --local status
python3 -m necst.ctrl.antenna.az_unwrap --local set --raw-az 8.997253 --continuous-az 368.997253
```

`--local` は実行した process の config/state_path を直接読むため、制御PCで使うと誤った `~/.necst` に書く可能性がある。
通常運用では使わない。

## service 名

```text
/necst/<OBSERVATORY>/ctrl/antenna/az_unwrap_state/get
/necst/<OBSERVATORY>/ctrl/antenna/az_unwrap_state/set
```

`set` service は raw/continuous/branch の整合性、drive range、enabled/state_path を encoder node 側で検査し、拒否時は JSON を書かない。


## unwrap無効時のAz指令制限

absolute-modulo Az encoder 用の `[antenna_encoder_unwrap.az]` が設定されているが、`enabled = false` の場合、controllerはbranchを持たない。この状態で `Az=370 deg` のような連続Azを許すと、raw `10 deg` と区別できず危険である。

そのため、本パッチでは `enabled = false` の間、Az指令を次のraw absolute encoder範囲に制限する。

```toml
[antenna_encoder_unwrap.az]
enabled = false
"raw_min[deg]" = 0.0
"raw_max[deg]" = 360.0
```

対象は次の共通経路である。

```text
necst mount-move
operator console mount move / dry-run
Commander.antenna(... az_target_mode="mount")
horizontal_coord node が生成する最終Az command
```

`antenna_encoder_unwrap` 設定自体が存在しない望遠鏡では、この制限は入らず従来通りである。北またぎ・Az>360運用を行う場合は、`enabled = true` とし、progress/consoleでbranchとraw Azを確認する。
