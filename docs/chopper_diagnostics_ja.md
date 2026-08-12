# Chopper待機停止の診断ログ

SkyDipなどの観測がchopper待機中に停止した場合、CommanderとChopper Controllerのログを同じ`command_time`で対応付ける。

## ログの流れ

正常時は次の順序で出力される。

1. Commander: `Chopper command published`
2. Chopper Controller: `Chopper command received`
3. Chopper Controller: `Chopper status published: source=before_move`
4. Chopper Controller: `Chopper motor command returned`
5. Chopper Controller: `Chopper status published: source=after_move`
6. Chopper Controller: `Chopper move completed`
7. Commander: `Chopper wait completed`

Commanderが目標と異なるstatusを受信し続ける場合は、5秒間隔で`Chopper wait pending: status mismatch`を出力する。statusを一度も受信できない場合は`Chopper wait pending: no status received`を出力する。

## 停止箇所の判定

| 最後に確認できたログ | 主な確認対象 |
| --- | --- |
| `command published`のみ | ROS 2のcommand配送、Chopper Controllerの起動状態・callback実行 |
| `command received`のみ | Controller側の`get_step()`、モーター通信 |
| `source=before_move`まで | `motor.set_step()`の停止・機械動作 |
| `motor command returned`まで | 移動後の`get_step()`、status生成・publish |
| `move failed` | 同じ行の例外型・例外メッセージ |
| `move completed`まで | status topicの配送、Commander側subscription・cache |
| `wait pending: status mismatch` | `status_insert`、`status_position`、`status_age_sec` |

`status_precedes_command=True`は、Commanderが現在のcommandより古いstatusを参照したことを示す。これは直ちに異常とは限らないが、同じstatusの`status_age_sec`が増え続ける場合は、Controllerから新しいstatusが届いていない可能性が高い。

この診断ログは原因切り分け用であり、chopper待機のタイムアウトや観測abortなどの動作は変更しない。
