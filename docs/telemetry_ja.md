# NECST telemetry

`necst-telemetry` は、`[console.health].nodes` に設定されたROSノードが
publishするTopicを動的に発見し、数値スカラーの最新値をNew Relic Metric APIへ
定期送信する。

## 設定

サイトTOMLに既存の `[console.health]` と、以下を追加する。

Discordやtelemetryで共用する環境ファイルは、機能個別ではなく
`[environment]` に指定する。

```toml
[environment]
env_file = "/root/necst_ws/.env"
```

```toml
[telemetry]
post_interval_sec = 10.0
discovery_interval_sec = 10.0
```

`telemetry` nodeを起動した時点でtelemetryは有効になる。

環境ファイルにはAPIキーを置く。プロセス環境に同じ変数が設定済みの場合は、
プロセス環境を優先する。

```sh
NEW_RELIC_LICENSE_KEY='...'
```

`TELESCOPE` は既存のプロセス環境変数を使用する。

起動時にサイトTOMLの環境ファイルが読み込まれる。

```sh
ros2 run necst telemetry
```

`TELESCOPE` はNew Relic上の望遠鏡識別に使う。対象ノードの一覧は
`[console.health].nodes`だけを参照し、telemetry側では重複管理しない。

## Topicと値

ROS graph introspectionで完全修飾Topic名とmessage型を取得するため、ネストした
Topicや同一階層の並列Topicも別々に扱う。同じTopicを複数ノードがpublishする場合は
subscribeを1つにまとめる。型が衝突するTopicは送信しない。

送信するのは有限な数値スカラーとboolのみで、配列・文字列・複雑なオブジェクトは
送信しない。高頻度の生データはRecorderの責務とし、telemetryは最新値を10秒ごとに
まとめて送信する。

Metric名は `necst.ros.topic` で固定し、`telescope`、`ros_topic`、`field_path`、
`ros_type` を属性にする。HTTP通信はROS callbackとは別の送信スレッドで行い、
1リクエストあたり最大500メトリクスに分割する。

## 今後の拡張

Event APIやLog送信はこのPRでは実装しない。将来は現在のTopic・ノード・望遠鏡・
フィールドのコンテキストを再利用して、ノード状態変化や通信障害をEventとして
送信する。
