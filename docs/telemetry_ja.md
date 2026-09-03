# NECST telemetry

`necst-telemetry` は、`[telemetry.topics.<name>]` に明示したROS Topicの指定fieldだけを
購読し、意味のあるMetric名で数値スカラーの最新値をNew Relic Metric APIへ定期送信する。

## 設定

サイトTOMLに以下を追加する。OMU 1.85mの具体例は
[`docs/examples/OMU1p85m_telemetry.toml`](examples/OMU1p85m_telemetry.toml)にある。

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

[telemetry.topics.encoder]
topic = "/necst/OMU1P85M/ctrl/antenna/pointing_status"
fields = [
  { path = "enc_az_deg", metric = "necst.antenna.encoder.az_deg" },
  { path = "enc_el_deg", metric = "necst.antenna.encoder.el_deg" },
  { path = "tracking_error_deg", metric = "necst.antenna.tracking.error_deg" },
]
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

`TELESCOPE` はNew Relic上の望遠鏡識別に使う。telemetryは
`[console.health].nodes`を参照せず、`[telemetry.topics.<name>]`に指定されたTopicだけを対象にする。
Topic設定は名前付きのtableでまとめて記述する。既存の`[[telemetry.topics]]`形式も読み込める。
複数の子Topicをまとめて購読する場合はTopic末尾に`/*`を指定する。この場合、実際の子Topic名を
Metric名の末尾へ自動的に追加する。例えば`/weather/ambient/*`の`out`は
`necst.weather.temperature_k.out`になるため、`in`と混ざらない。
Topicのmessage型はROS graphから取得し、複数のmessage型が見つかったTopicは送信しない。

## Topicと値

`fields`には送信するfield pathとMetric名を明示する。field pathはネストした
fieldを`.`でつなぐ。Topic名は完全修飾名で指定するため、ネストしたTopicや同一階層の
並列Topicを混同しない。

指定fieldが有限な数値スカラーまたはboolの場合だけ送信し、配列・文字列・複雑な
オブジェクトは送信しない。高頻度の生データはRecorderの責務とし、telemetryは最新値を
10秒ごとにまとめて送信する。

Metric名はfieldごとに設定し、wildcard Topicでは子Topic名を自動追加する。`telescope`をcommon attribute、`ros_topic`、
`field_path`、`ros_type`、`value_kind`をmetric attributeにする。Metric APIが要求する
timestampはバッチ単位の`common.timestamp`としてPOST直前に付与し、各fieldへは重複して付けない。
HTTP通信はROS callbackとは別の送信スレッドで行い、1リクエストあたり最大500メトリクスに分割する。

## 今後の拡張

Event APIやLog送信はこのPRでは実装しない。将来は現在のTopic・ノード・望遠鏡・
フィールドのコンテキストを再利用して、ノード状態変化や通信障害をEventとして
送信する。
