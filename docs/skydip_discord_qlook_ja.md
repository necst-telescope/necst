# SkyDip解析結果のDiscord投稿

SkyDip終了後の解析とDiscord投稿は、既存NECST Dockerコンテナ内の別ROS 2
`analysis` Nodeで実行する。Recorder Nodeは変更せず、`observation_progress` の
`finished` スナップショットと `record_status` の停止通知を両方受け取った後に、
解析をワーカースレッドへ渡す。

## コンテナ内の設定

RecorderとAnalysisが同じデータを見るように、ホストの保存先を `/data` へbind
mountする。

Analysis Nodeが使う `skydip_step_jupyter_necstdb_v10.py` を同じNECSTパッケージへ
配置する。スクリプトの解析・グラフ生成処理は変更せず、Analysis Nodeから
`analyze_skydip_boards()` を呼び出す接続層だけを追加する。実行時には
`numpy`、`pandas`、`matplotlib`、`necstdb` が必要になる。

```bash
docker run --network=host \\
  -v /home/necst/data:/data \\
  --env-file /etc/necst/secrets/discord.env \\
  -e NECST_RECORD_ROOT=/data \\
  necst:latest
```

`/etc/necst/secrets/discord.env` はGit管理外で、例えば次の2項目だけを置く。

```dotenv
DISCORD_BOT_TOKEN=...
DISCORD_CHANNEL_ID=...
```

`--env-file` はDocker起動時に読むホスト側ファイルのパス指定であり、
`DISCORD_BOT_TOKEN`の値をtelescope configやリポジトリへ書く必要はない。
env-fileはGit管理外に置き、所有者だけが読めるようにする。

```bash
chmod 600 /etc/necst/secrets/discord.env
```

env-fileをread-onlyでmountし、Analysis Nodeだけを再起動する運用にすれば、
Channel ID変更のたびにRecorderやコンテナ全体を再起動する必要はない。環境変数は
Analysis Nodeの起動時に読み込まれるため、変更後はAnalysis Nodeの再起動が必要になる。

## Nodeの起動

Recorderとは別プロセスで、同じコンテナ内から起動する。

```bash
ros2 run necst record
ros2 run necst analysis
```

`analysis` NodeはSkyDipだけを対象にし、SkyDip以外の観測終了は無視する。
同梱したv10スクリプトの `analyze_skydip_boards()` が解析とグラフ生成を行い、
返されたFigureを`BytesIO`へPNG化してDiscordへ直接添付する。解析失敗やDiscord
通信失敗はAnalysis Node側で記録し、Recorderのデータ保存処理へ例外を返さない。

## 環境変数

| 変数 | 必須 | 用途 |
| --- | --- | --- |
| `NECST_RECORD_ROOT` | 推奨 | コンテナ内の共通データルート。例 `/data` |
| `DISCORD_BOT_TOKEN` | Analysis起動時 | Discord Bot Token |
| `DISCORD_CHANNEL_ID` | Analysis起動時 | 投稿先チャンネルID |
| `NECST_SKYDIP_SCRIPT` | 任意 | 外部スクリプトを使う場合のパス。通常は同梱v10を使用 |
| `NECST_SKYDIP_BOARDS` | 任意 | 解析対象boardのカンマ区切り。未指定時はnecstdbから自動検出 |
| `NECST_SKYDIP_TELESCOPE` | 任意 | スクリプトへ渡す望遠鏡名。既定値 `OMU1P85M` |

Analysis Nodeは、指定されたDiscord設定が不足している場合は起動時に失敗する。
これにより、通知が無効なまま運用されることを防ぐ。
