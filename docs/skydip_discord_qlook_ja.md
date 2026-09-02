# SkyDip / R-Sky解析結果のDiscord投稿

SkyDip / R-Sky終了後の解析とDiscord投稿は、既存NECST Dockerコンテナ内の別ROS 2
`analysis` Nodeで実行する。Recorder Nodeは変更せず、`observation_progress` の
`finished` スナップショットと `record_status` の停止通知を両方受け取った後に、
解析をワーカースレッドへ渡す。

## コンテナ内の設定

RecorderとAnalysisが同じデータを見るように、ホストの保存先を `/data` へbind
mountする。

Analysis Nodeが使うSkyDipスクリプトを同じNECSTパッケージへ配置する。SkyDipの解析・
グラフ生成処理は変更せず、Analysis Nodeから`analyze_skydip_boards()`を呼び出す。
添付の`rsky_20241209.ipynb`
に合わせ、各spectral tableのHOT/SKY平均から
`Y=HOT/SKY`、`Tsys=290/(Y-1)`を計算してIFごとのグラフを生成する。数値マトリクスは
共有しない。実行時には
`numpy`、`pandas`、`matplotlib`、`necstdb` が必要になる。

```bash
docker run --network=host \\
  -v /home/necst/data:/data \\
  -e NECST_RECORD_ROOT=/data \\
  -e TELESCOPE=OMU1p85m \\
  necst:latest
```

以下は、既存のNECST configディレクトリがコンテナ内から見える構成を前提とする。

既存のNECST site configにenvファイルのパスだけを指定する。

```toml
[environment]
env_file = "/root/necst_ws/.env"
```

`/root/necst_ws/.env` はGit管理外で、例えば次の項目を置く。

```dotenv
DISCORD_BOT_TOKEN=...
DISCORD_CHANNEL_ID=...
# 任意。未指定時は10 MiB
DISCORD_ATTACHMENT_LIMIT_MIB=10
```

`TELESCOPE`は共有env-fileではなく、Analysis Nodeを起動するコンテナの環境変数として設定する。
値は通知内で大文字化される（例: `OMU1p85m` → `OMU1P85M`）。

Analysis Nodeはsite configの`env_file`を起動時に読み込む。
`DISCORD_BOT_TOKEN`の値をtelescope configやリポジトリへ書く必要はない。
env-fileはGit管理外に置き、所有者だけが読めるようにする。

```bash
chmod 600 /root/necst_ws/.env
```

Board番号とIF表示名は、NECST site configの1項目で対応付ける。
以下の設定は現在の4 board構成に対応する。

```toml
[analysis]
board_labels = [
  { name = "xffts-board1", label = "Band 6 USB" },
  { name = "xffts-board2", label = "Band 6 LSB" },
  { name = "xffts-board3", label = "Band 3 USB" },
  { name = "xffts-board4", label = "Band 3 LSB" },
]
```

`board_labels`は`hosts`設定と同じinline table配列で指定する。

この設定は解析スクリプトへ`{board: label}`形式で渡され、グラフのタイトルや
Discordの数値サマリーに使用できる。未設定のboardはtable名をそのまま表示する。

secretファイルが既存のNECST configディレクトリからコンテナ内にも見えていれば、
Channel ID変更後はAnalysis Nodeだけを再起動すればよい。Recorderやコンテナ全体の
再起動は不要である。

```bash
ros2 run necst analysis
```

環境変数はAnalysis Nodeの起動時に読み込まれるため、ファイル変更後はAnalysis Nodeの
再起動が必要になる。configディレクトリ自体がコンテナから見えない構成では、そこだけ
一度bind mountする必要がある。

## Nodeの起動

Recorderとは別プロセスで、同じコンテナ内から起動する。

```bash
ros2 run necst record
ros2 run necst analysis
```

`analysis` Nodeは全観測モードの正常終了時に、望遠鏡名・観測モード・観測データ名を含む
正常終了通知を送信する。SkyDip / R-Skyの場合はその後、録音停止を確認してから解析結果の通知も送信する。
同梱したv10スクリプトの `analyze_skydip_boards()` が解析とグラフ生成を行い、
返されたFigureを`BytesIO`へPNG化してDiscordへ直接添付する。SkyDipでは従来どおり、
board別の数値サマリーと解析エラーを本文へ付ける。
解析失敗やDiscord通信失敗はAnalysis Node側で記録し、Recorderのデータ保存処理へ例外を返さない。
PNGが添付上限を超えた場合は画像を送信せず、Analysis Nodeへ`WARNING`を出し、
Discordには容量超過で画像を送信できなかった旨をテキストで投稿する。

解析中は、スクリプト読み込み、Board自動検出、同梱スクリプト実行の開始・完了もログに
出る。`Analysis script execution started`の後で止まる場合は、同梱スクリプトのデータ
読み込みまたはBoard解析中であり、`Analysis script execution completed`が出てから
`Analysis completed`が出るまでが結果整形中である。
Board解析に失敗した場合は、`ERROR`ログにBoard名、例外種別、エラーメッセージ、tracebackを
出す。Boardごとに解析を継続し、成功Boardがあれば成功Boardのグラフと失敗Boardの
`ERROR`行を同じDiscord投稿へ付ける。全Boardが失敗した場合も、グラフなしのテキスト
投稿として失敗内容をDiscordへ送信する。

SkyDipのBoard解析自体の例外は、数値サマリーの後に`Analysis errors:`として表示する。

## 環境変数

| 変数 | 必須 | 用途 |
| --- | --- | --- |
| `NECST_RECORD_ROOT` | 推奨 | コンテナ内の共通データルート。例 `/data` |
| `TELESCOPE` | 必須 | 望遠鏡名。通知には大文字で表示 |
| `DISCORD_BOT_TOKEN` | env-file内 | Discord Bot Token |
| `DISCORD_CHANNEL_ID` | env-file内 | 投稿先チャンネルID |
| `DISCORD_ATTACHMENT_LIMIT_MIB` | 任意 | 添付上限。未指定時は `10` MiB |
| `[analysis].board_labels` | 任意 | board名とIF表示名の対応表 |
| `environment.env_file` | 推奨 | Discord/telemetry共通のenv-fileを指定 |
| `NECST_SKYDIP_SCRIPT` | 任意 | 外部スクリプトを使う場合のパス。通常は同梱v10を使用 |
| `NECST_SKYDIP_BOARDS` | 任意 | 解析対象boardのカンマ区切り。未指定時はnecstdbから自動検出 |
| `NECST_DISCORD_SHARE` | 内部 | Consoleの共有トグルから観測プロセスへ渡す。通常は手動設定不要 |

Analysis Nodeは、指定されたDiscord設定が不足している場合は起動時に失敗する。
これにより、通知が無効なまま運用されることを防ぐ。
