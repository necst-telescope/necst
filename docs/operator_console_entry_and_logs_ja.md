# NECST Operator Console 起動コマンドとログ運用（basev24 ver29）

## 1. 結論

Operator Console GUI は次で起動できる。

```bash
necst console --open
```

ROS の executable として明示する場合は次でもよい。

```bash
ros2 run necst necst console --open
ros2 run necst necst-console --open
```

従来通り、source tree から直接起動する場合は次も使える。

```bash
python3 bin/console.py --open
```

## 2. operator log の保存先

Console は operator action を JSONL 形式で追記保存する。既定保存先は次である。

```text
~/.necst/operator_console/operator_console.jsonl
```

`NECST_OPERATOR_LOG_DIR` を設定すると既定保存先を変更できる。CLI で明示する場合は `--operator-log-dir` を使う。

```bash
necst console --open --operator-log-dir /data/necst/operator_console
```

`operator_console.jsonl` は同じファイルへ追記する。各行は1つのJSON objectで、時刻、成功/失敗、action名、message、session id、関連dataを含む。

## 3. launcher / progress log

観測、RSky、SkyDip など console が起動した subprocess の stdout/stderr は、既定では operator log directory 配下に保存される。

```text
~/.necst/operator_console/launcher_logs/
~/.necst/operator_console/progress_logs/
```

operator log は「誰がいつ何を押したか、何が拒否されたか、どの launcher が起動したか」を追うための log である。一方、launcher log は各観測 subprocess の標準出力・標準エラーを見るための log である。

## 4. log の確認

GUI の Runtime status には operator log path と launcher log directory が表示される。`Read operator log` で `operator_console.jsonl` の末尾を読める。Log browser から launcher stdout/stderr の tail も確認できる。

端末から確認する場合は次を使う。

```bash
necst console-log path
necst console-log tail --limit 80
necst console-log cat
```

ROS executable として呼ぶ場合は次でもよい。

```bash
ros2 run necst necst-console-log tail --limit 80
```

## 5. 運用上の推奨

観測 trace を残す目的では、operator log directory は `/tmp` ではなく、制御PC上の永続ディレクトリ、または永続 bind mount された directory にする。

推奨例:

```bash
export NECST_OPERATOR_LOG_DIR=/data/necst/operator_console
necst console --open
```

これにより、console を再起動しても同じ `operator_console.jsonl` に追記される。
