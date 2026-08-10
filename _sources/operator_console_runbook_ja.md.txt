# NECST Operator Console runbook

作成日: 2026-06-05  
対象: `original zip + 06041931_v16_necst_progress_chopper_obs_defaults_upstream_patch.zip` に、本patchを適用した状態

## 1. 結論

通常観測時の起動は短くする。実機環境で active site config が正しく解決できる場合は、基本的に次だけでよい。

```bash
python3 bin/console.py --open
```

実機投入前の練習・確認では、`dry-run` で console server を起動し、`console-check.py` で自己診断を行う。

```bash
python3 bin/console.py --action-mode dry-run --open
python3 bin/console-check.py
```

`console-check.py` は read-only の確認であり、望遠鏡・chopper・観測launcherを動かさない。

## 2. 用語と正方向

```text
mount Az/El:
  架台の機械角。単位はdegree。
  Az=360をAz=0へ自動wrapしない。

STOP:
  常時使えるアンテナ停止導線。
  browser session authority gateを通さない。

ABORT observation:
  観測sequence cleanup付き中断導線。

Terminate local launcher:
  consoleが起動したローカル子プロセスだけを止める。
  望遠鏡STOPや観測cleanupは送らない。
```

## 3. dry-run 起動

実機にcommandを送らず、site config・画面・API・log directoryを確認する。通常は次でよい。

```bash
python3 bin/console.py --action-mode dry-run --open
```

実機外でsite configを明示する場合だけ、次のように指定する。

```bash
python3 bin/console.py \
  --action-mode dry-run \
  --site-config ../neclib-main/neclib/defaults/OMU1p85m_config.toml \
  --open
```

別端末で確認する。

```bash
python3 bin/console-check.py
```

consoleのURLやportを変えた場合だけ、`--url` を指定する。

```bash
python3 bin/console-check.py --url http://127.0.0.1:8092/
```

warningは、progress monitor未起動などの確認事項でも出る。errorが0であれば、read-only配線としては通っている。

## 4. live 起動前の確認

live modeへ進む前に、少なくとも次を確認する。

```text
1. site config sourceが意図したsiteである。
2. mount Az/El limitが実機のsite TOMLと一致している。
3. chopper IN/OUT位置がsiteに合っている。
4. disabled capabilityが意図通りである。
5. operator log directoryが書き込み可能である。
6. launcher log directoryが書き込み可能である。
7. /api/self-check の error_count が0である。
8. STOP / ABORT / Stop tracking が常時使える導線として表示される。
```

## 5. live 起動

通常観測時は次でよい。

```bash
python3 bin/console.py --open
```

`--action-mode` の既定値は `live` である。live modeでは、mount move / chopper / target tracking / Start observation / RSky / SkyDip などのwrite系操作が有効になる。

`--site-config` を省略した場合は、実機環境の `necst.config` / neclib config の解決結果を使う。解決できない場合、mount moveはserver側で拒否される。

status更新間隔の既定値は1秒である。必要な場合だけCLIで変更する。

```bash
python3 bin/console.py --open --status-refresh-ms 500
```

`--status-refresh-ms` はOperator Console自身の `/api/status` 更新間隔である。`--progress-refresh-ms` はconsoleが起動するprogress.py側のブラウザ更新間隔であり、別物である。


## 5.1 Current Az/El / Chopper state の取得

Operator Console は、通常起動では console process 自身が read-only ROS subscriber を持ち、
`/necst/antenna/encoder`, command, pointing status, chopper status などのlive telemetryを
`/api/status`へ反映する。したがって、通常はCurrent Az/ElとChopper IN/OUTはconsole画面だけで更新される。

```bash
python3 bin/console.py --open
```

実機外でROSなし確認をしたい場合、またはstatus取得経路を意図的に切り離す場合だけ、次を使う。

```bash
python3 bin/console.py --open --no-ros
```

`--no-ros` を指定すると、consoleはROS topicを購読しない。この場合、観測中のprogress sidecarから得られる値以外は、Current Az/ElやChopper状態が更新されないことがある。
実機でCurrent Az/Elが更新されない場合は、まずRuntime statusの `live_telemetry.available` 相当、operator logの `live_telemetry enabled/unavailable`、および `/api/status` の `live_telemetry` フィールドを確認する。

## 5.2 観測データディレクトリ表示

Runtime status の `Current observation data` には、現在の観測に対応するディレクトリを表示する。

```text
Record:
  observation record_name

Data directory:
  NECST_RECORD_ROOT/record_name
  ただし record_name が絶対pathならそのまま

Progress directory:
  NECST_PROGRESS_ROOT/<safe_record_name>
```

RecorderControllerの既定値と同じく、`NECST_RECORD_ROOT` が未設定の場合のData directoryは `~/data/<record_name>` として表示する。これは表示用の推定であり、実際の保存先はRecorderControllerの設定に従う。

## 6. smoke test の判定

通常表示:

```bash
python3 bin/console-check.py --url http://127.0.0.1:8092/
```

JSON出力:

```bash
python3 bin/console-check.py --url http://127.0.0.1:8092/ --json
```

warningも失敗扱いにする:

```bash
python3 bin/console-check.py --url http://127.0.0.1:8092/ --fail-on-warning
```

## 7. 既知の安全設計

```text
invalid mount Az/El:
  command送信前にserver側で拒否する。

browser session authority:
  Acquire authority は、console process内に保持Commanderを作り、
  mount move / chopper IN/OUT/maintenance / target tracking start で再利用する。
  このため、同じbrowser sessionからの直接Commander操作では、
  操作ごとのCommander生成・NECST privilege取得・releaseを省ける。
  保持session以外からの非安全操作は拒否する。
  STOP / Stop tracking / ABORTは安全導線として拒否しない。
  保持Commanderが外部要因でprivilegeを失った場合は、
  staleなbrowser gateを自動的に解除する。

Start / RSky / SkyDip:
  HTTP requestを観測終了までブロックしない。
  launcher subprocessとして起動し、PID・stdout/stderr・return codeを記録する。
  これらは別processでprivilegeを取得するため、consoleがAcquire authority済みなら、
  launcher commandのpreflight validationを通した後、launcher起動直前に
  console側の保持privilegeを解放する。入力不正の場合はauthorityを解放しない。
  従って、Acquire authorityは観測launcher自体を高速化しない。
  役割は、観測開始ボタンを押すまで同じconsole sessionで操作権を保持することに限られる。

console shutdown:
  console-owned authorityを解放する。
  console-owned progress monitorを停止する。
  console-owned local launcherをterminateする。
```

## 8. 実機で最初に押すべき順序

```text
1. Run self-check
2. Launch progress monitor
3. Check obs file
4. Dry run
5. Mount move dry-run
6. 必要ならAcquire authority
   - mount move / chopper / target tracking start を連続して行う場合は有効。
   - Start observation / RSky / SkyDip では、launcher起動直前にconsole側authorityを解放するため、
     観測launcherの実行時間短縮にはならない。
7. live操作へ進む
```

いきなりStart observationやmount moveを押さない。

## 8. operator log / launcher log の確認

console は操作結果を `operator_console.jsonl` に永続記録します。観測launcher / RSky / SkyDip をconsoleから起動した場合は、launcher の stdout/stderr も `launcher_logs/` 以下に保存します。

GUIでは Runtime status の `Log browser` から次を確認できます。

```text
Read operator log
  operator_console.jsonl の末尾を読む。
  操作時刻、action名、OK/NG、rejected reasonを確認する。

launcher stdout/stderr button
  consoleが起動したlocal launcher subprocessのstdout/stderr log末尾を読む。
  望遠鏡STOPやABORTは送らない。read-only確認である。
```

APIで確認する場合:

```bash
curl http://127.0.0.1:8092/api/operator-log?limit=80
curl "http://127.0.0.1:8092/api/log-file?path=/absolute/path/to/launcher.stdout.log&max_bytes=32768"
```

`/api/log-file` は任意ファイルを読ませないため、consoleが設定した operator log directory / launcher log directory / progress log directory の内側だけを許可します。観測データや任意の設定ファイルをブラウザから読む用途には使わないでください。

smoke testにも `/api/operator-log` のread-only確認を含めています。

```bash
python3 bin/console-check.py --url http://127.0.0.1:8092/
```

## 9. live / dry-run / guarded live の使い分け

通常の実機観測では、追加のarming optionは不要である。

```bash
python3 bin/console.py --open
```

実機にcommandを送りたくない検証では、`dry-run` を使う。

```bash
python3 bin/console.py --action-mode dry-run --open
```

特殊な診断として、live telemetryやread-only APIは見たいがwrite系live操作だけ止めたい場合は、`--guard-live-actions` を使える。ただし通常運用では使わない。

```bash
python3 bin/console.py --open --guard-live-actions
```

`--guard-live-actions` 中でも許可される安全操作:

```text
STOP
Stop tracking
ABORT observation
local launcher terminate/kill
```

write系操作を止めたい通常の確認では、`--guard-live-actions` より `--action-mode dry-run` を推奨する。
