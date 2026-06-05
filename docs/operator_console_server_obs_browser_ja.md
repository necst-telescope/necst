# NECST Operator Console: NECST側 file chooser

対象: `06051950_ver36_basev24_console_modal_file_chooser_patch.zip`

## 1. 結論

Operator Console の観測指示書選択は、ブラウザ標準の file chooser ではなく、**NECST console を起動している側のファイルシステム**を GUI から選ぶ方式である。

- console が Docker 内で起動している場合: Docker/container 内のファイルを見る。
- console が物理PC上で起動している場合: そのPC上のファイルを見る。
- 選んだ `.obs` / `.toml` は、Preview / Check / Dry run / Start で同じ path を使う。

ブラウザ標準 file chooser は、ブラウザを開いているPCのファイルしか見えない。そのため、Docker 内や NECST 側の実行 path とは一致しない。このずれを避けるため、Operator Console では NECST側 file chooser を使う。

## 2. 基本操作

通常起動:

```bash
necst console --open
```

Observation setup の `Obs file on NECST side` の横にある `Choose...` を押す。

ダイアログが開くので、通常の file chooser と同じ感覚でフォルダを開き、`.obs` または `.toml` をクリックする。

クリックしたファイルについて、次が自動で更新される。

```text
Obs file on NECST side
Obs directory on NECST side
Obs filename
Preview text
```

この `Obs file on NECST side` が、Check / Dry run / Start observation に使われる実行 path である。

## 3. 画面上の考え方

通常画面には、大きなファイル一覧を常時表示しない。

```text
通常画面:
  選択済みobs file
  Choose...
  Preview
  Check / Dry run / Start

Choose... ダイアログ:
  NECST側のフォルダ一覧
  .obs/.toml ファイル一覧
  Home / Up / Refresh
  Current folder
```

これにより、観測開始画面のスペースを無駄にせず、必要な時だけ file chooser を開く。

## 4. `--obs-root` を指定しない場合

`--obs-root` は必須ではない。指定しない場合でも、console は NECST console を起動している側から見える一般的な場所を開始候補として出す。

例:

```text
Home
Current directory
~/obs
~/observations
~/data
/root
/root/obs
/root/observations
/root/data
/root/ros2_ws
/data
/data/obs
/data/observations
/workspace
/workspaces
/
```

存在するディレクトリだけを表示する。`/` も候補に入るので、通常の file chooser に近い感覚で上位階層からたどれる。

## 5. `--obs-root` を指定する場合

観測指示書の置き場を限定したい場合、または特定ディレクトリを最初から出したい場合だけ明示する。

```bash
necst console --open --obs-root /root/obs
```

複数指定も可能:

```bash
necst console --open \
  --obs-root /root/obs \
  --obs-root /data/observations/current
```

環境変数でも指定できる。

```bash
export NECST_CONSOLE_OBS_ROOTS=/root/obs:/data/observations/current
necst console --open
```

`--obs-root` は必須設定ではなく、開始場所を絞るための補助である。

## 6. ブラウザPCの file chooser との違い

```text
ブラウザ標準 file chooser:
  ブラウザPCのファイルを見る
  Docker内/NECST側の絶対pathは取得できない

Operator Console の NECST側 file chooser:
  consoleを起動している側のファイルを見る
  Docker内で起動していればDocker内を見る
  previewしたファイルとStartするファイルが一致する
```

## 7. local preview

`Optional local preview only` は比較用である。Start observation には使わない。

通常運用では、`Choose...` で開く NECST側 file chooser から観測指示書を選ぶ。

## 8. preview制限

preview は観測指示書向けに限定し、`.obs`, `.toml`, `.txt` だけを表示する。

ネットワーク越しに console を公開する運用では、必要に応じて `--obs-root` で開始場所を観測指示書ディレクトリに限定する。

## 9. demoでの表示

`bin/console-demo.py` を単独で開いた場合、実際の NECST 側ファイルシステムは読めない。その場合は demo 用の仮想 file chooser を表示する。

実運用の `necst console --open` では、実際に console を起動している側のファイルシステムを表示する。

## 10. 確認方法

起動後に以下を確認する。

```text
Choose... を押すと NECST側 file chooser ダイアログが開く
フォルダをクリックするとその中へ移動する
Up / Home / Refresh が動く
.obs / .toml をクリックするとダイアログが閉じ、preview が出る
Obs file on NECST side が preview と同じ path になる
Check / Dry run / Start が同じ path を使う
```
