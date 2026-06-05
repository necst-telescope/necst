# NECST Operator Console: NECST側 file chooser

対象: `06051943_ver35_basev24_console_necst_side_file_chooser_patch.zip`

## 1. 結論

Operator Console の観測指示書選択は、ブラウザPCの通常 file chooser ではなく、**NECST console を起動している側のファイルシステム**を GUI 内で参照する方式にした。

- console が Docker 内で起動している場合: Docker/container 内のファイルを見る。
- console が物理PC上で起動している場合: そのPC上のファイルを見る。
- 選んだ `.obs` / `.toml` は、Preview / Check / Dry run / Start で同じ path を使う。

これにより、ブラウザPCのローカルファイルを選んだつもりで、Docker 内の観測実行 path とずれる問題を避ける。

## 2. 基本操作

通常起動:

```bash
necst console --open
```

GUI の Observation setup で、`NECST-side file location` から開始場所を選び、下の file chooser でフォルダを開いて `.obs` または `.toml` をクリックする。

クリックしたファイルについて、次が自動で更新される。

```text
Obs directory on NECST side
Obs filename
Computed run path on NECST side
Preview text
```

この `Computed run path` が、Check / Dry run / Start observation に使われる実行 path である。

## 3. `--obs-root` を指定しない場合

`--obs-root` を指定しない場合でも、GUI は空にならない。console は、NECST console を起動している側から見える次のような場所を開始候補として出す。

```text
Home
Current directory
~/obs
~/observations
~/data
/root/obs
/root/observations
/root/data
/root/ros2_ws
/root
/data
/data/obs
/data/observations
/workspace
/workspaces
/
```

存在するディレクトリだけを表示する。`/` も候補に入るので、通常の file chooser に近い感覚で上位階層からたどれる。

## 4. `--obs-root` を指定する場合

観測指示書の置き場を限定したい場合、または特殊な場所を最初から出したい場合だけ、明示する。

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

`--obs-root` または `NECST_CONSOLE_OBS_ROOTS` を指定した場合は、それらを file chooser の開始場所として使う。

## 5. ブラウザPCの file chooser との違い

HTML の通常 file chooser は、ブラウザを開いているPCのファイルしか見えない。そのため、Docker 内や NECST 側の path は取得できない。

今回の GUI 内 file chooser は、console が `/api/obs-list` と `/api/obs-preview` で NECST 側ファイルを一覧・previewする。

つまり:

```text
通常のブラウザ file chooser:
  ブラウザPCのファイルを見る

Operator Console の NECST-side file chooser:
  consoleを起動している側のファイルを見る
```

## 6. local preview

`Optional local preview only` は残しているが、これは比較用である。Start observation には使わない。

通常運用では、NECST-side file chooser で選んだファイルを使う。

## 7. 読み取り範囲とpreview

既定では通常の file chooser に近く使えるよう、`/` も開始候補に含める。ただし、preview は観測指示書向けに限定し、`.obs`, `.toml`, `.txt` だけを表示する。

ネットワーク越しに console を公開する運用では、必要に応じて `--obs-root` で開始場所を観測指示書ディレクトリに限定する。

## 8. demoでの表示

`bin/console-demo.py` を単独で開いた場合、実際の NECST 側ファイルシステムは読めない。その場合は demo 用の仮想 file chooser を表示する。

実運用の `necst console --open` では、実際に console を起動している側のファイルシステムを表示する。

## 9. 確認方法

起動後に以下を確認する。

```text
NECST-side file location が表示される
フォルダをクリックするとその中へ移動する
Up / Home / Refresh が動く
.obs / .toml をクリックすると preview が出る
Computed run path が preview と同じ path になる
Check / Dry run / Start が同じ path を使う
```
