# PR8g update13 徹底確認レポート

Version: update13_deep_review_2026-04-30  
対象ZIP: `u13_review.zip` を正本として再展開し、元ZIP群へ重ねた統合tree  
結論: snapshot自動検出の基本仕様は妥当。ただし、再確認で legacy DB 判定に実害のある境界条件を見つけたため、コードとdocsを修正した。

---

## 1. 確認した仕様

### 1.1 converter

期待仕様:

```text
明示 --spectral-recording-snapshot
  -> 指定snapshotを使う。

明示 --spectrometer-config
  -> 指定configを使う。

どちらも未指定で RawData 内に spectral_recording_snapshot.toml が一意にある
  -> DB内snapshotを自動検出して使う。

どちらも未指定で RawData 内に snapshot も new spectral setup sidecar も無い
  -> 従来の legacy single-stream fallback。

どちらも未指定で RawData 内に new spectral setup sidecar はあるが snapshot が無い
  -> incomplete new DB としてエラー。
```

実装確認結果:

```text
necst_v4_sdfits_converter.py:
  _discover_default_spectral_recording_snapshot(rawdata_path)
  -> discover_db_sidecar_files(..., recursive=True, strict_duplicates=True)
  -> snapshot_path があれば args.spectral_recording_snapshot へ設定
  -> setup sidecarありsnapshot無しはlegacy fallbackしない
```

### 1.2 sunscan extract / singlebeam

期待仕様:

```text
明示 --spectral-recording-snapshot
  -> 指定snapshotを使う。

明示 --spectrometer-config
  -> 指定configを使う。

どちらも未指定で RawData 内に snapshot が一意にある
  -> DB内snapshotを自動検出して使う。

どちらも未指定で RawData 内に snapshot が無い legacy DB
  -> 従来どおり明示configを要求する。
```

実装確認結果:

```text
config_io.py:
  discover_default_spectral_recording_snapshot(rawdata_path)

sunscan_extract_multibeam.py:
  run_extract() 内で自動検出
  main/config_from_args 経路でも自動検出

sunscan_singlebeam.py:
  config_from_args() で最初の RawData から自動検出
```

---

## 2. 見つけた不具合と修正

### 2.1 不具合: `.obs` / `config.toml` だけで incomplete new DB と誤判定し得る

update13の再確認で、`SidecarDiscovery.has_new_sidecars()` が `.obs` と generic config file も new sidecar 判定に含めていることを確認した。

問題となる条件:

```text
RawData内に spectral_recording_snapshot.toml は無い。
RawData内に lo_profile.toml / recording_window_setup.toml / beam_model.toml / pointing_param.toml も無い。
しかし RawData内に *.obs や *_config.toml がある。
```

この場合、本来は legacy DB と見なすべきである。ところが旧実装では `.obs` / config の存在だけで `has_new_sidecars() == True` となり、converterの legacy single-stream fallback や sunscanのlegacy明示config要求より前に incomplete new DB エラーになり得た。

### 2.2 修正内容

`SidecarDiscovery.has_new_sidecars()` を修正し、new spectral setup sidecar として扱うものを次に限定した。

```text
spectral_recording_snapshot.toml
lo_profile.toml
recording_window_setup.toml
beam_model.toml
pointing_param.toml
```

`.obs`、`config.toml`、`*_config.toml` は discovery 結果には保持するが、incomplete new DB 判定には使わない。

修正理由:

```text
.obs や generic config は旧RawDataにも存在し得る。
それだけで新spectral recording setupを開始した証拠にはならない。
legacy fallbackを阻害してはいけない。
```

---

## 3. シミュレーション確認

### 3.1 sidecar discovery

修正後の確認結果:

```text
legacy_empty:
  snapshot None
  has_new_sidecars False
  OK

obs_only:
  snapshot None
  has_new_sidecars False
  OK

config_only:
  snapshot None
  has_new_sidecars False
  OK

underscore_config_only:
  snapshot None
  has_new_sidecars False
  OK

flat_snapshot:
  snapshot spectral_recording_snapshot.toml
  has_new_sidecars True
  OK

nested_snapshot:
  snapshot metadata/config/spectral_recording_snapshot.toml
  has_new_sidecars True
  OK

duplicate_snapshot:
  SpectralRecordingSnapshotError

lo_profile_without_snapshot:
  incomplete new DB error

recording_window_setup_without_snapshot:
  incomplete new DB error

beam_model_without_snapshot:
  incomplete new DB error

pointing_param_without_snapshot:
  incomplete new DB error
```

### 3.2 pytest

対象:

```text
tests/test_necst_config_separation_snapshot_bundle.py
tests/test_necst_snapshot_stream_truth_beam_override.py
```

追加した回帰テスト:

```text
test_db_sidecar_discovery_does_not_treat_obs_or_config_as_new_setup
test_db_sidecar_discovery_rejects_incomplete_spectral_setup
```

結果:

```text
6 tests passed
```

### 3.3 構文確認

統合tree全体:

```text
Python files: 594
py_compile: OK
```

---

## 4. docs更新

以下のdocsに、legacy DB判定の境界条件を追記した。

```text
docs/manual_full_update13_ja.md
docs/manual_quick_update13_ja.md
docs/manual_update13_report.md
docs/u13_report.md
docs/preflight/update13_snapshot_autodiscovery_report.md
docs/preflight/update13_snapshot_autodiscovery_review_2026-04-30.md
```

追記した要点:

```text
`.obs` / `config.toml` / `*_config.toml` だけでは incomplete new DB とは判定しない。
incomplete new DB とするのは、lo_profile.toml / recording_window_setup.toml /
beam_model.toml / pointing_param.toml などの spectral setup sidecar があるのに
spectral_recording_snapshot.toml が無い場合である。
```

---

## 5. 適用可否

この修正版を使う前提であれば、update13は適用してよい。

ただし、次は実機環境で必ず確認する。

```text
1. ROS2 colcon build
2. service/msg生成
3. 実RawDataのDB内 snapshot 自動検出
4. converter通常実行
5. sunscan extract通常実行
6. sunscan singlebeam通常実行
7. multi-window DBの実append量とwriter queue
8. Astropyあり環境でのLSRK/VLSRK window実補正値
```

---

## 6. 現時点の制限・注意

### 6.1 sunscanの複数RawData入力

sunscan extract / singlebeam は、複数 RawData を一度に指定できる。ただし現状では、複数RawDataをまとめる運用は「同じ観測設定のDB群」を前提にするのが安全である。

理由:

```text
singlebeam:
  config_from_args() が最初の RawData から設定を作り、run_singlebeam_many() へ渡す。

extract:
  main/config_from_args 経路で最初の RawData の snapshot を参照して base_config を作る。
  run_extract_many() では per-run run_extract() があるが、CLI経路では明示/自動snapshotの扱いを
  複数DBごとに完全独立させる設計にはまだなっていない。
```

異なるsnapshotを持つDBを混ぜる場合:

```text
- DBごとに分けて実行する。
- あるいは共通snapshot/configを明示指定して、同一設定で解析する意図を明確にする。
```

これはupdate13の通常単一DB運用を妨げる問題ではないが、今後の改良候補である。

### 6.2 実RawData未検証

この環境では、実NECSTDB、XFFTS、ROS2、AstropyありのVLSRK実補正値までは確認していない。構文、軽量import、sidecar discovery、adapter単体テストまでの確認である。

---

## 7. 今後の改良候補

1. **multi-RawData sunscanのper-DB snapshot完全独立化**  
   複数RawDataを渡した場合、各DBごとに `spectral_recording_snapshot.toml` を自動検出し、設定差分がある場合は明示的にエラーまたはper-run独立解析へ分岐する。

2. **sidecar discoveryのdry-run CLI**  
   `necst-spectral-validate --discover-db RAWDATA_DIR` のように、どのsnapshot/sidecarを検出したか、legacy扱いか、incomplete new DBかを観測者がすぐ確認できるCLIを用意する。

3. **converter/sunscan起動時のconfig source表示統一**  
   `using spectral_recording_snapshot sidecar: ...` をconverterだけでなくsunscanにも標準表示し、出力manifestにも保存する。

4. **複数snapshot候補のエラー文改善**  
   duplicate snapshot時に、検出された全pathと、どれを削除すべきかをもう少し親切に表示する。

5. **実RawData fixtureの追加**  
   小さい疑似NECSTDBディレクトリをテストfixtureとして持ち、converter/sunscan CLI entrypointに近い形で自動検出を回帰試験する。

6. **docsファイル名整理**  
   旧短名の `u13_report.md` は混乱しやすいため、今後は `update13_*.md` に統一する。互換上残す場合も、正本は `manual_full_update13_ja.md` と `update13_deep_review_2026-04-30.md` と明記する。
