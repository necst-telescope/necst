# update14 累積統合レポート 2026-05-03

## 結論

original zip群に対し、update14本体、および2026-05-03までにこのチャット内で作成・確認したpatchを順に統合した。対象は単なるupdate14差分ではなく、originalからこの段階までの累積変更である。

## 適用順

1. `update14_time_consistency_testfix_simulation_changed_files_2026-05-01.zip`
2. `update14_sunscan_analysis_base_cleanup_changed_files_2026-05-01.zip`
3. `update14_lo_profile_rclpy_init_fix_changed_files_2026-05-03.zip`
4. `update14_lo_profile_sg_frequency_alias_fix_changed_files_2026-05-03.zip`
5. `update14_bin_execution_permission_fix_changed_files_2026-05-03.zip`
6. `update14_otf_setup_ch_none_fix_changed_files_2026-05-03.zip`
7. `update14_real_observation_preflight_followup_fix_changed_files_2026-05-03.zip`
8. `update14_recording_metadata_path_cleanup_2026-05-03.zip`
9. `update14_converter_sunscan_db_table_path_followup_2026-05-03.zip`
10. `update14_necstdb_boolean_alias_warning_fix_2026-05-03.zip`

## 今日の実機preflight由来の修正

### LO profile CLI

- `Commander()` 作成前に `rclpy.init()` する。
- CLI自身が初期化した場合だけ `rclpy.shutdown()` する。
- apply/verify終了時に `quit_privilege()` してから `destroy_node()` する。

### SG周波数キー

- `sg_set_frequency_hz`
- `sg_set_frequency_mhz`
- `sg_set_frequency_ghz`

を1つだけ許可し、内部ではHzへ正規化する。

### fixed LO

200 GHz帯1st localを観測制御しない225 GHz固定として扱う場合は、以下を使う。

```toml
[lo_roles.band6_1st]
source = "fixed"
fixed_lo_frequency_ghz = 225.0
```

`frequency_ghz = 225.0` はfixed LOとして読まない。

### 実行権限

`necst lo_profile ...`, `necst spectral_resolve ...`, `necst spectral_validate ...` を追加し、個別wrapperの実行ビットに依存しにくくした。

### setup modeとlegacy kwargs

`ch=None`, `tp_mode=False`, `tp_range=[]` は未指定として扱う。実値指定だけをlegacy spectral制御との競合として拒否する。

### metadata

single-window `contiguous_envelope` ではtop-levelに `line_name` / `window_id` を昇格する。

### DB path

multi-window streamの実保存pathを短縮する。

旧:

```text
data/spectral/xffts/xffts-board2__13CO_J2_1
```

新:

```text
data/spectral/xffts/board2__13CO_J2_1
```

converter/sunscanはsnapshotの `db_table_path` / `db_table_name` を優先するよう追従済み。

### NECSTDB boolean

`boolean` field typeを `bool` と同じformatで保存できるようにした。

## 追加したexample

`docs/examples/legacy_converter_20260503/` に、旧converter config 6個から再構成した4 board例を追加した。200 GHz帯1st localはfixed 225 GHzとして書いている。

## docs方針

`manual_full_update14_ja.md` は情報を削らない方針で、update13までの詳細manual本文を保持し、その前に2026-05-03時点の最新修正・実機注意点を追加した。

## 2026-05-04 追加

周波数・速度変換の検証を追加し、SD FITS writerのrow-level `RESTFREQ` / `VELDEF` 出力、beam optional空文字処理、snapshotからWCSへの1 channel境界確認を反映した。

## 2026-05-04 追加修正: VELDEF test expectation

`test_sdfits_writer_multiline_restfreq_columns.py` の期待値を修正した。入力意味としての `veldef="RADIO"` は、`specsys="TOPOCENT"` と組み合わされ、SDFITS table 出力では `VELDEF="RADI-OBS"` へ正規化される。writerの挙動は正しく、test期待値を実装仕様に合わせた。

## 2026-05-04 追加修正: chopper設定の配置

converter本体では `chopper_wheel/chopper_stat/chopper_win_sec` を使っていないため、exampleの `[converter_analysis]` からactive chopper設定を削除し、sunscan固有設定として `[sunscan_analysis]` に移した。`chopper_win_sec` は現在の1-load実装では未使用のlegacy互換optionなのでコメントアウト例にした。

## 2026-05-04 追加修正: examples directory reorganization

`docs/examples` を用途別に整理した。`NANTEN2_multibeam_260331TO/` をNANTEN2 multibeamの現行例、`OMU1P85M_single115_xx/` をOMU 1.85 m single115/legacy 4-board例の現行例とした。旧 `old_config_260331TO_update11/` と `legacy_converter_20260503/` は履歴として残し、READMEで現行ディレクトリへ誘導する。

## 2026-05-04 追加修正: spectral_time_source comments

NANTEN2/OMU examplesの `spectral_time_source` を `auto` に統一し、`host-time` と `xffts-timestamp` を明示指定できること、`spectrometer_time_offset_sec` はhost-time採用時だけ適用されることをコメントとmanualへ追加した。`xffts_timestamp_scale` と `xffts_gps_suffix_means` の取り得る値も明記した。

## 2026-05-12 追加修正: OMU 1.85 m 4-board current setup

添付された現行1.85 m用 `lo_profile.toml` / `recording_window_setup.toml` を基に、`docs/examples/OMU1P85M_4board_12co_13co_c18o_current/` を追加した。100/200 GHzは準光学フィルターでビームが分かれるため、`fdnum` を周波数pathごとに割り当てた。`ifnum` は4 boardで重複しないよう `0,1,2,3`、`plnum` は全streamで `0` とした。

## 2026-05-12 追加修正: per-window fdnum/ifnum/plnum overrides

`multi_window` で分割されたsaved productごとに `fdnum/ifnum/plnum` を指定できるようにした。これにより、同一XFFTS boardから切り出す13CO/C18O productにも異なるIFNUMを割り当てられる。OMU 1.85 m current exampleでは6 productにIFNUM 0--5を割り当てた。

## 2026-05-12 追加修正: fd-local IFNUM and all-product multi_window

OMU 1.85 m current setupのSDFITS番号付けを、`fdnum`ごとに `ifnum=0,1,2` とする方針へ更新した。全recording groupを `saved_window_policy="multi_window"` に統一し、12CO単独boardでも `window_id` がrecorded stream名/DB table pathへ反映されるようにした。静止周波数は指定された6値へ更新した。

## 2026-05-12 追加修正: OMU examples consolidation and NANTEN2 override update

重複していた `OMU1P85M_single115_xx/` を廃止し、analysis TOMLを `OMU1P85M_4board_12co_13co_c18o_current/` へ移動した。NANTEN2 multibeamのrecording exampleも最新仕様に合わせ、全groupを `multi_window` にし、window-level `ifnum` override例を追加した。NANTEN2ではbeam/polごとの `fdnum/plnum` をsource streamから継承する。

## 2026-05-12 追加修正: Sun/sunscan TP examples

Sun/sunscan用に、速度windowではなくchannel指定でTPを作るrecording setup例をOMU 1.85 mとNANTEN2に追加した。TP-only snapshotをsunscanで扱えるよう、snapshot adapterはspectral streamが無い場合にTP streamをmaterializeし、sunscan legacy readerは `tp_mean` / `tp_sum` を1-channel信号として読めるようにした。

## 2026-05-12 追加修正: focused test function name

`test_spectral_recording_setup_window_metadata_overrides.py` が存在しない `resolve_spectral_recording_snapshot` を呼んでいたため、実装上の正しいAPIである `resolve_spectral_recording_setup(..., beam_model=None, ...)` へ修正した。これにより、per-window `fdnum/ifnum/plnum` overrideのfocused testを実行可能にした。

## 2026-05-12 追加修正: focused test fixture frontend

`test_spectral_recording_setup_window_metadata_overrides.py` の最小fixtureに `frontend` を追加した。現在のresolverはstream truthとして `frontend` を必須にするため、test fixture側も実装仕様に合わせた。

## 2026-05-12 追加修正: NANTEN2 converter base cleanup

NANTEN2 converter analysis exampleに旧設定由来の `planet="Ori-KL"` と `spectral_name="xffts-board1"` がactiveで残っていたためコメントアウトした。Sun TP sunscan analysisには `planet="sun"` を明示した。

