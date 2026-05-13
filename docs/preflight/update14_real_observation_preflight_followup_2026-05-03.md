# update14 実観測前フォローアップ確認 2026-05-03

## 適用した前提

original zip群に対して、このチャット内で作成した以下のpatchを順に適用した状態を基準に確認した。

1. `u14(2).zip`
2. `update14_time_consistency_testfix_simulation_changed_files_2026-05-01.zip`
3. `update14_example_timing_config_cleanup_changed_files_2026-05-01.zip`
4. `update14_sunscan_analysis_base_cleanup_changed_files_2026-05-01.zip`
5. `update14_original_to_update14_complete_changed_files_with_cumulative_manuals_2026-05-02.zip`
6. `update14_lo_profile_rclpy_init_fix_changed_files_2026-05-03.zip`
7. `update14_lo_profile_sg_frequency_alias_fix_changed_files_2026-05-03.zip`
8. `update14_bin_execution_permission_fix_changed_files_2026-05-03.zip`
9. `update14_otf_setup_ch_none_fix_changed_files_2026-05-03.zip`

## 追加で見つけた実観測リスク

### 1. RadioPointing等で inactive `tp_mode=False` / `tp_range=[]` が旧式 recorder control として送られる可能性

`reject_legacy_recording_kwargs_for_setup()` では inactive default を許可するように修正していたが、`Observation.execute()` 側では `tp_mode` / `tp_range` のキーが存在すると、値が `False` / `[]` でも `record("tp_mode", tp_mode=False)` を送る構造だった。

OTF wrapperは通常 `tp_mode` を渡さないため踏みにくいが、`radio_pointing.py` は `tp_mode=False`, `tp_range=[]` を常に渡す。setup modeでこれが送られると、new spectral recording setup 適用後に旧式 recorder control が混入する可能性がある。

修正後は、spectral recording setup が有効な場合、inactive default は pop して消費し、旧式 recorder command は送らない。activeな `ch`, `tp_mode`, `tp_range` が来た場合は引き続きエラーにする。

### 2. `necst lo_profile apply/verify` が privilege を解放していない

`main_lo_profile()` は `Commander.get_privilege()` を呼ぶが、終了時に `quit_privilege()` を呼んでいなかった。実行後にnode destroyだけで終わると、実機運用上 privilege 解放の扱いが不安定になる恐れがある。

修正後は、`get_privilege()` が成功した場合だけ、`destroy_node()` 前に `quit_privilege()` を呼ぶ。`rclpy.init()` / `shutdown()` の既存修正は維持する。

## 修正ファイル

- `necst-second_OTF_branch/necst/procedures/observations/observation_base.py`
- `necst-second_OTF_branch/necst/procedures/observations/file_based.py`
- `necst-second_OTF_branch/necst/rx/spectral_recording_sg.py`
- `necst-second_OTF_branch/tests/test_spectral_recording_sequence_pr7.py`
- `necst-second_OTF_branch/tests/test_spectral_recording_sg_pr3.py`

## 検証

- 変更Pythonファイルの `py_compile`: OK
- `tests/test_spectral_recording_sg_pr3.py` direct run: OK
- `tests/test_spectral_recording_sequence_pr7.py` direct run: OK
- startup legacy recorder controlの静的確認: setup mode時は inactive default を送らず消費する構造になった
- SG apply CLIの静的確認: `quit_privilege()` → `destroy_node()` → 必要時のみ `rclpy.shutdown()` の順序になった

## 残る実機確認

以下は実機ROS2環境でのみ確認可能。

1. `necst lo_profile apply lo_profile.toml --id <id> --verify --timeout-sec 10`
2. `necst otf -f otf_lo_test.toml`
3. `necst radio_pointing -f <setup付きtoml>` を使う場合、旧式 `tp_mode=False` が送られないこと
4. 実XFFTS + recorderで snapshot sidecar が保存されること
