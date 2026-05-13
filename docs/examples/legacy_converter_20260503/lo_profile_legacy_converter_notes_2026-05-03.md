# Legacy converter configから作成した `lo_profile.toml`

## 結論

添付された旧converter config 6個から、`lo_profile_legacy_converter_4board_12co_13co_c18o.toml` を作成しました。
100 GHz帯の1st localは6逓倍、200 GHz帯の1st localは7逓倍、2nd localは逓倍なし、という前提で再現しています。

同時に、13CO/C18Oを同一board上のmulti-windowとして扱えるように、対応する `recording_window_legacy_converter_4board_12co_13co_c18o.toml` も作成しました。

## 前提と定義

- `sg_set_frequency_ghz`: 実際にSGへ送る周波数。
- `expected_lo_frequency_ghz`: 逓倍後に周波数計算で使う実効LO周波数。
- `signed_lo_sum_plus_if_v1`: `sky = signed_lo_sum + if_frequency_sign * IF` として旧converter式を再現する形式。
- IF帯域: 0.0--2.5 GHz, 32768 channels, channel center convention。
- telescope/frontend: 旧configに合わせて `OMU1P85M` としました。

## 追加した重要点

先生が最初に書かれていたSG設定には、200 GHz帯の1st local用SGがありませんでした。
旧configでは200 GHz帯の1st localが 225.0 GHz なので、7逓倍前提ではSG周波数は

```text
225.0 GHz / 7 = 32.142857142857146 GHz
```

です。そのため、次を追加しています。

```toml
[sg_devices.band6_1st]
sg_set_frequency_ghz = 32.142857142857146
...

[lo_roles.band6_1st]
source = "sg_device"
sg_id = "band6_1st"
multiplier = 7
expected_lo_frequency_ghz = 225.0
```

実機のSG IDが `band6_1st` でない場合は、`[sg_devices.<id>]` と `lo_roles.band6_1st.sg_id` の両方を実機設定に合わせて変更してください。

## 旧configとの対応

| line | legacy file | DB stream | old LO1 | old LO2 | new chain | signed sum GHz | IF sign | line IF GHz | approx channel |
|---|---|---:|---:|---:|---|---:|---:|---:|---:|
| 12CO J=1-0 | 12co10.conf | xffts-board3 | 104.5 USB | 11.6 LSB | rx100_12co10_usb_lsb | 116.1 | -1 | 0.829000000 | 10865.4 |
| 13CO J=1-0 | 13co10.conf | xffts-board4 | 104.5 USB | 6.35 LSB | rx100_13co_c18o10_usb_lsb | 110.85 | -1 | 0.649000000 | 8506.1 |
| C18O J=1-0 | c18o10.conf | xffts-board4 | 104.5 USB | 6.35 LSB | rx100_13co_c18o10_usb_lsb | 110.85 | -1 | 1.067818000 | 13995.6 |
| 12CO J=2-1 | 12co21.conf | xffts-board1 | 225 USB | 4 USB | rx200_12co21_usb_usb | 229 | +1 | 1.538000000 | 20158.4 |
| 13CO J=2-1 | 13co21.conf | xffts-board2 | 225 LSB | 6.35 LSB | rx200_13co_c18o21_lsb_lsb | 218.65 | +1 | 1.749000000 | 22924.0 |
| C18O J=2-1 | c18o21.conf | xffts-board2 | 225 LSB | 6.35 LSB | rx200_13co_c18o21_lsb_lsb | 218.65 | +1 | 0.910357000 | 11931.7 |

すべてのline中心IFは0.0--2.5 GHzの範囲内です。

## 使い方

SG設定だけを確認する場合:

```bash
necst lo_profile summary lo_profile_legacy_converter_4board_12co_13co_c18o.toml
```

個別SGをapplyする場合:

```bash
necst lo_profile apply lo_profile_legacy_converter_4board_12co_13co_c18o.toml --id band3_1st --verify --timeout-sec 10
```

全SGを順にapplyする場合は、`--id` を付けずに実行します。

```bash
necst lo_profile apply lo_profile_legacy_converter_4board_12co_13co_c18o.toml --verify --timeout-sec 10
```

snapshotを作る場合は、必要に応じて `beam_model.toml` と一緒に以下のように使います。

```bash
necst spectral_resolve   --lo-profile lo_profile_legacy_converter_4board_12co_13co_c18o.toml   --recording-window-setup recording_window_legacy_converter_4board_12co_13co_c18o.toml   --beam-model beam_model.toml   --setup-id legacy_converter_12co_13co_c18o_omu1p85m   --output spectral_recording_snapshot.toml
```

## 注意点

1. `sg_devices.<id>` は `/root/.necst/OMU1p85m_config.toml` の `[signal_generator.<id>]` と一致している必要があります。
2. `sg_set_frequency_ghz` を使うには、`sg_set_frequency_ghz` alias修正パッチが適用されている必要があります。未適用なら `sg_set_frequency_hz` に書き換えてください。
3. `output_required = true` の場合、SGへ `set` が送られ、出力ONとして扱われます。今回のファイルでは混乱を避けるため `output_policy = "require_off"` は書いていません。
4. `recording_window_setup` は観測保存window用です。SG applyだけなら不要です。
5. 13CO/C18Oのペアは同一boardを共有するため、`recording_window_setup` 側で `multi_window` として表現しています。
