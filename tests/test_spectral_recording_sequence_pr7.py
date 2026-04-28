from __future__ import annotations
import importlib.util, shutil, sys, types
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]
def _package(name: str, path: Path | None = None):
    mod = types.ModuleType(name); mod.__path__ = [str(path)] if path is not None else []; sys.modules[name] = mod
_package('necst', ROOT/'necst'); _package('necst.rx', ROOT/'necst'/'rx'); _package('necst.procedures', ROOT/'necst'/'procedures'); _package('necst.procedures.observations', ROOT/'necst'/'procedures'/'observations')
spec_setup = importlib.util.spec_from_file_location('necst.rx.spectral_recording_setup', ROOT/'necst'/'rx'/'spectral_recording_setup.py'); assert spec_setup and spec_setup.loader
srs = importlib.util.module_from_spec(spec_setup); sys.modules[spec_setup.name] = srs; spec_setup.loader.exec_module(srs)
spec_seq = importlib.util.spec_from_file_location('necst.procedures.observations.spectral_recording_sequence', ROOT/'necst'/'procedures'/'observations'/'spectral_recording_sequence.py'); assert spec_seq and spec_seq.loader
seq = importlib.util.module_from_spec(spec_seq); sys.modules[spec_seq.name] = seq; spec_seq.loader.exec_module(seq)

def _lo_profile():
    return {'sg_devices': {'sg_lsb': {'control_adapter': 'necst_commander_signal_generator_set_v1','sg_set_frequency_hz': 4.0e9,'sg_set_power_dbm': 15.0,'frequency_tolerance_hz': 10.0}}, 'lo_roles': {'lo1': {'source': 'fixed', 'fixed_lo_frequency_hz': 230.0e9}, 'lo2': {'source': 'sg_device', 'sg_id': 'sg_lsb', 'multiplier': 3}}, 'lo_chains': {'band6_lsb': {'formula_version': 'signed_lo_sum_plus_if_v1', 'lo_roles': ['lo1', 'lo2'], 'sideband_signs': [1, -1]}}, 'frequency_axes': {'axis_small': {'full_nchan': 8, 'if_freq_at_full_ch0_hz': 0.0, 'if_freq_step_hz': 1.0, 'channel_order': 'increasing_if'}}, 'stream_groups': {'g': {'spectrometer_key': 'xffts', 'frontend': 'nanten2_band6', 'backend': 'xffts', 'lo_chain': 'band6_lsb', 'polariza': 'XX', 'frequency_axis_id': 'axis_small', 'use_for_convert': True, 'use_for_sunscan': True, 'use_for_fit': True, 'streams': [{'stream_id': 'xffts_board3', 'board_id': 3, 'fdnum': 0, 'ifnum': 0, 'plnum': 0, 'beam_id': 'B00'}]}}}

def _beam_model():
    return {'beams': {'B00': {'beam_model_version': '2026-04-26', 'az_offset_arcsec': 0.0, 'el_offset_arcsec': 0.0, 'rotation_mode': 'elevation', 'reference_angle_deg': 0.0, 'rotation_sign': 1, 'rotation_slope_deg_per_deg': 1.0, 'dewar_angle_deg': 0.0}}}

def test_build_setup_from_relative_obs_paths_and_sidecars(tmp_path):
    obs = tmp_path/'obs'/'target.obs'; obs.parent.mkdir(parents=True); obs.write_text('dummy', encoding='utf-8')
    (obs.parent/'lo_profile.toml').write_text(srs.dumps_toml(_lo_profile()), encoding='utf-8')
    (obs.parent/'beam_model.toml').write_text(srs.dumps_toml(_beam_model()), encoding='utf-8')
    setup = seq.build_spectral_recording_observation_setup(params={'lo_profile': 'lo_profile.toml', 'beam_model': 'beam_model.toml', 'setup_id': 'obs_setup'}, obs_file=obs, default_setup_id='fallback')
    assert setup is not None; assert setup.setup_id == 'obs_setup'; assert setup.setup_hash
    assert [name for name, _ in setup.sidecars] == ['beam_model.toml', 'lo_profile.toml', 'spectral_recording_snapshot.toml']
    snapshot = srs._toml_loads(setup.snapshot_toml)
    assert snapshot['canonical_snapshot_sha256'] == setup.setup_hash
    assert snapshot['streams']['xffts_board3']['db_table_path'] == 'data/spectral/xffts/board3'
class _Response: success=True; errors=[]
class _FakeCommander:
    def __init__(self): self.calls=[]
    def apply_spectral_recording_setup(self, **kwargs): self.calls.append(('apply', kwargs)); return _Response()
    def record(self, cmd, **kwargs): self.calls.append((cmd, kwargs)); return _Response()
    def set_spectral_recording_gate(self, **kwargs): self.calls.append(('gate', kwargs)); return _Response()
    def clear_spectral_recording_setup(self, **kwargs): self.calls.append(('clear', kwargs)); return _Response()
def test_apply_save_gate_order(tmp_path):
    obs=tmp_path/'target.obs'; obs.write_text('dummy', encoding='utf-8')
    lo=tmp_path/'lo_profile.toml'; beam=tmp_path/'beam_model.toml'
    lo.write_text(srs.dumps_toml(_lo_profile()), encoding='utf-8'); beam.write_text(srs.dumps_toml(_beam_model()), encoding='utf-8')
    setup=seq.build_spectral_recording_observation_setup(params={'lo_profile': str(lo), 'beam_model': str(beam), 'setup_id': 'seq'}, obs_file=obs, default_setup_id='fallback')
    com=_FakeCommander(); seq.apply_setup_with_commander(com, setup); seq.save_sidecars_with_commander(com, setup); seq.set_gate_with_commander(com, setup, allow_save=True)
    assert [c[0] for c in com.calls] == ['apply', 'file', 'file', 'file', 'gate']
    assert com.calls[0][1]['snapshot_sha256'] == setup.setup_hash; assert com.calls[-1][1]['allow_save'] is True; assert all('content' in call[1] for call in com.calls[1:4])
def test_clear_and_legacy_conflict_helpers(tmp_path):
    obs=tmp_path/'target.obs'; obs.write_text('dummy', encoding='utf-8')
    lo=tmp_path/'lo_profile.toml'; beam=tmp_path/'beam_model.toml'
    lo.write_text(srs.dumps_toml(_lo_profile()), encoding='utf-8'); beam.write_text(srs.dumps_toml(_beam_model()), encoding='utf-8')
    setup=seq.build_spectral_recording_observation_setup(params={'lo_profile': str(lo), 'beam_model': str(beam), 'setup_id': 'seq'}, obs_file=obs, default_setup_id='fallback')
    com=_FakeCommander(); seq.clear_setup_with_commander(com, setup)
    assert com.calls[-1][0] == 'clear'; assert com.calls[-1][1]['setup_hash'] == setup.setup_hash
    for kwargs in ({'ch': 1024}, {'tp_mode': True}, {'tp_range': [1, 2]}):
        try: seq.reject_legacy_recording_kwargs_for_setup(kwargs, setup)
        except ValueError as exc: assert 'legacy observation kwargs' in str(exc)
        else: raise AssertionError('Expected legacy kwargs conflict')

def test_no_params_keeps_legacy_mode(tmp_path):
    obs=tmp_path/'target.obs'; obs.write_text('dummy', encoding='utf-8')
    assert seq.build_spectral_recording_observation_setup(params={}, obs_file=obs, default_setup_id='fallback') is None
def test_cleanup_order_source_keeps_clear_after_record_stop():
    obs_base = ROOT / 'necst' / 'procedures' / 'observations' / 'observation_base.py'
    file_based = ROOT / 'necst' / 'procedures' / 'observations' / 'file_based.py'
    base_text = obs_base.read_text(encoding='utf-8')
    file_text = file_based.read_text(encoding='utf-8')
    i_before = base_text.index('_cleanup_step("before_record_stop", self.before_record_stop)')
    i_stop = base_text.index('self.com.record("stop")', i_before)
    i_after = base_text.index('_cleanup_step("after_record_stop", self.after_record_stop)', i_stop)
    assert i_before < i_stop < i_after
    before_body = file_text.split('def before_record_stop', 1)[1].split('def after_record_stop', 1)[0]
    after_body = file_text.split('def after_record_stop', 1)[1].split('def _coord_to_tuple', 1)[0]
    assert 'clear_setup_with_commander' not in before_body
    assert 'clear_setup_with_commander' in after_body

if __name__ == '__main__':
    for name, fn in (('/tmp/pr7_test_a', test_build_setup_from_relative_obs_paths_and_sidecars),('/tmp/pr7_test_b', test_apply_save_gate_order),('/tmp/pr7_test_c', test_no_params_keeps_legacy_mode),('/tmp/pr7_test_d', test_clear_and_legacy_conflict_helpers),('/tmp/pr7_test_e', lambda path: test_cleanup_order_source_keeps_clear_after_record_stop())):
        path=Path(name); shutil.rmtree(path, ignore_errors=True); path.mkdir(parents=True, exist_ok=True); fn(path)
    print('all spectral_recording_sequence PR7 tests passed')
