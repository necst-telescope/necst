import time

from necst.rx.spectrometer import ControlSectionManager, ObservingModeManager


def _effective_metadata(mode_mgr, section_mgr, t):
    metadata = mode_mgr.get(t)
    section = section_mgr.get(t)
    line_index = -1
    line_label = ""
    if (metadata.position == "ON") and (section.kind == "line"):
        line_index = int(section.line_index)
        line_label = section.label[:64]
    return metadata.position, metadata.id, line_index, line_label


class TestSpectrometerScanBlockMetadata:
    def test_on_line_sets_fine_line_metadata_without_overwriting_block_id(self):
        now = time.time()
        mode = ObservingModeManager()
        section = ControlSectionManager()

        mode.enable(now - 1.0)
        mode.set(now - 1.0, position="ON", id="block01")
        section.set(now - 1.0, kind="initial_standby", label="scan01", line_index=0)
        section.set(now - 0.5, kind="line", label="scan01", line_index=3)

        position, coarse_id, line_index, line_label = _effective_metadata(
            mode, section, now
        )
        assert position == "ON"
        assert coarse_id == "block01"
        assert line_index == 3
        assert line_label == "scan01"

    def test_non_line_section_keeps_no_fine_line_metadata(self):
        now = time.time()
        mode = ObservingModeManager()
        section = ControlSectionManager()

        mode.enable(now - 1.0)
        mode.set(now - 1.0, position="ON", id="block01")
        section.set(now - 1.0, kind="turn", label="turn01", line_index=1)

        position, coarse_id, line_index, line_label = _effective_metadata(
            mode, section, now
        )
        assert position == "ON"
        assert coarse_id == "block01"
        assert line_index == -1
        assert line_label == ""
