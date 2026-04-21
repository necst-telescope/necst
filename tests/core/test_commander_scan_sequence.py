from pathlib import Path
from types import SimpleNamespace

from tests._helpers.scan_block_stub_runtime import FakeLogger, load_commander_module


REPO_ROOT = Path(__file__).resolve().parents[3]
MODULE = load_commander_module(REPO_ROOT)
Commander = MODULE.Commander
MODULE.config.antenna_scan_margin = SimpleNamespace(value=0.1)


class DummyPublisher:
    def __init__(self):
        self.messages = []

    def publish(self, msg):
        self.messages.append(msg)


class TestCommanderScanSequence:
    def test_scan_waits_then_sends_cmd_trans_then_waits_control(self, monkeypatch):
        com = Commander.__new__(Commander)
        com.logger = FakeLogger()
        publisher = DummyPublisher()
        com.publisher = {"cmd_trans": publisher}
        com.client = {"raw_coord": object()}

        call_order = []

        def fake_send_request(req, client):
            call_order.append(("raw_coord", req))
            return SimpleNamespace(id="scan-id")

        def fake_wait(target, mode="error", id=None):
            call_order.append(("wait", target, mode, id))

        monkeypatch.setattr(com, "_send_request", fake_send_request, raising=False)
        monkeypatch.setattr(com, "wait", fake_wait)

        scan_id = com.antenna(
            "scan",
            start=(-1.0, 0.0),
            stop=(1.0, 0.0),
            scan_frame="altaz",
            speed=0.5,
            reference=(30.0, 45.0, "altaz"),
            unit="deg",
            wait=True,
        )

        assert scan_id == "scan-id"
        assert call_order[0][0] == "raw_coord"
        assert call_order[1] == ("wait", "antenna", "error", None)
        assert len(publisher.messages) == 1
        assert publisher.messages[0].data is True
        assert call_order[2] == ("wait", "antenna", "control", "scan-id")

        req = call_order[0][1]
        assert tuple(req.lon) == (30.0,)
        assert tuple(req.lat) == (45.0,)
        assert tuple(req.offset_lon) == (-1.0, 1.0)
        assert tuple(req.offset_lat) == (0.0, 0.0)
        assert req.offset_frame == "altaz"


class DummySection:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


class TestCommanderScanBlockSequence:
    def test_scan_block_waits_then_sends_cmd_trans_then_waits_control(self, monkeypatch):
        com = Commander.__new__(Commander)
        com.logger = FakeLogger()
        publisher = DummyPublisher()
        com.publisher = {"cmd_trans": publisher}
        com.client = {"scan_block": object()}

        call_order = []

        def fake_send_request(req, client):
            call_order.append(("scan_block", req))
            return SimpleNamespace(id="block-id")

        def fake_wait(target, mode="error", id=None):
            call_order.append(("wait", target, mode, id))

        monkeypatch.setattr(com, "_send_request", fake_send_request, raising=False)
        monkeypatch.setattr(com, "wait", fake_wait)

        section = DummySection(
            kind="turn",
            tight=False,
            label="turn:0->1",
            line_index=1,
            start=(0.0, 0.0),
            stop=(1.0, 1.0),
            speed=0.5,
            margin=0.0,
            duration=None,
            turn_radius_hint=0.2,
        )

        block_id = com.scan_block(
            sections=[section],
            scan_frame="altaz",
            reference=(30.0, 45.0, "altaz"),
            unit="deg",
            wait=True,
        )

        assert block_id == "block-id"
        assert call_order[0][0] == "scan_block"
        assert call_order[1] == ("wait", "antenna", "error", None)
        assert len(publisher.messages) == 1
        assert publisher.messages[0].data is True
        assert call_order[2] == ("wait", "antenna", "control", "block-id")

        req = call_order[0][1]
        assert len(req.sections) == 1
        assert float(req.sections[0].turn_radius_hint) == 0.2
