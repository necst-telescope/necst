import json
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from necst.analysis.node import SkyDipAnalysisCoordinator
from necst.notification.discord import DiscordNotifier


def test_bundled_script_keeps_analysis_and_plot_api():
    source = Path(
        "necst/analysis/scripts/skydip_step_jupyter_necstdb_v10.py"
    ).read_text()
    assert "from skydip_step_jupyter_necstdb_v6" not in source
    assert "# fmt: off" in source
    assert "def analyze_skydip_boards(" in source
    assert "def plot_skydip_results(" in source


class FakeFigure:
    def savefig(self, buffer, **kwargs):
        buffer.write(b"png")


class FakeAnalyzer:
    def __init__(self):
        self.paths = []

    def analyze(self, path):
        self.paths.append(path)
        return FakeFigure()


class FakeNotifier:
    def __init__(self):
        self.posts = []

    def send_figure(self, figure, observation_name):
        self.posts.append((figure, observation_name))


def progress(record_name="necst_skydip_20260811_153000", state="finished"):
    return {
        "observation": {"type": "Skydip", "record_name": record_name},
        "lifecycle": {"state": state},
    }


def test_coordinator_waits_for_recorder_stop(tmp_path):
    record_name = "necst_skydip_20260811_153000"
    (tmp_path / record_name).mkdir()
    analyzer = FakeAnalyzer()
    notifier = FakeNotifier()
    executor = ThreadPoolExecutor(max_workers=1)
    coordinator = SkyDipAnalysisCoordinator(
        analyzer, notifier, tmp_path, executor=executor
    )

    coordinator.on_progress(progress(record_name))
    assert coordinator.on_recorder_status(False) is None
    assert analyzer.paths == []

    coordinator.on_recorder_status(True)
    future = coordinator.on_recorder_status(False)
    assert future is not None
    future.result(timeout=2)

    assert analyzer.paths == [tmp_path / record_name]
    assert notifier.posts[0][1] == record_name
    executor.shutdown()


def test_coordinator_ignores_non_skydip_and_non_finished(tmp_path):
    analyzer = FakeAnalyzer()
    notifier = FakeNotifier()
    executor = ThreadPoolExecutor(max_workers=1)
    coordinator = SkyDipAnalysisCoordinator(
        analyzer, notifier, tmp_path, executor=executor
    )

    for payload in (
        progress(state="running"),
        {**progress(), "observation": {"type": "OTF", "record_name": "x"}},
    ):
        coordinator.on_progress(payload)
    coordinator.on_recorder_status(True)
    assert coordinator.on_recorder_status(False) is None
    assert analyzer.paths == []
    executor.shutdown()


def test_coordinator_ignores_record_paths_outside_root(tmp_path):
    analyzer = FakeAnalyzer()
    notifier = FakeNotifier()
    executor = ThreadPoolExecutor(max_workers=1)
    coordinator = SkyDipAnalysisCoordinator(
        analyzer, notifier, tmp_path, executor=executor
    )

    coordinator.on_progress(progress("../outside"))
    coordinator.on_recorder_status(True)
    assert coordinator.on_recorder_status(False) is None
    executor.shutdown()


def test_discord_multipart_contains_png_and_message():
    requests = []

    class Response:
        def read(self):
            return b'{"id":"123"}'

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

    def opener(req, timeout):
        requests.append((req, timeout))
        return Response()

    notifier = DiscordNotifier("secret", "987", opener=opener)
    response = notifier.send_figure(FakeFigure(), "necst_skydip_test")

    assert response == {"id": "123"}
    req, timeout = requests[0]
    assert req.full_url.endswith("/channels/987/messages")
    assert req.get_header("Authorization") == "Bot secret"
    assert b"SkyDip Analysis" in req.data
    assert b"image/png" in req.data
    assert b"png" in req.data
    assert timeout == 30.0


def test_progress_payload_is_json_serializable():
    assert json.loads(json.dumps(progress()))["lifecycle"]["state"] == "finished"
