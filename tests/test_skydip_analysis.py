import json
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from necst.analysis.node import FinishedObservation, SkyDipAnalysisCoordinator
from necst.analysis.skydip import (
    AnalysisOutput,
    ScriptSkyDipAnalyzer,
    _normalize_board_labels,
    format_discord_summary,
)
from necst.notification import discord as discord_module
from necst.notification.discord import DiscordAttachmentTooLarge, DiscordNotifier


def test_bundled_script_keeps_analysis_and_plot_api():
    source = Path(
        "necst/analysis/scripts/skydip_step_jupyter_necstdb_v10.py"
    ).read_text()
    assert "from skydip_step_jupyter_necstdb_v6" not in source
    assert "# fmt: off" in source
    assert "def analyze_skydip_boards(" in source
    assert "def plot_skydip_results(" in source


def test_analysis_board_labels_are_passed_as_script_mapping():
    analyzer = ScriptSkyDipAnalyzer(
        board_labels={
            "xffts-board1": "Band 6 USB",
            "xffts-board2": "Band 6 LSB",
            "xffts-board3": "Band 3 USB",
            "xffts-board4": "Band 3 LSB",
        }
    )

    assert analyzer._board_selection(
        ["xffts-board1", "xffts-board3", "xffts-board9"]
    ) == {
        "xffts-board1": "Band 6 USB",
        "xffts-board3": "Band 3 USB",
        "xffts-board9": "xffts-board9",
    }


def test_board_labels_accept_inline_table_array():
    assert _normalize_board_labels(
        [
            {"name": "xffts-board1", "label": "Band 6 USB"},
            {"name": "xffts-board2", "label": "Band 6 LSB"},
            {"name": "xffts-board3", "label": "Band 3 USB"},
            {"name": "xffts-board4", "label": "Band 3 LSB"},
        ]
    ) == {
        "xffts-board1": "Band 6 USB",
        "xffts-board2": "Band 6 LSB",
        "xffts-board3": "Band 3 USB",
        "xffts-board4": "Band 3 LSB",
    }


def test_discord_summary_uses_backticks_labels_and_numeric_matrix():
    class Result:
        label = "Band 6 USB"
        quality = "WARN"
        quality_flags = ["large_reduced_chi2"]
        tau = 0.308
        tau_sigma = 0.327
        Tsys_sensitivity_zenith_K = 29466.3
        Trx_K = 21578.3
        reduced_chi2 = 8.75
        n_fit = 5

    summary = format_discord_summary(
        "necst_skydip_test",
        {"xffts-board1": Result()},
    )

    assert "Observation: `necst_skydip_test`" in summary
    assert "Band 6 USB" in summary
    assert "0.308 +/- 0.327" in summary
    assert "29466.300" in summary
    assert "large_reduced_chi2" in summary
    assert "```text" in summary


class FakeFigure:
    def savefig(self, buffer, **kwargs):
        buffer.write(b"png")


def test_analyzer_returns_figure_and_discord_summary(tmp_path):
    class Result:
        label = "Band 3 LSB"
        quality = "GOOD"
        quality_flags = []
        tau = 0.546
        tau_sigma = 0.138
        Tsys_sensitivity_zenith_K = 42301.1
        Trx_K = 24382.3
        reduced_chi2 = 2.44
        n_fit = 5

    class FakeScript:
        @staticmethod
        def analyze_skydip_boards(*args, **kwargs):
            return {"xffts-board4": Result()}, FakeFigure(), None

    analyzer = ScriptSkyDipAnalyzer(
        boards=["xffts-board4"],
        board_labels={"xffts-board4": "Band 3 LSB"},
    )
    analyzer._load_script = lambda: FakeScript

    output = analyzer.analyze(tmp_path / "necst_skydip_test")

    assert output.figure.__class__ is FakeFigure
    assert "Observation: `necst_skydip_test`" in output.discord_content
    assert "Band 3 LSB" in output.discord_content
    assert output.results["xffts-board4"].tau == 0.546


def test_analyzer_logs_script_stages(tmp_path):
    class Result:
        label = "Band 6 USB"
        quality = "GOOD"
        quality_flags = []
        tau = 0.1
        tau_sigma = 0.01
        Tsys_sensitivity_zenith_K = 100.0
        Trx_K = 80.0
        reduced_chi2 = 1.0
        n_fit = 4

    class FakeScript:
        @staticmethod
        def analyze_skydip_boards(*args, **kwargs):
            return {"xffts-board1": Result()}, FakeFigure(), None

    class Logger:
        def __init__(self):
            self.messages = []

        def info(self, message):
            self.messages.append(message)

    logger = Logger()
    analyzer = ScriptSkyDipAnalyzer(
        boards=["xffts-board1"],
        logger=logger,
    )
    analyzer._load_script = lambda: FakeScript

    analyzer.analyze(tmp_path / "necst_skydip_test")

    messages = "\n".join(logger.messages)
    assert "Analysis script load started" in messages
    assert "Analysis script loaded" in messages
    assert "Analysis boards configured" in messages
    assert "Analysis script execution started" in messages
    assert "Analysis script execution completed" in messages


def test_analyzer_logs_each_board_without_changing_script_result(tmp_path):
    class Result:
        label = "Band 6 USB"
        quality = "GOOD"
        quality_flags = []
        tau = 0.1
        tau_sigma = 0.01
        Tsys_sensitivity_zenith_K = 100.0
        Trx_K = 80.0
        reduced_chi2 = 1.0
        n_fit = 4

    class FakeScript:
        @staticmethod
        def analyze_skydip_board(*args, **kwargs):
            return Result()

        @staticmethod
        def analyze_skydip_boards(*args, **kwargs):
            board = args[1][0]
            result = FakeScript.analyze_skydip_board(args[0], board)
            return {board: result}, FakeFigure(), None

    class Logger:
        def __init__(self):
            self.messages = []

        def info(self, message):
            self.messages.append(message)

    logger = Logger()
    analyzer = ScriptSkyDipAnalyzer(
        boards=["xffts-board1"],
        logger=logger,
    )
    analyzer._load_script = lambda: FakeScript

    analyzer.analyze(tmp_path / "necst_skydip_test")

    messages = "\n".join(logger.messages)
    assert (
        "Board analysis started: record=necst_skydip_test, board=xffts-board1"
        in messages
    )
    assert (
        "Board analysis completed: record=necst_skydip_test, board=xffts-board1"
        in messages
    )


def test_analyzer_sends_partial_results_when_one_board_fails(tmp_path):
    class Result:
        label = "Band 6 LSB"
        quality = "GOOD"
        quality_flags = []
        tau = 0.2
        tau_sigma = 0.02
        Tsys_sensitivity_zenith_K = 110.0
        Trx_K = 90.0
        reduced_chi2 = 1.0
        n_fit = 5

    class FakeScript:
        @staticmethod
        def analyze_skydip_board(path, board, *, label, telescope):
            if board == "xffts-board1":
                raise RuntimeError("not enough fit points after filtering; n=1")
            return Result()

        @staticmethod
        def plot_skydip_results(results, **kwargs):
            return FakeFigure(), None

    analyzer = ScriptSkyDipAnalyzer(boards=["xffts-board1", "xffts-board2"])
    analyzer._load_script = lambda: FakeScript

    output = analyzer.analyze(tmp_path / "necst_skydip_test")

    assert output.figure.__class__ is FakeFigure
    assert output.results.keys() == {"xffts-board2"}.keys()
    assert "xffts-board1" in output.board_failures
    assert "not enough fit points" in output.board_failures["xffts-board1"]
    assert "Overall: `PARTIAL`" in output.discord_content
    assert "xffts-board1" in output.discord_content
    assert "ERROR" in output.discord_content


def test_analyzer_returns_text_only_result_when_all_boards_fail(tmp_path):
    class FakeScript:
        @staticmethod
        def analyze_skydip_board(path, board, *, label, telescope):
            raise RuntimeError("no valid signal")

    analyzer = ScriptSkyDipAnalyzer(boards=["xffts-board1"])
    analyzer._load_script = lambda: FakeScript

    output = analyzer.analyze(tmp_path / "necst_skydip_test")

    assert output.figure is None
    assert output.results == {}
    assert output.board_failures == {"xffts-board1": "RuntimeError: no valid signal"}
    assert "Overall: `ERROR`" in output.discord_content
    assert "no valid signal" in output.discord_content


class FakeAnalyzer:
    def __init__(self):
        self.paths = []

    def analyze(self, path):
        self.paths.append(path)
        return FakeFigure()


class FakeNotifier:
    def __init__(self):
        self.posts = []
        self.text_posts = []

    def send_figure(self, figure, observation_name, *, content=None):
        self.posts.append((figure, observation_name, content))

    def send_text(self, content):
        self.text_posts.append(content)


class FakeLogger:
    def __init__(self):
        self.infos = []
        self.errors = []
        self.warnings = []
        self.exceptions = []

    def info(self, message):
        self.infos.append(message)

    def error(self, message):
        self.errors.append(message)

    def warning(self, message):
        self.warnings.append(message)

    def exception(self, message):
        self.exceptions.append(message)

    def debug(self, message, **kwargs):
        pass


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

    coordinator.on_recorder_status(True)
    assert coordinator.on_progress(progress(record_name)) is None
    future = coordinator.on_recorder_status(False)
    assert future is not None
    future.result(timeout=2)

    assert analyzer.paths == [tmp_path / record_name]
    assert notifier.posts[0][1] == record_name
    executor.shutdown()


def test_coordinator_accepts_recorder_stop_before_finished_progress(tmp_path):
    record_name = "necst_skydip_20260811_153000"
    (tmp_path / record_name).mkdir()
    analyzer = FakeAnalyzer()
    notifier = FakeNotifier()
    executor = ThreadPoolExecutor(max_workers=1)
    coordinator = SkyDipAnalysisCoordinator(
        analyzer, notifier, tmp_path, executor=executor
    )

    coordinator.on_recorder_status(True)
    assert coordinator.on_recorder_status(False) is None
    future = coordinator.on_progress(progress(record_name))
    assert future is not None
    future.result(timeout=2)

    assert analyzer.paths == [tmp_path / record_name]
    executor.shutdown()


def test_coordinator_ignores_duplicate_finished_progress_after_scheduling(tmp_path):
    record_name = "necst_skydip_20260811_153000"
    (tmp_path / record_name).mkdir()
    analyzer = FakeAnalyzer()
    notifier = FakeNotifier()
    logger = FakeLogger()
    executor = ThreadPoolExecutor(max_workers=1)
    coordinator = SkyDipAnalysisCoordinator(
        analyzer, notifier, tmp_path, executor=executor, logger=logger
    )

    coordinator.on_recorder_status(False)
    future = coordinator.on_progress(progress(record_name))
    assert future is not None
    future.result(timeout=2)

    assert coordinator.on_progress(progress(record_name)) is None
    assert analyzer.paths == [tmp_path / record_name]
    assert (
        sum(
            "Finished observation progress received" in message
            for message in logger.infos
        )
        == 1
    )
    executor.shutdown()


def test_coordinator_logs_analysis_and_discord_phases(tmp_path):
    record_name = "necst_skydip_20260811_153000"
    (tmp_path / record_name).mkdir()
    logger = FakeLogger()
    coordinator = SkyDipAnalysisCoordinator(
        FakeAnalyzer(), FakeNotifier(), tmp_path, logger=logger
    )

    coordinator._analyze_and_notify(FinishedObservation(record_name, tmp_path))

    messages = "\n".join(logger.infos)
    assert "Analysis started" in messages
    assert "Analysis completed" in messages
    assert "Discord upload started" in messages
    assert "Discord post completed" in messages
    assert messages.index("Analysis completed") < messages.index(
        "Discord upload started"
    )
    assert messages.index("Discord upload started") < messages.index(
        "Discord post completed"
    )
    coordinator.shutdown()


def test_coordinator_sends_text_when_analysis_has_no_figure(tmp_path):
    record_name = "necst_skydip_20260811_153000"
    (tmp_path / record_name).mkdir()
    notifier = FakeNotifier()

    class TextOnlyAnalyzer:
        def analyze(self, path):
            return AnalysisOutput(
                figure=None,
                results={},
                discord_content="Overall: `ERROR`\nxffts-board1: no valid signal",
                board_failures={"xffts-board1": "RuntimeError: no valid signal"},
            )

    coordinator = SkyDipAnalysisCoordinator(TextOnlyAnalyzer(), notifier, tmp_path)
    coordinator._analyze_and_notify(FinishedObservation(record_name, tmp_path))

    assert notifier.posts == []
    assert notifier.text_posts == ["Overall: `ERROR`\nxffts-board1: no valid signal"]
    coordinator.shutdown()


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
    response = notifier.send_figure(
        FakeFigure(),
        "necst_skydip_test",
        content="**📡 Analysis Result**\nObservation: `necst_skydip_test`",
    )

    assert response == {"id": "123"}
    req, timeout = requests[0]
    assert req.full_url.endswith("/channels/987/messages")
    assert req.get_header("Authorization") == "Bot secret"
    assert b"Analysis Result" in req.data
    assert b"Observation: `necst_skydip_test`" in req.data
    assert b"image/png" in req.data
    assert b"png" in req.data
    assert timeout == 30.0


def test_discord_text_message():
    requests = []

    class Response:
        def read(self):
            return b'{"id":"456"}'

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

    def opener(req, timeout):
        requests.append((req, timeout))
        return Response()

    notifier = DiscordNotifier("secret", "987", opener=opener)
    response = notifier.send_text("Overall: `ERROR`\nxffts-board1: no valid signal")

    assert response == {"id": "456"}
    req, timeout = requests[0]
    assert req.get_header("Content-type") == "application/json"
    payload = json.loads(req.data.decode("utf-8"))
    assert payload["content"] == "Overall: `ERROR`\nxffts-board1: no valid signal"
    assert timeout == 30.0


def test_discord_oversize_png_sends_text_notice_without_attachment():
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

    notifier = DiscordNotifier("secret", "987", opener=opener, attachment_limit_bytes=2)

    with pytest.raises(DiscordAttachmentTooLarge) as exc_info:
        notifier.send_figure(FakeFigure(), "necst_skydip_test")

    assert exc_info.value.notification_sent is True
    assert exc_info.value.size_bytes == 3
    assert len(requests) == 1
    req, _ = requests[0]
    assert req.get_header("Content-type") == "application/json"
    payload = json.loads(req.data.decode("utf-8"))
    assert "could not be uploaded" in payload["content"]
    assert "image/png" not in payload["content"]


def test_coordinator_logs_attachment_limit_as_warning(tmp_path):
    record_name = "necst_skydip_20260811_153000"
    (tmp_path / record_name).mkdir()
    logger = FakeLogger()

    class OversizeNotifier:
        def send_figure(self, figure, observation_name):
            raise DiscordAttachmentTooLarge(
                11 * 1024 * 1024,
                10 * 1024 * 1024,
                notification_sent=True,
            )

    coordinator = SkyDipAnalysisCoordinator(
        FakeAnalyzer(), OversizeNotifier(), tmp_path, logger=logger
    )
    coordinator._analyze_and_notify(FinishedObservation(record_name, tmp_path))

    assert len(logger.warnings) == 1
    assert "size limit exceeded" in logger.warnings[0]
    assert "failure notice sent" in logger.warnings[0]
    assert logger.exceptions == []
    coordinator.shutdown()


def test_coordinator_logs_analysis_exception_with_traceback(tmp_path):
    record_name = "necst_skydip_20260811_153000"
    (tmp_path / record_name).mkdir()
    logger = FakeLogger()

    class FailingAnalyzer:
        def analyze(self, path):
            raise ValueError("synthetic board failure")

    coordinator = SkyDipAnalysisCoordinator(
        FailingAnalyzer(), FakeNotifier(), tmp_path, logger=logger
    )
    coordinator._analyze_and_notify(FinishedObservation(record_name, tmp_path))

    assert len(logger.errors) == 1
    assert "stage=analysis" in logger.errors[0]
    assert "ValueError: synthetic board failure" in logger.errors[0]
    coordinator.shutdown()


def test_discord_notifier_reads_channel_id_from_environment(monkeypatch):
    monkeypatch.setenv("DISCORD_BOT_TOKEN", "secret")
    monkeypatch.setenv("DISCORD_CHANNEL_ID", "987")
    monkeypatch.delenv("DISCORD_QLOOK_CHANNEL_ID", raising=False)

    notifier = DiscordNotifier.from_environment()

    assert notifier.channel_id == "987"


def test_discord_notifier_reads_configured_env_file(tmp_path, monkeypatch):
    env_file = tmp_path / "discord.env"
    env_file.write_text(
        "# shared service secrets\n"
        "DISCORD_BOT_TOKEN='file-secret'\n"
        "export DISCORD_CHANNEL_ID=987\n"
        "DISCORD_ATTACHMENT_LIMIT_MIB=12.5\n",
        encoding="utf-8",
    )
    monkeypatch.delenv("DISCORD_BOT_TOKEN", raising=False)
    monkeypatch.delenv("DISCORD_CHANNEL_ID", raising=False)
    monkeypatch.setattr(discord_module, "_configured_env_file", lambda: env_file)

    notifier = DiscordNotifier.from_environment()

    assert notifier._token == "file-secret"
    assert notifier.channel_id == "987"
    assert notifier.attachment_limit_bytes == int(12.5 * 1024 * 1024)


def test_progress_payload_is_json_serializable():
    assert json.loads(json.dumps(progress()))["lifecycle"]["state"] == "finished"
