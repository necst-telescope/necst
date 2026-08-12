"""Post-observation SkyDip analysis coordinator."""

from __future__ import annotations

import logging
import time
import traceback
from concurrent.futures import Executor, Future, ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from threading import Lock
from typing import Any, Mapping, Optional

from ..notification.discord import DiscordAttachmentTooLarge


@dataclass(frozen=True)
class FinishedObservation:
    record_name: str
    record_root: Path

    @property
    def record_path(self) -> Path:
        return self.record_root / self.record_name


def _is_skydip(value: Any) -> bool:
    return str(value or "").strip().lower() in {"skydip", "sky_dip", "sky-dip"}


def _finished_observation(
    payload: Mapping[str, Any], record_root: Path
) -> Optional[FinishedObservation]:
    observation = payload.get("observation")
    lifecycle = payload.get("lifecycle")
    if not isinstance(observation, Mapping) or not isinstance(lifecycle, Mapping):
        return None
    if not _is_skydip(observation.get("type")):
        return None
    if str(lifecycle.get("state", "")).lower() != "finished":
        return None
    raw_record_name = str(observation.get("record_name") or "").strip()
    if not raw_record_name:
        return None
    record_path = Path(raw_record_name)
    if record_path.is_absolute() or ".." in record_path.parts:
        return None
    record_name = raw_record_name.lstrip("/")
    return FinishedObservation(record_name, record_root)


class SkyDipAnalysisCoordinator:
    """Join progress and Recorder events, then run analysis off the ROS thread."""

    def __init__(
        self,
        analyzer: Any,
        notifier: Any,
        record_root: Path,
        *,
        executor: Optional[Executor] = None,
        logger: Optional[Any] = None,
    ) -> None:
        self.analyzer = analyzer
        self.notifier = notifier
        self.record_root = Path(record_root).expanduser()
        self.logger = logger or logging.getLogger(__name__)
        self._executor = executor or ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="skydip-analysis"
        )
        self._owns_executor = executor is None
        self._lock = Lock()
        self._pending: Optional[FinishedObservation] = None
        self._recording = False
        self._recording_state_seen = False
        self._scheduled_records = set()

    def on_progress(self, payload: Mapping[str, Any]) -> Optional[Future]:
        candidate = _finished_observation(payload, self.record_root)
        if candidate is None:
            return
        with self._lock:
            if candidate.record_name in self._scheduled_records:
                return None
            should_log = (
                self._pending is None
                or self._pending.record_name != candidate.record_name
            )
            self._pending = candidate
            if should_log:
                self.logger.info(
                    "Finished observation progress received: "
                    f"record={candidate.record_name}, path={candidate.record_path}"
                )
            return self._schedule_pending_locked()

    def _schedule_pending_locked(self) -> Optional[Future]:
        """Schedule once both final progress and recorder-stop are known."""

        if not self._recording_state_seen or self._recording or self._pending is None:
            return None
        candidate = self._pending
        self._pending = None
        if candidate.record_name in self._scheduled_records:
            return None
        self._scheduled_records.add(candidate.record_name)
        self.logger.info(f"Scheduling SkyDip analysis: {candidate.record_name}")
        return self._executor.submit(self._analyze_and_notify, candidate)

    def on_recorder_status(self, recording: bool) -> Optional[Future]:
        with self._lock:
            previous = self._recording if self._recording_state_seen else None
            self._recording_state_seen = True
            self._recording = bool(recording)
            if previous is None or previous != self._recording:
                self.logger.info(
                    f"Recorder status received: recording={self._recording}"
                )
            return self._schedule_pending_locked()

    def _analyze_and_notify(self, observation: FinishedObservation) -> None:
        figure = None
        started_at = time.monotonic()
        stage = "analysis"
        self.logger.info(
            f"Analysis started: record={observation.record_name}, "
            f"path={observation.record_path}"
        )
        try:
            if not observation.record_path.is_dir():
                raise FileNotFoundError(
                    f"SkyDip record directory does not exist: {observation.record_path}"
                )
            analysis_output = self.analyzer.analyze(observation.record_path)
            figure = getattr(analysis_output, "figure", analysis_output)
            discord_content = getattr(analysis_output, "discord_content", None)
            board_failures = getattr(analysis_output, "board_failures", {}) or {}
            self.logger.info(
                f"Analysis completed: record={observation.record_name}, "
                f"successful_boards={len(getattr(analysis_output, 'results', {}))}, "
                f"failed_boards={len(board_failures)}, "
                f"figure={'generated' if figure is not None else 'not_generated'}, "
                f"elapsed_sec={time.monotonic() - started_at:.2f}"
            )
            stage = "discord"
            self.logger.info(
                f"Discord upload started: record={observation.record_name}"
            )
            if figure is None:
                self.notifier.send_text(
                    discord_content or "Analysis completed without a figure"
                )
            elif discord_content is None:
                self.notifier.send_figure(figure, observation.record_name)
            else:
                self.notifier.send_figure(
                    figure, observation.record_name, content=discord_content
                )
            self.logger.info(
                f"Discord post completed: record={observation.record_name}, "
                f"elapsed_sec={time.monotonic() - started_at:.2f}"
            )
        except DiscordAttachmentTooLarge as exc:
            notification_result = (
                "failure notice sent"
                if exc.notification_sent
                else "failure notice could not be sent"
            )
            detail = ""
            if exc.notification_error is not None:
                detail = (
                    ", notification_error_type="
                    f"{type(exc.notification_error).__name__}"
                )
            self.logger.warning(
                "Discord attachment size limit exceeded: "
                f"record={observation.record_name}, "
                f"size_mib={exc.size_bytes / 1024 / 1024:.2f}, "
                f"limit_mib={exc.limit_bytes / 1024 / 1024:.2f}, "
                f"{notification_result}{detail}, "
                f"elapsed_sec={time.monotonic() - started_at:.2f}"
            )
        except Exception:
            self.logger.error(
                f"Analysis/Discord notification failed: "
                f"record={observation.record_name}, "
                f"stage={stage}, "
                f"elapsed_sec={time.monotonic() - started_at:.2f}\n"
                f"{traceback.format_exc()}"
            )
        finally:
            if figure is not None:
                try:
                    import matplotlib.pyplot as plt

                    plt.close(figure)
                except Exception:
                    self.logger.debug("Failed to close SkyDip figure", exc_info=True)

    def shutdown(self) -> None:
        if self._owns_executor:
            self._executor.shutdown(wait=True)
