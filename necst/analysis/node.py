"""Post-observation SkyDip analysis coordinator."""

from __future__ import annotations

import logging
from concurrent.futures import Executor, Future, ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from threading import Lock
from typing import Any, Mapping, Optional


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
            self._pending = candidate
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
            self._recording_state_seen = True
            self._recording = bool(recording)
            return self._schedule_pending_locked()

    def _analyze_and_notify(self, observation: FinishedObservation) -> None:
        figure = None
        try:
            if not observation.record_path.is_dir():
                raise FileNotFoundError(
                    f"SkyDip record directory does not exist: {observation.record_path}"
                )
            figure = self.analyzer.analyze(observation.record_path)
            self.notifier.send_figure(figure, observation.record_name)
            self.logger.info(
                "SkyDip analysis posted to Discord: %s", observation.record_name
            )
        except Exception:
            self.logger.exception(
                "SkyDip analysis/Discord notification failed: %s",
                observation.record_name,
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
