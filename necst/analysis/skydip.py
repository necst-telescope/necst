"""Adapter for the bundled SkyDip analysis script."""

from __future__ import annotations

import importlib.util
import math
import os
from dataclasses import dataclass
from pathlib import Path
from types import ModuleType
from typing import Any, Mapping, Optional, Sequence


@dataclass(frozen=True)
class AnalysisOutput:
    """Analysis products passed from the script adapter to notifications."""

    figure: Any
    results: Mapping[str, Any]
    discord_content: str


class ScriptSkyDipAnalyzer:
    """Run the supplied SkyDip script without changing its analysis pipeline.

    The script is imported lazily and its public ``analyze_skydip_boards`` API
    is called as-is. This adapter only discovers board names and disables the
    script's optional persistent output so the returned Figure can be sent to
    Discord by the notification layer.
    """

    def __init__(
        self,
        telescope: str = "OMU1P85M",
        boards: Optional[Sequence[str]] = None,
        board_labels: Optional[Mapping[str, str]] = None,
        script_path: Optional[Path] = None,
        logger: Optional[Any] = None,
    ) -> None:
        self.telescope = telescope
        self.boards = tuple(boards or ())
        self.board_labels = {
            str(board): str(label)
            for board, label in (board_labels or {}).items()
            if str(board).strip() and str(label).strip()
        }
        self.script_path = Path(script_path).expanduser() if script_path else None
        self.logger = logger
        self._script_module: Optional[ModuleType] = None

    def analyze(self, record_path: Path) -> AnalysisOutput:
        record_path = Path(record_path)
        self._log_info(f"Analysis script load started: record={record_path.name}")
        module = self._load_script()
        self._log_info(
            f"Analysis script loaded: record={record_path.name}, "
            f"script={self.script_path or 'bundled script'}"
        )
        if self.boards:
            self._log_info(
                f"Analysis boards configured: record={record_path.name}, "
                f"boards={','.join(self.boards)}"
            )
        else:
            self._log_info(
                f"Analysis board discovery started: record={record_path.name}"
            )
        board_names = list(self.boards) or self._discover_boards(record_path)
        if not board_names:
            raise ValueError(f"No spectral boards found in {record_path}")
        self._log_info(
            f"Analysis board discovery completed: record={record_path.name}, "
            f"boards={','.join(board_names)}"
        )
        board_selection = self._board_selection(board_names)
        self._log_info(
            f"Analysis script execution started: record={record_path.name}, "
            f"boards={len(board_names)}"
        )
        try:
            results, figure, _ = self._run_analysis_script(
                module, record_path, board_selection
            )
        except AttributeError as exc:
            raise RuntimeError(
                "SkyDip script must define analyze_skydip_boards"
            ) from exc
        self._log_info(
            f"Analysis script execution completed: record={record_path.name}, "
            f"boards={len(results)}"
        )
        return AnalysisOutput(
            figure=figure,
            results=results,
            discord_content=format_discord_summary(
                record_path.name, results, self.board_labels
            ),
        )

    def _run_analysis_script(
        self,
        module: ModuleType,
        record_path: Path,
        board_selection: Any,
    ) -> Any:
        """Run the script API while logging each board boundary.

        The bundled script remains the source of the analysis behavior.  The
        temporary wrapper only records entry/exit around its existing public
        per-board function so a slow or stuck board can be identified.
        """

        analyze_board = getattr(module, "analyze_skydip_board", None)
        if not callable(analyze_board):
            return module.analyze_skydip_boards(
                record_path,
                board_selection,
                telescope=self.telescope,
                save_prefix=None,
            )

        def logged_analyze_board(*args: Any, **kwargs: Any) -> Any:
            board = kwargs.get("board")
            if board is None and len(args) >= 2:
                board = args[1]
            board_name = str(board or "unknown")
            self._log_info(
                f"Board analysis started: record={record_path.name}, "
                f"board={board_name}"
            )
            try:
                result = analyze_board(*args, **kwargs)
            except Exception as exc:
                self._log_error(
                    f"Board analysis failed: record={record_path.name}, "
                    f"board={board_name}, error_type={type(exc).__name__}, "
                    f"error={exc}"
                )
                raise
            self._log_info(
                f"Board analysis completed: record={record_path.name}, "
                f"board={board_name}"
            )
            return result

        module.analyze_skydip_board = logged_analyze_board
        try:
            return module.analyze_skydip_boards(
                record_path,
                board_selection,
                telescope=self.telescope,
                save_prefix=None,
            )
        finally:
            module.analyze_skydip_board = analyze_board

    def _log_info(self, message: str) -> None:
        if self.logger is not None:
            self.logger.info(message)

    def _log_error(self, message: str) -> None:
        if self.logger is not None:
            self.logger.error(message)

    def _board_selection(self, board_names: Sequence[str]) -> Any:
        """Return script input with configured display labels when available."""

        if not self.board_labels:
            return list(board_names)
        return {board: self.board_labels.get(board, board) for board in board_names}

    def _load_script(self) -> ModuleType:
        if self._script_module is not None:
            return self._script_module

        if self.script_path is None:
            from .scripts import skydip_step_jupyter_necstdb_v10 as module

            self._script_module = module
            return module

        if not self.script_path.is_file():
            raise FileNotFoundError(
                f"SkyDip analysis script not found: {self.script_path}"
            )
        spec = importlib.util.spec_from_file_location(
            "necst_configured_skydip_script", self.script_path
        )
        if spec is None or spec.loader is None:
            raise ImportError(f"Cannot load SkyDip analysis script: {self.script_path}")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        self._script_module = module
        return module

    def _discover_boards(self, record_path: Path) -> Sequence[str]:
        try:
            import necstdb
        except ImportError as exc:
            raise RuntimeError(
                "necstdb is required by the SkyDip analysis script"
            ) from exc

        database = necstdb.opendb(record_path)
        prefix = f"necst-{self.telescope.upper()}-data-spectral-"
        boards = [
            table_name[len(prefix) :]
            for table_name in database.list_tables()
            if table_name.startswith(prefix)
        ]
        if boards:
            return sorted(boards)

        # Keep compatibility with records whose telescope prefix differs from
        # the current environment while retaining only spectral tables.
        return sorted(
            table_name.split("data-spectral-", 1)[1]
            for table_name in database.list_tables()
            if "data-spectral-" in table_name
        )


def analyzer_from_environment(*, logger: Optional[Any] = None) -> ScriptSkyDipAnalyzer:
    """Build the script adapter using the telescope environment."""

    boards = tuple(
        value.strip()
        for value in os.environ.get("NECST_SKYDIP_BOARDS", "").split(",")
        if value.strip()
    )
    script_path = os.environ.get("NECST_SKYDIP_SCRIPT", "").strip()
    board_labels = _board_labels_from_config()
    return ScriptSkyDipAnalyzer(
        telescope=os.environ.get("NECST_SKYDIP_TELESCOPE", "OMU1P85M"),
        boards=boards,
        board_labels=board_labels,
        script_path=Path(script_path) if script_path else None,
        logger=logger,
    )


def _board_labels_from_config() -> Mapping[str, str]:
    """Read optional display labels from ``[analysis].board_labels``."""

    try:
        from necst import config as necst_config

        raw_labels = necst_config.get("analysis.board_labels")
    except (AttributeError, ImportError, KeyError):
        return {}
    return _normalize_board_labels(raw_labels)


def _normalize_board_labels(raw_labels: Any) -> Mapping[str, str]:
    """Normalize mapping or ``[{name, label}, ...]`` config forms."""

    if isinstance(raw_labels, Mapping):
        pairs = raw_labels.items()
    elif isinstance(raw_labels, Sequence) and not isinstance(
        raw_labels, (str, bytes, bytearray)
    ):
        pairs = (
            (item.get("name") or item.get("board"), item.get("label"))
            for item in raw_labels
            if isinstance(item, Mapping)
        )
    else:
        return {}
    normalized = {}
    for board, label in pairs:
        if board is None or label is None:
            continue
        board_name = str(board).strip()
        display_label = str(label).strip()
        if board_name and display_label:
            normalized[board_name] = display_label
    return normalized


def format_discord_summary(
    observation_name: str,
    results: Mapping[str, Any],
    board_labels: Optional[Mapping[str, str]] = None,
) -> str:
    """Format the compact Markdown summary sent with the analysis image."""

    labels = board_labels or {}
    qualities = [
        str(getattr(result, "quality", "")).upper() for result in results.values()
    ]
    overall = "BAD" if "BAD" in qualities else "WARN" if "WARN" in qualities else "GOOD"
    rows = []
    for board, result in results.items():
        label = str(getattr(result, "label", "") or labels.get(board, board))
        flags = (
            ",".join(str(flag) for flag in getattr(result, "quality_flags", []) or [])
            or "-"
        )
        rows.append(
            [
                str(board),
                label,
                str(getattr(result, "quality", "n/a")),
                _format_tau(result),
                _format_value(
                    getattr(result, "Tsys_sensitivity_zenith_K", float("nan"))
                ),
                _format_value(getattr(result, "Trx_K", float("nan"))),
                _format_value(getattr(result, "reduced_chi2", float("nan"))),
                str(getattr(result, "n_fit", "n/a")),
                flags,
            ]
        )

    headers = [
        "Board",
        "IF",
        "Q",
        "tau",
        "Tsys0[K]",
        "Trx[K]",
        "chi2red",
        "Nfit",
        "Flags",
    ]
    widths = [len(header) for header in headers]
    for row in rows:
        widths = [max(width, len(value)) for width, value in zip(widths, row)]

    lines = [
        "  ".join(value.ljust(width) for value, width in zip(headers, widths)),
        "  ".join("-" * width for width in widths),
    ]
    lines.extend(
        "  ".join(value.ljust(width) for value, width in zip(row, widths))
        for row in rows
    )
    safe_name = str(observation_name).replace("`", "'")
    return (
        "**📡 Analysis Result**\n\n"
        f"Observation: `{safe_name}`\n"
        f"Overall: `{overall}`\n\n"
        "```text\n" + "\n".join(lines) + "\n```"
    )


def _format_tau(result: Any) -> str:
    tau = _format_value(getattr(result, "tau", float("nan")))
    sigma = _format_value(getattr(result, "tau_sigma", float("nan")))
    if tau == "n/a" or sigma == "n/a":
        return "n/a"
    return f"{tau} +/- {sigma}"


def _format_value(value: Any) -> str:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return "n/a"
    return "n/a" if not math.isfinite(number) else f"{number:.3f}"
