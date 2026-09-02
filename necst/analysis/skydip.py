"""Adapter for the bundled SkyDip analysis script."""

from __future__ import annotations

import importlib.util
import math
import os
from dataclasses import dataclass, field
from pathlib import Path
from types import ModuleType
from typing import Any, Mapping, Optional, Sequence


@dataclass(frozen=True)
class AnalysisOutput:
    """Analysis products passed from the script adapter to notifications."""

    figure: Any
    results: Mapping[str, Any]
    discord_content: Optional[str]
    board_failures: Mapping[str, str] = field(default_factory=dict)


class ScriptSkyDipAnalyzer:
    """Run the supplied SkyDip script without changing its analysis algorithms.

    The script is imported lazily and its existing per-board analysis and plot
    APIs are called. This adapter only discovers board names, isolates a
    failed board for notification, and disables persistent output so the
    returned Figure can be sent to Discord by the notification layer.
    """

    def __init__(
        self,
        telescope: str = "OMU1P85M",
        boards: Optional[Sequence[str]] = None,
        board_labels: Optional[Mapping[str, str]] = None,
        script_path: Optional[Path] = None,
        logger: Optional[Any] = None,
    ) -> None:
        self.telescope = str(telescope).strip().upper()
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
            results, figure, board_failures = self._run_analysis_script(
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
                record_path.name,
                results,
                self.board_labels,
                telescope=self.telescope,
                board_failures=board_failures,
            ),
            board_failures=board_failures,
        )

    def _run_analysis_script(
        self,
        module: ModuleType,
        record_path: Path,
        board_selection: Any,
    ) -> Any:
        """Run the existing script APIs while isolating board failures.

        The bundled script remains the source of the analysis behavior. The
        adapter calls its existing ``analyze_skydip_board`` function for each
        board and its existing ``plot_skydip_results`` function for successful
        results. It does not reimplement the analysis or plotting algorithms.
        """

        analyze_board = getattr(module, "analyze_skydip_board", None)
        if not callable(analyze_board):
            results, figure, _ = module.analyze_skydip_boards(
                record_path,
                board_selection,
                telescope=self.telescope,
                save_prefix=None,
            )
            return results, figure, {}

        if isinstance(board_selection, Mapping):
            board_items = list(board_selection.items())
        else:
            board_items = [(board, board) for board in board_selection]

        results = {}
        board_failures = {}
        for board, label in board_items:
            board_name = str(board)
            self._log_info(
                f"Board analysis started: record={record_path.name}, "
                f"board={board_name}"
            )
            try:
                result = analyze_board(
                    record_path,
                    board,
                    label=label,
                    telescope=self.telescope,
                )
            except Exception as exc:
                self._log_error(
                    f"Board analysis failed: record={record_path.name}, "
                    f"board={board_name}, error_type={type(exc).__name__}, "
                    f"error={exc}"
                )
                board_failures[board_name] = (
                    f"{type(exc).__name__}: {str(exc).replace(chr(10), ' ')}"
                )
                continue
            results[board_name] = result
            self._log_info(
                f"Board analysis completed: record={record_path.name}, "
                f"board={board_name}"
            )

        figure = None
        plot_results = getattr(module, "plot_skydip_results", None)
        if results and callable(plot_results):
            figure, _ = plot_results(
                results,
                suptitle=record_path.name,
                save_png=None,
                save_pdf=None,
            )
        return results, figure, board_failures

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
        telescope=os.environ["TELESCOPE"],
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
    *,
    telescope: Optional[str] = None,
    board_failures: Optional[Mapping[str, str]] = None,
) -> str:
    """Format the compact Markdown summary sent with the analysis image."""

    labels = board_labels or {}
    qualities = [
        str(getattr(result, "quality", "")).upper() for result in results.values()
    ]
    failures = board_failures or {}
    if failures and results:
        overall = "PARTIAL"
    elif failures:
        overall = "ERROR"
    else:
        overall = (
            "BAD" if "BAD" in qualities else "WARN" if "WARN" in qualities else "GOOD"
        )
    rows = []
    for board, result in results.items():
        label = str(getattr(result, "label", "") or labels.get(board, board))
        rows.append(
            [
                str(board),
                label,
                str(getattr(result, "quality", "n/a")),
                _format_value(getattr(result, "tau", float("nan"))),
                _format_value(getattr(result, "tau_sigma", float("nan"))),
                _format_value(
                    getattr(result, "Tsys_sensitivity_zenith_K", float("nan"))
                ),
                _format_value(getattr(result, "Trx_K", float("nan"))),
                _format_result_error(
                    result,
                    "Trx_sigma_K",
                    "Trx_err_K",
                    "Trx_robust_scatter_K",
                ),
                _format_value(getattr(result, "reduced_chi2", float("nan"))),
                str(getattr(result, "n_fit", "n/a")),
            ]
        )

    error_lines = []
    for board, error in failures.items():
        label = str(labels.get(board, board))
        short_error = str(error).replace("`", "'").replace("\n", " ")[:180]
        error_lines.append(f"{board}: {short_error}")
        rows.append(
            [
                str(board),
                label,
                "ERROR",
                "n/a",
                "n/a",
                "n/a",
                "n/a",
                "n/a",
                "n/a",
                "n/a",
            ]
        )

    headers = [
        "Board",
        "IF",
        "Q",
        "tau",
        "e_tau",
        "Tsys0",
        "Trx",
        "e_Trx",
        "chi2red",
        "Nfit",
    ]

    def markdown_row(values):
        cells = [str(value).replace("|", "\\|").replace("\n", " ") for value in values]
        return "| " + " | ".join(cells) + " |"

    lines = [
        markdown_row(headers),
        markdown_row(["---"] * len(headers)),
    ]
    lines.extend(markdown_row(row) for row in rows)
    safe_name = str(observation_name).replace("`", "'")
    safe_telescope = str(telescope or "unknown").replace("`", "'").strip().upper()
    return (
        "**📡 Skydip Analysis Result**\n\n"
        f"Telescope: `{safe_telescope}`\n"
        f"Observation: `{safe_name}`\n"
        f"Overall: `{overall}`\n\n"
        + "```text\n"
        + "\n".join(lines)
        + "\n```"
        + ("\n\nAnalysis errors:\n" + "\n".join(error_lines) if error_lines else "")
    )


def _format_result_error(result: Any, *attribute_names: str) -> str:
    for attribute_name in attribute_names:
        value = _format_value(getattr(result, attribute_name, float("nan")))
        if value != "n/a":
            return value
    return "n/a"


def _format_value(value: Any) -> str:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return "n/a"
    return "n/a" if not math.isfinite(number) else f"{number:.3f}"
