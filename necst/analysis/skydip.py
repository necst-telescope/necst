"""Adapter for the bundled SkyDip analysis script."""

from __future__ import annotations

import importlib.util
import os
from pathlib import Path
from types import ModuleType
from typing import Any, Optional, Sequence


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
        script_path: Optional[Path] = None,
    ) -> None:
        self.telescope = telescope
        self.boards = tuple(boards or ())
        self.script_path = Path(script_path).expanduser() if script_path else None
        self._script_module: Optional[ModuleType] = None

    def analyze(self, record_path: Path) -> Any:
        record_path = Path(record_path)
        module = self._load_script()
        board_names = list(self.boards) or self._discover_boards(record_path)
        if not board_names:
            raise ValueError(f"No spectral boards found in {record_path}")
        try:
            _, figure, _ = module.analyze_skydip_boards(
                record_path,
                board_names,
                telescope=self.telescope,
                save_prefix=None,
            )
        except AttributeError as exc:
            raise RuntimeError(
                "SkyDip script must define analyze_skydip_boards"
            ) from exc
        return figure

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


def analyzer_from_environment() -> ScriptSkyDipAnalyzer:
    """Build the script adapter using the telescope environment."""

    boards = tuple(
        value.strip()
        for value in os.environ.get("NECST_SKYDIP_BOARDS", "").split(",")
        if value.strip()
    )
    script_path = os.environ.get("NECST_SKYDIP_SCRIPT", "").strip()
    return ScriptSkyDipAnalyzer(
        telescope=os.environ.get("NECST_SKYDIP_TELESCOPE", "OMU1P85M"),
        boards=boards,
        script_path=Path(script_path) if script_path else None,
    )
