"""R-Sky analysis adapter based on the necbook calibration notebook."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping, Optional, Sequence

import numpy as np

from .skydip import AnalysisOutput


def _spectral_tables(database: Any, telescope: str) -> Sequence[tuple[str, str]]:
    prefix = f"necst-{str(telescope).strip().upper()}-data-spectral-"
    names = [str(name) for name in database.list_tables()]
    selected = [
        (name, name[len(prefix) :])
        for name in names
        if name.startswith(prefix)
    ]
    if selected:
        return sorted(selected, key=lambda item: item[1])
    return sorted(
        (name, name.split("data-spectral-", 1)[1])
        for name in names
        if "data-spectral-" in name
    )


def _position_labels(values: Any) -> np.ndarray:
    labels = np.asarray(values).reshape(-1)
    if labels.dtype.kind == "S":
        return np.char.strip(labels.astype("U"))
    return np.asarray(
        [
            value.decode(errors="replace") if isinstance(value, bytes) else str(value)
            for value in labels
        ],
        dtype="U",
    ).astype("U")


def _hot_sky_averages(table_data: Any) -> tuple[np.ndarray, np.ndarray]:
    data = np.asarray(table_data["data"], dtype=float)
    positions = _position_labels(table_data["position"])
    if data.ndim != 2 or data.shape[0] != positions.size:
        raise ValueError("R-Sky data and position lengths do not match")
    hot = data[positions == "HOT"]
    sky = data[positions == "SKY"]
    if not len(hot) or not len(sky):
        raise ValueError("R-Sky table must contain both HOT and SKY samples")
    return np.nanmean(hot, axis=0), np.nanmean(sky, axis=0)


def _finite_median(values: Any) -> float:
    values = np.asarray(values, dtype=float)
    valid = values[np.isfinite(values)]
    return float(np.median(valid)) if valid.size else float("nan")


def format_rsky_summary(
    observation_name: str,
    results: Mapping[str, Mapping[str, Any]],
    telescope: str,
) -> str:
    safe_name = str(observation_name or "unknown").replace("`", "'")
    lines = [
        "**📡 R-Sky Analysis Result**",
        f"Telescope: `{str(telescope or 'unknown').strip().upper()}`",
        f"Observation: `{safe_name}`",
        "",
        "| Board | Y-factor median | Tsys median [K] |",
        "| --- | ---: | ---: |",
    ]
    for board, result in results.items():
        lines.append(
            f"| {board} | {_finite_median(result['y_factor']):.3f} | "
            f"{_finite_median(result['tsys_K']):.3f} |"
        )
    return "\n".join(lines)


class RSkyAnalyzer:
    """Calculate and plot R-Sky HOT/SKY calibration products."""

    def __init__(self, telescope: str, logger: Optional[Any] = None) -> None:
        self.telescope = str(telescope).strip().upper()
        self.logger = logger

    def analyze(self, record_path: Path) -> AnalysisOutput:
        import matplotlib.pyplot as plt
        import necstdb

        record_path = Path(record_path)
        database = necstdb.opendb(record_path)
        tables = _spectral_tables(database, self.telescope)
        if not tables:
            raise ValueError(f"No spectral tables found in {record_path}")

        rows = (len(tables) + 3) // 4
        figure, axes = plt.subplots(rows, 4, squeeze=False, figsize=(16, 4 * rows))
        axes_flat = axes.reshape(-1)
        results = {}
        for index, (table_name, board) in enumerate(tables):
            hot, sky = _hot_sky_averages(
                database.open_table(table_name).read(astype="array")
            )
            with np.errstate(divide="ignore", invalid="ignore"):
                y_factor = hot / sky
                tsys = 290.0 / (y_factor - 1.0)
            result = {"hot": hot, "sky": sky, "y_factor": y_factor, "tsys_K": tsys}
            results[board] = result

            axis = axes_flat[index]
            axis.plot(sky, label="SKY")
            axis.plot(hot, label="HOT")
            axis.set_yscale("log")
            axis.set_title(board)
            twin = axis.twinx()
            twin.plot(tsys, color="green", label="Tsys")
            twin.set_ylabel("Tsys [K]")
            lines = axis.lines + twin.lines
            axis.legend(lines, [line.get_label() for line in lines], loc=0)

        for axis in axes_flat[len(tables) :]:
            axis.set_visible(False)
        figure.tight_layout()
        return AnalysisOutput(
            figure=figure,
            results=results,
            discord_content=format_rsky_summary(
                record_path.name,
                results,
                self.telescope,
            ),
        )
