#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# fmt: off
"""
skydip_step_jupyter_necstdb_v10.py

Jupyter-oriented step/stare sky-dip analysis for NECST v4 raw data.

Main design points
------------------
- This version is for step/stare sky-dip observations: the telescope stops at a fixed El, integrates SKY/OFF, then moves to the next El.
- It is not the continuous-scan reduction used by skydip_necst_v4_v1.py.
- nercst is NOT required.  The default loader reads necstdb tables directly.
- Az/El correction sign is configurable with azel_correction_apply:
  auto, subtract/minus, add/plus, or none.  auto uses subtract for OMU/1.85m and add for NANTEN2.
- SKY grouping can use the observation id column.  For the current NECST Skydip observation program,
  self.sky(integ_time, id=el) is expected to write one id per elevation.  Therefore sky_grouping="id"
  is the safest explicit setting; sky_grouping="auto" also selects id when the id column exists.
- The plot is intentionally close to the current nercst.skydip output:
  one panel per spectrometer board, points with error bars, fit line, and tau text.
- Extra diagnostics are added for observing decision support:
  Tsys at zenith, Trx, reduced chi^2, residual scatter, leave-one-out tau scatter,
  bootstrap tau interval, and explicit weighting information.
- Trx/Tsys estimation supports forward efficiency and ground/spillover pickup through
  forward_efficiency and spillover_temperature_K.  The tau fit itself remains the
  standard nercst-like linear skydip fit; forward efficiency is applied to the
  physical calibration from powers to Trx/Tsys.  The plot label Tsys0 means the sensitivity-scale zenith Tsys: Tsys_receiver_input_zenith / (eta_f * exp(-tau)).  Trx point-to-point scatter is reported as a diagnostic; Tspill is kept in tables for provenance but not shown in the plot text.

Definitions
-----------
x = sec Z = 1 / cos(Z),  Z = 90 deg - El_true.

Two vertical-axis definitions are supported:

1. y_mode="logdiff"  [default; current nercst-like display]
      y = ln(P_hot - P_sky)
      y = a - tau * secZ, if gain is stable.

2. y_mode="logratio" [gain-drift reduced; matches the user's older script]
      y = ln((P_hot - P_sky) / P_hot)
      y = a - tau * secZ, if P_hot traces gain drift.

Uncertainty propagation
-----------------------
For logdiff:
    sigma_y^2 = (sigma_hot^2 + sigma_sky^2) / (P_hot - P_sky)^2

For logratio:
    dy/dP_sky = -1 / (P_hot - P_sky)
    dy/dP_hot = 1 / (P_hot - P_sky) - 1 / P_hot
    sigma_y^2 = (dy/dP_sky)^2 sigma_sky^2 + (dy/dP_hot)^2 sigma_hot^2

Weighted fitting
----------------
The default fit minimizes
    sum_i ((y_i - (a - tau*x_i)) / sigma_y_i)^2.
Therefore the effective fitting weight is 1/sigma_y_i^2.
This is implemented explicitly, not through np.polyfit's w argument.
The plotted error bar is the same sigma_y used by the fit.
By default sigma_kind="sem", so sigma_y is propagated from the standard error
of each representative HOT/SKY point, plus error_floor in log units.
Use sigma_kind="scatter" to plot and fit using the within-El/run scatter itself.

Typical Jupyter use
-------------------
# The v10 module defines these APIs below; no v6 compatibility import is needed.

raw = "/home/ogawalab/data/necst_skydip_20260526_150502"
boards = {"xffts-board3": "B3USB", "xffts-board4": "B3LSB"}

results, fig, axes = analyze_skydip_boards(
    raw,
    boards,
    telescope="OMU1P85M",
    y_mode="logdiff",
    thot_K=293.0,
    tatm_K=293.0,
    forward_efficiency=0.95,          # eta_f; use 1.0 if unknown
    spillover_temperature_K=293.0,    # ground/spillover temperature; None -> thot_K
    bad_ch=[(0, 1000), (16000, 18000), (27000, 32767)],
    save_prefix="skydip_20260526_150502",
    sky_grouping="id",        # recommended for this Skydip program: one point per elevation id
    hot_assignment="previous", # nercst-like: use preceding HOT block
)

summary_dataframe(results)
results["xffts-board3"].points
"""

from __future__ import annotations

import builtins
import dataclasses
import math
import pathlib
import warnings
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple, Union

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

Number = Union[int, float, np.number]
Range = Tuple[int, int]

__version__ = "10.0-tsys0-trx-scatter-no-tspill-display"



# =============================================================================
# Dataclasses
# =============================================================================

@dataclasses.dataclass
class SkydipResult:
    """One-board sky-dip result."""

    board: str
    label: str
    y_mode: str
    tau: float
    tau_sigma: float
    intercept: float
    intercept_sigma: float
    n_fit: int
    dof: int
    chi2: float
    reduced_chi2: float
    residual_rms: float
    residual_mad_sigma: float
    loo_tau_std: float
    loo_tau_max_abs_diff: float
    bootstrap_tau_median: float
    bootstrap_tau_p16: float
    bootstrap_tau_p84: float
    # Clear names introduced in v8.
    # Tsys_receiver_input_zenith_K: receiver-input system temperature, no opacity/efficiency correction.
    # Tsys_sensitivity_zenith_K: system temperature for sensitivity/radiometer equation, corrected by eta_f*exp(-tau).
    Tsys_receiver_input_zenith_K: float
    Tsys_sensitivity_zenith_K: float
    Tsys_opacity_only_zenith_K: float
    # Backward-compatible aliases retained from v7.
    Tsys_zenith_K: float
    Tsys_input_zenith_K: float
    Tsys_opacity_corrected_zenith_K: float
    Tsys_tastar_zenith_K: float
    Trx_K: float
    Trx_robust_scatter_K: float
    Trx_robust_frac_scatter: float
    Tsky_zenith_K: float
    Tsky_forward_zenith_K: float
    forward_efficiency: float
    spillover_temperature_K: float
    gain_median: float
    gain_robust_frac_scatter: float
    quality: str
    quality_flags: List[str]
    weighting: str
    hot_assignment: str
    points: pd.DataFrame
    hot_blocks: pd.DataFrame
    mode_diag: Dict[str, float]
    time_info: Dict[str, Any]

    def as_dict(self) -> Dict[str, Any]:
        d = dataclasses.asdict(self)
        d.pop("points", None)
        d.pop("hot_blocks", None)
        return d


# =============================================================================
# Small utilities
# =============================================================================

def _decode_label(v: Any) -> str:
    if isinstance(v, (bytes, bytearray, np.bytes_)):
        try:
            return bytes(v).decode(errors="ignore").strip().upper()
        except Exception:
            return str(v).strip().upper()
    if hasattr(v, "item"):
        try:
            return _decode_label(v.item())
        except Exception:
            pass
    return str(v).strip().upper()


def _decode_id(v: Any) -> str:
    """Decode an observation id while preserving numeric/elevation labels.

    Unlike position labels, ids are not upper-cased.  The current Skydip
    observation program passes id=el, so values such as "70", "50", ...
    should remain visible in the representative-point table.
    """
    if isinstance(v, (bytes, bytearray, np.bytes_)):
        try:
            s = bytes(v).decode(errors="ignore")
        except Exception:
            s = str(v)
    else:
        if hasattr(v, "item"):
            try:
                return _decode_id(v.item())
            except Exception:
                pass
        s = str(v)
    s = s.strip().strip("\x00")
    if s.lower() in ("nan", "none"):
        return ""
    return s


def _nonempty_id_nunique(values: Any) -> int:
    vals = [_decode_id(v) for v in list(values)]
    return len({v for v in vals if str(v).strip() != ""})


def _as_float_array(a: Any) -> np.ndarray:
    return np.asarray(a, dtype=float)


def _mad_sigma(arr: Sequence[Number]) -> float:
    x = np.asarray(arr, dtype=float)
    x = x[np.isfinite(x)]
    if x.size == 0:
        return float("nan")
    med = float(np.nanmedian(x))
    return 1.4826 * float(np.nanmedian(np.abs(x - med)))


def _mean_sem_or_madsem(values: Sequence[Number], center: str = "median") -> Tuple[float, float, float, int]:
    """Return central value, scatter sigma, SEM-like sigma of the central value, and N."""
    x = np.asarray(values, dtype=float)
    x = x[np.isfinite(x)]
    n = int(x.size)
    if n == 0:
        return float("nan"), float("nan"), float("nan"), 0
    if center == "mean":
        c = float(np.nanmean(x))
    else:
        c = float(np.nanmedian(x))
    scatter = _mad_sigma(x)
    if (not np.isfinite(scatter)) or scatter <= 0:
        scatter = float(np.nanstd(x, ddof=1)) if n > 1 else 0.0
    sem = float(scatter / math.sqrt(builtins.max(n, 1)))
    return c, scatter, sem, n


def _parse_ranges(ranges: Optional[Union[str, Sequence[Range]]]) -> List[Range]:
    if ranges is None:
        return []
    if isinstance(ranges, str):
        s = ranges.strip()
        if not s:
            return []
        out: List[Range] = []
        for part in s.split(","):
            p = part.strip()
            if not p:
                continue
            if ":" in p:
                a, b = p.split(":", 1)
                ia, ib = int(a), int(b)
            else:
                ia = ib = int(p)
            if ib < ia:
                ia, ib = ib, ia
            out.append((ia, ib))
        return out
    out = []
    for a, b in ranges:
        ia, ib = int(a), int(b)
        if ib < ia:
            ia, ib = ib, ia
        out.append((ia, ib))
    return out


def _mask_channels(spec: np.ndarray, include_ch: List[Range], bad_ch: List[Range]) -> np.ndarray:
    x = np.asarray(spec, dtype=float).copy()
    if x.ndim != 1:
        x = np.ravel(x)
    n = x.size
    if include_ch:
        keep = np.zeros(n, dtype=bool)
        for a, b in include_ch:
            s = builtins.max(0, int(a))
            e = builtins.min(n - 1, int(b))
            if s <= e:
                keep[s : e + 1] = True
        x[~keep] = np.nan
    for a, b in bad_ch:
        s = builtins.max(0, int(a))
        e = builtins.min(n - 1, int(b))
        if s <= e:
            x[s : e + 1] = np.nan
    return x


def _finite_interp(t_src: np.ndarray, y_src: np.ndarray, t_dst: np.ndarray, *, circular_deg: bool = False) -> np.ndarray:
    t = np.asarray(t_src, dtype=float)
    y = np.asarray(y_src, dtype=float)
    q = np.asarray(t_dst, dtype=float)
    m = np.isfinite(t) & np.isfinite(y)
    if int(np.sum(m)) < 2:
        return np.full_like(q, np.nan, dtype=float)
    t = t[m]
    y = y[m]
    order = np.argsort(t)
    t = t[order]
    y = y[order]
    if circular_deg:
        yr = np.unwrap(np.deg2rad(y))
        out = np.interp(q, t, yr, left=np.nan, right=np.nan)
        return np.rad2deg(out) % 360.0
    return np.interp(q, t, y, left=np.nan, right=np.nan)


def normalize_azel_correction_apply(value: Optional[str], *, telescope: str = "") -> str:
    """Normalize Az/El correction mode.

    Returned values:
      subtract: Az_true = Az_enc - dlon, El_true = El_enc - dlat
      add     : Az_true = Az_enc + dlon, El_true = El_enc + dlat
      none    : Az_true = Az_enc,        El_true = El_enc

    Aliases:
      minus -> subtract, plus -> add.

    auto:
      NANTEN2 -> add
      OMU/1.85m and unknown telescopes -> subtract
    """
    s = str(value if value is not None else "auto").strip().lower().replace("_", "-")
    if s in ("", "auto", "default"):
        tel = str(telescope or "").strip().upper().replace("_", "").replace("-", "")
        if "NANTEN2" in tel:
            return "add"
        return "subtract"
    aliases = {
        "subtract": "subtract",
        "sub": "subtract",
        "minus": "subtract",
        "-": "subtract",
        "add": "add",
        "plus": "add",
        "+": "add",
        "none": "none",
        "raw": "none",
        "off": "none",
        "false": "none",
        "0": "none",
    }
    if s not in aliases:
        raise ValueError(
            "unsupported azel_correction_apply={!r}; choose auto, subtract/minus, add/plus, or none".format(value)
        )
    return aliases[s]


def apply_azel_correction(
    az_enc_deg: np.ndarray,
    el_enc_deg: np.ndarray,
    dlon_deg: np.ndarray,
    dlat_deg: np.ndarray,
    *,
    mode: str,
) -> Tuple[np.ndarray, np.ndarray]:
    """Apply NECST altaz correction to encoder Az/El."""
    az = np.asarray(az_enc_deg, dtype=float)
    el = np.asarray(el_enc_deg, dtype=float)
    dlon = np.asarray(dlon_deg, dtype=float)
    dlat = np.asarray(dlat_deg, dtype=float)
    mode = str(mode).lower().strip()
    if mode == "subtract":
        return (az - dlon) % 360.0, el - dlat
    if mode == "add":
        return (az + dlon) % 360.0, el + dlat
    if mode == "none":
        return az % 360.0, el
    raise ValueError("unsupported normalized azel correction mode={!r}".format(mode))


def _dedup_mean_time(df: pd.DataFrame, tcol: str, cols: Sequence[str]) -> Tuple[np.ndarray, Dict[str, np.ndarray]]:
    cols = list(cols)
    d = df[[tcol] + cols].copy()
    d[tcol] = pd.to_numeric(d[tcol], errors="coerce")
    for c in cols:
        d[c] = pd.to_numeric(d[c], errors="coerce")
    d = d[np.isfinite(d[tcol].to_numpy(float))]
    if d.empty:
        return np.array([], float), {c: np.array([], float) for c in cols}
    g = d.groupby(tcol, as_index=False).mean(numeric_only=True).sort_values(tcol)
    return g[tcol].to_numpy(float), {c: g[c].to_numpy(float) for c in cols}


def _pick_time_axis(
    df: pd.DataFrame,
    *,
    mode: str = "auto",
    time_col: str = "time",
    recorded_time_col: str = "recorded_time",
    label: str = "table",
) -> Tuple[np.ndarray, Dict[str, Any]]:
    mode = str(mode).lower().strip()
    has_time = time_col in df.columns
    has_rec = recorded_time_col in df.columns
    info: Dict[str, Any] = {"requested": mode, "used": None, "offset_sec": None}

    if mode == "time":
        if not has_time:
            raise RuntimeError(f"{label}: time column {time_col!r} not found")
        info["used"] = time_col
        return pd.to_numeric(df[time_col], errors="coerce").to_numpy(float), info

    if mode == "recorded":
        if not has_rec:
            raise RuntimeError(f"{label}: recorded_time column {recorded_time_col!r} not found")
        info["used"] = recorded_time_col
        return pd.to_numeric(df[recorded_time_col], errors="coerce").to_numpy(float), info

    if mode != "auto":
        raise ValueError("time mode must be 'auto', 'time', or 'recorded'")

    if has_time and has_rec:
        t = pd.to_numeric(df[time_col], errors="coerce").to_numpy(float)
        r = pd.to_numeric(df[recorded_time_col], errors="coerce").to_numpy(float)
        m = np.isfinite(t) & np.isfinite(r)
        if int(np.sum(m)) >= 20:
            off = float(np.nanmedian(r[m] - t[m]))
            info["used"] = f"{recorded_time_col}-median_offset"
            info["offset_sec"] = off
            return r - off, info

    if has_time:
        info["used"] = time_col
        return pd.to_numeric(df[time_col], errors="coerce").to_numpy(float), info
    if has_rec:
        info["used"] = recorded_time_col
        return pd.to_numeric(df[recorded_time_col], errors="coerce").to_numpy(float), info
    raise RuntimeError(f"{label}: neither {time_col!r} nor {recorded_time_col!r} found")


# =============================================================================
# necstdb-only loader
# =============================================================================

def _import_necstdb():
    try:
        import necstdb  # type: ignore
    except Exception as exc:
        raise RuntimeError(
            "necstdb is required for the default loader. Install/import necstdb, "
            "or pass a pre-built DataFrame to analyze_skydip_dataframe()."
        ) from exc
    return necstdb


def _read_table_raw_bytes(table: Any) -> bytes:
    """Read a necstdb table as raw bytes when normal pandas reading fails."""
    for kind in ("raw", "buffer"):
        try:
            b = table.read(astype=kind)
            if isinstance(b, (bytes, bytearray)):
                return bytes(b)
        except Exception:
            pass
    raise RuntimeError("cannot read raw/buffer from necstdb table")


def _get_table_dtype(table: Any) -> Optional[np.dtype]:
    """Best-effort dtype retrieval for tolerant necstdb reads."""
    for attr in ("dtype", "_dtype", "data_dtype", "_data_dtype"):
        try:
            dt = getattr(table, attr)
            if dt is not None:
                return np.dtype(dt)
        except Exception:
            pass
    try:
        arr = table.read(num=1, astype="array")
        return np.dtype(arr.dtype)
    except Exception:
        return None


def _read_structured_array_tolerant_from_table(table: Any, table_name: str) -> np.ndarray:
    """Read a necstdb table as a structured array, trimming an incomplete tail if needed.

    This mirrors the robust strategy used in the NECST v4 SDFITS converter: try the
    normal structured read first, then fall back to raw bytes and drop only the
    incomplete final record.
    """
    try:
        return table.read(astype="array")
    except Exception as first_exc:
        raw = _read_table_raw_bytes(table)
        dt = _get_table_dtype(table)
        if dt is None:
            raise RuntimeError(f"cannot determine dtype for table {table_name!r}: {first_exc}") from first_exc
        item = int(dt.itemsize)
        if item <= 0:
            raise RuntimeError(f"invalid dtype itemsize for table {table_name!r}: {item}")
        nrec = len(raw) // item
        if nrec <= 0:
            raise RuntimeError(f"raw buffer too small for table {table_name!r}: len={len(raw)} itemsize={item}")
        return np.frombuffer(raw[: nrec * item], dtype=dt)


def _structured_to_dataframe(arr: np.ndarray) -> pd.DataFrame:
    try:
        return pd.DataFrame.from_records(arr)
    except Exception:
        return pd.DataFrame({name: arr[name] for name in (arr.dtype.names or [])})


def _read_necstdb_table(rawdata_path: Union[str, pathlib.Path], table_name: str) -> pd.DataFrame:
    necstdb = _import_necstdb()
    db = necstdb.opendb(pathlib.Path(rawdata_path).expanduser().resolve())
    try:
        table = db.open_table(table_name)
    except Exception as exc:
        raise RuntimeError(f"cannot open necstdb table {table_name!r}: {exc}") from exc
    try:
        return table.read(astype="pandas")
    except Exception as pandas_exc:
        try:
            arr = _read_structured_array_tolerant_from_table(table, table_name)
            return _structured_to_dataframe(arr)
        except Exception as tolerant_exc:
            raise RuntimeError(
                f"cannot read necstdb table {table_name!r}: pandas={pandas_exc}; tolerant={tolerant_exc}"
            ) from tolerant_exc


def _infer_spectral_column(df: pd.DataFrame, preferred: Optional[str] = None) -> str:
    if preferred is not None:
        if preferred not in df.columns:
            raise RuntimeError(f"spectral column {preferred!r} not found; columns={list(df.columns)}")
        return preferred

    priority = [
        "data",
        "spectrum",
        "spectra",
        "spec",
        "array",
        "value",
        "power",
    ]
    for c in priority:
        if c in df.columns:
            sample = df[c].dropna()
            if len(sample) > 0:
                try:
                    arr = np.asarray(sample.iloc[0])
                    if arr.size > 8:
                        return c
                except Exception:
                    pass

    for c in df.columns:
        sample = df[c].dropna()
        if len(sample) == 0:
            continue
        for v in sample.iloc[:5]:
            try:
                arr = np.asarray(v)
                if arr.size > 8 and arr.ndim >= 1:
                    return c
            except Exception:
                continue
    raise RuntimeError(
        "Cannot infer spectral data column from necstdb spectral table. "
        f"Please pass spectral_data_col explicitly. columns={list(df.columns)}"
    )


def _infer_column(df: pd.DataFrame, candidates: Sequence[str], purpose: str) -> str:
    for c in candidates:
        if c in df.columns:
            return c
    raise RuntimeError(f"Cannot infer {purpose} column. candidates={candidates}, columns={list(df.columns)}")


def load_skydip_dataframe_necstdb(
    rawdata_path: Union[str, pathlib.Path],
    board: str,
    *,
    telescope: str = "OMU1P85M",
    spectral_table_template: str = "necst-{telescope}-data-spectral-{board}",
    encoder_table: str = "ctrl-antenna-encoder",
    altaz_table: str = "ctrl-antenna-altaz",
    spectral_data_col: Optional[str] = None,
    spectral_time_col: Optional[str] = None,
    position_col: str = "position",
    id_col: Optional[str] = "id",
    encoder_time_mode: str = "auto",
    altaz_time_mode: str = "auto",
    azel_correction_apply: str = "auto",
    time_col: str = "time",
    recorded_time_col: str = "recorded_time",
    include_ch: Optional[Union[str, Sequence[Range]]] = None,
    bad_ch: Optional[Union[str, Sequence[Range]]] = None,
    tp_statistic: str = "median",
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """Load one board from NECST v4 raw data using necstdb only.

    Returns a DataFrame with columns:
        t_unix, tp, az_true, el_true, position, id, az_enc, el_enc, dlon, dlat
    """
    raw = pathlib.Path(rawdata_path).expanduser().resolve()
    table = spectral_table_template.format(telescope=telescope, board=board)
    ds = _read_necstdb_table(raw, table)

    data_col = _infer_spectral_column(ds, spectral_data_col)
    if spectral_time_col is None:
        spectral_time_col = _infer_column(ds, ["time", "t", "timestamp", "recorded_time"], "spectral time")
    if position_col not in ds.columns:
        raise RuntimeError(f"position column {position_col!r} not found in {table}; columns={list(ds.columns)}")

    include = _parse_ranges(include_ch)
    bad = _parse_ranges(bad_ch)

    t_spec = pd.to_numeric(ds[spectral_time_col], errors="coerce").to_numpy(float)
    positions = np.array([_decode_label(v) for v in ds[position_col].to_numpy()], dtype=object)
    if id_col is not None and str(id_col) in ds.columns:
        id_values = np.array([_decode_id(v) for v in ds[str(id_col)].to_numpy()], dtype=object)
        id_col_used: Optional[str] = str(id_col)
    else:
        id_values = np.array([""] * len(ds), dtype=object)
        id_col_used = None

    tp: List[float] = []
    for v in ds[data_col].to_numpy():
        spec = _mask_channels(np.asarray(v, dtype=float), include, bad)
        if tp_statistic == "mean":
            tp.append(float(np.nanmean(spec)))
        else:
            tp.append(float(np.nanmedian(spec)))
    p_spec = np.asarray(tp, dtype=float)

    order = np.argsort(t_spec)
    t_spec = t_spec[order]
    p_spec = p_spec[order]
    positions = positions[order]
    id_values = id_values[order]

    enc_table_name = f"necst-{telescope}-{encoder_table}"
    alt_table_name = f"necst-{telescope}-{altaz_table}"
    enc_df = _read_necstdb_table(raw, enc_table_name)
    alt_df = _read_necstdb_table(raw, alt_table_name)

    t_enc_use, enc_info = _pick_time_axis(
        enc_df,
        mode=encoder_time_mode,
        time_col=time_col,
        recorded_time_col=recorded_time_col,
        label="encoder",
    )
    enc_df = enc_df.copy()
    enc_df["_t_use"] = t_enc_use
    t_enc, enc = _dedup_mean_time(enc_df, "_t_use", ["lon", "lat"])

    t_alt_use, alt_info = _pick_time_axis(
        alt_df,
        mode=altaz_time_mode,
        time_col=time_col,
        recorded_time_col=recorded_time_col,
        label="altaz",
    )
    alt_df = alt_df.copy()
    alt_df["_t_use"] = t_alt_use
    t_alt, alt = _dedup_mean_time(alt_df, "_t_use", ["dlon", "dlat"])

    az_enc_t = _finite_interp(t_enc, enc["lon"], t_spec, circular_deg=True)
    el_enc_t = _finite_interp(t_enc, enc["lat"], t_spec)
    dlon_t = _finite_interp(t_alt, alt["dlon"], t_spec)
    dlat_t = _finite_interp(t_alt, alt["dlat"], t_spec)

    azel_mode = normalize_azel_correction_apply(azel_correction_apply, telescope=telescope)
    az_true, el_true = apply_azel_correction(
        az_enc_t,
        el_enc_t,
        dlon_t,
        dlat_t,
        mode=azel_mode,
    )

    df = pd.DataFrame(
        {
            "t_unix": t_spec,
            "tp": p_spec,
            "az_true": az_true,
            "el_true": el_true,
            "az_enc": az_enc_t,
            "el_enc": el_enc_t,
            "dlon": dlon_t,
            "dlat": dlat_t,
            "position": positions,
            "id": id_values,
        },
        index=pd.to_datetime(t_spec, unit="s", errors="coerce"),
    )
    df.index.name = "timestamp"
    df = df[
        np.isfinite(df["t_unix"].to_numpy(float))
        & np.isfinite(df["tp"].to_numpy(float))
        & np.isfinite(df["el_true"].to_numpy(float))
    ].copy()

    info = {
        "loader": "necstdb-only",
        "spectral_table": table,
        "spectral_data_col": data_col,
        "spectral_time_col": spectral_time_col,
        "id_col": id_col_used,
        "encoder_table": enc_table_name,
        "altaz_table": alt_table_name,
        "encoder_time": enc_info,
        "altaz_time": alt_info,
        "azel_correction_apply_requested": azel_correction_apply,
        "azel_correction_apply": azel_mode,
        "azel_correction_meaning": {
            "subtract": "Az_true=Az_enc-dlon, El_true=El_enc-dlat",
            "add": "Az_true=Az_enc+dlon, El_true=El_enc+dlat",
            "none": "Az_true=Az_enc, El_true=El_enc",
        }[azel_mode],
    }
    return df, info


# =============================================================================
# Data grouping and phot assignment
# =============================================================================

def _make_hot_sky_masks(
    df: pd.DataFrame,
    *,
    hot_tags: Sequence[str] = ("HOT",),
    sky_tags: Sequence[str] = ("SKY", "OFF"),
) -> Tuple[np.ndarray, np.ndarray]:
    pos = np.asarray([_decode_label(v) for v in df["position"].to_numpy()], dtype=str)
    hot = np.zeros(pos.size, dtype=bool)
    sky = np.zeros(pos.size, dtype=bool)
    for tag in hot_tags:
        hot |= np.char.find(pos, str(tag).upper()) >= 0
    for tag in sky_tags:
        sky |= np.char.find(pos, str(tag).upper()) >= 0
    return hot, sky


def _run_slices(mask: np.ndarray) -> List[slice]:
    m = np.asarray(mask, dtype=bool)
    if m.size == 0:
        return []
    starts: List[int] = []
    stops: List[int] = []
    in_run = False
    start = 0
    for i, val in enumerate(m):
        if val and not in_run:
            start = i
            in_run = True
        if in_run and ((not val) or i == m.size - 1):
            stop = i if not val else i + 1
            starts.append(start)
            stops.append(stop)
            in_run = False
    return [slice(a, b) for a, b in zip(starts, stops)]


def build_hot_blocks(
    df: pd.DataFrame,
    hot_mask: np.ndarray,
    *,
    center: str = "median",
    hot_block_gap_sec: float = 10.0,
) -> pd.DataFrame:
    d = df.loc[np.asarray(hot_mask, bool), ["t_unix", "tp"]].copy().sort_values("t_unix")
    d = d[np.isfinite(d["t_unix"].to_numpy(float)) & np.isfinite(d["tp"].to_numpy(float))]
    if d.empty:
        raise RuntimeError("No HOT data found. Check position labels or hot_tags.")

    t = d["t_unix"].to_numpy(float)
    p = d["tp"].to_numpy(float)
    cut = np.flatnonzero(np.diff(t) > float(hot_block_gap_sec)) + 1
    edges = np.concatenate(([0], cut, [t.size]))
    rows: List[Dict[str, Any]] = []
    for i, (a, b) in enumerate(zip(edges[:-1], edges[1:])):
        pv, ps, pse, n = _mean_sem_or_madsem(p[a:b], center=center)
        rows.append(
            {
                "hot_block": i,
                "t_mid": float(np.nanmedian(t[a:b])),
                "t_start": float(np.nanmin(t[a:b])),
                "t_stop": float(np.nanmax(t[a:b])),
                "p_hot": pv,
                "p_hot_scatter": ps,
                "p_hot_sem": pse,
                "n_hot": n,
            }
        )
    return pd.DataFrame(rows)


def assign_phot_to_times(t_query: np.ndarray, hot_blocks: pd.DataFrame, *, method: str = "previous") -> Tuple[np.ndarray, np.ndarray, np.ndarray, List[str]]:
    """Assign P_hot and sigma(P_hot) to SKY representative times.

    method
    ------
    previous
        Use the most recent HOT block before the SKY time.  This is closest to the
        current nercst.skydip logic for step/stare observations.  If a SKY point
        precedes all HOT blocks, the first HOT block is used and labeled edge-left.
    nearest
        Use the temporally nearest HOT block.
    interp / linear / edgehold
        Linearly interpolate between adjacent HOT blocks, with edge-hold outside
        the bracketed time range.  This is useful when gain drift between HOT
        measurements is visible, but it is no longer the default for step/stare data.
    """
    t = np.asarray(t_query, dtype=float)
    tb = hot_blocks["t_mid"].to_numpy(float)
    pb = hot_blocks["p_hot"].to_numpy(float)
    sb = hot_blocks["p_hot_sem"].to_numpy(float)
    method = str(method).lower().strip()
    if tb.size == 0:
        return (
            np.full_like(t, np.nan, dtype=float),
            np.full_like(t, np.nan, dtype=float),
            np.full_like(t, np.nan, dtype=float),
            ["none"] * t.size,
        )

    order = np.argsort(tb)
    tb = tb[order]
    pb = pb[order]
    sb = sb[order]
    original_index = np.arange(tb.size)[order]

    if tb.size == 1 or method == "nearest":
        idx = np.abs(t[:, None] - tb[None, :]).argmin(axis=1)
        return pb[idx], sb[idx], tb[idx], [f"nearest:{int(original_index[i])}" for i in idx]

    if method in ("previous", "prev", "last"):
        idx = np.searchsorted(tb, t, side="right") - 1
        idx = np.clip(idx, 0, tb.size - 1)
        labels = []
        for tk, ii in zip(t, idx):
            if tk < tb[0]:
                labels.append(f"edge-left:{int(original_index[ii])}")
            else:
                labels.append(f"previous:{int(original_index[ii])}")
        return pb[idx], sb[idx], tb[idx], labels

    if method not in ("interp", "linear", "edgehold"):
        raise ValueError("hot_assignment must be 'previous', 'nearest', or 'interp'")

    j = np.searchsorted(tb, t, side="left")
    p = np.empty_like(t, dtype=float)
    s = np.empty_like(t, dtype=float)
    t_assigned = np.empty_like(t, dtype=float)
    labels: List[str] = []
    for k, (tk, jk) in enumerate(zip(t, j)):
        if jk <= 0:
            p[k] = pb[0]
            s[k] = sb[0]
            t_assigned[k] = tb[0]
            labels.append(f"edge-left:{int(original_index[0])}")
        elif jk >= tb.size:
            p[k] = pb[-1]
            s[k] = sb[-1]
            t_assigned[k] = tb[-1]
            labels.append(f"edge-right:{int(original_index[-1])}")
        else:
            t0, t1 = tb[jk - 1], tb[jk]
            w = float((tk - t0) / (t1 - t0)) if t1 > t0 else 0.0
            w = float(np.clip(w, 0.0, 1.0))
            p[k] = (1.0 - w) * pb[jk - 1] + w * pb[jk]
            s[k] = math.sqrt(((1.0 - w) * sb[jk - 1]) ** 2 + (w * sb[jk]) ** 2)
            t_assigned[k] = (1.0 - w) * t0 + w * t1
            labels.append(f"interp:{int(original_index[jk - 1])}-{int(original_index[jk])}:w={w:.3f}")
    return p, s, t_assigned, labels


def _aggregate_sky_samples_to_points(
    sky: pd.DataFrame,
    *,
    grouping: str,
    center: str,
    nbins: int,
    min_per_bin: int,
    el_step_bin_deg: float,
) -> pd.DataFrame:
    """Aggregate SKY samples into representative points.

    grouping:
      - "id"       : one point per observation id.  Recommended for the current 7-El Skydip program.
      - "run"      : one point per consecutive SKY/OFF run.
      - "secz_bin" : bin all SKY samples in secZ.
      - "el_bin"   : bin all SKY samples in El.
    """
    grouping = str(grouping).lower().strip()
    sky = sky.copy().sort_values("t_unix").reset_index(drop=True)
    if sky.empty:
        raise RuntimeError("No SKY/OFF samples available for aggregation.")

    rows: List[Dict[str, Any]] = []

    if grouping == "id":
        if "id" not in sky.columns:
            raise RuntimeError("sky_grouping='id' requested, but no id column is available in the SKY samples.")
        id_text = sky["id"].map(_decode_id).to_numpy(object)
        good_id = np.array([str(v).strip() != "" for v in id_text], dtype=bool)
        if int(np.count_nonzero(good_id)) < int(min_per_bin):
            raise RuntimeError("sky_grouping='id' requested, but SKY id values are empty or insufficient.")
        groups = []
        seen: List[str] = []
        for v in id_text[good_id]:
            vv = str(v)
            if vv not in seen:
                seen.append(vv)
        # Sort by the first time each id appears, preserving the actual observing sequence.
        seen.sort(key=lambda vv: float(np.nanmin(sky.loc[id_text == vv, "t_unix"].to_numpy(float))))
        for vv in seen:
            ii = np.flatnonzero(id_text == vv)
            if ii.size >= int(min_per_bin):
                safe = str(vv).replace(";", "_").replace("\n", "_")
                groups.append((ii, ii, f"id:{safe}"))  # type: ignore[assignment]
    elif grouping == "run":
        # The input sky DataFrame contains only SKY samples, so reconstruct runs using
        # the original integer index if present; otherwise use time gaps as a fallback.
        if "_orig_index" in sky.columns:
            idx = sky["_orig_index"].to_numpy(int)
            split = np.flatnonzero(np.diff(idx) > 1) + 1
        else:
            t = sky["t_unix"].to_numpy(float)
            dt = np.diff(t)
            med_dt = float(np.nanmedian(dt)) if dt.size else 0.0
            split = np.flatnonzero(dt > builtins.max(5.0 * med_dt, 1.0)) + 1
        edges = np.concatenate(([0], split, [len(sky)]))
        groups = []
        for a, b in zip(edges[:-1], edges[1:]):
            if b - a >= int(min_per_bin):
                groups.append((a, b, f"run{len(groups)}"))
    elif grouping == "secz_bin":
        x = sky["secZ"].to_numpy(float)
        finite = x[np.isfinite(x)]
        if finite.size < int(min_per_bin):
            raise RuntimeError("Too few finite secZ samples for secz_bin aggregation.")
        lo, hi = float(np.nanmin(finite)), float(np.nanmax(finite))
        if hi <= lo:
            hi = lo + 1.0e-6
        edges_val = np.linspace(lo, hi, int(nbins) + 1)
        groups = []
        for k in range(int(nbins)):
            if k == int(nbins) - 1:
                m = (x >= edges_val[k]) & (x <= edges_val[k + 1])
            else:
                m = (x >= edges_val[k]) & (x < edges_val[k + 1])
            ii = np.flatnonzero(m)
            if ii.size >= int(min_per_bin):
                groups.append((int(ii[0]), int(ii[-1]) + 1, f"secz_bin{k}"))
                # Store exact mask by replacing with subset below.
                groups[-1] = (ii, ii, f"secz_bin{k}")  # type: ignore[assignment]
    elif grouping == "el_bin":
        el = sky["el_true_deg"].to_numpy(float)
        finite = el[np.isfinite(el)]
        if finite.size < int(min_per_bin):
            raise RuntimeError("Too few finite El samples for el_bin aggregation.")
        step = float(el_step_bin_deg)
        if step <= 0:
            unique = np.unique(np.round(finite, 2))
            if unique.size > 1:
                d = np.diff(np.sort(unique))
                d = d[d > 0]
                step = float(np.clip(np.nanmedian(d) / 3.0, 0.02, 0.10)) if d.size else 0.05
            else:
                step = 0.05
        key = np.round(el / step) * step
        groups = []
        for k, val in enumerate(np.sort(np.unique(key[np.isfinite(key)]))):
            ii = np.flatnonzero(key == val)
            if ii.size >= int(min_per_bin):
                groups.append((ii, ii, f"el_bin{k}"))  # type: ignore[assignment]
    else:
        raise ValueError("grouping must be 'id', 'run', 'secz_bin', or 'el_bin'")

    for group_id, g in enumerate(groups):
        a, b, label = g
        if isinstance(a, np.ndarray):
            sub = sky.iloc[a]
        else:
            sub = sky.iloc[int(a) : int(b)]
        if len(sub) < int(min_per_bin):
            continue
        p_sky, p_sky_scatter, p_sky_sem, n_sky = _mean_sem_or_madsem(sub["p_sky_sample"].to_numpy(float), center=center)
        p_hot, p_hot_scatter, p_hot_sem, n_hot_assigned = _mean_sem_or_madsem(sub["p_hot_sample"].to_numpy(float), center=center)
        secz, secz_scatter, secz_sem, _ = _mean_sem_or_madsem(sub["secZ"].to_numpy(float), center=center)
        el, _, _, _ = _mean_sem_or_madsem(sub["el_true_deg"].to_numpy(float), center=center)
        az, _, _, _ = _mean_sem_or_madsem(sub["az_true_deg"].to_numpy(float), center=center)
        az_enc, _, _, _ = _mean_sem_or_madsem(sub["az_enc"].to_numpy(float), center=center) if "az_enc" in sub.columns else (float("nan"), float("nan"), float("nan"), 0)
        el_enc, _, _, _ = _mean_sem_or_madsem(sub["el_enc"].to_numpy(float), center=center) if "el_enc" in sub.columns else (float("nan"), float("nan"), float("nan"), 0)
        dlon, dlon_scatter, _, _ = _mean_sem_or_madsem(sub["dlon"].to_numpy(float), center=center) if "dlon" in sub.columns else (float("nan"), float("nan"), float("nan"), 0)
        dlat, dlat_scatter, _, _ = _mean_sem_or_madsem(sub["dlat"].to_numpy(float), center=center) if "dlat" in sub.columns else (float("nan"), float("nan"), float("nan"), 0)
        rows.append(
            {
                "sky_run": group_id,
                "group_label": label,
                "t_mid": float(np.nanmedian(sub["t_unix"].to_numpy(float))),
                "t_start": float(np.nanmin(sub["t_unix"].to_numpy(float))),
                "t_stop": float(np.nanmax(sub["t_unix"].to_numpy(float))),
                "az_true_deg": az,
                "el_true_deg": el,
                "az_enc_deg": az_enc,
                "el_enc_deg": el_enc,
                "dlon_deg": dlon,
                "dlat_deg": dlat,
                "dlon_scatter_deg": dlon_scatter,
                "dlat_scatter_deg": dlat_scatter,
                "secZ": secz,
                "secZ_scatter": secz_scatter,
                "secZ_sem": secz_sem,
                "p_sky": p_sky,
                "p_sky_scatter": p_sky_scatter,
                "p_sky_sem": p_sky_sem,
                "p_hot": p_hot,
                "p_hot_scatter": p_hot_scatter,
                "p_hot_sem": p_hot_sem,
                "n_sky": n_sky,
                "n_hot_assigned": n_hot_assigned,
                "id_values": ";".join(sorted(set(map(str, sub["id"].to_numpy()))))[:200] if "id" in sub.columns else "",
                "n_unique_id": _nonempty_id_nunique(sub["id"].to_numpy()) if "id" in sub.columns else 0,
                "hot_assignment": ";".join(sorted(set(map(str, sub["hot_assignment"].to_numpy()))))[:200],
            }
        )

    if not rows:
        raise RuntimeError(
            f"No representative SKY points after aggregation. grouping={grouping}, min_per_bin={min_per_bin}"
        )
    pts = pd.DataFrame(rows)
    pts["p_diff"] = pts["p_hot"] - pts["p_sky"]
    return pts


def representative_sky_points(
    df: pd.DataFrame,
    *,
    hot_tags: Sequence[str] = ("HOT",),
    sky_tags: Sequence[str] = ("SKY", "OFF"),
    center: str = "median",
    hot_block_gap_sec: float = 10.0,
    hot_assignment: str = "previous",
    min_sky_samples: int = 1,
    sky_grouping: str = "auto",
    nbins: int = 8,
    min_per_bin: int = 3,
    el_step_bin_deg: float = 0.05,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Build representative sky-dip points.

    Unlike the old nercst.Skydip grouping, this function is optimized for step observations: repeated SKY/OFF samples at each fixed El.
    It can still bin continuous scans with sky_grouping="secz_bin", but that is not the default here.
    """
    d = df.sort_values("t_unix").reset_index(drop=True).copy()
    d["_orig_index"] = np.arange(len(d), dtype=int)
    hot_mask, sky_mask = _make_hot_sky_masks(d, hot_tags=hot_tags, sky_tags=sky_tags)
    hot_blocks = build_hot_blocks(d, hot_mask, center=center, hot_block_gap_sec=hot_block_gap_sec)

    sky_cols = ["_orig_index", "t_unix", "tp", "az_true", "el_true"]
    if "id" in d.columns:
        sky_cols.append("id")
    for extra_col in ("az_enc", "el_enc", "dlon", "dlat"):
        if extra_col in d.columns:
            sky_cols.append(extra_col)
    sky = d.loc[sky_mask, sky_cols].copy()
    if sky.empty:
        raise RuntimeError("No SKY/OFF data found. Check position labels or sky_tags.")
    sky = sky.rename(columns={"tp": "p_sky_sample", "az_true": "az_true_deg", "el_true": "el_true_deg"})
    sky["secZ"] = 1.0 / np.cos(np.deg2rad(90.0 - sky["el_true_deg"].to_numpy(float)))
    p_hot, s_hot, t_hot_assigned, assign_labels = assign_phot_to_times(
        sky["t_unix"].to_numpy(float), hot_blocks, method=hot_assignment
    )
    sky["p_hot_sample"] = p_hot
    sky["p_hot_sample_sem"] = s_hot
    sky["t_hot_assigned"] = t_hot_assigned
    sky["hot_assignment"] = assign_labels

    grouping = str(sky_grouping).lower().strip()
    if grouping == "auto":
        # Prefer explicit observation id when available.  The current Skydip observation
        # program calls self.sky(integ_time, id=el), so id is the most direct El-step key.
        id_unique = 0
        id_spans: List[float] = []
        if "id" in sky.columns:
            ids = sky["id"].map(_decode_id)
            ids_nonempty = ids[ids.astype(str).str.strip() != ""]
            id_unique = int(ids_nonempty.nunique(dropna=True))
            for _id, sub in sky.loc[ids.astype(str).str.strip() != ""].groupby(ids_nonempty):
                if len(sub):
                    id_spans.append(float(np.nanmax(sub["el_true_deg"]) - np.nanmin(sub["el_true_deg"])))
        if id_unique >= 3 and (not id_spans or float(np.nanmax(id_spans)) < 0.3):
            grouping = "id"
        else:
            # If there are several separated SKY runs and each run is almost stationary in El,
            # treat them as step points.  If there is one long SKY run or a large El span within
            # a SKY run, bin continuously in secZ.
            runs = _run_slices(sky_mask)
            el_span_total = float(np.nanmax(sky["el_true_deg"]) - np.nanmin(sky["el_true_deg"]))
            run_spans = []
            for sl in runs:
                sub = d.iloc[sl]
                if len(sub):
                    run_spans.append(float(np.nanmax(sub["el_true"]) - np.nanmin(sub["el_true"])))
            max_run_span = float(np.nanmax(run_spans)) if run_spans else float("nan")
            if len(runs) >= 3 and (np.isfinite(max_run_span) and max_run_span < 0.15):
                grouping = "run"
            elif el_span_total < 0.3:
                grouping = "run"
            else:
                grouping = "el_bin"

    pts = _aggregate_sky_samples_to_points(
        sky,
        grouping=grouping,
        center=center,
        nbins=nbins,
        min_per_bin=min_per_bin if grouping != "run" else min_sky_samples,
        el_step_bin_deg=el_step_bin_deg,
    )
    pts["sky_grouping"] = grouping
    return pts, hot_blocks

# =============================================================================
# Fitting and calibration
# =============================================================================

def add_y_and_uncertainty(
    points: pd.DataFrame,
    *,
    y_mode: str = "logdiff",
    sigma_kind: str = "sem",
    error_floor: float = 0.0,
) -> pd.DataFrame:
    pts = points.copy()
    y_mode = str(y_mode).lower().strip()
    sigma_kind = str(sigma_kind).lower().strip()
    if sigma_kind not in ("sem", "scatter"):
        raise ValueError("sigma_kind must be 'sem' or 'scatter'")

    ph = pts["p_hot"].to_numpy(float)
    ps = pts["p_sky"].to_numpy(float)
    pdiff = ph - ps
    if sigma_kind == "scatter":
        sh = pts.get("p_hot_scatter", pts["p_hot_sem"]).to_numpy(float)
        ss = pts.get("p_sky_scatter", pts["p_sky_sem"]).to_numpy(float)
    else:
        sh = pts["p_hot_sem"].to_numpy(float)
        ss = pts["p_sky_sem"].to_numpy(float)

    y = np.full_like(pdiff, np.nan, dtype=float)
    sy = np.full_like(pdiff, np.nan, dtype=float)

    good = np.isfinite(ph) & np.isfinite(ps) & np.isfinite(pdiff) & (ph > 0) & (pdiff > 0)
    if y_mode == "logdiff":
        y[good] = np.log(pdiff[good])
        sy[good] = np.sqrt(sh[good] ** 2 + ss[good] ** 2) / pdiff[good]
    elif y_mode == "logratio":
        y[good] = np.log(pdiff[good] / ph[good])
        dy_dsky = -1.0 / pdiff[good]
        dy_dhot = 1.0 / pdiff[good] - 1.0 / ph[good]
        sy[good] = np.sqrt((dy_dsky * ss[good]) ** 2 + (dy_dhot * sh[good]) ** 2)
    else:
        raise ValueError("y_mode must be 'logdiff' or 'logratio'")

    sy_from_data = sy.copy()
    if float(error_floor) > 0:
        sy = np.sqrt(sy**2 + float(error_floor) ** 2)

    pts["y"] = y
    pts["sigma_y_from_data"] = sy_from_data
    pts["sigma_y"] = sy
    pts["error_floor"] = float(error_floor)
    pts["y_mode"] = y_mode
    pts["sigma_kind"] = sigma_kind
    return pts


def weighted_linear_fit(
    x: np.ndarray,
    y: np.ndarray,
    sigma_y: np.ndarray,
    *,
    weight_mode: str = "inverse_variance",
) -> Dict[str, Any]:
    """Fit y = intercept - tau*x.  Returns explicit WLS diagnostics."""
    x = np.asarray(x, dtype=float)
    y = np.asarray(y, dtype=float)
    s = np.asarray(sigma_y, dtype=float)
    m = np.isfinite(x) & np.isfinite(y)
    weight_mode = str(weight_mode).lower().strip()
    if weight_mode in ("inverse_variance", "ivar", "weighted"):
        m &= np.isfinite(s) & (s > 0)
    if int(np.sum(m)) < 2:
        raise RuntimeError("Not enough finite points for linear fit")
    x = x[m]
    y = y[m]
    s = s[m]

    A = np.column_stack([np.ones_like(x), -x])  # beta=[intercept, tau]
    if weight_mode in ("none", "uniform", "unweighted"):
        w = np.ones_like(x)
        weighting = "uniform: minimize sum residual^2"
    elif weight_mode in ("inverse_variance", "ivar", "weighted"):
        w = 1.0 / np.square(s)
        weighting = "inverse_variance: minimize sum ((y-model)/sigma_y)^2; effective weight=1/sigma_y^2"
    else:
        raise ValueError("weight_mode must be 'inverse_variance' or 'uniform'")

    sw = np.sqrt(w)
    Aw = A * sw[:, None]
    yw = y * sw
    beta, *_ = np.linalg.lstsq(Aw, yw, rcond=None)
    intercept, tau = float(beta[0]), float(beta[1])
    model = A @ beta
    resid = y - model
    chi2 = float(np.sum(w * resid**2))
    dof = int(x.size - 2)
    red = float(chi2 / dof) if dof > 0 else float("nan")

    try:
        cov0 = np.linalg.inv(A.T @ (w[:, None] * A))
        if weight_mode in ("none", "uniform", "unweighted") and dof > 0:
            cov = cov0 * (chi2 / dof)
        else:
            cov = cov0 * (red if np.isfinite(red) and red > 1.0 else 1.0)
        intercept_sigma = float(math.sqrt(builtins.max(cov[0, 0], 0.0)))
        tau_sigma = float(math.sqrt(builtins.max(cov[1, 1], 0.0)))
    except Exception:
        intercept_sigma = float("nan")
        tau_sigma = float("nan")

    return {
        "intercept": intercept,
        "tau": tau,
        "intercept_sigma": intercept_sigma,
        "tau_sigma": tau_sigma,
        "model": model,
        "residual": resid,
        "mask": m,
        "chi2": chi2,
        "dof": dof,
        "reduced_chi2": red,
        "weighting": weighting,
        "x_fit": x,
        "y_fit": y,
        "sigma_y_fit": s,
    }


def _fit_tau_only(x: np.ndarray, y: np.ndarray, s: np.ndarray, weight_mode: str) -> float:
    return float(weighted_linear_fit(x, y, s, weight_mode=weight_mode)["tau"])


def leave_one_out_tau(x: np.ndarray, y: np.ndarray, s: np.ndarray, weight_mode: str) -> np.ndarray:
    x = np.asarray(x, float)
    y = np.asarray(y, float)
    s = np.asarray(s, float)
    m = np.isfinite(x) & np.isfinite(y)
    if weight_mode not in ("none", "uniform", "unweighted"):
        m &= np.isfinite(s) & (s > 0)
    x, y, s = x[m], y[m], s[m]
    if x.size <= 2:
        return np.array([], dtype=float)
    out = []
    for i in range(x.size):
        keep = np.ones(x.size, dtype=bool)
        keep[i] = False
        try:
            out.append(_fit_tau_only(x[keep], y[keep], s[keep], weight_mode))
        except Exception:
            out.append(np.nan)
    return np.asarray(out, dtype=float)


def bootstrap_tau(
    x: np.ndarray,
    y: np.ndarray,
    s: np.ndarray,
    weight_mode: str,
    *,
    n_bootstrap: int = 500,
    seed: int = 12345,
) -> np.ndarray:
    x = np.asarray(x, float)
    y = np.asarray(y, float)
    s = np.asarray(s, float)
    m = np.isfinite(x) & np.isfinite(y)
    if weight_mode not in ("none", "uniform", "unweighted"):
        m &= np.isfinite(s) & (s > 0)
    x, y, s = x[m], y[m], s[m]
    if x.size < 3 or int(n_bootstrap) <= 0:
        return np.array([], dtype=float)
    rng = np.random.default_rng(seed)
    out = []
    for _ in range(int(n_bootstrap)):
        idx = rng.integers(0, x.size, size=x.size)
        try:
            out.append(_fit_tau_only(x[idx], y[idx], s[idx], weight_mode))
        except Exception:
            out.append(np.nan)
    return np.asarray(out, dtype=float)


def atmospheric_sky_temperature(secZ: np.ndarray, tau: float, *, tatm_K: float, tcmb_K: float) -> np.ndarray:
    """Forward-sky brightness temperature before spillover mixing.

    T_sky_fwd(secZ) = T_atm * (1 - exp(-tau*secZ)) + T_cmb * exp(-tau*secZ)
    """
    x = np.asarray(secZ, dtype=float)
    trans = np.exp(-float(tau) * x)
    return float(tatm_K) * (1.0 - trans) + float(tcmb_K) * trans


def input_sky_temperature(
    secZ: np.ndarray,
    tau: float,
    *,
    tatm_K: float,
    tcmb_K: float,
    forward_efficiency: float = 1.0,
    spillover_temperature_K: Optional[float] = None,
    thot_K: float = 293.0,
) -> Tuple[np.ndarray, np.ndarray, float]:
    """Return receiver-input sky temperature including forward efficiency.

    Definitions
    -----------
    eta_f = forward_efficiency.
    T_spill = spillover_temperature_K if specified, otherwise thot_K.

    T_sky_fwd(secZ) = T_atm * (1 - exp(-tau*secZ)) + T_cmb * exp(-tau*secZ)
    T_sky_in(secZ)  = eta_f * T_sky_fwd(secZ) + (1 - eta_f) * T_spill

    This is the temperature that should be paired with Trx in the Y-factor
    equation for SKY observations.  The HOT load is assumed to fill the input
    with T_hot and therefore does not get multiplied by eta_f.
    """
    eta = float(forward_efficiency)
    if not np.isfinite(eta) or eta <= 0.0 or eta > 1.0:
        raise ValueError("forward_efficiency must be in the range 0 < eta_f <= 1")
    tspill = float(thot_K if spillover_temperature_K is None else spillover_temperature_K)
    if not np.isfinite(tspill) or tspill <= 0.0:
        raise ValueError("spillover_temperature_K must be positive when specified")
    tfwd = atmospheric_sky_temperature(secZ, tau, tatm_K=tatm_K, tcmb_K=tcmb_K)
    tin = eta * tfwd + (1.0 - eta) * tspill
    return tin, tfwd, tspill


def add_tsys_trx(
    points: pd.DataFrame,
    *,
    tau: float,
    thot_K: float = 293.0,
    tatm_K: float = 293.0,
    tcmb_K: float = 2.725,
    forward_efficiency: float = 1.0,
    spillover_temperature_K: Optional[float] = None,
) -> Tuple[pd.DataFrame, Dict[str, float]]:
    """Estimate gain, Trx, and system temperatures from HOT/SKY powers.

    Receiver-input model:
        P_hot = G * (T_hot + T_rx)
        P_sky = G * (T_rx + T_sky_in(secZ))

    where
        T_sky_fwd(secZ) = T_atm*(1-exp(-tau*secZ)) + T_cmb*exp(-tau*secZ)
        T_sky_in(secZ)  = eta_f*T_sky_fwd(secZ) + (1-eta_f)*T_spill

    The receiver-input/on-sky system temperature is
        Tsys_input(secZ) = T_rx + T_sky_in(secZ)

    For observing sensitivity on an above-atmosphere temperature scale, the
    source signal is attenuated by atmospheric transmission exp(-tau*secZ).
    If the forward-efficiency correction is also included (T_A* style scale),
    the corrected system temperature is
        Tsys_tastar(secZ) = Tsys_input(secZ) / (eta_f * exp(-tau*secZ))

    v8 naming convention
    --------------------
    Clear names:
    - Tsys_receiver_input_*: receiver-input / uncorrected on-sky system temperature.
    - Tsys_opacity_only_*: divided by atmospheric transmission exp(-tau*secZ) only.
    - Tsys_sensitivity_*: divided by eta_f*exp(-tau*secZ); use this for sensitivity/radiometer equation.

    Backward-compatible aliases from v7 are retained:
    - Tsys_input_* == Tsys_receiver_input_*
    - Tsys_opacity_corrected_* == Tsys_opacity_only_*
    - Tsys_tastar_* == Tsys_sensitivity_*
    - Tsys_zenith_K == Tsys_sensitivity_zenith_K
    """
    pts = points.copy()
    eta = float(forward_efficiency)
    secz = pts["secZ"].to_numpy(float)
    ph = pts["p_hot"].to_numpy(float)
    ps = pts["p_sky"].to_numpy(float)
    tsky_in, tsky_fwd, tspill = input_sky_temperature(
        secz,
        tau,
        tatm_K=tatm_K,
        tcmb_K=tcmb_K,
        forward_efficiency=eta,
        spillover_temperature_K=spillover_temperature_K,
        thot_K=thot_K,
    )
    denom = float(thot_K) - tsky_in
    good = np.isfinite(ph) & np.isfinite(ps) & np.isfinite(denom) & (ph > ps) & (denom > 0)
    gain = np.full_like(ph, np.nan, dtype=float)
    trx = np.full_like(ph, np.nan, dtype=float)
    tsys_input = np.full_like(ph, np.nan, dtype=float)
    gain[good] = (ph[good] - ps[good]) / denom[good]
    trx[good] = ph[good] / gain[good] - float(thot_K)
    tsys_input[good] = trx[good] + tsky_in[good]

    transmission = np.exp(-float(tau) * secz)
    opacity_factor = transmission
    tastar_factor = eta * transmission
    tsys_opacity_corrected = np.full_like(ph, np.nan, dtype=float)
    tsys_tastar = np.full_like(ph, np.nan, dtype=float)
    ok_opacity = np.isfinite(tsys_input) & np.isfinite(opacity_factor) & (opacity_factor > 0)
    ok_tastar = np.isfinite(tsys_input) & np.isfinite(tastar_factor) & (tastar_factor > 0)
    tsys_opacity_corrected[ok_opacity] = tsys_input[ok_opacity] / opacity_factor[ok_opacity]
    tsys_tastar[ok_tastar] = tsys_input[ok_tastar] / tastar_factor[ok_tastar]

    # Keep Tsky_K as the receiver-input sky temperature for backward-compatible column names.
    pts["Tsky_forward_K"] = tsky_fwd
    pts["Tsky_input_K"] = tsky_in
    pts["Tsky_K"] = tsky_in
    pts["Tspill_K"] = tspill
    pts["forward_efficiency"] = eta
    pts["atmospheric_transmission"] = transmission
    pts["gain_K_to_power"] = gain
    pts["Trx_K"] = trx
    # Clear v8 names.
    pts["Tsys_receiver_input_K"] = tsys_input
    pts["Tsys_opacity_only_K"] = tsys_opacity_corrected
    pts["Tsys_sensitivity_K"] = tsys_tastar

    # Backward-compatible v7 names.
    pts["Tsys_input_K"] = tsys_input
    pts["Tsys_opacity_corrected_K"] = tsys_opacity_corrected
    pts["Tsys_tastar_K"] = tsys_tastar
    pts["Tsys_sky_K"] = tsys_tastar

    trx_med = float(np.nanmedian(trx))
    gain_med = float(np.nanmedian(gain))
    gain_scatter = _mad_sigma(gain)
    gain_frac = float(gain_scatter / abs(gain_med)) if np.isfinite(gain_med) and gain_med != 0 else float("nan")
    trx_scatter = _mad_sigma(trx)
    trx_frac = float(trx_scatter / abs(trx_med)) if np.isfinite(trx_med) and trx_med != 0 else float("nan")

    tsky_zen_in, tsky_zen_fwd, _ = input_sky_temperature(
        np.array([1.0]),
        tau,
        tatm_K=tatm_K,
        tcmb_K=tcmb_K,
        forward_efficiency=eta,
        spillover_temperature_K=tspill,
        thot_K=thot_K,
    )
    trans_zen = float(np.exp(-float(tau)))
    tsys_input_zen = float(trx_med + tsky_zen_in[0]) if np.isfinite(trx_med) else float("nan")
    tsys_opacity_zen = tsys_input_zen / trans_zen if np.isfinite(tsys_input_zen) and trans_zen > 0 else float("nan")
    tsys_tastar_zen = tsys_input_zen / (eta * trans_zen) if np.isfinite(tsys_input_zen) and eta > 0 and trans_zen > 0 else float("nan")

    out = {
        "Trx_K": trx_med,
        "Trx_robust_scatter_K": trx_scatter,
        "Trx_robust_frac_scatter": trx_frac,
        "Tsky_zenith_K": float(tsky_zen_in[0]),
        "Tsky_forward_zenith_K": float(tsky_zen_fwd[0]),
        # Clear v8 names.
        "Tsys_receiver_input_zenith_K": tsys_input_zen,
        "Tsys_opacity_only_zenith_K": tsys_opacity_zen,
        "Tsys_sensitivity_zenith_K": tsys_tastar_zen,
        # Backward-compatible v7 names.
        "Tsys_input_zenith_K": tsys_input_zen,
        "Tsys_opacity_corrected_zenith_K": tsys_opacity_zen,
        "Tsys_tastar_zenith_K": tsys_tastar_zen,
        "Tsys_zenith_K": tsys_tastar_zen,
        "gain_median": gain_med,
        "gain_robust_frac_scatter": gain_frac,
        "forward_efficiency": eta,
        "spillover_temperature_K": tspill,
    }
    return pts, out

def quality_flags(
    *,
    tau: float,
    tau_sigma: float,
    reduced_chi2: float,
    loo_tau_std: float,
    residual_mad_sigma: float,
    n_fit: int,
    Trx_K: float,
    Tsys_zenith_K: float,
    gain_robust_frac_scatter: float,
    Trx_robust_frac_scatter: float = float("nan"),
) -> Tuple[str, List[str]]:
    flags: List[str] = []
    if n_fit < 4:
        flags.append("too_few_fit_points")
    if not np.isfinite(tau) or tau <= 0:
        flags.append("non_positive_tau")
    if np.isfinite(tau) and np.isfinite(tau_sigma) and tau > 0 and tau_sigma / tau > 0.25:
        flags.append("large_tau_uncertainty")
    if np.isfinite(reduced_chi2) and reduced_chi2 > 5.0:
        flags.append("large_reduced_chi2")
    if np.isfinite(loo_tau_std) and np.isfinite(tau) and abs(tau) > 0 and loo_tau_std / abs(tau) > 0.25:
        flags.append("large_leave_one_out_tau_scatter")
    if np.isfinite(residual_mad_sigma) and residual_mad_sigma > 0.15:
        flags.append("large_residual_scatter")
    if (not np.isfinite(Trx_K)) or Trx_K <= 0:
        flags.append("invalid_Trx")
    if (not np.isfinite(Tsys_zenith_K)) or Tsys_zenith_K <= 0:
        flags.append("invalid_Tsys_zenith")
    if np.isfinite(gain_robust_frac_scatter) and gain_robust_frac_scatter > 0.10:
        flags.append("large_gain_scatter")
    if np.isfinite(Trx_robust_frac_scatter) and Trx_robust_frac_scatter > 0.25:
        flags.append("large_Trx_scatter")

    if not flags:
        return "GOOD", []
    if any(f in flags for f in ["too_few_fit_points", "non_positive_tau", "invalid_Trx", "invalid_Tsys_zenith"]):
        return "BAD", flags
    return "WARN", flags


# =============================================================================
# Main analysis APIs
# =============================================================================

def analyze_skydip_dataframe(
    df: pd.DataFrame,
    *,
    board: str = "board",
    label: Optional[str] = None,
    y_mode: str = "logdiff",
    sigma_kind: str = "sem",
    weight_mode: str = "inverse_variance",
    secz_fit_range: Tuple[float, float] = (1.0, 3.2),
    error_floor: float = 0.02,
    hot_tags: Sequence[str] = ("HOT",),
    sky_tags: Sequence[str] = ("SKY", "OFF"),
    center: str = "median",
    min_sky_samples: int = 1,
    hot_block_gap_sec: float = 10.0,
    hot_assignment: str = "previous",
    sky_grouping: str = "auto",
    nbins: int = 8,
    min_per_bin: int = 3,
    el_step_bin_deg: float = 0.05,
    thot_K: float = 293.0,
    tatm_K: float = 293.0,
    tcmb_K: float = 2.725,
    forward_efficiency: float = 1.0,
    spillover_temperature_K: Optional[float] = None,
    n_bootstrap: int = 500,
    bootstrap_seed: int = 12345,
    time_info: Optional[Dict[str, Any]] = None,
) -> SkydipResult:
    label = board if label is None else str(label)
    pts, hot_blocks = representative_sky_points(
        df,
        hot_tags=hot_tags,
        sky_tags=sky_tags,
        center=center,
        hot_block_gap_sec=hot_block_gap_sec,
        hot_assignment=hot_assignment,
        min_sky_samples=min_sky_samples,
        sky_grouping=sky_grouping,
        nbins=nbins,
        min_per_bin=min_per_bin,
        el_step_bin_deg=el_step_bin_deg,
    )
    pts = add_y_and_uncertainty(pts, y_mode=y_mode, sigma_kind=sigma_kind, error_floor=error_floor)

    xmin, xmax = secz_fit_range
    fit_mask = (
        np.isfinite(pts["secZ"].to_numpy(float))
        & np.isfinite(pts["y"].to_numpy(float))
        & (pts["secZ"].to_numpy(float) >= float(xmin))
        & (pts["secZ"].to_numpy(float) <= float(xmax))
    )
    if weight_mode not in ("none", "uniform", "unweighted"):
        fit_mask &= np.isfinite(pts["sigma_y"].to_numpy(float)) & (pts["sigma_y"].to_numpy(float) > 0)
    pts["fit_used"] = fit_mask
    if int(np.sum(fit_mask)) < 2:
        raise RuntimeError(f"{board}: not enough fit points after filtering; n={int(np.sum(fit_mask))}")

    fit = weighted_linear_fit(
        pts.loc[fit_mask, "secZ"].to_numpy(float),
        pts.loc[fit_mask, "y"].to_numpy(float),
        pts.loc[fit_mask, "sigma_y"].to_numpy(float),
        weight_mode=weight_mode,
    )
    tau = float(fit["tau"])
    intercept = float(fit["intercept"])
    pts.loc[fit_mask, "model_y"] = fit["model"]
    pts.loc[fit_mask, "residual_y"] = fit["residual"]

    loo = leave_one_out_tau(
        pts.loc[fit_mask, "secZ"].to_numpy(float),
        pts.loc[fit_mask, "y"].to_numpy(float),
        pts.loc[fit_mask, "sigma_y"].to_numpy(float),
        weight_mode=weight_mode,
    )
    loo_std = float(np.nanstd(loo, ddof=1)) if np.sum(np.isfinite(loo)) > 1 else float("nan")
    loo_max = float(np.nanmax(np.abs(loo - tau))) if np.any(np.isfinite(loo)) else float("nan")

    boot = bootstrap_tau(
        pts.loc[fit_mask, "secZ"].to_numpy(float),
        pts.loc[fit_mask, "y"].to_numpy(float),
        pts.loc[fit_mask, "sigma_y"].to_numpy(float),
        weight_mode=weight_mode,
        n_bootstrap=n_bootstrap,
        seed=bootstrap_seed,
    )
    boot_good = boot[np.isfinite(boot)]
    if boot_good.size:
        boot_med = float(np.nanmedian(boot_good))
        boot_p16 = float(np.nanpercentile(boot_good, 16))
        boot_p84 = float(np.nanpercentile(boot_good, 84))
    else:
        boot_med = boot_p16 = boot_p84 = float("nan")

    residual = pts.loc[fit_mask, "residual_y"].to_numpy(float)
    residual_rms = float(np.sqrt(np.nanmean(residual**2))) if residual.size else float("nan")
    residual_mad = _mad_sigma(residual)

    pts, cal = add_tsys_trx(
        pts,
        tau=tau,
        thot_K=thot_K,
        tatm_K=tatm_K,
        tcmb_K=tcmb_K,
        forward_efficiency=forward_efficiency,
        spillover_temperature_K=spillover_temperature_K,
    )

    q, qflags = quality_flags(
        tau=tau,
        tau_sigma=float(fit["tau_sigma"]),
        reduced_chi2=float(fit["reduced_chi2"]),
        loo_tau_std=loo_std,
        residual_mad_sigma=residual_mad,
        n_fit=int(np.sum(fit_mask)),
        Trx_K=float(cal["Trx_K"]),
        Tsys_zenith_K=float(cal["Tsys_zenith_K"]),
        gain_robust_frac_scatter=float(cal["gain_robust_frac_scatter"]),
        Trx_robust_frac_scatter=float(cal["Trx_robust_frac_scatter"]),
    )

    # simple mode diagnostics: enough for Jupyter output
    mode_diag = {
        "n_input": float(len(df)),
        "n_hot_samples": float(np.sum(_make_hot_sky_masks(df, hot_tags=hot_tags, sky_tags=sky_tags)[0])),
        "n_sky_samples": float(np.sum(_make_hot_sky_masks(df, hot_tags=hot_tags, sky_tags=sky_tags)[1])),
        "n_hot_blocks": float(len(hot_blocks)),
        "n_sky_points": float(len(pts)),
        "n_unique_sky_id": float(_nonempty_id_nunique(df.loc[_make_hot_sky_masks(df, hot_tags=hot_tags, sky_tags=sky_tags)[1], "id"].to_numpy())) if "id" in df.columns else 0.0,
    }

    return SkydipResult(
        board=board,
        label=label,
        y_mode=y_mode,
        tau=tau,
        tau_sigma=float(fit["tau_sigma"]),
        intercept=intercept,
        intercept_sigma=float(fit["intercept_sigma"]),
        n_fit=int(np.sum(fit_mask)),
        dof=int(fit["dof"]),
        chi2=float(fit["chi2"]),
        reduced_chi2=float(fit["reduced_chi2"]),
        residual_rms=residual_rms,
        residual_mad_sigma=residual_mad,
        loo_tau_std=loo_std,
        loo_tau_max_abs_diff=loo_max,
        bootstrap_tau_median=boot_med,
        bootstrap_tau_p16=boot_p16,
        bootstrap_tau_p84=boot_p84,
        Tsys_receiver_input_zenith_K=float(cal["Tsys_receiver_input_zenith_K"]),
        Tsys_sensitivity_zenith_K=float(cal["Tsys_sensitivity_zenith_K"]),
        Tsys_opacity_only_zenith_K=float(cal["Tsys_opacity_only_zenith_K"]),
        Tsys_zenith_K=float(cal["Tsys_zenith_K"]),
        Tsys_input_zenith_K=float(cal["Tsys_input_zenith_K"]),
        Tsys_opacity_corrected_zenith_K=float(cal["Tsys_opacity_corrected_zenith_K"]),
        Tsys_tastar_zenith_K=float(cal["Tsys_tastar_zenith_K"]),
        Trx_K=float(cal["Trx_K"]),
        Trx_robust_scatter_K=float(cal["Trx_robust_scatter_K"]),
        Trx_robust_frac_scatter=float(cal["Trx_robust_frac_scatter"]),
        Tsky_zenith_K=float(cal["Tsky_zenith_K"]),
        Tsky_forward_zenith_K=float(cal["Tsky_forward_zenith_K"]),
        forward_efficiency=float(cal["forward_efficiency"]),
        spillover_temperature_K=float(cal["spillover_temperature_K"]),
        gain_median=float(cal["gain_median"]),
        gain_robust_frac_scatter=float(cal["gain_robust_frac_scatter"]),
        quality=q,
        quality_flags=qflags,
        weighting=str(fit["weighting"]),
        hot_assignment=str(hot_assignment),
        points=pts,
        hot_blocks=hot_blocks,
        mode_diag=mode_diag,
        time_info={} if time_info is None else time_info,
    )


def analyze_skydip_board(
    rawdata_path: Union[str, pathlib.Path],
    board: str,
    *,
    label: Optional[str] = None,
    telescope: str = "OMU1P85M",
    y_mode: str = "logdiff",
    include_ch: Optional[Union[str, Sequence[Range]]] = None,
    bad_ch: Optional[Union[str, Sequence[Range]]] = None,
    spectral_data_col: Optional[str] = None,
    spectral_time_col: Optional[str] = None,
    id_col: Optional[str] = "id",
    azel_correction_apply: str = "auto",
    **kwargs: Any,
) -> SkydipResult:
    df, info = load_skydip_dataframe_necstdb(
        rawdata_path,
        board,
        telescope=telescope,
        include_ch=include_ch,
        bad_ch=bad_ch,
        spectral_data_col=spectral_data_col,
        spectral_time_col=spectral_time_col,
        id_col=id_col,
        azel_correction_apply=azel_correction_apply,
    )
    return analyze_skydip_dataframe(df, board=board, label=label, y_mode=y_mode, time_info=info, **kwargs)


def _auto_grid(n: int) -> Tuple[int, int]:
    if n <= 1:
        return 1, 1
    nx = int(math.ceil(math.sqrt(n)))
    ny = int(math.ceil(n / nx))
    return ny, nx


def plot_skydip_results(
    results: Mapping[str, SkydipResult],
    *,
    figsize_per_panel: Tuple[float, float] = (5.2, 4.4),
    show_table: bool = True,
    suptitle: Optional[str] = None,
    save_png: Optional[Union[str, pathlib.Path]] = None,
    save_pdf: Optional[Union[str, pathlib.Path]] = None,
) -> Tuple[plt.Figure, np.ndarray]:
    items = list(results.items())
    ny, nx = _auto_grid(len(items))
    fig, axes = plt.subplots(ny, nx, figsize=(figsize_per_panel[0] * nx, figsize_per_panel[1] * ny), squeeze=False)
    flat = axes.ravel()
    for ax in flat[len(items) :]:
        ax.axis("off")

    for ax, (board, res) in zip(flat, items):
        pts = res.points.copy()
        m = np.isfinite(pts["secZ"].to_numpy(float)) & np.isfinite(pts["y"].to_numpy(float))
        fit = pts["fit_used"].to_numpy(bool) if "fit_used" in pts.columns else m
        x = pts.loc[m, "secZ"].to_numpy(float)
        y = pts.loc[m, "y"].to_numpy(float)
        sy = pts.loc[m, "sigma_y"].to_numpy(float)

        # Error bars: these are propagated sigma_y values used for the fit.
        ax.errorbar(x, y, yerr=sy, fmt="o", capsize=4, markerfacecolor="white", markeredgecolor="black")
        if np.any(fit):
            xf = pts.loc[fit, "secZ"].to_numpy(float)
            xx = np.linspace(float(np.nanmin(xf)), float(np.nanmax(xf)), 200)
            yy = res.intercept - res.tau * xx
            ax.plot(xx, yy, "-")
        ax.set_title(f"{board}: {res.label}", fontsize=16)
        ax.set_xlabel("sec Z", fontsize=14)
        ylabel = "log(hot-sky)" if res.y_mode == "logdiff" else "log((hot-sky)/hot)"
        ax.set_ylabel(ylabel, fontsize=14)
        ax.grid(True)

        text = (
            rf"$\tau={res.tau:.3f}\pm{res.tau_sigma:.3f}$" + "\n"
            + f"Tsys0={res.Tsys_sensitivity_zenith_K:.1f} K\n"
            + f"Trx={res.Trx_K:.1f}±{res.Trx_robust_scatter_K:.1f} K, eta_f={res.forward_efficiency:.3f}\n"
            + f"chi2r={res.reduced_chi2:.2f}, LOO σtau={res.loo_tau_std:.3f}\n"
            + f"{res.quality}"
        )
        ax.text(0.04, 0.06, text, transform=ax.transAxes, fontsize=12, va="bottom", ha="left")

    if suptitle is not None:
        fig.suptitle(suptitle, fontsize=16)
    fig.tight_layout()

    if show_table:
        # Show table below the figure in notebook environments as a normal pandas repr.
        pass

    if save_png is not None:
        pathlib.Path(save_png).parent.mkdir(parents=True, exist_ok=True)
        fig.savefig(save_png, dpi=160)
    if save_pdf is not None:
        pathlib.Path(save_pdf).parent.mkdir(parents=True, exist_ok=True)
        fig.savefig(save_pdf)
    return fig, axes


def analyze_skydip_boards(
    rawdata_path: Union[str, pathlib.Path],
    boards: Union[Sequence[str], Mapping[str, str]],
    *,
    telescope: str = "OMU1P85M",
    y_mode: str = "logdiff",
    include_ch: Optional[Union[str, Sequence[Range]]] = None,
    bad_ch: Optional[Union[str, Sequence[Range]]] = None,
    save_prefix: Optional[Union[str, pathlib.Path]] = None,
    **kwargs: Any,
) -> Tuple[Dict[str, SkydipResult], plt.Figure, np.ndarray]:
    """Analyze multiple boards and make a nercst-like multi-panel plot.

    Parameters
    ----------
    boards:
        Either ["xffts-board3", ...] or {"xffts-board3": "B3USB", ...}.
    save_prefix:
        If given, saves '<prefix>.png', '<prefix>.pdf', '<prefix>_summary.csv',
        and '<prefix>_<board>_points.csv'.
    """
    if isinstance(boards, Mapping):
        board_items = list(boards.items())
    else:
        board_items = [(b, b) for b in boards]

    results: Dict[str, SkydipResult] = {}
    for board, label in board_items:
        res = analyze_skydip_board(
            rawdata_path,
            board,
            label=label,
            telescope=telescope,
            y_mode=y_mode,
            include_ch=include_ch,
            bad_ch=bad_ch,
            **kwargs,
        )
        results[board] = res

    png = pdf = None
    if save_prefix is not None:
        prefix = pathlib.Path(save_prefix)
        png = prefix.with_suffix(".png")
        pdf = prefix.with_suffix(".pdf")
    fig, axes = plot_skydip_results(results, suptitle=pathlib.Path(rawdata_path).name, save_png=png, save_pdf=pdf)

    if save_prefix is not None:
        prefix = pathlib.Path(save_prefix)
        summary_dataframe(results).to_csv(prefix.parent / f"{prefix.name}_summary.csv", index=False)
        for board, res in results.items():
            safe = board.replace("/", "_").replace(" ", "_")
            res.points.to_csv(prefix.parent / f"{prefix.name}_{safe}_points.csv", index=False)
            res.hot_blocks.to_csv(prefix.parent / f"{prefix.name}_{safe}_hot_blocks.csv", index=False)

    return results, fig, axes


def summary_dataframe(results: Mapping[str, SkydipResult]) -> pd.DataFrame:
    rows = []
    for board, res in results.items():
        rows.append(
            {
                "board": board,
                "label": res.label,
                "quality": res.quality,
                "tau": res.tau,
                "tau_sigma": res.tau_sigma,
                "tau_boot_p16": res.bootstrap_tau_p16,
                "tau_boot_p84": res.bootstrap_tau_p84,
                "Tsys0_K": res.Tsys_sensitivity_zenith_K,
                "Tsys_sensitivity_zenith_K": res.Tsys_sensitivity_zenith_K,
                "Tsys_receiver_input_zenith_K": res.Tsys_receiver_input_zenith_K,
                "Tsys_opacity_only_zenith_K": res.Tsys_opacity_only_zenith_K,
                # Backward-compatible v7 aliases.
                "Tsys_zenith_K": res.Tsys_zenith_K,
                "Tsys_input_zenith_K": res.Tsys_input_zenith_K,
                "Tsys_opacity_corrected_zenith_K": res.Tsys_opacity_corrected_zenith_K,
                "Tsys_tastar_zenith_K": res.Tsys_tastar_zenith_K,
                "Trx_K": res.Trx_K,
                "Trx_robust_scatter_K": res.Trx_robust_scatter_K,
                "Trx_robust_frac_scatter": res.Trx_robust_frac_scatter,
                "Tsky_zenith_K": res.Tsky_zenith_K,
                "Tsky_forward_zenith_K": res.Tsky_forward_zenith_K,
                "forward_efficiency": res.forward_efficiency,
                "spillover_temperature_K": res.spillover_temperature_K,
                "reduced_chi2": res.reduced_chi2,
                "residual_rms": res.residual_rms,
                "residual_mad_sigma": res.residual_mad_sigma,
                "loo_tau_std": res.loo_tau_std,
                "loo_tau_max_abs_diff": res.loo_tau_max_abs_diff,
                "gain_robust_frac_scatter": res.gain_robust_frac_scatter,
                "n_fit": res.n_fit,
                "dof": res.dof,
                "y_mode": res.y_mode,
                "weighting": res.weighting,
                "hot_assignment": res.hot_assignment,
                "sky_grouping": str(res.points["sky_grouping"].iloc[0]) if "sky_grouping" in res.points.columns and len(res.points) else "",
                "n_sky_points": len(res.points),
                "n_unique_id_points": int(res.points["n_unique_id"].sum()) if "n_unique_id" in res.points.columns else 0,
                "id_col": (res.time_info or {}).get("id_col", ""),
                "azel_correction_apply": (res.time_info or {}).get("azel_correction_apply", ""),
                "azel_correction_meaning": (res.time_info or {}).get("azel_correction_meaning", ""),
                "flags": ";".join(res.quality_flags),
            }
        )
    return pd.DataFrame(rows)


# =============================================================================
# Compatibility helper: make a DataFrame from arrays for testing or custom loaders
# =============================================================================

def make_dataframe_from_arrays(
    t_unix: Sequence[Number],
    tp: Sequence[Number],
    el_true_deg: Sequence[Number],
    position: Sequence[Any],
    az_true_deg: Optional[Sequence[Number]] = None,
    id_values: Optional[Sequence[Any]] = None,
) -> pd.DataFrame:
    t = np.asarray(t_unix, dtype=float)
    if az_true_deg is None:
        az = np.full_like(t, np.nan, dtype=float)
    else:
        az = np.asarray(az_true_deg, dtype=float)
    if id_values is None:
        ids = np.array([""] * len(t), dtype=object)
    else:
        ids = np.asarray([_decode_id(v) for v in id_values], dtype=object)
    df = pd.DataFrame(
        {
            "t_unix": t,
            "tp": np.asarray(tp, dtype=float),
            "az_true": az,
            "el_true": np.asarray(el_true_deg, dtype=float),
            "position": np.asarray([_decode_label(v) for v in position], dtype=object),
            "id": ids,
        },
        index=pd.to_datetime(t, unit="s", errors="coerce"),
    )
    df.index.name = "timestamp"
    return df.sort_values("t_unix")


# =============================================================================
# CLI, kept secondary to the Jupyter API
# =============================================================================

def _main(argv: Optional[Sequence[str]] = None) -> None:
    import argparse

    parser = argparse.ArgumentParser(description="NECST v4 sky-dip analysis, nercst-free loader")
    parser.add_argument("rawdata")
    parser.add_argument("--boards", nargs="+", required=True, help="e.g. xffts-board3 xffts-board4")
    parser.add_argument("--labels", nargs="*", default=None, help="optional labels matching --boards")
    parser.add_argument("--telescope", default="OMU1P85M")
    parser.add_argument("--y-mode", default="logdiff", choices=["logdiff", "logratio"])
    parser.add_argument("--bad-ch", default="")
    parser.add_argument("--include-ch", default="")
    parser.add_argument("--save-prefix", default="skydip")
    parser.add_argument("--thot-K", type=float, default=293.0)
    parser.add_argument("--tatm-K", type=float, default=293.0)
    parser.add_argument("--forward-efficiency", type=float, default=1.0, help="Forward efficiency eta_f used for Trx/Tsys calibration, 0 < eta_f <= 1")
    parser.add_argument("--spillover-temperature-K", type=float, default=None, help="Ground/spillover temperature. Default: thot_K")
    parser.add_argument("--weight-mode", default="inverse_variance", choices=["inverse_variance", "uniform"])
    parser.add_argument("--sky-grouping", default="auto", choices=["id", "run", "el_bin", "secz_bin", "auto"], help="auto uses id when available; otherwise falls back to run/el_bin")
    parser.add_argument("--hot-assignment", default="previous", choices=["previous", "nearest", "interp"], help="step/stare default is previous, matching nercst-like behavior")
    parser.add_argument("--id-col", default="id", help="Spectral table column used for sky_grouping='id'. Use empty string to disable.")
    parser.add_argument(
        "--azel-correction-apply",
        default="auto",
        choices=["auto", "subtract", "minus", "add", "plus", "none"],
        help="Az/El correction sign. auto: OMU/1.85m=subtract, NANTEN2=add.",
    )
    args = parser.parse_args(argv)

    if args.labels and len(args.labels) == len(args.boards):
        boards: Union[Sequence[str], Mapping[str, str]] = dict(zip(args.boards, args.labels))
    else:
        boards = args.boards
    results, fig, axes = analyze_skydip_boards(
        args.rawdata,
        boards,
        telescope=args.telescope,
        y_mode=args.y_mode,
        bad_ch=args.bad_ch,
        include_ch=args.include_ch,
        save_prefix=args.save_prefix,
        thot_K=args.thot_K,
        tatm_K=args.tatm_K,
        forward_efficiency=args.forward_efficiency,
        spillover_temperature_K=args.spillover_temperature_K,
        weight_mode=args.weight_mode,
        sky_grouping=args.sky_grouping,
        hot_assignment=args.hot_assignment,
        azel_correction_apply=args.azel_correction_apply,
        id_col=(args.id_col if str(args.id_col).strip() else None),
    )
    print(summary_dataframe(results).to_string(index=False))


if __name__ == "__main__":
    _main()
