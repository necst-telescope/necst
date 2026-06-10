"""Operator-facing observation CSV log for the NECST console.

This log is intentionally separate from the internal operator JSONL log.  The
CSV is compact and append-only for observers and later analysis; the JSON sidecar
keeps session/environment metadata out of each CSV row.
"""

from __future__ import annotations

import csv
import json
import os
import re
import socket
import threading
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path, PureWindowsPath
from typing import Any, Dict, List, Mapping, Optional, Tuple


CSV_HEADER = [
    "utc_iso",
    "enc_az_deg",
    "enc_el_deg",
    "comment",
    "mode",
    "event",
    "action_or_obsfile",
    "result",
    "user",
    "record_dir",
    "session_id",
    "row_id",
    "temp_C",
    "humidity_pct",
    "pressure_hPa",
    "weather_source",
]

SCHEMA_VERSION = "1.0"
DEFAULT_PREFIX = "obslog"
DEFAULT_OBSERVER = "User"


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_iso(dt: Optional[datetime] = None) -> str:
    dt = dt or utc_now()
    return dt.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def utc_stamp(dt: Optional[datetime] = None) -> str:
    dt = dt or utc_now()
    return dt.strftime("%Y%m%dT%H%M%SZ")


def sanitize_prefix(prefix: Any) -> str:
    text = str(prefix or "").strip() or DEFAULT_PREFIX
    text = re.sub(r"[^A-Za-z0-9_.-]+", "_", text)
    text = text.strip("._-") or DEFAULT_PREFIX
    return text[:64]


def sanitize_observer(observer: Any) -> str:
    text = str(observer or "").strip() or DEFAULT_OBSERVER
    return text[:128]


def _split_env_paths(value: str) -> List[str]:
    out: List[str] = []
    for chunk in str(value or "").replace(";", os.pathsep).split(os.pathsep):
        chunk = chunk.strip()
        if chunk:
            out.append(chunk)
    return out


def _candidate_record_roots(extra_roots: Optional[List[os.PathLike[str] | str]] = None) -> List[Path]:
    raw: List[str] = []
    for value in extra_roots or []:
        if str(value or "").strip():
            raw.append(str(value))
    for env_name in (
        "NECST_CONSOLE_PC_RECORD_ROOT",
        "NECST_CONSOLE_RECORD_ROOT",
        "NECST_RECORD_ROOT",
        "NECST_DATA_ROOT",
        "NECST_CONSOLE_DATA_ROOT",
    ):
        raw.extend(_split_env_paths(os.environ.get(env_name, "")))
    roots: List[Path] = []
    seen: set[str] = set()
    for item in raw:
        try:
            path = Path(item).expanduser()
        except Exception:
            continue
        key = str(path)
        if key in seen:
            continue
        seen.add(key)
        roots.append(path)
    return roots


def _ensure_writable_dir(path: Path) -> Tuple[bool, str]:
    try:
        path.mkdir(parents=True, exist_ok=True)
        probe = path / ".necst_obslog_write_probe"
        with probe.open("a", encoding="utf-8") as fh:
            fh.write("")
            fh.flush()
            try:
                os.fsync(fh.fileno())
            except Exception:
                pass
        try:
            probe.unlink()
        except Exception:
            pass
        return True, ""
    except Exception as exc:
        return False, str(exc)


def resolve_log_dir(
    configured_dir: Optional[os.PathLike[str] | str] = None,
    *,
    site_configured_dir: Optional[os.PathLike[str] | str] = None,
    record_roots: Optional[List[os.PathLike[str] | str]] = None,
) -> Tuple[Path, str, Optional[str], List[str]]:
    """Return ``(directory, source, record_root, warnings)``.

    Directory selection order:
    1. explicit argument, usually CLI ``--obslog-dir``;
    2. ``NECST_OBSLOG_DIR``;
    3. site TOML ``[console.observation_log].directory``;
    4. ``<record root>/obslogs`` from explicit/site/env record-root candidates;
    5. ``~/.necst/observation_logs``.
    """

    warnings: List[str] = []
    explicit_arg = str(configured_dir or "").strip()
    if explicit_arg:
        path = Path(explicit_arg).expanduser()
        ok, reason = _ensure_writable_dir(path)
        if ok:
            return path, "configured_dir", None, warnings
        warnings.append(f"configured observation log directory is not writable: {path}: {reason}")

    explicit_env = str(os.environ.get("NECST_OBSLOG_DIR", "")).strip()
    if explicit_env:
        path = Path(explicit_env).expanduser()
        ok, reason = _ensure_writable_dir(path)
        if ok:
            return path, "NECST_OBSLOG_DIR", None, warnings
        warnings.append(f"NECST_OBSLOG_DIR is not writable: {path}: {reason}")

    site_dir = str(site_configured_dir or "").strip()
    if site_dir:
        path = Path(site_dir).expanduser()
        ok, reason = _ensure_writable_dir(path)
        if ok:
            return path, "site_config_observation_log_dir", None, warnings
        warnings.append(f"site TOML observation log directory is not writable: {path}: {reason}")

    for root in _candidate_record_roots(record_roots):
        path = root / "obslogs"
        ok, reason = _ensure_writable_dir(path)
        if ok:
            return path, "record_root_obslogs", str(root), warnings
        warnings.append(f"record-root obslogs is not writable: {path}: {reason}")

    fallback = Path.home() / ".necst" / "observation_logs"
    ok, reason = _ensure_writable_dir(fallback)
    if ok:
        return fallback, "home_fallback", None, warnings
    warnings.append(f"fallback observation log directory is not writable: {fallback}: {reason}")
    # Let the caller fail explicitly when it tries to open the path.
    return fallback, "home_fallback_unwritable", None, warnings


def _metadata_path(csv_path: Path) -> Path:
    return csv_path.with_suffix(csv_path.suffix + ".meta.json")


def _finite_number(value: Any) -> Optional[float]:
    try:
        number = float(value)
    except Exception:
        return None
    if not (number == number) or number in (float("inf"), float("-inf")):
        return None
    return number


def _format_float(value: Any, ndigits: int) -> str:
    number = _finite_number(value)
    if number is None:
        return ""
    return f"{number:.{ndigits}f}"




def _last_path_component(value: Any) -> str:
    """Return only the final path component for compact observer-facing CSV fields.

    Paths in the operator console are container paths, but this also tolerates
    Windows-style separators so copied paths still become a compact basename.
    Non-path values are returned unchanged.
    """

    text = str(value or "").strip()
    if not text:
        return ""
    trimmed = text.rstrip("/\\")
    if not trimmed:
        return text
    if "\\" in trimmed and "/" not in trimmed:
        name = PureWindowsPath(trimmed).name
    else:
        name = Path(trimmed).name
    return name or trimmed


def obsfile_name_for_csv(value: Any) -> str:
    """Return the observation-instruction filename to store in action_or_obsfile."""

    return _last_path_component(value)


def record_dir_name_for_csv(value: Any) -> str:
    """Return the final record-directory component to store in record_dir."""

    return _last_path_component(value)


def _first_present(payload: Mapping[str, Any], keys: Tuple[str, ...]) -> Any:
    for key in keys:
        if key in payload and payload.get(key) is not None:
            return payload.get(key)
    return None


def _temperature_c_from_weather(payload: Mapping[str, Any], *, inside: bool = False) -> str:
    if inside:
        c_value = _first_present(payload, ("in_temperature_c", "in_temperature_degC"))
        k_value = _first_present(payload, ("in_temperature_k", "in_temperature"))
    else:
        c_value = _first_present(payload, ("temperature_c", "temperature_degC"))
        k_value = _first_present(payload, ("temperature_k", "temperature"))
    if c_value is not None:
        return _format_float(c_value, 2)
    number = _finite_number(k_value)
    if number is None:
        return ""
    # NECST WeatherMsg.temperature is Kelvin.  Values in a physical Celsius
    # range may still arrive under temperature_c/temperature_degC and are
    # handled above; bare temperature/temperature_k are treated as Kelvin.
    return _format_float(number - 273.15, 2)


def _humidity_pct_from_weather(payload: Mapping[str, Any], *, inside: bool = False) -> str:
    if inside:
        value = _first_present(payload, ("in_humidity_percent", "in_humidity_pct", "in_humidity"))
    else:
        value = _first_present(payload, ("humidity_percent", "humidity_pct", "humidity"))
    number = _finite_number(value)
    if number is None:
        return ""
    # NECST WeatherMsg.humidity is a fraction in the range 0--1, while some
    # logs or simulators may already use percent.  Normalize fractions to %.
    if -1.0 <= number <= 1.0:
        number *= 100.0
    return _format_float(number, 2)


def _pressure_hpa_from_weather(payload: Mapping[str, Any], *, inside: bool = False) -> str:
    if inside:
        value = _first_present(payload, ("in_pressure_hpa", "in_pressure_hPa", "in_pressure"))
        if value is not None:
            return _format_float(value, 2)
    return _format_float(_first_present(payload, ("pressure_hpa", "pressure_hPa", "pressure")), 2)


def _weather_from_payload(payload: Mapping[str, Any], *, source: str, inside: bool = False) -> Tuple[str, str, str, str]:
    temp = _temperature_c_from_weather(payload, inside=inside)
    humidity = _humidity_pct_from_weather(payload, inside=inside)
    pressure = _pressure_hpa_from_weather(payload, inside=inside)
    if temp or humidity or pressure:
        return temp, humidity, pressure, source
    return "", "", "", ""


def select_weather(weather: Mapping[str, Any]) -> Tuple[str, str, str, str]:
    """Return temp_C, humidity_pct, pressure_hPa, source.

    Prefer outside weather.  If outside data are unavailable, fall back to inside
    weather.  NECST WeatherMsg humidity is normally a 0--1 fraction, but the CSV
    column is explicitly percent, so fractions are normalized here.
    """

    if not isinstance(weather, Mapping):
        return "", "", "", ""

    for source in ("out", "outside", "outdoor"):
        payload = weather.get(source)
        if isinstance(payload, Mapping):
            result = _weather_from_payload(payload, source="out", inside=False)
            if result[3]:
                return result

    # Some NECST live payloads carry inside values as in_* fields in the same
    # outside weather message.  Try those before falling back to explicit
    # inside/indoor payloads.
    for source in ("out", "outside", "outdoor"):
        payload = weather.get(source)
        if isinstance(payload, Mapping):
            result = _weather_from_payload(payload, source="in", inside=True)
            if result[3]:
                return result

    for source in ("in", "inside", "indoor"):
        payload = weather.get(source)
        if isinstance(payload, Mapping):
            # Inside payloads may use either generic keys (temperature,
            # humidity, pressure) or explicit in_* keys depending on the source.
            result = _weather_from_payload(payload, source="in", inside=False)
            if result[3]:
                return result
            result = _weather_from_payload(payload, source="in", inside=True)
            if result[3]:
                return result

    # Last resort: tolerate a flat weather mapping.
    result = _weather_from_payload(weather, source="out", inside=False)
    if result[3]:
        return result
    result = _weather_from_payload(weather, source="in", inside=True)
    if result[3]:
        return result
    return "", "", "", ""


@dataclass
class ObservationLogManager:
    """Append-only observer CSV log with a JSON metadata sidecar."""

    log_dir: Path
    log_dir_source: str
    record_root: Optional[str] = None
    prefix: str = DEFAULT_PREFIX
    observer: str = DEFAULT_OBSERVER
    telescope: str = "NECST"
    directory_warnings: List[str] = field(default_factory=list)
    session_id: str = field(init=False)
    created_utc: str = field(init=False)
    csv_path: Path = field(init=False)
    meta_path: Path = field(init=False)
    row_id: int = field(default=0, init=False)
    closed_utc: Optional[str] = field(default=None, init=False)
    last_rows: List[Dict[str, Any]] = field(default_factory=list, init=False)
    last_error: str = field(default="", init=False)
    _lock: threading.RLock = field(default_factory=threading.RLock, init=False, repr=False)
    _pending_open_warning: str = field(default="", init=False, repr=False)
    _fh: Any = field(default=None, init=False, repr=False)
    _writer: Optional[csv.writer] = field(default=None, init=False, repr=False)

    def __post_init__(self) -> None:
        self.prefix = sanitize_prefix(self.prefix)
        self.observer = sanitize_observer(self.observer)
        created = utc_now()
        self.created_utc = utc_iso(created)
        self.session_id = f"{utc_stamp(created)}-{uuid.uuid4().hex[:4]}"
        self.open_new(prefix=self.prefix, observer=self.observer, initial=True, created=created)

    @classmethod
    def create(
        cls,
        *,
        configured_dir: Optional[os.PathLike[str] | str] = None,
        site_configured_dir: Optional[os.PathLike[str] | str] = None,
        record_roots: Optional[List[os.PathLike[str] | str]] = None,
        prefix: Any = DEFAULT_PREFIX,
        observer: Any = DEFAULT_OBSERVER,
        telescope: str = "NECST",
    ) -> "ObservationLogManager":
        log_dir, source, record_root, warnings = resolve_log_dir(
            configured_dir,
            site_configured_dir=site_configured_dir,
            record_roots=record_roots,
        )
        return cls(
            log_dir=log_dir,
            log_dir_source=source,
            record_root=record_root,
            prefix=sanitize_prefix(prefix),
            observer=sanitize_observer(observer),
            telescope=telescope,
            directory_warnings=warnings,
        )

    def _default_path(self, *, prefix: str, created: Optional[datetime] = None) -> Path:
        stamp = utc_stamp(created)
        base = self.log_dir / f"{sanitize_prefix(prefix)}_{stamp}.csv"
        if not base.exists():
            return base
        for idx in range(2, 1000):
            candidate = self.log_dir / f"{sanitize_prefix(prefix)}_{stamp}_{idx}.csv"
            if not candidate.exists():
                return candidate
        return self.log_dir / f"{sanitize_prefix(prefix)}_{stamp}_{uuid.uuid4().hex[:6]}.csv"

    def _write_meta(self) -> None:
        payload = {
            "schema_version": SCHEMA_VERSION,
            "session_id": self.session_id,
            "created_utc": self.created_utc,
            "closed_utc": self.closed_utc,
            "observer": self.observer,
            "prefix": self.prefix,
            "csv_path": str(self.csv_path),
            "meta_path": str(self.meta_path),
            "log_dir": str(self.log_dir),
            "log_dir_source": self.log_dir_source,
            "record_root": self.record_root,
            "telescope": self.telescope,
            "hostname": socket.gethostname(),
            "pid": os.getpid(),
            "schema_columns": list(CSV_HEADER),
            "notes": "CSV is append-only; web reload does not change the file. Paths are inside the console container.",
        }
        tmp = self.meta_path.with_suffix(self.meta_path.suffix + ".tmp")
        tmp.parent.mkdir(parents=True, exist_ok=True)
        with tmp.open("w", encoding="utf-8") as fh:
            json.dump(payload, fh, ensure_ascii=False, indent=2, sort_keys=True)
            fh.write("\n")
            fh.flush()
            try:
                os.fsync(fh.fileno())
            except Exception:
                pass
        tmp.replace(self.meta_path)

    def _open_path(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        existed = path.exists()
        size = path.stat().st_size if existed else 0
        self.csv_path = path
        self.meta_path = _metadata_path(path)
        self.last_rows = []
        self._pending_open_warning = ""
        self._fh = path.open("a", encoding="utf-8", newline="")
        self._writer = csv.writer(self._fh)
        if size == 0:
            self.row_id = 0
            self._writer.writerow(CSV_HEADER)
            self._sync()
        elif existed:
            try:
                with path.open("r", encoding="utf-8", newline="") as check_fh:
                    reader = csv.reader(check_fh)
                    header = next(reader, [])
                    row_count = 0
                    max_existing_row_id = 0
                    recent: List[Dict[str, Any]] = []
                    for row in reader:
                        if not row or not any(str(cell).strip() for cell in row):
                            continue
                        row_count += 1
                        if header == CSV_HEADER:
                            values = list(row)[: len(CSV_HEADER)]
                            if len(values) < len(CSV_HEADER):
                                values.extend([""] * (len(CSV_HEADER) - len(values)))
                            row_dict = dict(zip(CSV_HEADER, values))
                            try:
                                max_existing_row_id = max(max_existing_row_id, int(str(row_dict.get("row_id") or "0")))
                            except Exception:
                                pass
                            recent.append(row_dict)
                            if len(recent) > 20:
                                del recent[0]
                self.row_id = max_existing_row_id if header == CSV_HEADER and max_existing_row_id > 0 else row_count
                if header != CSV_HEADER:
                    self._pending_open_warning = "CSV header differs from the current observation-log schema; appending anyway"
                    self.last_error = self._pending_open_warning
                else:
                    self.last_rows = list(reversed(recent))
            except Exception as exc:
                self.row_id = 0
                self._pending_open_warning = f"failed to inspect existing CSV header; appending anyway: {exc}"
                self.last_error = self._pending_open_warning
        self._write_meta()

    def _resolve_user_csv_path(self, path_text: Any) -> Path:
        raw = str(path_text or "").strip()
        if not raw:
            raise ValueError("log file path is empty")
        path = Path(raw).expanduser()
        if not path.is_absolute():
            # Relative paths are intended to stay under the active log
            # directory.  Reject traversal explicitly so that a friendly UI
            # string such as "../other.csv" cannot escape the log directory.
            if any(part == ".." for part in path.parts):
                raise ValueError("relative observation log path must not contain '..'")
            path = self.log_dir / path
        if raw.endswith(("/", "\\")):
            raise ValueError("observation log path must include a CSV filename")
        if path.suffix == "":
            path = path.with_suffix(".csv")
        elif path.suffix.lower() != ".csv":
            raise ValueError("observation log path must end with .csv")
        return path

    def _preflight_csv_path(self, path: Path) -> None:
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("a", encoding="utf-8", newline="") as fh:
                fh.flush()
                try:
                    os.fsync(fh.fileno())
                except Exception:
                    pass
        except Exception as exc:
            raise OSError(f"observation log path is not writable: {path}: {exc}") from exc

    def _record_switch_failure(self, path: Path, exc: BaseException) -> None:
        try:
            self.write_event(
                {},
                comment=str(exc),
                mode="Console",
                event="log_open_failed",
                action_or_obsfile=str(path),
                result="failed",
            )
        except Exception:
            self.last_error = str(exc)

    def _restore_previous_after_switch_failure(self, previous: Optional[Path], target: Path, exc: BaseException) -> None:
        try:
            if self._fh is not None:
                self._fh.close()
        except Exception:
            pass
        self._fh = None
        self._writer = None
        if previous is None:
            self.last_error = str(exc)
            return
        try:
            self.closed_utc = None
            self._open_path(previous)
            self.write_event(
                {},
                comment=str(exc),
                mode="Console",
                event="log_open_failed",
                action_or_obsfile=str(target),
                result="failed",
            )
        except Exception as restore_exc:
            self.last_error = f"failed to open {target}: {exc}; failed to restore {previous}: {restore_exc}"

    def _sync(self) -> None:
        if self._fh is None:
            return
        self._fh.flush()
        os.fsync(self._fh.fileno())

    def update_observer(self, observer: Any) -> bool:
        with self._lock:
            return self._update_observer_unlocked(observer)

    def _update_observer_unlocked(self, observer: Any) -> bool:
        new_observer = sanitize_observer(observer)
        if new_observer == self.observer:
            return False
        old = self.observer
        self.observer = new_observer
        self.write_event(
            {},
            comment="",
            mode="Console",
            event="observer_changed",
            action_or_obsfile=f"{old} -> {new_observer}",
            result="success",
        )
        self._write_meta()
        return True

    def open_new(
        self,
        *,
        prefix: Any = None,
        observer: Any = None,
        initial: bool = False,
        created: Optional[datetime] = None,
    ) -> None:
        with self._lock:
            self._open_new_unlocked(prefix=prefix, observer=observer, initial=initial, created=created)

    def _open_new_unlocked(
        self,
        *,
        prefix: Any = None,
        observer: Any = None,
        initial: bool = False,
        created: Optional[datetime] = None,
    ) -> None:
        old_observer = self.observer
        old_prefix = self.prefix
        if observer is not None:
            self.observer = sanitize_observer(observer)
        if prefix is not None:
            self.prefix = sanitize_prefix(prefix)
        created = created or utc_now()
        path = self._default_path(prefix=self.prefix, created=created)
        previous = getattr(self, "csv_path", None)
        try:
            self._preflight_csv_path(path)
        except Exception as exc:
            self.observer = old_observer
            self.prefix = old_prefix
            self._record_switch_failure(path, exc)
            raise
        if not initial and self._fh is not None:
            self.write_event(
                {},
                mode="Console",
                event="log_closed",
                action_or_obsfile=str(path),
                result="success",
            )
            self.close(write_log_closed=False)
        self.closed_utc = None
        self.created_utc = utc_iso(created)
        try:
            self._open_path(path)
        except Exception as exc:
            self._restore_previous_after_switch_failure(previous, path, exc)
            raise
        self.write_event(
            {},
            mode="Console",
            event="log_opened" if not initial else "console_start",
            action_or_obsfile=(f"from {previous}" if previous else str(path)),
            result="success",
        )

    def open_existing(self, path_text: Any, *, observer: Any = None) -> None:
        with self._lock:
            self._open_existing_unlocked(path_text, observer=observer)

    def _open_existing_unlocked(self, path_text: Any, *, observer: Any = None) -> None:
        try:
            path = self._resolve_user_csv_path(path_text)
        except Exception as exc:
            fallback = self.log_dir / str(path_text or "")
            self._record_switch_failure(fallback, exc)
            raise
        old_observer = self.observer
        if observer is not None:
            self.observer = sanitize_observer(observer)
        previous = getattr(self, "csv_path", None)
        try:
            self._preflight_csv_path(path)
        except Exception as exc:
            self.observer = old_observer
            self._record_switch_failure(path, exc)
            raise
        if self._fh is not None:
            self.write_event({}, mode="Console", event="log_closed", action_or_obsfile=str(path), result="success")
            self.close(write_log_closed=False)
        self.closed_utc = None
        self.created_utc = utc_iso()
        # Keep the original session id for this console process, even when the
        # file changes.  This lets merged CSVs identify the console session.
        try:
            self._open_path(path)
        except Exception as exc:
            self._restore_previous_after_switch_failure(previous, path, exc)
            raise
        self.write_event(
            {},
            mode="Console",
            event="log_opened",
            action_or_obsfile=(f"from {previous}" if previous else str(path)),
            result="success",
        )

    def write_event(
        self,
        context: Mapping[str, Any],
        *,
        comment: Any = "",
        mode: Any = "Console",
        event: Any = "event",
        action_or_obsfile: Any = "",
        result: Any = "unknown",
        record_dir: Optional[Any] = None,
    ) -> bool:
        with self._lock:
            return self._write_event_unlocked(
                context,
                comment=comment,
                mode=mode,
                event=event,
                action_or_obsfile=action_or_obsfile,
                result=result,
                record_dir=record_dir,
            )

    def _write_event_unlocked(
        self,
        context: Mapping[str, Any],
        *,
        comment: Any = "",
        mode: Any = "Console",
        event: Any = "event",
        action_or_obsfile: Any = "",
        result: Any = "unknown",
        record_dir: Optional[Any] = None,
    ) -> bool:
        if self._writer is None:
            return False
        try:
            weather = context.get("weather") if isinstance(context, Mapping) else {}
            temp, humidity, pressure, weather_source = select_weather(weather if isinstance(weather, Mapping) else {})
            enc_az = _format_float(context.get("enc_az_deg"), 4) if isinstance(context, Mapping) else ""
            enc_el = _format_float(context.get("enc_el_deg"), 4) if isinstance(context, Mapping) else ""
            chosen_record_dir = record_dir
            if chosen_record_dir in (None, "") and isinstance(context, Mapping):
                chosen_record_dir = context.get("record_dir")
            chosen_record_dir = record_dir_name_for_csv(chosen_record_dir)
            next_row_id = self.row_id + 1
            row = [
                utc_iso(),
                enc_az,
                enc_el,
                str(comment or ""),
                str(mode or ""),
                str(event or ""),
                str(action_or_obsfile or ""),
                str(result or ""),
                self.observer,
                str(chosen_record_dir or ""),
                self.session_id,
                next_row_id,
                temp,
                humidity,
                pressure,
                weather_source,
            ]
            self._writer.writerow(row)
            self._fh.flush()
            fsync_error = ""
            try:
                os.fsync(self._fh.fileno())
            except Exception as exc:
                fsync_error = str(exc)
            self.row_id = next_row_id
            row_dict = dict(zip(CSV_HEADER, row))
            self.last_rows.insert(0, row_dict)
            del self.last_rows[20:]
            self.last_error = fsync_error or self._pending_open_warning
            return not bool(fsync_error)
        except Exception as exc:
            self.last_error = str(exc)
            return False

    def close(self, *, write_log_closed: bool = True) -> None:
        with self._lock:
            self._close_unlocked(write_log_closed=write_log_closed)

    def _close_unlocked(self, *, write_log_closed: bool = True) -> None:
        if self._fh is None:
            return
        if write_log_closed:
            self.write_event({}, mode="Console", event="log_closed", action_or_obsfile="console server stopping", result="success")
        self.closed_utc = utc_iso()
        try:
            self._write_meta()
        except Exception as exc:
            self.last_error = str(exc)
        try:
            self._sync()
        finally:
            try:
                self._fh.close()
            finally:
                self._fh = None
                self._writer = None

    def status(self) -> Dict[str, Any]:
        with self._lock:
            return self._status_unlocked()

    def _status_unlocked(self) -> Dict[str, Any]:
        return {
            "ok": self._writer is not None,
            "writing": self._writer is not None,
            "csv_path": str(self.csv_path),
            "csv_name": self.csv_path.name,
            "meta_path": str(self.meta_path),
            "meta_name": self.meta_path.name,
            "created_utc": self.created_utc,
            "closed_utc": self.closed_utc,
            "log_dir": str(self.log_dir),
            "log_dir_source": self.log_dir_source,
            "record_root": self.record_root,
            "prefix": self.prefix,
            "observer": self.observer,
            "session_id": self.session_id,
            "row_id": self.row_id,
            "schema_version": SCHEMA_VERSION,
            "columns": list(CSV_HEADER),
            "last_rows": list(self.last_rows),
            "last_error": self.last_error,
            "directory_warnings": list(self.directory_warnings),
            "tooltips": {
                "directory": (
                    "Log directory selection:\n"
                    "1. --obslog-dir, if set.\n"
                    "2. NECST_OBSLOG_DIR, if set.\n"
                    "3. [console.observation_log].directory in site TOML, if set.\n"
                    "4. <record root>/obslogs, if available.\n"
                    "3. ~/.necst/observation_logs as fallback.\n"
                    "This path is inside the console container. Use a bind-mounted path if you need the log on the host."
                ),
                "file": "A new log file is created when the console server starts. Reloading the web page does not change the log file.",
                "switch": "Close the current log file and continue writing to a new or selected CSV file. This does not rename the existing file.",
            },
        }
