"""Runtime support for absolute-modulo azimuth encoder unwrapping."""

from __future__ import annotations

import json
import math
import os
import queue
import tempfile
import threading
import time
from pathlib import Path
from typing import Any, Mapping, Optional, Tuple

from neclib.coordinates.angle_unwrap import (
    AbsoluteModuloUnwrapConfig,
    AbsoluteModuloUnwrapper,
    AmbiguousBranchError,
    AngleUnwrapError,
    BranchJumpError,
    NoValidBranchError,
    RawAngleRangeError,
    normalize_absolute_modulo_raw,
)
from necst_msgs.msg import AntennaAzUnwrapStatus

from ... import config


def _cfg_get(obj: Any, key: str, default: Any = None) -> Any:
    if obj is None:
        return default
    try:
        if isinstance(obj, Mapping) and key in obj:
            return obj[key]
    except Exception:
        pass
    try:
        value = getattr(obj, key)
    except Exception:
        return default
    return default if value is None else value


def _to_float(
    value: Any, unit: str = "deg", default: Optional[float] = None
) -> Optional[float]:
    if value is None:
        return default
    if hasattr(value, "to_value"):
        try:
            return float(value.to_value(unit))
        except Exception:
            try:
                return float(value.to_value(""))
            except Exception:
                return default
    try:
        return float(value)
    except Exception:
        return default


def _to_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on", "enabled"}
    return bool(value)


def _expand_path(value: Any) -> Optional[Path]:
    if value in (None, ""):
        return None
    return Path(os.path.expandvars(os.path.expanduser(str(value))))


def _drive_range_deg() -> Tuple[float, float]:
    drive = getattr(config, "antenna_drive", None)
    crit = _cfg_get(drive, "critical_limit_az", None)
    if crit is None:
        return 0.0, 360.0
    try:
        lo, hi = crit
    except Exception:
        return 0.0, 360.0
    return float(_to_float(lo, "deg", 0.0)), float(_to_float(hi, "deg", 360.0))




def _az_unwrap_az_section() -> Optional[Any]:
    """Return the configured Az unwrap subsection, if any exists.

    A missing ``antenna_encoder_unwrap`` section means this telescope is not
    configured for absolute-modulo Az unwrap and must keep the legacy behavior.
    A present section with ``enabled=false`` means an absolute-modulo encoder is
    configured but the software unwrap is disabled; in that state commands must
    not exceed the raw absolute encoder range.
    """

    root = getattr(config, "antenna_encoder_unwrap", None)
    if root is None:
        return None
    az = _cfg_get(root, "az", None)
    return root if az is None else az


def load_unwrap_disabled_mount_az_limit() -> Optional[dict]:
    """Return raw-Az command limits when absolute unwrap is configured off.

    When an absolute-modulo Az encoder is configured but
    ``antenna_encoder_unwrap.az.enabled`` is false, the control system has no
    branch information.  In that state Az commands must be confined to the raw
    absolute encoder range (normally 0..360 deg).  If no unwrap section exists,
    return ``None`` so NANTEN2/incremental-encoder configurations are unchanged.
    """

    az = _az_unwrap_az_section()
    if az is None:
        return None
    enabled = _to_bool(_cfg_get(az, "enabled", False), False)
    if enabled:
        return None
    mode = str(_cfg_get(az, "mode", "absolute_modulo") or "").strip().lower()
    if mode not in {"absolute_modulo", "absolute-modulo"}:
        return None
    raw_min = float(
        _to_float(
            _cfg_get(az, "raw_min", _cfg_get(az, "raw_min_deg", 0.0)),
            "deg",
            0.0,
        )
    )
    raw_max = float(
        _to_float(
            _cfg_get(az, "raw_max", _cfg_get(az, "raw_max_deg", 360.0)),
            "deg",
            360.0,
        )
    )
    if not (math.isfinite(raw_min) and math.isfinite(raw_max)) or raw_min >= raw_max:
        raise ValueError(
            "antenna_encoder_unwrap.az raw_min/raw_max are invalid: "
            f"raw_min={raw_min!r}, raw_max={raw_max!r}"
        )
    return {
        "enabled": False,
        "mode": mode,
        "raw_min_deg": raw_min,
        "raw_max_deg": raw_max,
    }


def _flatten_degree_values(value: Any) -> list[float]:
    if hasattr(value, "to_value"):
        value = value.to_value("deg")
    if hasattr(value, "tolist"):
        value = value.tolist()
    if isinstance(value, (str, bytes)):
        return [float(value)]
    try:
        iterator = iter(value)
    except TypeError:
        return [float(value)]
    values: list[float] = []
    for item in iterator:
        values.extend(_flatten_degree_values(item))
    return values


def assert_mount_az_allowed_when_unwrap_disabled(
    az_deg: Any, *, action_label: str = "Az target"
) -> Optional[dict]:
    """Reject Az commands outside raw range when unwrap is configured off.

    This is a fail-closed guard for absolute-modulo encoders.  It intentionally
    uses the configured raw absolute encoder range, not the wider continuous
    drive range, because without unwrap the controller cannot distinguish e.g.
    10 deg from 370 deg.
    """

    limit = load_unwrap_disabled_mount_az_limit()
    if limit is None:
        return None
    raw_min = float(limit["raw_min_deg"])
    raw_max = float(limit["raw_max_deg"])
    values = _flatten_degree_values(az_deg)
    for value in values:
        if not math.isfinite(value) or value < raw_min or value > raw_max:
            raise ValueError(
                f"{action_label}: Az={value:g} deg is outside raw absolute "
                f"encoder range {raw_min:g}..{raw_max:g} deg while "
                "antenna_encoder_unwrap.az.enabled=false; enable Az unwrap "
                "or keep Az commands within raw_min/raw_max."
            )
    checked = dict(limit)
    checked["checked_value_count"] = len(values)
    return checked

def load_az_unwrap_config() -> (
    tuple[
        AbsoluteModuloUnwrapConfig, Optional[Path], float, float, float, str, str, int
    ]
):
    root = getattr(config, "antenna_encoder_unwrap", None)
    az = _cfg_get(root, "az", root)
    enabled = _to_bool(_cfg_get(az, "enabled", False), False)
    mode = str(_cfg_get(az, "mode", "absolute_modulo" if enabled else "pass_through"))
    drive_min, drive_max = _drive_range_deg()
    cfg = AbsoluteModuloUnwrapConfig(
        enabled=enabled and mode == "absolute_modulo",
        period_deg=float(
            _to_float(
                _cfg_get(az, "period", _cfg_get(az, "period_deg", 360.0)), "deg", 360.0
            )
        ),
        raw_min_deg=float(
            _to_float(
                _cfg_get(az, "raw_min", _cfg_get(az, "raw_min_deg", 0.0)), "deg", 0.0
            )
        ),
        raw_max_deg=float(
            _to_float(
                _cfg_get(az, "raw_max", _cfg_get(az, "raw_max_deg", 360.0)),
                "deg",
                360.0,
            )
        ),
        drive_min_deg=float(
            _to_float(
                _cfg_get(az, "drive_min", _cfg_get(az, "drive_min_deg", drive_min)),
                "deg",
                drive_min,
            )
        ),
        drive_max_deg=float(
            _to_float(
                _cfg_get(az, "drive_max", _cfg_get(az, "drive_max_deg", drive_max)),
                "deg",
                drive_max,
            )
        ),
        zero_offset_deg=float(
            _to_float(
                _cfg_get(az, "zero_offset", _cfg_get(az, "zero_offset_deg", 0.0)),
                "deg",
                0.0,
            )
        ),
        sign=int(_cfg_get(az, "sign", 1)),
        max_jump_deg=_to_float(
            _cfg_get(az, "max_jump", _cfg_get(az, "max_jump_deg", None)), "deg", None
        ),
    )
    state_path = _expand_path(_cfg_get(az, "state_path", None))
    heartbeat_value = _cfg_get(
        az,
        "state_heartbeat_sec",
        _cfg_get(
            az,
            "persist_heartbeat_interval",
            _cfg_get(az, "persist_heartbeat_interval_sec", 5.0),
        ),
    )
    heartbeat_sec = float(_to_float(heartbeat_value, "s", 5.0))
    state_warn_age_value = _cfg_get(
        az, "state_warn_age_sec", _cfg_get(az, "state_warn_age", 86400.0)
    )
    state_warn_age_sec = float(_to_float(state_warn_age_value, "s", 86400.0))
    state_raw_tol_value = _cfg_get(
        az, "state_raw_tolerance", _cfg_get(az, "state_raw_tolerance_deg", 5.0)
    )
    state_raw_tolerance_deg = float(_to_float(state_raw_tol_value, "deg", 5.0))
    startup_policy = str(
        _cfg_get(
            az,
            "startup_policy",
            "recover_if_raw_matches" if cfg.enabled else "pass_through",
        )
    )
    startup_discard_raw_out_of_range_samples = int(
        _cfg_get(
            az,
            "startup_discard_raw_out_of_range_samples",
            _cfg_get(az, "startup_discard_out_of_range_samples", 3),
        )
    )
    return (
        cfg,
        state_path,
        heartbeat_sec,
        state_warn_age_sec,
        state_raw_tolerance_deg,
        mode,
        startup_policy,
        max(0, startup_discard_raw_out_of_range_samples),
    )


class AtomicStateWriter:
    """Low-rate atomic state persistence, isolated from encoder callbacks."""

    def __init__(self, path: Optional[Path]) -> None:
        self.path = path
        self._queue: "queue.Queue[Optional[dict]]" = queue.Queue(maxsize=1)
        self._thread: Optional[threading.Thread] = None
        self.last_persist_time: float = float("nan")
        if self.path is not None:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            self._thread = threading.Thread(
                target=self._run, name="az_unwrap_state_writer", daemon=True
            )
            self._thread.start()

    def submit(self, payload: dict) -> None:
        if self.path is None:
            return
        try:
            while True:
                self._queue.get_nowait()
        except queue.Empty:
            pass
        try:
            self._queue.put_nowait(dict(payload))
        except queue.Full:
            pass

    def close(self, final_payload: Optional[dict] = None, timeout: float = 1.0) -> None:
        if final_payload is not None:
            self._write(final_payload)
        if self._thread is not None:
            try:
                while True:
                    self._queue.get_nowait()
            except queue.Empty:
                pass
            try:
                self._queue.put(None, timeout=0.05)
            except Exception:
                pass
            self._thread.join(timeout=timeout)

    def _run(self) -> None:
        while True:
            payload = self._queue.get()
            if payload is None:
                return
            self._write(payload)

    def _write(self, payload: dict) -> None:
        if self.path is None:
            return
        data = json.dumps(payload, sort_keys=True, indent=2) + "\n"
        fd = None
        tmp_path = None
        try:
            fd, tmp_name = tempfile.mkstemp(
                prefix=self.path.name + ".", suffix=".tmp", dir=str(self.path.parent)
            )
            tmp_path = Path(tmp_name)
            with os.fdopen(fd, "w") as fp:
                fd = None
                fp.write(data)
                fp.flush()
                os.fsync(fp.fileno())
            os.replace(tmp_path, self.path)
            try:
                dir_fd = os.open(str(self.path.parent), os.O_DIRECTORY)
                try:
                    os.fsync(dir_fd)
                finally:
                    os.close(dir_fd)
            except Exception:
                pass
            self.last_persist_time = time.time()
        finally:
            if fd is not None:
                try:
                    os.close(fd)
                except Exception:
                    pass
            if tmp_path is not None and tmp_path.exists():
                try:
                    tmp_path.unlink()
                except Exception:
                    pass


class AzUnwrapRuntime:
    def __init__(self) -> None:
        (
            self.cfg,
            self.state_path,
            self.heartbeat_sec,
            self.state_warn_age_sec,
            self.state_raw_tolerance_deg,
            self.mode,
            self.startup_policy,
            self.startup_discard_raw_out_of_range_samples,
        ) = load_az_unwrap_config()
        self.enabled = bool(self.cfg.enabled)
        if self.enabled and self.state_path is None:
            raise ValueError(
                "antenna_encoder_unwrap.az.state_path is required when az unwrap is enabled"
            )
        self.writer = AtomicStateWriter(self.state_path if self.enabled else None)
        self._last_state_time = float("nan")
        self._last_status: Optional[AntennaAzUnwrapStatus] = None
        self._previous_payload = self._load_previous_payload() if self.enabled else None
        self._previous_payload_age_sec = self._state_payload_age_sec(
            self._previous_payload
        )
        self._previous_payload_old = (
            self._previous_payload_age_sec is not None
            and self._previous_payload_age_sec > self.state_warn_age_sec
        )
        self._startup_state_note: Optional[str] = None
        self._startup_initialized = not self.enabled
        self._last_state_reload_time = 0.0
        self._startup_raw_out_of_range_discards = 0
        self.unwrapper = AbsoluteModuloUnwrapper(self.cfg, previous_continuous_deg=None)

    def _load_previous_payload(self) -> Optional[dict]:
        if self.state_path is None or not self.state_path.exists():
            return None
        try:
            payload = json.loads(self.state_path.read_text())
            if payload.get("enabled") is not True:
                return None
            if float(payload.get("period_deg")) != float(self.cfg.period_deg):
                return None
            if float(payload.get("drive_min_deg")) != float(self.cfg.drive_min_deg):
                return None
            if float(payload.get("drive_max_deg")) != float(self.cfg.drive_max_deg):
                return None
            if float(payload.get("zero_offset_deg", self.cfg.zero_offset_deg)) != float(
                self.cfg.zero_offset_deg
            ):
                return None
            if int(payload.get("sign", self.cfg.sign)) != int(self.cfg.sign):
                return None
            # Validate required numeric fields while loading.  Age is never a
            # rejection condition here; old but self-consistent state is allowed
            # and is exposed in progress as state=old.
            float(payload.get("raw_az_deg"))
            float(payload.get("continuous_az_deg"))
            int(payload.get("branch"))
            return payload
        except Exception:
            return None

    def _refresh_previous_payload(self, *, min_interval_sec: float = 0.5) -> None:
        """Reload the state file while waiting for manual branch initialization.

        Normal operation never polls the state file.  This is used only before
        startup has completed, so an operator can run the manual initialization
        command after an ambiguous startup without restarting the encoder node.
        The small throttle avoids accidental high-rate file I/O during a fault.
        """
        if not self.enabled or self.state_path is None:
            return
        now = time.time()
        if (now - self._last_state_reload_time) < min_interval_sec:
            return
        self._last_state_reload_time = now
        payload = self._load_previous_payload()
        self._previous_payload = payload
        self._previous_payload_age_sec = self._state_payload_age_sec(payload)
        self._previous_payload_old = (
            self._previous_payload_age_sec is not None
            and self._previous_payload_age_sec > self.state_warn_age_sec
        )

    def _state_payload_age_sec(self, payload: Optional[dict]) -> Optional[float]:
        if payload is None:
            return None
        try:
            t = float(
                payload.get(
                    "persist_request_time_unix", payload.get("encoder_time_unix", 0.0)
                )
            )
            return max(0.0, time.time() - t)
        except Exception:
            return None

    def _raw_within_nominal_range(self, raw_az_deg: float) -> bool:
        try:
            raw = float(raw_az_deg)
        except Exception:
            return False
        if not math.isfinite(raw):
            return False
        eps = 1e-9
        return (
            (float(self.cfg.raw_min_deg) - eps)
            <= raw
            <= (float(self.cfg.raw_max_deg) + eps)
        )

    def _raw_range_is_full_period(self) -> bool:
        return (
            abs(
                (float(self.cfg.raw_max_deg) - float(self.cfg.raw_min_deg))
                - float(self.cfg.period_deg)
            )
            <= 1e-9
        )

    def _startup_should_discard_nominal_out_of_range_raw(
        self, raw_az_deg: float
    ) -> bool:
        """Suppress the first few finite periodic aliases after node startup.

        RS-232C devices can leave a stale or partial response in the input stream
        immediately after opening the port.  Values such as 727 deg can be a
        harmless 2*360+7 deg alias, but they can also be a startup garbage read.
        Before the unwrap branch is initialized, discard a small configurable
        number of finite out-of-nominal-range samples.  After that grace window,
        the normal modulo-aware unwrap logic may initialize from a finite alias.
        """
        if self._startup_initialized:
            return False
        if self.startup_discard_raw_out_of_range_samples <= 0:
            return False
        if not self._raw_range_is_full_period():
            return False
        if self._raw_within_nominal_range(raw_az_deg):
            return False
        # Non-finite values are handled by the unwrap validator, not by this
        # startup alias grace path.
        try:
            raw = float(raw_az_deg)
        except Exception:
            return False
        if not math.isfinite(raw):
            return False
        if (
            self._startup_raw_out_of_range_discards
            >= self.startup_discard_raw_out_of_range_samples
        ):
            return False
        self._startup_raw_out_of_range_discards += 1
        return True

    def _raw_matches_previous_payload(self, raw_az_deg: float, payload: dict) -> bool:
        try:
            previous_raw = float(payload.get("raw_az_deg"))
            current_modulo = normalize_absolute_modulo_raw(float(raw_az_deg), self.cfg)
            previous_modulo = normalize_absolute_modulo_raw(previous_raw, self.cfg)
        except Exception:
            return False
        # raw is an absolute-modulo value; compare calibrated modulo positions on
        # the period circle so values near 0/360 and startup aliases such as
        # 727 deg (= 2 * 360 + 7 deg) are treated correctly.
        period = float(self.cfg.period_deg)
        delta = (
            (current_modulo - previous_modulo + 0.5 * period) % period
        ) - 0.5 * period
        return abs(delta) <= float(self.state_raw_tolerance_deg)

    def _initialize_startup_branch(self, raw_az_deg: float) -> None:
        if self._startup_initialized:
            return
        self._refresh_previous_payload()
        if self._previous_payload is not None and self._raw_matches_previous_payload(
            raw_az_deg, self._previous_payload
        ):
            previous = float(self._previous_payload["continuous_az_deg"])
            self.unwrapper = AbsoluteModuloUnwrapper(
                self.cfg, previous_continuous_deg=previous
            )
            self._startup_initialized = True
            self._startup_state_note = "old" if self._previous_payload_old else "ok"
            return

        # No usable state file: allow automatic initialization only when the
        # current raw reading maps to exactly one continuous branch.  If it maps
        # to multiple branches, AbsoluteModuloUnwrapper.unwrap() will raise
        # AmbiguousBranchError and the encoder sample is suppressed until manual
        # initialization is performed.
        self.unwrapper = AbsoluteModuloUnwrapper(self.cfg, previous_continuous_deg=None)
        self._startup_initialized = True
        if self._previous_payload is None:
            self._startup_state_note = "auto-init"
        else:
            self._startup_state_note = "state-mismatch"

    def process(
        self, raw_az_deg: float, *, el_deg: float, encoder_time: float
    ) -> tuple[float, AntennaAzUnwrapStatus]:
        now = time.time()
        if not self.enabled:
            status = self._make_status(
                now=now,
                encoder_time=encoder_time,
                raw_az=float(raw_az_deg),
                modulo_az=float(raw_az_deg),
                continuous_az=float(raw_az_deg),
                branch=0,
                branch_changed=False,
                valid=True,
                state="disabled",
                reason="unwrap disabled",
            )
            self._last_status = status
            return float(raw_az_deg), status

        try:
            if self._startup_should_discard_nominal_out_of_range_raw(raw_az_deg):
                raw = float(raw_az_deg)
                folded = normalize_absolute_modulo_raw(raw, self.cfg)
                status = self._make_status(
                    now=now,
                    encoder_time=encoder_time,
                    raw_az=raw,
                    modulo_az=folded,
                    continuous_az=-1.0,
                    branch=0,
                    branch_changed=False,
                    valid=False,
                    state="startup-raw-wait",
                    reason=(
                        f"startup raw Az {raw:.6f} deg is outside nominal "
                        f"range [{self.cfg.raw_min_deg:.6f}, {self.cfg.raw_max_deg:.6f}] deg; "
                        f"discarded {self._startup_raw_out_of_range_discards}/"
                        f"{self.startup_discard_raw_out_of_range_samples} before alias acceptance"
                    ),
                )
                self._last_status = status
                raise RuntimeError(status.reason)
            self._initialize_startup_branch(raw_az_deg)
            result = self.unwrapper.unwrap(raw_az_deg)
            state = "ok"
            reason = ""
            if self._startup_state_note == "old":
                state = "old"
                reason = "recovered from old but raw-matching state file"
            elif self._startup_state_note == "auto-init":
                state = "auto-init"
                reason = "state file missing; unique branch auto-initialized"
            elif self._startup_state_note == "state-mismatch":
                state = "auto-init"
                reason = "state file ignored because raw did not match; unique branch auto-initialized"
            status = self._make_status(
                now=now,
                encoder_time=encoder_time,
                raw_az=result.raw_deg,
                modulo_az=result.modulo_deg,
                continuous_az=result.continuous_deg,
                branch=result.branch,
                branch_changed=result.branch_changed,
                valid=True,
                state=state,
                reason=reason,
            )
            self._last_status = status
            if (
                result.branch_changed
                or not math.isfinite(self._last_state_time)
                or (now - self._last_state_time) >= self.heartbeat_sec
            ):
                self._persist(status)
            return result.continuous_deg, status
        except AmbiguousBranchError as exc:
            # Keep startup open so a subsequently written manual state file can be
            # picked up without restarting the encoder node.
            self._startup_initialized = False
            status = self._error_status(
                now, encoder_time, raw_az_deg, "ambiguous", str(exc)
            )
        except NoValidBranchError as exc:
            self._startup_initialized = False
            status = self._error_status(
                now, encoder_time, raw_az_deg, "no-valid-branch", str(exc)
            )
        except BranchJumpError as exc:
            status = self._error_status(now, encoder_time, raw_az_deg, "jump", str(exc))
        except RawAngleRangeError as exc:
            status = self._error_status(
                now, encoder_time, raw_az_deg, "raw-range", str(exc)
            )
        except AngleUnwrapError as exc:
            status = self._error_status(
                now, encoder_time, raw_az_deg, "error", str(exc)
            )
        self._last_status = status
        raise RuntimeError(status.reason)


    def get_state_report(self) -> dict:
        """Return runtime/state-file information for ROS service responses.

        The report is generated inside the encoder node process.  Therefore the
        reported state_path and state-file content refer to the path that
        encoder_readout actually uses, not to a caller's local ~/.necst.
        """
        status = self._last_status
        payload = self._load_previous_payload() if self.enabled else None
        if status is not None:
            return {
                "success": True,
                "enabled": bool(status.enabled),
                "valid": bool(status.valid),
                "state": str(status.state),
                "reason": str(status.reason),
                "state_path": "" if self.state_path is None else str(self.state_path),
                "raw_az_deg": float(status.raw_az_deg),
                "modulo_az_deg": float(status.modulo_az_deg),
                "continuous_az_deg": float(status.continuous_az_deg),
                "branch": int(status.branch),
                "period_deg": float(status.period_deg),
                "drive_min_deg": float(status.drive_min_deg),
                "drive_max_deg": float(status.drive_max_deg),
                "state_age_sec": float(status.state_age_sec),
                "persist_age_sec": float(status.persist_age_sec),
            }
        if payload is not None:
            age = self._state_payload_age_sec(payload)
            state = str(payload.get("state", "state-file"))
            reason = "state file loaded; no encoder status has been published yet"
            if age is not None and age > self.state_warn_age_sec:
                state = "old"
                reason = (
                    "old state file loaded; "
                    "no encoder status has been published yet"
                )
            return {
                "success": True,
                "enabled": bool(self.enabled),
                "valid": True,
                "state": state,
                "reason": reason,
                "state_path": "" if self.state_path is None else str(self.state_path),
                "raw_az_deg": float(payload.get("raw_az_deg")),
                "modulo_az_deg": float(payload.get("modulo_az_deg")),
                "continuous_az_deg": float(payload.get("continuous_az_deg")),
                "branch": int(payload.get("branch")),
                "period_deg": float(self.cfg.period_deg),
                "drive_min_deg": float(self.cfg.drive_min_deg),
                "drive_max_deg": float(self.cfg.drive_max_deg),
                "state_age_sec": 0.0 if age is None else float(age),
                "persist_age_sec": 0.0 if age is None else float(age),
            }
        return {
            "success": True,
            "enabled": bool(self.enabled),
            "valid": False,
            "state": "no-state",
            "reason": (
                "state file does not exist and "
                "no encoder status has been published yet"
            ),
            "state_path": "" if self.state_path is None else str(self.state_path),
            "raw_az_deg": -1.0,
            "modulo_az_deg": -1.0,
            "continuous_az_deg": -1.0,
            "branch": 0,
            "period_deg": float(self.cfg.period_deg),
            "drive_min_deg": float(self.cfg.drive_min_deg),
            "drive_max_deg": float(self.cfg.drive_max_deg),
            "state_age_sec": -1.0,
            "persist_age_sec": -1.0,
        }

    def manual_initialize(
        self,
        *,
        raw_az: Optional[float],
        continuous_az: Optional[float],
        branch: Optional[int],
        reason: str = "manual state set via encoder service",
    ) -> AntennaAzUnwrapStatus:
        """Validate and persist a manual unwrap state inside encoder_readout.

        This is the service-side equivalent of the old local-file CLI.  The
        validation rules and JSON schema are shared with the local fallback, but
        the write happens in the encoder node process, so Docker/HOME differences
        cannot redirect the state file to the wrong computer.
        """
        if not self.enabled:
            raise ValueError("az unwrap is not enabled in the encoder node config")
        if self.state_path is None:
            raise ValueError(
                "antenna_encoder_unwrap.az.state_path is not configured "
                "in the encoder node config"
            )
        payload = _manual_state_payload(
            self.cfg,
            self.mode,
            raw_az=raw_az,
            continuous_az=continuous_az,
            branch=branch,
        )
        self.writer._write(payload)
        self._previous_payload = payload
        self._previous_payload_age_sec = 0.0
        self._previous_payload_old = False
        self.unwrapper = AbsoluteModuloUnwrapper(
            self.cfg, previous_continuous_deg=float(payload["continuous_az_deg"])
        )
        self._startup_initialized = True
        self._startup_state_note = "manual"
        now = time.time()
        status = self._make_status(
            now=now,
            encoder_time=now,
            raw_az=float(payload["raw_az_deg"]),
            modulo_az=float(payload["modulo_az_deg"]),
            continuous_az=float(payload["continuous_az_deg"]),
            branch=int(payload["branch"]),
            branch_changed=False,
            valid=True,
            state="manual",
            reason=reason,
        )
        self._last_status = status
        self._last_state_time = now
        return status

    def close(self) -> None:
        if self._last_status is not None and self.enabled and self._last_status.valid:
            self._persist(self._last_status, force_sync=True)
        self.writer.close()

    def _persist(
        self, status: AntennaAzUnwrapStatus, *, force_sync: bool = False
    ) -> None:
        payload = {
            "version": 1,
            "enabled": bool(status.enabled),
            "mode": str(status.mode),
            "state": str(status.state),
            "raw_az_deg": float(status.raw_az_deg),
            "modulo_az_deg": float(status.modulo_az_deg),
            "continuous_az_deg": float(status.continuous_az_deg),
            "branch": int(status.branch),
            "encoder_time_unix": float(status.encoder_time_unix),
            "persist_request_time_unix": time.time(),
            "period_deg": float(status.period_deg),
            "drive_min_deg": float(status.drive_min_deg),
            "drive_max_deg": float(status.drive_max_deg),
            "zero_offset_deg": float(self.cfg.zero_offset_deg),
            "sign": int(self.cfg.sign),
        }
        self._last_state_time = time.time()
        if force_sync:
            self.writer._write(payload)
        else:
            self.writer.submit(payload)

    @staticmethod
    def _finite_float(value: Any, *, unknown: float = -1.0) -> float:
        """Return a finite float suitable for ROS message float64 fields.

        ROS 2 message field validation rejects NaN and infinities even though
        they are Python floats.  Use a documented finite sentinel for unknown
        status-only quantities so a status publish failure can never suppress
        the encoder topic.
        """
        try:
            value_f = float(value)
        except Exception:
            return float(unknown)
        if math.isfinite(value_f):
            return value_f
        return float(unknown)

    def _make_status(
        self,
        *,
        now: float,
        encoder_time: float,
        raw_az: float,
        modulo_az: float,
        continuous_az: float,
        branch: int,
        branch_changed: bool,
        valid: bool,
        state: str,
        reason: str,
    ) -> AntennaAzUnwrapStatus:
        # -1.0 means "not persisted yet".  Do not use NaN/inf here: rclpy
        # validates float64 assignment and will reject non-finite values.
        persist_age = -1.0
        if math.isfinite(self.writer.last_persist_time):
            persist_age = max(0.0, now - self.writer.last_persist_time)
        state_age = (
            0.0
            if not math.isfinite(self._last_state_time)
            else max(0.0, now - self._last_state_time)
        )
        return AntennaAzUnwrapStatus(
            enabled=bool(self.enabled),
            valid=bool(valid),
            mode=str(self.mode)[:32],
            state=str(state)[:32],
            reason=str(reason)[:128],
            publish_time_unix=self._finite_float(now),
            encoder_time_unix=self._finite_float(encoder_time),
            raw_az_deg=self._finite_float(raw_az),
            modulo_az_deg=self._finite_float(modulo_az),
            continuous_az_deg=self._finite_float(continuous_az),
            branch=int(branch),
            branch_nonzero=bool(branch != 0),
            branch_changed=bool(branch_changed),
            period_deg=self._finite_float(self.cfg.period_deg),
            drive_min_deg=self._finite_float(self.cfg.drive_min_deg),
            drive_max_deg=self._finite_float(self.cfg.drive_max_deg),
            state_age_sec=self._finite_float(state_age),
            persist_age_sec=self._finite_float(persist_age),
        )

    def _error_status(
        self, now: float, encoder_time: float, raw_az: float, state: str, reason: str
    ) -> AntennaAzUnwrapStatus:
        previous = self.unwrapper.previous_continuous_deg
        continuous = -1.0 if previous is None else float(previous)
        branch = (
            0
            if self.unwrapper.previous_branch is None
            else int(self.unwrapper.previous_branch)
        )
        try:
            modulo = normalize_absolute_modulo_raw(float(raw_az), self.cfg)
        except Exception:
            modulo = -1.0
        return self._make_status(
            now=now,
            encoder_time=encoder_time,
            raw_az=float(raw_az),
            modulo_az=modulo,
            continuous_az=continuous,
            branch=branch,
            branch_changed=False,
            valid=False,
            state=state,
            reason=reason,
        )


def _manual_state_payload(
    cfg: AbsoluteModuloUnwrapConfig,
    mode: str,
    *,
    raw_az: Optional[float],
    continuous_az: Optional[float],
    branch: Optional[int],
) -> dict:
    def _check_raw_range(raw: float) -> None:
        try:
            normalize_absolute_modulo_raw(float(raw), cfg)
        except RawAngleRangeError as exc:
            raise ValueError(f"manual raw Az is unusable: {exc}") from exc

    if continuous_az is None:
        if raw_az is None or branch is None:
            raise ValueError(
                "manual initialization requires --continuous-az or both --raw-az and --branch"
            )
        raw_az = float(raw_az)
        _check_raw_range(raw_az)
        modulo = normalize_absolute_modulo_raw(raw_az, cfg)
        continuous_az = modulo + int(branch) * cfg.period_deg
    else:
        continuous_az = float(continuous_az)
        modulo = ((continuous_az - cfg.raw_min_deg) % cfg.period_deg) + cfg.raw_min_deg
        derived_branch = int(round((continuous_az - modulo) / cfg.period_deg))
        if branch is not None and int(branch) != derived_branch:
            raise ValueError(
                f"manual branch {branch!r} is inconsistent with continuous_az={continuous_az:.6f} deg "
                f"(expected branch {derived_branch})"
            )
        branch = derived_branch
        if raw_az is None:
            # Invert calibrated_modulo = sign * raw + zero_offset modulo period.
            raw_az = (
                (cfg.sign * (modulo - cfg.zero_offset_deg) - cfg.raw_min_deg)
                % cfg.period_deg
            ) + cfg.raw_min_deg
            _check_raw_range(raw_az)
        else:
            raw_az = float(raw_az)
            _check_raw_range(raw_az)
            raw_modulo = normalize_absolute_modulo_raw(raw_az, cfg)
            if abs(raw_modulo - modulo) > 1e-6:
                raise ValueError(
                    f"manual raw_az={raw_az:.6f} deg is inconsistent with "
                    f"continuous_az={continuous_az:.6f} deg"
                )
    if not (cfg.drive_min_deg <= float(continuous_az) <= cfg.drive_max_deg):
        raise ValueError(
            f"manual continuous Az {continuous_az:.6f} deg is outside drive range "
            f"[{cfg.drive_min_deg:.6f}, {cfg.drive_max_deg:.6f}] deg"
        )
    now = time.time()
    return {
        "version": 1,
        "enabled": True,
        "mode": mode,
        "state": "manual",
        "raw_az_deg": float(raw_az),
        "modulo_az_deg": float(modulo),
        "continuous_az_deg": float(continuous_az),
        "branch": int(branch),
        "encoder_time_unix": now,
        "persist_request_time_unix": now,
        "period_deg": float(cfg.period_deg),
        "drive_min_deg": float(cfg.drive_min_deg),
        "drive_max_deg": float(cfg.drive_max_deg),
        "zero_offset_deg": float(cfg.zero_offset_deg),
        "sign": int(cfg.sign),
    }



def _local_cli(args) -> int:
    (
        cfg,
        state_path,
        _heartbeat,
        _state_warn_age,
        _state_raw_tolerance,
        mode,
        _startup_policy,
        _startup_discard_raw_out_of_range_samples,
    ) = load_az_unwrap_config()
    if state_path is None:
        raise SystemExit("antenna_encoder_unwrap.az.state_path is not configured")
    if args.cmd == "status":
        print(f"enabled={cfg.enabled} mode={mode} state_path={state_path}")
        if state_path.exists():
            print(state_path.read_text().strip())
        else:
            print("state file does not exist")
        return 0
    if args.cmd == "set":
        if not cfg.enabled:
            raise SystemExit("az unwrap is not enabled in the current config")
        payload = _manual_state_payload(
            cfg,
            mode,
            raw_az=args.raw_az,
            continuous_az=args.continuous_az,
            branch=args.branch,
        )
        writer = AtomicStateWriter(state_path)
        writer._write(payload)
        writer.close()
        print(
            f"wrote {state_path}: continuous={payload['continuous_az_deg']:.6f} deg "
            f"branch={payload['branch']} raw={payload['raw_az_deg']:.6f} deg"
        )
        return 0
    return 2


def _copy_report_to_response(response: Any, report: Mapping[str, Any]) -> Any:
    for key, value in report.items():
        if hasattr(response, key):
            setattr(response, key, value)
    return response


def fill_get_response(response: Any, report: Mapping[str, Any]) -> Any:
    """Fill GetAzUnwrapState.Response-like objects from a runtime report."""
    return _copy_report_to_response(response, report)


def fill_set_response(
    response: Any,
    *,
    success: bool,
    state: str,
    reason: str,
    state_path: str,
    enabled: bool,
    valid: bool,
    raw_az_deg: float,
    modulo_az_deg: float,
    continuous_az_deg: float,
    branch: int,
    period_deg: float,
    drive_min_deg: float,
    drive_max_deg: float,
) -> Any:
    """Fill SetAzUnwrapState.Response-like objects without importing ROS types."""
    values = {
        "success": bool(success),
        "state": str(state),
        "reason": str(reason),
        "state_path": str(state_path),
        "enabled": bool(enabled),
        "valid": bool(valid),
        "raw_az_deg": float(raw_az_deg),
        "modulo_az_deg": float(modulo_az_deg),
        "continuous_az_deg": float(continuous_az_deg),
        "branch": int(branch),
        "period_deg": float(period_deg),
        "drive_min_deg": float(drive_min_deg),
        "drive_max_deg": float(drive_max_deg),
    }
    return _copy_report_to_response(response, values)


def report_from_status(
    runtime: AzUnwrapRuntime,
    status: AntennaAzUnwrapStatus,
    *,
    success: bool = True,
) -> dict:
    """Convert a status message to a response/report dictionary."""
    return {
        "success": bool(success),
        "enabled": bool(status.enabled),
        "valid": bool(status.valid),
        "state": str(status.state),
        "reason": str(status.reason),
        "state_path": "" if runtime.state_path is None else str(runtime.state_path),
        "raw_az_deg": float(status.raw_az_deg),
        "modulo_az_deg": float(status.modulo_az_deg),
        "continuous_az_deg": float(status.continuous_az_deg),
        "branch": int(status.branch),
        "period_deg": float(status.period_deg),
        "drive_min_deg": float(status.drive_min_deg),
        "drive_max_deg": float(status.drive_max_deg),
        "state_age_sec": float(status.state_age_sec),
        "persist_age_sec": float(status.persist_age_sec),
    }


def _ros_wait_for_future(rclpy, node, future, *, timeout_sec: float):
    end_time = time.monotonic() + float(timeout_sec)
    while rclpy.ok() and not future.done():
        remaining = end_time - time.monotonic()
        if remaining <= 0:
            break
        rclpy.spin_once(node, timeout_sec=min(0.1, remaining))
    return future.done()


def _response_to_report(response: Any) -> dict:
    return {
        name: getattr(response, name)
        for name in response.get_fields_and_field_types()
    }


def _print_state_report(report: Mapping[str, Any]) -> None:
    print(
        "enabled={enabled} valid={valid} state={state} branch={branch} "
        "raw={raw:.6f} continuous={continuous:.6f} state_path={state_path}".format(
            enabled=bool(report.get("enabled", False)),
            valid=bool(report.get("valid", False)),
            state=str(report.get("state", "")),
            branch=int(report.get("branch", 0)),
            raw=float(report.get("raw_az_deg", -1.0)),
            continuous=float(report.get("continuous_az_deg", -1.0)),
            state_path=str(report.get("state_path", "")),
        )
    )
    reason = str(report.get("reason", ""))
    if reason:
        print(f"reason={reason}")


def _ros_cli(args) -> int:
    import rclpy

    from necst import service
    from necst_msgs.srv import GetAzUnwrapState, SetAzUnwrapState

    rclpy.init(args=None)
    node = rclpy.create_node("az_unwrap_state_cli", namespace="/")
    try:
        if args.cmd == "status":
            client = service.az_unwrap_state_get.client(node)
            if not client.wait_for_service(timeout_sec=args.timeout):
                raise SystemExit(
                    "az unwrap get service is not available. "
                    "Start encoder_readout, or use --local only when "
                    "intentionally reading the local state_path."
                )
            request = GetAzUnwrapState.Request()
            future = client.call_async(request)
            if not _ros_wait_for_future(rclpy, node, future, timeout_sec=args.timeout):
                raise SystemExit("timed out waiting for az unwrap get service response")
            response = future.result()
            if response is None:
                raise SystemExit("az unwrap get service failed without a response")
            report = _response_to_report(response)
            _print_state_report(report)
            return 0 if bool(response.success) else 1
        if args.cmd == "set":
            client = service.az_unwrap_state_set.client(node)
            if not client.wait_for_service(timeout_sec=args.timeout):
                raise SystemExit(
                    "az unwrap set service is not available. "
                    "Start encoder_readout, or run on the encoder node with "
                    "--local only as an emergency fallback."
                )
            request = SetAzUnwrapState.Request()
            request.use_raw_az = args.raw_az is not None
            request.raw_az_deg = 0.0 if args.raw_az is None else float(args.raw_az)
            request.use_continuous_az = args.continuous_az is not None
            request.continuous_az_deg = (
                0.0 if args.continuous_az is None else float(args.continuous_az)
            )
            request.use_branch = args.branch is not None
            request.branch = 0 if args.branch is None else int(args.branch)
            future = client.call_async(request)
            if not _ros_wait_for_future(rclpy, node, future, timeout_sec=args.timeout):
                raise SystemExit("timed out waiting for az unwrap set service response")
            response = future.result()
            if response is None:
                raise SystemExit("az unwrap set service failed without a response")
            report = _response_to_report(response)
            _print_state_report(report)
            return 0 if bool(response.success) else 1
        return 2
    finally:
        node.destroy_node()
        rclpy.try_shutdown()


def main(argv=None) -> int:
    """Inspect or initialize Az unwrap state.

    Default mode uses ROS services served by encoder_readout, so the state is
    written to the encoder node's actual state_path.  Use --local only as a
    deliberate emergency fallback on the encoder PC/container.
    """
    import argparse

    parser = argparse.ArgumentParser(
        description=(
            "Inspect or initialize NECST Az unwrap state "
            "via encoder_readout service."
        )
    )
    parser.add_argument(
        "--local",
        action="store_true",
        help=(
            "read/write the local state_path directly; "
            "use only on the encoder PC/container"
        ),
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=3.0,
        help="ROS service wait/response timeout [s]",
    )
    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("status", help="show encoder_readout unwrap state")
    setp = sub.add_parser("set", help="set encoder_readout unwrap state")

    def _deg_arg(text: str) -> float:
        return float(str(text).strip().removesuffix("deg").removesuffix("°"))

    setp.add_argument(
        "--raw-az",
        type=_deg_arg,
        default=None,
        help="current absolute modulo raw Az [deg]",
    )
    setp.add_argument(
        "--continuous-az",
        type=_deg_arg,
        default=None,
        help="desired continuous Az [deg]",
    )
    setp.add_argument(
        "--branch",
        type=int,
        default=None,
        help="desired branch; requires --raw-az when --continuous-az is omitted",
    )
    args = parser.parse_args(argv)
    if args.local:
        return _local_cli(args)
    return _ros_cli(args)


if __name__ == "__main__":
    raise SystemExit(main())
