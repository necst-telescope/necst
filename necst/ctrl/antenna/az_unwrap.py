"""Runtime support for absolute-modulo azimuth encoder unwrapping."""

from __future__ import annotations

import json
import math
import os
import queue
import tempfile
import threading
import time
from dataclasses import asdict
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


def _to_float(value: Any, unit: str = "deg", default: Optional[float] = None) -> Optional[float]:
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


def load_az_unwrap_config() -> tuple[AbsoluteModuloUnwrapConfig, Optional[Path], float, float, float, str, str]:
    root = getattr(config, "antenna_encoder_unwrap", None)
    az = _cfg_get(root, "az", root)
    enabled = _to_bool(_cfg_get(az, "enabled", False), False)
    mode = str(_cfg_get(az, "mode", "absolute_modulo" if enabled else "pass_through"))
    drive_min, drive_max = _drive_range_deg()
    cfg = AbsoluteModuloUnwrapConfig(
        enabled=enabled and mode == "absolute_modulo",
        period_deg=float(_to_float(_cfg_get(az, "period", _cfg_get(az, "period_deg", 360.0)), "deg", 360.0)),
        raw_min_deg=float(_to_float(_cfg_get(az, "raw_min", _cfg_get(az, "raw_min_deg", 0.0)), "deg", 0.0)),
        raw_max_deg=float(_to_float(_cfg_get(az, "raw_max", _cfg_get(az, "raw_max_deg", 360.0)), "deg", 360.0)),
        drive_min_deg=float(_to_float(_cfg_get(az, "drive_min", _cfg_get(az, "drive_min_deg", drive_min)), "deg", drive_min)),
        drive_max_deg=float(_to_float(_cfg_get(az, "drive_max", _cfg_get(az, "drive_max_deg", drive_max)), "deg", drive_max)),
        zero_offset_deg=float(_to_float(_cfg_get(az, "zero_offset", _cfg_get(az, "zero_offset_deg", 0.0)), "deg", 0.0)),
        sign=int(_cfg_get(az, "sign", 1)),
        max_jump_deg=_to_float(_cfg_get(az, "max_jump", _cfg_get(az, "max_jump_deg", None)), "deg", None),
    )
    state_path = _expand_path(_cfg_get(az, "state_path", None))
    heartbeat_value = _cfg_get(
        az,
        "state_heartbeat_sec",
        _cfg_get(az, "persist_heartbeat_interval", _cfg_get(az, "persist_heartbeat_interval_sec", 5.0)),
    )
    heartbeat_sec = float(_to_float(heartbeat_value, "s", 5.0))
    state_warn_age_value = _cfg_get(az, "state_warn_age_sec", _cfg_get(az, "state_warn_age", 86400.0))
    state_warn_age_sec = float(_to_float(state_warn_age_value, "s", 86400.0))
    state_raw_tol_value = _cfg_get(az, "state_raw_tolerance", _cfg_get(az, "state_raw_tolerance_deg", 5.0))
    state_raw_tolerance_deg = float(_to_float(state_raw_tol_value, "deg", 5.0))
    startup_policy = str(_cfg_get(az, "startup_policy", "recover_if_raw_matches" if cfg.enabled else "pass_through"))
    return cfg, state_path, heartbeat_sec, state_warn_age_sec, state_raw_tolerance_deg, mode, startup_policy


class AtomicStateWriter:
    """Low-rate atomic state persistence, isolated from encoder callbacks."""

    def __init__(self, path: Optional[Path]) -> None:
        self.path = path
        self._queue: "queue.Queue[Optional[dict]]" = queue.Queue(maxsize=1)
        self._thread: Optional[threading.Thread] = None
        self.last_persist_time: float = float("nan")
        if self.path is not None:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            self._thread = threading.Thread(target=self._run, name="az_unwrap_state_writer", daemon=True)
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
            fd, tmp_name = tempfile.mkstemp(prefix=self.path.name + ".", suffix=".tmp", dir=str(self.path.parent))
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
        self._previous_payload_age_sec = self._state_payload_age_sec(self._previous_payload)
        self._previous_payload_old = (
            self._previous_payload_age_sec is not None
            and self._previous_payload_age_sec > self.state_warn_age_sec
        )
        self._startup_state_note: Optional[str] = None
        self._startup_initialized = not self.enabled
        self._last_state_reload_time = 0.0
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
            if float(payload.get("zero_offset_deg", self.cfg.zero_offset_deg)) != float(self.cfg.zero_offset_deg):
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
            t = float(payload.get("persist_request_time_unix", payload.get("encoder_time_unix", 0.0)))
            return max(0.0, time.time() - t)
        except Exception:
            return None

    def _raw_matches_previous_payload(self, raw_az_deg: float, payload: dict) -> bool:
        try:
            previous_raw = float(payload.get("raw_az_deg"))
        except Exception:
            return False
        # raw is an absolute-modulo value; compare on the period circle so values
        # near 0/360 are treated correctly.
        period = float(self.cfg.period_deg)
        delta = ((float(raw_az_deg) - previous_raw + 0.5 * period) % period) - 0.5 * period
        return abs(delta) <= float(self.state_raw_tolerance_deg)

    def _initialize_startup_branch(self, raw_az_deg: float) -> None:
        if self._startup_initialized:
            return
        self._refresh_previous_payload()
        if self._previous_payload is not None and self._raw_matches_previous_payload(raw_az_deg, self._previous_payload):
            previous = float(self._previous_payload["continuous_az_deg"])
            self.unwrapper = AbsoluteModuloUnwrapper(self.cfg, previous_continuous_deg=previous)
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

    def process(self, raw_az_deg: float, *, el_deg: float, encoder_time: float) -> tuple[float, AntennaAzUnwrapStatus]:
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
            if result.branch_changed or not math.isfinite(self._last_state_time) or (now - self._last_state_time) >= self.heartbeat_sec:
                self._persist(status)
            return result.continuous_deg, status
        except AmbiguousBranchError as exc:
            # Keep startup open so a subsequently written manual state file can be
            # picked up without restarting the encoder node.
            self._startup_initialized = False
            status = self._error_status(now, encoder_time, raw_az_deg, "ambiguous", str(exc))
        except NoValidBranchError as exc:
            self._startup_initialized = False
            status = self._error_status(now, encoder_time, raw_az_deg, "no-valid-branch", str(exc))
        except BranchJumpError as exc:
            status = self._error_status(now, encoder_time, raw_az_deg, "jump", str(exc))
        except RawAngleRangeError as exc:
            status = self._error_status(now, encoder_time, raw_az_deg, "raw-range", str(exc))
        except AngleUnwrapError as exc:
            status = self._error_status(now, encoder_time, raw_az_deg, "error", str(exc))
        self._last_status = status
        raise RuntimeError(status.reason)

    def close(self) -> None:
        if self._last_status is not None and self.enabled and self._last_status.valid:
            self._persist(self._last_status, force_sync=True)
        self.writer.close()

    def _persist(self, status: AntennaAzUnwrapStatus, *, force_sync: bool = False) -> None:
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
        persist_age = float("nan")
        if math.isfinite(self.writer.last_persist_time):
            persist_age = max(0.0, now - self.writer.last_persist_time)
        state_age = 0.0 if not math.isfinite(self._last_state_time) else max(0.0, now - self._last_state_time)
        return AntennaAzUnwrapStatus(
            enabled=bool(self.enabled),
            valid=bool(valid),
            mode=str(self.mode)[:32],
            state=str(state)[:32],
            reason=str(reason)[:128],
            publish_time_unix=float(now),
            encoder_time_unix=float(encoder_time),
            raw_az_deg=float(raw_az),
            modulo_az_deg=float(modulo_az),
            continuous_az_deg=float(continuous_az),
            branch=int(branch),
            branch_nonzero=bool(branch != 0),
            branch_changed=bool(branch_changed),
            period_deg=float(self.cfg.period_deg),
            drive_min_deg=float(self.cfg.drive_min_deg),
            drive_max_deg=float(self.cfg.drive_max_deg),
            state_age_sec=float(state_age),
            persist_age_sec=float(persist_age),
        )

    def _error_status(self, now: float, encoder_time: float, raw_az: float, state: str, reason: str) -> AntennaAzUnwrapStatus:
        previous = self.unwrapper.previous_continuous_deg
        continuous = float("nan") if previous is None else float(previous)
        branch = 0 if self.unwrapper.previous_branch is None else int(self.unwrapper.previous_branch)
        return self._make_status(
            now=now,
            encoder_time=encoder_time,
            raw_az=float(raw_az),
            modulo_az=float("nan"),
            continuous_az=continuous,
            branch=branch,
            branch_changed=False,
            valid=False,
            state=state,
            reason=reason,
        )



def _manual_state_payload(cfg: AbsoluteModuloUnwrapConfig, mode: str, *, raw_az: Optional[float], continuous_az: Optional[float], branch: Optional[int]) -> dict:
    def _check_raw_range(raw: float) -> None:
        if not (cfg.raw_min_deg <= float(raw) <= cfg.raw_max_deg):
            raise ValueError(
                f"manual raw Az {float(raw):.6f} deg is outside raw range "
                f"[{cfg.raw_min_deg:.6f}, {cfg.raw_max_deg:.6f}] deg"
            )

    if continuous_az is None:
        if raw_az is None or branch is None:
            raise ValueError("manual initialization requires --continuous-az or both --raw-az and --branch")
        raw_az = float(raw_az)
        _check_raw_range(raw_az)
        modulo = ((cfg.sign * raw_az + cfg.zero_offset_deg - cfg.raw_min_deg) % cfg.period_deg) + cfg.raw_min_deg
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
            raw_az = ((cfg.sign * (modulo - cfg.zero_offset_deg) - cfg.raw_min_deg) % cfg.period_deg) + cfg.raw_min_deg
            _check_raw_range(raw_az)
        else:
            raw_az = float(raw_az)
            _check_raw_range(raw_az)
            raw_modulo = ((cfg.sign * raw_az + cfg.zero_offset_deg - cfg.raw_min_deg) % cfg.period_deg) + cfg.raw_min_deg
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


def main(argv=None) -> int:
    """Small CLI for manual Az unwrap state inspection/initialization."""
    import argparse

    parser = argparse.ArgumentParser(description="Inspect or initialize NECST Az unwrap state file.")
    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("status", help="show current configured state file")
    setp = sub.add_parser("set", help="write a manual unwrap state")
    def _deg_arg(text: str) -> float:
        return float(str(text).strip().removesuffix("deg").removesuffix("°"))

    setp.add_argument("--raw-az", type=_deg_arg, default=None, help="current absolute modulo raw Az [deg]")
    setp.add_argument("--continuous-az", type=_deg_arg, default=None, help="desired continuous Az [deg]")
    setp.add_argument("--branch", type=int, default=None, help="desired branch; requires --raw-az when --continuous-az is omitted")
    args = parser.parse_args(argv)

    cfg, state_path, _heartbeat, _state_warn_age, _state_raw_tolerance, mode, _startup_policy = load_az_unwrap_config()
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
        payload = _manual_state_payload(cfg, mode, raw_az=args.raw_az, continuous_az=args.continuous_az, branch=args.branch)
        writer = AtomicStateWriter(state_path)
        writer._write(payload)
        writer.close()
        print(f"wrote {state_path}: continuous={payload['continuous_az_deg']:.6f} deg branch={payload['branch']} raw={payload['raw_az_deg']:.6f} deg")
        return 0
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
