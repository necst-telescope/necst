import json
import os
import sys
import time
from abc import ABC, abstractmethod
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any, Generator, Optional, Union, final

import rclpy
from neclib import NECSTAuthorityError, get_logger
from neclib.coordinates import PointingError

from ... import config
from ...core import Commander
from .progress import NullProgressReporter, ObservationProgressReporter


class ObservationAbort(RuntimeError):
    """Raised when a running observation is asked to abort cooperatively."""


class Observation(ABC):
    r"""Observation runner.

    Parameters
    ----------
    record_name
        Record name. This will prefixed by auto-generated observation identifier
        ``necst_{start_datetime}_{observation_type}``.
    **kwargs
        Keyword arguments passed to :meth:`run`.

    Examples
    --------
    >>> obs = necst.procedures.Observation(...)
    >>> obs.execute()

    """

    observation_type: str
    target: Optional[str] = None

    def __init__(self, record_name: Optional[str] = None, /, **kwargs) -> None:
        try:
            self.telescope = os.environ.get("TELESCOPE")
            self.parameter_files = (
                f"{self.telescope}_config.toml",
                "pointing_param.toml",
                "device_setting.toml",
            )
        except KeyError:
            self.parameter_files = (
                "config.toml",
                "pointing_param.toml",
                "device_setting.toml",
            )
        self.logger = get_logger(self.__class__.__name__)
        self._record_suffix: Optional[str] = record_name
        self._record_qualname = None
        self._kwargs = kwargs
        self._start: Optional[float] = None
        self._abort_requested = False
        self._abort_reason = ""
        self._abort_request_id = ""
        self._abort_subscription = None
        self._legacy_binning_changed_specs = set()
        self.progress = NullProgressReporter()

    def _reset_abort_state(self) -> None:
        self._abort_requested = False
        self._abort_reason = ""
        self._abort_request_id = ""

    def _abort_exception(self) -> ObservationAbort:
        reason = self._abort_reason or "observation abort requested"
        if self._abort_request_id:
            reason = f"{reason} (request_id={self._abort_request_id})"
        return ObservationAbort(reason)

    def _handle_abort_request(self, msg) -> None:
        raw = getattr(msg, "data", "")
        reason = "operator requested abort"
        request_id = ""
        try:
            payload = json.loads(raw) if raw else {}
            if isinstance(payload, dict):
                reason = str(payload.get("reason") or reason)
                request_id = str(payload.get("request_id") or "")
        except Exception:
            reason = str(raw or reason)
        self._abort_requested = True
        self._abort_reason = reason
        self._abort_request_id = request_id
        self.logger.warning(
            "Observation abort requested"
            + (f" request_id={request_id}" if request_id else "")
            + f": {reason}"
        )
        try:
            self.progress.update(
                abort={
                    "requested": True,
                    "reason": reason,
                    "request_id": request_id,
                    "time_unix": time.time(),
                }
            )
            self.progress.event("abort_requested", reason=reason, request_id=request_id)
        except Exception:
            pass

    def _attach_abort_listener(self) -> None:
        try:
            from ... import topic

            self._abort_subscription = topic.observation_abort.subscription(
                self.com, self._handle_abort_request
            )
        except Exception as exc:
            self._abort_subscription = None
            self.logger.exception(f"Failed to attach observation-abort listener: {exc}")

    def _abort_requested_now(self) -> bool:
        return bool(self._abort_requested)

    def _raise_if_abort_requested(self) -> None:
        if self._abort_requested_now():
            raise self._abort_exception()

    def _sleep_with_abort(self, duration_sec: Union[int, float]) -> None:
        end = time.monotonic() + max(0.0, float(duration_sec))
        while True:
            self._raise_if_abort_requested()
            remaining = end - time.monotonic()
            if remaining <= 0:
                return
            time.sleep(min(0.1, remaining))

    def execute(self) -> None:
        self._start = time.time()
        run_completed = False
        with self.ros2env():
            self.com = Commander()
            self._reset_abort_state()
            self._attach_abort_listener()
            self.com.abort_checker = self._abort_requested_now
            self.com.abort_exception_factory = self._abort_exception
            self.progress = ObservationProgressReporter(
                observation_type=self.observation_type,
                record_name=self.record_name,
                obs_file=getattr(self, "_obs_file", None),
                target=getattr(self, "target", None),
                started_at_unix=self._start,
                logger=self.logger,
            )
            self.progress.attach_commander(self.com)
            privileged = self.com.get_privilege()
            try:
                if not privileged:
                    raise NECSTAuthorityError("Couldn't acquire privilege")
                self.progress.set_lifecycle("initializing")
                self._raise_if_abort_requested()
                self.before_record_controls()
                self._raise_if_abort_requested()
                if "save" in self._kwargs.keys():
                    savespec = self._kwargs.pop("save")
                    self.com.record("savespec", save=savespec)
                if "rate" in self._kwargs.keys():
                    conv_rate = int(self._kwargs.pop("rate") * 10)
                    self.com.record("reduce", nth=conv_rate)
                legacy_startup_allowed = self.allow_legacy_recording_startup_controls()
                if "ch" in self._kwargs.keys():
                    ch = self._kwargs.pop("ch")
                    if legacy_startup_allowed:
                        specnames = set(
                            [val.split(".")[0] for val in config.spectrometer.keys()]
                        )
                        for spec_name in specnames:
                            self.binning(ch, spec_name)
                        if ch is not None:
                            self._legacy_binning_changed_specs.update(specnames)
                    elif ch is not None:
                        raise ValueError(
                            "spectral recording setup cannot be combined with legacy "
                            f"channel binning ch={ch!r}"
                        )
                if "tp_mode" in self._kwargs or "tp_range" in self._kwargs:
                    tp_range = self._kwargs.pop("tp_range", None)
                    tp_mode = self._kwargs.pop("tp_mode", False)
                    if legacy_startup_allowed:
                        if tp_mode or tp_range:
                            self.com.record(
                                "tp_mode", tp_mode=True, tp_range=tp_range or []
                            )
                        else:
                            self.com.record("tp_mode", tp_mode=False)
                    elif tp_mode or tp_range:
                        raise ValueError(
                            "spectral recording setup cannot be combined with legacy "
                            f"tp_mode={tp_mode!r}, tp_range={tp_range!r}"
                        )
                self.progress.set_lifecycle("recording_starting")
                self._raise_if_abort_requested()
                self.com.metadata("set", position="", id="")
                self.com.record("start", name=self.record_name)
                self.record_parameter_files()
                self.after_record_start()
                self._raise_if_abort_requested()
                self.progress.set_lifecycle("running")
                rclpy.uninstall_signal_handlers()
                # if hasattr(config, "dome"):
                #    self.com.dome("sync", dome_sync=True)
                #    self.com.dome("open")
                #    self.logger.info("Dome opened")
                self._raise_if_abort_requested()
                self.run(**self._kwargs)
                self._raise_if_abort_requested()
                run_completed = True
            finally:
                cleanup_errors = []
                original_exc = sys.exc_info()[1]
                if original_exc is None and run_completed:
                    self.progress.set_lifecycle("cleanup")
                elif isinstance(original_exc, (KeyboardInterrupt, ObservationAbort)):
                    self.progress.set_lifecycle("aborted", error=repr(original_exc))
                elif original_exc is not None:
                    self.progress.set_lifecycle("error", error=repr(original_exc))

                def _cleanup_step(label, func):
                    try:
                        return func()
                    except (
                        Exception
                    ) as exc:  # pragma: no cover - hardware/ROS dependent cleanup
                        cleanup_errors.append((label, exc))
                        self.logger.exception(f"Cleanup step failed: {label}: {exc}")
                        return None

                _cleanup_step("save progress sidecars", self.progress.record_sidecars)
                _cleanup_step("before_record_stop", self.before_record_stop)

                record_stop_ok = False
                try:
                    self.com.record("stop")
                    record_stop_ok = True
                except (
                    Exception
                ) as exc:  # pragma: no cover - hardware/ROS dependent cleanup
                    cleanup_errors.append(("record stop", exc))
                    self.logger.exception(f"Cleanup step failed: record stop: {exc}")

                if record_stop_ok:
                    _cleanup_step("after_record_stop", self.after_record_stop)
                else:
                    self.logger.error(
                        "Recorder stop failed; active spectral recording setup, if any, "
                        "is intentionally not cleared to avoid reopening legacy recording "
                        "while recorder state is uncertain."
                    )

                legacy_cleanup_allowed = self.allow_legacy_recording_cleanup_controls()
                if legacy_cleanup_allowed:
                    _cleanup_step(
                        "reset legacy tp_mode",
                        lambda: self.com.record("tp_mode", tp_mode=False, tp_range=[]),
                    )
                else:
                    self.logger.error(
                        "Skipping legacy tp_mode reset because an active spectral recording setup "
                        "may still be present. This avoids sending legacy controls while recorder "
                        "state is uncertain."
                    )

                _cleanup_step(
                    "reset savespec", lambda: self.com.record("savespec", save=True)
                )
                _cleanup_step("antenna stop", lambda: self.com.antenna("stop"))

                def _reset_binning():
                    changed_specs = set(self._legacy_binning_changed_specs)
                    for _key, val in config.spectrometer.items():
                        spec_name, key = _key.split(".", 1)
                        if (key == "max_ch") and (spec_name in changed_specs):
                            self.binning(val, spec_name)  # set max channel number
                    self._legacy_binning_changed_specs.difference_update(changed_specs)

                if legacy_cleanup_allowed:
                    _cleanup_step("reset channel binning", _reset_binning)
                else:
                    self.logger.error(
                        "Skipping legacy channel-binning reset because an active spectral recording "
                        "setup may still be present. Manual recovery/clear is required after the "
                        "recorder state is known."
                    )
                # if hasattr(config, "dome"):
                #    self.com.dome("close")
                #    self.logger.info("Dome closed")
                #    self.com.dome("sync", dome_sync=False)
                if cleanup_errors and original_exc is None:
                    labels = ", ".join(label for label, _ in cleanup_errors)
                    self.progress.set_lifecycle(
                        "error", error=f"cleanup failed: {labels}"
                    )
                elif original_exc is None and run_completed:
                    self.progress.set_lifecycle("finished")
                # Try once more after the final lifecycle state is written.  The
                # earlier sidecar save runs before recorder stop so at least a
                # cleanup/error snapshot can be captured while recording is active;
                # this second best-effort save makes the live/final state available
                # to record sidecars too when the recorder still accepts file saves.
                _cleanup_step(
                    "save final progress sidecars", self.progress.record_sidecars
                )
                _cleanup_step(
                    "copy final progress sidecars to local record directory",
                    lambda: self.progress.copy_sidecars_to_local_record_dir(
                        record_name=self.record_name
                    ),
                )
                _cleanup_step(
                    "detach abort checker",
                    lambda: setattr(self.com, "abort_checker", None),
                )
                _cleanup_step("quit privilege", self.com.quit_privilege)
                _cleanup_step("destroy commander", self.com.destroy_node)
                _observing_duration = (time.time() - self._start) / 60
                self.logger.info(
                    f"Observation finished, took {_observing_duration:.2f} min."
                )
                self.logger.info(f"Record name: \033[1m{self.record_name!r}\033[0m")
                _cleanup_step("install signal handlers", rclpy.install_signal_handlers)
                if isinstance(original_exc, ObservationAbort):
                    self.logger.warning(f"Observation aborted: {original_exc}")
                    raise SystemExit(130) from None
                if isinstance(original_exc, KeyboardInterrupt):
                    self.logger.warning("Observation aborted by KeyboardInterrupt.")
                    raise SystemExit(130) from None
                if cleanup_errors and original_exc is None:
                    labels = ", ".join(label for label, _ in cleanup_errors)
                    raise RuntimeError(
                        f"Observation cleanup failed in step(s): {labels}"
                    ) from cleanup_errors[0][1]

    @property
    def start_datetime(self) -> Optional[str]:
        if self._start is None:
            return None
        if not hasattr(self, "_start_datetime"):
            dt = datetime.fromtimestamp(self._start)
            self._start_datetime = dt.strftime(r"%Y%m%d_%H%M%S")
        return self._start_datetime

    @contextmanager
    def ros2env(self) -> Generator[None, None, None]:
        should_shutdown = not rclpy.ok()
        if should_shutdown:
            rclpy.init()
        try:
            yield
        finally:
            if should_shutdown:
                rclpy.shutdown()

    @final
    @property
    def record_name(self) -> str:
        if self._record_qualname is None:
            obstype = self.observation_type
            self._record_qualname = f"necst_{obstype}_{self.start_datetime}"
            if self._record_suffix:
                self._record_qualname += f"_{self._record_suffix}"
        return self._record_qualname.lower()

    def before_record_controls(self) -> None:
        """Hook after privilege acquisition and before legacy record controls."""

    def after_record_start(self) -> None:
        """Hook after recorder start and default parameter-file sidecars."""

    def before_record_stop(self) -> None:
        """Hook before recorder stop in the cleanup path.

        Implementations must not clear active spectral-recording setup here,
        because the recorder may still be running until ``record("stop")``
        returns.  Use :meth:`after_record_stop` for deactivation that is safe
        only after recorder stop.
        """

    def after_record_stop(self) -> None:
        """Hook after recorder stop in the cleanup path."""

    def allow_legacy_recording_cleanup_controls(self) -> bool:
        """Whether cleanup may send legacy TP/binning reset commands.

        File-based spectral-recording setup observations override this to prevent
        legacy controls from being sent while an active setup remains uncleared
        after an uncertain recorder-stop failure.
        """
        return True

    def allow_legacy_recording_startup_controls(self) -> bool:
        """Whether startup may send legacy recorder-control commands.

        File-based spectral-recording setup mode overrides this after setup
        application so inactive CLI defaults such as ``ch=None`` or
        ``tp_mode=False`` are consumed without sending old recorder controls.
        """
        return True

    def record_parameter_files(self) -> None:
        root = os.environ.get("NECST_ROOT", Path.home() / ".necst")
        for filename in self.parameter_files:
            try:
                path = f"{str(root).rstrip('/')}/{filename.lstrip('/')}"
                self.com.record("file", name=path)
            except FileNotFoundError:
                self.logger.error(f"Failed to save parameter file {filename!r}")

    @abstractmethod
    def run(self, *args, **kwargs) -> None: ...

    def hot(
        self,
        integ_time: Union[int, float],
        id: Any,
        *,
        preserve_tracking: bool = False,
        phase: str = "HOT",
        location_context: Optional[str] = None,
        geometry: Optional[dict] = None,
    ) -> None:
        # TODO: Remove this workaround, by attaching control section ID to spectra
        # metadata command; if it's "", don't require tight control
        # This will use SpectralMetadata.srv
        #
        # When preserve_tracking=True, the caller guarantees that the antenna has
        # already arrived at the desired calibration point and settled there. In
        # that case, HOT must not issue any extra antenna command because the
        # following HOT->OFF pair is intended to run on exactly the same tracking
        # solution and sky position.
        if (not preserve_tracking) and (
            not self.com.get_message("antenna_control").tight
        ):
            enc = self.com.get_message("encoder")
            params = PointingError.from_file(config.antenna_pointing_parameter_path)
            az, el = params.apparent_to_refracted(az=enc.lon, el=enc.lat, unit="deg")
            self.com.antenna(
                "point", target=(az.value, el.value, "altaz"), unit="deg", wait=False
            )

        self.logger.info("Starting HOT...")
        with self.progress.integration(
            phase=phase,
            metadata_position="HOT",
            id=id,
            location_context=location_context,
            geometry=geometry,
            integ_time_sec=float(integ_time),
        ):
            self.com.chopper("insert")
            self.com.metadata("set", position="HOT", id=str(id))
            self._sleep_with_abort(integ_time)
            self.com.metadata("set", position="", id=str(id))
            self.com.chopper("remove")
        self.logger.debug("Complete HOT")

    def sky(
        self,
        integ_time: Union[int, float],
        id: Any,
        *,
        phase: str = "SKY",
        location_context: Optional[str] = None,
        geometry: Optional[dict] = None,
    ) -> None:
        self.logger.info("Starting SKY...")
        with self.progress.integration(
            phase=phase,
            metadata_position="SKY",
            id=id,
            location_context=location_context,
            geometry=geometry,
            integ_time_sec=float(integ_time),
        ):
            self.com.chopper("remove")
            self.com.metadata("set", position="SKY", id=str(id))
            self._sleep_with_abort(integ_time)
            self.com.metadata("set", position="", id=str(id))
        self.logger.debug("Complete SKY")

    def off(
        self,
        integ_time: Union[int, float],
        id: Any,
        *,
        phase: str = "OFF",
        location_context: Optional[str] = None,
        geometry: Optional[dict] = None,
    ) -> None:
        self.logger.info("Starting OFF...")
        with self.progress.integration(
            phase=phase,
            metadata_position="OFF",
            id=id,
            location_context=location_context,
            geometry=geometry,
            integ_time_sec=float(integ_time),
        ):
            self.com.chopper("remove")
            self.com.metadata("set", position="OFF", id=str(id))
            self._sleep_with_abort(integ_time)
            self.com.metadata("set", position="", id=str(id))
        self.logger.debug("Complete OFF")

    def on(
        self,
        integ_time: Union[int, float],
        id: Any,
        *,
        phase: str = "ON",
        location_context: Optional[str] = None,
        geometry: Optional[dict] = None,
    ) -> None:
        self.logger.info("Starting ON...")
        with self.progress.integration(
            phase=phase,
            metadata_position="ON",
            id=id,
            location_context=location_context,
            geometry=geometry,
            integ_time_sec=float(integ_time),
        ):
            self.com.chopper("remove")
            self.com.metadata("set", position="ON", id=str(id))
            self._sleep_with_abort(integ_time)
            self.com.metadata("set", position="", id=str(id))
        self.logger.debug("Complete ON")

    def binning(self, ch, spectrometer):
        if ch is not None:
            if (ch & (ch - 1)) == 0:
                self.com.record("binning", ch=ch, spectrometer=spectrometer)
            else:
                raise ValueError(f"Input channel number {ch} is not power of 2.")
