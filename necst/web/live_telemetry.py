"""Best-effort live ROS telemetry cache for the Operator Console.

This module is intentionally read-only.  It subscribes to NECST status topics
and exposes a compact dictionary that can be merged by ``status_model``.  It is
safe to import outside a ROS environment; ROS imports happen only when the cache
is started.
"""

from __future__ import annotations

import math
import sys
import threading
from pathlib import Path
from typing import Any, Dict, Optional


def _finite_float(value: Any) -> Optional[float]:
    try:
        out = float(value)
    except Exception:
        return None
    return out if math.isfinite(out) else None


def _msg_coord(msg: Any, *, prefix: str) -> Dict[str, Any]:
    """Convert common NECST coordinate messages to status_model field names."""

    lon = _finite_float(getattr(msg, "lon", None))
    lat = _finite_float(getattr(msg, "lat", None))
    if lon is None:
        lon = _finite_float(getattr(msg, "az", None))
    if lat is None:
        lat = _finite_float(getattr(msg, "el", None))
    if lon is None:
        lon = _finite_float(getattr(msg, "az_deg", None))
    if lat is None:
        lat = _finite_float(getattr(msg, "el_deg", None))

    payload: Dict[str, Any] = {}
    if lon is not None:
        payload[f"{prefix}_lon_deg"] = lon
    if lat is not None:
        payload[f"{prefix}_lat_deg"] = lat
    frame = getattr(msg, "frame", None)
    unit = getattr(msg, "unit", None)
    name = getattr(msg, "name", None)
    ts = _finite_float(getattr(msg, "time", None))
    if frame not in (None, ""):
        payload[f"{prefix}_frame"] = str(frame)
    if unit not in (None, ""):
        payload[f"{prefix}_unit"] = str(unit)
    if name not in (None, ""):
        payload[f"{prefix}_name"] = str(name)
    if ts is not None:
        payload[f"{prefix}_time_unix"] = ts
    return payload


class LiveTelemetryCache:
    """Read-only ROS status cache for current antenna/chopper console display."""

    def __init__(self, *, enabled: bool = True, node_name: str = "necst_operator_console_status") -> None:
        self.requested = bool(enabled)
        self.available = False
        self.error: Optional[str] = None
        self.node_name = str(node_name)
        self._rclpy = None
        self._node = None
        self._executor = None
        self._spin_thread: Optional[threading.Thread] = None
        self._owns_context = False
        self._lock = threading.RLock()
        self._spin_mode = "disabled"

        self.command: Dict[str, Any] = {}
        self.encoder: Dict[str, Any] = {}
        self.tracking: Dict[str, Any] = {}
        self.pointing_status: Dict[str, Any] = {}
        self.az_unwrap_status: Dict[str, Any] = {}
        self.queue_status: Dict[str, Any] = {}
        self.chopper_status: Dict[str, Any] = {}
        self.spectrometer_status: Dict[str, Any] = {}
        self.weather: Dict[str, Dict[str, Any]] = {}

        if self.requested:
            self._start()

    def _start(self) -> None:
        try:
            repo_root = str(Path(__file__).resolve().parents[2])
            if repo_root not in sys.path:
                sys.path.insert(0, repo_root)
            import rclpy  # type: ignore
            from necst import config as necst_config  # type: ignore
            from necst.definitions import topic  # type: ignore

            self._rclpy = rclpy
            self._owns_context = not rclpy.ok()
            if self._owns_context:
                rclpy.init(args=None)
            self._node = rclpy.create_node(self.node_name)

            def command_cb(msg: Any) -> None:
                with self._lock:
                    self.command = _msg_coord(msg, prefix="command")

            def encoder_cb(msg: Any) -> None:
                with self._lock:
                    self.encoder = _msg_coord(msg, prefix="encoder")

            def tracking_cb(msg: Any) -> None:
                with self._lock:
                    self.tracking = {
                        "ok": bool(getattr(msg, "ok", False)),
                        "error_deg": _finite_float(getattr(msg, "error", None)),
                        "time_unix": _finite_float(getattr(msg, "time", None)),
                    }

            def pointing_status_cb(msg: Any) -> None:
                with self._lock:
                    self.pointing_status = {
                        "valid": bool(getattr(msg, "valid", False)),
                        "publish_time_unix": _finite_float(getattr(msg, "publish_time_unix", None)),
                        "command_time_unix": _finite_float(getattr(msg, "command_time_unix", None)),
                        "encoder_time_unix": _finite_float(getattr(msg, "encoder_time_unix", None)),
                        "cmd_az_deg": _finite_float(getattr(msg, "cmd_az_deg", None)),
                        "cmd_el_deg": _finite_float(getattr(msg, "cmd_el_deg", None)),
                        "enc_az_deg": _finite_float(getattr(msg, "enc_az_deg", None)),
                        "enc_el_deg": _finite_float(getattr(msg, "enc_el_deg", None)),
                        "delta_az_deg": _finite_float(getattr(msg, "delta_az_deg", None)),
                        "delta_el_deg": _finite_float(getattr(msg, "delta_el_deg", None)),
                        "delta_az_cos_el_deg": _finite_float(getattr(msg, "delta_az_cos_el_deg", None)),
                        "tracking_error_deg": _finite_float(getattr(msg, "tracking_error_deg", None)),
                        "tracking_threshold_deg": _finite_float(getattr(msg, "tracking_threshold_deg", None)),
                        "tracking_ok": bool(getattr(msg, "tracking_ok", False)),
                        "cmd_age_sec": _finite_float(getattr(msg, "cmd_age_sec", None)),
                        "enc_age_sec": _finite_float(getattr(msg, "enc_age_sec", None)),
                        "command_stale": bool(getattr(msg, "command_stale", False)),
                        "encoder_stale": bool(getattr(msg, "encoder_stale", False)),
                        "basis": str(getattr(msg, "basis", "")),
                    }

            def az_unwrap_status_cb(msg: Any) -> None:
                with self._lock:
                    self.az_unwrap_status = {
                        "enabled": bool(getattr(msg, "enabled", False)),
                        "valid": bool(getattr(msg, "valid", False)),
                        "mode": str(getattr(msg, "mode", "")),
                        "state": str(getattr(msg, "state", "")),
                        "reason": str(getattr(msg, "reason", "")),
                        "publish_time_unix": _finite_float(getattr(msg, "publish_time_unix", None)),
                        "encoder_time_unix": _finite_float(getattr(msg, "encoder_time_unix", None)),
                        "raw_az_deg": _finite_float(getattr(msg, "raw_az_deg", None)),
                        "modulo_az_deg": _finite_float(getattr(msg, "modulo_az_deg", None)),
                        "continuous_az_deg": _finite_float(getattr(msg, "continuous_az_deg", None)),
                        "branch": int(getattr(msg, "branch", 0)),
                        "state_age_sec": _finite_float(getattr(msg, "state_age_sec", None)),
                    }

            def queue_status_cb(msg: Any) -> None:
                with self._lock:
                    self.queue_status = {
                        "active": bool(getattr(msg, "active", False)),
                        "guard_latched": bool(getattr(msg, "guard_latched", False)),
                        "queue_depth": int(getattr(msg, "queue_depth", 0)),
                        "time_unix": _finite_float(getattr(msg, "time", None)),
                    }

            def spectrometer_status_cb(msg: Any) -> None:
                with self._lock:
                    self.spectrometer_status = {
                        "status": str(getattr(msg, "status", "")),
                        "time_src": str(getattr(msg, "time_src", "")),
                        "time_unix": _finite_float(getattr(msg, "time", None)),
                    }

            def chopper_status_cb(msg: Any) -> None:
                position = _finite_float(getattr(msg, "position", None))
                try:
                    insert = bool(getattr(msg, "insert"))
                except Exception:
                    insert = None
                try:
                    insert_position = int(necst_config.chopper_motor_position["insert"])
                    remove_position = int(necst_config.chopper_motor_position["remove"])
                except Exception:
                    insert_position = None
                    remove_position = None
                try:
                    simulator_boolean_only = bool(getattr(necst_config, "simulator", False))
                except Exception:
                    simulator_boolean_only = False

                if position is not None and insert_position is not None and int(position) == insert_position and insert is not False:
                    state = "in"
                elif position is not None and remove_position is not None and int(position) == remove_position and insert is not True:
                    state = "out"
                elif simulator_boolean_only and position == 0 and insert is True:
                    state = "in"
                elif simulator_boolean_only and position == 0 and insert is False:
                    state = "out"
                elif insert is True:
                    state = "in?"
                elif insert is False:
                    state = "out?"
                else:
                    state = "unknown"
                with self._lock:
                    self.chopper_status = {
                        "insert": insert,
                        "state": state,
                        "position": position,
                        "time_unix": _finite_float(getattr(msg, "time", None)),
                    }

            def weather_cb(key: str):
                def _callback(msg: Any) -> None:
                    payload = {
                        "temperature_k": _finite_float(getattr(msg, "temperature", None)),
                        "pressure_hpa": _finite_float(getattr(msg, "pressure", None)),
                        "humidity_percent": _finite_float(getattr(msg, "humidity", None)),
                        "wind_speed_mps": _finite_float(getattr(msg, "wind_speed", None)),
                        "wind_direction_deg": _finite_float(getattr(msg, "wind_direction", None)),
                        "time_unix": _finite_float(getattr(msg, "time", None)),
                    }
                    with self._lock:
                        self.weather[key] = payload

                return _callback

            topic.altaz_cmd.subscription(self._node, command_cb)
            topic.antenna_encoder.subscription(self._node, encoder_cb)
            try:
                topic.antenna_tracking.subscription(self._node, tracking_cb)
            except Exception:
                pass
            for topic_obj, callback in (
                (getattr(topic, "antenna_pointing_status", None), pointing_status_cb),
                (getattr(topic, "antenna_az_unwrap_status", None), az_unwrap_status_cb),
                (getattr(topic, "antenna_command_queue_status", None), queue_status_cb),
                (getattr(topic, "spectrometer_status", None), spectrometer_status_cb),
                (getattr(topic, "chopper_status", None), chopper_status_cb),
            ):
                try:
                    if topic_obj is not None:
                        topic_obj.subscription(self._node, callback)
                except Exception:
                    pass
            try:
                topic.weather["out"].subscription(self._node, weather_cb("out"))
                topic.weather["in"].subscription(self._node, weather_cb("in"))
            except Exception:
                pass
            self.available = True
            self._start_background_spin()
        except Exception as exc:
            self.available = False
            self.error = str(exc)
            try:
                if self._node is not None:
                    self._node.destroy_node()
            except Exception:
                pass
            self._node = None

    def _start_background_spin(self) -> None:
        if not (self.available and self._rclpy is not None and self._node is not None):
            return
        try:
            from rclpy.executors import MultiThreadedExecutor  # type: ignore

            executor = MultiThreadedExecutor(num_threads=2)
            executor.add_node(self._node)
            self._executor = executor
            self._spin_mode = "background-executor"
            self._spin_thread = threading.Thread(
                target=executor.spin,
                name="necst-console-ros-spin",
                daemon=True,
            )
            self._spin_thread.start()
        except Exception as exc:
            self._executor = None
            self._spin_thread = None
            self._spin_mode = "request-spin-fallback"
            self.error = f"background ROS spin disabled: {exc}"

    def spin_once(self, timeout_sec: float = 0.0) -> None:
        if not (self.available and self._rclpy is not None and self._node is not None):
            return
        if self._executor is not None:
            return
        try:
            self._rclpy.spin_once(self._node, timeout_sec=timeout_sec)
        except Exception as exc:
            self.error = str(exc)
            self.available = False

    def snapshot(self) -> Dict[str, Any]:
        if self.available:
            self.spin_once(timeout_sec=0.0)
        with self._lock:
            payload: Dict[str, Any] = {
                "requested": self.requested,
                "available": self.available,
                "spin_mode": self._spin_mode,
            }
            if self.error:
                payload["error"] = self.error
            if self.command:
                payload["command"] = dict(self.command)
            if self.encoder:
                payload["encoder"] = dict(self.encoder)
            if self.tracking:
                payload["tracking"] = dict(self.tracking)
            if self.pointing_status:
                payload["pointing_status"] = dict(self.pointing_status)
            if self.az_unwrap_status:
                payload["az_unwrap_status"] = dict(self.az_unwrap_status)
            if self.queue_status:
                payload["queue_status"] = dict(self.queue_status)
            if self.chopper_status:
                payload["chopper_status"] = dict(self.chopper_status)
            if self.spectrometer_status:
                payload["spectrometer_status"] = dict(self.spectrometer_status)
            if self.weather:
                payload["weather"] = {key: dict(value) for key, value in self.weather.items()}
            return payload

    def close(self) -> None:
        try:
            if self._executor is not None:
                self._executor.shutdown()
        except Exception:
            pass
        try:
            if self._spin_thread is not None and self._spin_thread.is_alive():
                self._spin_thread.join(timeout=1.0)
        except Exception:
            pass
        try:
            if self._node is not None:
                self._node.destroy_node()
        except Exception:
            pass
        try:
            if self._owns_context and self._rclpy is not None and self._rclpy.ok():
                self._rclpy.shutdown()
        except Exception:
            pass
