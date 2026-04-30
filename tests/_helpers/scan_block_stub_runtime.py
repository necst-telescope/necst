from __future__ import annotations

import importlib.util
from collections import defaultdict
import sys
import types
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace
from typing import Iterable, Tuple

import numpy as np


class FakeUnit:
    def __init__(self, name: str):
        self.name = name

    def __rmul__(self, value):
        return FakeQuantity(value, self.name)

    def __mul__(self, value):
        return FakeQuantity(value, self.name)

    def __truediv__(self, other):
        other_name = getattr(other, "name", str(other))
        return FakeUnit(f"{self.name}/{other_name}")

    def __repr__(self):
        return self.name


class FakeQuantity(np.ndarray):
    __array_priority__ = 1000

    def __new__(cls, value=0.0, unit: str = ""):
        obj = np.asarray(value, dtype=float).view(cls)
        obj.unit = unit
        return obj

    def __array_finalize__(self, obj):
        self.unit = getattr(obj, "unit", "")

    def to_value(self, unit=None):
        arr = np.asarray(self)
        if arr.shape == ():
            return float(arr)
        return arr.astype(float)

    def to(self, unit=None):
        return FakeQuantity(np.asarray(self), getattr(unit, "name", unit) or self.unit)

    def decompose(self):
        return self

    def __getitem__(self, item):
        out = super().__getitem__(item)
        if isinstance(out, np.ndarray):
            return out.view(FakeQuantity)
        return FakeQuantity(out, self.unit)

    def __mul__(self, other):
        if isinstance(other, FakeUnit):
            return FakeQuantity(np.asarray(self), f"{self.unit}*{other.name}")
        return super().__mul__(other)

    def __truediv__(self, other):
        if isinstance(other, FakeUnit):
            return FakeQuantity(np.asarray(self), f"{self.unit}/{other.name}")
        return super().__truediv__(other)


class FakeLogger:
    def __init__(self):
        self.infos = []
        self.warnings = []

    def info(self, msg, *args, **kwargs):
        self.infos.append(str(msg))

    def warning(self, msg, *args, **kwargs):
        self.warnings.append(str(msg))


def q(value: float, unit: str = "deg") -> FakeQuantity:
    return FakeQuantity(value, unit)


def clear_modules(prefixes: Iterable[str]) -> None:
    prefixes = tuple(prefixes)
    for name in list(sys.modules):
        if name.startswith(prefixes):
            del sys.modules[name]


@dataclass(frozen=True)
class StubScanBlockLine:
    start: Tuple[FakeQuantity, FakeQuantity]
    stop: Tuple[FakeQuantity, FakeQuantity]
    speed: object
    margin: object
    label: str = ""
    line_index: int = -1


class StubObservationMode:
    ON = "ON"
    HOT = "HOT"
    OFF = "OFF"
    SKY = "SKY"


class DummyObservation:
    def __init__(self, *args, **kwargs):
        pass


def _load_source(module_name: str, file_path: Path):
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def install_astropy_stub() -> None:
    astropy = types.ModuleType("astropy")
    astropy.__path__ = []
    units = types.ModuleType("astropy.units")
    units.Quantity = FakeQuantity
    units.Unit = FakeUnit
    units.deg = FakeUnit("deg")
    units.s = FakeUnit("s")
    units.GHz = FakeUnit("GHz")
    units.dimensionless_unscaled = FakeUnit("")
    sys.modules["astropy"] = astropy
    sys.modules["astropy.units"] = units


def load_file_based_module(repo_root: Path):
    clear_modules(["necst", "neclib", "astropy"])

    necst = types.ModuleType("necst")
    necst.__path__ = []
    sys.modules["necst"] = necst

    procedures = types.ModuleType("necst.procedures")
    procedures.__path__ = []
    sys.modules["necst.procedures"] = procedures

    observations_pkg = types.ModuleType("necst.procedures.observations")
    observations_pkg.__path__ = []
    sys.modules["necst.procedures.observations"] = observations_pkg

    observation_base = types.ModuleType(
        "necst.procedures.observations.observation_base"
    )
    observation_base.Observation = DummyObservation
    sys.modules["necst.procedures.observations.observation_base"] = observation_base

    cfg = types.ModuleType("necst.config")
    cfg.antenna = SimpleNamespace(scan_margin=SimpleNamespace(value=0.1))
    sys.modules["necst.config"] = cfg
    necst.config = cfg

    neclib = types.ModuleType("neclib")
    neclib.__path__ = []
    sys.modules["neclib"] = neclib

    coordinates = types.ModuleType("neclib.coordinates")
    coordinates.__path__ = []
    coordinates.ScanBlockLine = StubScanBlockLine
    sys.modules["neclib.coordinates"] = coordinates

    observations = types.ModuleType("neclib.coordinates.observations")
    observations.GridSpec = object
    observations.OTFSpec = object
    observations.PSWSpec = object
    observations.RadioPointingSpec = object
    sys.modules["neclib.coordinates.observations"] = observations

    obs_base = types.ModuleType("neclib.coordinates.observations.observation_spec_base")
    obs_base.ObservationMode = StubObservationMode
    obs_base.ObservationSpec = object
    sys.modules["neclib.coordinates.observations.observation_spec_base"] = obs_base

    paths = types.ModuleType("neclib.coordinates.paths")
    paths.build_scan_block_sections = lambda *a, **k: ["DUMMY_SECTION"]
    paths.margin_start_of = lambda line: line.start
    paths.plan_scan_block_kinematics = lambda lines: {
        "limits": types.SimpleNamespace(max_acceleration=q(1.6, "deg/s^2")),
        "lines": [],
        "turns": [],
    }
    sys.modules["neclib.coordinates.paths"] = paths

    return _load_source(
        "necst.procedures.observations.file_based",
        repo_root / "necst" / "procedures" / "observations" / "file_based.py",
    )


def load_horizontal_coord_module(repo_root: Path):
    clear_modules(["necst", "neclib", "necst_msgs", "astropy", "rclpy"])
    install_astropy_stub()

    necst = types.ModuleType("necst")
    necst.__path__ = []
    sys.modules["necst"] = necst

    ctrl = types.ModuleType("necst.ctrl")
    ctrl.__path__ = []
    sys.modules["necst.ctrl"] = ctrl

    antenna = types.ModuleType("necst.ctrl.antenna")
    antenna.__path__ = []
    sys.modules["necst.ctrl.antenna"] = antenna

    core = types.ModuleType("necst.core")

    class AlertHandlerNode:
        pass

    core.AlertHandlerNode = AlertHandlerNode
    sys.modules["necst.core"] = core

    cfg = types.ModuleType("necst.config")
    cfg.location = object()
    cfg.antenna_pointing_parameter_path = "dummy.toml"
    cfg.antenna_drive = SimpleNamespace(
        critical_limit_az=0,
        warning_limit_az=0,
        critical_limit_el=0,
        warning_limit_el=0,
    )
    cfg.antenna_command_frequency = 10.0
    sys.modules["necst.config"] = cfg
    necst.config = cfg

    namespace = types.ModuleType("necst.namespace")
    namespace.antenna = "antenna"
    sys.modules["necst.namespace"] = namespace
    necst.namespace = namespace

    class DummyEndpoint:
        def publisher(self, *args, **kwargs):
            return SimpleNamespace(publish=lambda *a, **k: None)

        def subscription(self, *args, **kwargs):
            return None

        def service(self, *args, **kwargs):
            return None

    service = types.ModuleType("necst.service")
    service.raw_coord = DummyEndpoint()
    service.scan_block = DummyEndpoint()
    sys.modules["necst.service"] = service
    necst.service = service

    topic = types.ModuleType("necst.topic")
    topic.altaz_cmd = DummyEndpoint()
    topic.antenna_encoder = DummyEndpoint()
    topic.weather = DummyEndpoint()
    topic.antenna_cmd_transition = DummyEndpoint()
    topic.antenna_control_status = DummyEndpoint()
    sys.modules["necst.topic"] = topic
    necst.topic = topic

    neclib = types.ModuleType("neclib")
    neclib.__path__ = []
    sys.modules["neclib"] = neclib

    coords = types.ModuleType("neclib.coordinates")
    coords.__path__ = []

    class CoordinateGeneratorManager:
        def clear(self):
            pass

        def attach(self, generator):
            self.generator = generator

    class DriveLimitChecker:
        def __init__(self, *a, **k):
            pass

    class PathFinder:
        def __init__(self, *a, **k):
            pass

    class FinderScanBlockSection:
        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)

    coords.CoordinateGeneratorManager = CoordinateGeneratorManager
    coords.DriveLimitChecker = DriveLimitChecker
    coords.PathFinder = PathFinder
    coords.ScanBlockSection = FinderScanBlockSection
    sys.modules["neclib.coordinates"] = coords

    coords_paths = types.ModuleType("neclib.coordinates.paths")

    class ControlContext:
        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)

    coords_paths.ControlContext = ControlContext
    sys.modules["neclib.coordinates.paths"] = coords_paths

    necst_msgs = types.ModuleType("necst_msgs")
    necst_msgs.__path__ = []
    sys.modules["necst_msgs"] = necst_msgs

    msgs = types.ModuleType("necst_msgs.msg")

    class Boolean:
        def __init__(self, data=False):
            self.data = data

    class ControlStatus:
        pass

    class CoordMsg:
        pass

    class WeatherMsg:
        pass

    class ScanBlockSection:
        MOVE_TO_ENTRY = 0
        FIRST_STANDBY = 1
        ACCELERATE = 2
        LINE = 3
        TURN = 4
        DECELERATE = 5
        FINAL_STANDBY = 6

    msgs.Boolean = Boolean
    msgs.ControlStatus = ControlStatus
    msgs.CoordMsg = CoordMsg
    msgs.ScanBlockSection = ScanBlockSection
    msgs.WeatherMsg = WeatherMsg
    sys.modules["necst_msgs.msg"] = msgs

    srv = types.ModuleType("necst_msgs.srv")

    class CoordinateCommand:
        class Request:
            pass

        class Response:
            pass

    class ScanBlockCommand:
        class Request:
            pass

        class Response:
            pass

    srv.CoordinateCommand = CoordinateCommand
    srv.ScanBlockCommand = ScanBlockCommand
    sys.modules["necst_msgs.srv"] = srv

    rclpy = types.ModuleType("rclpy")
    rclpy.__path__ = []
    sys.modules["rclpy"] = rclpy

    return _load_source(
        "necst.ctrl.antenna.horizontal_coord",
        repo_root / "necst" / "ctrl" / "antenna" / "horizontal_coord.py",
    )


def load_commander_module(repo_root: Path):
    clear_modules(["necst", "neclib", "necst_msgs", "rclpy"])

    neclib = types.ModuleType("neclib")
    neclib.__path__ = []
    sys.modules["neclib"] = neclib

    neclib_core = types.ModuleType("neclib.core")
    neclib_core.read = lambda *a, **k: {}
    sys.modules["neclib.core"] = neclib_core

    neclib_utils = types.ModuleType("neclib.utils")

    class ConditionChecker:
        def __init__(self, *a, **k):
            pass

    class ParameterList(list):
        pass

    neclib_utils.ConditionChecker = ConditionChecker
    neclib_utils.ParameterList = ParameterList
    sys.modules["neclib.utils"] = neclib_utils

    necst = types.ModuleType("necst")
    necst.__path__ = []
    necst.NECSTTimeoutError = type("NECSTTimeoutError", (Exception,), {})
    sys.modules["necst"] = necst

    cfg = types.ModuleType("necst.config")
    cfg.dome_command_frequency = 1.0
    sys.modules["necst.config"] = cfg
    necst.config = cfg

    namespace = types.ModuleType("necst.namespace")
    namespace.core = "core"
    namespace.dome = "dome"
    sys.modules["necst.namespace"] = namespace
    necst.namespace = namespace

    class DummyEndpoint:
        def publisher(self, *args, **kwargs):
            return SimpleNamespace(publish=lambda *a, **k: None)

        def subscription(self, *args, **kwargs):
            return None

        def client(self, *args, **kwargs):
            return None

        def service(self, *args, **kwargs):
            return None

    service = types.ModuleType("necst.service")
    service.scan_block = DummyEndpoint()
    service.raw_coord = DummyEndpoint()
    service.dome_coord = DummyEndpoint()
    service.dome_sync = DummyEndpoint()
    service.dome_pid_sync = DummyEndpoint()
    service.ccd_cmd = DummyEndpoint()
    service.com_delay = DummyEndpoint()
    service.file = DummyEndpoint()
    sys.modules["necst.service"] = service
    necst.service = service

    topic = types.ModuleType("necst.topic")
    names = [
        "antenna_cmd_transition",
        "manual_stop_alert",
        "pid_param",
        "chopper_cmd",
        "mirror_m2_cmd",
        "mirror_m4_cmd",
        "membrane_cmd",
        "drive_cmd",
        "spectra_meta",
        "qlook_meta",
        "sis_bias_cmd",
        "lo_signal_cmd",
        "attenuator_cmd",
        "local_attenuator_cmd",
        "record_cmd",
        "spectra_rec",
        "channel_binning",
        "manual_stop_dome_alert",
        "dome_oc",
        "timeonly",
        "tp_mode",
        "antenna_tracking",
        "antenna_encoder",
        "altaz_cmd",
        "antenna_speed_cmd",
        "chopper_status",
        "mirror_m2_status",
        "mirror_m4_status",
        "membrane_status",
        "drive_status",
        "antenna_control_status",
        "sis_bias",
        "hemt_bias",
        "analog_logger",
        "lo_signal",
        "thermometer",
        "vacuum_gauge",
        "powermeter",
        "local_attenuator",
        "camera_image",
        "quick_spectra",
        "dome_tracking",
        "dome_encoder",
        "dome_speed",
        "dome_status",
        "dome_limit",
        "weather",
        "alert",
    ]
    for name in names:
        setattr(topic, name, DummyEndpoint())
    sys.modules["necst.topic"] = topic
    necst.topic = topic

    utils = types.ModuleType("necst.utils")

    class Topic:
        pass

    utils.Topic = Topic
    sys.modules["necst.utils"] = utils

    core_pkg = types.ModuleType("necst.core")
    core_pkg.__path__ = []
    sys.modules["necst.core"] = core_pkg

    auth = types.ModuleType("necst.core.auth")

    class PrivilegedNode:
        pass

    def require_privilege(*args, **kwargs):
        def deco(func):
            return func

        return deco

    auth.PrivilegedNode = PrivilegedNode
    auth.require_privilege = require_privilege
    sys.modules["necst.core.auth"] = auth

    necst_msgs = types.ModuleType("necst_msgs")
    necst_msgs.__path__ = []
    sys.modules["necst_msgs"] = necst_msgs

    msgs = types.ModuleType("necst_msgs.msg")

    def _simple_msg(name):
        class _M:
            def __init__(self, **kwargs):
                self.__dict__.update(kwargs)

        _M.__name__ = name
        return _M

    for cls_name in [
        "AlertMsg",
        "Binning",
        "Boolean",
        "ChopperMsg",
        "DeviceReading",
        "DomeOC",
        "DriveMsg",
        "LocalAttenuatorMsg",
        "LocalSignal",
        "MembraneMsg",
        "MirrorMsg",
        "PIDMsg",
        "RecordMsg",
        "Sampling",
        "SISBias",
        "Spectral",
        "TimeOnly",
        "TPModeMsg",
    ]:
        setattr(msgs, cls_name, _simple_msg(cls_name))

    class ScanBlockSection:
        MOVE_TO_ENTRY = 0
        FIRST_STANDBY = 1
        ACCELERATE = 2
        LINE = 3
        TURN = 4
        DECELERATE = 5
        FINAL_STANDBY = 6

        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)

    msgs.ScanBlockSection = ScanBlockSection
    sys.modules["necst_msgs.msg"] = msgs

    srv = types.ModuleType("necst_msgs.srv")

    def _simple_srv(name):
        class _Req:
            def __init__(self, **kwargs):
                self.__dict__.update(kwargs)

        class _Res:
            def __init__(self, **kwargs):
                self.__dict__.update(kwargs)

        class _S:
            Request = _Req
            Response = _Res

        _S.__name__ = name
        return _S

    for cls_name in [
        "CCDCommand",
        "ComDelaySrv",
        "CoordinateCommand",
        "DomeSync",
        "File",
        "ScanBlockCommand",
    ]:
        setattr(srv, cls_name, _simple_srv(cls_name))
    sys.modules["necst_msgs.srv"] = srv

    rclpy = types.ModuleType("rclpy")
    rclpy.__path__ = []
    sys.modules["rclpy"] = rclpy
    rclpy_pub = types.ModuleType("rclpy.publisher")

    class Publisher:
        pass

    rclpy_pub.Publisher = Publisher
    sys.modules["rclpy.publisher"] = rclpy_pub
    rclpy_sub = types.ModuleType("rclpy.subscription")

    class Subscription:
        pass

    rclpy_sub.Subscription = Subscription
    sys.modules["rclpy.subscription"] = rclpy_sub

    return _load_source(
        "necst.core.commander",
        repo_root / "necst" / "core" / "commander.py",
    )


def load_spectrometer_module(repo_root: Path):
    clear_modules(["necst", "neclib", "necst_msgs", "rclpy"])

    neclib = types.ModuleType("neclib")
    neclib.__path__ = []
    sys.modules["neclib"] = neclib

    neclib_data = types.ModuleType("neclib.data")

    class Resize:
        def __init__(self, keep_duration=1.0):
            self.keep_duration = keep_duration

        def push(self, *a, **k):
            pass

        def get(self, *a, **k):
            return []

    neclib_data.Resize = Resize
    sys.modules["neclib.data"] = neclib_data

    neclib_recorders = types.ModuleType("neclib.recorders")

    class NECSTDBWriter:
        pass

    class Recorder:
        def __init__(self, *a, **k):
            self.writers = []
            self.is_recording = False

        def add_writer(self, writer):
            self.writers.append(writer)

    neclib_recorders.NECSTDBWriter = NECSTDBWriter
    neclib_recorders.Recorder = Recorder
    sys.modules["neclib.recorders"] = neclib_recorders

    neclib_utils = types.ModuleType("neclib.utils")

    class ConditionChecker:
        def __init__(self, *a, **k):
            pass

        def check(self, *a, **k):
            return True

    neclib_utils.ConditionChecker = ConditionChecker
    sys.modules["neclib.utils"] = neclib_utils

    necst = types.ModuleType("necst")
    necst.__path__ = []
    sys.modules["necst"] = necst

    cfg = types.ModuleType("necst.config")
    cfg.record_every_n_spectral_data = 1
    sys.modules["necst.config"] = cfg
    necst.config = cfg

    namespace = types.ModuleType("necst.namespace")
    namespace.rx = "rx"
    sys.modules["necst.namespace"] = namespace
    necst.namespace = namespace

    class DummyEndpoint:
        def publisher(self, *args, **kwargs):
            return SimpleNamespace(publish=lambda *a, **k: None)

        def subscription(self, *args, **kwargs):
            return None

    topic = types.ModuleType("necst.topic")
    topic.spectra_meta = DummyEndpoint()
    topic.qlook_meta = DummyEndpoint()
    topic.antenna_control_status = DummyEndpoint()
    topic.spectra_rec = DummyEndpoint()
    topic.tp_mode = DummyEndpoint()
    topic.channel_binning = DummyEndpoint()
    topic.quick_spectra = defaultdict(DummyEndpoint)
    sys.modules["necst.topic"] = topic
    necst.topic = topic

    core = types.ModuleType("necst.core")

    class DeviceNode:
        pass

    core.DeviceNode = DeviceNode
    sys.modules["necst.core"] = core

    necst_msgs = types.ModuleType("necst_msgs")
    necst_msgs.__path__ = []
    sys.modules["necst_msgs"] = necst_msgs
    msgs = types.ModuleType("necst_msgs.msg")

    def _simple_msg(name):
        class _M:
            def __init__(self, **kwargs):
                self.__dict__.update(kwargs)

            def get_fields_and_field_types(self):
                return {
                    k: "string" if isinstance(v, str) else "float64"
                    for k, v in self.__dict__.items()
                }

        _M.__name__ = name
        return _M

    for cls_name in ["Binning", "ControlStatus", "Sampling", "TPModeMsg"]:
        setattr(msgs, cls_name, _simple_msg(cls_name))

    class Spectral:
        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)

        def get_fields_and_field_types(self):
            return {
                k: "string" if isinstance(v, str) else "float64"
                for k, v in self.__dict__.items()
            }

    msgs.Spectral = Spectral
    sys.modules["necst_msgs.msg"] = msgs

    rclpy = types.ModuleType("rclpy")
    rclpy.__path__ = []
    sys.modules["rclpy"] = rclpy
    rclpy_pub = types.ModuleType("rclpy.publisher")

    class Publisher:
        pass

    rclpy_pub.Publisher = Publisher
    sys.modules["rclpy.publisher"] = rclpy_pub

    return _load_source(
        "necst.rx.spectrometer",
        repo_root / "necst" / "rx" / "spectrometer.py",
    )
