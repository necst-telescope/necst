# flake8: noqa

import array
import typing

class AlertMsg:
    SLOT_TYPES: typing.Tuple
    def __init__(
        self,
        threshold: float = float(),
        actual: float = float(),
        warning: bool = bool(),
        critical: bool = bool(),
        target: typing.List[str] = list(),
    ) -> None: ...
    @classmethod
    def get_fields_and_field_types(cls) -> typing.Dict[str, str]: ...
    @property
    def threshold(self) -> float: ...
    @threshold.setter
    def threshold(self, value: float) -> None: ...
    @property
    def actual(self) -> float: ...
    @actual.setter
    def actual(self, value: float) -> None: ...
    @property
    def warning(self) -> bool: ...
    @warning.setter
    def warning(self, value: bool) -> None: ...
    @property
    def critical(self) -> bool: ...
    @critical.setter
    def critical(self, value: bool) -> None: ...
    @property
    def target(self) -> typing.List[str]: ...
    @target.setter
    def target(self, value: typing.List[str]) -> None: ...

class ChopperMsg:
    SLOT_TYPES: typing.Tuple
    def __init__(self, insert: bool = bool(), time: float = float()) -> None: ...
    @classmethod
    def get_fields_and_field_types(cls) -> typing.Dict[str, str]: ...
    @property
    def insert(self) -> bool: ...
    @insert.setter
    def insert(self, value: bool) -> None: ...
    @property
    def time(self) -> float: ...
    @time.setter
    def time(self, value: float) -> None: ...

class CoordCmdMsg:
    SLOT_TYPES: typing.Tuple
    def __init__(
        self,
        lon: array.array = array.array("d"),
        lat: array.array = array.array("d"),
        unit: str = str(),
        frame: str = str(),
        time: array.array = array.array("d"),
        name: str = str(),
        speed: float = float(),
    ) -> None: ...
    @classmethod
    def get_fields_and_field_types(cls) -> typing.Dict[str, str]: ...
    @property
    def lon(self) -> array.array: ...
    @lon.setter
    def lon(self, value: array.array) -> None: ...
    @property
    def lat(self) -> array.array: ...
    @lat.setter
    def lat(self, value: array.array) -> None: ...
    @property
    def unit(self) -> str: ...
    @unit.setter
    def unit(self, value: str) -> None: ...
    @property
    def frame(self) -> str: ...
    @frame.setter
    def frame(self, value: str) -> None: ...
    @property
    def time(self) -> array.array: ...
    @time.setter
    def time(self, value: array.array) -> None: ...
    @property
    def name(self) -> str: ...
    @name.setter
    def name(self, value: str) -> None: ...
    @property
    def speed(self) -> float: ...
    @speed.setter
    def speed(self, value: float) -> None: ...

class CoordMsg:
    SLOT_TYPES: typing.Tuple
    def __init__(
        self,
        lon: float = float(),
        lat: float = float(),
        unit: str = str(),
        frame: str = str(),
        time: float = float(),
        name: str = str(),
    ) -> None: ...
    @classmethod
    def get_fields_and_field_types(cls) -> typing.Dict[str, str]: ...
    @property
    def lon(self) -> float: ...
    @lon.setter
    def lon(self, value: float) -> None: ...
    @property
    def lat(self) -> float: ...
    @lat.setter
    def lat(self, value: float) -> None: ...
    @property
    def unit(self) -> str: ...
    @unit.setter
    def unit(self, value: str) -> None: ...
    @property
    def frame(self) -> str: ...
    @frame.setter
    def frame(self, value: str) -> None: ...
    @property
    def time(self) -> float: ...
    @time.setter
    def time(self, value: float) -> None: ...
    @property
    def name(self) -> str: ...
    @name.setter
    def name(self, value: str) -> None: ...

class PIDMsg:
    SLOT_TYPES: typing.Tuple
    def __init__(
        self,
        k_p: float = float(),
        k_i: float = float(),
        k_d: float = float(),
        axis: str = str(),
    ) -> None: ...
    @classmethod
    def get_fields_and_field_types(cls) -> typing.Dict[str, str]: ...
    @property
    def k_p(self) -> float: ...
    @k_p.setter
    def k_p(self, value: float) -> None: ...
    @property
    def k_i(self) -> float: ...
    @k_i.setter
    def k_i(self, value: float) -> None: ...
    @property
    def k_d(self) -> float: ...
    @k_d.setter
    def k_d(self, value: float) -> None: ...
    @property
    def axis(self) -> str: ...
    @axis.setter
    def axis(self, value: str) -> None: ...

class TimedAzElFloat64:
    SLOT_TYPES: typing.Tuple
    def __init__(
        self, az: float = float(), el: float = float(), time: float = float()
    ) -> None: ...
    @classmethod
    def get_fields_and_field_types(cls) -> typing.Dict[str, str]: ...
    @property
    def az(self) -> float: ...
    @az.setter
    def az(self, value: float) -> None: ...
    @property
    def el(self) -> float: ...
    @el.setter
    def el(self, value: float) -> None: ...
    @property
    def time(self) -> float: ...
    @time.setter
    def time(self, value: float) -> None: ...

class TimedAzElInt64:
    SLOT_TYPES: typing.Tuple
    def __init__(
        self,
        az: int = int(),
        el: int = int(),
        time: float = float(),
    ) -> None: ...
    @classmethod
    def get_fields_and_field_types(cls) -> typing.Dict[str, str]: ...
    @property
    def az(self) -> int: ...
    @az.setter
    def az(self, value: int) -> None: ...
    @property
    def el(self) -> int: ...
    @el.setter
    def el(self, value: int) -> None: ...
    @property
    def time(self) -> float: ...
    @time.setter
    def time(self, value: float) -> None: ...

class TimedFloat64:
    SLOT_TYPES: typing.Tuple
    def __init__(
        self,
        data: float = float(),
        time: float = float(),
    ) -> None: ...
    @classmethod
    def get_fields_and_field_types(cls) -> typing.Dict[str, str]: ...
    @property
    def data(self) -> float: ...
    @data.setter
    def data(self, value: float) -> None: ...
    @property
    def time(self) -> float: ...
    @time.setter
    def time(self, value: float) -> None: ...