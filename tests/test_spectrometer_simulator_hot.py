from types import SimpleNamespace

from necst import config
from necst.rx.spectrometer_simulator import SimulatedSpectralData


class _FakeSpectrometerSimulator:
    def __init__(self) -> None:
        self.hot_states = []

    def set_hot(self, enabled: bool) -> None:
        self.hot_states.append(bool(enabled))


def _node_with_fake_simulator():
    node = SimulatedSpectralData.__new__(SimulatedSpectralData)
    simulator = _FakeSpectrometerSimulator()
    node._simulated_spectrometers = (simulator,)
    return node, simulator


def test_chopper_insert_position_enables_hot():
    node, simulator = _node_with_fake_simulator()
    msg = SimpleNamespace(position=config.chopper_motor_position["insert"], insert=True)

    node.update_chopper_status(msg)

    assert simulator.hot_states == [True]


def test_chopper_insert_flag_alone_does_not_enable_hot():
    node, simulator = _node_with_fake_simulator()
    msg = SimpleNamespace(
        position=config.chopper_motor_position["insert"] + 1,
        insert=True,
    )

    node.update_chopper_status(msg)

    assert simulator.hot_states == [False]


def test_chopper_remove_position_disables_hot():
    node, simulator = _node_with_fake_simulator()
    msg = SimpleNamespace(position=config.chopper_motor_position["remove"], insert=False)

    node.update_chopper_status(msg)

    assert simulator.hot_states == [False]
