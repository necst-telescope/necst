from necst_msgs.msg import ChopperMsg

from .. import config, topic
from .spectrometer import SpectralData


class SimulatedSpectralData(SpectralData):
    """SpectralData extension used only when NECST runs in simulator mode.

    Simulator-only observing-state effects belong here so the real spectrometer
    path never contains synthetic signal-generation branches.
    """

    def __init__(self) -> None:
        super().__init__()

        simulated = []
        for key, io in self.io.items():
            if not getattr(io, "is_simulator", False):
                raise RuntimeError(
                    "SimulatedSpectralData requires simulator spectrometers; "
                    f"{key!r} is not a simulator"
                )
            if not callable(getattr(io, "set_hot", None)):
                raise RuntimeError(
                    "Configured spectrometer simulator does not support HOT state; "
                    f"{key!r} has no set_hot()"
                )
            simulated.append(io)

        self._simulated_spectrometers = tuple(simulated)
        topic.chopper_status.subscription(self, self.update_chopper_status)

    def update_chopper_status(self, msg: ChopperMsg) -> None:
        """Apply HOT only after the chopper reaches the configured insert position."""
        hot = msg.position == config.chopper_motor_position["insert"]
        for io in self._simulated_spectrometers:
            io.set_hot(hot)
