import time

from .measurement_base import Measurement


class Knife_edge(Measurement):
    observation_type = "Knife_edge"

    def run(
        self,
        id: list,
        positions=np.linspace(4750, 19700, 300),
    ) -> None:
        for position in positions:
            self.com.chopper(cmd=position, wait_oc=True)
        self.com.chopper(cmd="remove")
