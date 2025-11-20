#!/usr/bin/env python3
from necst.procedures import Knife_edge

if __name__ == "__main__":
    description = "Beam pattern Measurement"

    meas = Knife_edge(id="beam")
    meas.execute()
