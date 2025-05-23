#!/usr/bin/env python3
r"""SIS I-V Measurement.

Measure and save the I-V of SIS.
"id" argument specifies the beam.
Specify the minimum voltage, maximum voltage, and step voltage in units of mV.

Examples
--------
$ necst sis_iv -id "USB" "LSB" -min -8.0 -max 8.0 -s 0.1

"""
import argparse

from necst.procedures import Knife_edge

if __name__ == "__main__":
    description = "Beam pattern Measurement"

    meas = Knife_edge(id="beam")
    meas.execute()
