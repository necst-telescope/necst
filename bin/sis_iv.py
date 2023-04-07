#!/usr/bin/env python3
"""SIS I-V Measurement.

Measure and save the I-V of SIS.
"id" argument specifies the sideband （USB or LSB)
Specify the minimum voltage, maximum voltage, and step voltage in units of mV.

Examples
--------
$ sis_iv -id "USB" -min -8.0 -max 8.0 -s 0.1

"""
import argparse

from necst.procedures import SIS_IV

if __name__ == "__main__":
    description = "SIS I-V Measurement"
    p = argparse.ArgumentParser(description=description)
    p.add_argument(
        "-id",
        type=str,
        help="Selection of sideband (USB/LSB)",
    )
    p.add_argument(
        "-min",
        type=float,
        help="Minimum voltage in mV",
        default=-8.0,
    )
    p.add_argument(
        "-max",
        type=float,
        help="Max voltage in mV",
        default=8.0,
    )
    p.add_argument(
        "-s",
        "--step",
        type=float,
        help="Step voltage in mV",
        default=0.1,
    )
    args = p.parse_args()

    meas = SIS_IV(
        id=args.id,
        min_voltage_mV=args.min,
        max_voltage_mV=args.max,
        step_voltage_mV=args.step,
    )
    meas.execute()
