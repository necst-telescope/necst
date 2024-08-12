#!/usr/bin/env python3
r"""SIS Tuning Parameter Searching.

Measure and save the sarching SIS tuning parameter.
"id" argument specifies the beam.
Specify the minimum voltage, maximum voltage, and step voltage in units of mV.
Specify the minimum local attenuator current, maximum local attenuator current,
and step local attenuator current in units of mA.
Specify the measurement interval in units of second.
Note: Do NOT set the interval to under 1.5 sec (at least 1.0 sec). 
The measurement interval of SIS Bias is up to 1 sec.

Examples
--------
$ necst sis_tuning -id "USB" "LSB" -vmin -8.0 -vmax 8.0 -vs 0.1 -lomin 4.0 -lomax 7.0 -los 0.1 -i 1.5

"""
import argparse

from necst.procedures import SIS_Tuning

if __name__ == "__main__":
    description = "SIS Tuning Parameter Searching"
    p = argparse.ArgumentParser(description=description)
    p.add_argument(
        "-id",
        nargs="*",
        type=str,
        help="Selection of beam",
    )
    p.add_argument(
        "-vmin",
        type=float,
        help="Minimum voltage in mV",
        default=-8.0,
    )
    p.add_argument(
        "-vmax",
        type=float,
        help="Max voltage in mV",
        default=8.0,
    )
    p.add_argument(
        "-vs",
        "--vstep",
        type=float,
        help="Step voltage in mV",
        default=0.1,
    )
    p.add_argument(
        "-lomin",
        type=float,
        help="Minimum voltage in mA",
        default=4.0,
    )
    p.add_argument(
        "-lomax",
        type=float,
        help="Max voltage in mA",
        default=7.0,
    )
    p.add_argument(
        "-los",
        "--lostep",
        type=float,
        help="Step voltage in mA",
        default=0.1,
    )
    p.add_argument(
        "-i",
        "--interval",
        type=float,
        help="Measurement interval",
        default=1.5,
    )
    args = p.parse_args()

    meas = SIS_Tuning(
        id=args.id,
        min_voltage_mV=args.vmin,
        max_voltage_mV=args.vmax,
        step_voltage_mV=args.vstep,
        min_loatt_mA=args.lomin,
        max_loatt_mA=args.lomax,
        step_loatt_mA=args.lostep,
        interval=args.interval,
    )
    meas.execute()
