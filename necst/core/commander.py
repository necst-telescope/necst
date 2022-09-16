import time as pytime
from typing import Literal

from rclpy.node import Node

from necst import config, namespace
from necst_msgs.msg import CoordMsg


class Commander(Node):

    NodeName = "commander"
    Namespace = namespace.core

    def __init__(self):
        super().__init__(self.NodeName, namespace=self.Namespace)
        self.publisher = {
            "coord": self.create_publisher(
                CoordMsg, f"{namespace.antenna}/raw_coord", 1
            ),
        }

    def antenna(
        self,
        cmd: Literal["stop", "drive"],
        *,
        lon: float = None,
        lat: float = None,
        unit: str = None,
        frame: str = None,
        time: float = 0.0,
        name: str = None,
        tracking_check: bool = True,
    ) -> None:

        if cmd == "stop":
            recv = False

            def send_cmd(msg: CoordMsg) -> None:
                nonlocal recv
                self.publisher["coord"].publish(msg)
                recv = True

            subs_enc = self.create_subscription(
                CoordMsg, f"{namespace.antenna}/encoder", send_cmd, 1
            )

            while not recv:
                pytime.sleep(0.02)

            self.destroy_subscription(subs_enc)

        else:
            if name is not None:
                msg = CoordMsg(time=time, name=name)
                self.publisher["coord"].publish(msg)

            else:
                msg = CoordMsg(
                    lon=float(lon), lat=float(lat), unit=unit, frame=frame, time=time
                )
                self.publisher["coord"].publish(msg)

            if tracking_check:
                self.tracking_check("antenna")

    def tracking_check(self, target: Literal["antenna"]) -> bool:
        enc_az = enc_el = cmd_az = cmd_el = None

        def enc_update(msg: CoordMsg) -> float:
            nonlocal enc_az, enc_el
            enc_az = msg.lon
            enc_el = msg.lat

        def cmd_update(msg: CoordMsg) -> float:
            nonlocal cmd_az, cmd_el
            cmd_az = msg.lon
            cmd_el = msg.lat

        subs_enc = self.create_subscription(
            CoordMsg, f"{namespace.antenna}/encoder", enc_update, 1
        )
        subs_cmd = self.create_subscription(
            CoordMsg, f"{namespace.antenna}/altaz", cmd_update, 1
        )

        counter = 0

        while True:
            Condition = Checker(enc_az, enc_el, cmd_az, cmd_el)
            if Condition.Check():
                counter += 1
            else:
                counter = 0
            if counter > 10:
                self.destroy_subscription(subs_enc)
                self.destroy_subscription(subs_cmd)
                return Condition.Check()

            pytime.sleep(0.05)


class Checker:
    def __init__(self, enc_az, enc_el, cmd_az, cmd_el) -> float:
        self.threshold = config.antenna_pointing_accuracy.to("deg").value
        try:
            self.condition = (cmd_az - enc_az) ** 2 + (cmd_el - enc_el) ** 2
        except TypeError:
            self.condition = self.threshold + 1

    def Check(self):
        return self.condition < self.threshold
