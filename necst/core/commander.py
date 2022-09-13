from math import degrees
from typing import Literal
from rclpy.node import Node
from necst_msgs.msg import CoordMsg, TimedAzElFloat64
import necst
import time
from necst import config
# TimedAzElFloat64 は未実装 {az: float64, el: float64, time: float64}


class Commander(Node):

    node_name = "commander"
    Namespace = f"/necst/{config.observatory}/core"

    def __init__(self):
        super().__init__(self.node_name, namespace = self.Namespace)
        self.publisher = {
            "coord": self.create_publisher(CoordMsg, "raw_coord", 1),
        }

    def antenna(
        self,
        cmd: Literal["stop", "drive"],
        *,
        lon: float = None,
        lat: float = None,
        unit: str = None,
        frame: str = None,
        obstime: float = None,
        name: str = None,
        tracking_check: bool = True,
    ) -> None:

        if cmd == 'stop':
            recv = False
            def send_cmd(msg: CoordMsg) -> None:
                nonlocal recv
                self.publisher["coord"].publish(msg)
                recv = True
            subs_enc = self.create_subscription(CoordMsg, "encoder", send_cmd,1)

            rep_time = 0
            while not recv:
                time.sleep(0.02)
                rep_time += 0.02


            subs_enc.destroy()

        else:
            if name is not None:
                msg = CoordMsg(time = obstime, name = name)
                self.publisher["coord"].publish(msg)

            else:
                msg = CoordMsg(lon = lon, lat = lat, unit = unit, frame = frame, time = obstime)
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

        subs_enc = self.create_subscription(CoordMsg, "encoder", enc_update, 1)
        subs_cmd = self.create_subscription(CoordMsg, "altaz", cmd_update, 1)


        counter = 0

        while True:
            Condition = Checker(enc_az, enc_el, cmd_az, cmd_el)
            if Condition.Check():
                counter += 1
            else:
                counter = 0
            if counter > 10:
                return Condition.Check()

            time.sleep(0.05)

        subs_enc.destroy()
        subs_cmd.destroy()



class Checker:
    def __init__(self, enc_az, enc_el, cmd_az, cmd_el) -> float:
        self.threshold = necst.config.antenna_pointing_accuracy.to("deg").value
        try:
            self.condition = (cmd_az - enc_az)**2 + (cmd_el - enc_el)**2
        except TypeError:
            self.condition = self.threshold + 1


    def Check(self):
        return self.condition < self.threshold


