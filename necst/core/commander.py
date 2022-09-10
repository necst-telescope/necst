from math import degrees
from typing import Literal
from rclpy.node import Node
from necst_msgs.msg import CoordMsg, TimedAzElFloat64
import necst
import time
# TimedAzElFloat64 は未実装 {az: float64, el: float64, time: float64}


class Commander(Node):

    node_name = "commander"

    def __init__(self):
        super().__init__(node_name)
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
        time: float = None,
        name: str = None,
        tracking_check: bool = True,
    ) -> None:

        if cmd == 'stop':
            def send_cmd(msg: CoordMsg) -> None:
                self.publisher["coord"].publish(msg)
            subs_enc = self.create_subscription(CoordMsg, "encoder", send_cmd)
            subs_enc.destroy()

        else:
            if name != None:
                msg = CoordMsg(time = time, name = name)
                self.publisher["coord"].publish(msg)

            else:
                msg = CoordMsg(lon = lon, lat = lat, unit = unit, frame = frame, time = time)
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

        subs_enc = self.create_subscription(CoordMsg, "encoder", enc_update)
        subs_cmd = self.create_subscription(CoordMsg, "altaz", cmd_update)

        Condition = Checker(enc_az, enc_el, cmd_az, cmd_el)
        counter = 0

        while counter == 10:
            if Condition.Check():
                counter += 1
            else:
                counter = 0
            time.sleep(0.05)

        subs_enc.destroy()
        subs_cmd.destroy()

        return Condition.Check()

class Checker:
    def __init__(self, enc_az, enc_el, cmd_az, cmd_el) -> float:
        self.condition = (cmd_az - enc_az)**2 + (cmd_el - enc_el)**2
        self.threshold = necst.config.antenna_pointing_accuracy

    def Check(self):
        self.condition < self.threshold.to("deg").value

