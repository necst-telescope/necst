import time as pytime
from typing import Literal

from neclib.utils import ConditionChecker
from rclpy.node import Node

from necst import config, namespace, qos
from necst_msgs.msg import CoordMsg


class Commander(Node):

    NodeName = "commander"
    Namespace = namespace.core

    def __init__(self):
        super().__init__(self.NodeName, namespace=self.Namespace)
        self.publisher = {
            "coord": self.create_publisher(
                CoordMsg, f"{namespace.antenna}/raw_coord", qos.reliable
            ),
        }

    def antenna(
        self,
        cmd: Literal["stop", "point", "scan", "jog"],
        *,
        lon: float = None,
        lat: float = None,
        unit: str = None,
        frame: str = None,
        time: float = 0.0,
        name: str = None,
        tracking_check: bool = True,
    ) -> None:
        if cmd.lower() == "stop":
            # TODO: Consider using alert.
            recv = False

            def send_cmd(msg: CoordMsg) -> None:
                nonlocal recv
                self.publisher["coord"].publish(msg)
                recv = True

            subs_enc = self.create_subscription(
                CoordMsg, f"{namespace.antenna}/encoder", send_cmd, qos.realtime
            )
            while not recv:
                pytime.sleep(0.02)
            self.destroy_subscription(subs_enc)
        elif cmd.lower() == "point":
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
        else:
            raise NotImplementedError(f"Command '{cmd}' isn't implemented yet.")

    def tracking_check(
        self, target: Literal["antenna", "dome"], timeout_sec: float = 10
    ) -> bool:
        topic_name = {
            "antenna": [f"{namespace.antenna}/encoder", f"{namespace.antenna}/altaz"],
            "dome": ["", ""],  # TODO: Implement.
        }
        enc_az = enc_el = cmd_az = cmd_el = None

        def enc_update(msg: CoordMsg) -> float:
            nonlocal enc_az, enc_el
            enc_az, enc_el = msg.lon, msg.lat

        def cmd_update(msg: CoordMsg) -> float:
            nonlocal cmd_az, cmd_el
            cmd_az, cmd_el = msg.lon, msg.lat

        subs_enc = self.create_subscription(
            CoordMsg, topic_name[target.lower()][0], enc_update, qos.realtime
        )
        subs_cmd = self.create_subscription(
            CoordMsg, topic_name[target.lower()][1], cmd_update, qos.realtime
        )

        timelimit = pytime.time() + timeout_sec
        checker = ConditionChecker(10, reset_on_failure=True)
        threshold = config.antenna_pointing_accuracy.to_value("deg")
        while True:
            error_az = enc_az - cmd_az
            error_el = enc_el - cmd_el
            if checker.check(error_az**2 + error_el**2 < threshold**2):
                self.destroy_subscription(subs_enc)
                self.destroy_subscription(subs_cmd)
                return True
            if pytime.time() > timelimit:
                return False
            pytime.sleep(0.05)
