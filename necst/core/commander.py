import time as pytime
from typing import Literal

from neclib.utils import ConditionChecker

from .. import config, namespace, qos
from .auth import PrivilegedNode, require_privilege
from necst_msgs.msg import CoordMsg


class Commander(PrivilegedNode):

    NodeName = "commander"
    Namespace = namespace.core

    def __init__(self):
        super().__init__(self.NodeName, namespace=self.Namespace)
        self.publisher = {
            "coord": self.create_publisher(
                CoordMsg, f"{namespace.antenna}/raw_coord", qos.reliable
            ),
        }

    @require_privilege
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
        wait: bool = True,
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

            if wait:
                self.tracking_check("antenna")
        else:
            raise NotImplementedError(f"Command '{cmd}' isn't implemented yet.")

    @require_privilege
    def chopper(self, cmd: Literal["insert", "eject"]):
        ...

    def tracking_check(
        self, target: Literal["antenna", "dome"], timeout_sec: float = None
    ) -> bool:
        # TODO: Implement as independent node.
        topic_name = {
            "antenna": [f"{namespace.antenna}/encoder", f"{namespace.antenna}/altaz"],
            "dome": ["", ""],  # TODO: Implement.
        }
        threshold = {
            "antenna": config.antenna_pointing_accuracy.to_value("deg"),
            "dome": ...,
        }
        enc_az = enc_el = cmd_az = cmd_el = None

        def enc_update(msg: CoordMsg) -> float:
            nonlocal enc_az, enc_el
            enc_az, enc_el = msg.lon, msg.lat

        def cmd_update(msg: CoordMsg) -> float:
            nonlocal cmd_az, cmd_el
            cmd_az, cmd_el = msg.lon, msg.lat

        subs_enc = self.create_subscription(
            CoordMsg, topic_name[target][0], enc_update, qos.realtime
        )
        subs_cmd = self.create_subscription(
            CoordMsg, topic_name[target][1], cmd_update, qos.realtime
        )

        timelimit = None if timeout_sec is None else pytime.time() + timeout_sec
        checker = ConditionChecker(10, reset_on_failure=True)
        while True:
            if any(p is None for p in [enc_az, enc_el, cmd_az, cmd_el]):
                pytime.sleep(0.05)
                continue
            error_az = enc_az - cmd_az
            error_el = enc_el - cmd_el
            if checker.check(error_az**2 + error_el**2 < threshold[target] ** 2):
                self.destroy_subscription(subs_enc)
                self.destroy_subscription(subs_cmd)
                return True
            if (timelimit is not None) and (pytime.time() > timelimit):
                return False
            pytime.sleep(0.05)
