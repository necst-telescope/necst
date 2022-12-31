import time

from neclib.coordinates import Observer
from necst_msgs.msg import Clock, CoordMsg
from rclpy.node import Node

from .. import config, namespace, topic


class ObserverInfo(Node):

    NodeName = "info"
    Namespace = namespace.root

    def __init__(self) -> None:
        super().__init__(self.NodeName, namespace=self.Namespace)
        topic.antenna_encoder.subscription(self, self.update_encoder_reading)
        self.pub = topic.clock.publisher(self)
        self.calculator = Observer(config.location)
        self.create_timer(1, self.stream)
        self.encoder_reading = None

    def update_encoder_reading(self, msg: CoordMsg) -> None:
        self.encoder_reading = msg

    def stream(self) -> None:
        if self.encoder_reading is None:
            return

        calc_time = time.time()
        lst = self.calculator.lst(calc_time)
        v_obs = self.calculator.v_obs(
            lon=self.encoder_reading.lon,
            lat=self.encoder_reading.lat,
            frame=self.encoder_reading.frame,
            unit=self.encoder_reading.unit,
            time=calc_time,
        )
        msg = Clock(time=calc_time, lst=lst, v_obs=v_obs)
        self.pub.publish(msg)
