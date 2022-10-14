import time
import struct
import ogameasure
import threading

import rclpy
from rclpy.node import Node
from std_msgs.msg import Float64
from neclib.simulators.antenna import Thermometer  # temperture, humidity, pressureを返す関数

from necst import namespace, qos


class ThermometerReader(Node):

    NodeName = "thermometer_reader"
    Namespace = namespace.antenna

    def __init__(self):
        super().__init__(self.NodeName, namespace=self.Namespace)

        port = self.declare_parameter("~ondotori_usbport")
        self.ondotori = ogameasure.TandD.tr_73u(port)

        self.publisher = self.create_publisher(Float64, "temperature", qos.realtime)
        self.publisher = self.create_publisher(Float64, "humidity", qos.realtime)
        self.publisher = self.create_publisher(Float64, "pressure", qos.realtime)

        self.thermo = Thermometer()

    # def termometer_reader(self, msg):
    # 値を与える必要がないならいらないかも

    def stream(self):
        thermometer = self.thermo.read()
        msg_temp = Float64(thermometer.temp)
        msg_hum = Float64(thermometer.hum)
        msg_press = Float64(thermometer.press)

        self.publisher.publish(msg_temp)
        self.publisher.publish(msg_hum)
        self.publisher.publish(msg_press)

    def start_thread(self):
        th = threading.Thread(target=self.publish_data)
        th.setDaemon(True)
        th.start()


def main(args=None):
    rclpy.init(args=args)
    node = ThermometerReader()
    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    finally:
        node.destroy_node()
        rclpy.try_shutdown()


if __name__ == "__main__":
    main()
