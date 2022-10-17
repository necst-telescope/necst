import time
import struct
import ogameasure

import rclpy
from rclpy.node import Node
from std_msgs.msg import Float64
from neclib.simulators.antenna import Thermometer  # temperture, humidity, pressureを返す関数

from necst import namespace, qos, config


class ThermometerReader(Node):

    NodeName = "thermometer_reader"
    Namespace = namespace.antenna

    def __init__(self):
        super().__init__(self.NodeName, namespace=self.Namespace)

        port = self.declare_parameter("~ondotori_usbport")
        self.ondotori = ogameasure.TandD.tr_73u(port)

        self.publisher = {
            "temperature": self.create_publisher(Float64, "temperature", qos.realtime),
            "humidity": self.create_publisher(Float64, "humidity", qos.realtime),
            "pressure": self.create_publisher(Float64, "pressure", qos.realtime),
        }

        self.thermo = Thermometer()
        self.create_timer(1 / config.antenna_command_frequency, self.publisher)

    # def thermometer_reader(self, msg):
    # 値を与える必要がないならいらないかも

    def stream(self):
        thermometer = self.thermo.read()
        msg_temp = Float64(thermometer.temp)
        msg_hum = Float64(thermometer.hum)
        msg_press = Float64(thermometer.press)

        self.publisher["temperature"].publish(msg_temp)
        self.publisher["humidity"].publish(msg_hum)
        self.publisher["pressure"].publish(msg_press)


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