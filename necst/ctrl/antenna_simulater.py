import rclpy
from neclib.simulators.antenna import AntennaEncoderEmulator
from neclib.controllers import PIDController
from rclpy.node import Node
from necst_msgs.msg import TimedAzElFloat64

class AntennaSimulater(Node):

    NodeName = "antenna_simulater"
    Namespace = f"/necst/{config.observatory}/ctrl/antenna"

    def __init__(self): #はじめに実行される関数。配信や購読、タイマーを生成する。
        super().__init__(self.NodeName, namespace=self.Namespace)
        self.publisher = self.create_publisher(TimedAzElFloat64, "encorder", 1) #これで実測値を流せる？
        self.create_subscription(TimedAzElFloat64, "speed", self.antenna_simulater, 1) #これでantenna.pyから読み込める？ antenna.pyはどうやって指定する？

    def antenna_simulater(self):
        enc = AntennaEncoderEmulator()
        pid_az = PIDController() #もしかしたらこれはいらなくなる？
        speed = pid_az.get_speed(30, enc.read.az) #この30にspeedから受け取った値を入れればいい？
        #speed = pid_az.get_speed()
        az = enc.command(speed, "az") #これで位置を返せる？
        self.publisher.publish(az) #これでspeedから位置に変換したものを渡せる？

    def main(args=None):
    rclpy.init(args=args)
    node = AntennaSimulater()
    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    finally:
        node.destroy_node()
        rclpy.try_shutdown()

if __name__ == '__main__':
    main()
