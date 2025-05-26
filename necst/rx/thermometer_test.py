import rclpy
from necst_msgs.msg import DeviceReading
from ..core import DeviceNode
from .. import qos, namespace

# import sqlite3


class ThermometerSubscriber(DeviceNode):
    NodeName = "thermometer_test"
    Namespace = namespace.rx

    def __init__(self):
        super().__init__(self.NodeName, namespace=self.Namespace)

        name = "/necst/OMU1P85M/rx/thermometer/"
        topic_names = ["Shield40K1", "Shield40K2", "Shield4K1", "Shield4K2"]
        self.data_dict = {}

        for topic_name in topic_names.split(","):
            self.subscription = self.create_subscription(
                DeviceReading,
                f"{name}/{topic_name}",
                self.listener_callback,
                qos.realtime,
            )

    def listener_callback(self, msg: DeviceReading):
        self.data_dict[msg.id] = msg.value
        print(
            f"Received - ID: {msg.id}, Value: {msg.value:.2f} K, Time: {msg.time:.0f}"
        )

    #     self.record_db(msg)

    # def record_db(self, msg: DeviceReading):
    #     conn = sqlite3.connect("thermometer_data.db")
    #     cursor = conn.cursor()
    #     cursor.execute(
    #         "CREATE TABLE IF NOT EXISTS readings (id TEXT, value REAL, time REAL)"
    #     )
    #     cursor.execute(
    #         "INSERT INTO readings (id, value, time) VALUES (?, ?, ?)",
    #         (msg.id, msg.value, msg.time),
    #     )
    #     conn.commit()
    #     conn.close()


def main(args=None):
    rclpy.init(args=args)
    node = ThermometerSubscriber()
    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    finally:
        node.io.close()
        node.destroy_node()
        rclpy.try_shutdown()


if __name__ == "__main__":
    main()
