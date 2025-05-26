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

        topic_names = ["Shield40K1", "Shield40K2", "Stage4K1", "Stage4K2"]
        self.data_dict = {}
        self.subscriptions = {}
        for ch in topic_names:
            topic_path = f"/necst/OMU1P85M/rx/thermometer/{ch}"
            self.subscriptions[ch] = self.create_safe_subscription(
                DeviceReading,
                topic_path,
                self.listener_callback,
                qos.adaptive(ch, self),
            )

    def listener_callback(self, msg: DeviceReading):
        self.data_dict[msg.id] = msg.value
        print(self.data_dict)
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
