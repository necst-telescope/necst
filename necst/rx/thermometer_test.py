import time
import sqlite3

import rclpy
from necst_msgs.msg import DeviceReading
from necst.core import DeviceNode
from necst import qos, namespace


class ThermometerSubscriber(DeviceNode):
    NodeName = "thermometer_sqlite_logger"
    Namespace = namespace.rx

    def __init__(self):
        super().__init__(self.NodeName, namespace=self.Namespace)
        self.get_logger().info("Started ThermometerSQLiteLogger…")

        self.db_path = "thermometer_data.db"
        self.conn = sqlite3.connect(self.db_path)
        self.cur = self.conn.cursor()
        self.cur.execute(
            """
            CREATE TABLE IF NOT EXISTS thermometer_data (
                timestamp REAL,
                Shield40K1 REAL,
                Shield40K2 REAL,
                Stage4K1   REAL,
                Stage4K2   REAL
            )
        """
        )
        self.conn.commit()

        self.topic_names = ["Shield40K1", "Shield40K2", "Stage4K1", "Stage4K2"]
        self.data_dict = {}
        for ch in self.topic_names:
            topic_path = f"/necst/OMU1P85M/rx/thermometer/{ch}"
            self.create_subscription(
                DeviceReading,
                topic_path,
                self._make_cb(ch),
                qos.realtime,
            )

        self.create_timer(1.0, self._flush_if_complete)

    def _make_cb(self, ch: str):
        def _cb(msg: DeviceReading):
            self.data_dict[ch] = msg.value
            self.get_logger().info(f"{ch} ← {msg.value:.2f} K @ {msg.time:.0f}")

        return _cb

    def _flush_if_complete(self):
        if len(self.data_dict) == len(self.topic_names):
            ts = time.time()
            cols = ", ".join(self.topic_names)
            placeholders = ", ".join(["?"] * (len(self.topic_names) + 1))
            sql = f"INSERT INTO thermometer_data (timestamp, {cols}) VALUES ({placeholders})"
            params = [ts] + [self.data_dict[ch] for ch in self.topic_names]
            self.cur.execute(sql, params)
            self.conn.commit()
            self.get_logger().info(f"Saved @ {ts:.0f}: {self.data_dict}")
            self.data_dict.clear()

    def destroy_node(self):
        try:
            if self.conn:
                self.conn.close()
        except Exception:
            pass
        super().destroy_node()


def main(args=None):
    rclpy.init(args=args)
    node = ThermometerSubscriber()
    try:
        rclpy.spin(node)
    finally:
        node.destroy_node()
        rclpy.shutdown()


if __name__ == "__main__":
    main()
