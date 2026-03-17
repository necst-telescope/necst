import os
import rclpy
from rclpy.node import Node
from dotenv import load_dotenv
from functools import partial

from influxdb_client import InfluxDBClient, Point, WriteOptions, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS, ASYNCHRONOUS

from neclib import config
from .. import namespace, topic
from ..utils import Topic
from necst_msgs.msg import WeatherMsg, CoordMsg, DeviceReading


load_dotenv()


class Monitor(Node): # Assuming DeviceNode is a Node, changed to Node for clarity as DeviceNode is not defined in snippet
    NodeName = "monitor"
    Namespace = namespace.rx

    def __init__(self) -> None:
        super().__init__(self.NodeName, namespace=self.Namespace)

        # InfluxDB Configuration
        url = os.getenv("INFLUXDB_URL")
        token = os.getenv("INFLUXDB_TOKEN")
        org = os.getenv("INFLUXDB_ORG")
        self.bucket = os.getenv("INFLUXDB_BUCKET")

        if not all([url, token, org, self.bucket]):
            self.logger.error("InfluxDB settings are missing from the .env file.")
            exit(1)

        # InfluxDB Client Setup
        self.client = InfluxDBClient(url=url, token=token, org=org)
        # Enable batching to stay within free tier limits.
        self.write_api = self.client.write_api(
            write_options=WriteOptions(batch_size=50, flush_interval=1000)
        )

        self.subscriptions_list = {}

        # 1. Subscribe to Static (Core) Topics
        self.static_configs = [
            (topic.weather, "weather", {"loc": "outdoor"}, ["temperature", "humidity", "pressure"]),
            (topic.antenna_encoder, "antenna", {"host": "antenna-pc"}, ["lon", "lat"]),
        ]
        for t_obj, measurement, tags, fields in self.static_configs:
            self._subscribe(t_obj._qualname, t_obj.msg_type, measurement, tags, fields)

        # 2. Dynamic Discovery for Branched Topics (Thermometers, Attenuators, etc.)
        # Discovery period: 10 seconds
        self.create_timer(10.0, self.discover_topics)
        self.discover_topics()

        self.logger.info("Monitor Node Started with generic discovery.")

    def _subscribe(self, topic_name, msg_type, measurement, tags, fields):
        """Register a new subscription if not already tracked."""
        if topic_name in self.subscriptions_list:
            return

        handler = partial(
            self.handle_msg,
            measurement=measurement,
            tags=tags,
            fields=fields
        )
        sub = self.create_subscription(msg_type, topic_name, handler, 10)
        self.subscriptions_list[topic_name] = sub
        self.logger.info(f"New subscription: {topic_name} -> [{measurement}] with tags {tags}")

    def discover_topics(self):
        """Scan the ROS 2 network and neclib config for branched topics."""
        import inspect

        # Identify all 'branched' (subscriptable) topic definitions in our system
        for name, topic_obj in inspect.getmembers(topic):
            if isinstance(topic_obj, Topic) and topic_obj.support_index:
                
                # Option A: Discovery via ROS 2 network scan
                try:
                    children = topic_obj.get_children(self)
                    for branch_name, child_topic in children.items():
                        # Default field handling (DeviceReading has 'value')
                        fields = ["value"] 
                        # Use branch name as a tag for differentiation
                        tags = {"id": branch_name}
                        self._subscribe(child_topic._qualname, child_topic.msg_type, name, tags, fields)
                except Exception as e:
                    self.logger.debug(f"Discovery failed for {name} via ROS 2 scan: {e}")

                # Option B: Discovery via neclib config (if hardware is defined but node not started)
                try:
                    # Generic lookup for 'channel' key in config sections (e.g., config.thermometer.channel)
                    config_section = getattr(config, name, None)
                    if config_section and hasattr(config_section, 'channel'):
                        for sensor_name in config_section.channel.keys():
                            child_topic = topic_obj[sensor_name]
                            fields = ["value"]
                            tags = {"id": sensor_name}
                            self._subscribe(child_topic._qualname, child_topic.msg_type, name, tags, fields)
                except Exception as e:
                    self.logger.debug(f"Discovery failed for {name} via neclib config: {e}")

    def handle_msg(self, msg, measurement: str, tags: dict, fields: list):
        """Generic message handler to convert ROS 2 messages to InfluxDB points."""
        try:
            point = Point(measurement)
            for k, v in tags.items():
                point.tag(k, v)

            for f in fields:
                val = getattr(msg, f)
                point.field(f, float(val))

            self.write_api.write(bucket=self.bucket, record=point)
        except Exception as e:
            self.logger.error(f"Failed to send [{measurement}]: {e}", throttle_duration_sec=10)

    def destroy_node(self):
        self.client.close()
        super().destroy_node()


def main(args=None):
    rclpy.init(args=args)
    node = Monitor()
    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    finally:
        node.destroy_node()
        rclpy.shutdown()

if __name__ == '__main__':
    main()
