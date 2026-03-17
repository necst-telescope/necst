import os
import rclpy
from pathlib import Path
from rclpy.node import Node
from dotenv import load_dotenv
from functools import partial

from influxdb_client import InfluxDBClient, Point, WriteOptions, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS, ASYNCHRONOUS

from neclib import config
from .. import namespace, topic
from ..utils import Topic
from ..core import DeviceNode
from necst_msgs.msg import WeatherMsg, CoordMsg, DeviceReading


env_path = Path(__file__).resolve().parent.parent.parent / ".env"
load_dotenv(dotenv_path=env_path)


class Monitor(DeviceNode): # Assuming DeviceNode is a Node, changed to Node for clarity as DeviceNode is not defined in snippet
    NodeName = "monitor"
    Namespace = namespace.rx

    def __init__(self) -> None:
        super().__init__(self.NodeName, namespace=self.Namespace)
        self.logger = self.get_logger()

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

        # -------------------------------------------------------------
        # 1. Unified Topic Configuration
        # -------------------------------------------------------------
        # Format: (TopicObject, MeasurementName, DefaultTags, FieldListOrNone)
        # If FieldList is None, all fields except metadata will be extracted.
        self.configs = [
            (topic.weather, "weather_station", {"loc": "outdoor"}, ["temperature", "humidity", "pressure"]),
            (topic.antenna_encoder, "encoder", {"host": "antenna-pc"}, None),
            (topic.thermometer, "thermometer", {}, None),
            (topic.attenuator, "attenuator", {}, None),
            (topic.vacuum_gauge, "vacuum_gauge", {}, None),
        ]

        self.subscriptions_list = {}
        
        # Immediate subscription/discovery
        self.discover_all()
        
        # Rescan for new branches periodically (every 10s)
        self.create_timer(10.0, self.discover_all)

        self.logger.info(f"Monitor Node Started. Tracking {len(self.configs)} categories.")

    def _subscribe(self, topic_name, msg_type, measurement, tags, fields_config, qos):
        """Register a new subscription if not already tracked."""
        if topic_name in self.subscriptions_list:
            return

        handler = partial(
            self.handle_msg,
            measurement=measurement,
            tags=tags,
            fields_config=fields_config
        )
        sub = self.create_subscription(msg_type, topic_name, handler, qos)
        self.subscriptions_list[topic_name] = sub
        self.logger.info(f"Subscribed: {topic_name} -> [{measurement}]")

    def discover_all(self):
        """Iterate through configs and either subscribe directly or discover branches."""
        for t_obj, measurement, tags, fields in self.configs:
            if not t_obj.support_index:
                # Core topic: Subscribe directly
                self._subscribe(
                    t_obj._qualname,
                    t_obj.msg_type,
                    measurement,
                    tags,
                    fields,
                    qos=t_obj.qos_profile,
                )
            else:
                # Branched topic: Discover via ROS 2 scan and neclib config
                self._discover_branches(t_obj, measurement, tags, fields)

    def _discover_branches(self, t_obj, measurement, base_tags, fields):
        """Helper to find children for subscriptable topics."""
        # A: ROS 2 network scan
        try:
            children = t_obj.get_children(self)
            for branch_name, child_topic in children.items():
                tags = base_tags.copy()
                tags["id"] = branch_name
                self._subscribe(
                    child_topic._qualname,
                    child_topic.msg_type,
                    measurement,
                    tags,
                    fields,
                    qos=child_topic.qos_profile,
                )
        except Exception as e:
            self.logger.debug(f"Scan failed for {measurement}: {e}")

        # B: neclib config lookup
        try:
            config_section = getattr(config, measurement, None)
            if config_section and hasattr(config_section, 'channel'):
                for sensor_name in config_section.channel.keys():
                    child_topic = t_obj[sensor_name]
                    tags = base_tags.copy()
                    tags["id"] = sensor_name
                    self._subscribe(
                        child_topic._qualname,
                        child_topic.msg_type,
                        measurement,
                        tags,
                        fields,
                        qos=child_topic.qos_profile,
                    )
        except:
            pass

    def handle_msg(self, msg, measurement: str, tags: dict, fields_config: list):
        """Generic message handler. If fields_config is None, send all fields."""
        try:
            point = Point(measurement)
            for k, v in tags.items():
                point.tag(k, v)

            # Automatic field extraction
            fields = fields_config if fields_config else msg.get_fields_and_field_types().keys()

            for f in fields:
                if f in ["time", "id", "header"]: # Skip metadata/tags
                    continue
                val = getattr(msg, f)
                try:
                    point.field(f, float(val))
                except (TypeError, ValueError):
                    point.field(f, str(val))

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
