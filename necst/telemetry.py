"""Publish selected ROS telemetry to the New Relic Metric API."""

from __future__ import annotations

import argparse
import gzip
import json
import math
import os
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Sequence, Tuple
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from .web import site_config

DEFAULT_METRIC_ENDPOINT = "https://metric-api.newrelic.com/metric/v1"
MAX_RETRIES = 3
MAX_METRICS_PER_REQUEST = 500


@dataclass(frozen=True)
class TelemetryField:
    path: str
    metric_name: str


@dataclass(frozen=True)
class TelemetryTopic:
    name: str
    fields: Tuple[TelemetryField, ...]


@dataclass(frozen=True)
class TopicRef:
    name: str
    message_type: str
    config: TelemetryTopic


@dataclass(frozen=True)
class TelemetryConfig:
    post_interval_sec: float = 10.0
    discovery_interval_sec: float = 10.0
    metric_endpoint: str = DEFAULT_METRIC_ENDPOINT
    topics: Sequence[TelemetryTopic] = ()

    @classmethod
    def from_mapping(cls, mapping: Mapping[str, Any] | None) -> "TelemetryConfig":
        raw = mapping if isinstance(mapping, Mapping) else {}

        def interval(name: str) -> float:
            try:
                value = float(raw.get(name, 10.0))
            except (TypeError, ValueError):
                return 10.0
            return max(0.5, value) if math.isfinite(value) else 10.0

        endpoint = str(
            os.environ.get(
                "NEW_RELIC_METRIC_ENDPOINT",
                raw.get("metric_endpoint", DEFAULT_METRIC_ENDPOINT),
            )
            or DEFAULT_METRIC_ENDPOINT
        ).strip()
        topics: List[TelemetryTopic] = []
        raw_topics = raw.get("topics", [])
        if isinstance(raw_topics, list):
            for raw_topic in raw_topics:
                if not isinstance(raw_topic, Mapping):
                    continue
                topic_name = normalize_topic_name(raw_topic.get("topic"))
                raw_fields = raw_topic.get("fields", [])
                if not topic_name or not isinstance(raw_fields, list):
                    continue
                fields: List[TelemetryField] = []
                for raw_field in raw_fields:
                    if not isinstance(raw_field, Mapping):
                        continue
                    path = str(raw_field.get("path") or "").strip().strip(".")
                    metric_name = str(raw_field.get("metric") or "").strip()
                    if path and metric_name:
                        fields.append(TelemetryField(path, metric_name))
                if fields:
                    topics.append(TelemetryTopic(topic_name, tuple(fields)))
        return cls(
            post_interval_sec=interval("post_interval_sec"),
            discovery_interval_sec=interval("discovery_interval_sec"),
            metric_endpoint=endpoint,
            topics=tuple(topics),
        )


@dataclass(frozen=True)
class MetricSample:
    metric_name: str
    topic: str
    message_type: str
    field_path: str
    value: float
    value_kind: str


def normalize_topic_name(name: Any) -> str:
    text = str(name or "").strip()
    while "//" in text:
        text = text.replace("//", "/")
    if text and not text.startswith("/"):
        text = "/" + text
    return text.rstrip("/") or "/"


def discover_topic_refs(
    ros_node: Any, configured_topics: Iterable[TelemetryTopic]
) -> Tuple[TopicRef, ...]:
    """Discover message types for explicitly configured Topics."""

    configured = {topic.name: topic for topic in configured_topics}
    refs: List[TopicRef] = []
    for topic_name, message_types in ros_node.get_topic_names_and_types():
        topic = configured.get(normalize_topic_name(topic_name))
        types = {str(message_type).strip() for message_type in message_types}
        if topic is not None and len(types) == 1:
            refs.append(TopicRef(topic.name, next(iter(types)), topic))
    return tuple(sorted(refs, key=lambda ref: (ref.name, ref.message_type)))


def scalar_fields(message: Any, prefix: str = "") -> Tuple[Tuple[str, float, str], ...]:
    """Return finite scalar leaves; sequences and strings are skipped."""

    fields = getattr(message, "get_fields_and_field_types", lambda: {})()
    result: List[Tuple[str, float, str]] = []
    for field_name in fields:
        value = getattr(message, field_name, None)
        path = f"{prefix}.{field_name}" if prefix else field_name
        if isinstance(value, bool):
            result.append((path, float(value), "bool"))
        elif isinstance(value, int):
            result.append((path, float(value), "int"))
        elif isinstance(value, float) and math.isfinite(value):
            result.append((path, value, "float"))
        elif hasattr(value, "get_fields_and_field_types"):
            result.extend(scalar_fields(value, path))
    return tuple(result)


def build_metric_payload(
    samples: Iterable[MetricSample], telescope: str
) -> List[Dict[str, Any]]:
    metrics = []
    for sample in samples:
        metrics.append(
            {
                "name": sample.metric_name,
                "type": "gauge",
                "value": sample.value,
                "attributes": {
                    "ros_topic": sample.topic,
                    "field_path": sample.field_path,
                    "ros_type": sample.message_type,
                    "value_kind": sample.value_kind,
                },
            }
        )
    return [
        {
            "common": {
                "timestamp": int(time.time()),
                "attributes": {"telescope": telescope},
            },
            "metrics": metrics,
        }
    ]


class NewRelicMetricClient:
    def __init__(
        self, api_key: str, endpoint: str, timeout_sec: float = 5.0
    ) -> None:
        self._api_key = api_key
        self._endpoint = endpoint
        self._timeout_sec = timeout_sec

    def send(self, payload: Sequence[Mapping[str, Any]]) -> None:
        body = gzip.compress(json.dumps(payload, separators=(",", ":")).encode())
        request = Request(
            self._endpoint,
            data=body,
            headers={
                "Api-Key": self._api_key,
                "Content-Type": "application/json",
                "Content-Encoding": "gzip",
            },
            method="POST",
        )
        for attempt in range(MAX_RETRIES):
            try:
                with urlopen(request, timeout=self._timeout_sec) as response:
                    if not 200 <= response.status < 300:
                        raise RuntimeError(
                            f"Metric API returned HTTP {response.status}"
                        )
                    return
            except HTTPError as exc:
                if exc.code != 429 and not 500 <= exc.code < 600:
                    raise
                if attempt == MAX_RETRIES - 1:
                    raise
                retry_after = exc.headers.get("Retry-After", "1")
                try:
                    delay = min(10.0, max(0.1, float(retry_after)))
                except ValueError:
                    delay = 1.0
                time.sleep(delay)
            except URLError:
                if attempt == MAX_RETRIES - 1:
                    raise
                time.sleep(1.0)


class _TelemetryNodeMixin:
    def _telemetry_init(
        self,
        summary: site_config.SiteConfigSummary,
        settings: TelemetryConfig,
        client: NewRelicMetricClient,
        telescope: str,
    ) -> None:
        from . import definitions
        from .utils import import_msg

        self._summary = summary
        self._settings = settings
        self._client = client
        self._telescope = telescope
        self._definitions = definitions
        self._import_msg = import_msg
        self._telemetry_subscriptions: Dict[Tuple[str, str], Any] = {}
        self._latest: Dict[Tuple[str, str, str], MetricSample] = {}
        self._latest_lock = threading.Lock()
        self._stop_event = threading.Event()
        self._discovery_reported = False
        self._send_success_reported = False
        self._sender = threading.Thread(
            target=self._send_loop,
            name="necst-telemetry-sender",
            daemon=True,
        )
        self.create_timer(settings.discovery_interval_sec, self._discover)
        self._discover()
        self.get_logger().info(
            f"telemetry started: telescope={telescope}, "
            f"subscriptions={len(self._telemetry_subscriptions)}, "
            f"post_interval={settings.post_interval_sec}s"
        )
        self._sender.start()

    def _discover(self) -> None:
        try:
            refs = discover_topic_refs(self, self._settings.topics)
        except Exception as exc:
            self.get_logger().warning(f"telemetry Topic discovery failed: {exc}")
            return

        current = {(ref.name, ref.message_type): ref for ref in refs}
        for key, subscription in list(self._telemetry_subscriptions.items()):
            if key not in current:
                self.destroy_subscription(subscription)
                del self._telemetry_subscriptions[key]
        for key, ref in current.items():
            if key in self._telemetry_subscriptions:
                continue
            try:
                message_type = self._import_msg(ref.message_type)
                subscription = self.create_subscription(
                    message_type,
                    ref.name,
                    lambda message, topic_ref=ref: self._record(topic_ref, message),
                    self._definitions.qos.adaptive(ref.name, self),
                )
            except Exception as exc:
                self.get_logger().warning(
                    f"telemetry subscription failed for {ref.name}: {exc}"
                )
                continue
            self._telemetry_subscriptions[key] = subscription

        if not self._discovery_reported:
            self.get_logger().info(
                f"telemetry subscriptions ready: {len(self._telemetry_subscriptions)}"
            )
            self._discovery_reported = True

    def _record(
        self,
        ref: TopicRef,
        message: Any,
    ) -> None:
        fields = {field.path: field for field in ref.config.fields}
        with self._latest_lock:
            for field_path, value, value_kind in scalar_fields(message):
                field = fields.get(field_path)
                if field is None:
                    continue
                key = (ref.name, ref.message_type, field_path)
                self._latest[key] = MetricSample(
                    metric_name=field.metric_name,
                    topic=ref.name,
                    message_type=ref.message_type,
                    field_path=field_path,
                    value=value,
                    value_kind=value_kind,
                )

    def _send_loop(self) -> None:
        while not self._stop_event.wait(self._settings.post_interval_sec):
            with self._latest_lock:
                samples = tuple(self._latest.values())
            if not samples:
                continue
            try:
                sent_count = 0
                for start in range(0, len(samples), MAX_METRICS_PER_REQUEST):
                    batch = samples[start : start + MAX_METRICS_PER_REQUEST]
                    self._client.send(build_metric_payload(batch, self._telescope))
                    sent_count += len(batch)
                if not self._send_success_reported:
                    self.get_logger().info(
                        f"telemetry Metric API send succeeded: metrics={sent_count}"
                    )
                    self._send_success_reported = True
            except Exception as exc:
                self.get_logger().error(f"telemetry Metric API send failed: {exc}")

    def _telemetry_destroy(self) -> None:
        self._stop_event.set()
        self._sender.join(timeout=2.0)


def _make_node(
    summary: site_config.SiteConfigSummary,
    settings: TelemetryConfig,
    client: NewRelicMetricClient,
    telescope: str,
) -> Any:
    from rclpy.node import Node

    class TelemetryNode(_TelemetryNodeMixin, Node):
        def __init__(self) -> None:
            Node.__init__(self, "necst_telemetry")
            self._telemetry_init(summary, settings, client, telescope)

        def destroy_node(self) -> Any:
            self._telemetry_destroy()
            return Node.destroy_node(self)

    return TelemetryNode()


def main(args: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--site-config", default=None)
    options = parser.parse_args(args)

    summary = site_config.resolve_site_config(site_config_path=options.site_config)
    if summary.env_file:
        from .utils import env_file

        env_file.load(Path(os.path.expanduser(summary.env_file)))
    settings = TelemetryConfig.from_mapping(summary.telemetry)
    if not settings.topics:
        raise RuntimeError("telemetry requires [telemetry.topics]")
    telescope = os.environ.get("TELESCOPE", "").strip()
    if not telescope:
        raise RuntimeError("TELESCOPE is required for telemetry")
    api_key = os.environ.get("NEW_RELIC_LICENSE_KEY", "").strip()
    if not api_key:
        raise RuntimeError("NEW_RELIC_LICENSE_KEY is required for telemetry")

    import rclpy

    rclpy.init()
    node = _make_node(
        summary,
        settings,
        NewRelicMetricClient(api_key, settings.metric_endpoint),
        telescope,
    )
    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    finally:
        node.destroy_node()
        rclpy.try_shutdown()


if __name__ == "__main__":
    main()
