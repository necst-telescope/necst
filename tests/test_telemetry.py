import os
from types import SimpleNamespace

from necst.utils.env_file import load
from necst.telemetry import (
    MetricSample,
    TelemetryConfig,
    TelemetryField,
    TelemetryTopic,
    TopicRef,
    build_metric_payload,
    discover_topic_refs,
    metric_name_for_topic,
    scalar_fields,
)


class FakeMessage:
    def __init__(self):
        self.ok = True
        self.temperature = 12.5
        self.invalid = float("nan")
        self.name = "ignored"
        self.values = [1, 2]
        self.nested = SimpleNamespace()
        self.nested.value = 3
        self.nested.get_fields_and_field_types = lambda: {"value": "int32"}

    def get_fields_and_field_types(self):
        return {
            "ok": "boolean",
            "temperature": "float32",
            "invalid": "float32",
            "name": "string",
            "values": "sequence<int32>",
            "nested": "SomeMessage",
        }


class FakeNode:
    def get_topic_names_and_types(self):
        return [
            ("/necst/OMU1P85M/status/out", ["example/msg/Status"]),
            ("/necst/OMU1P85M/status/in", ["example/msg/Status"]),
        ]


class FakeNodeWithCollision:
    def get_topic_names_and_types(self):
        return [("/status", ["a/msg/Status", "b/msg/Status"])]


class FakeWeatherNode:
    def get_topic_names_and_types(self):
        return [
            ("/weather/ambient/out", ["example/msg/Weather"]),
            ("/weather/ambient/in", ["example/msg/Weather"]),
            ("/weather/ambiently/out", ["example/msg/Weather"]),
        ]


def test_scalar_fields_skip_sequences_strings_and_nonfinite_values():
    assert scalar_fields(FakeMessage()) == (
        ("ok", 1.0, "bool"),
        ("temperature", 12.5, "float"),
        ("nested.value", 3.0, "int"),
    )


def test_discovery_uses_only_configured_topics():
    topics = [
        TelemetryTopic(
            "/necst/OMU1P85M/status/out",
            (TelemetryField("nested.value", "necst.status.value"),),
        )
    ]

    refs = discover_topic_refs(FakeNode(), topics)

    assert len(refs) == 1
    assert refs[0].name == "/necst/OMU1P85M/status/out"
    assert refs[0].message_type == "example/msg/Status"


def test_discovery_skips_topics_with_multiple_message_types():
    topics = [TelemetryTopic("/status", (TelemetryField("value", "necst.status"),))]

    assert discover_topic_refs(FakeNodeWithCollision(), topics) == ()


def test_discovery_expands_topic_prefix_children():
    topics = [
        TelemetryTopic(
            "/weather/ambient/*",
            (TelemetryField("temperature", "necst.weather.temperature_k"),),
        )
    ]

    refs = discover_topic_refs(FakeWeatherNode(), topics)

    assert [ref.name for ref in refs] == ["/weather/ambient/in", "/weather/ambient/out"]


def test_wildcard_topic_suffix_is_added_to_metric_name():
    field = TelemetryField("temperature", "necst.weather.temperature_k")
    ref = TopicRef(
        "/weather/ambient/out",
        "example/msg/Weather",
        TelemetryTopic("/weather/ambient/*", (field,)),
    )

    assert metric_name_for_topic(field, ref) == "necst.weather.out.temperature_k"


def test_payload_uses_full_topic_and_telescope_attributes():
    payload = build_metric_payload(
        [
            MetricSample(
                metric_name="necst.status.value",
                topic="/topic/out",
                message_type="example/msg/Status",
                field_path="nested.value",
                value=3.0,
                value_kind="int",
            )
        ],
        "NANTEN2",
    )
    assert len(payload) == 1
    batch = payload[0]
    metric = batch["metrics"][0]
    assert "timestamp" not in metric
    assert isinstance(batch["common"]["timestamp"], int)
    assert batch["common"]["attributes"]["telescope"] == "NANTEN2"
    assert metric["attributes"]["ros_topic"] == "/topic/out"
    assert metric["attributes"]["field_path"] == "nested.value"
    assert metric["name"] == "necst.status.value"


def test_config_clamps_intervals_and_parses_topics():
    config = TelemetryConfig.from_mapping(
        {
            "post_interval_sec": 0,
            "discovery_interval_sec": "bad",
            "topics": [
                {
                    "topic": "status/out",
                    "fields": [
                        {"path": "value", "metric": "necst.status.value"}
                    ],
                }
            ],
        }
    )
    assert config.post_interval_sec == 0.5
    assert config.discovery_interval_sec == 10.0
    assert config.topics == (
        TelemetryTopic(
            "/status/out",
            (TelemetryField("value", "necst.status.value"),),
        ),
    )


def test_config_parses_grouped_topics():
    config = TelemetryConfig.from_mapping(
        {
            "topics": {
                "encoder": {
                    "topic": "/encoder",
                    "fields": [{"path": "lon", "metric": "necst.encoder.az_deg"}],
                }
            }
        }
    )
    assert config.topics == (
        TelemetryTopic(
            "/encoder",
            (TelemetryField("lon", "necst.encoder.az_deg"),),
        ),
    )


def test_env_file_does_not_override_existing_environment(tmp_path, monkeypatch):
    path = tmp_path / ".env"
    path.write_text(
        "NEW_RELIC_LICENSE_KEY='from-file'\nTELESCOPE=from-file\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("TELESCOPE", "from-process")
    monkeypatch.delenv("NEW_RELIC_LICENSE_KEY", raising=False)

    load(path)

    assert os.environ["NEW_RELIC_LICENSE_KEY"] == "from-file"
    assert os.environ["TELESCOPE"] == "from-process"
