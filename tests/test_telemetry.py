import os
from types import SimpleNamespace

from necst.utils.env_file import load
from necst.telemetry import (
    MetricSample,
    TelemetryConfig,
    build_metric_payload,
    discover_topic_refs,
    scalar_fields,
    unambiguous_topic_refs,
)
from necst.web.node_health import ExpectedNode


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
    def get_publisher_names_and_types_by_node(self, node_name, namespace):
        assert namespace == "/necst/OMU1P85M/ctrl"
        assert node_name in {"antenna_a", "antenna_b"}
        return [
            ("/necst/OMU1P85M/status/out", ["example/msg/Status"]),
            ("/necst/OMU1P85M/status/in", ["example/msg/Status"]),
        ]


def test_scalar_fields_skip_sequences_strings_and_nonfinite_values():
    assert scalar_fields(FakeMessage()) == (
        ("ok", 1.0, "bool"),
        ("temperature", 12.5, "float"),
        ("nested.value", 3.0, "int"),
    )


def test_discovery_keeps_nested_topics_and_deduplicates_parallel_publishers():
    nodes = [
        ExpectedNode("/necst/OMU1P85M/ctrl/antenna_a", "a"),
        ExpectedNode("/necst/OMU1P85M/ctrl/antenna_b", "b"),
    ]
    refs = discover_topic_refs(FakeNode(), nodes)
    assert [ref.name for ref in refs] == [
        "/necst/OMU1P85M/status/in",
        "/necst/OMU1P85M/status/out",
    ]
    assert refs[0].publisher_nodes == tuple(node.name for node in nodes)


def test_type_collision_is_not_subscribed():
    refs = (
        SimpleNamespace(
            name="/status", message_type="a/msg/Status", publisher_nodes=("/a",)
        ),
        SimpleNamespace(
            name="/status", message_type="b/msg/Status", publisher_nodes=("/b",)
        ),
    )
    assert unambiguous_topic_refs(refs) == ()


def test_payload_uses_full_topic_and_telescope_attributes():
    payload = build_metric_payload(
        [
            MetricSample(
                "/topic/out",
                "example/msg/Status",
                "nested.value",
                3.0,
                "int",
                ("/node",),
                1700000000,
            )
        ],
        "NANTEN2",
    )
    metric = payload["metrics"][0]
    assert payload["common"]["attributes"]["telescope"] == "NANTEN2"
    assert metric["attributes"]["ros_topic"] == "/topic/out"
    assert metric["attributes"]["field_path"] == "nested.value"
    assert metric["attributes"]["source_node"] == "/node"


def test_config_defaults_to_disabled_and_clamps_intervals():
    config = TelemetryConfig.from_mapping(
        {"enabled": True, "post_interval_sec": 0, "discovery_interval_sec": "bad"}
    )
    assert config.enabled is True
    assert config.post_interval_sec == 0.5
    assert config.discovery_interval_sec == 10.0


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
