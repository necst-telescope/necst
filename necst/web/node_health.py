"""Read-only ROS node health evaluation for the operator console.

Phase 1 intentionally checks only whether configured ROS node names are present
in the ROS graph.  It never starts, stops, restarts, or kills any process.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
import math
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Set


VALID_CLASSES = {
    "manual_mount_required",
    "observation_required",
    "optional",
    "debug",
}


@dataclass(frozen=True)
class ExpectedNode:
    name: str
    label: str
    node_class: str = "optional"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "label": self.label,
            "class": self.node_class,
        }


@dataclass(frozen=True)
class NodeHealthConfig:
    enabled: bool = False
    poll_interval_sec: float = 2.0
    nodes: Sequence[ExpectedNode] = field(default_factory=tuple)
    warnings: Sequence[str] = field(default_factory=tuple)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "enabled": bool(self.enabled),
            "poll_interval_sec": float(self.poll_interval_sec),
            "nodes": [node.to_dict() for node in self.nodes],
            "warnings": list(self.warnings),
        }


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace(
        "+00:00", "Z"
    )


def normalize_node_name(name: Any) -> str:
    text = str(name or "").strip()
    if not text:
        return ""
    while "//" in text:
        text = text.replace("//", "/")
    if len(text) > 1:
        text = text.rstrip("/")
    return text


def full_node_name(name: Any, namespace: Any = "/") -> str:
    node = str(name or "").strip().strip("/")
    ns = str(namespace or "/").strip()
    if not node:
        return ""
    if not ns or ns == "/":
        return "/" + node
    return normalize_node_name(ns.rstrip("/") + "/" + node)


def config_from_mapping(config: Mapping[str, Any]) -> NodeHealthConfig:
    """Parse ``[console.health]`` from a site TOML mapping.

    Missing ``[console.health]`` means fully disabled.  Invalid entries are kept
    out of the expected-node list and reported as warnings so the console can
    start normally.
    """

    console = config.get("console") if isinstance(config.get("console"), Mapping) else None
    health = console.get("health") if isinstance(console, Mapping) else None
    if not isinstance(health, Mapping):
        return NodeHealthConfig(enabled=False)

    warnings: List[str] = []
    enabled = bool(health.get("enabled", True))
    poll_raw = health.get("poll_interval_sec", 2.0)
    try:
        poll_interval = float(poll_raw)
        if not math.isfinite(poll_interval) or poll_interval <= 0:
            raise ValueError
        poll_interval = max(0.5, poll_interval)
    except Exception:
        poll_interval = 2.0
        warnings.append(
            f"invalid console.health.poll_interval_sec={poll_raw!r}; using 2.0 sec"
        )

    raw_nodes = health.get("nodes", [])
    if raw_nodes is None:
        raw_nodes = []
    if not isinstance(raw_nodes, list):
        warnings.append("console.health.nodes must be an array of tables")
        raw_nodes = []

    nodes: List[ExpectedNode] = []
    seen: Set[str] = set()
    for index, item in enumerate(raw_nodes):
        if not isinstance(item, Mapping):
            warnings.append(f"console.health.nodes[{index}] is not a table; ignored")
            continue
        name = normalize_node_name(item.get("name"))
        if not name:
            warnings.append(f"console.health.nodes[{index}] has no name; ignored")
            continue
        if not name.startswith("/"):
            warnings.append(
                f"console.health.nodes[{index}] name={name!r} is not fully qualified; ignored"
            )
            continue
        if name in seen:
            warnings.append(f"duplicate console.health node {name!r}; ignored")
            continue
        seen.add(name)
        label_raw = str(item.get("label") or "").strip()
        label = label_raw or name.rsplit("/", 1)[-1] or name
        node_class = str(item.get("class") or "optional").strip()
        if node_class not in VALID_CLASSES:
            warnings.append(
                f"unknown console.health class {node_class!r} for {name}; treating as optional"
            )
            node_class = "optional"
        nodes.append(ExpectedNode(name=name, label=label, node_class=node_class))

    return NodeHealthConfig(
        enabled=enabled,
        poll_interval_sec=poll_interval,
        nodes=tuple(nodes),
        warnings=tuple(warnings),
    )


def _status_class(value: str) -> str:
    if value == "OK":
        return "ok"
    if value == "DEGRADED":
        return "warn"
    if value == "NG":
        return "bad"
    return "warn"


def evaluate(
    config: NodeHealthConfig,
    actual_nodes: Optional[Iterable[str]],
    *,
    error: Optional[str] = None,
    checked_utc: Optional[str] = None,
) -> Dict[str, Any]:
    """Return the node_health status payload for ``/api/status``."""

    if not config.enabled:
        return {"enabled": False}

    checked = checked_utc or _utc_now_iso()
    base: Dict[str, Any] = {
        "enabled": True,
        "poll_interval_sec": float(config.poll_interval_sec),
        "last_checked_utc": checked,
        "config_warnings": list(config.warnings),
    }
    if error:
        base.update(
            {
                "system": "UNKNOWN",
                "manual_mount": "UNKNOWN",
                "observation": "UNKNOWN",
                "summary_class": "warn",
                "error": str(error),
                "missing_required": [],
                "missing_optional": [],
                "nodes": [
                    {**node.to_dict(), "present": None} for node in config.nodes
                ],
            }
        )
        return base

    if not config.nodes:
        base.update(
            {
                "system": "UNKNOWN",
                "manual_mount": "UNKNOWN",
                "observation": "UNKNOWN",
                "summary_class": "warn",
                "error": "no expected ROS nodes configured in [console.health]",
                "missing_required": [],
                "missing_optional": [],
                "nodes": [],
            }
        )
        return base

    actual: Set[str] = {normalize_node_name(name) for name in (actual_nodes or [])}
    actual.discard("")
    node_payload: List[Dict[str, Any]] = []
    missing_manual: List[str] = []
    missing_observation: List[str] = []
    missing_optional: List[str] = []
    missing_debug: List[str] = []

    for node in config.nodes:
        present = node.name in actual
        row = {**node.to_dict(), "present": bool(present)}
        node_payload.append(row)
        if present:
            continue
        if node.node_class == "manual_mount_required":
            missing_manual.append(node.label)
        elif node.node_class == "observation_required":
            missing_observation.append(node.label)
        elif node.node_class == "debug":
            missing_debug.append(node.label)
        else:
            missing_optional.append(node.label)

    missing_required = missing_manual + missing_observation
    manual_mount = "NG" if missing_manual else "OK"
    observation = "NG" if missing_required else "OK"
    if missing_required:
        system = "NG"
    elif missing_optional:
        system = "DEGRADED"
    else:
        system = "OK"

    base.update(
        {
            "system": system,
            "manual_mount": manual_mount,
            "observation": observation,
            "summary_class": _status_class(system),
            "missing_required": missing_required,
            "missing_manual_mount": missing_manual,
            "missing_observation": missing_observation,
            "missing_optional": missing_optional,
            "missing_debug": missing_debug,
            "nodes": node_payload,
            "actual_node_count": len(actual),
        }
    )
    return base
