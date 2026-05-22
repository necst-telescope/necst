"""Signal-generator apply/verify helpers for spectral recording LO profiles.

This module implements the PR3 SG layer without importing ROS at module import
time.  The pure functions are unit-testable with fake readback objects; the CLI
imports ``necst.core.Commander`` only when an actual apply/verify command is
requested.

Frequency convention
--------------------
``sg_set_frequency_hz`` in ``lo_profile.toml`` is the frequency sent to the
physical signal generator.  Commander currently publishes ``LocalSignal.freq``
in GHz, so readback objects with a ``freq`` attribute are interpreted as GHz
unless an explicit ``frequency_hz``/``sg_readback_frequency_hz`` field exists.
"""

from __future__ import annotations

import argparse
import json
import math
import time as pytime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

try:  # direct script execution fallback
    from .spectral_recording_setup import read_toml
except Exception:  # pragma: no cover
    from spectral_recording_setup import read_toml  # type: ignore


class SpectralRecordingSGError(RuntimeError):
    """Base error for SG apply/verify failures."""


class SpectralRecordingSGValidationError(SpectralRecordingSGError):
    """Raised when a profile/readback pair violates strict SG semantics."""


def _get_value(obj: Any, *names: str, default: Any = None) -> Any:
    if isinstance(obj, Mapping):
        for name in names:
            if name in obj:
                return obj[name]
    for name in names:
        if hasattr(obj, name):
            return getattr(obj, name)
    return default


def _readback_frequency_hz(readback: Any) -> float:
    explicit = _get_value(
        readback,
        "sg_readback_frequency_hz",
        "frequency_hz",
        "freq_hz",
        default=None,
    )
    if explicit is not None:
        return float(explicit)

    freq = _get_value(readback, "freq", "frequency", default=None)
    if freq is None:
        raise SpectralRecordingSGValidationError(
            "Readback has no frequency field; expected frequency_hz or freq."
        )
    freq = float(freq)
    # NECST LocalSignal.freq is currently expressed in GHz.  If a fake/device
    # readback supplies a very large value, treat it as Hz for testability.
    if abs(freq) < 1.0e6:
        return freq * 1.0e9
    return freq


def _frequency_alias_hz(
    table: Mapping[str, Any],
    *,
    field_base: str,
    canonical: str,
    aliases: Mapping[str, float],
    required: bool = False,
    default: Optional[float] = None,
) -> float:
    """Resolve one frequency value from explicit unit-suffixed TOML keys.

    The returned value is always Hz.  More than one alias is rejected because
    accepting conflicting unit keys silently would be unsafe for SG control.
    """

    found = [(name, table[name]) for name in aliases if name in table]
    if not found:
        if required:
            names = ", ".join(aliases)
            raise SpectralRecordingSGValidationError(
                f"{field_base}: missing {canonical}; expected one of {names}"
            )
        if default is None:
            raise SpectralRecordingSGValidationError(
                f"{field_base}: no default supplied for optional frequency {canonical}"
            )
        return float(default)
    if len(found) > 1:
        names = ", ".join(name for name, _ in found)
        raise SpectralRecordingSGValidationError(
            f"{field_base}: multiple frequency aliases for {canonical}: {names}. "
            "Specify exactly one unit-suffixed key."
        )
    name, value = found[0]
    return float(value) * float(aliases[name])


def _readback_power_dbm(readback: Any) -> Optional[float]:
    value = _get_value(readback, "power_dbm", "sg_readback_power_dbm", "power", default=None)
    return None if value is None else float(value)


def _readback_time(readback: Any) -> Optional[float]:
    value = _get_value(readback, "time", "readback_time", "timestamp", default=None)
    return None if value is None else float(value)


def _readback_id(readback: Any) -> str:
    value = _get_value(readback, "id", "sg_id", default="")
    return "" if value is None else str(value)


def _readback_output(readback: Any) -> Optional[bool]:
    value = _get_value(
        readback,
        "output_status",
        "output",
        "sg_output_status",
        "output_enabled",
        default=None,
    )
    return None if value is None else bool(value)


def sg_devices_from_profile(lo_profile: Mapping[str, Any]) -> Dict[str, Dict[str, Any]]:
    devices = lo_profile.get("sg_devices", {})
    if not isinstance(devices, Mapping):
        raise SpectralRecordingSGValidationError("lo_profile.sg_devices must be a table.")
    return {str(k): dict(v) for k, v in devices.items()}


def build_sg_apply_plan(lo_profile: Mapping[str, Any], sg_id: Optional[str] = None) -> Dict[str, Dict[str, Any]]:
    devices = sg_devices_from_profile(lo_profile)
    if sg_id is not None:
        if sg_id not in devices:
            raise SpectralRecordingSGValidationError(f"Unknown sg_id: {sg_id!r}")
        devices = {sg_id: devices[sg_id]}

    plan: Dict[str, Dict[str, Any]] = {}
    for key, dev in devices.items():
        output_required = bool(dev.get("output_required", True))
        output_policy = str(dev.get("output_policy", "require_off"))
        entry = {
            "sg_id": key,
            "control_adapter": str(
                dev.get("control_adapter", "necst_commander_signal_generator_set_v1")
            ),
            "sg_set_frequency_hz": _frequency_alias_hz(
                dev,
                field_base=f"sg_devices.{key}",
                canonical="sg_set_frequency_hz",
                aliases={
                    "sg_set_frequency_hz": 1.0,
                    "sg_set_frequency_ghz": 1.0e9,
                    "sg_set_frequency_mhz": 1.0e6,
                },
                required=True,
            ),
            "sg_set_power_dbm": float(dev.get("sg_set_power_dbm", 0.0)),
            "frequency_tolerance_hz": float(dev.get("frequency_tolerance_hz", 0.0)),
            "power_tolerance_db": float(dev.get("power_tolerance_db", 0.0)),
            "output_required": output_required,
            "output_policy": output_policy,
        }
        if entry["control_adapter"] != "necst_commander_signal_generator_set_v1":
            raise SpectralRecordingSGValidationError(
                f"{key}: unsupported control_adapter={entry['control_adapter']!r}"
            )
        if (not output_required) and output_policy not in {"ignore", "require_off"}:
            raise SpectralRecordingSGValidationError(
                f"{key}: output_policy must be ignore or require_off."
            )
        plan[key] = entry
    return plan


def filter_sg_apply_plan(
    plan: Mapping[str, Mapping[str, Any]],
    sg_ids: Iterable[str],
) -> Dict[str, Dict[str, Any]]:
    """Return a deterministic subset of an SG apply/verify plan.

    Missing IDs are rejected so that a typo in `.obs` does not silently disable
    SG preflight checks.
    """

    requested = [str(sg_id) for sg_id in sg_ids]
    missing = [sg_id for sg_id in requested if sg_id not in plan]
    if missing:
        raise SpectralRecordingSGValidationError(
            f"Unknown sg_preflight_ids not present in lo_profile.sg_devices: {missing}"
        )
    return {sg_id: dict(plan[sg_id]) for sg_id in sorted(dict.fromkeys(requested))}


def active_sg_ids_from_snapshot(
    snapshot: Mapping[str, Any],
    *,
    fail_on_legacy_chain: bool = True,
) -> Tuple[str, ...]:
    """Infer active SG IDs from a resolved spectral-recording snapshot.

    The active path is `streams -> lo_chains -> lo_roles -> sg_id`.  Fixed LO
    roles are intentionally skipped.  Legacy inline LO chains (`lo1_hz/sb1/...`)
    do not encode which physical SG generated the LO; if such a chain is active
    and `fail_on_legacy_chain` is true, raise a clear error instead of giving a
    false sense that hardware was checked.
    """

    streams = snapshot.get("streams", {})
    chains = snapshot.get("lo_chains", {})
    roles = snapshot.get("lo_roles", {})
    if not isinstance(streams, Mapping):
        raise SpectralRecordingSGValidationError("snapshot.streams must be a table for SG preflight")
    if not isinstance(chains, Mapping):
        raise SpectralRecordingSGValidationError("snapshot.lo_chains must be a table for SG preflight")
    if not isinstance(roles, Mapping):
        raise SpectralRecordingSGValidationError("snapshot.lo_roles must be a table for SG preflight")

    active_chain_ids = []
    for stream_id, raw_stream in streams.items():
        if not isinstance(raw_stream, Mapping):
            raise SpectralRecordingSGValidationError(f"snapshot.streams.{stream_id} must be a table")
        chain_id = raw_stream.get("lo_chain")
        if chain_id in (None, ""):
            raise SpectralRecordingSGValidationError(f"snapshot.streams.{stream_id}.lo_chain is missing")
        active_chain_ids.append(str(chain_id))

    active_sg_ids = set()
    legacy_chain_ids = []
    for chain_id in sorted(set(active_chain_ids)):
        raw_chain = chains.get(chain_id)
        if not isinstance(raw_chain, Mapping):
            raise SpectralRecordingSGValidationError(
                f"snapshot.streams references unknown lo_chain {chain_id!r}"
            )
        role_ids = raw_chain.get("lo_roles")
        if role_ids is None:
            if "legacy_local_oscillators" in raw_chain or "lo_frequencies_hz" in raw_chain:
                legacy_chain_ids.append(chain_id)
            continue
        if not isinstance(role_ids, Sequence) or isinstance(role_ids, (str, bytes)):
            raise SpectralRecordingSGValidationError(f"snapshot.lo_chains.{chain_id}.lo_roles must be an array")
        for role_id in role_ids:
            role_key = str(role_id)
            raw_role = roles.get(role_key)
            if not isinstance(raw_role, Mapping):
                raise SpectralRecordingSGValidationError(
                    f"snapshot.lo_chains.{chain_id} references unknown lo_role {role_key!r}"
                )
            source = str(raw_role.get("source", "sg_device"))
            if source == "sg_device":
                sg_id = raw_role.get("sg_id")
                if sg_id in (None, ""):
                    raise SpectralRecordingSGValidationError(
                        f"snapshot.lo_roles.{role_key}.sg_id is missing"
                    )
                active_sg_ids.add(str(sg_id))
            elif source == "fixed":
                continue
            else:
                raise SpectralRecordingSGValidationError(
                    f"snapshot.lo_roles.{role_key}.source must be 'sg_device' or 'fixed', got {source!r}"
                )

    if legacy_chain_ids and fail_on_legacy_chain:
        raise SpectralRecordingSGValidationError(
            "sg_preflight_scope='active_lo_chains' cannot infer SG IDs from active legacy LO chains "
            f"{legacy_chain_ids}. Use role-based lo_chains with lo_roles.source='sg_device', "
            "or set sg_preflight_ids explicitly."
        )
    return tuple(sorted(active_sg_ids))


def select_sg_preflight_plan(
    *,
    lo_profile: Mapping[str, Any],
    snapshot: Mapping[str, Any],
    scope: str = "active_lo_chains",
    sg_ids: Optional[Iterable[str]] = None,
) -> Dict[str, Dict[str, Any]]:
    """Build the SG subset to check/set for observation-time preflight."""

    full_plan = build_sg_apply_plan(lo_profile)
    scope_norm = str(scope or "active_lo_chains").strip().lower()
    if sg_ids is not None:
        explicit = [str(sg_id) for sg_id in sg_ids]
        if scope_norm not in {"explicit_sg_ids", "ids", "active_lo_chains", "all_sg_devices"}:
            raise SpectralRecordingSGValidationError(f"Unsupported sg_preflight_scope: {scope!r}")
        if scope_norm in {"active_lo_chains", "all_sg_devices"} and explicit:
            # Explicit IDs take precedence only when the caller intentionally
            # passes them.  This allows `.obs` to use sg_preflight_ids without
            # also setting scope='explicit_sg_ids'.
            scope_norm = "explicit_sg_ids"
    if scope_norm in {"explicit_sg_ids", "ids"}:
        if sg_ids is None:
            raise SpectralRecordingSGValidationError(
                "sg_preflight_scope='explicit_sg_ids' requires sg_preflight_ids"
            )
        return filter_sg_apply_plan(full_plan, sg_ids)
    if scope_norm == "all_sg_devices":
        return {sg_id: dict(full_plan[sg_id]) for sg_id in sorted(full_plan)}
    if scope_norm == "active_lo_chains":
        active_ids = active_sg_ids_from_snapshot(snapshot, fail_on_legacy_chain=True)
        return filter_sg_apply_plan(full_plan, active_ids)
    raise SpectralRecordingSGValidationError(f"Unsupported sg_preflight_scope: {scope!r}")


def run_sg_preflight(
    commander: Any,
    *,
    plan: Mapping[str, Mapping[str, Any]],
    policy: str,
    timeout_sec: float = 10.0,
    allow_command_echo: bool = False,
    poll_interval_sec: float = 0.1,
) -> Dict[str, Dict[str, Any]]:
    """Execute observation-time SG preflight.

    Policies:
    - none: do nothing.
    - verify_only: read and strictly verify fresh SG readbacks.
    - apply_and_verify: send set/stop according to the plan, then verify.
    - warn_only: verify fresh readbacks but return failures instead of raising.
    """

    policy_norm = str(policy or "none").strip().lower()
    if policy_norm in {"", "none", "off", "false", "0"}:
        return {}
    if policy_norm not in {"verify_only", "apply_and_verify", "warn_only"}:
        raise SpectralRecordingSGValidationError(
            "sg_preflight_policy must be one of none, verify_only, apply_and_verify, warn_only; "
            f"got {policy!r}"
        )

    results: Dict[str, Dict[str, Any]] = {}
    failures: List[str] = []
    for sg_id in sorted(plan):
        entry = plan[sg_id]
        started = pytime.time()
        if policy_norm == "apply_and_verify":
            apply_sg_plan_entry(commander, entry)
        try:
            result = poll_sg_readback(
                commander,
                sg_id=sg_id,
                entry=entry,
                verify_started_at=started,
                timeout_sec=float(timeout_sec),
                strict=(policy_norm != "warn_only"),
                allow_command_echo=allow_command_echo,
                poll_interval_sec=poll_interval_sec,
            )
        except SpectralRecordingSGValidationError as exc:
            if policy_norm == "warn_only":
                result = {
                    "sg_id": sg_id,
                    "success": False,
                    "warnings": [],
                    "errors": [str(exc)],
                }
            else:
                raise
        results[sg_id] = dict(result)
        if not bool(result.get("success", False)):
            failures.append(f"{sg_id}: {'; '.join(map(str, result.get('errors', [])))}")
    if failures and policy_norm != "warn_only":
        raise SpectralRecordingSGValidationError("SG preflight failed: " + " | ".join(failures))
    return results


def apply_sg_plan_entry(commander: Any, entry: Mapping[str, Any]) -> None:
    sg_id = str(entry["sg_id"])
    output_required = bool(entry.get("output_required", True))
    if output_required:
        commander.signal_generator(
            "set",
            GHz=float(entry["sg_set_frequency_hz"]) / 1.0e9,
            dBm=float(entry["sg_set_power_dbm"]),
            id=sg_id,
        )
    elif str(entry.get("output_policy", "require_off")) == "require_off":
        commander.signal_generator("stop", id=sg_id)


def verify_sg_readback(
    *,
    sg_id: str,
    entry: Mapping[str, Any],
    readback: Any,
    verify_started_at: float,
    strict: bool = True,
    allow_command_echo: bool = False,
) -> Dict[str, Any]:
    """Validate one SG readback and return normalized values.

    Strict mode requires an id match, non-stale timestamp, frequency tolerance,
    power tolerance when power is present, and a real device readback unless
    ``allow_command_echo`` is explicitly set.
    """

    errors = []
    warnings = []

    rb_id = _readback_id(readback)
    if rb_id != sg_id:
        errors.append(f"id mismatch: expected {sg_id!r}, got {rb_id!r}")

    rb_time = _readback_time(readback)
    if rb_time is None:
        errors.append("readback has no time field")
    elif rb_time < verify_started_at:
        errors.append(
            f"stale readback: readback_time={rb_time:.6f} < verify_started_at={verify_started_at:.6f}"
        )

    rb_source = str(_get_value(readback, "readback_source", "source", default="device_readback"))
    if strict and rb_source == "command_echo_detected" and not allow_command_echo:
        errors.append("strict verify requires device_readback; command_echo_detected was returned")
    elif rb_source == "command_echo_detected":
        warnings.append("readback_source=command_echo_detected accepted by explicit override")

    expected_freq = float(entry["sg_set_frequency_hz"])
    freq_tol = float(entry.get("frequency_tolerance_hz", 0.0))
    rb_freq = _readback_frequency_hz(readback)
    freq_error = rb_freq - expected_freq
    if abs(freq_error) > freq_tol:
        errors.append(
            f"frequency out of tolerance: expected={expected_freq:.6f} Hz, "
            f"readback={rb_freq:.6f} Hz, error={freq_error:.6f} Hz, tolerance={freq_tol:.6f} Hz"
        )

    rb_power = _readback_power_dbm(readback)
    expected_power = float(entry.get("sg_set_power_dbm", 0.0))
    power_tol = float(entry.get("power_tolerance_db", 0.0))
    power_error = None
    if rb_power is not None:
        power_error = rb_power - expected_power
        if abs(power_error) > power_tol:
            errors.append(
                f"power out of tolerance: expected={expected_power:.3f} dBm, "
                f"readback={rb_power:.3f} dBm, error={power_error:.3f} dB, tolerance={power_tol:.3f} dB"
            )

    rb_output = _readback_output(readback)
    output_required = bool(entry.get("output_required", True))
    output_policy = str(entry.get("output_policy", "require_off"))
    if output_required and rb_output is False:
        errors.append("output_required=true but readback output is off")
    if (not output_required) and output_policy == "require_off" and rb_output is True:
        errors.append("output_required=false/output_policy=require_off but readback output is on")

    result = {
        "sg_id": sg_id,
        "readback_frequency_hz": rb_freq,
        "frequency_error_hz": freq_error,
        "readback_power_dbm": rb_power,
        "power_error_db": power_error,
        "readback_time": rb_time,
        "readback_source": rb_source,
        "output_status": rb_output,
        "warnings": warnings,
        "errors": errors,
        "success": not errors,
    }
    if errors and strict:
        raise SpectralRecordingSGValidationError("; ".join(errors))
    return result



def _candidate_readbacks(readback: Any, sg_id: str) -> List[Any]:
    """Return candidate readback objects from Commander output.

    Commander variants may return a single LocalSignal-like object, a mapping
    keyed by ``lo_signal.<id>``, or a mapping of multiple topic children.
    """

    if isinstance(readback, Mapping):
        exact_key = f"lo_signal.{sg_id}"
        if exact_key in readback:
            return [readback[exact_key]]
        # A mapping with scalar/message fields is itself a readback object.
        scalar_fields = {
            "id",
            "sg_id",
            "freq",
            "frequency",
            "frequency_hz",
            "sg_readback_frequency_hz",
            "time",
            "readback_time",
            "timestamp",
        }
        if any(name in readback for name in scalar_fields):
            return [readback]
        return list(readback.values())
    return [readback]


def poll_sg_readback(
    commander: Any,
    *,
    sg_id: str,
    entry: Mapping[str, Any],
    verify_started_at: float,
    timeout_sec: float,
    strict: bool = True,
    allow_command_echo: bool = False,
    poll_interval_sec: float = 0.1,
) -> Dict[str, Any]:
    """Poll Commander until a fresh id-matching SG readback verifies or times out."""

    deadline = verify_started_at + max(float(timeout_sec), 0.0)
    last_errors: List[str] = []
    last_warnings: List[str] = []
    attempts = 0
    while True:
        attempts += 1
        raw = commander.signal_generator("?", id=sg_id)
        for candidate in _candidate_readbacks(raw, sg_id):
            try:
                result = verify_sg_readback(
                    sg_id=sg_id,
                    entry=entry,
                    readback=candidate,
                    verify_started_at=verify_started_at,
                    strict=False,
                    allow_command_echo=allow_command_echo,
                )
            except Exception as exc:  # malformed candidate; keep polling until timeout
                last_errors = [str(exc)]
                continue
            last_errors = list(result.get("errors", []))
            last_warnings = list(result.get("warnings", []))
            if result.get("success"):
                result["attempts"] = attempts
                return result
        now = pytime.time()
        if now >= deadline:
            errors = last_errors or [
                f"timed out waiting for fresh SG readback for {sg_id!r} after {timeout_sec:.3f} s"
            ]
            message = "; ".join(errors)
            if strict:
                raise SpectralRecordingSGValidationError(message)
            return {
                "sg_id": sg_id,
                "success": False,
                "attempts": attempts,
                "warnings": last_warnings,
                "errors": errors,
            }
        pytime.sleep(max(0.0, min(float(poll_interval_sec), deadline - now)))

def summarize_lo_profile(path: Path) -> str:
    profile = read_toml(path)
    plan = build_sg_apply_plan(profile)
    lines = ["sg_id,frequency_Hz,power_dBm,output_required,output_policy,tolerance_Hz"]
    for sg_id, entry in sorted(plan.items()):
        lines.append(
            f"{sg_id},{entry['sg_set_frequency_hz']:.6f},{entry['sg_set_power_dbm']:.3f},"
            f"{entry['output_required']},{entry['output_policy']},{entry['frequency_tolerance_hz']:.6f}"
        )
    return "\n".join(lines)


def _readback_to_dict(readback: Any) -> Dict[str, Any]:
    if isinstance(readback, Mapping):
        return dict(readback)
    fields = {}
    for name in ("id", "freq", "frequency_hz", "power", "power_dbm", "time", "output_status"):
        if hasattr(readback, name):
            fields[name] = getattr(readback, name)
    return fields


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="necst-lo-profile")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("summary")
    p.add_argument("lo_profile", type=Path)

    p = sub.add_parser("apply")
    p.add_argument("lo_profile", type=Path)
    p.add_argument("--id", dest="sg_id")
    p.add_argument("--verify", action="store_true")
    p.add_argument("--timeout-sec", type=float, default=10.0)
    p.add_argument("--allow-command-echo", action="store_true")

    p = sub.add_parser("verify")
    p.add_argument("lo_profile", type=Path)
    p.add_argument("--id", dest="sg_id")
    p.add_argument("--timeout-sec", type=float, default=10.0)
    p.add_argument("--allow-command-echo", action="store_true")
    return parser


def main_lo_profile(argv: Optional[Iterable[str]] = None) -> int:
    args = _build_parser().parse_args(argv)
    if args.cmd == "summary":
        print(summarize_lo_profile(args.lo_profile))
        return 0

    profile = read_toml(args.lo_profile)
    plan = build_sg_apply_plan(profile, getattr(args, "sg_id", None))

    from necst.core import Commander  # imported lazily to keep pure tests ROS-free
    import rclpy  # imported lazily so summary/pure helper tests do not require ROS

    should_shutdown = not rclpy.ok()
    if should_shutdown:
        rclpy.init()

    commander = None
    privilege_acquired = False
    try:
        commander = Commander()
        privilege_acquired = bool(commander.get_privilege())

        results = {}
        for sg_id, entry in plan.items():
            started = pytime.time()
            if args.cmd == "apply":
                apply_sg_plan_entry(commander, entry)
                if not args.verify:
                    continue
            elif args.cmd != "verify":  # pragma: no cover
                raise SpectralRecordingSGError(f"Unknown command: {args.cmd!r}")

            result = poll_sg_readback(
                commander,
                sg_id=sg_id,
                entry=entry,
                verify_started_at=started,
                timeout_sec=float(args.timeout_sec),
                strict=True,
                allow_command_echo=args.allow_command_echo,
            )
            results[sg_id] = result

        if results:
            print(json.dumps(results, indent=2, sort_keys=True))
        return 0
    finally:
        if commander is not None and privilege_acquired and hasattr(commander, "quit_privilege"):
            try:
                commander.quit_privilege()
            except Exception:
                pass
        if commander is not None and hasattr(commander, "destroy_node"):
            commander.destroy_node()
        if should_shutdown:
            rclpy.shutdown()


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main_lo_profile())
