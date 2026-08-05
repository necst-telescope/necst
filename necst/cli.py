"""Small installable ``necst`` command dispatcher.

The historical source-tree ``bin/necst`` wrapper still works.  This module makes
``necst console`` available after a normal Python/ROS installation as well.
"""

from __future__ import annotations

import runpy
import sys
from pathlib import Path
from typing import Callable, Optional


def _console(argv: list[str]) -> int:
    from .web.console_entry import main

    return int(main(argv))


def _console_log(argv: list[str]) -> int:
    from .web.console_log_cli import main

    return int(main(argv))


def _az_unwrap_state(argv: list[str]) -> int:
    from .ctrl.antenna.az_unwrap import main

    return int(main(argv))


COMMANDS: dict[str, Callable[[list[str]], int]] = {
    "console": _console,
    "console-log": _console_log,
    "az-unwrap-state": _az_unwrap_state,
}


def _source_tree_bin_dir() -> Path:
    # In a source checkout this is <repo>/bin.  In an installed package it will
    # usually not exist, which is fine because installable commands are handled
    # above.
    return Path(__file__).resolve().parents[1] / "bin"


def _run_source_script(command: str, argv: list[str]) -> Optional[int]:
    script = _source_tree_bin_dir() / f"{command}.py"
    if not script.exists():
        return None
    old_argv = sys.argv[:]
    try:
        sys.argv = [str(script)] + list(argv)
        runpy.run_path(str(script), run_name="__main__")
    except SystemExit as exc:
        code = exc.code
        if code is None:
            return 0
        if isinstance(code, int):
            return code
        print(code)
        return 1
    finally:
        sys.argv = old_argv
    return 0


def _print_help() -> None:
    print("NECST; NEw Control System for Telescope")
    print("")
    print("USAGE")
    print("  necst <command> [<options>]")
    print("")
    print("COMMON COMMANDS")
    print("  console          Start the Operator Console GUI")
    print("  console-log      Show the persistent Operator Console JSONL log")
    print("  az-unwrap-state  Get/set Az unwrap state via encoder-node service")
    bin_dir = _source_tree_bin_dir()
    if bin_dir.exists():
        scripts = sorted(p.stem for p in bin_dir.glob("*.py"))
        if scripts:
            print("")
            print("SOURCE-TREE COMMANDS")
            for name in scripts:
                print(f"  {name}")


def main(argv: Optional[list[str]] = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    if not args or args[0] in {"-h", "--help"}:
        _print_help()
        return 0
    command = args.pop(0)
    command = command[:-3] if command.endswith(".py") else command
    command = command.replace("_", "-")
    if command in COMMANDS:
        return COMMANDS[command](args)
    source_rc = _run_source_script(command, args)
    if source_rc is not None:
        return int(source_rc)
    print(f"unknown necst command: {command}", file=sys.stderr)
    print("run 'necst --help' for available commands", file=sys.stderr)
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
