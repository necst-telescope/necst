"""Load simple KEY=VALUE environment files without a dotenv dependency."""

from __future__ import annotations

import os
import re
import shlex
from pathlib import Path
from typing import Dict


_ENV_KEY = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def load(path: Path) -> None:
    """Load values that are not already present in the process environment."""

    if not path.is_file():
        raise FileNotFoundError(f"Environment file does not exist: {path}")
    for key, value in read(path).items():
        os.environ.setdefault(key, value)


def read(path: Path) -> Dict[str, str]:
    values: Dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[7:].lstrip()
        key, separator, raw_value = line.partition("=")
        key = key.strip()
        if not separator or not _ENV_KEY.fullmatch(key):
            continue
        value = raw_value.strip()
        if value[:1] in {"'", '"'} and value[-1:] == value[:1]:
            try:
                parsed = shlex.split(value, comments=False, posix=True)
                value = parsed[0] if parsed else ""
            except ValueError as exc:
                raise ValueError(f"Invalid quoted value for {key}") from exc
        values[key] = value
    return values
