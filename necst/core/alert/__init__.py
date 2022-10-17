"""Check system status, and invoke safe guards if needed.

Multiple safeguards (subclasses of ``Guard``) can be attached to centralized alert
handler ``AlertHandler``.

Notes
-----
This package should have high-extensibility, as status to watch differs between
telescopes.

"""

from .alert_handler import AlertHandler  # noqa: F401

from .guard_base import Guard  # noqa: F401
