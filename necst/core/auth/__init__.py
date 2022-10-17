"""Handle conflict-unsafe operations.

If anyone can send command to drive the antenna, someone may interrupt running
observation, and conflicting commands sent by multiple controllers can bring the system
into uncontrollable state. Access to such operations should strictly be limited, and
the restriction should be reliably applied.

This package implements server-client pair for the privilege handling. To perform
*privileged* operation, the following implementations and conditions are required:

- Privilege server ``Authorizer`` is running (spinning)
- Node should subclass privilege client ``PrivilegedNode``

No need to run the server-client on single machine, they can be on any computers in same
network. In the following situations, the privilege will be removed from the client:

- Client explicitly quit the privilege
- Client failed to respond the ping signal from server

Notes
-----
This package doesn't expect future implementation of server-client variants. Reliability
will come from simplicity and low-extensibility.

"""

from .authorizer import Authorizer  # noqa: F401
from .privileged_node import PrivilegedNode  # noqa: F401
