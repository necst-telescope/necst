__all__ = ["respond_to_ping"]

from std_srvs.srv import Empty


def respond_to_ping(request: Empty.Request, response: Empty.Response) -> Empty.Response:
    """Callback function for ping server, which uses ``std_srvs.srv.Empty``."""
    return response
