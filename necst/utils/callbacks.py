__all__ = ["coord_msg_clbk", "respond_ping"]

from types import SimpleNamespace

from std_srvs.srv import Empty

from necst_msgs.msg import CoordMsg


def _get_all_attrs(msg) -> SimpleNamespace:
    attrs = {k: getattr(msg, k) for k in msg.get_fields_and_field_types().keys()}
    return SimpleNamespace(**attrs)


def coord_msg_clbk(msg: CoordMsg) -> SimpleNamespace:
    attrs = _get_all_attrs(msg)
    if msg.name != "":
        del attrs.lon
        del attrs.lat
        del attrs.frame
        del attrs.unit
    else:
        del attrs.name
    return attrs


def respond_ping(request: Empty.Request, response: Empty.Response) -> Empty.Response:
    return response
