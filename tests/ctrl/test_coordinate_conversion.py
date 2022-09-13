import time

from necst.ctrl.calculations import HorizontalCoord
from necst_msgs.msg import CoordMsg
from ..conftest import TesterNode, destroy, spinning


class TestHorizontalCoord(TesterNode):

    NodeName = "test_horizontal_coord"

    def test_node_info(self):
        converter = HorizontalCoord()
        assert "ctrl/antenna" in converter.get_namespace()
        assert "altaz_coord" in converter.get_name()

        destroy(converter)

    def test_coordinate_frame_conversion(self):
        converter = HorizontalCoord()

        subscribed = False

        def update(msg: CoordMsg) -> None:
            nonlocal subscribed
            assert msg.lat > 0
            assert msg.unit == "deg"
            assert msg.frame == "altaz"
            subscribed = True

        ns = converter.get_namespace()
        raw_cmd = self.node.create_publisher(CoordMsg, f"{ns}/raw_coord", 1)
        converted = self.node.create_subscription(CoordMsg, f"{ns}/altaz", update, 1)

        with spinning([converter, self.node]):
            cmd = {"lat": 80.0, "unit": "deg", "frame": "fk5"}
            for lon in [45.0 * i for i in range(8)]:
                if subscribed:
                    break
                msg = CoordMsg(lon=lon, **cmd, time=time.time() + 1)
                raw_cmd.publish(msg)

                timelimit = time.time() + 1
                while not subscribed:
                    if time.time() > timelimit:
                        break
                    time.sleep(0.02)
            assert subscribed is True, "AltAz coordinate not published in 1s"

        destroy(converter)
        destroy([raw_cmd, converted], self.node)

    def test_name_to_coordinate(self):
        # [print(n.topic_name) for n in self.node.subscriptions]
        converter = HorizontalCoord()

        subscribed = False

        def update(msg: CoordMsg) -> None:
            nonlocal subscribed
            assert msg.lat > 0
            assert msg.unit == "deg"
            assert msg.frame == "altaz"
            subscribed = True

        ns = converter.get_namespace()
        raw_cmd = self.node.create_publisher(CoordMsg, f"{ns}/raw_coord", 1)
        converted = self.node.create_subscription(CoordMsg, f"{ns}/altaz", update, 1)

        with spinning([converter, self.node]):
            targets = ["Spica", "IRC+10216", "Procyon", "M42", "M33", "M2", "M22"]
            for name in targets:
                msg = CoordMsg(name=name, time=time.time() + 2)
                raw_cmd.publish(msg)

                timelimit = time.time() + 2
                while not subscribed:
                    if time.time() > timelimit:
                        break
                    time.sleep(0.02)
            assert subscribed is True, "AltAz coordinate not published in 2s"

        destroy(converter)
        destroy([raw_cmd, converted], self.node)

    def test_outdated_query(self):
        converter = HorizontalCoord()

        subscribed = False

        def update(msg: CoordMsg) -> None:
            nonlocal subscribed
            assert msg.lat > 0
            assert msg.unit == "deg"
            assert msg.frame == "altaz"
            subscribed = True

        ns = converter.get_namespace()
        raw_cmd = self.node.create_publisher(CoordMsg, f"{ns}/raw_coord", 1)
        converted = self.node.create_subscription(CoordMsg, f"{ns}/altaz", update, 1)

        with spinning([converter, self.node]):
            cmd = {"lat": 80.0, "unit": "deg", "frame": "fk5"}
            for lon in [45.0 * i for i in range(8)]:
                msg = CoordMsg(lon=lon, **cmd, time=time.time() - 1)
                raw_cmd.publish(msg)

                timelimit = time.time() + 1
                while not subscribed:
                    if time.time() > timelimit:
                        break
                    time.sleep(0.02)
            assert subscribed is False, "AltAz coordinate accidentally published in 1s"

        destroy(converter)
        destroy([raw_cmd, converted], self.node)

    def test_out_of_drive_range(self):
        converter = HorizontalCoord()

        subscribed = False

        def update(msg: CoordMsg) -> None:
            nonlocal subscribed
            assert msg.lat > 0
            assert msg.unit == "deg"
            assert msg.frame == "altaz"
            subscribed = True

        ns = converter.get_namespace()
        raw_cmd = self.node.create_publisher(CoordMsg, f"{ns}/raw_coord", 1)
        converted = self.node.create_subscription(CoordMsg, f"{ns}/altaz", update, 1)

        with spinning([converter, self.node]):
            msg = CoordMsg(
                lon=45.0, lat=-90.0, unit="deg", frame="fk5", time=time.time() + 1
            )
            raw_cmd.publish(msg)

            timelimit = time.time() + 1
            while not subscribed:
                if time.time() > timelimit:
                    break
                time.sleep(0.02)
            assert subscribed is False, "AltAz coordinate accidentally published in 1s"

        destroy(converter)
        destroy([raw_cmd, converted], self.node)
