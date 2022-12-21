import time
from pathlib import Path

import pytest
from neclib.recorders import NECSTDBWriter
from necst_msgs.msg import TimedAzElFloat64
from std_msgs.msg import Float64, Int32

from necst import config, qos
from necst.core import Recorder
from necst.utils import spinning

from ..conftest import TesterNode, destroy


@pytest.fixture
def mock_record_root(tmp_path_factory):
    config.record_root: Path = tmp_path_factory.mktemp("data")
    yield


@pytest.mark.usefixtures("mock_record_root")
class TestRecorder(TesterNode):

    NodeName = "test_recorder"

    def test_single_topic_single_message(self):
        recorder = Recorder()
        writers = recorder.recorder.writers
        dbwriter, *_ = [w for w in writers if isinstance(w, NECSTDBWriter)]
        recorder.recorder.start_recording()
        db = dbwriter.db

        pub1 = self.node.create_publisher(Float64, "/test/topic1", qos.realtime)

        with spinning([self.node, recorder]):
            time.sleep(config.ros_topic_scan_interval_sec + 0.2)
            # Wait for this topic to be detected

            assert "/test/topic1" in recorder.subscriber
            assert self.node.count_publishers("/test/topic1") == 1
            assert self.node.count_subscribers("/test/topic1") == 1

            pub1.publish(Float64(data=12.3))
            time.sleep(0.1)
            recorder.recorder.stop_recording()

        assert db.list_tables() == ["test-topic1"]
        assert db.get_info().loc["test-topic1", "#records"] == 1

        destroy(recorder)
        destroy([pub1], node=self.node)

    def test_single_topic_multiple_message(self):
        recorder = Recorder()
        writers = recorder.recorder.writers
        dbwriter, *_ = [w for w in writers if isinstance(w, NECSTDBWriter)]
        recorder.recorder.start_recording()
        db = dbwriter.db

        pub1 = self.node.create_publisher(Float64, "/test/topic1", qos.reliable)

        with spinning([self.node, recorder]):
            time.sleep(config.ros_topic_scan_interval_sec + 0.2)
            # Wait for this topic to be detected

            assert "/test/topic1" in recorder.subscriber
            assert self.node.count_publishers("/test/topic1") == 1
            assert self.node.count_subscribers("/test/topic1") == 1

            pub1.publish(Float64(data=12.3))
            pub1.publish(Float64(data=12.3))
            time.sleep(0.1)
            recorder.recorder.stop_recording()

        assert db.list_tables() == ["test-topic1"]
        assert db.get_info().loc["test-topic1", "#records"] == 2

        destroy(recorder)
        destroy([pub1], node=self.node)

    def test_multiple_topic_single_message(self):
        recorder = Recorder()
        writers = recorder.recorder.writers
        dbwriter, *_ = [w for w in writers if isinstance(w, NECSTDBWriter)]
        recorder.recorder.start_recording()
        db = dbwriter.db

        pub1 = self.node.create_publisher(Float64, "/test/topic1", qos.reliable)
        pub2 = self.node.create_publisher(Int32, "/topic2", qos.reliable)

        with spinning([self.node, recorder]):
            time.sleep(config.ros_topic_scan_interval_sec + 0.2)
            # Wait for this topic to be detected

            assert "/test/topic1" in recorder.subscriber
            assert "/topic2" in recorder.subscriber
            assert self.node.count_publishers("/test/topic1") == 1
            assert self.node.count_subscribers("/test/topic1") == 1
            assert self.node.count_publishers("/topic2") == 1
            assert self.node.count_subscribers("/topic2") == 1

            pub1.publish(Float64(data=12.3))
            pub2.publish(Int32(data=12))
            time.sleep(0.1)
            recorder.recorder.stop_recording()

        assert db.list_tables() == ["test-topic1", "topic2"]
        assert db.get_info().loc["test-topic1", "#records"] == 1
        assert db.get_info().loc["topic2", "#records"] == 1

        destroy(recorder)
        destroy([pub1, pub2], node=self.node)

    def test_multiple_topic_multiple_message(self):
        recorder = Recorder()
        writers = recorder.recorder.writers
        dbwriter, *_ = [w for w in writers if isinstance(w, NECSTDBWriter)]
        recorder.recorder.start_recording()
        db = dbwriter.db

        pub1 = self.node.create_publisher(Float64, "/test/topic1", qos.reliable)
        pub2 = self.node.create_publisher(Int32, "/topic2", qos.reliable)

        with spinning([self.node, recorder]):
            time.sleep(config.ros_topic_scan_interval_sec + 0.2)
            # Wait for this topic to be detected

            assert "/test/topic1" in recorder.subscriber
            assert "/topic2" in recorder.subscriber
            assert self.node.count_publishers("/test/topic1") == 1
            assert self.node.count_subscribers("/test/topic1") == 1
            assert self.node.count_publishers("/topic2") == 1
            assert self.node.count_subscribers("/topic2") == 1

            pub1.publish(Float64(data=12.3))
            pub2.publish(Int32(data=12))
            pub1.publish(Float64(data=12.3))
            pub2.publish(Int32(data=12))
            time.sleep(0.1)
            recorder.recorder.stop_recording()

        assert db.list_tables() == ["test-topic1", "topic2"]
        assert db.get_info().loc["test-topic1", "#records"] == 2
        assert db.get_info().loc["topic2", "#records"] == 2

        destroy(recorder)
        destroy([pub1, pub2], node=self.node)

    def test_message_with_multiple_fields(self):
        recorder = Recorder()
        writers = recorder.recorder.writers
        dbwriter, *_ = [w for w in writers if isinstance(w, NECSTDBWriter)]
        recorder.recorder.start_recording()
        db = dbwriter.db

        pub1 = self.node.create_publisher(TimedAzElFloat64, "/test/azel", qos.reliable)

        with spinning([self.node, recorder]):
            time.sleep(config.ros_topic_scan_interval_sec + 0.2)
            # Wait for this topic to be detected

            assert "/test/azel" in recorder.subscriber
            assert self.node.count_publishers("/test/azel") == 1
            assert self.node.count_subscribers("/test/azel") == 1

            pub1.publish(TimedAzElFloat64())
            time.sleep(0.1)
            recorder.recorder.stop_recording()

        assert db.list_tables() == ["test-azel"]
        assert db.get_info().loc["test-azel", "#records"] == 1
        assert db.get_info().loc["test-azel", "record size [byte]"] == 24 + 8
        # ddd + d (received_time)

        destroy(recorder)
        destroy([pub1], node=self.node)

    def test_not_recorded_after_destroy(self):
        recorder = Recorder()
        writers = recorder.recorder.writers
        dbwriter, *_ = [w for w in writers if isinstance(w, NECSTDBWriter)]
        recorder.recorder.start_recording()
        db = dbwriter.db

        pub1 = self.node.create_publisher(Float64, "/test/topic1", qos.reliable)

        with spinning([self.node, recorder]):
            time.sleep(config.ros_topic_scan_interval_sec + 0.2)
            # Wait for this topic to be detected

            assert "/test/topic1" in recorder.subscriber
            assert self.node.count_publishers("/test/topic1") == 1
            assert self.node.count_subscribers("/test/topic1") == 1

            pub1.publish(Float64(data=12.3))
            time.sleep(0.1)

        destroy(recorder)
        pub1.publish(Float64(data=12.3))
        time.sleep(0.1)

        assert db.list_tables() == ["test-topic1"]
        assert db.get_info().loc["test-topic1", "#records"] == 1

        destroy([pub1], node=self.node)

    def test_qos_compatibility(self):
        recorder = Recorder()
        writers = recorder.recorder.writers
        dbwriter, *_ = [w for w in writers if isinstance(w, NECSTDBWriter)]
        recorder.recorder.start_recording()
        db = dbwriter.db

        pub1 = self.node.create_publisher(Float64, "/test/topic1", qos.reliable)
        pub2 = self.node.create_publisher(Float64, "/test/topic2", qos.realtime)
        pub3 = self.node.create_publisher(Float64, "/test/topic3", qos.reliable_latched)
        pub4 = self.node.create_publisher(Float64, "/test/topic4", qos.realtime_latched)
        pub5 = self.node.create_publisher(Float64, "/test/topic5", qos.lowest)

        with spinning([self.node, recorder]):
            time.sleep(config.ros_topic_scan_interval_sec + 0.2)
            # Wait for this topic to be detected

            for idx in range(1, 6):
                assert f"/test/topic{idx}" in recorder.subscriber
                assert self.node.count_publishers(f"/test/topic{idx}") == 1
                assert self.node.count_subscribers(f"/test/topic{idx}") == 1

            pub1.publish(Float64(data=12.3))
            pub2.publish(Float64(data=23.4))
            pub3.publish(Float64(data=34.5))
            pub4.publish(Float64(data=45.6))
            pub5.publish(Float64(data=56.7))
            time.sleep(0.1)

        destroy(recorder)

        assert db.list_tables() == [
            "test-topic1",
            "test-topic2",
            "test-topic3",
            "test-topic4",
            "test-topic5",
        ]
        assert db.get_info().loc["test-topic1", "#records"] == 1
        assert db.get_info().loc["test-topic2", "#records"] == 1
        assert db.get_info().loc["test-topic3", "#records"] == 1
        assert db.get_info().loc["test-topic4", "#records"] == 1
        assert db.get_info().loc["test-topic5", "#records"] == 1

        destroy([pub1, pub2, pub3, pub4, pub5], node=self.node)
