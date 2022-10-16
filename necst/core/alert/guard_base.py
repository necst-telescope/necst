from abc import ABC, abstractmethod
from typing import Any, Dict, List, Tuple, final

from rclpy.qos import QoSProfile


class Guard(ABC):

    TargetTopic: Tuple[Any, str, QoSProfile]
    """Topic configuration (MsgType, TopicName, QosProfile) this guard checks."""
    MetaTopics: List[Tuple[Any, str, QoSProfile]] = []
    """List of configurations required to activate this safe guard."""

    def __init__(self) -> None:
        self.metaparams: Dict[str, Any] = {
            k: None for k in map(lambda x: x[1], self.MetaTopics)
        }

    @final
    def update(self, **kwargs: Any) -> None:
        self.metaparams.update(kwargs)

    @abstractmethod
    def handle_critical(self, msg: Any):
        ...

    @abstractmethod
    def handle_warning(self, msg: Any):
        ...

    @abstractmethod
    def handle_normal(self, msg: Any):
        ...

    @final
    def check(self, msg: Any, *, critical: bool, warning: bool) -> Dict[str, Any]:
        if critical:
            return self.handle_critical(msg)
        elif warning:
            return self.handle_warning(msg)
        else:
            return self.handle_normal(msg)

    @final
    def __eq__(self, other) -> bool:
        """Test if other topic handles the same topic.

        Notes
        -----
        Safe guards are designed to just modify the subscribed value, then publish it.
        If multiple guards are attached to single topic, multiple
        *independently-checked* values will be published to single topic. Such situation
        must be avoided, as it can give invalid command rate or conflicting commands.

        For the aforementioned reason, this equality tester has no interest in anything
        other than the target topic name.

        """
        return other.TargetTopic[1] == self.TargetTopic[1]
