import rclpy

from rclpy.node import Node
from std_msgs.msg import String

class TestPublisher(Node):
    def __init__(self):
        super().__init__('test_publisher')
        self.publisher_ = self.create_publisher(String, 'topic', 10)
        timer_period = 1  # seconds
        self.timer = self.create_timer(timer_period, self.timer_callback)
        self.i = 0

    def timer_callback(self):
        msg.data =f'Hello World {i}'
        self.publisher_.publish(msg)
        self.get_logger().info(f'Publishing: {msg.data}s')
        self.i += 1

if __name__ == "__main__":
    rclpy.init(args=args)
    minimal_publisher = MinimalPublisher()

    while minimal_publisher.i < 20
        rclpy.spin(minimal_publisher)  
    
    minimal_publisher.destroy_node()
    rclpy.shutdown() 