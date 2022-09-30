from rclpy.node import Node
from .. import config, namespace, qos
from . import Commander
class Recv_cmd(Node):
    
    NodeName = "recv_cmd"
    Namespace = namespace.core
    
    def __init__(self, cmd_az: float, cmd_el: float, msg.time: float):
        super().__init__(self.NodeName, namespace=self.Namespace)
        self.msg.time = cmd_time
        cmd = [cmd_time, cmd_az, cmd_el] 
        
        if cmd[0].time > time.time():
             
    def cmd_time(self):
        return self.msg.time
    

def main(args=None):
    import rclpy

    rclpy.init(args=args)
    node = Recorder()
    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    finally:
        node.destroy_node()
        rclpy.try_shutdown()    