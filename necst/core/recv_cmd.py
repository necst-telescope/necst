from rclpy.node import Node
from .. import config, namespace, qos
from . import Commander
class Recv_cmd(Node):
    
    NodeName = "recv_cmd"
    Namespace = namespace.core
    
    def __init__(self , msg.lon , msg.lat , msg.time):
        super().__init__(self.NodeName, namespace=self.Namespace)
        self.cmd_az = msg.lon
        self.cmd_el = msg.lat
        self.cmd_time = msg.time
        cmd = [msg.time, msg.lon, msg.lat] 
        
        if cmd[0].time > time.time():
           
    def cmd_az(self) -> float:
        return self.cmd_az
    
    def cmd_el(self) -> float:
        return self.cmd_el
    
    def cmd_time(self) -> float:
        return self.cmd_time
    

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