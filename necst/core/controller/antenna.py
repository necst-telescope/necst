#PID controller 出力値と目標値との偏差、その微分、積分の３要素から入力値の制御を行う
from multiprocessing import dummy
from nntplib import NNTPPermanentError
import time
import std_msgs.msg
from typing import Dict, Literal, Tuple
import ros2
from neclib.controllers import PIDController
from rclpy.node import Node
from necst_msgs.msg import CoordMsg



class Antenna_device(Node):

    node_name = "pid"

    command_ang_speed = 0
    ang_rate_d = 0
    pre_hensa = 0
    ihensa = 0
    enc_before = 0
    pre_arcsec = 0 
    t_now = t_past = 0
    p_coeff = 3.0
    i_coeff = 0.7
    d_coeff = 0.1
    

    def __init__(self,frequency:float):
        super().__init__(self.node_name)
        self.create_subscription(CoordMsg,"altaz",arcsec)
        self.create_subscription(CoordMsg,"encorder",arcsec)
        self.publisher = self.create_publisher(value,"speed",)
        self.create_timer(frequency,self.calc_pid)

        self.p_coeff = ros2.get_param("p_coeff")
        self.i_coeff = ros2.get_param("i_coeff")
        self.d_coeff = ros2.get_param("d_coeff")

        PIDController.ang_unit = "arcsec"
        self._ang = PIDController(
            pid_param=[self.p_coeff, self.i_coeff, self.d_coeff,]
            max_speed="1.5deg/s"
        )

    def init_speed(self) -> None:
        dummy = 0
        self._ang.get_speed(dummy,dummy,stop=True)
        self._command(0,"ang")

        speed_ang = target_speed + (p_coeff * error) + (i_coeff * error_integral) + (d_coeff * error_derivative)

    def set_pid_param(
        self,param:Dict[Literal["altaz"]],taple[float,float]) -> None:
        self._ang.k_p, self._ang.k_i, self._ang.k_d = param["altaz"] 

    def calc_pid(self):
        calculator = PIDController(pid_param=[p_coeff, i_coeff, d_coeff])

        calculator.time.push(t_past)
        calculator.cmd_coord.push(pre_arcsec / 3600)
        calculator.enc_coord.push(enc_before / 3600)
        calculator.error.push(pre_hensa / 3600)

        calculator.time.push(t_now)
        calculator.cmd_coord.push(target_arcsec / 3600)
        calculator.enc_coord.push(encoder_arcsec / 3600)
        calculator.error.push((target_arcsec - encoder_arcsec) / 3600)

        speed = calculator._calc_pid()
        
        error_diff = (calculator.error[Now] - calculator.error[Last]) / calculator.dt
        if abs(error_diff) * 3600 > 1:
            error_diff = 0

        return (
            speed,
            calculator.error_integral,
            calculator.k_p * calculator.error[Now],
            calculator.k_i * calculator.error_integral * calculator.dt,
            calculator.k_d * error_diff / calculator.dt,
        )
