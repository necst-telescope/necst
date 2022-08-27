from neclib.controllers import PIDController
from rclpy.node import Node
from necst_msgs.msg import CoordMsg
from typing import Dict, Literal, Tuple
import time

Last = -2
Now = -1


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

    def __init__(self, frequency: float):
        PIDController.ANGLE_UNIT = "arcsec"
        self.ang = PIDController(pid_param=[
            self.p_coeff, self.i_coeff, self.d_coeff], max_speed="1.5ged/s")
        super().__init__(self.node_name)
        self.create_subscription_ang(CoordMsg, "altaz", "arcsec")
        self.create_subscription_enc(CoordMsg, "encorder", "arcsec")
        self.publisher = self.create_publisher("value", "speed", "init_speed")
        self.create_timer(frequency, self.calc_pid)

    def command(self, value: int, altaz: Literal["ang"]) -> None:
        if not self.create_subscription_ang(CoordMsg, altaz):
            self.driver = Antenna_device(Node)

    def init_speed(self) -> None:
        dummy = 0
        self._ang.get_speed(dummy, dummy, stop=True)
        self._command(0, "ang")

    def set_pid_param(self, param: Dict[Literal["altaz"], Tuple[float, float, float]]
    ) -> None:
        self.ang.k_p, self.ang.k_i, self.ang.k_d = param["altaz"]

    def move_ang(self, ang_arcsec: float, enc_ang: float,
        pid_param: Dict[str, Tuple[float, float]] = None,
        m_bStop: Literal["FALSE", "TRUE"] = "FALSE",
        ) -> Tuple[float, ...]:
        if pid_param is not None:
            self.set_pid_param(pid_param)

        if m_bStop == "FALSE":
            stop = False
        elif m_bStop == "TRUE":
            stop = True

    def emergency_stop(self) -> None:
        dummy = 0
        for _ in range(5):
            self.command(0, 'altaz')
            _ = self.command_ang_speed(dummy, dummy, stop=True)
            time.sleep(0.05)

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
        speed = target_speed + (k_p * error) + (k_i * error_integral) + (k_d * error_derivative)

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
