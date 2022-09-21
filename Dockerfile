ARG DISTRO=humble
FROM ros:${DISTRO}-ros-base
ARG DISTRO

RUN apt-get update \
    && apt-get -y install curl git python3-pip ros-${DISTRO}-rmw-cyclonedds-cpp \
    && apt-get clean

ENV ROS2_WS=/root/ros2_ws
ENV RMW_IMPLEMENTATION=rmw_cyclonedds_cpp

COPY .  $ROS2_WS/src/necst/

RUN ( cd $ROS2_WS/src/necst && pip install "neclib>=0.10.2" )

RUN git clone https://github.com/necst-telescope/necst-msgs.git $ROS2_WS/src/necst-msgs \
    && . /opt/ros/humble/setup.sh \
    && ( cd $ROS2_WS && colcon build ) \
    && echo ". /opt/ros/humble/setup.sh" >> /root/.bashrc \
    && echo ". $ROS2_WS/install/setup.sh" >> /root/.bashrc \
    && . $ROS2_WS/install/setup.sh 

ENTRYPOINT [ "/ros_entrypoint.sh" ]
CMD ["bash"]
