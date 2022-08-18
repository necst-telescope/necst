ARG DISTRO=humble
FROM ros:${DISTRO}-ros-core
ARG DISTRO

COPY config.sh /root/

RUN apt-get update \
    && apt-get -y install curl git python3-pip ros-${DISTRO}-rmw-cyclonedds-cpp \
    && apt-get clean \
    && curl -sSL https://install.python-poetry.org | python3 -

ENV PATH=$PATH:/root/.local/bin
ENV POETRY_VIRTUALENVS_CREATE=false
ENV ROS2_WS=/root/ros2_ws

RUN mkdir -p $ROS2_WS/src \
    && git clone https://github.com/necst-telescope/necst-msgs.git $ROS2_WS/src/necst-msgs \
    && . /opt/ros/humble/setup.sh \
    && ( cd $ROS2_WS && colcon build )
    && echo ". /opt/ros/humble/setup.sh" >> /root/.bashrc \
    && echo ". $ROS2_WS/install/setup.sh" >> /root/.bashrc
COPY . $ROS2_WS/src/necst
RUN ( cd $ROS2_WS/src/necst && poetry install )
