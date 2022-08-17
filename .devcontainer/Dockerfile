FROM ros:humble-ros-base

ENV PATH=$PATH:/root/.local/bin
ENV POETRY_VIRTUALENVS_CREATE=false
ENV ROS2_WS=/root/ros2_ws

RUN apt-get update \
    && apt-get -y install curl git gnupg2 python3-pip vim \
    && apt-get clean \
    && curl -sSL https://install.python-poetry.org | python3 - \
    && mkdir -p $ROS2_WS/src \
    && git clone https://github.com/necst-telescope/necst-msgs.git $ROS2_WS/src/necst-msgs \
    && . /opt/ros/humble/setup.sh \
    && ( cd $ROS2_WS && colcon build ) \
    && echo ". /opt/ros/humble/setup.sh" >> /root/.bashrc \
    && echo ". $ROS2_WS/install/setup.sh" >> /root/.bashrc \
    && curl -o /root/.bashgit https://raw.githubusercontent.com/oyvindstegard/bashgit/master/.bashgit \
    && echo 'if [ -f /root/.bashgit ]; then . ~/.bashgit; fi' >> /root/.bashrc
