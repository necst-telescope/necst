ARG DISTRO=humble
FROM ros:${DISTRO}-ros-base
ARG DISTRO

RUN apt-get update \
    && apt-get -y install curl git python3-pip ros-${DISTRO}-rmw-cyclonedds-cpp \
    && apt-get clean \
    && curl -sSL https://install.python-poetry.org | python3 -

ENV PATH=$PATH:/root/.local/bin
ENV POETRY_VIRTUALENVS_CREATE=false
ENV ROS2_WS=/root/ros2_ws
ENV RMW_IMPLEMENTATION=rmw_cyclonedds_cpp

COPY necst $ROS2_WS/src/necst/necst
COPY resource $ROS2_WS/src/necst/resource
COPY tests $ROS2_WS/src/necst/tests
COPY LICENSE $ROS2_WS/src/necst/LICENSE
COPY package.xml $ROS2_WS/src/necst/package.xml
COPY poetry.lock $ROS2_WS/src/necst/poetry.lock
COPY pyproject.toml $ROS2_WS/src/necst/pyproject.toml
COPY README.md $ROS2_WS/src/necst/README.md
COPY setup.py $ROS2_WS/src/necst/setup.py
COPY setup.cfg $ROS2_WS/src/necst/setup.cfg

RUN ( cd $ROS2_WS/src/necst && poetry install )

RUN git clone https://github.com/necst-telescope/necst-msgs.git $ROS2_WS/src/necst-msgs \
    && . /opt/ros/humble/setup.sh \
    && ( cd $ROS2_WS && colcon build ) \
    && echo ". /opt/ros/humble/setup.sh" >> /root/.bashrc \
    && echo ". $ROS2_WS/install/setup.sh" >> /root/.bashrc

ENTRYPOINT [ "/ros_entrypoint.sh" ]
