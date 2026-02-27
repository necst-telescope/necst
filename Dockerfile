ARG DISTRO=humble
FROM ros:${DISTRO}-ros-base
ARG DISTRO

SHELL ["/bin/bash", "-c"]
ENV SHELL=/bin/bash

RUN rm -f /etc/apt/sources.list.d/ros*.list \
    && rm -f /etc/apt/trusted.gpg.d/ros*.gpg \
    && rm -f /etc/apt/sources.list.d/ros*.list \
    && rm -f /etc/apt/trusted.gpg.d/ros*.gpg

RUN apt-get update && apt-get install -y \
    curl gnupg lsb-release \
    && curl -sSL https://raw.githubusercontent.com/ros/rosdistro/master/ros.key \
    | gpg --dearmor -o /usr/share/keyrings/ros-archive-keyring.gpg \
    && echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/ros-archive-keyring.gpg] \
    http://packages.ros.org/ros2/ubuntu $(lsb_release -cs) main" \
    > /etc/apt/sources.list.d/ros2.list

RUN apt-get update \
    && apt-get -y install curl git pciutils python3-pip ros-${DISTRO}-rmw-cyclonedds-cpp \
    && apt-get clean \
    && apt-get -y install emacs vim python3.10

ENV ROS2_WS=/root/ros2_ws
ENV RMW_IMPLEMENTATION=rmw_cyclonedds_cpp

COPY . $ROS2_WS/src/necst/

RUN pip install --upgrade pip==24.1.2
RUN pip install setuptools==70.3.0
RUN ( cd $ROS2_WS/src/necst && pip install git+https://github.com/necst-telescope/neclib.git)
RUN pip install ipython

RUN git clone https://github.com/necst-telescope/necst-msgs.git $ROS2_WS/src/necst-msgs \
    && . /opt/ros/humble/setup.bash \
    && ( cd $ROS2_WS && colcon build --symlink-install ) \
    && . $ROS2_WS/install/setup.bash \
    && echo ". /opt/ros/humble/setup.bash" >> /root/.bashrc \
    && echo ". $ROS2_WS/install/setup.bash" >> /root/.bashrc \
    && echo "PS1='\[\033[1;36m\][NECST]\[\033[0m\]\w\$ '" >> /root/.bashrc \
    && echo -e 'PATH=$ROS2_WS/src/necst/bin:$PATH' >> /root/.bashrc \
    && echo -e ". $ROS2_WS/install/setup.bash\n. /ros_entrypoint.sh $@" > /entrypoint.sh \
    && chmod +x /entrypoint.sh \
    && python3 -c "import neclib"

ENTRYPOINT [ "bash", "/entrypoint.sh" ]
CMD ["bash"]
