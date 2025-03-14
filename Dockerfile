ARG DISTRO=humble
FROM ros:${DISTRO}-ros-base
ARG DISTRO

SHELL ["/bin/bash", "-c"]
ENV SHELL=/bin/bash

RUN apt-get update \
    && apt-get -y install curl git pciutils python3-pip ros-${DISTRO}-rmw-cyclonedds-cpp \
    && apt-get clean \
    && apt-get -y install emacs vim python3.10

ENV ROS2_WS=/root/ros2_ws
ENV RMW_IMPLEMENTATION=rmw_cyclonedds_cpp

COPY . $ROS2_WS/src/necst/

RUN pip install --upgrade pip==22.0.2
RUN ( cd $ROS2_WS/src/necst && pip install git+https://github.com/necst-telescope/neclib.git && pip install ipython )

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
