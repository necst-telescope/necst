ARG DISTRO=humble
FROM ros:${DISTRO}-ros-base
ARG DISTRO

ARG USERNAME=user
ARG USER_UID=1000
ARG USER_GID=$USER_UID

SHELL ["/bin/bash", "-c"]
ENV SHELL=/bin/bash

ENV PATH=$PATH:/home/$USERNAME/.local/bin
ENV ROS2_WS=/home/$USERNAME/ros2_ws

RUN groupadd --gid $USER_GID $USERNAME \
    && useradd --uid $USER_UID --gid $USER_GID -m $USERNAME

RUN apt-get update \
    && apt-get install -y curl git gnupg2 openssh-client python3-pip sudo vim \
    && apt-get clean \
    && echo $USERNAME ALL=\(root\) NOPASSWD:ALL > /etc/sudoers.d/$USERNAME \
    && chmod 0440 /etc/sudoers.d/$USERNAME

USER $USERNAME

RUN pip3 install \
        "docker-launch>=0.2.1" \
        "git+https://github.com/necst-telescope/neclib.git" \
    && pip3 install \
        "black>=22.3" \
        "flake8>=3.8" \
        "ipykernel>=6.3" \
        "jupyter>=1.0" \
        "myst-parser==0.18.0" \
        "notebook>=6.4" \
        "pydata-sphinx-theme==0.11.0" \
        "pytest>=5.2" \
        "pytest-cov==3.0.0" \
        "sphinx==5.1.1"

RUN mkdir -p $ROS2_WS/src \
    && git clone https://github.com/necst-telescope/necst-msgs.git $ROS2_WS/src/necst-msgs \
    && . /opt/ros/${DISTRO}/setup.bash \
    && ( cd $ROS2_WS && colcon build --symlink-install ) \
    && echo ". /opt/ros/${DISTRO}/setup.bash" >> /home/$USERNAME/.bashrc \
    && echo ". $ROS2_WS/install/setup.bash" >> /home/$USERNAME/.bashrc \
    && curl -o /home/$USERNAME/.bashgit https://raw.githubusercontent.com/oyvindstegard/bashgit/master/.bashgit \
    && echo "if [ -f /home/$USERNAME/.bashgit ]; then . ~/.bashgit; fi" >> /home/$USERNAME/.bashrc
