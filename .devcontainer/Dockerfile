FROM ros:humble-ros-base

ARG USERNAME=user
ARG USER_UID=1000
ARG USER_GID=$USER_UID

ENV PATH=$PATH:/home/$USERNAME/.local/bin
ENV POETRY_VIRTUALENVS_CREATE=false
ENV ROS2_WS=/home/$USERNAME/ros2_ws

RUN groupadd --gid $USER_GID $USERNAME \
    && useradd --uid $USER_UID --gid $USER_GID -m $USERNAME \
    && apt-get update \
    && apt-get install -y curl git gnupg2 openssh-client sudo vim \
    && apt-get clean \
    && echo $USERNAME ALL=\(root\) NOPASSWD:ALL > /etc/sudoers.d/$USERNAME \
    && chmod 0440 /etc/sudoers.d/$USERNAME

USER $USERNAME

RUN curl -sSL https://install.python-poetry.org | python3 - \
    && mkdir -p $ROS2_WS/src \
    && git clone https://github.com/necst-telescope/necst-msgs.git $ROS2_WS/src/necst-msgs \
    && . /opt/ros/humble/setup.sh \
    && ( cd $ROS2_WS && colcon build ) \
    && echo ". /opt/ros/humble/setup.sh" >> /home/$USERNAME/.bashrc \
    && echo ". $ROS2_WS/install/setup.sh" >> /home/$USERNAME/.bashrc \
    && curl -o /home/$USERNAME/.bashgit https://raw.githubusercontent.com/oyvindstegard/bashgit/master/.bashgit \
    && echo 'if [ -f /home/$USERNAME/.bashgit ]; then . ~/.bashgit; fi' >> /home/$USERNAME/.bashrc
