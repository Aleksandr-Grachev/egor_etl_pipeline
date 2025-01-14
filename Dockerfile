FROM bitnami/minideb:bookworm

ARG USERNAME
ARG USER_UID
ARG USER_GID

USER root
# Create the user
RUN groupadd --gid $USER_GID $USERNAME \
    && useradd -s /bin/bash --uid $USER_UID --gid $USER_GID -m $USERNAME \
    #
    # [Optional] Add git and sudo support. Omit if you don't need to install software after connecting.
    && apt-get update \
    && apt-get install -y git \
    && apt-get install -y sudo \
    && echo $USERNAME ALL=\(root\) NOPASSWD:ALL > /etc/sudoers.d/$USERNAME \
    && chmod 0440 /etc/sudoers.d/$USERNAME

RUN install_packages postgresql-client

RUN mkdir /.vscode-server /workspace
RUN chown -R $USERNAME /.vscode-server /workspace

USER $USERNAME


