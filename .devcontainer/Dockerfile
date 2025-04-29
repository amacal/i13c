FROM mcr.microsoft.com/vscode/devcontainers/base:ubuntu-24.04
USER vscode

RUN sudo apt-get update \
    && sudo apt-get upgrade -y \
    && sudo apt-get install -y \
        xxd wget tar make cmake libwayland-client0 \
        xdg-utils libgvc6 pkg-config git build-essential \
        libcapstone-dev qtbase5-dev libqt5xmlpatterns5-dev \
        libqt5svg5-dev qttools5-dev \
    && sudo rm -rf /var/lib/apt/lists/*

RUN cd /tmp \
    && wget https://www.nasm.us/pub/nasm/releasebuilds/2.16.03/nasm-2.16.03.tar.gz \
    && tar -xvzf nasm-2.16.03.tar.gz \
    && cd nasm-2.16.03 \
    && ./configure \
    && make \
    && sudo make install \
    && nasm --version \
    && sudo rm -rf /tmp/nasm-2.16.03

RUN cd /tmp \
    && wget https://github.com/eteran/edb-debugger/releases/download/1.5.0/edb-debugger-1.5.0.tgz \
    && tar -xvzf edb-debugger-1.5.0.tgz \
    && cd edb-debugger \
    && mkdir build \
    && cd build \
    && cmake .. \
    && make \
    && sudo make install

RUN cd /tmp \
    && wget https://github.com/yamnikov-oleg/nasmfmt/releases/download/v0.1/nasmfmt_linux64.tar.gz \
    && tar -xvzf nasmfmt_linux64.tar.gz \
    && sudo mv /tmp/bin/nasmfmt /usr/local/bin

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y \
    && . "$HOME/.cargo/env" \
    && rustup update 1.86.0 \
    && rustup default 1.86.0 \
    && rustup component add rustfmt \
    && rustc --version
