# FROM giovtorres/slurm-docker-cluster
#
# LABEL maintainer="Wes Bonelli"
#
# COPY . /opt/plantit-cli
#
# RUN set -ex \
#     && yum -y update \
#     && yum -y install \
#     build-essential \
#     python3-setuptools \
#     python3-pip \
#     python3-numexpr \
#     libgl1-mesa-glx \
#     libsm6 \
#     libxext6 \
#     libfontconfig1 \
#     libxrender1 \
#     openssh-server \
#
# # SSH server
# EXPOSE 22
# ENV NOTVISIBLE "in users profile"
# RUN mkdir /var/run/sshd \
#     && echo 'root:root' | chpasswd \
#     && sed -i 's/#PermitRootLogin prohibit-password/PermitRootLogin yes/' /etc/ssh/sshd_config \
#     && sed -i 's/#PasswordAuthentication no/PasswordAuthentication yes/' /etc/ssh/sshd_config \
#     # SSH login fix. Otherwise user is kicked off after login
#     && sed 's@session\s*required\s*pam_loginuid.so@session optional pam_loginuid.so@g' -i /etc/pam.d/sshd \
#     && echo "export VISIBLE=now" >> /etc/profile \
#     && /usr/bin/ssh-keygen -A
#
# RUN pip3 install --upgrade pip && \
#     pip3 install -e /opt/plantit-cli
#
# ENV LC_ALL=C.UTF-8
# ENV LANG=C.UTF-8
#
# SHELL ["/bin/bash", "-c"]
# RUN echo $'if [ -d /etc/profile.d ]; then for i in /etc/profile.d/*.sh; do if [ -r $i ]; then . $i; fi done; fi' >> /etc/bash.bashrc

# adapted from https://github.com/kaczmarj/singularity-in-docker/blob/master/Dockerfile
FROM golang:1.15.7-alpine3.13 as builder

WORKDIR $GOPATH/src/github.com/sylabs
RUN apk add --no-cache gawk gcc git libc-dev linux-headers libressl-dev libuuid libseccomp-dev make util-linux-dev

ARG SINGULARITY_COMMITISH="master"
RUN git clone https://github.com/sylabs/singularity.git \
    && cd singularity \
    && git checkout "$SINGULARITY_COMMITISH" \
    && ./mconfig -p /usr/local/singularity \
    && cd builddir \
    && make \
    && make install

FROM alpine:3.13
COPY --from=builder /usr/local/singularity /usr/local/singularity
COPY . /opt/plantit-cli
ENV PYTHONUNBUFFERED=1

# sshd
RUN apk update && apk add --update --no-cache openssh && rm  -rf /tmp/* /var/cache/apk/*
RUN rm -rf /etc/ssh/ssh_host_rsa_key /etc/ssh/ssh_host_dsa_key
EXPOSE 22

# python and plantit CLI
RUN apk add --update --no-cache \
        gcc \
        make \
        musl-dev \
        python3 \
        python3-dev && \
    apk add --update --no-cache \
        python3 \
        python3-dev \
        krb5-dev \
        libxml2-dev \
        libxslt-dev \
        libffi-dev \
        jpeg-dev \
        zlib-dev \
        libffi-dev \
        openssl-dev \
        git && \
    ln -sf python3 /usr/bin/python && \
    python3 -m ensurepip && \
    pip3 install --no-cache --upgrade pip setuptools && \
    pip3 install --upgrade pip && \
    pip3 install -e /opt/plantit-cli

# remaining singularity install steps
ENV PATH="/usr/local/singularity/bin:$PATH" \
    SINGULARITY_TMPDIR="/tmp-singularity"

RUN apk add --no-cache ca-certificates libseccomp squashfs-tools tzdata \
    && mkdir -p $SINGULARITY_TMPDIR \
    && cp /usr/share/zoneinfo/UTC /etc/localtime \
    && apk del tzdata \
    && rm -rf /tmp/* /var/cache/apk/*

# not sure if still necessary?
ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8

# set working directory
WORKDIR /work