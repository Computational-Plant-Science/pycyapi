FROM giovtorres/slurm-docker-cluster

LABEL maintainer="Wes Bonelli"

COPY . /opt/plantit-cli

RUN set -ex \
    && yum -y update \
    && yum -y install \
    build-essential \
    python3-setuptools \
    python3-pip \
    python3-numexpr \
    libgl1-mesa-glx \
    libsm6 \
    libxext6 \
    libfontconfig1 \
    libxrender1 \
    openssh-server \

# SSH server
EXPOSE 22
ENV NOTVISIBLE "in users profile"
RUN mkdir /var/run/sshd \
    && echo 'root:root' | chpasswd \
    && sed -i 's/#PermitRootLogin prohibit-password/PermitRootLogin yes/' /etc/ssh/sshd_config \
    && sed -i 's/#PasswordAuthentication no/PasswordAuthentication yes/' /etc/ssh/sshd_config \
    # SSH login fix. Otherwise user is kicked off after login
    && sed 's@session\s*required\s*pam_loginuid.so@session optional pam_loginuid.so@g' -i /etc/pam.d/sshd \
    && echo "export VISIBLE=now" >> /etc/profile \
    && /usr/bin/ssh-keygen -A

RUN pip3 install --upgrade pip && \
    pip3 install -e /opt/plantit-cli

ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8

SHELL ["/bin/bash", "-c"]
RUN echo $'if [ -d /etc/profile.d ]; then for i in /etc/profile.d/*.sh; do if [ -r $i ]; then . $i; fi done; fi' >> /etc/bash.bashrc