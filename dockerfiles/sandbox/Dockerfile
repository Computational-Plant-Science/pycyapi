FROM giovtorres/slurm-docker-cluster

# RUN apt-get update
# RUN DEBIAN_FRONTEND=noninteractive apt-get install -y \
RUN set -ex \
    && yum -y update \
    && yum -y install \
    autoconf \
    automake \
    build-essential \
    libssl-dev \
    uuid-dev \
    libuuid-devel \
    libgpgme11-dev \
    squashfs-tools \
    libseccomp-dev \
    wget \
    pkg-config \
    cryptsetup \
    openssh-server \
    postgresql-client \
    git \
    python3-devel \
    libfuse2 \
    vim \
    tcl \
    rsync \
    tclsh \
    gcc \
    make \
    # ruby \
    # ruby-dev \
    # libpam0g-dev \
    # libmariadb-client-lgpl-dev \
    # libmysqlclient-dev \
    # libmunge-dev \
    # libmunge2 \
    # mariadb-server \
    # munge \
    # slurmd \
    # slurm-client \
    # slurmctld \
    # slurmdbd \
    uuid-dev

# golang
RUN mkdir /opt/go \
    && cd /opt/go \
    && export VERSION=1.13 OS=linux ARCH=amd64 \
    && wget https://dl.google.com/go/go$VERSION.$OS-$ARCH.tar.gz \
    && tar -C /usr/local -xzvf go$VERSION.$OS-$ARCH.tar.gz \
    && rm go$VERSION.$OS-$ARCH.tar.gz \
    && export GOROOT=/usr/local/go \
    && export GOPATH=/opt/go \
    && export PATH=/usr/local/go/bin:$PATH \
    && echo 'export GOROOT=/usr/local/go' >> ~/.bashrc \
    && echo 'export GOPATH=/opt/go' >> ~/.bashrc \
    && echo 'export PATH=/usr/local/go/bin:$PATH' >> ~/.bashrc

# singularity
RUN mkdir /opt/singularity; cd /opt/singularity \
    && export VERSION=3.5.2  \
    && export GOROOT=/usr/local/go \
    && export GOPATH=/opt/go \
    && export PATH=/usr/local/go/bin:$PATH \
    && wget https://github.com/sylabs/singularity/releases/download/v${VERSION}/singularity-${VERSION}.tar.gz \
    && tar -xzf singularity-${VERSION}.tar.gz \
    && cd singularity \
    && ./mconfig \
    && make -C builddir \
    && make -C builddir install

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

# SLURM
# RUN mkdir /opt/slurm \
#     && mkdir /etc/slurm
# COPY dockerfiles/sandbox/configure-slurm-db.sql /opt/slurm
# COPY dockerfiles/sandbox/slurm.conf /etc/slurm-llnl
# RUN cd /opt \
#     && git clone https://github.com/mknoxnv/ubuntu-slurm.git \
#     && cd /opt/slurm \
#     && gem install fpm \
#     && wget https://download.schedmd.com/slurm/slurm-17.11.12.tar.bz2 \
#     && tar xvjf slurm-17.11.12.tar.bz2 \
#     && cd slurm-17.11.12 \
#     && ./configure --prefix=/tmp/slurm-build --sysconfdir=/etc/slurm --enable-pam --with-pam_dir=/lib/x86_64-linux-gnu/security/ --without-shared-libslurm \
#     && make \
#     && make install \
#     && cd .. \
#     && mkdir -p /etc/slurm /etc/slurm/prolog.d /etc/slurm/epilog.d /var/spool/slurm/ctld /var/spool/slurm/d /var/log/slurm \
#     && chown slurm /var/spool/slurm/ctld /var/spool/slurm/d /var/log/slurm \
#     && cp /opt/ubuntu-slurm/slurmd.service /etc/systemd/system/ \
#     && cp /opt/ubuntu-slurm/slurmctld.service /etc/systemd/system/ \
#     && service munge start \
#     && service slurmctld start \
#     && service slurmd start \
#     && update-rc.d munge defaults \
#     && update-rc.d slurmctld defaults \
#     && update-rc.d slurmd defaults

RUN mkdir -p /opt/plantit-cli/scripts \
    && mkdir -p /opt/plantit-cli/runs

COPY ./setup.py /opt/plantit-cli
COPY ./LICENSE /opt/plantit-cli
COPY ./README.md /opt/plantit-cli
COPY ./MANIFEST.in /opt/plantit-cli
COPY ./plantit_cli /opt/plantit-cli/plantit_cli

RUN pip3 install coveralls \
    && pip3 install pytest \
    && pip3 install dask[distributed] \
    && pip3 install dask-jobqueue \
    && pip3 install -e /opt/plantit-cli

SHELL ["/bin/bash", "-c"]
RUN echo $'if [ -d /etc/profile.d ]; then for i in /etc/profile.d/*.sh; do if [ -r $i ]; then . $i; fi done; fi' >> /etc/bash.bashrc
