FROM ubuntu:18.04

RUN apt-get update
RUN DEBIAN_FRONTEND=noninteractive apt-get install -y \
    openssh-server \
    postgresql-client \
    singularity-container \
    git \
    python3 \
    python3-pip \
    libfuse2 \
    vim \
    tcl \
    rsync \
    tclsh \
    gcc \
    make \
    ruby \
    ruby-dev \
    libpam0g-dev \
    libmariadb-client-lgpl-dev \
    libmysqlclient-dev \
    libmunge-dev \
    libmunge2 \
    mariadb-server \
    munge \
    slurmd \
    slurm-client \
    slurmctld \
    slurmdbd

RUN wget https://sourceforge.net/projects/lmod/files/lua-5.1.4.9.tar.bz2 \
    && tar xf lua-5.1.4.9.tar.bz2 \
    && cd lua-5.1.4.9 \
    && ./configure --prefix=/opt/apps/lua/5.1.4.9 \
    && make; make install \
    && cd /opt/apps/lua; ln -s 5.1.4.9 lua \
    && ln -s /opt/apps/lua/lua/bin/lua /usr/local/bin \
    && ln -s /opt/apps/lua/lua/bin/luac /usr/local/bin \
    && export PATH=$PATH:/usr/local/bin/lua \
    && export PATH=$PATH:/usr/local/bin/luac \
    && cd

RUN wget https://sourceforge.net/projects/lmod/files/Lmod-8.2.tar.bz2 \
    && tar xf Lmod-8.2.tar.bz2 \
    && cd Lmod-8.2 \
    && ./configure --prefix=/opt/apps --with-fastTCLInterp=no \
    && make install \
    && ln -s /opt/apps/lmod/lmod/init/profile /etc/profile.d/z00_lmod.sh \
    && ln -s /opt/apps/lmod/lmod/init/cshrc /etc/profile.d/z00_lmod.csh

RUN mkdir /var/run/sshd
RUN echo 'root:root' | chpasswd
RUN sed -i 's/#PermitRootLogin prohibit-password/PermitRootLogin yes/' /etc/ssh/sshd_config
RUN sed -i 's/#PasswordAuthentication no/PasswordAuthentication yes/' /etc/ssh/sshd_config

# SSH login fix. Otherwise user is kicked off after login
RUN sed 's@session\s*required\s*pam_loginuid.so@session optional pam_loginuid.so@g' -i /etc/pam.d/sshd

ENV NOTVISIBLE "in users profile"
RUN echo "export VISIBLE=now" >> /etc/profile

EXPOSE 22
CMD ["/usr/sbin/sshd", "-D"]

COPY dockerfiles/cluster/qsub /usr/bin

RUN wget https://files.renci.org/pub/irods/releases/4.1.12/ubuntu14/irods-icommands-4.1.12-ubuntu14-x86_64.deb
RUN dpkg -i irods-icommands-4.1.12-ubuntu14-x86_64.deb
RUN rm irods-icommands-4.1.12-ubuntu14-x86_64.deb
COPY ./wait-for-it.sh /root/
COPY ./configure-irods.sh /root/
RUN chmod +x /root/wait-for-it.sh
RUN chmod +x /root/configure-irods.sh

RUN mkdir /opt/slurm
RUN mkdir /etc/slurm
COPY dockerfiles/cluster/configure-slurm-db.sql /opt/slurm
COPY dockerfiles/cluster/slurm.conf /etc/slurm-llnl

RUN cd /opt/slurm \
    && gem install fpm \
    && service munge start \
    && service mysql start \
    && mysql -u root < /opt/slurm/configure-slurm-db.sql \
    && wget https://download.schedmd.com/slurm/slurm-17.11.12.tar.bz2 \
    && tar xvjf slurm-17.11.12.tar.bz2 \
    && cd slurm-17.11.12 \
    && ./configure --prefix=/tmp/slurm-build --sysconfdir=/etc/slurm --enable-pam --with-pam_dir=/lib/x86_64-linux-gnu/security/ --without-shared-libslurm \
    && make \
    && make install \
    && cd .. \
    && mkdir -p /etc/slurm /etc/slurm/prolog.d /etc/slurm/epilog.d /var/spool/slurm/ctld /var/spool/slurm/d /var/log/slurm \
    # && chown slurm /var/spool/slurm/ctld /var/spool/slurm/d /var/log/slurm \
    # && systemctl enable mysql \
    # && systemctl start mysql \
    # && mysql -u root < /opt/slurm/configure-slurm-db.sql \
    # && cp ubuntu-slurm/slurmdbd.service /etc/systemd/system/ \
    # && cp ubuntu-slurm/slurmctld.service /etc/systemd/system/ \
    # && systemctl daemon-reload \
    # && systemctl enable slurmdbd \
    # && systemctl start slurmdbd \
    # && systemctl enable slurmctld \
    # && systemctl start slurmctld \
    # && sacctmgr add cluster compute-cluster \
    # && sacctmgr add account compute-account description="PlantIT" Organization=PlantIT \
    # && sacctmgr create user myuser account=compute-account adminlevel=None

RUN mkdir -p /opt/plantit-clusterside-home \
    && mkdir -p /opt/plantit-clusterside
WORKDIR /opt/plantit-clusterside
COPY . ./
RUN chmod +x configure-irods.sh \
    && chmod +x wait-for-it.sh \
    && chmod +x wait-for-postgres.sh \
    && pip3 install -e . \
    && pip3 install -U pytest

SHELL ["/bin/bash", "-c"]
RUN echo $'if [ -d /etc/profile.d ]; then for i in /etc/profile.d/*.sh; do if [ -r $i ]; then . $i; fi done; fi' >> /etc/bash.bashrc
