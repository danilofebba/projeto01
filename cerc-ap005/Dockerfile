FROM ubuntu:latest

RUN apt-get update && apt-get install -y \
    vim \
    net-tools \
    sudo \
    python3 \
    python3-dev \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

RUN python3 -m pip install --upgrade \
    pip \
    setuptools \
    wheel \
    pyarrow \
    s3fs

RUN mkdir -p /opt/pyetlapp
COPY cerc-ap005.py /opt/pyetlapp
COPY docker-entrypoint.sh /opt/pyetlapp

RUN addgroup pyetlappgroup
RUN adduser --gecos '' --ingroup pyetlappgroup --disabled-password pyetlappuser
RUN echo "pyetlappuser ALL=(ALL)   NOPASSWD:ALL" >> /etc/sudoers
RUN chown -R pyetlappuser:pyetlappgroup /opt/pyetlapp
USER pyetlappuser
WORKDIR /home/pyetlappuser

ENTRYPOINT ["/opt/pyetlapp/docker-entrypoint.sh"]
