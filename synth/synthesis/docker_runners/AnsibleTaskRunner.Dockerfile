FROM docker:20.10.12-alpine3.15 AS DOCKER

FROM python:3.9.7

WORKDIR /root/runner
CMD /bin/systemd
COPY ansible.cfg ansible.cfg
COPY apt-proxy.sh /proxy-scripts/apt-proxy.sh
COPY pypi-proxy.sh /proxy-scripts/pypi-proxy.sh
COPY --from=DOCKER /usr/local/bin/docker /usr/local/bin/docker
RUN mkdir -p /root/.ansible

RUN apt-get update
RUN apt-get install -y net-tools netcat

RUN /proxy-scripts/apt-proxy.sh 3142
RUN /proxy-scripts/pypi-proxy.sh 3141

RUN apt-get update
RUN apt-get install -y systemd procps python3-apt
RUN pip install ansible==4.5.0

COPY run_command.sh /scripts/run_command.sh
COPY cleanup.sh /scripts/cleanup.sh
RUN /scripts/cleanup.sh
RUN rm -r /var/lib/apt/lists/*
