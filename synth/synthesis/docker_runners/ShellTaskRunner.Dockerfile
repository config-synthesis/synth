FROM debian:11

WORKDIR /root/runner
CMD /bin/systemd
COPY apt-proxy.sh /proxy-scripts/apt-proxy.sh
COPY pypi-proxy.sh /proxy-scripts/pypi-proxy.sh

RUN apt-get update
RUN apt-get install -y net-tools netcat

RUN /proxy-scripts/apt-proxy.sh 3142
RUN /proxy-scripts/pypi-proxy.sh 3141

RUN apt-get update
RUN apt-get install -y systemd procps

COPY run_command.sh /scripts/run_command.sh
COPY cleanup.sh /scripts/cleanup.sh
RUN /scripts/cleanup.sh
RUN rm -r /var/lib/apt/lists/*
