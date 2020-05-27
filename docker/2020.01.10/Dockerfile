FROM alpine:3.8

LABEL maintainer="Abe.Chua <abecai@gmail.com>" \
    description="Beacon MQTT Broker"

ENV VERSION=2020.01.10

RUN set -x && \
    addgroup -S -g 1883 beacon 2>/dev/null && \
    adduser -S -u 1883 -D -H -h /var/empty -s /sbin/nologin -G beacon -g beacon beacon 2>/dev/null && \
    mkdir -p /beacon/config && \
    chown -R beacon:beacon /beacon

WORKDIR /beacon
# Set up the entry point script and default command

COPY docker-entrypoint.sh /
COPY mbs_linux ./
COPY mbs.conf ./config/

EXPOSE 1883
ENTRYPOINT ["/docker-entrypoint.sh"]
CMD ["./mbs_linux", "-c", "./config/mbs.conf"]