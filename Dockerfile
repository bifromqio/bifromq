ARG BASE_IMAGE=debian:buster-slim

FROM --platform=$TARGETPLATFORM ${BASE_IMAGE} AS builder

ARG TARGETPLATFORM

# Install necessary tools for diagnostics and JDK download
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl net-tools lsof netcat procps less \
    && if [[ "$TARGETPLATFORM" == *amd* ]]; then \
        export JDK_ARCH=x64; \
    else \
        export JDK_ARCH=aarch64; \
    fi \
    && echo "JDK_ARCH is set to ${JDK_ARCH}" \
    && curl --retry 5 -S -L -O https://download.java.net/java/GA/jdk17.0.2/dfd4a8d0985749f896bed50d7138ee7f/8/GPL/openjdk-17.0.2_linux-${JDK_ARCH}_bin.tar.gz \
    && tar -zxvf openjdk-17.0.2_linux-${JDK_ARCH}_bin.tar.gz \
    && rm -rf openjdk-17.0.2_linux-${JDK_ARCH}_bin.tar.gz \
    && rm -rf /var/lib/apt/lists/*

COPY bifromq-*-standalone.tar.gz /

RUN mkdir /bifromq && tar -zxvf /bifromq-*-standalone.tar.gz --strip-components 1 -C /bifromq \
    && rm -rf /bifromq-*-standalone.tar.gz

FROM --platform=$TARGETPLATFORM ${BASE_IMAGE}

RUN groupadd -r -g 1000 bifromq \
    && useradd -r -m -u 1000 -g bifromq bifromq \
    && apt-get update \
    && apt-get install -y --no-install-recommends net-tools lsof netcat procps less \
    && rm -rf /var/lib/apt/lists/*

COPY --chown=bifromq:bifromq --from=builder /jdk-17.0.2 /usr/share/jdk-17.0.2
COPY --chown=bifromq:bifromq --from=builder /bifromq /home/bifromq/

ENV JAVA_HOME /usr/share/jdk-17.0.2
ENV PATH /usr/share/jdk-17.0.2/bin:$PATH

WORKDIR /home/bifromq

USER bifromq

# Set common command aliases
RUN echo "alias ll='ls -al'" >> ~/.bashrc

EXPOSE 1883 1884 80 443

CMD ["./bin/standalone.sh", "start", "-fg"]