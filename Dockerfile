ARG BASE_IMAGE=centos:7
FROM --platform=$TARGETPLATFORM ${BASE_IMAGE} AS builder
ARG TARGETPLATFORM
ARG BUILDPLATFORM

# download openjdk
RUN if [[ "$TARGETPLATFORM" == *amd* ]]; then \
      export JDK_ARCH=x64;  \
    else \
      export JDK_ARCH=aarch64; \
    fi \
    && curl --retry 5 -S -L -O https://download.java.net/java/GA/jdk17.0.2/dfd4a8d0985749f896bed50d7138ee7f/8/GPL/openjdk-17.0.2_linux-${JDK_ARCH}_bin.tar.gz \
    && tar -zxvf openjdk-17.0.2_linux-${JDK_ARCH}_bin.tar.gz \
    && rm -rf openjdk-17.0.2_linux-${JDK_ARCH}_bin.tar.gz

FROM --platform=$TARGETPLATFORM ${BASE_IMAGE}

RUN groupadd -r -g 1000 bifromq  \
    && useradd -r -m -u 1000 -g bifromq bifromq \
    && mkdir /usr/share/bifromq

COPY --chown=bifromq:bifromq --from=builder /jdk-17.0.2 /usr/share/bifromq/jdk-17.0.2
COPY --chown=bifromq:bifromq bifromq-*-standalone.tar.gz /usr/share/bifromq/

RUN tar -zxvf /usr/share/bifromq/bifromq-*-standalone.tar.gz --strip-components 1 -C /usr/share/bifromq \
    && rm -rf /usr/share/bifromq/bifromq-*-standalone.tar.gz

ENV JAVA_HOME /usr/share/bifromq/jdk-17.0.2
ENV PATH /usr/share/bifromq/jdk-17.0.2/bin:$PATH
WORKDIR /usr/share/bifromq

USER bifromq
EXPOSE 1883 1884 80 443
CMD ["./bin/standalone.sh", "start", "-fg"]
