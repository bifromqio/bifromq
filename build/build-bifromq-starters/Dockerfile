ARG BASE_IMAGE=centos:7
FROM --platform=$BUILDPLATFORM ${BASE_IMAGE} AS builder
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


FROM ${BASE_IMAGE}

RUN groupadd -r -g 1000 bifromq  \
    && useradd -r -m -u 1000 -g bifromq bifromq \
    && mkdir /usr/share/bifromq

COPY --chown=bifromq:bifromq --from=builder /jdk-17.0.2 /usr/share/bifromq/jdk-17.0.2
COPY --chown=bifromq:bifromq target/bifromq-1.0.0-SNAPSHOT-standalone.tar.gz /usr/share/bifromq/

RUN cd /usr/share/bifromq \
    && tar -zxvf bifromq-*.tar.gz --strip-components 1 \
    && rm -rf bifromq-*.tar.gz \
    && chown -Rf bifromq:bifromq /usr/share/bifromq


ENV JAVA_HOME /usr/share/bifromq/jdk-17.0.2
ENV PATH /usr/share/bifromq/jdk-17.0.2/bin:$PATH
WORKDIR /usr/share/bifromq

USER bifromq
EXPOSE 1883 1884 80 443
CMD ["./bin/standalone.sh", "start", "-fg"]
