FROM openjdk:8-jdk-slim as builder
ARG DOCKER_TAG=latest

COPY . /src
WORKDIR /src

RUN ./gradlew clean distTar -PprojVersion=${DOCKER_TAG}
RUN find /src

FROM openjdk:8-jdk-slim
ARG DOCKER_TAG=latest

COPY --from=builder /src/build/distributions/cqlkit-${DOCKER_TAG}.tar .

RUN tar xvf cqlkit-${DOCKER_TAG}.tar -C /usr/share/ && \
    rm /usr/share/cqlkit/bin/*.bat && \
    cp -rs /usr/share/cqlkit/bin/* /usr/bin/ && \
    rm cqlkit-${DOCKER_TAG}.tar