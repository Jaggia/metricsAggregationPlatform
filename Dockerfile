FROM gradle:4.1 as builder
COPY . /project
WORKDIR /project
USER root
RUN ["gradle", "kafkaJar", "dbJar"]

FROM openjdk:9
#FROM openjdk:9-jre-slim
MAINTAINER Anadi Jaggia

COPY --from=builder /project/build/libs/kafkaJar.jar /home/kafkaJar.jar
COPY --from=builder /project/build/libs/dbJar.jar /home/dbJar.jar
ENV JAVA_OPTS="-Xmx1G -Xms1G"
ENV NUM_THREADS 4
RUN mkdir -p /etc/jaggia

ADD dockerEntryPoint.sh /dockerEntryPoint.sh
ENTRYPOINT ["/dockerEntryPoint.sh"]