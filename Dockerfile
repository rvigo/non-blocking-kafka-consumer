FROM adoptopenjdk/openjdk11:alpine
RUN mkdir -p /opt/app
COPY target/consumer-0.0.1-SNAPSHOT.jar /opt/app/consumer.jar
EXPOSE 8082

ENTRYPOINT exec java -Djava.security.egd=file:/dev/./urandom -jar /opt/app/consumer.jar
