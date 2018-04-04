FROM 192.168.0.132/library/java8-alpine:latest
MAINTAINER lai dingqing <laidingqing@gmail.com>

ADD security-1.0.0.jar app.jar
RUN bash -c 'touch /app.jar'

COPY application.properties /etc/application.properties

ENTRYPOINT ["java", "-jar", "/app.jar", "--spring.config.location=file:/etc/"]
