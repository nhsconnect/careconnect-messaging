FROM openjdk:11-slim
VOLUME /tmp

ADD target/ccri-messaging.jar ccri-messaging.jar

ENV JAVA_OPTS="-Xms512m -Xmx1024m"

ENTRYPOINT ["java","-Djava.security.egd=file:/dev/./urandom","-jar","/ccri-messaging.jar"]

