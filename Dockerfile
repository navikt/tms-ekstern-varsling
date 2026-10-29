FROM europe-north1-docker.pkg.dev/cgr-nav/pull-through/nav.no/jre:openjdk-21
ENV JAVA_OPTS='-XX:MaxRAMPercentage=75'
COPY build/libs/*.jar ./
ENV TZ="Europe/Oslo"
EXPOSE 8080
CMD ["-jar", "app.jar"]