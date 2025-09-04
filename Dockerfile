FROM gcr.io/distroless/java21-debian12
ENV JAVA_OPTS='-XX:MaxRAMPercentage=75'
COPY build/libs/*.jar ./
ENV TZ="Europe/Oslo"
EXPOSE 8080
CMD ["app.jar"]