FROM gcr.io/distroless/java17-debian11
COPY build/libs/tms-ekstern-varsling-all.jar app/app.jar
ENV PORT=8080
EXPOSE $PORT
WORKDIR app
CMD ["app.jar"]
