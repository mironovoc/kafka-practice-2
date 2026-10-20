FROM eclipse-temurin:21-jdk AS build
WORKDIR /workspace/kafka-practice2
COPY kafka-practice2/ ./
RUN ./gradlew bootJar --no-daemon

FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /workspace/kafka-practice2/build/libs/kafka-practice2.jar app.jar
ENTRYPOINT ["java", "-jar", "app.jar"]
