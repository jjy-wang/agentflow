# ---- 构建阶段 ----
FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /build
COPY pom.xml .
RUN mvn -B -q dependency:go-offline
COPY src ./src
RUN mvn -B -q clean package -DskipTests

# ---- 运行阶段 ----
FROM eclipse-temurin:17-jre-alpine
WORKDIR /app
RUN addgroup -S app && adduser -S app -G app
COPY --from=build /build/target/agentflow-1.0.0.jar app.jar
RUN chown -R app:app /app
USER app
EXPOSE 8080
ENV JAVA_OPTS="-Xms512m -Xmx512m -XX:+UseG1GC -XX:MaxGCPauseMillis=200"
ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar app.jar"]
