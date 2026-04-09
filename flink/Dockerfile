# --- Stage 1: Build the Flink job JAR ---
FROM maven:3.9-eclipse-temurin-17 AS builder
WORKDIR /build
COPY pom.xml .
RUN mvn dependency:go-offline -B
COPY src/ src/
RUN mvn clean package -DskipTests -B

# --- Stage 2: Runtime image ---
FROM flink:1.19-java17
# Ensure checkpoint directory is writable by the flink user
RUN mkdir -p /opt/flink/checkpoints && chown -R flink:flink /opt/flink/checkpoints
# Copy the built JAR into Flink's usrlib directory
COPY --from=builder /build/target/ingestion-hot-path-1.0.0.jar /opt/flink/usrlib/ingestion-hot-path-1.0.0.jar
