FROM flink:1.19-java17

# Ensure checkpoint directory is writable by the flink user
RUN mkdir -p /opt/flink/checkpoints && chown -R flink:flink /opt/flink/checkpoints

# Copy the built JAR into Flink's usrlib directory
COPY target/ingestion-hot-path-1.0.0.jar /opt/flink/usrlib/ingestion-hot-path-1.0.0.jar
