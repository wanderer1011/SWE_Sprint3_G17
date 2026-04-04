FROM timberio/vector:0.38.0-debian

# the below 2 lines added by rohith for "vector" 's health check
# Required by docker-compose healthcheck in docker-compose.yml
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*

COPY vector.yaml /etc/vector/vector.yaml
COPY vrl/ /etc/vector/vrl/
EXPOSE 4317 8686 8687
ENTRYPOINT ["vector", "--config", "/etc/vector/vector.yaml"]
