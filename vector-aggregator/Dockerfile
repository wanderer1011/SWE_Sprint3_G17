FROM timberio/vector:0.38.0-debian
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*
COPY vector.yaml /etc/vector/vector.yaml
COPY vrl/ /etc/vector/vrl/
EXPOSE 4317 8686 8687 9598              
ENTRYPOINT ["vector", "--config", "/etc/vector/vector.yaml"]
