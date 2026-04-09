# Pin to a specific patch release — avoid floating `:v1.30-latest` tag (supply-chain risk)
FROM envoyproxy/envoy:v1.30.4

# Install curl for healthchecks (run as root before USER switch)
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*

# Copy configuration and validate it at build time (fail-fast on bad YAML)
COPY envoy.yaml /etc/envoy/envoy.yaml
RUN envoy --mode validate -c /etc/envoy/envoy.yaml

# Ingestion listener + admin API
EXPOSE 10000 9901

# Run as non-root (Envoy's base image provides uid 101)
USER envoy

HEALTHCHECK --interval=15s --timeout=3s --start-period=10s --retries=3 \
    CMD curl -sf http://127.0.0.1:9901/ready || exit 1

ENTRYPOINT ["envoy"]
CMD ["-c", "/etc/envoy/envoy.yaml"]       # FIX: explicit config path — base image CMD was cleared by ENTRYPOINT override
CMD ["-c", "/etc/envoy/envoy.yaml", "--log-level", "info"]
