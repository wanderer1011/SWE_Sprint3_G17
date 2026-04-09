package com.idop.flink.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serializable;
import java.util.Map;

/**
 * Represents a telemetry event from the telemetry-hot topic.
 * Matches the schema produced by the Vector Aggregator.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class TelemetryEvent implements Serializable {

    private static final long serialVersionUID = 1L;

    @JsonProperty("timestamp")
    private String timestamp;

    @JsonProperty("trace_id")
    private String traceId;

    @JsonProperty("span_id")
    private String spanId;

    @JsonProperty("parent_span_id")
    private String parentSpanId;

    @JsonProperty("service_name")
    private String serviceName;

    @JsonProperty("service_version")
    private String serviceVersion;

    @JsonProperty("severity_text")
    private String severityText;

    @JsonProperty("severity")
    private String severity;

    @JsonProperty("severity_number")
    private int severityNumber;

    @JsonProperty("category")
    private String category;

    @JsonProperty("source_type")
    private String sourceType;

    @JsonProperty("signal_type")
    private String signalType;

    @JsonProperty("status_code")
    private int statusCode;

    @JsonProperty("body")
    private String body;

    @JsonProperty("error_class")
    private String errorClass;

    @JsonProperty("host_name")
    private String hostName;

    @JsonProperty("environment")
    private String environment;

    @JsonProperty("deployment_id")
    private String deploymentId;

    @JsonProperty("duration_ms")
    private double durationMs = -1.0;

    @JsonProperty("pipeline_ts")
    private String pipelineTs;

    @JsonProperty("pii_masked")
    private boolean piiMasked;

    @JsonProperty("ml_features")
    private Map<String, Object> mlFeatures;

    @JsonProperty("is_anomalous")
    private boolean isAnomalous;

    @JsonProperty("anomaly_reason")
    private String anomalyReason;

    @JsonProperty("is_metric")
    private boolean isMetric;

    @JsonProperty("is_security_flag")
    private boolean isSecurityFlag;

    @JsonProperty("resource_flat")
    private Map<String, Object> resourceFlat;

    // Constructors
    public TelemetryEvent() {}

    // Getters & Setters
    public String getTimestamp() { return timestamp; }
    public void setTimestamp(String timestamp) { this.timestamp = timestamp; }

    public String getTraceId() { return traceId; }
    public void setTraceId(String traceId) { this.traceId = traceId; }

    public String getSpanId() { return spanId; }
    public void setSpanId(String spanId) { this.spanId = spanId; }

    public String getParentSpanId() { return parentSpanId; }
    public void setParentSpanId(String parentSpanId) { this.parentSpanId = parentSpanId; }

    public String getServiceName() {
        if (serviceName != null) return serviceName;
        return "unknown";
    }
    public void setServiceName(String serviceName) { this.serviceName = serviceName; }

    public String getServiceVersion() { return serviceVersion; }
    public void setServiceVersion(String serviceVersion) { this.serviceVersion = serviceVersion; }

    public String getSeverityText() { return severityText; }
    public void setSeverityText(String severityText) { this.severityText = severityText; }

    public String getSeverity() { return severity; }
    public void setSeverity(String severity) { this.severity = severity; }

    public int getSeverityNumber() { return severityNumber; }
    public void setSeverityNumber(int severityNumber) { this.severityNumber = severityNumber; }

    public String getCategory() { return category; }
    public void setCategory(String category) { this.category = category; }

    public String getSourceType() { return sourceType; }
    public void setSourceType(String sourceType) { this.sourceType = sourceType; }

    public String getSignalType() { return signalType; }
    public void setSignalType(String signalType) { this.signalType = signalType; }

    public int getStatusCode() { return statusCode; }
    public void setStatusCode(int statusCode) { this.statusCode = statusCode; }

    public String getBody() { return body; }
    public void setBody(String body) { this.body = body; }

    public String getErrorClass() { return errorClass; }
    public void setErrorClass(String errorClass) { this.errorClass = errorClass; }

    public String getHostName() { return hostName; }
    public void setHostName(String hostName) { this.hostName = hostName; }

    public String getEnvironment() { return environment; }
    public void setEnvironment(String environment) { this.environment = environment; }

    public String getDeploymentId() { return deploymentId; }
    public void setDeploymentId(String deploymentId) { this.deploymentId = deploymentId; }

    public double getDurationMs() { return durationMs; }
    public void setDurationMs(double durationMs) { this.durationMs = durationMs; }

    public String getPipelineTs() { return pipelineTs; }
    public void setPipelineTs(String pipelineTs) { this.pipelineTs = pipelineTs; }

    public boolean isPiiMasked() { return piiMasked; }
    public void setPiiMasked(boolean piiMasked) { this.piiMasked = piiMasked; }

    public Map<String, Object> getMlFeatures() { return mlFeatures; }
    public void setMlFeatures(Map<String, Object> mlFeatures) { this.mlFeatures = mlFeatures; }

    public boolean isAnomalous() { return isAnomalous; }
    public void setAnomalous(boolean anomalous) { isAnomalous = anomalous; }

    public String getAnomalyReason() { return anomalyReason; }
    public void setAnomalyReason(String anomalyReason) { this.anomalyReason = anomalyReason; }

    public boolean isMetric() { return isMetric; }
    public void setMetric(boolean metric) { isMetric = metric; }

    public boolean isSecurityFlag() { return isSecurityFlag; }
    public void setSecurityFlag(boolean securityFlag) { isSecurityFlag = securityFlag; }

    public Map<String, Object> getResourceFlat() { return resourceFlat; }
    public void setResourceFlat(Map<String, Object> resourceFlat) { this.resourceFlat = resourceFlat; }

    // ── ML Feature Accessors (convenience) ──

    /** Safely extract a double from the ml_features map. */
    public double getMlFeatureDouble(String key) {
        if (mlFeatures == null) return 0.0;
        Object val = mlFeatures.get(key);
        if (val instanceof Number) return ((Number) val).doubleValue();
        return 0.0;
    }

    /** Safely extract a boolean from the ml_features map. */
    public boolean getMlFeatureBool(String key) {
        if (mlFeatures == null) return false;
        return Boolean.TRUE.equals(mlFeatures.get(key));
    }

    /** Safely extract a string from the ml_features map. */
    public String getMlFeatureString(String key) {
        if (mlFeatures == null) return "";
        Object val = mlFeatures.get(key);
        return val != null ? val.toString() : "";
    }

    /** Safely extract an int from the ml_features map. */
    public int getMlFeatureInt(String key) {
        if (mlFeatures == null) return 0;
        Object val = mlFeatures.get(key);
        if (val instanceof Number) return ((Number) val).intValue();
        return 0;
    }
}
