package com.gpb.datafirewall.audit;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;

public final class CefAuditEvent {

    private final String eventId;
    private final Instant eventTime;
    private final AuditEventType eventType;
    private final String status;
    private final String jobName;
    private final String jobVersion;
    private final String clusterId;
    private final String hostname;
    private final Integer subtaskIndex;
    private final Integer parallelism;
    private final Map<String, Object> payload;

    private CefAuditEvent(Builder b) {
        this.eventId = b.eventId == null || b.eventId.isBlank() ? UUID.randomUUID().toString() : b.eventId;
        this.eventTime = parseInstantOrNow(b.eventTime);
        this.eventType = b.eventType;
        this.status = b.status;
        this.jobName = b.jobName;
        this.jobVersion = b.jobVersion;
        this.clusterId = b.clusterId;
        this.hostname = b.hostname;
        this.subtaskIndex = b.subtaskIndex;
        this.parallelism = b.parallelism;
        this.payload = b.payload == null ? Map.of() : Collections.unmodifiableMap(new LinkedHashMap<>(b.payload));
    }

    public static Builder builder(AuditEventType eventType) {
        return new Builder(eventType);
    }

    public String eventId() { return eventId; }
    public Instant eventTime() { return eventTime; }
    public AuditEventType eventType() { return eventType; }
    public String status() { return status; }
    public String jobName() { return jobName; }
    public String jobVersion() { return jobVersion; }
    public String clusterId() { return clusterId; }
    public String hostname() { return hostname; }
    public Integer subtaskIndex() { return subtaskIndex; }
    public Integer parallelism() { return parallelism; }
    public Map<String, Object> payload() { return payload; }

    public Map<String, Object> toMap() {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("eventId", eventId);
        out.put("eventTime", eventTime.toString());
        out.put("eventType", eventType == null ? null : eventType.name());
        out.put("eventClass", eventType == null ? null : eventType.eventClass());
        out.put("eventName", eventType == null ? null : eventType.eventName());
        out.put("status", status);
        out.put("jobName", jobName);
        out.put("jobVersion", jobVersion);
        out.put("clusterId", clusterId);
        out.put("hostname", hostname);
        out.put("subtaskIndex", subtaskIndex);
        out.put("parallelism", parallelism);
        out.put("payload", payload);
        return out;
    }

    private static Instant parseInstantOrNow(String value) {
        if (value == null || value.isBlank()) {
            return Instant.now();
        }
        try {
            return Instant.parse(value);
        } catch (Exception ignored) {
            return Instant.now();
        }
    }

    public static final class Builder {
        private String eventId;
        private String eventTime;
        private final AuditEventType eventType;
        private String status = "SUCCESS";
        private String jobName;
        private String jobVersion;
        private String clusterId;
        private String hostname;
        private Integer subtaskIndex;
        private Integer parallelism;
        private final Map<String, Object> payload = new LinkedHashMap<>();

        private Builder(AuditEventType eventType) {
            this.eventType = eventType;
        }

        public Builder eventId(String eventId) { this.eventId = eventId; return this; }
        public Builder eventTime(String eventTime) { this.eventTime = eventTime; return this; }
        public Builder status(String status) { this.status = status; return this; }
        public Builder jobName(String jobName) { this.jobName = jobName; return this; }
        public Builder jobVersion(String jobVersion) { this.jobVersion = jobVersion; return this; }
        public Builder clusterId(String clusterId) { this.clusterId = clusterId; return this; }
        public Builder hostname(String hostname) { this.hostname = hostname; return this; }
        public Builder subtaskIndex(Integer subtaskIndex) { this.subtaskIndex = subtaskIndex; return this; }
        public Builder parallelism(Integer parallelism) { this.parallelism = parallelism; return this; }

        public Builder put(String key, Object value) {
            if (key != null && !key.isBlank()) {
                this.payload.put(key, value);
            }
            return this;
        }

        public Builder putAll(Map<String, ?> values) {
            if (values != null) { values.forEach(this::put); }
            return this;
        }

        public CefAuditEvent build() { return new CefAuditEvent(this); }
    }
}
