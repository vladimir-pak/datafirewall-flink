package ru.gpbapp.datafirewallflink.dto;

import java.io.Serializable;
import java.time.Instant;

public class AuditRecord implements Serializable {

    private String eventId;           // correlation id в строковом виде или UUID
    private String requestJson;
    private String shortAnswerJson;
    private String detailAnswerJson;  // можно оставить null, если не нужно
    private Instant timestamp;
    private String status;            // SUCCESS / FAILED / PARTIAL и т.д.

    public AuditRecord() {
    }

    public AuditRecord(String eventId, String requestJson, String shortAnswerJson, String detailAnswerJson, String status) {
        this.eventId = eventId;
        this.requestJson = requestJson;
        this.shortAnswerJson = shortAnswerJson;
        this.detailAnswerJson = detailAnswerJson;
        this.timestamp = Instant.now();
        this.status = status != null ? status : "SUCCESS";
    }

    // getters + setters
    public String getEventId() { return eventId; }
    public void setEventId(String eventId) { this.eventId = eventId; }

    public String getRequestJson() { return requestJson; }
    public void setRequestJson(String requestJson) { this.requestJson = requestJson; }

    public String getShortAnswerJson() { return shortAnswerJson; }
    public void setShortAnswerJson(String shortAnswerJson) { this.shortAnswerJson = shortAnswerJson; }

    public String getDetailAnswerJson() { return detailAnswerJson; }
    public void setDetailAnswerJson(String detailAnswerJson) { this.detailAnswerJson = detailAnswerJson; }

    public Instant getTimestamp() { return timestamp; }
    public void setTimestamp(Instant timestamp) { this.timestamp = timestamp; }

    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }
}