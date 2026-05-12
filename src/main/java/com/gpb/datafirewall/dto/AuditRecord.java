package com.gpb.datafirewall.dto;

import java.io.Serializable;
import java.time.Instant;

public class AuditRecord implements Serializable {

    private String eventId;
    private String requestJson;
    private String shortAnswerJson;
    private String detailAnswerJson;
    private String timestamp;
    private String status;

    public AuditRecord() {
    }

    public AuditRecord(
            String eventId,
            String requestJson,
            String shortAnswerJson,
            String detailAnswerJson,
            String status
    ) {
        this.eventId = eventId;
        this.requestJson = requestJson;
        this.shortAnswerJson = shortAnswerJson;
        this.detailAnswerJson = detailAnswerJson;
        this.timestamp = Instant.now().toString();
        this.status = status != null ? status : "SUCCESS";
    }

    public String getEventId() {
        return eventId;
    }

    public void setEventId(String eventId) {
        this.eventId = eventId;
    }

    public String getRequestJson() {
        return requestJson;
    }

    public void setRequestJson(String requestJson) {
        this.requestJson = requestJson;
    }

    public String getShortAnswerJson() {
        return shortAnswerJson;
    }

    public void setShortAnswerJson(String shortAnswerJson) {
        this.shortAnswerJson = shortAnswerJson;
    }

    public String getDetailAnswerJson() {
        return detailAnswerJson;
    }

    public void setDetailAnswerJson(String detailAnswerJson) {
        this.detailAnswerJson = detailAnswerJson;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }
}