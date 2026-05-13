package com.gpb.datafirewall.dto;

import java.io.Serializable;

import com.fasterxml.jackson.databind.JsonNode;

public class AuditRecord implements Serializable {

    private String eventId;
    private JsonNode requestJson;
    private JsonNode shortAnswerJson;
    private JsonNode detailAnswerJson;
    private String actionDttm;
    private String status;

    public AuditRecord() {
    }

    public AuditRecord(
            String eventId,
            JsonNode requestJson,
            JsonNode shortAnswerJson,
            JsonNode detailAnswerJson,
            String actionDttm,
            String status
    ) {
        this.eventId = eventId;
        this.requestJson = requestJson;
        this.shortAnswerJson = shortAnswerJson;
        this.detailAnswerJson = detailAnswerJson;
        this.actionDttm = actionDttm;
        this.status = status != null ? status : "SUCCESS";
    }

    public String getEventId() {
        return eventId;
    }

    public void setEventId(String eventId) {
        this.eventId = eventId;
    }

    public JsonNode getRequestJson() {
        return requestJson;
    }

    public void setRequestJson(JsonNode requestJson) {
        this.requestJson = requestJson;
    }

    public JsonNode getShortAnswerJson() {
        return shortAnswerJson;
    }

    public void setShortAnswerJson(JsonNode shortAnswerJson) {
        this.shortAnswerJson = shortAnswerJson;
    }

    public JsonNode getDetailAnswerJson() {
        return detailAnswerJson;
    }

    public void setDetailAnswerJson(JsonNode detailAnswerJson) {
        this.detailAnswerJson = detailAnswerJson;
    }

    public String getActionDttm() {
        return actionDttm;
    }

    public void setActionDttm(String actionDttm) {
        this.actionDttm = actionDttm;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }
}