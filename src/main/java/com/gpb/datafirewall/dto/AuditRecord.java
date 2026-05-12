package com.gpb.datafirewall.dto;

import java.io.Serializable;

public class AuditRecord implements Serializable {

    private String eventId;
    private String requestJson;
    private String shortAnswerJson;
    private String detailAnswerJson;
    private String actionDttm;
    private String status;

    public AuditRecord() {
    }

    public AuditRecord(
            String eventId,
            String requestJson,
            String shortAnswerJson,
            String detailAnswerJson,
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