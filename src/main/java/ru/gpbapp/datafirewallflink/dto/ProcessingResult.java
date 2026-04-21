package ru.gpbapp.datafirewallflink.dto;

import java.io.Serializable;

public class ProcessingResult implements Serializable {

    private byte[] correlId;
    private String shortJson;
    private String detailJson;

    public ProcessingResult() {
    }

    public ProcessingResult(byte[] correlId, String shortJson, String detailJson) {
        this.correlId = correlId;
        this.shortJson = shortJson;
        this.detailJson = detailJson;
    }

    public byte[] getCorrelId() {
        return correlId;
    }

    public void setCorrelId(byte[] correlId) {
        this.correlId = correlId;
    }

    public String getShortJson() {
        return shortJson;
    }

    public void setShortJson(String shortJson) {
        this.shortJson = shortJson;
    }

    public String getDetailJson() {
        return detailJson;
    }

    public void setDetailJson(String detailJson) {
        this.detailJson = detailJson;
    }

    @Override
    public String toString() {
        return "ProcessingResult{" +
                "correlId=" + (correlId == null ? "null" : ("byte[" + correlId.length + "]")) +
                ", shortJson='" + shortJson + '\'' +
                ", detailJson='" + detailJson + '\'' +
                '}';
    }
}