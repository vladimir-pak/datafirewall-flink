package com.gpb.datafirewall.dto;

import com.gpb.datafirewall.util.HexUtils;
import java.io.Serializable;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

public class ProcessingResult implements Serializable {
    private byte[] mqCorrelationId;
    private String jmsCorrelationId;
    private String eventId;
    private String shortJson;
    private String originalJson;
    private String detailJson;
    private Map<String, Object> headers;
    private Long createdDttm;
    private Long readedDttm;

    public ProcessingResult() {}

    public ProcessingResult(byte[] mqCorrelationId, String jmsCorrelationId, String eventId,
                            String shortJson, String detailJson, String originalJson) {
        this.mqCorrelationId = copyBytes(mqCorrelationId);
        this.jmsCorrelationId = jmsCorrelationId;
        this.eventId = eventId;
        this.shortJson = shortJson;
        this.detailJson = detailJson;
        this.originalJson = originalJson;
    }

    public static ProcessingResult forMq(byte[] mqCorrelationId, String shortJson,
                                         String detailJson, String originalJson) {
        return new ProcessingResult(mqCorrelationId, null, mqIdToHex(mqCorrelationId),
                shortJson, detailJson, originalJson);
    }

    public static ProcessingResult forJms(String jmsCorrelationId, String shortJson,
                                          String detailJson, String originalJson) {
        return new ProcessingResult(null, jmsCorrelationId,
                jmsCorrelationId == null || jmsCorrelationId.isBlank() ? "unknown" : jmsCorrelationId,
                shortJson, detailJson, originalJson);
    }

    public byte[] getMqCorrelationId() { return copyBytes(mqCorrelationId); }
    public void setMqCorrelationId(byte[] value) { this.mqCorrelationId = copyBytes(value); }
    public String getJmsCorrelationId() { return jmsCorrelationId; }
    public void setJmsCorrelationId(String value) { this.jmsCorrelationId = value; }
    public String getEventId() {
        if (eventId != null && !eventId.isBlank()) return eventId;
        if (mqCorrelationId != null && mqCorrelationId.length > 0) return mqIdToHex(mqCorrelationId);
        if (jmsCorrelationId != null && !jmsCorrelationId.isBlank()) return jmsCorrelationId;
        return "unknown";
    }
    public void setEventId(String value) { this.eventId = value; }
    public String getShortJson() { return shortJson; }
    public void setShortJson(String value) { this.shortJson = value; }
    public String getDetailJson() { return detailJson; }
    public void setDetailJson(String value) { this.detailJson = value; }
    public String getOriginalJson() { return originalJson; }
    public void setOriginalJson(String value) { this.originalJson = value; }
    public Map<String, Object> getHeaders() {
        return headers == null ? null : new LinkedHashMap<>(headers);
    }
    public void setHeaders(Map<String, Object> value) {
        this.headers = value == null ? null : new LinkedHashMap<>(value);
    }
    public Long getCreatedDttm() { return createdDttm; }
    public void setCreatedDttm(Long value) { this.createdDttm = value; }
    public Long getReadedDttm() { return readedDttm; }
    public void setReadedDttm(Long value) { this.readedDttm = value; }
    public boolean isMq() { return mqCorrelationId != null; }
    public boolean isJms() { return jmsCorrelationId != null && !jmsCorrelationId.isBlank(); }

    public static String mqIdToHex(byte[] bytes) {
        if (bytes == null || bytes.length == 0) return "unknown";
        return HexUtils.toUpperHex(bytes);
    }

    private static byte[] copyBytes(byte[] bytes) {
        return bytes == null ? null : Arrays.copyOf(bytes, bytes.length);
    }

    private static String preview(String value, int maxLen) {
        if (value == null) return "null";
        if (value.length() <= maxLen) return value;
        return value.substring(0, maxLen) + "...";
    }

    @Override
    public String toString() {
        return "ProcessingResult{" +
                "eventId='" + getEventId() + '\'' +
                ", mqCorrelationId=" + (mqCorrelationId == null
                ? "null"
                : "byte[" + mqCorrelationId.length + "]") +
                ", jmsCorrelationId='" + jmsCorrelationId + '\'' +
                ", shortJson='" + preview(shortJson, 300) + '\'' +
                ", originalJson='" + preview(originalJson, 300) + '\'' +
                ", detailJson='" + preview(detailJson, 300) + '\'' +
                '}';
    }
}
