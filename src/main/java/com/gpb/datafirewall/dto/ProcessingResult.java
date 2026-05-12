package com.gpb.datafirewall.dto;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Locale;

public class ProcessingResult implements Serializable {

    private byte[] mqCorrelationId;
    private String jmsCorrelationId;
    private String eventId;

    private String shortJson;
    private String originalJson;
    private String detailJson;

    public ProcessingResult() {
    }

    public ProcessingResult(
            byte[] mqCorrelationId,
            String jmsCorrelationId,
            String eventId,
            String shortJson,
            String detailJson,
            String originalJson
    ) {
        this.mqCorrelationId = copyBytes(mqCorrelationId);
        this.jmsCorrelationId = jmsCorrelationId;
        this.eventId = eventId;
        this.shortJson = shortJson;
        this.detailJson = detailJson;
        this.originalJson = originalJson;
    }

    public static ProcessingResult forMq(
            byte[] mqCorrelationId,
            String shortJson,
            String detailJson,
            String originalJson
    ) {
        return new ProcessingResult(
                mqCorrelationId,
                null,
                mqIdToHex(mqCorrelationId),
                shortJson,
                detailJson,
                originalJson
        );
    }

    public static ProcessingResult forJms(
            String jmsCorrelationId,
            String shortJson,
            String detailJson,
            String originalJson
    ) {
        return new ProcessingResult(
                null,
                jmsCorrelationId,
                jmsCorrelationId == null || jmsCorrelationId.isBlank() ? "unknown" : jmsCorrelationId,
                shortJson,
                detailJson,
                originalJson
        );
    }

    public byte[] getMqCorrelationId() {
        return copyBytes(mqCorrelationId);
    }

    public void setMqCorrelationId(byte[] mqCorrelationId) {
        this.mqCorrelationId = copyBytes(mqCorrelationId);
    }

    public String getJmsCorrelationId() {
        return jmsCorrelationId;
    }

    public void setJmsCorrelationId(String jmsCorrelationId) {
        this.jmsCorrelationId = jmsCorrelationId;
    }

    public String getEventId() {
        if (eventId != null && !eventId.isBlank()) {
            return eventId;
        }

        if (mqCorrelationId != null && mqCorrelationId.length > 0) {
            return mqIdToHex(mqCorrelationId);
        }

        if (jmsCorrelationId != null && !jmsCorrelationId.isBlank()) {
            return jmsCorrelationId;
        }

        return "unknown";
    }

    public void setEventId(String eventId) {
        this.eventId = eventId;
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

    public String getOriginalJson() {
        return originalJson;
    }

    public void setOriginalJson(String originalJson) {
        this.originalJson = originalJson;
    }

    public boolean isMq() {
        return mqCorrelationId != null;
    }

    public boolean isJms() {
        return jmsCorrelationId != null && !jmsCorrelationId.isBlank();
    }

    public static String mqIdToHex(byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            return "unknown";
        }

        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            sb.append(String.format(Locale.ROOT, "%02X", b));
        }
        return sb.toString();
    }

    private static byte[] copyBytes(byte[] bytes) {
        return bytes == null ? null : Arrays.copyOf(bytes, bytes.length);
    }

    private static String preview(String value, int maxLen) {
        if (value == null) {
            return "null";
        }

        if (value.length() <= maxLen) {
            return value;
        }

        return value.substring(0, maxLen) + "...";
    }

    @Override
    public String toString() {
        return "ProcessingResult{" +
                "eventId='" + getEventId() + '\'' +
                ", mqCorrelationId=" + (mqCorrelationId == null ? "null" : ("byte[" + mqCorrelationId.length + "]")) +
                ", jmsCorrelationId='" + jmsCorrelationId + '\'' +
                ", shortJson='" + preview(shortJson, 300) + '\'' +
                ", originalJson='" + preview(originalJson, 300) + '\'' +
                ", detailJson='" + preview(detailJson, 300) + '\'' +
                '}';
    }
}