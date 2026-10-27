package com.gpb.datafirewall.services;

import com.gpb.datafirewall.util.HexUtils;
import java.io.Serializable;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

public final class MessageRecord implements Serializable {
    public static final int MQ_ID_LEN = 24;

    public byte[] mqMessageId;
    public String jmsMessageId;
    public String payload;
    public Map<String, Object> headers;
    Long createdDttm;
    Long readedDttm;

    public MessageRecord() {}

    private MessageRecord(byte[] mqMessageId, String jmsMessageId, String payload,
                          Long createdDttm, Long readedDttm, Map<String, Object> headers) {
        this.mqMessageId = mqMessageId == null ? null : normalizeMqId(mqMessageId);
        this.jmsMessageId = jmsMessageId;
        this.payload = payload;
        this.createdDttm = createdDttm;
        this.readedDttm = readedDttm;
        this.headers = headers == null ? null : new LinkedHashMap<>(headers);
    }

    public static MessageRecord fromMq(byte[] mqMessageId, String payload,
                                       Long createdDttm, Long readedDttm) {
        return fromMq(mqMessageId, payload, createdDttm, readedDttm, null);
    }

    public static MessageRecord fromMq(byte[] mqMessageId, String payload,
                                       Long createdDttm, Long readedDttm, Map<String, Object> headers) {
        return new MessageRecord(mqMessageId, null, payload, createdDttm, readedDttm, headers);
    }

    public static MessageRecord fromJms(String jmsMessageId, String payload,
                                        Long createdDttm, Long readedDttm) {
        return new MessageRecord(null, jmsMessageId, payload, createdDttm, readedDttm, null);
    }

    public boolean isMq() { return mqMessageId != null; }
    public boolean isJms() { return jmsMessageId != null && !jmsMessageId.isBlank(); }

    public String eventId() {
        if (isMq()) return mqIdToHex(mqMessageId);
        if (isJms()) return jmsMessageId;
        return "unknown";
    }

    private static byte[] normalizeMqId(byte[] id) {
        if (id == null) return null;
        if (id.length == MQ_ID_LEN) return Arrays.copyOf(id, MQ_ID_LEN);
        byte[] out = new byte[MQ_ID_LEN];
        System.arraycopy(id, 0, out, 0, Math.min(id.length, MQ_ID_LEN));
        return out;
    }

    public static String mqIdToHex(byte[] id) {
        if (id == null || id.length == 0) return "unknown";
        return HexUtils.toUpperHex(id);
    }

    @Override
    public String toString() {
        return "MessageRecord{" +
                "mqMessageId=" + (mqMessageId == null
                ? "null"
                : "byte[" + mqMessageId.length + "]") +
                ", jmsMessageId='" + jmsMessageId + '\'' +
                ", payloadLen=" + (payload == null ? 0 : payload.length()) +
                '}';
    }
}
