package ru.gpb.datafirewall.services;

import java.io.Serializable;
import java.util.Arrays;

public final class MessageRecord implements Serializable {

    public static final int MQ_ID_LEN = 24;

    /**
     * IBM MQ messageId.
     * Для IBM MQ это бинарный идентификатор длиной 24 байта.
     */
    public byte[] mqMessageId;

    /**
     * JMS/Artemis messageId.
     * Например: ID:...
     */
    public String jmsMessageId;

    /**
     * Тело сообщения.
     */
    public String payload;

    public MessageRecord() {
    }

    private MessageRecord(byte[] mqMessageId, String jmsMessageId, String payload) {
        this.mqMessageId = mqMessageId;
        this.jmsMessageId = jmsMessageId;
        this.payload = payload;
    }

    public static MessageRecord fromMq(byte[] mqMessageId, String payload) {
        return new MessageRecord(normalizeMqId(mqMessageId), null, payload);
    }

    public static MessageRecord fromJms(String jmsMessageId, String payload) {
        return new MessageRecord(null, jmsMessageId, payload);
    }

    public boolean isMq() {
        return mqMessageId != null;
    }

    public boolean isJms() {
        return jmsMessageId != null && !jmsMessageId.isBlank();
    }

    public String eventId() {
        if (isMq()) {
            return mqIdToHex(mqMessageId);
        }
        if (isJms()) {
            return jmsMessageId;
        }
        return "unknown";
    }

    private static byte[] normalizeMqId(byte[] id) {
        if (id == null) {
            return null;
        }

        if (id.length == MQ_ID_LEN) {
            return Arrays.copyOf(id, MQ_ID_LEN);
        }

        byte[] out = new byte[MQ_ID_LEN];
        System.arraycopy(id, 0, out, 0, Math.min(id.length, MQ_ID_LEN));
        return out;
    }

    public static String mqIdToHex(byte[] id) {
        if (id == null || id.length == 0) {
            return "unknown";
        }

        StringBuilder sb = new StringBuilder(id.length * 2);
        for (byte b : id) {
            sb.append(String.format("%02X", b));
        }
        return sb.toString();
    }

    @Override
    public String toString() {
        return "MessageRecord{" +
                "mqMessageId=" + (mqMessageId == null ? "null" : ("byte[" + mqMessageId.length + "]")) +
                ", jmsMessageId='" + jmsMessageId + '\'' +
                ", payloadLen=" + (payload == null ? 0 : payload.length()) +
                '}';
    }
}