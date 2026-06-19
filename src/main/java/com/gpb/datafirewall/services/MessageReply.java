package com.gpb.datafirewall.services;

import java.io.Serializable;
import java.util.Arrays;

public final class MessageReply implements Serializable {

    public static final int MQ_ID_LEN = 24;

    /**
     * IBM MQ correlationId.
     * Для ответа в IBM MQ сюда нужно положить messageId входящего сообщения.
     */
    public byte[] mqCorrelationId;

    /**
     * JMS/Artemis correlationId.
     * Для ответа в Artemis сюда нужно положить JMSMessageID входящего сообщения.
     */
    public String jmsCorrelationId;

    /**
     * Kafka correlationId.
     * Для ответа в Kafka сюда нужно положить kafkaMessageId входящего сообщения.
     */
    public String kafkaCorrelationId;

    /**
     * Тело ответа.
     */
    public String payload;

    public MessageReply() {
    }

    private MessageReply(byte[] mqCorrelationId, String jmsCorrelationId, String kafkaCorrelationId, String payload) {
        this.mqCorrelationId = mqCorrelationId;
        this.jmsCorrelationId = jmsCorrelationId;
        this.kafkaCorrelationId = kafkaCorrelationId;
        this.payload = payload;
    }

    public static MessageReply forMq(byte[] mqCorrelationId, String payload) {
        return new MessageReply(normalizeMqId(mqCorrelationId), null, null, payload);
    }

    public static MessageReply forJms(String jmsCorrelationId, String payload) {
        return new MessageReply(null, jmsCorrelationId, null, payload);
    }

    public static MessageReply forKafka(String kafkaCorrelationId, String payload) {
        return new MessageReply(null, null, kafkaCorrelationId, payload);
    }

    public boolean isMq() {
        return mqCorrelationId != null;
    }

    public boolean isJms() {
        return jmsCorrelationId != null && !jmsCorrelationId.isBlank();
    }

    public boolean isKafka() {
        return kafkaCorrelationId != null && !kafkaCorrelationId.isBlank();
    }

    public String correlationIdForLog() {
        if (isMq()) {
            return mqIdToHex(mqCorrelationId);
        }
        if (isJms()) {
            return jmsCorrelationId;
        }
        if (isKafka()) {
            return kafkaCorrelationId;
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
        return "MessageReply{" +
                "mqCorrelationId=" + (mqCorrelationId == null ? "null" : ("byte[" + mqCorrelationId.length + "]")) +
                ", jmsCorrelationId='" + jmsCorrelationId + '\'' +
                ", kafkaCorrelationId='" + kafkaCorrelationId + '\'' +
                ", payloadLen=" + (payload == null ? 0 : payload.length()) +
                '}';
    }
}