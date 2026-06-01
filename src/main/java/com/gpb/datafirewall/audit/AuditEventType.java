package com.gpb.datafirewall.audit;

import java.util.Locale;

public enum AuditEventType {
    JOB_SUBMITTED,
    JOB_STARTED,
    JOB_STOPPED,
    JOB_FAILED,
    JOB_PROPERTIES_LOADED,
    JOB_PROPERTIES_CHANGED,

    KAFKA_CONNECTION_CHECK_OK,
    KAFKA_CONNECTION_CHECK_FAILED,
    KAFKA_AUDIT_PRODUCER_STARTED,
    KAFKA_AUDIT_PRODUCER_STOPPED,
    KAFKA_RULES_SOURCE_CONFIGURED,

    ARTEMIS_SOURCE_CONNECTING,
    ARTEMIS_SOURCE_CONNECTED,
    ARTEMIS_SOURCE_CONNECTION_FAILED,
    ARTEMIS_SOURCE_DISCONNECTED,
    ARTEMIS_SINK_CONNECTING,
    ARTEMIS_SINK_CONNECTED,
    ARTEMIS_SINK_CONNECTION_FAILED,
    ARTEMIS_SINK_DISCONNECTED,

    IBM_MQ_SOURCE_CONNECTING,
    IBM_MQ_SOURCE_CONNECTED,
    IBM_MQ_SOURCE_CONNECTION_FAILED,
    IBM_MQ_SOURCE_DISCONNECTED,
    IBM_MQ_SINK_CONNECTING,
    IBM_MQ_SINK_CONNECTED,
    IBM_MQ_SINK_CONNECTION_FAILED,
    IBM_MQ_SINK_DISCONNECTED;

    public String eventClass() {
        if (name().startsWith("JOB_PROPERTIES")) {
            return "JOB_PROPERTIES";
        }
        if (name().startsWith("JOB_")) {
            return "JOB";
        }
        if (name().startsWith("KAFKA_")) {
            return "KAFKA";
        }
        if (name().startsWith("ARTEMIS_SOURCE")) {
            return "ARTEMIS_SOURCE";
        }
        if (name().startsWith("ARTEMIS_SINK")) {
            return "ARTEMIS_SINK";
        }
        if (name().startsWith("IBM_MQ_SOURCE")) {
            return "IBM_MQ_SOURCE";
        }
        if (name().startsWith("IBM_MQ_SINK")) {
            return "IBM_MQ_SINK";
        }
        return name();
    }

    public String eventName() {
        String[] parts = name().toLowerCase(Locale.ROOT).split("_");
        StringBuilder sb = new StringBuilder();
        for (String part : parts) {
            if (part.isBlank()) {
                continue;
            }
            if (!sb.isEmpty()) {
                sb.append(' ');
            }
            if ("mq".equals(part)) {
                sb.append("MQ");
            } else if ("ibm".equals(part)) {
                sb.append("IBM");
            } else if ("kafka".equals(part)) {
                sb.append("Kafka");
            } else if ("artemis".equals(part)) {
                sb.append("Artemis");
            } else if ("job".equals(part)) {
                sb.append("Job");
            } else {
                sb.append(Character.toUpperCase(part.charAt(0))).append(part.substring(1));
            }
        }
        return sb.toString();
    }
}
