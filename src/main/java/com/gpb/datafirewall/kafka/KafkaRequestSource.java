
package com.gpb.datafirewall.kafka;

import com.gpb.datafirewall.services.MessageRecord;
import com.gpb.datafirewall.audit.AuditConfig;
import com.gpb.datafirewall.audit.AuditEventType;
import com.gpb.datafirewall.audit.CefAuditEvent;
import com.gpb.datafirewall.audit.CefAuditPublisher;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.Properties;

/**
 * Kafka source для чтения запросов из Kafka topic.
 * Аналог MqSource и ArtemisSource для единообразия архитектуры.
 */
public class KafkaRequestSource extends RichParallelSourceFunction<MessageRecord> {

    private static final Logger log = LoggerFactory.getLogger(KafkaRequestSource.class);

    private final String bootstrapServers;
    private final String topic;
    private final String groupId;
    private final Properties consumerProps;
    private final long pollTimeoutMs;
    private final boolean logPayloads;
    private final int logPreviewLen;
    private final AuditConfig auditConfig;

    private transient volatile boolean running;
    private transient KafkaConsumer<String, String> consumer;
    private transient CefAuditPublisher auditPublisher;

    public KafkaRequestSource(
            String bootstrapServers,
            String topic,
            String groupId,
            Properties consumerProps,
            long pollTimeoutMs
    ) {
        this(bootstrapServers, topic, groupId, consumerProps, pollTimeoutMs, false, 600, null);
    }

    public KafkaRequestSource(
            String bootstrapServers,
            String topic,
            String groupId,
            Properties consumerProps,
            long pollTimeoutMs,
            boolean logPayloads,
            int logPreviewLen,
            AuditConfig auditConfig
    ) {
        this.bootstrapServers = bootstrapServers;
        this.topic = topic;
        this.groupId = groupId;
        this.consumerProps = consumerProps;
        this.pollTimeoutMs = pollTimeoutMs;
        this.logPayloads = logPayloads;
        this.logPreviewLen = logPreviewLen;
        this.auditConfig = auditConfig;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        running = true;

        try {
            auditPublisher = new CefAuditPublisher(auditConfig);
            publishAudit(AuditEventType.KAFKA_SOURCE_CONNECTING, "SUCCESS", null);

            log.info(
                    "KafkaSource connecting: subtask={}, bootstrap={}, topic={}, groupId={}",
                    getRuntimeContext().getIndexOfThisSubtask(),
                    bootstrapServers,
                    topic,
                    groupId
            );

            consumer = new KafkaConsumer<>(consumerProps);
            consumer.subscribe(Collections.singletonList(topic));

            log.info(
                    "KafkaSource opened: subtask={}, topic={}, pollTimeoutMs={}, log.payloads={}",
                    getRuntimeContext().getIndexOfThisSubtask(),
                    topic,
                    pollTimeoutMs,
                    logPayloads
            );
            publishAudit(AuditEventType.KAFKA_SOURCE_CONNECTED, "SUCCESS", null);
        } catch (Exception e) {
            publishAudit(AuditEventType.KAFKA_SOURCE_CONNECTION_FAILED, "FAILED", e);
            close();
            throw new RuntimeException(
                    "Failed to open KafkaSource. bootstrap=" + bootstrapServers +
                            ", topic=" + topic + ", groupId=" + groupId,
                    e
            );
        }
    }

    @Override
    public void run(SourceContext<MessageRecord> ctx) throws Exception {
        while (running) {
            try {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(pollTimeoutMs));

                for (ConsumerRecord<String, String> record : records) {
                    Long readedDttm = Instant.now().toEpochMilli();
                    Long createdDttm = record.timestamp() > 0 ? record.timestamp() : null;

                    String payload = record.value();
                    String kafkaMessageId = buildKafkaMessageId(record);

                    if (logPayloads) {
                        log.info("KAFKA READ topic={} partition={} offset={} key={} BODY:\n{}",
                                record.topic(),
                                record.partition(),
                                record.offset(),
                                record.key(),
                                preview(payload, logPreviewLen));
                    }

                    synchronized (ctx.getCheckpointLock()) {
                        ctx.collect(MessageRecord.fromKafka(
                                kafkaMessageId,
                                payload,
                                createdDttm,
                                readedDttm
                        ));
                    }
                }
            } catch (Exception e) {
                if (!running) {
                    log.info("KafkaSource stopped during shutdown.");
                    return;
                }

                log.error("Kafka poll failed", e);
                throw e;
            }
        }
    }

    @Override
    public void cancel() {
        running = false;
        close();
    }

    @Override
    public void close() {
        publishAudit(AuditEventType.KAFKA_SOURCE_DISCONNECTED, "SUCCESS", null);

        try {
            if (consumer != null) {
                consumer.close();
            }
        } catch (Exception e) {
            log.warn("Failed to close KafkaConsumer", e);
        } finally {
            consumer = null;
        }

        try {
            if (auditPublisher != null) {
                auditPublisher.close();
            }
        } catch (Exception e) {
            log.warn("Failed to close CefAuditPublisher", e);
        } finally {
            auditPublisher = null;
        }
    }

    private String buildKafkaMessageId(ConsumerRecord<String, String> record) {
        // Формируем уникальный идентификатор сообщения
        // Формат: kafka-{topic}-{partition}-{offset}
        return String.format(
                "kafka-%s-%d-%d",
                record.topic(),
                record.partition(),
                record.offset()
        );
    }

    private static String preview(String s, int max) {
        if (s == null) {
            return "null";
        }
        if (s.length() <= max) {
            return s;
        }
        return s.substring(0, max) + "...(+" + (s.length() - max) + " chars)";
    }

    private void publishAudit(AuditEventType type, String status, Exception error) {
        if (auditPublisher == null || auditConfig == null || !auditConfig.enabled()) {
            return;
        }
        CefAuditEvent.Builder builder = auditConfig.enrich(CefAuditEvent.builder(type))
                .status(status)
                .subtaskIndex(getRuntimeContext().getIndexOfThisSubtask())
                .parallelism(getRuntimeContext().getNumberOfParallelSubtasks())
                .put("component", "KafkaSource")
                .put("bootstrap", bootstrapServers)
                .put("topic", topic)
                .put("groupId", groupId)
                .put("pollTimeoutMs", pollTimeoutMs);
        if (error != null) {
            builder.put("errorClass", error.getClass().getName())
                    .put("errorMessage", error.getMessage());
        }
        auditPublisher.publish(builder.build());
    }
}
