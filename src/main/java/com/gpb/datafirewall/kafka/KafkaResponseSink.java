package com.gpb.datafirewall.kafka;

import com.gpb.datafirewall.services.MessageReply;
import com.gpb.datafirewall.audit.AuditConfig;
import com.gpb.datafirewall.audit.AuditEventType;
import com.gpb.datafirewall.audit.CefAuditEvent;
import com.gpb.datafirewall.audit.CefAuditPublisher;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.Future;

/**
 * Kafka sink для отправки ответов в Kafka topic.
 * Аналог MqSink и ArtemisSink для единообразия архитектуры.
 */
public class KafkaResponseSink extends RichSinkFunction<MessageReply> {

    private static final Logger log = LoggerFactory.getLogger(KafkaResponseSink.class);

    private final String bootstrapServers;
    private final String topic;
    private final Properties producerProps;
    private final AuditConfig auditConfig;

    private transient KafkaProducer<String, String> producer;
    private transient CefAuditPublisher auditPublisher;

    public KafkaResponseSink(
            String bootstrapServers,
            String topic,
            Properties producerProps
    ) {
        this(bootstrapServers, topic, producerProps, null);
    }

    public KafkaResponseSink(
            String bootstrapServers,
            String topic,
            Properties producerProps,
            AuditConfig auditConfig
    ) {
        this.bootstrapServers = bootstrapServers;
        this.topic = topic;
        this.producerProps = producerProps;
        this.auditConfig = auditConfig;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        try {
            auditPublisher = new CefAuditPublisher(auditConfig);
            publishAudit(AuditEventType.KAFKA_SINK_CONNECTING, "SUCCESS", null);

            log.info(
                    "KafkaSink connecting: subtask={}, bootstrap={}, topic={}",
                    getRuntimeContext().getIndexOfThisSubtask(),
                    bootstrapServers,
                    topic
            );

            producer = new KafkaProducer<>(producerProps);

            log.info(
                    "KafkaSink opened: subtask={}, topic={}",
                    getRuntimeContext().getIndexOfThisSubtask(),
                    topic
            );
            publishAudit(AuditEventType.KAFKA_SINK_CONNECTED, "SUCCESS", null);
        } catch (Exception e) {
            publishAudit(AuditEventType.KAFKA_SINK_CONNECTION_FAILED, "FAILED", e);
            close();
            throw new RuntimeException(
                    "Failed to open KafkaSink. bootstrap=" + bootstrapServers +
                            ", topic=" + topic,
                    e
            );
        }
    }

    @Override
    public void invoke(MessageReply value, Context context) throws Exception {
        if (value == null) {
            return;
        }

        if (value.kafkaCorrelationId == null || value.kafkaCorrelationId.isBlank()) {
            throw new IllegalArgumentException("Kafka reply requires kafkaCorrelationId");
        }

        String payload = value.payload == null ? "" : value.payload;

        // Используем correlationId как key для Kafka сообщения
        ProducerRecord<String, String> record = new ProducerRecord<>(
                topic,
                value.kafkaCorrelationId,
                payload
        );

        Future<RecordMetadata> future = producer.send(record);

        // Ждем подтверждения отправки (синхронно)
        RecordMetadata metadata = future.get();

        log.debug(
                "Kafka message sent: topic={} partition={} offset={} key={}",
                metadata.topic(),
                metadata.partition(),
                metadata.offset(),
                value.kafkaCorrelationId
        );
    }

    @Override
    public void close() {
        publishAudit(AuditEventType.KAFKA_SINK_DISCONNECTED, "SUCCESS", null);

        try {
            if (producer != null) {
                producer.flush();
                producer.close();
            }
        } catch (Exception e) {
            log.warn("Failed to close KafkaProducer", e);
        } finally {
            producer = null;
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

    private void publishAudit(AuditEventType type, String status, Exception error) {
        if (auditPublisher == null || auditConfig == null || !auditConfig.enabled()) {
            return;
        }
        CefAuditEvent.Builder builder = auditConfig.enrich(CefAuditEvent.builder(type))
                .status(status)
                .subtaskIndex(getRuntimeContext().getIndexOfThisSubtask())
                .parallelism(getRuntimeContext().getNumberOfParallelSubtasks())
                .put("component", "KafkaSink")
                .put("bootstrap", bootstrapServers)
                .put("topic", topic);
        if (error != null) {
            builder.put("errorClass", error.getClass().getName())
                    .put("errorMessage", error.getMessage());
        }
        auditPublisher.publish(builder.build());
    }
}
