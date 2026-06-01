package com.gpb.datafirewall.audit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

public final class CefAuditPublisher implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(CefAuditPublisher.class);

    private static final AtomicLong EXTERNAL_ID_COUNTER = new AtomicLong(0);

    private final AuditConfig config;
    private KafkaProducer<String, String> producer;

    public CefAuditPublisher(AuditConfig config) {
        this.config = config;
    }

    public synchronized void publish(CefAuditEvent event) {
        if (config == null || !config.enabled() || event == null) {
            return;
        }

        try {
            String cef = CefAuditFormatter.format(config, event, EXTERNAL_ID_COUNTER.incrementAndGet());

            if (config.fileEnabled()) {
                appendToFile(cef);
            }

            if (config.kafkaEnabled()) {
                sendToKafka(event.eventId(), cef);
            }
        } catch (Exception e) {
            log.warn("CEF audit publish failed: eventId={}", event.eventId(), e);
            if (config.failOnError()) {
                throw new RuntimeException("CEF audit publish failed", e);
            }
        }
    }

    public void publish(AuditEventType type, String status, java.util.Map<String, ?> payload) {
        CefAuditEvent.Builder builder = config.enrich(CefAuditEvent.builder(type))
                .status(status);
        if (payload != null) {
            builder.putAll(payload);
        }
        publish(builder.build());
    }

    private void appendToFile(String cef) throws Exception {
        Path path = Path.of(config.cefFilePath()).toAbsolutePath().normalize();
        Path parent = path.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        Files.writeString(
                path,
                cef + System.lineSeparator(),
                StandardCharsets.UTF_8,
                StandardOpenOption.CREATE,
                StandardOpenOption.APPEND
        );
    }

    private void sendToKafka(String key, String cef) {
        KafkaProducer<String, String> p = getOrCreateProducer();
        p.send(new ProducerRecord<>(config.kafkaTopic(), key, cef), (metadata, exception) -> {
            if (exception != null) {
                log.warn("Failed to send CEF audit event to Kafka topic={}", config.kafkaTopic(), exception);
            }
        });
    }

    private KafkaProducer<String, String> getOrCreateProducer() {
        if (producer == null) {
            producer = new KafkaProducer<>(config.toKafkaProducerProperties());
            CefAuditEvent event = config.enrich(CefAuditEvent.builder(AuditEventType.KAFKA_AUDIT_PRODUCER_STARTED))
                    .put("bootstrap", config.kafkaBootstrap())
                    .put("topic", config.kafkaTopic())
                    .put("protocol", config.kafkaSecurityProtocol())
                    .put("connectionUser", config.kafkaUser())
                    .put("cefFilePath", Path.of(config.cefFilePath()).toAbsolutePath().normalize().toString())
                    .build();
            try {
                String cef = CefAuditFormatter.format(config, event, EXTERNAL_ID_COUNTER.incrementAndGet());
                if (config.fileEnabled()) {
                    appendToFile(cef);
                }
                producer.send(new ProducerRecord<>(config.kafkaTopic(), event.eventId(), cef));
            } catch (Exception e) {
                log.warn("Failed to publish audit producer started event", e);
            }
        }
        return producer;
    }

    @Override
    public synchronized void close() {
        if (producer == null) {
            return;
        }
        try {
            CefAuditEvent event = config.enrich(CefAuditEvent.builder(AuditEventType.KAFKA_AUDIT_PRODUCER_STOPPED))
                    .put("topic", config.kafkaTopic())
                    .put("protocol", config.kafkaSecurityProtocol())
                    .put("connectionUser", config.kafkaUser())
                    .build();
            String cef = CefAuditFormatter.format(config, event, EXTERNAL_ID_COUNTER.incrementAndGet());
            if (config.fileEnabled()) {
                appendToFile(cef);
            }
            producer.send(new ProducerRecord<>(config.kafkaTopic(), event.eventId(), cef));
            producer.flush();
        } catch (Exception e) {
            log.warn("Failed to publish audit producer stopped event", e);
        } finally {
            try {
                producer.close(Duration.ofSeconds(5));
            } catch (Exception e) {
                log.warn("Failed to close CEF audit Kafka producer", e);
            } finally {
                producer = null;
            }
        }
    }
}
