package com.gpb.datafirewall.transaction.kafka;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Optional, lossy Kafka processing audit. Not part of MQ/JMS transactions or Flink checkpoints.
 * The Kafka client reconnects on its own; a missing broker/buffer drops an audit event, never
 * fails a Flink task. The Kafka producer's buffer and send wait are explicitly bounded.
 */
@SuppressWarnings("deprecation")
public final class BestEffortKafkaAuditSink extends RichSinkFunction<String> {
    private static final Logger log = LoggerFactory.getLogger(BestEffortKafkaAuditSink.class);
    private final String bootstrap;
    private final String topic;
    private final Properties baseProperties;
    private final long maxBlockMs;
    private transient KafkaProducer<String, String> producer;
    private transient long nextProducerAttemptNanos;
    // A transport error pauses submissions for five seconds to prevent audit backpressure.
    private transient volatile long nextSendAttemptNanos;
    private transient AtomicLong dropped;
    private transient AtomicLong enqueued;

    public BestEffortKafkaAuditSink(String bootstrap, String topic,
            Properties baseProperties, long maxBlockMs) {
        if (maxBlockMs < 1 || maxBlockMs > 1000) {
            throw new IllegalArgumentException("audit.kafka.best-effort.max-block.ms must be 1..1000");
        }
        this.bootstrap = bootstrap;
        this.topic = topic;
        this.baseProperties = new Properties();
        this.baseProperties.putAll(baseProperties);
        this.maxBlockMs = maxBlockMs;
    }

    @Override
    public void open(Configuration parameters) {
        dropped = new AtomicLong();
        enqueued = new AtomicLong();
        getRuntimeContext().getMetricGroup().gauge("auditKafkaDropped", dropped::get);
        getRuntimeContext().getMetricGroup().gauge("auditKafkaEnqueued", enqueued::get);
        // Do not connect here: Kafka downtime during startup must not fail the task.
    }

    @Override
    public void invoke(String value, Context context) {
        if (value == null) return;
        long now = System.nanoTime();
        if (nextSendAttemptNanos != 0 && now - nextSendAttemptNanos < 0) {
            dropped.incrementAndGet();
            return;
        }
        if (!ensureProducer()) {
            dropped.incrementAndGet();
            return;
        }
        try {
            producer.send(new ProducerRecord<>(topic, value), (metadata, error) -> {
                if (error != null) {
                    nextSendAttemptNanos = System.nanoTime() + Duration.ofSeconds(5).toNanos();
                    dropped.incrementAndGet();
                    log.warn("Best-effort Kafka audit dropped after asynchronous failure: topic={} cause={}",
                            topic, error.toString());
                }
            });
            enqueued.incrementAndGet();
        } catch (Exception error) {
            nextSendAttemptNanos = System.nanoTime() + Duration.ofSeconds(5).toNanos();
            dropped.incrementAndGet();
            log.warn("Best-effort Kafka audit dropped: topic={} cause={}", topic, error.toString());
            // The producer itself handles broker reconnects. Don't recreate it for every event.
        }
    }

    private boolean ensureProducer() {
        if (producer != null) return true;
        long now = System.nanoTime();
        if (nextProducerAttemptNanos != 0 && now - nextProducerAttemptNanos < 0) return false;
        try {
            Properties properties = new Properties();
            properties.putAll(baseProperties);
            properties.setProperty("bootstrap.servers", bootstrap);
            properties.setProperty("key.serializer", StringSerializer.class.getName());
            properties.setProperty("value.serializer", StringSerializer.class.getName());
            properties.setProperty("max.block.ms", String.valueOf(maxBlockMs));
            properties.setProperty("buffer.memory", "4194304");
            properties.setProperty("delivery.timeout.ms", "5000");
            properties.setProperty("request.timeout.ms", "3000");
            properties.setProperty("linger.ms", "0");
            properties.setProperty("retries", "0");
            properties.setProperty("enable.idempotence", "false");
            producer = new KafkaProducer<>(properties);
            nextProducerAttemptNanos = 0L;
            return true;
        } catch (Exception error) {
            nextProducerAttemptNanos = System.nanoTime() + Duration.ofSeconds(5).toNanos();
            log.warn("Best-effort Kafka producer unavailable; next construction in 5s: {}",
                    error.toString());
            return false;
        }
    }

    @Override
    public void close() {
        KafkaProducer<String, String> current = producer;
        producer = null;
        if (current != null) {
            try {
                // Never flush indefinitely: pending Kafka audit messages are disposable.
                current.close(Duration.ofMillis(200));
            } catch (Exception error) {
                log.warn("Best-effort Kafka producer close failed: {}", error.toString());
            }
        }
    }
}
