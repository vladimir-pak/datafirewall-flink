package com.gpb.datafirewall.source;

import com.gpb.datafirewall.bundle.CacheBundleState;
import com.gpb.datafirewall.bundle.CacheBundleEnvelope;
import com.gpb.datafirewall.bundle.CacheBundleStateSerializer;
import com.gpb.datafirewall.config.IgniteRulesApiClient;
import com.gpb.datafirewall.dto.IgniteBytecodeSource;
import com.gpb.datafirewall.ignite.BytecodeSource;
import com.gpb.datafirewall.ignite.impl.IgniteClientFacadeImpl;
import com.gpb.datafirewall.kafka.CacheUpdateEvent;
import com.gpb.datafirewall.kafka.CacheUpdateEventDeserializationSchema;
import com.gpb.datafirewall.services.CacheBootstrapService;
import com.gpb.datafirewall.services.IncompatibleCacheBundleException;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.clients.consumer.OffsetOutOfRangeException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * One-partition notification source; independently versioned rules and politics.
 * Checkpoint contains the complete active bundle and the next Kafka offset.
 * First startup fixes Kafka position BEFORE independently querying /latest.
 */
@SuppressWarnings("deprecation")
public final class CacheSnapshotSource extends RichSourceFunction<CacheBundleEnvelope>
        implements CheckpointedFunction {

    private static final Logger log = LoggerFactory.getLogger(CacheSnapshotSource.class);
    private static final long serialVersionUID = 2L;
    private static final int STATE_MAGIC = 0x43535332; // CSS2
    private static final int MAX_BUNDLE_BYTES = 256 * 1024 * 1024;

    private final ParameterTool pt;
    private final String jwt;
    private final Properties kafkaProperties;
    private final String kafkaBootstrap;
    private final String kafkaTopic;
    private final String kafkaGroup;
    private volatile boolean running = true;

    private transient volatile KafkaConsumer<byte[], byte[]> consumer;
    private transient ListState<byte[]> state;
    private transient CacheBundleState active;
    // Independent routing state; handler notifications never rebuild or reserialize caches.
    private transient String selectedHandler;
    // A handler-only event received after a staged cache update takes precedence
    // over that staged bundle's embedded handler, without rewriting its bytecode.
    private transient boolean handlerOverridePending;
    private transient byte[] activeEncoded;
    // A valid but temporarily incompatible combination; not sent to processing subtasks.
    private transient CacheBundleState staged;
    private transient byte[] stagedEncoded;
    private transient long nextOffset = -1L;
    private transient boolean restored;
    private transient CacheBootstrapService bootstrap;
    private transient IgniteClientFacadeImpl thinClient;

    public CacheSnapshotSource(ParameterTool pt, String jwt, Properties kafkaProperties,
                               String kafkaBootstrap, String kafkaTopic, String kafkaGroup) {
        this.pt = pt;
        this.jwt = jwt;
        this.kafkaProperties = kafkaProperties;
        this.kafkaBootstrap = kafkaBootstrap;
        this.kafkaTopic = kafkaTopic;
        this.kafkaGroup = kafkaGroup;
    }

    private CacheBootstrapService loader() {
        if (bootstrap != null) return bootstrap;
        IgniteRulesApiClient api = new IgniteRulesApiClient(
                pt.get("ignite.apiUrl", "http://127.0.0.1:8080"),
                pt.get("ignite.api.tls.ca.pem", null), jwt);
        String backend = pt.get("rules.loader", "http");
        if ("http".equalsIgnoreCase(backend)) {
            bootstrap = CacheBootstrapService.http(api);
        } else if ("thin".equalsIgnoreCase(backend)) {
            thinClient = new IgniteClientFacadeImpl(pt.get("ignite.host", "127.0.0.1"),
                    pt.getInt("ignite.port", 10800));
            BytecodeSource source = new IgniteBytecodeSource(thinClient);
            bootstrap = new CacheBootstrapService(api, source);
        } else {
            throw new IllegalArgumentException("Unsupported rules.loader=" + backend);
        }
        return bootstrap;
    }

    @Override public void run(SourceContext<CacheBundleEnvelope> ctx) throws Exception {
        if (restored && active != null && nextOffset < 0) {
            throw new IllegalStateException("Restored cache bundle is missing Kafka position");
        }
        if (active != null) {
            active.verify();
            synchronized (ctx.getCheckpointLock()) {
                ctx.collect(CacheBundleEnvelope.fromBundle(active));
                if (selectedHandler != null && !selectedHandler.isBlank()
                        && !selectedHandler.equals(active.handler())) {
                    ctx.collect(CacheBundleEnvelope.forHandler(selectedHandler));
                }
            }
            log.info("[CACHE] restored rules={} politics={} without API",
                    active.compiledRulesVersion(), active.politicsVersion());
        }

        Properties props = new Properties();
        props.putAll(kafkaProperties);
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrap);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, kafkaGroup);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "10");
        CacheUpdateEventDeserializationSchema deserializer =
                new CacheUpdateEventDeserializationSchema();
        long backoffMs = 1_000L;
        while (running) {
            try {
                if (consumer == null) {
                    connectKafka(props, ctx);
                    backoffMs = 1_000L;
                }
                if (active == null) {
                    CacheBundleState initial = loadWithRetry(null);
                    if (initial == null) return;
                    publish(ctx, initial, nextOffset);
                }
                ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<byte[], byte[]> record : records) {
                    if (!running) break;
                    processNotification(ctx, deserializer, record);
                }
            } catch (OffsetOutOfRangeException expired) {
                // Without this explicit failure the source would silently miss cache updates.
                throw new IllegalStateException("Notification offset expired: " + nextOffset
                        + ". Manual reconciliation required", expired);
            } catch (WakeupException wakeup) {
                if (!running) return;
                log.warn("[CACHE] Kafka consumer wakeup; reconnecting from offset={}", nextOffset);
                closeConsumerQuietly();
                backoffMs = retryDelay(backoffMs);
            } catch (KafkaException kafkaError) {
                if (kafkaError instanceof InvalidTopicException
                        || kafkaError instanceof ConfigException) {
                    // Permanent client configuration errors still fail loudly.
                    throw kafkaError;
                }
                log.warn("[CACHE] Kafka connection/metadata failed; reconnecting from offset={} "
                        + "after {}ms: {}", nextOffset, backoffMs, kafkaError.toString());
                closeConsumerQuietly();
                sleepRetry(backoffMs);
                backoffMs = Math.min(60_000L, backoffMs * 2);
            }
        }
    }

    private void connectKafka(Properties props, SourceContext<CacheBundleEnvelope> ctx) {
        KafkaConsumer<byte[], byte[]> candidate = new KafkaConsumer<>(props);
        // Expose to cancel() so a blocked metadata lookup can be interrupted.
        consumer = candidate;
        var partitions = candidate.partitionsFor(kafkaTopic, Duration.ofSeconds(5));
        if (partitions == null || partitions.size() != 1 || partitions.get(0).partition() != 0) {
            throw new IllegalStateException("Cache update topic must have 1 partition: " + kafkaTopic);
        }
        TopicPartition partition = new TopicPartition(kafkaTopic, 0);
        candidate.assign(Collections.singleton(partition));
        if (nextOffset >= 0) {
            // When Kafka reconnects use the last PROCESSED offset, never latest().
            candidate.seek(partition, nextOffset);
        } else {
            // On a fresh start establish Kafka position before loading the API snapshot.
            candidate.seekToEnd(Collections.singleton(partition));
            long start = candidate.position(partition);
            synchronized (ctx.getCheckpointLock()) { nextOffset = start; }
            log.info("[CACHE] Kafka offset fixed before bootstrap: {}", start);
        }
        log.info("[CACHE] Kafka notification consumer connected; resume offset={}", nextOffset);
    }

    private void closeConsumerQuietly() {
        KafkaConsumer<byte[], byte[]> current = consumer;
        consumer = null;
        if (current != null) {
            try { current.close(Duration.ofMillis(500)); }
            catch (Exception e) { log.warn("[CACHE] Kafka consumer close failed: {}", e.toString()); }
        }
    }

    private void sleepRetry(long millis) throws InterruptedException {
        long remaining = millis;
        while (running && remaining > 0) {
            long chunk = Math.min(remaining, 250L);
            Thread.sleep(chunk);
            remaining -= chunk;
        }
    }

    private long retryDelay(long previous) throws InterruptedException {
        sleepRetry(previous);
        return Math.min(60_000L, previous * 2);
    }

    private void processNotification(SourceContext<CacheBundleEnvelope> ctx,
            CacheUpdateEventDeserializationSchema deserializer,
            ConsumerRecord<byte[], byte[]> record) throws Exception {
        if (!running || record.offset() < nextOffset) return;
        CacheUpdateEvent event = deserializer.deserialize(record.value());
        if (event.isHandler()) {
            applyHandlerOnly(ctx, event, record.offset() + 1);
            return;
        }
        CacheBundleState base = staged != null ? staged : active;
        int currentVersion = event.isRules()
                ? Integer.parseInt(base.compiledRulesVersion())
                : Integer.parseInt(base.politicsVersion());
        if (event.version <= currentVersion) {
            advance(ctx, record.offset() + 1);
            return;
        }
        CacheBundleState next = loadWithRetry(event, base);
        if (next == null) return;
        // A standalone handler update takes precedence over the older staged bundle.
        String requestedHandler = event.handler != null
                ? event.handler
                : (staged != null && !handlerOverridePending)
                        ? staged.handler() : selectedHandler;
        if (requestedHandler != null && !requestedHandler.equals(next.handler())) {
            next = next.withHandler(requestedHandler);
        }
        try {
            CacheBootstrapService.validate(next);
            publish(ctx, next, record.offset() + 1);
            log.info("[CACHE] applied {} version={} rules={} politics={} handler={} kafkaOffset={}",
                    event.cacheName, event.version, next.compiledRulesVersion(),
                    next.politicsVersion(), next.handler(), record.offset());
        } catch (IncompatibleCacheBundleException incompatible) {
            // A rules update may precede the corresponding politics update (or vice versa).
            // Keep processing on the old valid bundle. Checkpoint the pending candidate,
            // then re-evaluate after the next notification for either component.
            byte[] encoded = CacheBundleStateSerializer.encode(next);
            synchronized (ctx.getCheckpointLock()) {
                staged = next;
                stagedEncoded = encoded;
                nextOffset = record.offset() + 1;
            }
            log.warn("[CACHE] staged rules={} politics={}, waiting for compatible update: {}",
                    next.compiledRulesVersion(), next.politicsVersion(), incompatible.getMessage());
        }
    }

    /** No API reads, bundle serialization, class loading, or policy mutation. */
    void applyHandlerOnly(SourceContext<CacheBundleEnvelope> ctx, CacheUpdateEvent event,
                          long offsetAfterEvent) {
        if (event == null || !event.isHandler() || !event.isValid()) {
            throw new IllegalArgumentException("Not a valid handler update: " + event);
        }
        synchronized (ctx.getCheckpointLock()) {
            ctx.collect(CacheBundleEnvelope.forHandler(event.handler));
            selectedHandler = event.handler;
            handlerOverridePending = true;
            nextOffset = offsetAfterEvent;
        }
        log.info("[CACHE] handler changed to {} without cache reload, kafkaOffset={}",
                event.handler, offsetAfterEvent - 1);
    }

    private CacheBundleState loadWithRetry(CacheUpdateEvent update) throws Exception {
        return loadWithRetry(update, active);
    }

    private CacheBundleState loadWithRetry(CacheUpdateEvent update, CacheBundleState base) throws Exception {
        long delay = Math.max(100, pt.getLong("cache.reload.retry.initial.ms", 5000L));
        long maxDelay = Math.max(delay, pt.getLong("cache.reload.retry.max.ms", 60000L));
        while (running) {
            try {
                return update == null ? loader().loadLatest(
                        pt.get("handler", pt.get("handler.default", "flink")))
                        : loader().applyUpdate(base, update);
            } catch (Exception failure) {
                if (!running) return null;
                if (Thread.currentThread().isInterrupted()) throw new InterruptedException("Cache loading interrupted");
                if (!CacheRetryPolicy.retryable(failure)
                        && !(update == null && failure instanceof IncompatibleCacheBundleException)) {
                    throw failure;
                }
                log.warn("[CACHE] load failed event={} retryMs={} reason={}", update, delay, failure.toString());
                long until = System.nanoTime() + Duration.ofMillis(delay).toNanos();
                while (running && System.nanoTime() < until) {
                    Thread.sleep(Math.min(500, Math.max(1,
                            Duration.ofNanos(until - System.nanoTime()).toMillis())));
                }
                delay = delay >= maxDelay / 2 ? maxDelay : delay * 2;
            }
        }
        return null;
    }

    private void publish(SourceContext<CacheBundleEnvelope> ctx, CacheBundleState bundle,
                         long offset) throws Exception {
        byte[] encoded = CacheBundleStateSerializer.encode(bundle);
        CacheBundleEnvelope envelope = CacheBundleEnvelope.fromBundle(bundle);
        synchronized (ctx.getCheckpointLock()) {
            ctx.collect(envelope);
            active = bundle;
            activeEncoded = encoded;
            selectedHandler = bundle.handler();
            handlerOverridePending = false;
            staged = null;
            stagedEncoded = null;
            nextOffset = offset;
        }
    }

    private void advance(SourceContext<CacheBundleEnvelope> ctx, long offset) {
        synchronized (ctx.getCheckpointLock()) { nextOffset = offset; }
    }

    @Override public void cancel() {
        running = false;
        KafkaConsumer<byte[], byte[]> c = consumer;
        if (c != null) c.wakeup();
    }
    @Override public void close() throws Exception {
        cancel();
        closeConsumerQuietly();
        if (thinClient != null) thinClient.close();
        super.close();
    }

    @Override public void initializeState(FunctionInitializationContext context) throws Exception {
        state = context.getOperatorStateStore().getListState(
                new ListStateDescriptor<>("cache-source-state-v2", BytePrimitiveArraySerializer.INSTANCE));
        active = null;
        activeEncoded = null;
        staged = null;
        stagedEncoded = null;
        nextOffset = -1;
        selectedHandler = null;
        handlerOverridePending = false;
        restored = context.isRestored();
        if (restored) {
            for (byte[] bytes : state.get()) {
                if (active != null) throw new IllegalStateException("Duplicate source state");
                DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes));
                if (in.readInt() != STATE_MAGIC) throw new IllegalStateException("Wrong source state format");
                nextOffset = in.readLong();
                int size = in.readInt();
                if (size < 0 || size > MAX_BUNDLE_BYTES) throw new IllegalStateException("Bad bundle size");
                if (size != 0) {
                    byte[] encoded = new byte[size];
                    in.readFully(encoded);
                    active = CacheBundleStateSerializer.decode(encoded);
                    activeEncoded = encoded;
                }
                int stagedSize = in.readInt();
                if (stagedSize < 0 || stagedSize > MAX_BUNDLE_BYTES) {
                    throw new IllegalStateException("Invalid staged bundle size");
                }
                if (stagedSize != 0) {
                    byte[] encoded = new byte[stagedSize];
                    in.readFully(encoded);
                    staged = CacheBundleStateSerializer.decode(encoded);
                    stagedEncoded = encoded;
                }
                // Backwards-compatible optional routing field after the v2 bundle bytes.
                if (in.available() > 0) selectedHandler = in.readUTF();
                if (in.available() > 0) handlerOverridePending = in.readBoolean();
                if (in.available() != 0) throw new IllegalStateException("Trailing source state bytes");
                if ((selectedHandler == null || selectedHandler.isBlank()) && active != null) {
                    selectedHandler = active.handler();
                }
            }
        }
    }

    @Override public void snapshotState(FunctionSnapshotContext context) throws Exception {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(buf);
        out.writeInt(STATE_MAGIC);
        out.writeLong(nextOffset);
        byte[] encoded = activeEncoded;
        out.writeInt(encoded == null ? 0 : encoded.length);
        if (encoded != null) out.write(encoded);
        byte[] pending = stagedEncoded;
        out.writeInt(pending == null ? 0 : pending.length);
        if (pending != null) out.write(pending);
        out.writeUTF(selectedHandler == null ? "" : selectedHandler);
        out.writeBoolean(handlerOverridePending);
        out.flush();
        state.update(Collections.singletonList(buf.toByteArray()));
    }
}
