package com.gpb.datafirewall.source;

import com.gpb.datafirewall.bundle.CacheBundleState;
import com.gpb.datafirewall.bundle.CacheBundleStateSerializer;
import com.gpb.datafirewall.config.IgniteRulesApiClient;
import com.gpb.datafirewall.dto.IgniteBytecodeSource;
import com.gpb.datafirewall.ignite.BytecodeSource;
import com.gpb.datafirewall.ignite.impl.IgniteClientFacadeImpl;
import com.gpb.datafirewall.kafka.CacheUpdateEvent;
import com.gpb.datafirewall.kafka.CacheUpdateEventDeserializationSchema;
import com.gpb.datafirewall.services.CacheBootstrapService;
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
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * 1 Kafka partition, parallelism=1. Иницииализирующая позиция Kafka устанавливается 
 * ПЕРЕД HTTP bootstrap. Сохраняет в состоянии оператора последний отправленный bundle + следующий Kafka offset,
 * используя явный формат передачи данных; 
 * При восстановлении из checkpoint API не требуется.
 *
 * В этом источнике намерено используется legacy SourceFunction API для Flink 1.20,
 * поэтому, Flink может сохранять checkpoint bundle + offset под блокировкой checkpoint источника.
 */
@SuppressWarnings("deprecation")
public final class CacheSnapshotSource extends RichSourceFunction<CacheBundleState>
        implements CheckpointedFunction {

    private static final Logger log = 
            LoggerFactory.getLogger(CacheSnapshotSource.class);

    private static final long serialVersionUID=1L;
    private static final int STATE_MAGIC=0x43535331;
    
    private final ParameterTool pt;
    private final String jwt;
    private final Properties kafkaProperties;
    private final String kafkaBootstrap;
    private final String kafkaTopic;
    private final String kafkaGroup;

    private volatile boolean running = true;

    private transient KafkaConsumer<byte[],byte[]> consumer;
    private transient ListState<byte[]> state;
    private transient CacheBundleState active;
    private transient byte[] activeEncoded;

    private transient long nextOffset=-1L;
    private transient long pendingOffset=-1L;
    private transient long pendingEpoch=-1L;
    private transient boolean restored;

    private transient CacheBootstrapService bootstrap;
    private transient IgniteClientFacadeImpl thinClient;

    public CacheSnapshotSource(
            ParameterTool pt,
            String jwt,
            Properties kafkaProperties,
            String kafkaBootstrap,
            String kafkaTopic,
            String kafkaGroup) {

        this.pt = pt;
        this.jwt = jwt;
        this.kafkaProperties = kafkaProperties;
        this.kafkaBootstrap = kafkaBootstrap;
        this.kafkaTopic = kafkaTopic;
        this.kafkaGroup = kafkaGroup;
    }

    private CacheBootstrapService loader() {
        if (bootstrap==null) {
            IgniteRulesApiClient api = new IgniteRulesApiClient(
                    pt.get(
                            "ignite.apiUrl",
                            "http://127.0.0.1:8080"
                    ),
                    pt.get(
                            "ignite.api.tls.ca.pem",
                            null
                    ),
                    jwt
            );

            boolean checksum = pt.getBoolean(
                    "cache.manifest.require-publisher-checksum",
                    true
            );

            if (
                    "thin".equalsIgnoreCase(
                            pt.get("rules.loader", "http")
                    )
            ) {
                thinClient = 
                        new IgniteClientFacadeImpl(
                                pt.get("ignite.host", "127.0.0.1"),
                                pt.getInt("ignite.port", 10800)
                        );

                BytecodeSource source = new IgniteBytecodeSource(thinClient);

                bootstrap = new CacheBootstrapService(
                        api,
                        source,
                        checksum
                );
                
            } else if (
                    "http".equalsIgnoreCase(
                            pt.get("rules.loader", "http")
                    )
            ) {
                bootstrap = CacheBootstrapService.http(
                        api,
                        checksum
                );

            } else throw new IllegalArgumentException("Unsupported rules.loader");
        }

        return bootstrap;
    }

    @Override public void run(
            SourceContext<CacheBundleState> ctx) throws Exception {

        if (
                !pt.getBoolean("cache.bootstrap.enabled", true) 
                        && active==null
        )
            throw new IllegalStateException(
                    "cache.bootstrap.enabled=false is incompatible with an empty CacheSnapshotSource");

        if (restored && active != null && nextOffset < 0)
            throw new IllegalStateException("Restored bundle has no Kafka offset");

        // Не дожидаясь получения метаданных Kafka перед выпуском восстановленного пакета. 
        // Координатор обработки также может восстановить свой зафиксированный пакет независимо.
        if (active != null) {
            active.verify();

            synchronized(
                    ctx.getCheckpointLock()
            ) { 
                ctx.collect(active); 
            }

            log.info(
                    "[CACHE-SOURCE] restored epoch={} without API", 
                    active.cacheEpoch()
            );
        }

        Properties props = new Properties();

        props.putAll(kafkaProperties);
        
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrap);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, kafkaGroup);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,"10");

        consumer = new KafkaConsumer<>(props);

        var partitions = consumer.partitionsFor(kafkaTopic);

        if (
                partitions==null 
                        || partitions.size()!=1 
                        || partitions.get(0).partition()!=0
        )
            throw new IllegalStateException(
                    "Cache update topic must have exactly one partition: " + kafkaTopic
            );

        TopicPartition partition = new TopicPartition(kafkaTopic, 0);

        consumer.assign(
                Collections.singleton(partition)
        );

        if (restored && nextOffset >= 0) {
            consumer.seek(partition, nextOffset);
        } else {
            // Фиксируем offset до начала медленного процесса инициализации API.
            consumer.seekToEnd(
                    Collections.singleton(
                            partition
                    )
            );

            long initialOffset = consumer.position(partition);

            synchronized (ctx.getCheckpointLock()) { 
                nextOffset = initialOffset; 
            }

            log.info(
                    "[CACHE-SOURCE] captured Kafka offset={} before first bootstrap",
                    nextOffset
            );
        }

        if (active == null) {
            CacheBundleState bundle = loadWithRetry(null);
            
            if (bundle == null) return; // Отменяем до retry

            byte[] encoded = CacheBundleStateSerializer.encode(bundle);
            synchronized (ctx.getCheckpointLock()) {
                ctx.collect(bundle);
                active = bundle;
                activeEncoded = encoded;
            }
            log.info("[CACHE-SOURCE] initialized epoch={} compiled={} politics={}",
                    bundle.cacheEpoch(), bundle.compiledRulesVersion(), bundle.politicsVersion());
        }

        if (!running) return;

        CacheUpdateEventDeserializationSchema deserializer = 
                new CacheUpdateEventDeserializationSchema();

        outer: while(running) {
            ConsumerRecords<byte[],byte[]> records;

            try {
                records = consumer.poll(Duration.ofMillis(500));
            } catch (org.apache.kafka.clients.consumer.OffsetOutOfRangeException ex) {
                throw new IllegalStateException(
                        "Cache Kafka offset is no longer retained: topic="
                        + kafkaTopic + " nextOffset=" + nextOffset + " epoch=" + active.cacheEpoch()
                        + ". Restore a retained offset or perform an explicit cache reconciliation; "
                        + "automatic reset to latest is disabled.", ex
                );
            } catch (WakeupException ex) { 
                if (!running) break;
                throw ex; 
            }

            for (ConsumerRecord<byte[],byte[]> record : records) {
                if (!running) break outer;

                long offset = record.offset();

                // В ходе одного опроса можно предварительно получить несколько записей; 
                // nextOffset отслеживает только выданную часть (prefix).
                if (offset < nextOffset) continue;

                CacheUpdateEvent event = deserializer
                        .deserialize(record.value());

                if (!event.isValid() || event.cacheEpoch < 0) {
                    throw new IllegalStateException(
                            "Cache update must contain cacheEpoch; Kafka offset=" + offset
                    );
                }
                
                if (event.cacheEpoch <= active.cacheEpoch()) {
                    synchronized(ctx.getCheckpointLock()) {
                        nextOffset = offset + 1;
                        pendingOffset = -1;
                        pendingEpoch = -1;
                    }

                    continue;
                }

                synchronized (ctx.getCheckpointLock()) {
                    pendingOffset = offset;
                    pendingEpoch = event.cacheEpoch;
                }

                CacheBundleState next = loadWithRetry(event.cacheEpoch);

                if (next == null) return;

                if (next.cacheEpoch() != event.cacheEpoch) {
                    log.warn("[CACHE-SOURCE] epoch={} was removed; loading complete newer epoch={}",
                            event.cacheEpoch, next.cacheEpoch());
                }

                if (next.cacheEpoch() <= active.cacheEpoch())
                    throw new IllegalStateException("Stale bundle epoch");

                byte[] encoded = CacheBundleStateSerializer.encode(next);

                // Сбои Downstream/serialization должны останавливать task;
                // Не должны запускать цикл повторных попыток API.
                synchronized (ctx.getCheckpointLock()) {
                    ctx.collect(next);
                    active = next;
                    activeEncoded = encoded;
                    nextOffset = offset + 1;
                    pendingOffset = -1;
                    pendingEpoch = -1;
                }

                log.info("[CACHE-SOURCE] published epoch={} compiled={} politics={} offset={}",
                        next.cacheEpoch(), next.compiledRulesVersion(), next.politicsVersion(), offset);
            }
        }
    }

    private CacheBundleState loadWithRetry(Long epoch) throws Exception {
        long delay = Math.max(
                100L, 
                pt.getLong("cache.reload.retry.initial.ms", 5000L)
        );
        long maxDelay = Math.max(
                delay, 
                pt.getLong("cache.reload.retry.max.ms", 60000L)
        );

        while (running) {
            try {
                return epoch == null ? loader().loadLatest() : CacheLoadPolicy.load(epoch,
                        requested -> loader().load(requested), () -> loader().loadLatest());
            } catch (Exception failure) {
                if (!running) return null;

                if (Thread.currentThread().isInterrupted())
                    throw new InterruptedException("Cache source interrupted");

                if (!CacheLoadPolicy.retryable(failure)) throw failure;

                log.warn("[CACHE-SOURCE] load unavailable epoch={} retryInMs={} error={}",
                        epoch, delay, failure.toString());

                pause(delay);
                
                delay = delay >= maxDelay / 2 ? maxDelay : delay * 2;
            }
        }
        return null;
    }

    private void pause(long ms) throws InterruptedException {
        long end = 
                System.nanoTime()  
                        + Duration.ofMillis(
                                Math.max(100,ms)
                        ).toNanos();

        while (running && System.nanoTime() < end)
            Thread.sleep(
                    Math.min(
                            500,
                            Math.max(
                                    1,
                                    Duration.ofNanos(
                                            end - System.nanoTime()
                                    ).toMillis()
                            )
                    )
            );
    }

    @Override public void cancel() {
        running = false;
        KafkaConsumer<byte[],byte[]> c = consumer;
        if (c != null) c.wakeup();
    }

    @Override public void close() throws Exception {
        cancel();

        if (consumer != null) 
            consumer.close(Duration.ofSeconds(2));

        if (thinClient != null)
            thinClient.close();

        super.close();
    }

    @Override public void initializeState(
            FunctionInitializationContext context) throws Exception {

        state = context
                .getOperatorStateStore()
                .getListState(
                        new ListStateDescriptor<>(
                                "cache-source-state-v1", 
                                BytePrimitiveArraySerializer.INSTANCE
                        )
                );

        active = null;
        activeEncoded = null;
        nextOffset = -1L;
        pendingOffset = -1L;
        pendingEpoch = -1L;
        restored = context.isRestored();

        if (restored) 
            for (byte[] bytes : state.get()) {
        
                if (active != null)
                    throw new IllegalStateException("Duplicate CacheSnapshotSource state");

                DataInputStream in = new DataInputStream(
                        new ByteArrayInputStream(bytes)
                );

                if (in.readInt() != STATE_MAGIC)
                    throw new IllegalStateException("Bad cache source state magic");

                nextOffset = in.readLong();
                pendingOffset = in.readLong();
                pendingEpoch = in.readLong();

                int n = in.readInt();

                if (n<0 || n > 256*1024*1024)
                        throw new IllegalStateException("Bad cache state length");

                if (n>0) {
                    byte[] b = new byte[n];
                    in.readFully(b);
                    active = CacheBundleStateSerializer.decode(b);
                    activeEncoded = b;
                }

                if (in.available() != 0)
                    throw new IllegalStateException("Trailing source state");
            }
    }

    @Override public void snapshotState(
            FunctionSnapshotContext context)throws Exception {

        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(buf);
        
        out.writeInt(STATE_MAGIC);
        out.writeLong(nextOffset);
        out.writeLong(pendingOffset);
        out.writeLong(pendingEpoch);

        // The immutable bundle is serialized once per epoch, outside the checkpoint lock.
        byte[] encoded = activeEncoded;
        
        out.writeInt(
                encoded == null ? 0 : encoded.length
        );

        if (encoded != null) out.write(encoded);

        out.flush();

        state.update(
                Collections.singletonList(
                        buf.toByteArray()
                )
        );
    }
}
