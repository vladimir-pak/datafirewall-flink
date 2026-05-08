package ru.gpbapp.datafirewallflink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.gpbapp.datafirewallflink.artemis.ArtemisSink;
import ru.gpbapp.datafirewallflink.artemis.ArtemisSource;
import ru.gpbapp.datafirewallflink.config.JobConfig;
import ru.gpbapp.datafirewallflink.dto.ProcessingResult;
import ru.gpbapp.datafirewallflink.kafka.CacheUpdateEvent;
import ru.gpbapp.datafirewallflink.kafka.CacheUpdateEventDeserializationSchema;
import ru.gpbapp.datafirewallflink.mq.MqSink;
import ru.gpbapp.datafirewallflink.mq.MqSource;
import ru.gpbapp.datafirewallflink.services.MessageRecord;
import ru.gpbapp.datafirewallflink.services.MessageReply;
import ru.gpbapp.datafirewallflink.services.RulesReloadBroadcastProcessFunction;
import com.fasterxml.jackson.databind.ObjectMapper;
import ru.gpbapp.datafirewallflink.dto.AuditRecord;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class Main {

    private static final Logger log = LoggerFactory.getLogger(Main.class);

    private static final String DEFAULT_KAFKA_BOOTSTRAP = "localhost:9092";
    private static final String DEFAULT_KAFKA_TOPIC = "rules-update";
    private static final String DEFAULT_KAFKA_GROUP = "dfw-rules-group";
    private static final String DEFAULT_DETAIL_KAFKA_TOPIC = "detail-answer";
    private static final String DEFAULT_MESSAGING_BACKEND = "mq";
    private static final int DEFAULT_PARALLELISM = 1;

    private static final long DEFAULT_CHECKPOINT_INTERVAL_MS = 5000L;
    private static final long DEFAULT_CHECKPOINT_TIMEOUT_MS = 60000L;
    private static final long DEFAULT_MIN_PAUSE_BETWEEN_CHECKPOINTS_MS = 1000L;
    private static final int DEFAULT_MAX_CONCURRENT_CHECKPOINTS = 1;

    private static final String DEFAULT_ARTEMIS_BROKER_URL = "tcp://localhost:61616";
    private static final String DEFAULT_ARTEMIS_IN_QUEUE = "IN.Q";
    private static final String DEFAULT_ARTEMIS_OUT_QUEUE = "OUT.Q";
    private static final long DEFAULT_ARTEMIS_RECEIVE_TIMEOUT_MS = 1000L;

    @SuppressWarnings("deprecation")
    public static void main(String[] args) throws Exception {
        String[] normalized = normalizeArgs(args);
        ParameterTool pt = ParameterTool.fromArgs(normalized);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(pt);

        int parallelism = pt.getInt("parallelism.source", DEFAULT_PARALLELISM);
        env.setParallelism(parallelism);

        boolean detailKafkaEnabled = pt.getBoolean("detail.kafka.enabled", false);
        boolean checkpointingEnabled = pt.getBoolean("flink.checkpoint.enabled", detailKafkaEnabled);

        if (checkpointingEnabled) {
            long checkpointInterval = pt.getLong("flink.checkpoint.interval.ms", DEFAULT_CHECKPOINT_INTERVAL_MS);
            long checkpointTimeout = pt.getLong("flink.checkpoint.timeout.ms", DEFAULT_CHECKPOINT_TIMEOUT_MS);
            long minPauseBetweenCheckpoints = pt.getLong(
                    "flink.checkpoint.min.pause.ms",
                    DEFAULT_MIN_PAUSE_BETWEEN_CHECKPOINTS_MS
            );
            int maxConcurrentCheckpoints = pt.getInt(
                    "flink.checkpoint.max.concurrent",
                    DEFAULT_MAX_CONCURRENT_CHECKPOINTS
            );

            env.enableCheckpointing(checkpointInterval);
            env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE);
            env.getCheckpointConfig().setCheckpointTimeout(checkpointTimeout);
            env.getCheckpointConfig().setMinPauseBetweenCheckpoints(minPauseBetweenCheckpoints);
            env.getCheckpointConfig().setMaxConcurrentCheckpoints(maxConcurrentCheckpoints);

            log.info(
                    "[MAIN] checkpointing enabled: intervalMs={}, timeoutMs={}, minPauseMs={}, maxConcurrent={}",
                    checkpointInterval,
                    checkpointTimeout,
                    minPauseBetweenCheckpoints,
                    maxConcurrentCheckpoints
            );
        } else {
            log.info("[MAIN] checkpointing disabled");
        }

        String backend = pt.get("messaging.backend", DEFAULT_MESSAGING_BACKEND).trim().toLowerCase();
        JobConfig cfg = JobConfig.fromArgs(pt);

        String kafkaBootstrap = pt.get("kafka.bootstrap", DEFAULT_KAFKA_BOOTSTRAP);
        String kafkaTopic = pt.get("kafka.topic", DEFAULT_KAFKA_TOPIC);
        String kafkaGroup = pt.get("kafka.group", DEFAULT_KAFKA_GROUP);

        String detailKafkaBootstrap = pt.get("detail.kafka.bootstrap", kafkaBootstrap);
        String detailKafkaTopic = pt.get("detail.kafka.topic", DEFAULT_DETAIL_KAFKA_TOPIC);

        String artemisBrokerUrl = pt.get("artemis.broker.url", DEFAULT_ARTEMIS_BROKER_URL);
        String artemisUser = firstNonBlank(
                System.getenv("ARTEMIS_USER"),
                pt.get("artemis.user", null),
                ""
        );
        String artemisPassword = firstNonBlank(
                System.getenv("ARTEMIS_PASSWORD"),
                pt.get("artemis.password", null),
                ""
        );
        String artemisInQueue = pt.get("artemis.in.queue", DEFAULT_ARTEMIS_IN_QUEUE);
        String artemisOutQueue = pt.get("artemis.out.queue", DEFAULT_ARTEMIS_OUT_QUEUE);
        long artemisReceiveTimeoutMs = pt.getLong(
                "artemis.receive.timeout.ms",
                DEFAULT_ARTEMIS_RECEIVE_TIMEOUT_MS
        );

        boolean mqTlsEnabled = pt.getBoolean("mq.tls.enabled", false);
        String mqTlsCipherSuite = pt.get("mq.tls.cipherSuite", null);
        String mqTrustStore = pt.get("mq.ssl.truststore.location", null);
        String mqTrustStorePassword = pt.get("mq.ssl.truststore.password", null);

        boolean artemisTlsEnabled = pt.getBoolean("artemis.tls.enabled", false);
        String artemisTrustStore = pt.get("artemis.ssl.truststore.location", null);
        String artemisTrustStorePassword = pt.get("artemis.ssl.truststore.password", null);

        String igniteApiUrl = pt.get("ignite.apiUrl", "http://127.0.0.1:8080");
        String rulesLoader = pt.get("rules.loader", "http");
        boolean cacheBootstrapEnabled = pt.getBoolean("cache.bootstrap.enabled", true);
        boolean politicsBootstrapEnabled = pt.getBoolean("politics.bootstrap.enabled", false);
        boolean logPayloads = pt.getBoolean("log.payloads", false);

        boolean auditKafkaEnabled = pt.getBoolean("audit.kafka.enabled", true);
        String auditKafkaBootstrap = pt.get("audit.kafka.bootstrap", kafkaBootstrap);
        String auditKafkaTopic = pt.get("audit.kafka.topic", "audit-datafirewall");

        KafkaSink<String> auditSink = null;

        if ("artemis".equals(backend)) {
            requireNotBlank(artemisUser, "artemis.user or ENV ARTEMIS_USER");
            requireNotBlank(artemisPassword, "artemis.password or ENV ARTEMIS_PASSWORD");
        }

        validateMinimalTls(
                pt,
                backend,
                detailKafkaEnabled,
                mqTlsEnabled,
                mqTlsCipherSuite,
                mqTrustStore,
                mqTrustStorePassword,
                artemisTlsEnabled,
                artemisTrustStore,
                artemisTrustStorePassword
        );

        logStartupConfig(
                parallelism,
                backend,
                kafkaBootstrap,
                kafkaTopic,
                kafkaGroup,
                igniteApiUrl,
                rulesLoader,
                cacheBootstrapEnabled,
                politicsBootstrapEnabled,
                logPayloads,
                detailKafkaEnabled,
                detailKafkaBootstrap,
                detailKafkaTopic,
                checkpointingEnabled,
                artemisBrokerUrl,
                artemisInQueue,
                artemisOutQueue,
                mqTlsEnabled,
                artemisTlsEnabled,
                cfg
        );

        final DataStream<MessageRecord> inputStream;

        switch (backend) {
            case "mq" -> {
                log.info("[MAIN] INPUT -> MQ");

                inputStream = env.addSource(
                                new MqSource(
                                        cfg.mqHost(),
                                        cfg.mqPort(),
                                        cfg.mqChannel(),
                                        cfg.mqQmgr(),
                                        cfg.mqInQueue(),
                                        cfg.mqUser(),
                                        cfg.mqPassword(),
                                        logPayloads,
                                        pt.getInt("log.preview.len", 600),
                                        pt.getInt("mq.wait.interval.ms", 1000),
                                        mqTlsEnabled,
                                        mqTlsCipherSuite,
                                        mqTrustStore,
                                        mqTrustStorePassword
                                ),
                                "mq-source"
                        )
                        .name("mq-source")
                        .setParallelism(1);
            }

            case "artemis" -> {
                log.info("[MAIN] INPUT -> ARTEMIS");

                inputStream = env.addSource(
                                new ArtemisSource(
                                        artemisBrokerUrl,
                                        artemisUser,
                                        artemisPassword,
                                        artemisInQueue,
                                        artemisReceiveTimeoutMs,
                                        artemisTlsEnabled,
                                        artemisTrustStore,
                                        artemisTrustStorePassword
                                ),
                                "artemis-source"
                        )
                        .name("artemis-source")
                        .setParallelism(1);
            }

            default -> throw new IllegalArgumentException(
                    "Unsupported messaging.backend=" + backend + ". Supported: mq|artemis"
            );
        }

        Properties kafkaClientProps = buildKafkaClientProperties(pt, "kafka");
        Properties detailKafkaClientProps = buildKafkaClientProperties(pt, "detail.kafka");

        KafkaSource<CacheUpdateEvent> kafkaSource = KafkaSource.<CacheUpdateEvent>builder()
                .setBootstrapServers(kafkaBootstrap)
                .setTopics(kafkaTopic)
                .setGroupId(kafkaGroup)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setProperties(kafkaClientProps)
                .setValueOnlyDeserializer(new CacheUpdateEventDeserializationSchema())
                .build();

        DataStream<CacheUpdateEvent> updates = env
                .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "rules-kafka")
                .name("rules-kafka")
                .setParallelism(1);

        MapStateDescriptor<String, CacheUpdateEvent> bcDesc =
                new MapStateDescriptor<>(
                        "cache-update-broadcast",
                        Types.STRING,
                        Types.POJO(CacheUpdateEvent.class)
                );

        BroadcastStream<CacheUpdateEvent> bcUpdates = updates.broadcast(bcDesc);

        DataStream<ProcessingResult> processed = inputStream
                .connect(bcUpdates)
                .process(new RulesReloadBroadcastProcessFunction(bcDesc))
                .name("process-with-rules-reload")
                .setParallelism(1);

        if ("mq".equals(backend)) {
            DataStream<MessageReply> shortReplies = processed
                    .map(new MapFunction<ProcessingResult, MessageReply>() {
                        @Override
                        public MessageReply map(ProcessingResult result) {
                            if (result == null || result.getShortJson() == null) {
                                return null;
                            }
                            return new MessageReply(result.getCorrelId(), result.getShortJson());
                        }
                    })
                    .returns(Types.POJO(MessageReply.class))
                    .name("build-short-reply-for-mq")
                    .setParallelism(1)
                    .filter(new FilterFunction<MessageReply>() {
                        @Override
                        public boolean filter(MessageReply value) {
                            return value != null;
                        }
                    })
                    .name("filter-short-reply-mq-non-null")
                    .setParallelism(1);

            shortReplies
                    .addSink(new MqSink(
                            cfg.mqHost(),
                            cfg.mqPort(),
                            cfg.mqChannel(),
                            cfg.mqQmgr(),
                            cfg.mqOutQueue(),
                            cfg.mqUser(),
                            cfg.mqPassword(),
                            mqTlsEnabled,
                            mqTlsCipherSuite,
                            mqTrustStore,
                            mqTrustStorePassword
                    ))
                    .name("mq-sink")
                    .setParallelism(1);

            log.info("[MAIN] MQ output sink enabled");
        } else if ("artemis".equals(backend)) {
            DataStream<MessageReply> shortReplies = processed
                    .map(new MapFunction<ProcessingResult, MessageReply>() {
                        @Override
                        public MessageReply map(ProcessingResult result) {
                            if (result == null || result.getShortJson() == null) {
                                return null;
                            }
                            return new MessageReply(result.getCorrelId(), result.getShortJson());
                        }
                    })
                    .returns(Types.POJO(MessageReply.class))
                    .name("build-short-reply-for-artemis")
                    .setParallelism(1)
                    .filter(new FilterFunction<MessageReply>() {
                        @Override
                        public boolean filter(MessageReply value) {
                            return value != null;
                        }
                    })
                    .name("filter-short-reply-artemis-non-null")
                    .setParallelism(1);

            shortReplies
                    .addSink(new ArtemisSink(
                            artemisBrokerUrl,
                            artemisUser,
                            artemisPassword,
                            artemisOutQueue,
                            artemisTlsEnabled,
                            artemisTrustStore,
                            artemisTrustStorePassword
                    ))
                    .name("artemis-sink")
                    .setParallelism(1);

            log.info("[MAIN] Artemis output sink enabled");
        }

        if (detailKafkaEnabled) {
            DataStream<String> detailReplies = processed
                    .map(new MapFunction<ProcessingResult, String>() {
                        @Override
                        public String map(ProcessingResult result) {
                            return result == null ? null : result.getDetailJson();
                        }
                    })
                    .returns(Types.STRING)
                    .name("extract-detail-json")
                    .setParallelism(1)
                    .filter(new FilterFunction<String>() {
                        @Override
                        public boolean filter(String value) {
                            return value != null && !value.isBlank();
                        }
                    })
                    .name("filter-detail-json-non-null")
                    .setParallelism(1);

            KafkaSink<String> detailKafkaSink = KafkaSink.<String>builder()
                    .setBootstrapServers(detailKafkaBootstrap)
                    .setKafkaProducerConfig(detailKafkaClientProps)
                    .setRecordSerializer(
                            KafkaRecordSerializationSchema.builder()
                                    .setTopic(detailKafkaTopic)
                                    .setValueSerializationSchema(new SimpleStringSchema())
                                    .build()
                    )
                    .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                    .build();

            detailReplies
                    .sinkTo(detailKafkaSink)
                    .name("detail-kafka-sink")
                    .setParallelism(1);

            log.info("[MAIN] detail kafka sink enabled: bootstrap={}, topic={}",
                    detailKafkaBootstrap, detailKafkaTopic);
        } else {
            log.info("[MAIN] detail kafka sink is disabled.");
        }
        if (auditKafkaEnabled) {
            Properties auditProps = buildKafkaClientProperties(pt, "audit.kafka");

            auditSink = KafkaSink.<String>builder()
                    .setBootstrapServers(auditKafkaBootstrap)
                    .setKafkaProducerConfig(auditProps)
                    .setRecordSerializer(
                            KafkaRecordSerializationSchema.builder()
                                    .setTopic(auditKafkaTopic)
                                    .setValueSerializationSchema(new SimpleStringSchema())
                                    .build()
                    )
                    .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                    .build();

            log.info("[MAIN] Audit Kafka sink enabled → topic={}", auditKafkaTopic);
        }
        if (auditKafkaEnabled && auditSink != null) {
            processed
                    .map(result -> {
                        if (result == null || result.getShortJson() == null) {
                            return null;
                        }
                        try {
                            ObjectMapper mapper = new ObjectMapper();
                            AuditRecord audit = new AuditRecord(
                                    result.getCorrelId() != null ? new String(result.getCorrelId(), StandardCharsets.UTF_8) : "unknown",
                                    result.getOriginalJson(),
                                    result.getShortJson(),
                                    result.getDetailJson(),
                                    "PROCESSED"
                            );
                            return mapper.writeValueAsString(audit);
                        } catch (Exception e) {
                            log.warn("Failed to serialize audit record", e);
                            return null;
                        }
                    })
                    .filter(Objects::nonNull)
                    .sinkTo(auditSink)
                    .name("audit-kafka-sink")
                    .setParallelism(1);
        }

        env.execute("DataFirewall Messaging Job (SHORT ANSWER -> MQ/ARTEMIS, DETAIL ANSWER -> Kafka) + Kafka Rules Reload");
    }

    private static void validateMinimalTls(
            ParameterTool pt,
            String backend,
            boolean detailKafkaEnabled,
            boolean mqTlsEnabled,
            String mqTlsCipherSuite,
            String mqTrustStore,
            String mqTrustStorePassword,
            boolean artemisTlsEnabled,
            String artemisTrustStore,
            String artemisTrustStorePassword
    ) {
        if (pt.getBoolean("kafka.tls.enabled", false)) {
            requireNotBlank(pt.get("kafka.ssl.truststore.location", null), "kafka.ssl.truststore.location");
            requireNotBlank(pt.get("kafka.ssl.truststore.password", null), "kafka.ssl.truststore.password");
        }

        if (detailKafkaEnabled && pt.getBoolean("detail.kafka.tls.enabled", false)) {
            requireNotBlank(pt.get("detail.kafka.ssl.truststore.location", null), "detail.kafka.ssl.truststore.location");
            requireNotBlank(pt.get("detail.kafka.ssl.truststore.password", null), "detail.kafka.ssl.truststore.password");
        }

        if ("mq".equals(backend) && mqTlsEnabled) {
            requireNotBlank(mqTlsCipherSuite, "mq.tls.cipherSuite");
            requireNotBlank(mqTrustStore, "mq.ssl.truststore.location");
            requireNotBlank(mqTrustStorePassword, "mq.ssl.truststore.password");
        }

        if ("artemis".equals(backend) && artemisTlsEnabled) {
            requireNotBlank(artemisTrustStore, "artemis.ssl.truststore.location");
            requireNotBlank(artemisTrustStorePassword, "artemis.ssl.truststore.password");
            String brokerUrl = pt.get("artemis.broker.url", DEFAULT_ARTEMIS_BROKER_URL);
            if (!brokerUrl.startsWith("ssl://")) {
                throw new IllegalArgumentException(
                        "When artemis.tls.enabled=true, artemis.broker.url must start with ssl://"
                );
            }
        }
    }

    private static Properties buildKafkaClientProperties(ParameterTool pt, String prefix) {
        Properties props = new Properties();

        boolean tlsEnabled = pt.getBoolean(prefix + ".tls.enabled", false);
        if (!tlsEnabled) {
            return props;
        }

        putIfNotBlank(props, "security.protocol", pt.get(prefix + ".security.protocol", "SSL"));
        putIfNotBlank(props, "ssl.truststore.location", pt.get(prefix + ".ssl.truststore.location", null));
        putIfNotBlank(props, "ssl.truststore.password", pt.get(prefix + ".ssl.truststore.password", null));
        putIfNotBlank(
                props,
                "ssl.endpoint.identification.algorithm",
                pt.get(prefix + ".ssl.endpoint.identification.algorithm", "https")
        );

        return props;
    }

    private static void putIfNotBlank(Properties props, String key, String value) {
        if (value != null && !value.isBlank()) {
            props.setProperty(key, value);
        }
    }

    private static void requireNotBlank(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must be provided");
        }
    }

    private static String firstNonBlank(String... values) {
        if (values == null) {
            return null;
        }
        for (String value : values) {
            if (value != null && !value.isBlank()) {
                return value;
            }
        }
        return null;
    }

    private static void logStartupConfig(
            int parallelism,
            String backend,
            String kafkaBootstrap,
            String kafkaTopic,
            String kafkaGroup,
            String igniteApiUrl,
            String rulesLoader,
            boolean cacheBootstrapEnabled,
            boolean politicsBootstrapEnabled,
            boolean logPayloads,
            boolean detailKafkaEnabled,
            String detailKafkaBootstrap,
            String detailKafkaTopic,
            boolean checkpointingEnabled,
            String artemisBrokerUrl,
            String artemisInQueue,
            String artemisOutQueue,
            boolean mqTlsEnabled,
            boolean artemisTlsEnabled,
            JobConfig cfg
    ) {
        log.info(
                "[MAIN] startup config: parallelism={}, messaging.backend={}, kafka.bootstrap={}, kafka.topic={}, kafka.group={}, " +
                        "ignite.apiUrl={}, rules.loader={}, cache.bootstrap.enabled={}, politics.bootstrap.enabled={}, " +
                        "log.payloads={}, detail.kafka.enabled={}, detail.kafka.bootstrap={}, detail.kafka.topic={}, " +
                        "checkpointing.enabled={}, artemis.broker.url={}, artemis.in.queue={}, artemis.out.queue={}, artemis.tls.enabled={}, " +
                        "mq.host={}, mq.port={}, mq.channel={}, mq.qmgr={}, mq.inQueue={}, mq.outQueue={}, mq.user={}, mq.tls.enabled={}",
                parallelism,
                backend,
                kafkaBootstrap,
                kafkaTopic,
                kafkaGroup,
                igniteApiUrl,
                rulesLoader,
                cacheBootstrapEnabled,
                politicsBootstrapEnabled,
                logPayloads,
                detailKafkaEnabled,
                detailKafkaBootstrap,
                detailKafkaTopic,
                checkpointingEnabled,
                artemisBrokerUrl,
                artemisInQueue,
                artemisOutQueue,
                artemisTlsEnabled,
                cfg.mqHost(),
                cfg.mqPort(),
                cfg.mqChannel(),
                cfg.mqQmgr(),
                cfg.mqInQueue(),
                cfg.mqOutQueue(),
                cfg.mqUser() == null || cfg.mqUser().isBlank() ? "<empty>" : "<set>",
                mqTlsEnabled
        );
    }

    private static String[] normalizeArgs(String[] args) {
        if (args == null || args.length == 0) {
            return new String[0];
        }

        List<String> out = new ArrayList<>();
        for (String a : args) {
            if (a == null) {
                continue;
            }
            if (a.startsWith("--") && a.contains("=")) {
                int idx = a.indexOf('=');
                out.add(a.substring(0, idx));
                out.add(a.substring(idx + 1));
            } else {
                out.add(a);
            }
        }
        return out.toArray(new String[0]);
    }
}