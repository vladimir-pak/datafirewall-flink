package com.gpb.datafirewall;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.OpenContext;
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
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.gpb.datafirewall.artemis.ArtemisConnectionUrlBuilder;
import com.gpb.datafirewall.artemis.ArtemisSink;
import com.gpb.datafirewall.artemis.ArtemisSource;
import com.gpb.datafirewall.config.JobConfig;
import com.gpb.datafirewall.dto.AuditRecord;
import com.gpb.datafirewall.dto.ProcessingResult;
import com.gpb.datafirewall.kafka.CacheUpdateEvent;
import com.gpb.datafirewall.kafka.CacheUpdateEventDeserializationSchema;
import com.gpb.datafirewall.mq.MqSink;
import com.gpb.datafirewall.mq.MqSource;
import com.gpb.datafirewall.services.MessageRecord;
import com.gpb.datafirewall.services.MessageReply;
import com.gpb.datafirewall.services.RulesReloadBroadcastProcessFunction;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class Main {

    private static final Logger log = LoggerFactory.getLogger(Main.class);

    private static final String DEFAULT_KAFKA_BOOTSTRAP = "localhost:9092";
    private static final String DEFAULT_KAFKA_TOPIC = "rules-update";
    private static final String DEFAULT_KAFKA_GROUP = "dfw-rules-group";
    // private static final String DEFAULT_DETAIL_KAFKA_TOPIC = "detail-answer";
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
        ParameterTool pt = loadParameters(args);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(pt);

        int parallelism = pt.getInt("parallelism", DEFAULT_PARALLELISM);
        env.setParallelism(parallelism);

        // boolean detailKafkaEnabled = pt.getBoolean("detail.kafka.enabled", false);
        boolean checkpointingEnabled = pt.getBoolean("flink.checkpoint.enabled", true);

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

        // String detailKafkaBootstrap = pt.get("detail.kafka.bootstrap", kafkaBootstrap);
        // String detailKafkaTopic = pt.get("detail.kafka.topic", DEFAULT_DETAIL_KAFKA_TOPIC);

        // String artemisBrokerUrl = pt.get("artemis.broker.url", DEFAULT_ARTEMIS_BROKER_URL);
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
        String artemisKeyStore = pt.get("artemis.ssl.keystore.location", null);
        String artemisKeyStorePassword = pt.get("artemis.ssl.keystore.password", null);
        String artemisCipherSuites = pt.get("artemis.ssl.enabled.cipher.suites", null);

        String artemisBrokerUrlRaw = pt.get("artemis.broker.url", DEFAULT_ARTEMIS_BROKER_URL);

        String artemisBrokerUrl = ArtemisConnectionUrlBuilder.buildFromBrokerUrl(
                artemisBrokerUrlRaw,
                artemisTlsEnabled,
                artemisTrustStore,
                artemisTrustStorePassword,
                artemisKeyStore,
                artemisKeyStorePassword,
                artemisCipherSuites
        );
        log.info("Artemis URL: {}", artemisBrokerUrl);

        String igniteApiUrl = pt.get("ignite.apiUrl", "http://127.0.0.1:8080");
        String rulesLoader = pt.get("rules.loader", "http");
        boolean cacheBootstrapEnabled = pt.getBoolean("cache.bootstrap.enabled", true);
        boolean politicsBootstrapEnabled = pt.getBoolean("politics.bootstrap.enabled", false);
        boolean logPayloads = pt.getBoolean("log.payloads", false);

        boolean auditKafkaEnabled = pt.getBoolean("audit.kafka.enabled", true);
        String auditKafkaBootstrap = pt.get("audit.kafka.bootstrap", kafkaBootstrap);
        String auditKafkaTopic = pt.get("audit.kafka.topic", "audit-datafirewall");

        if ("artemis".equals(backend)) {
            requireNotBlank(artemisUser, "artemis.user or ENV ARTEMIS_USER");
            requireNotBlank(artemisPassword, "artemis.password or ENV ARTEMIS_PASSWORD");
        }

        validateMinimalTls(
                pt,
                backend,
                auditKafkaEnabled,
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
                auditKafkaEnabled,
                auditKafkaBootstrap,
                auditKafkaTopic,
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
                        .uid("mq-source");
                        // .setParallelism(1);
            }

            case "artemis" -> {
                log.info("[MAIN] INPUT -> ARTEMIS");

                inputStream = env.addSource(
                                new ArtemisSource(
                                        artemisBrokerUrl,
                                        artemisUser,
                                        artemisPassword,
                                        artemisInQueue,
                                        artemisReceiveTimeoutMs
                                ),
                                "artemis-source"
                        )
                        .name("artemis-source")
                        .uid("artemis-source");
                        // .setParallelism(1);
            }

            default -> throw new IllegalArgumentException(
                    "Unsupported messaging.backend=" + backend + ". Supported: mq|artemis"
            );
        }

        Properties kafkaClientProps = buildKafkaClientProperties(pt, "kafka");
        Properties auditKafkaClientProps = buildKafkaClientProperties(pt, "audit.kafka");

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
                .uid("rules-kafka-source")
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
                .uid("process-with-rules-reload");

        if ("mq".equals(backend)) {
            DataStream<MessageReply> shortReplies = processed
                .flatMap(new FlatMapFunction<ProcessingResult, MessageReply>() {
                    @Override
                    public void flatMap(ProcessingResult result, Collector<MessageReply> out) {
                        if (result == null) {
                            return;
                        }

                        String shortJson = result.getShortJson();
                        if (shortJson == null || shortJson.isBlank()) {
                            return;
                        }

                        out.collect(MessageReply.forMq(result.getMqCorrelationId(), shortJson));
                    }
                })
                .returns(Types.POJO(MessageReply.class))
                .name("build-short-reply")
                .uid("build-short-reply");

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
                    .uid("mq-sink");

            log.info("[MAIN] MQ output sink enabled");
        } else if ("artemis".equals(backend)) {
            DataStream<MessageReply> shortReplies = processed
                .flatMap(new FlatMapFunction<ProcessingResult, MessageReply>() {
                    @Override
                    public void flatMap(ProcessingResult result, Collector<MessageReply> out) {
                        if (result == null) {
                            return;
                        }

                        String shortJson = result.getShortJson();
                        if (shortJson == null || shortJson.isBlank()) {
                            return;
                        }

                        out.collect(MessageReply.forJms(result.getJmsCorrelationId(), shortJson));
                    }
                })
                .returns(Types.POJO(MessageReply.class))
                .name("build-short-reply")
                .uid("build-short-reply");

            shortReplies
                    .addSink(new ArtemisSink(
                            artemisBrokerUrl,
                            artemisUser,
                            artemisPassword,
                            artemisOutQueue
                    ))
                    .name("artemis-sink")
                    .uid("artemis-sink");

            log.info("[MAIN] Artemis output sink enabled");
        }

        if (auditKafkaEnabled) {

            KafkaSink<String> auditKafkaSink = KafkaSink.<String>builder()
                    .setBootstrapServers(auditKafkaBootstrap)
                    .setKafkaProducerConfig(auditKafkaClientProps)
                    .setRecordSerializer(
                            KafkaRecordSerializationSchema.builder()
                                    .setTopic(auditKafkaTopic)
                                    .setValueSerializationSchema(new SimpleStringSchema())
                                    .build()
                    )
                    .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                    .build();

            processed
                    .flatMap(new RichFlatMapFunction<ProcessingResult, String>() {
                        private transient ObjectMapper mapper;

                        @Override
                        public void open(OpenContext openContext) {
                            this.mapper = new ObjectMapper();
                        }

                        @Override
                        public void flatMap(ProcessingResult result, Collector<String> out) {
                            if (result == null) {
                                return;
                            }

                            try {
                                JsonNode requestNode = safeReadTree(mapper, result.getOriginalJson());
                                JsonNode shortNode = safeReadTree(mapper, result.getShortJson());
                                JsonNode detailNode = safeReadTree(mapper, result.getDetailJson());

                                AuditRecord audit = new AuditRecord(
                                        result.getEventId(),
                                        requestNode,
                                        shortNode,
                                        detailNode,
                                        Instant.now().toString(),
                                        "PROCESSED"
                                );

                                out.collect(mapper.writeValueAsString(audit));
                            } catch (Exception e) {
                                log.warn("Failed to serialize audit record", e);
                            }
                        }
                    })
                    .returns(Types.STRING)
                    .name("build-audit-record-json")
                    .uid("build-audit-record-json")
                    .sinkTo(auditKafkaSink)
                    .name("audit-kafka-sink")
                    .uid("audit-kafka-sink");
                    
            log.info("[MAIN] audit kafka sink enabled: bootstrap={}, topic={}",
                auditKafkaBootstrap, auditKafkaTopic);
        } else {
            log.info("[MAIN] audit kafka sink is disabled.");
        }

        env.execute("DataFirewall Flink Job");
    }

    private static void validateMinimalTls(
            ParameterTool pt,
            String backend,
            boolean auditKafkaEnabled,
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

        if (auditKafkaEnabled && pt.getBoolean("audit.kafka.tls.enabled", false)) {
            requireNotBlank(pt.get("audit.kafka.ssl.truststore.location", null), "audit.kafka.ssl.truststore.location");
            requireNotBlank(pt.get("audit.kafka.ssl.truststore.password", null), "audit.kafka.ssl.truststore.password");
        }

        if ("mq".equals(backend) && mqTlsEnabled) {
            requireNotBlank(mqTlsCipherSuite, "mq.tls.cipherSuite");
            requireNotBlank(mqTrustStore, "mq.ssl.truststore.location");
            requireNotBlank(mqTrustStorePassword, "mq.ssl.truststore.password");
        }

        if ("artemis".equals(backend) && artemisTlsEnabled) {
            requireNotBlank(artemisTrustStore, "artemis.ssl.truststore.location");
            requireNotBlank(artemisTrustStorePassword, "artemis.ssl.truststore.password");
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
            boolean auditKafkaEnabled,
            String auditKafkaBootstrap,
            String auditKafkaTopic,
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
                        "log.payloads={}, audit.kafka.enabled={}, audit.kafka.bootstrap={}, audit.kafka.topic={}, " +
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
                auditKafkaEnabled,
                auditKafkaBootstrap,
                auditKafkaTopic,
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

    private static ParameterTool loadParameters(String[] args) throws Exception {
        String[] normalized = normalizeArgs(args);

        ParameterTool cliParams = ParameterTool.fromArgs(normalized);

        String configFile = cliParams.get("config.file", null);
        if (configFile == null || configFile.isBlank()) {
            return cliParams;
        }

        ParameterTool fileParams = ParameterTool.fromPropertiesFile(configFile);

        // Важно: параметры из CLI должны иметь приоритет над файлом
        return fileParams.mergeWith(cliParams);
    }

    private static JsonNode safeReadTree(ObjectMapper mapper, String json) {
        if (json == null || json.isBlank()) {
            return null;
        }

        try {
            return mapper.readTree(json);
        } catch (Exception e) {
            return mapper.getNodeFactory().textNode(json);
        }
    }
}