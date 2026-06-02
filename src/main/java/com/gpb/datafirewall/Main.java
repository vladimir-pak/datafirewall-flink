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
import org.apache.kafka.clients.admin.AdminClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.gpb.datafirewall.artemis.ArtemisConnectionUrlBuilder;
import com.gpb.datafirewall.audit.AuditConfig;
import com.gpb.datafirewall.audit.AuditEventType;
import com.gpb.datafirewall.audit.AuditPropertiesUtil;
import com.gpb.datafirewall.audit.CefAuditEvent;
import com.gpb.datafirewall.audit.CefAuditPublisher;
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
import com.gpb.datafirewall.vault.VaultClient;
import com.gpb.datafirewall.vault.VaultClientConfig;
import com.gpb.datafirewall.vault.dto.VaultSecretsDto;

import java.time.Instant;
import java.nio.file.Path;
import java.util.Map;
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

        VaultSecretsDto vaultSecrets = VaultClient.loadSecrets(
                VaultClientConfig.fromArgs(pt)
        );

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
        JobConfig cfg = JobConfig.fromArgs(pt, vaultSecrets);

        String kafkaBootstrap = pt.get("kafka.bootstrap", DEFAULT_KAFKA_BOOTSTRAP);
        String kafkaTopic = pt.get("kafka.topic", DEFAULT_KAFKA_TOPIC);
        String kafkaGroup = pt.get("kafka.group", DEFAULT_KAFKA_GROUP);

        AuditConfig cefAuditConfig = AuditConfig.from(pt, vaultSecrets, kafkaBootstrap);
        try (CefAuditPublisher jobAuditPublisher = new CefAuditPublisher(cefAuditConfig)) {
            publishJobPropertiesAudit(pt, cefAuditConfig, jobAuditPublisher);
            jobAuditPublisher.publish(
                    cefAuditConfig.enrich(CefAuditEvent.builder(AuditEventType.JOB_SUBMITTED))
                            .put("argsCount", args == null ? 0 : args.length)
                            .put("cefFilePath", Path.of(cefAuditConfig.cefFilePath()).toAbsolutePath().normalize().toString())
                            .put("auditKafkaTopic", cefAuditConfig.kafkaTopic())
                            .build()
            );
        }

        String artemisUser = vaultSecrets.mqUser();
        String artemisPassword = vaultSecrets.mqPassword();

        String artemisInQueue = pt.get("artemis.in.queue", DEFAULT_ARTEMIS_IN_QUEUE);
        String artemisOutQueue = pt.get("artemis.out.queue", DEFAULT_ARTEMIS_OUT_QUEUE);
        long artemisReceiveTimeoutMs = pt.getLong(
                "artemis.receive.timeout.ms",
                DEFAULT_ARTEMIS_RECEIVE_TIMEOUT_MS
        );

        boolean mqTlsEnabled = pt.getBoolean("mq.tls.enabled", false);
        String mqTlsCipherSuite = pt.get("mq.tls.cipherSuite", null);
        String mqTrustStore = pt.get("mq.ssl.truststore.location", null);
        String mqTrustStorePassword = vaultSecrets.mqTruststorePassword();

        boolean artemisTlsEnabled = pt.getBoolean("artemis.tls.enabled", false);
        String artemisTrustStore = pt.get("artemis.ssl.truststore.location", null);
        String artemisTrustStorePassword = vaultSecrets.mqTruststorePassword();
        String artemisKeyStore = pt.get("artemis.ssl.keystore.location", null);
        String artemisKeyStorePassword = vaultSecrets.mqKeystorePassword();
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
        String auditKafkaTopic = pt.get("audit.kafka.topic", "datafirewall.processing.audit");

        if ("artemis".equals(backend)) {
            requireNotBlank(artemisUser, "mqUser in vault");
            requireNotBlank(artemisPassword, "mqPassword in vault");
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
                                        mqTrustStorePassword,
                                        cefAuditConfig
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
                                        artemisReceiveTimeoutMs,
                                        cefAuditConfig
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

        Properties kafkaClientProps = buildKafkaClientProperties(pt, "kafka", vaultSecrets);
        Properties auditKafkaClientProps = buildKafkaClientProperties(pt, "audit.kafka", vaultSecrets);

        try (CefAuditPublisher jobAuditPublisher = new CefAuditPublisher(cefAuditConfig)) {
            publishKafkaConnectionCheck(jobAuditPublisher, cefAuditConfig, "rules-kafka", kafkaBootstrap, kafkaClientProps, vaultSecrets.kafkaUser());
            publishKafkaConnectionCheck(jobAuditPublisher, cefAuditConfig, "cef-audit-kafka", cefAuditConfig.kafkaBootstrap(), cefAuditConfig.toKafkaProducerProperties(), vaultSecrets.kafkaUser());
            jobAuditPublisher.publish(
                    cefAuditConfig.enrich(CefAuditEvent.builder(AuditEventType.KAFKA_RULES_SOURCE_CONFIGURED))
                            .put("bootstrap", kafkaBootstrap)
                            .put("protocol", String.valueOf(kafkaClientProps.getOrDefault("security.protocol", "TCP")))
                            .put("connectionUser", vaultSecrets.kafkaUser())
                            .put("topic", kafkaTopic)
                            .put("groupId", kafkaGroup)
                            .build()
            );
        }

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
                .process(new RulesReloadBroadcastProcessFunction(bcDesc, vaultSecrets.jwt()))
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
                            mqTrustStorePassword,
                            cefAuditConfig
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
                            artemisOutQueue,
                            cefAuditConfig
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

        try (CefAuditPublisher jobAuditPublisher = new CefAuditPublisher(cefAuditConfig)) {
            jobAuditPublisher.publish(
                    cefAuditConfig.enrich(CefAuditEvent.builder(AuditEventType.JOB_STARTED))
                            .put("parallelism", parallelism)
                            .put("messagingBackend", backend)
                            .build()
            );

            env.execute(cefAuditConfig.jobName());

            jobAuditPublisher.publish(
                    cefAuditConfig.enrich(CefAuditEvent.builder(AuditEventType.JOB_STOPPED))
                            .put("result", "env.execute returned")
                            .build()
            );
        } catch (Exception e) {
            try (CefAuditPublisher jobAuditPublisher = new CefAuditPublisher(cefAuditConfig)) {
                jobAuditPublisher.publish(
                        cefAuditConfig.enrich(CefAuditEvent.builder(AuditEventType.JOB_FAILED))
                                .status("FAILED")
                                .put("errorClass", e.getClass().getName())
                                .put("errorMessage", e.getMessage())
                                .build()
                );
            }
            throw e;
        }
    }


    private static void publishJobPropertiesAudit(
            ParameterTool pt,
            AuditConfig auditConfig,
            CefAuditPublisher publisher
    ) {
        String currentHash = AuditPropertiesUtil.sha256OfMaskedProperties(pt);
        Map<String, String> masked = AuditPropertiesUtil.maskedProperties(pt);

        String stateFile = pt.get(
                "cef.audit.properties.hash.file",
                auditConfig.cefFilePath() + ".properties.sha256"
        );
        String previousHash = AuditPropertiesUtil.detectPreviousHashAndStore(stateFile, currentHash);

        publisher.publish(
                auditConfig.enrich(CefAuditEvent.builder(AuditEventType.JOB_PROPERTIES_LOADED))
                        .put("configFile", pt.get("config.file", null))
                        .put("propertiesHash", currentHash)
                        .put("propertiesHashStateFile", Path.of(stateFile).toAbsolutePath().normalize().toString())
                        .put("properties", masked)
                        .build()
        );

        if (previousHash != null && !previousHash.equals(currentHash)) {
            publisher.publish(
                    auditConfig.enrich(CefAuditEvent.builder(AuditEventType.JOB_PROPERTIES_CHANGED))
                            .put("configFile", pt.get("config.file", null))
                            .put("oldPropertiesHash", previousHash)
                            .put("newPropertiesHash", currentHash)
                            .put("propertiesHashStateFile", Path.of(stateFile).toAbsolutePath().normalize().toString())
                            .put("properties", masked)
                            .build()
            );
        }
    }

    private static void publishKafkaConnectionCheck(
            CefAuditPublisher publisher,
            AuditConfig auditConfig,
            String connectionName,
            String bootstrap,
            Properties properties,
            String connectionUser
    ) {
        Properties adminProps = new Properties();
        if (properties != null) {
            adminProps.putAll(properties);
        }
        adminProps.setProperty("bootstrap.servers", bootstrap);
        adminProps.putIfAbsent("request.timeout.ms", "5000");
        adminProps.putIfAbsent("default.api.timeout.ms", "5000");

        long t0 = System.nanoTime();
        try (AdminClient adminClient = AdminClient.create(adminProps)) {
            String clusterId = adminClient.describeCluster()
                    .clusterId()
                    .get(5, java.util.concurrent.TimeUnit.SECONDS);
            long durationMs = (System.nanoTime() - t0) / 1_000_000;

            publisher.publish(
                    auditConfig.enrich(CefAuditEvent.builder(AuditEventType.KAFKA_CONNECTION_CHECK_OK))
                            .put("connectionName", connectionName)
                            .put("bootstrap", bootstrap)
                            .put("protocol", String.valueOf(adminProps.getOrDefault("security.protocol", "TCP")))
                            .put("connectionUser", connectionUser)
                            .put("kafkaClusterId", clusterId)
                            .put("durationMs", durationMs)
                            .build()
            );
        } catch (Exception e) {
            long durationMs = (System.nanoTime() - t0) / 1_000_000;
            publisher.publish(
                    auditConfig.enrich(CefAuditEvent.builder(AuditEventType.KAFKA_CONNECTION_CHECK_FAILED))
                            .status("FAILED")
                            .put("connectionName", connectionName)
                            .put("bootstrap", bootstrap)
                            .put("protocol", String.valueOf(adminProps.getOrDefault("security.protocol", "TCP")))
                            .put("connectionUser", connectionUser)
                            .put("durationMs", durationMs)
                            .put("errorClass", e.getClass().getName())
                            .put("errorMessage", e.getMessage())
                            .build()
            );
        }
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
        validateKafkaSecurity(pt, "kafka", "Kafka rules source");

        if (auditKafkaEnabled) {
            validateKafkaSecurity(pt, "audit.kafka", "Kafka processing audit sink");
        }

        if ("mq".equals(backend) && mqTlsEnabled) {
            requireNotBlank(mqTlsCipherSuite, "mq.tls.cipherSuite");
            requireNotBlank(mqTrustStore, "mq.ssl.truststore.location");
            // requireNotBlank(mqTrustStorePassword, "mq.ssl.truststore.password");
        }

        if ("artemis".equals(backend) && artemisTlsEnabled) {
            requireNotBlank(artemisTrustStore, "artemis.ssl.truststore.location");
            // requireNotBlank(artemisTrustStorePassword, "artemis.ssl.truststore.password");
        }
    }


    private static void validateKafkaSecurity(ParameterTool pt, String prefix, String componentName) {
        String securityProtocol = kafkaParam(pt, prefix, "security.protocol", null);
        boolean tlsEnabled = kafkaBooleanParam(pt, prefix, "tls.enabled", false) || isSslProtocol(securityProtocol);
        boolean saslEnabled = kafkaBooleanParam(pt, prefix, "sasl.enabled", false) || isSaslProtocol(securityProtocol);

        if (tlsEnabled) {
            requireNotBlank(kafkaParam(pt, prefix, "ssl.truststore.location", null), componentName + " ssl.truststore.location");
            requireNotBlank(kafkaParam(pt, prefix, "ssl.keystore.location", null), componentName + " ssl.keystore.location");
        }

        if (saslEnabled) {
            requireNotBlank(kafkaParam(pt, prefix, "sasl.mechanism", "SCRAM-SHA-512"), componentName + " sasl.mechanism");
        }
    }

    private static Properties buildKafkaClientProperties(ParameterTool pt, String prefix, VaultSecretsDto vaultSecrets) {
        Properties props = new Properties();

        String securityProtocol = kafkaParam(pt, prefix, "security.protocol", null);
        boolean tlsEnabled = kafkaBooleanParam(pt, prefix, "tls.enabled", false)
                || isSslProtocol(securityProtocol);
        boolean saslEnabled = kafkaBooleanParam(pt, prefix, "sasl.enabled", false)
                || isSaslProtocol(securityProtocol);

        if (securityProtocol == null || securityProtocol.isBlank()) {
            if (saslEnabled) {
                securityProtocol = tlsEnabled ? "SASL_SSL" : "SASL_PLAINTEXT";
            } else if (tlsEnabled) {
                securityProtocol = "SSL";
            }
        }

        putIfNotBlank(props, "security.protocol", securityProtocol);

        if (saslEnabled) {
            String mechanism = kafkaParam(pt, prefix, "sasl.mechanism", "SCRAM-SHA-512");
            putIfNotBlank(props, "sasl.mechanism", mechanism);
            putIfNotBlank(props, "sasl.jaas.config", buildKafkaJaasConfig(mechanism, vaultSecrets));
        }

        if (tlsEnabled) {
            putIfNotBlank(props, "ssl.truststore.location", kafkaParam(pt, prefix, "ssl.truststore.location", null));
            putIfNotBlank(props, "ssl.truststore.type", kafkaParam(pt, prefix, "ssl.truststore.type", null));
            putIfNotBlank(props, "ssl.truststore.password", vaultSecrets == null ? null : vaultSecrets.truststorePassword());

            putIfNotBlank(props, "ssl.keystore.location", kafkaParam(pt, prefix, "ssl.keystore.location", null));
            putIfNotBlank(props, "ssl.keystore.type", kafkaParam(pt, prefix, "ssl.keystore.type", null));
            putIfNotBlank(props, "ssl.keystore.password", vaultSecrets == null ? null : vaultSecrets.keystorePassword());
            putIfNotBlank(props, "ssl.key.password", kafkaParam(
                    pt,
                    prefix,
                    "ssl.key.password",
                    vaultSecrets == null ? null : vaultSecrets.keystorePassword()
            ));

            putIfNotBlank(
                    props,
                    "ssl.endpoint.identification.algorithm",
                    kafkaParam(pt, prefix, "ssl.endpoint.identification.algorithm", "https")
            );
        }

        copyKafkaClientOverride(pt, prefix, props, "client.id");
        copyKafkaClientOverride(pt, prefix, props, "request.timeout.ms");
        copyKafkaClientOverride(pt, prefix, props, "default.api.timeout.ms");
        copyKafkaClientOverride(pt, prefix, props, "metadata.max.age.ms");

        return props;
    }

    private static String buildKafkaJaasConfig(String mechanism, VaultSecretsDto vaultSecrets) {
        requireNotBlank(vaultSecrets == null ? null : vaultSecrets.kafkaUser(), "kafkaUser in vault");
        requireNotBlank(vaultSecrets == null ? null : vaultSecrets.kafkaPassword(), "kafkaPassword in vault");

        String loginModule = "PLAIN".equalsIgnoreCase(mechanism)
                ? "org.apache.kafka.common.security.plain.PlainLoginModule"
                : "org.apache.kafka.common.security.scram.ScramLoginModule";

        return loginModule
                + " required username=\"" + escapeJaas(vaultSecrets.kafkaUser())
                + "\" password=\"" + escapeJaas(vaultSecrets.kafkaPassword()) + "\";";
    }

    private static String escapeJaas(String value) {
        return value == null ? "" : value.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    private static boolean isSslProtocol(String securityProtocol) {
        return securityProtocol != null && securityProtocol.toUpperCase().contains("SSL");
    }

    private static boolean isSaslProtocol(String securityProtocol) {
        return securityProtocol != null && securityProtocol.toUpperCase().startsWith("SASL_");
    }

    private static String kafkaParam(ParameterTool pt, String prefix, String suffix, String defaultValue) {
        String value = pt.get(prefix + "." + suffix, null);
        if (value != null && !value.isBlank()) {
            return value;
        }

        if (!"kafka".equals(prefix)) {
            value = pt.get("kafka." + suffix, null);
            if (value != null && !value.isBlank()) {
                return value;
            }
        }

        return defaultValue;
    }

    private static boolean kafkaBooleanParam(ParameterTool pt, String prefix, String suffix, boolean defaultValue) {
        String value = kafkaParam(pt, prefix, suffix, null);
        return value == null ? defaultValue : Boolean.parseBoolean(value);
    }

    private static void copyKafkaClientOverride(ParameterTool pt, String prefix, Properties props, String kafkaProperty) {
        putIfNotBlank(props, kafkaProperty, kafkaParam(pt, prefix, kafkaProperty, null));
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