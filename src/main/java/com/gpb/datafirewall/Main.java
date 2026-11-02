package com.gpb.datafirewall;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.kafka.clients.admin.AdminClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.gpb.datafirewall.util.DfwDateTimeFormatter;
import com.gpb.datafirewall.util.HostnameUtils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.gpb.datafirewall.audit.AuditConfig;
import com.gpb.datafirewall.audit.AuditEventType;
import com.gpb.datafirewall.audit.AuditPropertiesUtil;
import com.gpb.datafirewall.audit.CefAuditEvent;
import com.gpb.datafirewall.audit.CefAuditPublisher;
import com.gpb.datafirewall.config.JobConfig;
import com.gpb.datafirewall.dto.AuditRecord;
import com.gpb.datafirewall.dto.ProcessingResult;
import com.gpb.datafirewall.services.DynamicHandler;
import com.gpb.datafirewall.bundle.CacheBundleEnvelope;
import com.gpb.datafirewall.source.CacheSnapshotSource;
import com.gpb.datafirewall.transaction.TransactionSettings;
import com.gpb.datafirewall.transaction.kafka.TransactionalRulesOperatorFactory;
import com.gpb.datafirewall.transaction.artemis.ArtemisConnectionUrlBuilder;
import com.gpb.datafirewall.transaction.kafka.BestEffortKafkaAuditSink;
import com.gpb.datafirewall.transaction.kafka.ProcessingAuditDiscardSink;
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
    private static final String DEFAULT_MESSAGING_BACKEND = "mq";
    private static final int DEFAULT_PARALLELISM = 1;
    private static final String DEFAULT_ARTEMIS_BROKER_URL = "tcp://localhost:61616";
    private static final String DEFAULT_ARTEMIS_IN_QUEUE = "IN.Q";
    private static final String DEFAULT_ARTEMIS_OUT_QUEUE = "OUT.Q";
    private static final long DEFAULT_ARTEMIS_RECEIVE_TIMEOUT_MS = 5000L;

    @SuppressWarnings("deprecation")
    public static void main(String[] args) throws Exception {
        ParameterTool pt = loadParameters(args);

        // Flink creates the execution environment using the cluster's config.yaml.
        // Never override restart strategy or checkpoint settings from job.properties.
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        logIgnoredLegacyFlinkProperties(pt);
        env.getConfig().setGlobalJobParameters(pt);

        final int environmentParallelism = env.getParallelism() > 0
                ? env.getParallelism() : DEFAULT_PARALLELISM;
        final int parallelism = positiveParallelism(
                "parallelism", pt.getInt("parallelism", environmentParallelism));
        if (pt.has("parallelism")) {
            env.setParallelism(parallelism);
        }

        final int processParallelism = positiveParallelism(
                "parallelism.process", pt.getInt("parallelism.process", parallelism));
        final int auditParallelism = positiveParallelism(
                "parallelism.audit", pt.getInt("parallelism.audit", processParallelism));
        // Set audit.slot-sharing-group only when audit needs dedicated slot capacity.
        final String auditSlotSharingGroup = pt.get("audit.slot-sharing-group", "").trim();
        final boolean checkpointingEnabled = env.getCheckpointConfig().isCheckpointingEnabled();
        if (!checkpointingEnabled) {
            log.warn("[MAIN] Checkpointing is disabled: fresh starts/restarts bootstrap the cache from API");
        }

        log.info("[MAIN] Flink cluster checkpointing: enabled={}, intervalMs={}, timeoutMs={}, mode={}, " +
                        "maxConcurrent={}, minPauseMs={}",
                checkpointingEnabled,
                env.getCheckpointConfig().getCheckpointInterval(),
                env.getCheckpointConfig().getCheckpointTimeout(),
                env.getCheckpointConfig().getCheckpointingConsistencyMode(),
                env.getCheckpointConfig().getMaxConcurrentCheckpoints(),
                env.getCheckpointConfig().getMinPauseBetweenCheckpoints());
        log.info("[MAIN] parallelism: default={}, transactionalWorkers={}, audit={}, auditSlotSharingGroup={}",
                parallelism, processParallelism, auditParallelism,
                auditSlotSharingGroup.isBlank() ? "<inherited>" : auditSlotSharingGroup);

        VaultSecretsDto vaultSecrets = VaultClient.loadSecrets(
                VaultClientConfig.fromArgs(pt)
        );

        String backend = pt.get("messaging.backend", DEFAULT_MESSAGING_BACKEND).trim().toLowerCase();
        DynamicHandler startupHandler = DynamicHandler.from(
                pt.get("handler", pt.get("handler.default", "flink")),
                DynamicHandler.FLINK
        );
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
        String mqReplyXFrom = firstNotBlank(
                pt.get("mq.reply.xFrom", null),
                pt.get("mq.xFrom", null),
                pt.get("X_From", null),
                "MKD"
        );
        String mqReplyXServiceId = firstNotBlank(
                pt.get("mq.reply.xServiceId", null),
                pt.get("mq.xServiceId", null),
                pt.get("X_ServiceID", null)
        );

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
        log.info("Artemis URL: {}", maskConnectionUrl(artemisBrokerUrl));

        String igniteApiUrl = pt.get("ignite.apiUrl", "http://127.0.0.1:8080");
        String rulesLoader = pt.get("rules.loader", "http");
        boolean cacheBootstrapEnabled = pt.getBoolean("cache.bootstrap.enabled", true);
        boolean politicsBootstrapEnabled = pt.getBoolean("politics.bootstrap.enabled", false);
        boolean logPayloads = pt.getBoolean("log.payloads", false);

        boolean auditKafkaEnabled = pt.getBoolean("audit.kafka.enabled", true);
        String auditKafkaBootstrap = pt.get("audit.kafka.bootstrap", kafkaBootstrap);
        String auditKafkaTopic = pt.get("audit.kafka.topic", "datafirewall.processing.audit");

        if (startupHandler == DynamicHandler.DOTNET) {
            requireNotBlank(
                    pt.get("handler.dotnet.url", pt.get("dotnet.handler.url", null)),
                    "handler.dotnet.url/dotnet.handler.url"
            );
            requireNotBlank(vaultSecrets.dotnetJwt(), "dotnetJwt in vault");
        }

        if ("artemis".equals(backend)) {
            requireNotBlank(artemisUser, "mqUser in vault");
            requireNotBlank(artemisPassword, "mqPassword in vault");
        }

        if ("mq".equals(backend)) {
            requireNotBlank(cfg.mqQmgr(), "mq.qmgr/mq.queueManager");
            requireNotBlank(mqReplyXServiceId, "mq.reply.xServiceId/mq.xServiceId/X_ServiceID");
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

        if (!"mq".equals(backend) && !"artemis".equals(backend)) {
            throw new IllegalArgumentException("Unsupported messaging.backend=" + backend);
        }
        // A broker transaction needs one local connection for RECEIVE + SEND + COMMIT.
        // parallelism.process is the number of independent transactional workers.
        if (pt.has("parallelism.source") || pt.has("parallelism.sink")) {
            log.warn("parallelism.source/sink are ignored by transactional broker processing; "
                    + "set parallelism.process instead");
        }

        Properties kafkaClientProps = buildKafkaClientProperties(pt, "kafka", vaultSecrets);
        Properties auditKafkaClientProps = buildKafkaClientProperties(pt, "audit.kafka", vaultSecrets);

        try (CefAuditPublisher jobAuditPublisher = new CefAuditPublisher(cefAuditConfig)) {

            publishKafkaConnectionCheck(
                    jobAuditPublisher, 
                    cefAuditConfig, 
                    "rules-kafka", 
                    kafkaBootstrap, 
                    kafkaClientProps, 
                    vaultSecrets.kafkaUser()
            );

            publishKafkaConnectionCheck(
                    jobAuditPublisher, 
                    cefAuditConfig, 
                    "cef-audit-kafka", 
                    cefAuditConfig.kafkaBootstrap(), 
                    cefAuditConfig.toKafkaProducerProperties(), 
                    vaultSecrets.kafkaUser()
            );

            jobAuditPublisher.publish(
                    cefAuditConfig.enrich(
                            CefAuditEvent.builder(
                                    AuditEventType.KAFKA_RULES_SOURCE_CONFIGURED
                            )
                    )
                    .put("bootstrap", kafkaBootstrap)
                    .put("protocol", String.valueOf(
                            kafkaClientProps.getOrDefault(
                                    "security.protocol", 
                                    "TCP"
                            )
                    ))
                    .put("connectionUser", vaultSecrets.kafkaUser())
                    .put("topic", kafkaTopic)
                    .put("groupId", kafkaGroup)
                    .build()
            );
        }

        // Источник полных снимков кэша
        DataStream<CacheBundleEnvelope> cacheUpdates = env.addSource(
                new CacheSnapshotSource(
                        pt,
                        vaultSecrets.jwt(),
                        kafkaClientProps,
                        kafkaBootstrap,
                        kafkaTopic,
                        kafkaGroup
                ),
                "cache-snapshot-source",
                Types.POJO(CacheBundleEnvelope.class)
            )
            .name("cache-snapshot-source")
            .uid("cache-snapshot-source-v2")
            .setParallelism(1);

        // Independent broker transactions: a worker owns its own MQ manager/JMS session.
        // Kafka cache updates are the only Flink input and are prepared off-mailbox.
        TransactionSettings txSettings = new TransactionSettings(
                backend,
                cfg.mqHost(), cfg.mqPort(), cfg.mqChannel(), cfg.mqQmgr(),
                "mq".equals(backend) ? cfg.mqInQueue() : artemisInQueue,
                "mq".equals(backend) ? cfg.mqOutQueue() : artemisOutQueue,
                "mq".equals(backend) ? cfg.mqUser() : artemisUser,
                "mq".equals(backend) ? cfg.mqPassword() : artemisPassword,
                mqTlsEnabled, mqTlsCipherSuite, mqTrustStore, mqTrustStorePassword,
                mqReplyXFrom, mqReplyXServiceId, artemisBrokerUrl,
                "mq".equals(backend)
                        ? pt.getInt("mq.wait.interval.ms", 5000)
                        : artemisReceiveTimeoutMs);

        if (pt.has("transaction.pulse.interval.ms")) {
            log.warn("[MAIN] transaction.pulse.interval.ms is ignored: blocking broker receivers replaced pulses");
        }

        SingleOutputStreamOperator<ProcessingResult> processed = cacheUpdates
                .broadcast()
                .transform(
                        "transactional-broker-worker",
                        Types.POJO(ProcessingResult.class),
                        new TransactionalRulesOperatorFactory(
                                vaultSecrets.jwt(), vaultSecrets, txSettings, cefAuditConfig))
                .uid("transactional-broker-worker-v3")
                .setParallelism(processParallelism);

        // Only the independent Kafka audit is emitted after successful broker commit.
        // It is NOT part of the MQ/JMS transaction and is not a durable outbox.
        DataStream<ProcessingResult> flinkAuditResults = processed;

        if (auditKafkaEnabled) {
            // Deliberately lossy audit. Kafka producer errors must never fail the broker job.
            // Legacy delivery guarantees are not applicable to this fire-and-forget sink.
            String configuredGuarantee = pt.get("audit.kafka.delivery-guarantee", "NONE").trim();
            if (!"NONE".equalsIgnoreCase(configuredGuarantee)) {
                log.warn("[MAIN] Ignoring audit.kafka.delivery-guarantee={}; "
                        + "audit is now best-effort and never checkpoints Kafka transactions",
                        configuredGuarantee);
            }
            BestEffortKafkaAuditSink auditKafkaSink = new BestEffortKafkaAuditSink(
                    auditKafkaBootstrap, auditKafkaTopic, auditKafkaClientProps,
                    pt.getLong("audit.kafka.best-effort.max-block.ms", 100L));

            SingleOutputStreamOperator<String> auditRecords = flinkAuditResults
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

                                // Enrich only the Kafka audit copy of QUERY, not the MQ body or ANSWER.

                                if (result.isMq() && requestNode != null && requestNode.isObject()) {
                                    ObjectNode query = ((ObjectNode) requestNode).deepCopy();

                                    // Сохраняем заголовки IBM MQ
                                    if (result.getHeaders() != null) {
                                        query.set("headers", mapper.valueToTree(result.getHeaders()));
                                    }

                                    // Заполняем поля по алгоритму ANSWER
                                    query.put("dfw_hostname", HostnameUtils.getHostname());

                                    query.put("dfw_action_dttm", Instant.now().toString());

                                    query.put(
                                            "dfw_created_dttm",
                                            DfwDateTimeFormatter.formatEpochMillis(result.getCreatedDttm())
                                    );

                                    query.put(
                                            "dfw_readed_dttm",
                                            DfwDateTimeFormatter.formatEpochMillis(result.getReadedDttm())
                                    );

                                    // Получаем dataset_code из ANSWER,
                                    // где он уже заполнен из controlArea
                                    JsonNode answerData = shortNode == null
                                            ? null
                                            : shortNode.path("data");

                                    JsonNode datasetCode = answerData == null
                                            ? null
                                            : answerData.get("dfw_dataset_code");

                                    if (datasetCode != null && !datasetCode.isNull()) {
                                        query.set("dfw_dataset_code", datasetCode.deepCopy());
                                    }

                                    requestNode = query;
                                }


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
                                log.warn("Dropping malformed Kafka processing audit eventId={}: {}",
                                        result.getEventId(), e.toString());
                            }
                        }
                    })
                    .returns(Types.STRING)
                    .name("build-flink-audit-record-json")
                    .uid("build-flink-audit-record-json")
                    .setParallelism(auditParallelism)
                    // This is a separate Flink task / task thread from rules processing.
                    // Note: this is NOT a durable queue and doesn't eliminate backpressure.
                    .startNewChain();

            // Both audit operators stay in the same slot sharing group by default.
            // Optionally isolate them in dedicated slots, at the cost of extra slots.
            if (!auditSlotSharingGroup.isBlank()) {
                auditRecords.slotSharingGroup(auditSlotSharingGroup);
            }

            DataStreamSink<String> auditSink = auditRecords
                    .addSink(auditKafkaSink)
                    .name("flink-audit-kafka-sink")
                    .uid("flink-audit-kafka-best-effort-v1")
                    .setParallelism(auditParallelism);
            if (!auditSlotSharingGroup.isBlank()) {
                auditSink.slotSharingGroup(auditSlotSharingGroup);
            }

            log.info("[MAIN] Flink audit Kafka sink enabled: bootstrap={}, topic={}",
                    auditKafkaBootstrap, auditKafkaTopic);
        } else {
            processed.addSink(new ProcessingAuditDiscardSink())
                    .name("broker-commit-events-discard")
                    .uid("broker-commit-events-discard-v1")
                    .setParallelism(auditParallelism);
            log.info("[MAIN] audit kafka sink is disabled; broker transactions remain enabled.");
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

    private static String maskConnectionUrl(String value) {
        return value == null ? null : value
                .replaceAll("(?i)(trustStorePassword=)[^&]*", "$1***")
                .replaceAll("(?i)(keyStorePassword=)[^&]*", "$1***")
                .replaceAll("(?i)(password=)[^&]*", "$1***");
    }

    private static int positiveParallelism(String key, int value) {
        if (value < 1) {
            throw new IllegalArgumentException(key + " must be >= 1, got " + value);
        }
        return value;
    }

    private static void logIgnoredLegacyFlinkProperties(ParameterTool pt) {
        for (String property : new String[]{
                "flink.restart.attempts", "flink.restart.delay.ms",
                "flink.checkpoint.enabled", "flink.checkpoint.interval.ms",
                "flink.checkpoint.timeout.ms", "flink.checkpoint.min.pause.ms",
                "flink.checkpoint.max.concurrent"
        }) {
            if (pt.has(property)) {
                log.warn("[MAIN] Ignoring legacy job property '{}': configure Flink in config.yaml instead", property);
            }
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
        copyKafkaClientOverride(pt, prefix, props, "retries");
        copyKafkaClientOverride(pt, prefix, props, "retry.backoff.ms");
        copyKafkaClientOverride(pt, prefix, props, "reconnect.backoff.ms");
        copyKafkaClientOverride(pt, prefix, props, "reconnect.backoff.max.ms");
        copyKafkaClientOverride(pt, prefix, props, "delivery.timeout.ms");
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

    private static String firstNotBlank(String... values) {
        if (values == null) {
            return null;
        }
        for (String value : values) {
            if (value != null && !value.isBlank()) {
                return value.trim();
            }
        }
        return null;
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
                maskConnectionUrl(artemisBrokerUrl),
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
            throw new IllegalStateException("Invalid JSON in processing audit", e);
        }
    }
}
