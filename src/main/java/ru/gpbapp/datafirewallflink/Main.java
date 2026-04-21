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
import ru.gpbapp.datafirewallflink.mq.MessageRecord;
import ru.gpbapp.datafirewallflink.mq.MessageReply;
import ru.gpbapp.datafirewallflink.mq.MqSink;
import ru.gpbapp.datafirewallflink.mq.MqSource;
import ru.gpbapp.datafirewallflink.services.RulesReloadBroadcastProcessFunction;

import java.util.ArrayList;
import java.util.List;

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
            long checkpointInterval = pt.getLong(
                    "flink.checkpoint.interval.ms",
                    DEFAULT_CHECKPOINT_INTERVAL_MS
            );
            long checkpointTimeout = pt.getLong(
                    "flink.checkpoint.timeout.ms",
                    DEFAULT_CHECKPOINT_TIMEOUT_MS
            );
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

        JobConfig cfg = JobConfig.fromArgs(pt);

        String backend = pt.get("messaging.backend", DEFAULT_MESSAGING_BACKEND).trim().toLowerCase();
        String kafkaBootstrap = pt.get("kafka.bootstrap", DEFAULT_KAFKA_BOOTSTRAP);
        String kafkaTopic = pt.get("kafka.topic", DEFAULT_KAFKA_TOPIC);
        String kafkaGroup = pt.get("kafka.group", DEFAULT_KAFKA_GROUP);

        String detailKafkaBootstrap = pt.get("detail.kafka.bootstrap", kafkaBootstrap);
        String detailKafkaTopic = pt.get("detail.kafka.topic", DEFAULT_DETAIL_KAFKA_TOPIC);

        String artemisBrokerUrl = pt.get("artemis.broker.url", DEFAULT_ARTEMIS_BROKER_URL);
        String artemisUser = pt.get("artemis.user", "");
        String artemisPassword = pt.get("artemis.password", "");
        String artemisInQueue = pt.get("artemis.in.queue", DEFAULT_ARTEMIS_IN_QUEUE);
        String artemisOutQueue = pt.get("artemis.out.queue", DEFAULT_ARTEMIS_OUT_QUEUE);
        long artemisReceiveTimeoutMs = pt.getLong("artemis.receive.timeout.ms", DEFAULT_ARTEMIS_RECEIVE_TIMEOUT_MS);

        String igniteApiUrl = pt.get("ignite.apiUrl", "http://127.0.0.1:8080");
        String rulesLoader = pt.get("rules.loader", "http");
        boolean cacheBootstrapEnabled = pt.getBoolean("cache.bootstrap.enabled", true);
        boolean politicsBootstrapEnabled = pt.getBoolean("politics.bootstrap.enabled", false);
        boolean logPayloads = pt.getBoolean("log.payloads", false);

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
                cfg
        );

        log.error("[MAIN_MARKER] backend={}, detailKafkaEnabled={}", backend, detailKafkaEnabled);

        final DataStream<MessageRecord> inputStream;

        switch (backend) {
            case "mq" -> {
                log.error("[MAIN_MARKER] INPUT -> MQ");

                inputStream = env.addSource(
                                new MqSource(
                                        cfg.mqHost(),
                                        cfg.mqPort(),
                                        cfg.mqChannel(),
                                        cfg.mqQmgr(),
                                        cfg.mqInQueue(),
                                        cfg.mqUser(),
                                        cfg.mqPassword()
                                ),
                                "mq-source"
                        )
                        .name("mq-source")
                        .setParallelism(1);
            }

            case "artemis" -> {
                log.error("[MAIN_MARKER] INPUT -> ARTEMIS");

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
                        .setParallelism(1);
            }

            default -> throw new IllegalArgumentException(
                    "Unsupported messaging.backend=" + backend + ". Supported: mq|artemis"
            );
        }

        KafkaSource<CacheUpdateEvent> kafkaSource = KafkaSource.<CacheUpdateEvent>builder()
                .setBootstrapServers(kafkaBootstrap)
                .setTopics(kafkaTopic)
                .setGroupId(kafkaGroup)
                .setStartingOffsets(OffsetsInitializer.latest())
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
            log.error("[MAIN_MARKER] SHORT -> MQ");

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
                            cfg.mqPassword()
                    ))
                    .name("mq-sink")
                    .setParallelism(1);

            log.info("[MAIN] MQ output sink enabled");
        } else if ("artemis".equals(backend)) {
            log.error("[MAIN_MARKER] SHORT -> ARTEMIS");

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
                            artemisOutQueue
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

            log.info(
                    "[MAIN] detail kafka sink enabled: bootstrap={}, topic={}",
                    detailKafkaBootstrap,
                    detailKafkaTopic
            );
        } else {
            log.info("[MAIN] detail kafka sink is disabled.");
        }

        env.execute("DataFirewall Messaging Job (SHORT ANSWER -> MQ/ARTEMIS, DETAIL ANSWER -> Kafka) + Kafka Rules Reload");
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
            JobConfig cfg
    ) {
        log.info(
                "[MAIN] startup config: parallelism={}, messaging.backend={}, kafka.bootstrap={}, kafka.topic={}, kafka.group={}, " +
                        "ignite.apiUrl={}, rules.loader={}, cache.bootstrap.enabled={}, politics.bootstrap.enabled={}, " +
                        "log.payloads={}, detail.kafka.enabled={}, detail.kafka.bootstrap={}, detail.kafka.topic={}, " +
                        "checkpointing.enabled={}, artemis.broker.url={}, artemis.in.queue={}, artemis.out.queue={}, " +
                        "mq.host={}, mq.port={}, mq.channel={}, mq.qmgr={}, mq.inQueue={}, mq.outQueue={}, mq.user={}",
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
                cfg.mqHost(),
                cfg.mqPort(),
                cfg.mqChannel(),
                cfg.mqQmgr(),
                cfg.mqInQueue(),
                cfg.mqOutQueue(),
                cfg.mqUser()
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