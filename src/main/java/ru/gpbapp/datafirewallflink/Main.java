package ru.gpbapp.datafirewallflink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.gpbapp.datafirewallflink.config.JobConfig;
import ru.gpbapp.datafirewallflink.dto.ProcessingResult;
import ru.gpbapp.datafirewallflink.kafka.CacheUpdateEvent;
import ru.gpbapp.datafirewallflink.kafka.CacheUpdateEventDeserializationSchema;
import ru.gpbapp.datafirewallflink.mq.MqRecord;
import ru.gpbapp.datafirewallflink.mq.MqReply;
import ru.gpbapp.datafirewallflink.mq.MqSink;
import ru.gpbapp.datafirewallflink.mq.MqSource;
import ru.gpbapp.datafirewallflink.services.MqWithRulesReloadBroadcastProcessFunction;

import java.io.BufferedWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

public class Main {

    private static final Logger log = LoggerFactory.getLogger(Main.class);

    private static final String DEFAULT_KAFKA_BOOTSTRAP = "localhost:9092";
    private static final String DEFAULT_KAFKA_TOPIC = "rules-update";
    private static final String DEFAULT_KAFKA_GROUP = "dfw-rules-group";
    private static final String DEFAULT_DETAIL_KAFKA_TOPIC = "detail-answer";
    private static final int DEFAULT_PARALLELISM = 1;

    private static final long DEFAULT_CHECKPOINT_INTERVAL_MS = 5000L;
    private static final long DEFAULT_CHECKPOINT_TIMEOUT_MS = 60000L;
    private static final long DEFAULT_MIN_PAUSE_BETWEEN_CHECKPOINTS_MS = 1000L;
    private static final int DEFAULT_MAX_CONCURRENT_CHECKPOINTS = 1;

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

        boolean useMq = pt.getBoolean("use.mq", true);
        String kafkaBootstrap = pt.get("kafka.bootstrap", DEFAULT_KAFKA_BOOTSTRAP);
        String kafkaTopic = pt.get("kafka.topic", DEFAULT_KAFKA_TOPIC);
        String kafkaGroup = pt.get("kafka.group", DEFAULT_KAFKA_GROUP);

        String detailKafkaBootstrap = pt.get("detail.kafka.bootstrap", kafkaBootstrap);
        String detailKafkaTopic = pt.get("detail.kafka.topic", DEFAULT_DETAIL_KAFKA_TOPIC);

        String igniteApiUrl = pt.get("ignite.apiUrl", "http://127.0.0.1:8080");
        String rulesLoader = pt.get("rules.loader", "http");
        boolean cacheBootstrapEnabled = pt.getBoolean("cache.bootstrap.enabled", true);
        boolean politicsBootstrapEnabled = pt.getBoolean("politics.bootstrap.enabled", false);
        boolean logPayloads = pt.getBoolean("log.payloads", false);

        String testJsonPath = pt.get("test.json.path", "").trim();
        String shortAnswerFilePath = buildShortAnswerFilePath(testJsonPath);

        logStartupConfig(
                parallelism,
                useMq,
                kafkaBootstrap,
                kafkaTopic,
                kafkaGroup,
                igniteApiUrl,
                rulesLoader,
                cacheBootstrapEnabled,
                politicsBootstrapEnabled,
                logPayloads,
                testJsonPath,
                shortAnswerFilePath,
                detailKafkaEnabled,
                detailKafkaBootstrap,
                detailKafkaTopic,
                checkpointingEnabled,
                cfg
        );

        log.error("[MAIN_MARKER] use.mq={}, shortAnswerFilePath={}", useMq, shortAnswerFilePath);

        final DataStream<MqRecord> mqStream;

        if (useMq) {
            log.error("[MAIN_MARKER] INPUT -> MQ");

            mqStream = env.addSource(
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
        } else {
            log.error("[MAIN_MARKER] INPUT -> FILE");

            if (testJsonPath.isBlank()) {
                throw new IllegalArgumentException(
                        "For --use.mq=false you must provide --test.json.path=/path/to/file.json"
                );
            }

            String testJson = readJsonFile(testJsonPath);

            log.warn(
                    "[MAIN] TEST MODE: MQ is fully disabled. Using JSON from file instead of MQ. path={}",
                    testJsonPath
            );

            prepareOutputFile(shortAnswerFilePath);

            mqStream = env.fromElements(new MqRecord(new byte[0], testJson))
                    .name("test-json-source")
                    .setParallelism(1);
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

        DataStream<ProcessingResult> processed = mqStream
                .connect(bcUpdates)
                .process(new MqWithRulesReloadBroadcastProcessFunction(bcDesc))
                .name("mq-process-with-rules-reload")
                .setParallelism(1);

        if (useMq) {
            log.error("[MAIN_MARKER] SHORT -> MQ");

            DataStream<MqReply> shortReplies = processed
                    .map(new MapFunction<ProcessingResult, MqReply>() {
                        @Override
                        public MqReply map(ProcessingResult result) {
                            if (result == null || result.getShortJson() == null) {
                                return null;
                            }
                            return new MqReply(result.getCorrelId(), result.getShortJson());
                        }
                    })
                    .returns(Types.POJO(MqReply.class))
                    .name("build-short-reply-for-mq")
                    .setParallelism(1)
                    .filter(new FilterFunction<MqReply>() {
                        @Override
                        public boolean filter(MqReply value) {
                            return value != null;
                        }
                    })
                    .name("filter-short-reply-non-null")
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
        } else {
            log.error("[MAIN_MARKER] SHORT -> FILE");

            DataStream<String> shortRepliesForFile = processed
                    .map(new MapFunction<ProcessingResult, String>() {
                        @Override
                        public String map(ProcessingResult result) {
                            return result == null ? null : result.getShortJson();
                        }
                    })
                    .returns(Types.STRING)
                    .name("extract-short-json-for-file")
                    .setParallelism(1)
                    .filter(new FilterFunction<String>() {
                        @Override
                        public boolean filter(String value) {
                            return value != null;
                        }
                    })
                    .name("filter-short-json-for-file")
                    .setParallelism(1);

            shortRepliesForFile
                    .addSink(new LocalFileAppendSink(shortAnswerFilePath))
                    .name("short-file-sink")
                    .setParallelism(1);

            log.warn("[MAIN] MQ output sink disabled because use.mq=false");
            log.info("[MAIN] Short answers will be written to file: {}", shortAnswerFilePath);
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

        env.execute("DataFirewall IBM MQ Job (SHORT ANSWER -> MQ or FILE, DETAIL ANSWER -> Kafka) + Kafka Rules Reload");
    }

    private static void logStartupConfig(
            int parallelism,
            boolean useMq,
            String kafkaBootstrap,
            String kafkaTopic,
            String kafkaGroup,
            String igniteApiUrl,
            String rulesLoader,
            boolean cacheBootstrapEnabled,
            boolean politicsBootstrapEnabled,
            boolean logPayloads,
            String testJsonPath,
            String shortAnswerFilePath,
            boolean detailKafkaEnabled,
            String detailKafkaBootstrap,
            String detailKafkaTopic,
            boolean checkpointingEnabled,
            JobConfig cfg
    ) {
        log.info(
                "[MAIN] startup config: parallelism={}, use.mq={}, kafka.bootstrap={}, kafka.topic={}, kafka.group={}, " +
                        "ignite.apiUrl={}, rules.loader={}, cache.bootstrap.enabled={}, politics.bootstrap.enabled={}, " +
                        "log.payloads={}, test.json.path={}, short.answer.file.path={}, detail.kafka.enabled={}, detail.kafka.bootstrap={}, detail.kafka.topic={}, " +
                        "checkpointing.enabled={}, mq.host={}, mq.port={}, mq.channel={}, mq.qmgr={}, mq.inQueue={}, mq.outQueue={}, mq.user={}",
                parallelism,
                useMq,
                kafkaBootstrap,
                kafkaTopic,
                kafkaGroup,
                igniteApiUrl,
                rulesLoader,
                cacheBootstrapEnabled,
                politicsBootstrapEnabled,
                logPayloads,
                testJsonPath.isBlank() ? "<empty>" : testJsonPath,
                shortAnswerFilePath == null || shortAnswerFilePath.isBlank() ? "<empty>" : shortAnswerFilePath,
                detailKafkaEnabled,
                detailKafkaBootstrap,
                detailKafkaTopic,
                checkpointingEnabled,
                cfg.mqHost(),
                cfg.mqPort(),
                cfg.mqChannel(),
                cfg.mqQmgr(),
                cfg.mqInQueue(),
                cfg.mqOutQueue(),
                cfg.mqUser()
        );
    }

    private static String readJsonFile(String filePath) {
        try {
            Path path = Path.of(filePath);

            if (!Files.exists(path)) {
                throw new IllegalArgumentException("Test JSON file does not exist: " + filePath);
            }
            if (!Files.isRegularFile(path)) {
                throw new IllegalArgumentException("Test JSON path is not a file: " + filePath);
            }

            String json = Files.readString(path, StandardCharsets.UTF_8);
            if (json == null || json.isBlank()) {
                throw new IllegalArgumentException("Test JSON file is empty: " + filePath);
            }

            return json;
        } catch (Exception e) {
            throw new RuntimeException("Failed to read test JSON file: " + filePath, e);
        }
    }

    private static String buildShortAnswerFilePath(String testJsonPath) {
        if (testJsonPath == null || testJsonPath.isBlank()) {
            return "";
        }

        Path inputPath = Path.of(testJsonPath);
        String fileName = inputPath.getFileName().toString();

        String outputFileName;
        int dotIndex = fileName.lastIndexOf('.');
        if (dotIndex > 0) {
            outputFileName = fileName.substring(0, dotIndex) + "_short_answer" + fileName.substring(dotIndex);
        } else {
            outputFileName = fileName + "_short_answer.json";
        }

        Path parent = inputPath.getParent();
        if (parent == null) {
            return outputFileName;
        }

        return parent.resolve(outputFileName).toString();
    }

    private static void prepareOutputFile(String filePath) {
        if (filePath == null || filePath.isBlank()) {
            return;
        }

        try {
            Path path = Path.of(filePath);
            Path parent = path.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }

            Files.writeString(
                    path,
                    "",
                    StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING
            );
        } catch (Exception e) {
            throw new RuntimeException("Failed to prepare short answer output file: " + filePath, e);
        }
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

    public static final class LocalFileAppendSink extends RichSinkFunction<String> {

        private static final Logger log = LoggerFactory.getLogger(LocalFileAppendSink.class);

        private final String filePath;
        private transient BufferedWriter writer;

        public LocalFileAppendSink(String filePath) {
            this.filePath = filePath;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            Path path = Path.of(filePath);
            Path parent = path.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }

            writer = Files.newBufferedWriter(
                    path,
                    StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.APPEND
            );

            log.info("[FILE] opened short answer output file: {}", filePath);
        }

        @Override
        public void invoke(String value, Context context) throws Exception {
            if (value == null) {
                return;
            }

            log.info("[FILE] writing short answer to {}", filePath);
            writer.write(value);
            writer.newLine();
            writer.flush();
        }

        @Override
        public void close() throws Exception {
            if (writer != null) {
                writer.flush();
                writer.close();
            }
        }
    }
}