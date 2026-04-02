package ru.gpbapp.datafirewallflink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.gpbapp.datafirewallflink.config.JobConfig;
import ru.gpbapp.datafirewallflink.kafka.CacheUpdateEvent;
import ru.gpbapp.datafirewallflink.kafka.CacheUpdateEventDeserializationSchema;
import ru.gpbapp.datafirewallflink.mq.MqRecord;
import ru.gpbapp.datafirewallflink.mq.MqReply;
import ru.gpbapp.datafirewallflink.mq.MqSink;
import ru.gpbapp.datafirewallflink.mq.MqSource;
import ru.gpbapp.datafirewallflink.services.MqWithRulesReloadBroadcastProcessFunction;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class Main {

    private static final Logger log = LoggerFactory.getLogger(Main.class);

    private static final String DEFAULT_KAFKA_BOOTSTRAP = "localhost:9092";
    private static final String DEFAULT_KAFKA_TOPIC = "rules-update";
    private static final String DEFAULT_KAFKA_GROUP = "dfw-rules-group";
    private static final int DEFAULT_PARALLELISM = 1;

    public static void main(String[] args) throws Exception {
        String[] normalized = normalizeArgs(args);
        ParameterTool pt = ParameterTool.fromArgs(normalized);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(pt);

        int parallelism = pt.getInt("parallelism.source", DEFAULT_PARALLELISM);
        env.setParallelism(parallelism);

        JobConfig cfg = JobConfig.fromArgs(pt);

        boolean useMq = pt.getBoolean("use.mq", true);
        String kafkaBootstrap = pt.get("kafka.bootstrap", DEFAULT_KAFKA_BOOTSTRAP);
        String kafkaTopic = pt.get("kafka.topic", DEFAULT_KAFKA_TOPIC);
        String kafkaGroup = pt.get("kafka.group", DEFAULT_KAFKA_GROUP);

        String igniteApiUrl = pt.get("ignite.apiUrl", "http://127.0.0.1:8080");
        String rulesLoader = pt.get("rules.loader", "http");
        boolean cacheBootstrapEnabled = pt.getBoolean("cache.bootstrap.enabled", true);
        boolean politicsBootstrapEnabled = pt.getBoolean("politics.bootstrap.enabled", false);
        boolean logPayloads = pt.getBoolean("log.payloads", false);

        String testJsonPath = pt.get("test.json.path", "").trim();

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
                cfg
        );

        final DataStream<MqRecord> mqStream;

        if (useMq) {
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
            if (testJsonPath.isBlank()) {
                throw new IllegalArgumentException(
                        "For --use.mq=false you must provide --test.json.path=/path/to/file.json"
                );
            }

            String testJson = readJsonFile(testJsonPath);

            log.warn(
                    "TEST MODE: using JSON from file instead of MQ. path={}. Set --use.mq=true to read from MQ.",
                    testJsonPath
            );

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

        DataStream<MqReply> processed = mqStream
                .connect(bcUpdates)
                .process(new MqWithRulesReloadBroadcastProcessFunction(bcDesc))
                .name("mq-process-with-rules-reload")
                .setParallelism(1);

        processed
                .filter(Objects::nonNull)
                .name("filter-non-null")
                .setParallelism(1)
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

        env.execute("DataFirewall IBM MQ Job (SHORT ANSWER) + Kafka Rules Reload");
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
            JobConfig cfg
    ) {
        log.info(
                "[MAIN] startup config: parallelism={}, use.mq={}, kafka.bootstrap={}, kafka.topic={}, kafka.group={}, " +
                        "ignite.apiUrl={}, rules.loader={}, cache.bootstrap.enabled={}, politics.bootstrap.enabled={}, " +
                        "log.payloads={}, test.json.path={}, mq.host={}, mq.port={}, mq.channel={}, mq.qmgr={}, mq.inQueue={}, mq.outQueue={}, mq.user={}",
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