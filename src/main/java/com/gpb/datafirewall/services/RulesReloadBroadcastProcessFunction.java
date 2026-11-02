package com.gpb.datafirewall.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.gpb.datafirewall.bundle.CacheBundleState;
import com.gpb.datafirewall.dto.ProcessingResult;
import com.gpb.datafirewall.vault.dto.VaultSecretsDto;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.function.Consumer;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Map;

/** Consumers never call the cache API. Only full verified epochs are activated. */
public final class RulesReloadBroadcastProcessFunction
        extends BroadcastProcessFunction<MessageRecord, CacheBundleState, ProcessingResult>
        implements CheckpointedFunction {
    private static final Logger log = LoggerFactory.getLogger(RulesReloadBroadcastProcessFunction.class);
    private static final String STATE_KEY = "active";
    public static final OutputTag<ProcessingResult> FLINK_AUDIT_RESULT_TAG =
            new OutputTag<ProcessingResult>("flink-audit-result") {};

    private final MapStateDescriptor<String, CacheBundleState> rulesBroadcastDesc;
    private final String jwt;
    private final VaultSecretsDto vaultSecrets;
    private transient ObjectMapper mapper;
    private transient RulesCacheRuntime cacheRuntime;
    private transient MessageProcessingService messageProcessor;
    private transient DotnetHandlerClient dotnetHandlerClient;
    private transient HandlerRoutingState handlerRoutingState;
    private transient Deque<MessageRecord> startupPending;
    private transient ListState<byte[]> startupPendingState;
    private transient int maxStartupPending;

    public RulesReloadBroadcastProcessFunction(MapStateDescriptor<String,CacheBundleState> descriptor,
                                               String jwt,VaultSecretsDto secrets) {
        this.rulesBroadcastDesc=descriptor;this.jwt=jwt;this.vaultSecrets=secrets;
    }
    @Override public void open(
            Configuration parameters) {

        ExecutionConfig.GlobalJobParameters global =
                getRuntimeContext()
                        .getExecutionConfig()
                        .getGlobalJobParameters();

        ParameterTool pt =
                global == null 
                        ? ParameterTool.fromMap(Map.of()) 
                        : ParameterTool.fromMap(global.toMap());

        boolean logPayloads = 
                pt.getBoolean(
                        "log.payloads",
                        false
                );

        maxStartupPending = Math.max(
                1,
                pt.getInt(
                        "cache.startup.max-pending-messages",
                        10000
                )
        );

        if (startupPending == null) startupPending = new ArrayDeque<>();
        
        DynamicHandler defaultHandler = 
                DynamicHandler.from(
                        pt.get(
                                "handler",
                                pt.get(
                                        "handler.default",
                                        "flink"
                                )
                        ),
                        DynamicHandler.FLINK
                );

        this.handlerRoutingState = new HandlerRoutingState(defaultHandler);
        this.mapper = new ObjectMapper();
        this.cacheRuntime = new RulesCacheRuntime(mapper,jwt);

        this.cacheRuntime.open(pt);

        this.messageProcessor = new MessageProcessingService(
                mapper,
                cacheRuntime,
                logPayloads
        );

        String dotnetUrl = pt.get("handler.dotnet.url");

        long dotnetTimeoutMs = pt.getLong(
                "handler.dotnet.timeout.ms",
                20_000L
        );

        String vaultDotnetJwt =
                vaultSecrets == null ? null : vaultSecrets.dotnetJwt();

        String vaultTruststorePassword =
                vaultSecrets == null ? null : vaultSecrets.truststorePassword();

        String dotnetJwt = firstNotBlank(
                pt.get(
                        "handler.dotnet.jwt", 
                        null
                ),
                vaultDotnetJwt
        );

        String dotnetTrustStorePath =
                pt.get("handler.dotnet.ssl.truststore.location");

        String dotnetTrustStorePassword = firstNotBlank(
                pt.get(
                        "handler.dotnet.ssl.truststore.password", 
                        null
                ),
                vaultTruststorePassword
        );

        String dotnetTrustStoreType =
                pt.get("handler.dotnet.ssl.truststore.type");

        boolean verifySsl =
                pt.getBoolean("handler.dotnet.ssl.verify", true);

        this.dotnetHandlerClient = new DotnetHandlerClient(
                dotnetUrl,
                dotnetJwt,
                dotnetTimeoutMs,
                mapper,
                dotnetTrustStorePath,
                dotnetTrustStorePassword,
                dotnetTrustStoreType,
                verifySsl
        );


        log.info("[CACHE] processor subtask={} ready for broadcast, no API call during open",
                getRuntimeContext().getIndexOfThisSubtask());
    }

    private void activate(
            CacheBundleState bundle) {

        bundle.verify();
        
        long currentEpoch = 
                cacheRuntime
                        .snapshot()
                        .cacheEpoch();

        if (bundle.cacheEpoch() <= currentEpoch) return;

        // Prepare does not touch active state; exceptions leave old snapshot intact.
        RulesCacheSnapshot prepared = cacheRuntime.prepare(bundle);

        cacheRuntime.activate(prepared);

        handlerRoutingState.update(
                DynamicHandler.from(
                        bundle.handler(),
                        DynamicHandler.FLINK
                )
        );

        log.info(
                "[CACHE] subtask={} activated epoch={} rules={} compiledVersion={} politicsVersion={}",
                getRuntimeContext().getIndexOfThisSubtask(), 
                prepared.cacheEpoch(),
                prepared.compiledRules().size(),
                prepared.compiledRulesVersion(),
                prepared.politicsVersion()
        );
    }

    @Override
    public void processElement(
            MessageRecord in, 
            ReadOnlyContext ctx,
            Collector<ProcessingResult> out) throws Exception {
        CacheBundleState bundle =
                ctx.getBroadcastState(rulesBroadcastDesc).get(STATE_KEY);

        if (bundle == null) {
            // Prevent empty-rules execution when MQ and bundle inputs race at cold start.
            if (startupPending.size() >= maxStartupPending)
                    throw new IllegalStateException("Cache startup message buffer full: " + maxStartupPending);

            startupPending.addLast(in);
            return;
        }

        if (bundle.cacheEpoch() > cacheRuntime.snapshot().cacheEpoch()) activate(bundle);

        drainPending(result->ctx.output(FLINK_AUDIT_RESULT_TAG,result),out);

        processReady(in,result->ctx.output(FLINK_AUDIT_RESULT_TAG,result),out);
    }

    private void drainPending(
            Consumer<ProcessingResult> audit,
            Collector<ProcessingResult> out) {

        if (startupPending == null) return;

        while (!startupPending.isEmpty()) {
            processReady(
                    startupPending.peekFirst(),
                    audit,
                    out
            );
            startupPending.removeFirst();
        }
    }

    private void processReady(
            MessageRecord in, 
            Consumer<ProcessingResult> audit,
            Collector<ProcessingResult> out) {

        String eventId = in == null ? "unknown" : in.eventId();
        
        DynamicHandler handler = handlerRoutingState.currentHandler();

        log.debug(
                "[ROUTING][eventId={}] selected handler={}",
                eventId,
                handler.value()
        );

        if(handler == DynamicHandler.DOTNET) {
            processWithDotnetHandler(in,eventId,audit,out);
        } else {
            processWithFlinkHandler(in,eventId,audit,out);
        }
    }

    /**
     * handler=flink:
     * - внешний сервис не вызывается;
     * - внутренний Flink-результат идет обратно в MQ/Artemis;
     * - этот же результат идет в Kafka audit через side output.
     */
    private void processWithFlinkHandler(
            MessageRecord in,
            String eventId,
            Consumer<ProcessingResult> audit,
            Collector<ProcessingResult> out
    ) {
        log.debug(
                "[FLINK][eventId={}] internal processing START",
                eventId
        );

        long startedAtNs = System.nanoTime();

        try {
            ProcessingResult flinkResult = messageProcessor.process(in);

            long durationMs =
                    (System.nanoTime() - startedAtNs) / 1_000_000L;

            if (flinkResult == null) {
                log.warn(
                        "[FLINK][eventId={}] internal handler returned null durationMs={}",
                        eventId,
                        durationMs
                );
                return;
            }

            log.debug(
                    "[FLINK][eventId={}] internal processing SUCCESS durationMs={}",
                    eventId,
                    durationMs
            );

            // Основной выход: ответ обратно в MQ/Artemis.
            out.collect(flinkResult);

            log.debug(
                    "[FLINK][eventId={}] result sent to main output -> MQ/Artemis",
                    eventId
            );

            // Отдельный выход: внутренний результат в Kafka audit.
            audit.accept(flinkResult);

            log.debug(
                    "[FLINK][eventId={}] result sent to Kafka audit side output",
                    eventId
            );

        } catch (Exception e) {
            long durationMs =
                    (System.nanoTime() - startedAtNs) / 1_000_000L;

            log.error(
                    "[FLINK][eventId={}] internal processing FAILED durationMs={}",
                    eventId,
                    durationMs,
                    e
            );
        }
    }

    /**
     * handler=dotnet:
     * - внешний результат идет обратно в MQ/Artemis;
     * - внутренний Flink handler тоже выполняется;
     * - внутренний Flink-результат идет только в Kafka audit.
     */
    private void processWithDotnetHandler(
            MessageRecord in,
            String eventId,
            Consumer<ProcessingResult> audit,
            Collector<ProcessingResult> out
    ) {
        /*
         * Внешний .NET handler.
         */
        log.debug(
                "[DOTNET][eventId={}] external processing START",
                eventId
        );

        long externalStartedAtNs = System.nanoTime();

        try {
            ProcessingResult externalResult =
                    dotnetHandlerClient.process(in);

            long externalDurationMs =
                    (System.nanoTime() - externalStartedAtNs) / 1_000_000L;

            if (externalResult != null) {
                log.debug(
                        "[DOTNET][eventId={}] external processing SUCCESS durationMs={}",
                        eventId,
                        externalDurationMs
                );

                out.collect(externalResult);

                log.debug(
                        "[DOTNET][eventId={}] external result sent to main output -> MQ/Artemis",
                        eventId
                );
            } else {
                log.warn(
                        "[DOTNET][eventId={}] external handler returned null durationMs={}",
                        eventId,
                        externalDurationMs
                );
            }

        } catch (Exception e) {
            long externalDurationMs =
                    (System.nanoTime() - externalStartedAtNs) / 1_000_000L;

            log.error(
                    "[DOTNET][eventId={}] external processing FAILED durationMs={}",
                    eventId,
                    externalDurationMs,
                    e
            );
        }

        /*
         * Внутренняя Flink-обработка выполняется независимо
         * от результата внешнего .NET handler.
         */
        log.debug(
                "[FLINK-AUDIT][eventId={}] internal processing START",
                eventId
        );

        long flinkStartedAtNs = System.nanoTime();

        try {
            ProcessingResult flinkResult =
                    messageProcessor.process(in);

            long flinkDurationMs =
                    (System.nanoTime() - flinkStartedAtNs) / 1_000_000L;

            if (flinkResult != null) {
                log.debug(
                        "[FLINK-AUDIT][eventId={}] internal processing SUCCESS durationMs={}",
                        eventId,
                        flinkDurationMs
                );

                audit.accept(flinkResult);

                log.debug(
                        "[FLINK-AUDIT][eventId={}] result sent to Kafka audit side output",
                        eventId
                );

            } else {
                log.warn(
                        "[FLINK-AUDIT][eventId={}] internal handler returned null durationMs={}",
                        eventId,
                        flinkDurationMs
                );
            }

        } catch (Exception e) {
            long flinkDurationMs =
                    (System.nanoTime() - flinkStartedAtNs) / 1_000_000L;

            log.error(
                    "[FLINK-AUDIT][eventId={}] internal processing FAILED durationMs={}",
                    eventId,
                    flinkDurationMs,
                    e
            );
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

    @Override
    public void processBroadcastElement(
            CacheBundleState bundle, 
            Context ctx,
            Collector<ProcessingResult> out) throws Exception {

        if (bundle==null) return;

        BroadcastState<String,CacheBundleState> state =
                ctx.getBroadcastState(rulesBroadcastDesc);

        CacheBundleState prior = state.get(STATE_KEY);

        if (prior != null && bundle.cacheEpoch() <= prior.cacheEpoch()) {
            // After restore, broadcast state may already contain this epoch,
            // while the transient local ClassLoader has not been rebuilt.
            activate(prior);
            drainPending(
                    result -> ctx.output(
                            FLINK_AUDIT_RESULT_TAG,
                            result
                    ),
                    out
            );
            return;
        }
        activate(bundle);
        state.put(STATE_KEY,bundle);
        drainPending(
                result -> ctx.output(
                        FLINK_AUDIT_RESULT_TAG,
                        result
                ),
                out
        );
    }

    @Override public void initializeState(
            FunctionInitializationContext context)throws Exception {

        startupPendingState = 
                context
                        .getOperatorStateStore()
                        .getListState(
                                new ListStateDescriptor<>(
                                        "cache-startup-pending-v1",
                                        BytePrimitiveArraySerializer.INSTANCE
                                )
                        );
                        
        startupPending = new ArrayDeque<>();

        if (context.isRestored()) for (byte[] bytes : startupPendingState.get())
            startupPending.addLast(
                    MessageRecordCodec.decode(bytes)
            );
    }

    @Override public void snapshotState(
            FunctionSnapshotContext context)throws Exception {

        java.util.List<byte[]> values = 
                new java.util.ArrayList<>(startupPending.size());

        for (MessageRecord in : startupPending) values.add(MessageRecordCodec.encode(in));

        startupPendingState.update(values);
    }

    @Override public void close() throws Exception {
        if (cacheRuntime != null) cacheRuntime.close();
        super.close();
    }
}
