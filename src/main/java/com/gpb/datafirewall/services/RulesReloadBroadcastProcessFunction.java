package com.gpb.datafirewall.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.gpb.datafirewall.dto.ProcessingResult;
import com.gpb.datafirewall.kafka.CacheUpdateEvent;
import com.gpb.datafirewall.vault.dto.VaultSecretsDto;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class RulesReloadBroadcastProcessFunction
        extends BroadcastProcessFunction<MessageRecord, CacheUpdateEvent, ProcessingResult> {

    private static final Logger log =
            LoggerFactory.getLogger(RulesReloadBroadcastProcessFunction.class);

    private static final String CACHE_HANDLER = "handler";

    /**
     * Отдельный выходной поток только для результата внутренней Flink-обработки.
     *
     * handler=flink:
     * - основной output: Flink result -> MQ/Artemis;
     * - side output: тот же Flink result -> Kafka audit.
     *
     * handler=dotnet:
     * - основной output: внешний result -> MQ/Artemis;
     * - side output: внутренний Flink result -> Kafka audit.
     */
    public static final OutputTag<ProcessingResult> FLINK_AUDIT_RESULT_TAG =
            new OutputTag<ProcessingResult>("flink-audit-result") {
            };

    private final MapStateDescriptor<String, CacheUpdateEvent> rulesBroadcastDesc;
    private final String jwt;
    private final VaultSecretsDto vaultSecrets;

    private transient ObjectMapper mapper;
    private transient RulesCacheRuntime cacheRuntime;
    private transient MessageProcessingService messageProcessor;
    private transient DotnetHandlerClient dotnetHandlerClient;
    private transient HandlerRoutingState handlerRoutingState;

    public RulesReloadBroadcastProcessFunction(
            MapStateDescriptor<String, CacheUpdateEvent> rulesBroadcastDesc,
            String jwt
    ) {
        this(rulesBroadcastDesc, jwt, null);
    }

    public RulesReloadBroadcastProcessFunction(
            MapStateDescriptor<String, CacheUpdateEvent> rulesBroadcastDesc,
            String jwt,
            VaultSecretsDto vaultSecrets
    ) {
        this.rulesBroadcastDesc = rulesBroadcastDesc;
        this.jwt = jwt;
        this.vaultSecrets = vaultSecrets;
    }

    @Override
    public void open(Configuration parameters) {
        RuntimeContext rc = getRuntimeContext();

        ExecutionConfig.GlobalJobParameters globalParams =
                rc.getExecutionConfig().getGlobalJobParameters();

        ParameterTool pt = globalParams == null
                ? ParameterTool.fromMap(Map.of())
                : ParameterTool.fromMap(globalParams.toMap());

        boolean logPayloads = pt.getBoolean("log.payloads", false);

        DynamicHandler defaultHandler = DynamicHandler.from(
                pt.get("handler", pt.get("handler.default", "flink")),
                DynamicHandler.FLINK
        );

        this.handlerRoutingState = new HandlerRoutingState(defaultHandler);

        this.mapper = new ObjectMapper();

        this.cacheRuntime = new RulesCacheRuntime(mapper, jwt);
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
                pt.get("handler.dotnet.jwt", null),
                vaultDotnetJwt
        );

        String dotnetTrustStorePath =
                pt.get("handler.dotnet.ssl.truststore.location");

        String dotnetTrustStorePassword = firstNotBlank(
                pt.get("handler.dotnet.ssl.truststore.password", null),
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

        log.info(
                "[INIT] subtask={} handler.default={} dotnetUrl={} dotnetTimeoutMs={} "
                        + "dotnetSslVerify={} rulesLoaded={} dataset2controlAreaLoaded={} "
                        + "controlAreaRulesLoaded={} errorMessagesLoaded={} "
                        + "datasetExclusionLoaded={} filterFlagLoaded={}",
                rc.getIndexOfThisSubtask(),
                defaultHandler.value(),
                dotnetUrl,
                dotnetTimeoutMs,
                verifySsl,
                cacheRuntime.rulesSize(),
                cacheRuntime.dataset2ControlAreaSize(),
                cacheRuntime.controlAreaRulesSize(),
                cacheRuntime.errorMessagesSize(),
                cacheRuntime.datasetExclusionSize(),
                cacheRuntime.filterFlagSize()
        );
    }

    @Override
    public void processElement(
            MessageRecord in,
            ReadOnlyContext ctx,
            Collector<ProcessingResult> out
    ) {
        String eventId = in == null ? "unknown" : in.eventId();

        DynamicHandler handler = handlerRoutingState.currentHandler();

        log.info(
                "[ROUTING][eventId={}] selected handler={}",
                eventId,
                handler.value()
        );

        if (handler == DynamicHandler.DOTNET) {
            processWithDotnetHandler(in, ctx, out);
            return;
        }

        processWithFlinkHandler(in, ctx, out);
    }

    /**
     * handler=flink:
     * - внешний сервис не вызывается;
     * - внутренний Flink-результат идет обратно в MQ/Artemis;
     * - этот же результат идет в Kafka audit через side output.
     */
    private void processWithFlinkHandler(
            MessageRecord in,
            ReadOnlyContext ctx,
            Collector<ProcessingResult> out
    ) {
        String eventId = in == null ? "unknown" : in.eventId();

        log.info(
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

            log.info(
                    "[FLINK][eventId={}] internal processing SUCCESS durationMs={}",
                    eventId,
                    durationMs
            );

            // Основной выход: ответ обратно в MQ/Artemis.
            out.collect(flinkResult);

            log.info(
                    "[FLINK][eventId={}] result sent to main output -> MQ/Artemis",
                    eventId
            );

            // Отдельный выход: внутренний результат в Kafka audit.
            ctx.output(FLINK_AUDIT_RESULT_TAG, flinkResult);

            log.info(
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
            ReadOnlyContext ctx,
            Collector<ProcessingResult> out
    ) {
        String eventId = in == null ? "unknown" : in.eventId();

        /*
         * Внешний .NET handler.
         */
        log.info(
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
                log.info(
                        "[DOTNET][eventId={}] external processing SUCCESS durationMs={}",
                        eventId,
                        externalDurationMs
                );

                out.collect(externalResult);

                log.info(
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
        log.info(
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
                log.info(
                        "[FLINK-AUDIT][eventId={}] internal processing SUCCESS durationMs={}",
                        eventId,
                        flinkDurationMs
                );

                ctx.output(
                        FLINK_AUDIT_RESULT_TAG,
                        flinkResult
                );

                log.info(
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
            CacheUpdateEvent ev,
            Context ctx,
            Collector<ProcessingResult> out
    ) throws Exception {

        if (ev == null || !ev.isValid()) {
            return;
        }

        BroadcastState<String, CacheUpdateEvent> st =
                ctx.getBroadcastState(rulesBroadcastDesc);

        CacheUpdateEvent current = st.get(ev.cacheName);

        if (CACHE_HANDLER.equalsIgnoreCase(ev.cacheName)) {
            DynamicHandler newHandler =
                    DynamicHandler.from(ev.handler, null);

            if (newHandler == null) {
                log.warn(
                        "[HANDLER][KAFKA] ignore handler event with invalid handler='{}'",
                        ev.handler
                );
                return;
            }

            DynamicHandler currentHandler =
                    handlerRoutingState.currentHandler();

            if (currentHandler == newHandler) {
                log.info(
                        "[HANDLER][KAFKA] ignore handler event handler={} current={}",
                        newHandler.value(),
                        currentHandler.value()
                );
                return;
            }

            handlerRoutingState.update(newHandler);
            st.put(ev.cacheName, ev);

            log.info(
                    "[HANDLER][KAFKA] handler switched from {} to {} by event",
                    currentHandler.value(),
                    newHandler.value()
            );

            return;
        }

        if (current != null && ev.version <= current.version) {
            log.info(
                    "[CACHE][KAFKA] ignore cacheName={} version={} (current={})",
                    ev.cacheName,
                    ev.version,
                    current.version
            );
            return;
        }

        log.info(
                "[CACHE][KAFKA] new event cacheName={} version={} (prev={}) -> reloading...",
                ev.cacheName,
                ev.version,
                current != null ? current.version : null
        );

        long t0 = System.nanoTime();

        try {
            cacheRuntime.reload(ev);

            long ms =
                    (System.nanoTime() - t0) / 1_000_000L;

            st.put(ev.cacheName, ev);

            log.info(
                    "[CACHE][KAFKA] reload OK cacheName={} version={} in {}ms",
                    ev.cacheName,
                    ev.version,
                    ms
            );

        } catch (Exception ex) {
            long ms =
                    (System.nanoTime() - t0) / 1_000_000L;

            log.error(
                    "[CACHE][KAFKA] reload FAILED cacheName={} version={} "
                            + "after {}ms (keep old snapshot)",
                    ev.cacheName,
                    ev.version,
                    ms,
                    ex
            );
        }
    }

    @Override
    public void close() {
        if (cacheRuntime != null) {
            cacheRuntime.close();
        }
    }
}