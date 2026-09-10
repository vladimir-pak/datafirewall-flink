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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

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
    private transient ScheduledExecutorService cacheRetryExecutor;
    private transient AtomicReference<CacheUpdateEvent> pendingCompiledRules;
    private transient AtomicReference<CacheUpdateEvent> pendingPolitics;
    private transient AtomicBoolean compiledRulesRetryActive;
    private transient AtomicBoolean politicsRetryActive;
    private transient AtomicLong appliedCompiledRulesVersion;
    private transient AtomicLong appliedPoliticsVersion;
    private transient Object cacheReloadLock;
    private transient long cacheRetryInitialDelayMs;
    private transient long cacheRetryMaxDelayMs;

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
        this.pendingCompiledRules = new AtomicReference<>();
        this.pendingPolitics = new AtomicReference<>();
        this.compiledRulesRetryActive = new AtomicBoolean(false);
        this.politicsRetryActive = new AtomicBoolean(false);
        this.appliedCompiledRulesVersion = new AtomicLong(0L);
        this.appliedPoliticsVersion = new AtomicLong(0L);
        this.cacheReloadLock = new Object();
        this.cacheRetryInitialDelayMs = Math.max(1_000L, pt.getLong("cache.reload.retry.initial.ms", 5_000L));
        this.cacheRetryMaxDelayMs = Math.max(cacheRetryInitialDelayMs, pt.getLong("cache.reload.retry.max.ms", 60_000L));
        this.cacheRetryExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread thread = new Thread(r, "cache-spring-retry-" + rc.getIndexOfThisSubtask());
            thread.setDaemon(true);
            return thread;
        });
        log.info("[CACHE][SPRING] background retry initialized subtask={} initialDelayMs={} maxDelayMs={}", rc.getIndexOfThisSubtask(), cacheRetryInitialDelayMs, cacheRetryMaxDelayMs);

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

        log.info(
                "[INIT-DIAG] subtask={} rulesSnapshot.size={} diagnosticsEnabled=true",
                rc.getIndexOfThisSubtask(),
                cacheRuntime.rulesSnapshot() == null ? 0 : cacheRuntime.rulesSnapshot().size()
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
    public void processBroadcastElement(CacheUpdateEvent ev, Context ctx, Collector<ProcessingResult> out) throws Exception {
        if (ev == null || !ev.isValid()) {
            return;
        }
        BroadcastState<String, CacheUpdateEvent> st = ctx.getBroadcastState(rulesBroadcastDesc);
        CacheUpdateEvent current = st.get(ev.cacheName);
        if (CACHE_HANDLER.equalsIgnoreCase(ev.cacheName)) {
            DynamicHandler newHandler = DynamicHandler.from(ev.handler, null);
            if (newHandler == null) {
                log.warn("[HANDLER][KAFKA] ignore handler event with invalid handler='{}'", ev.handler);
                return;
            }
            DynamicHandler currentHandler = handlerRoutingState.currentHandler();
            if (currentHandler == newHandler) {
                log.info("[HANDLER][KAFKA] ignore handler event handler={} current={}", newHandler.value(), currentHandler.value());
                return;
            }
            handlerRoutingState.update(newHandler);
            st.put(ev.cacheName, ev);
            log.info("[HANDLER][KAFKA] handler switched from {} to {} by event", currentHandler.value(), newHandler.value());
            return;
        }
        long appliedVersion = appliedVersion(ev.cacheName);
        long stateVersion = current == null ? 0L : current.version;
        long effectiveCurrentVersion = Math.max(appliedVersion, stateVersion);
        if (ev.version <= effectiveCurrentVersion) {
            log.info("[CACHE][KAFKA] ignore cacheName={} version={} currentVersion={}", ev.cacheName, ev.version, effectiveCurrentVersion);
            return;
        }
        log.info("[CACHE][KAFKA] new event cacheName={} version={} currentVersion={} -> reloading", ev.cacheName, ev.version, effectiveCurrentVersion);
        long t0 = System.nanoTime();
        try {
            synchronized (cacheReloadLock) {
                long latestApplied = appliedVersion(ev.cacheName);
                if (ev.version <= latestApplied) {
                    log.info("[CACHE][KAFKA] reload skipped cacheName={} version={} alreadyApplied={}", ev.cacheName, ev.version, latestApplied);
                    return;
                }
                cacheRuntime.reload(ev);
                markApplied(ev.cacheName, ev.version);
            }
            long ms = (System.nanoTime() - t0) / 1_000_000L;
            st.put(ev.cacheName, ev);
            clearPendingUpTo(ev.cacheName, ev.version);
            log.info("[CACHE][KAFKA] reload OK cacheName={} version={} in {}ms", ev.cacheName, ev.version, ms);
        } catch (Exception ex) {
            long ms = (System.nanoTime() - t0) / 1_000_000L;
            log.warn("[CACHE][SPRING] reload failed cacheName={} requestedVersion={} currentVersion={} afterMs={} -> keep old snapshot and retry in background. cause={}", ev.cacheName, ev.version, appliedVersion(ev.cacheName), ms, rootMessage(ex));
            registerPendingReload(ev);
        }
    }

    private void registerPendingReload(CacheUpdateEvent ev) {
        AtomicReference<CacheUpdateEvent> pendingRef = pendingReference(ev.cacheName);
        AtomicBoolean activeRef = retryActiveReference(ev.cacheName);
        if (pendingRef == null || activeRef == null) {
            log.warn("[CACHE][SPRING] background retry is not supported for cacheName={}", ev.cacheName);
            return;
        }
        CacheUpdateEvent previous = pendingRef.getAndUpdate(current -> current == null || ev.version > current.version ? ev : current);
        CacheUpdateEvent actual = pendingRef.get();
        if (previous == null) {
            log.warn("[CACHE][SPRING] pending reload registered cacheName={} pendingVersion={} retryInMs={}", ev.cacheName, actual.version, cacheRetryInitialDelayMs);
        } else if (actual.version > previous.version) {
            log.info("[CACHE][KAFKA] newer pending version received cacheName={} oldPendingVersion={} newPendingVersion={}", ev.cacheName, previous.version, actual.version);
        } else {
            log.info("[CACHE][KAFKA] pending version unchanged cacheName={} pendingVersion={} receivedVersion={}", ev.cacheName, actual.version, ev.version);
        }
        if (activeRef.compareAndSet(false, true)) {
            scheduleRetry(ev.cacheName, cacheRetryInitialDelayMs, 1);
        }
    }

    private void scheduleRetry(String cacheName, long delayMs, int attempt) {
        if (cacheRetryExecutor == null || cacheRetryExecutor.isShutdown()) {
            finishRetryLoop(cacheName);
            return;
        }
        cacheRetryExecutor.schedule(() -> retryPending(cacheName, delayMs, attempt), delayMs, TimeUnit.MILLISECONDS);
    }

    private void retryPending(String cacheName, long previousDelayMs, int attempt) {
        AtomicReference<CacheUpdateEvent> pendingRef = pendingReference(cacheName);
        if (pendingRef == null) {
            finishRetryLoop(cacheName);
            return;
        }
        CacheUpdateEvent pending = pendingRef.get();
        if (pending == null) {
            finishRetryLoop(cacheName);
            return;
        }
        long alreadyApplied = appliedVersion(cacheName);
        if (pending.version <= alreadyApplied) {
            pendingRef.compareAndSet(pending, null);
            continueOrFinishRetry(cacheName, cacheRetryInitialDelayMs, 1);
            return;
        }
        log.warn("[CACHE][SPRING] retrying cache reload cacheName={} pendingVersion={} currentVersion={} attempt={}", cacheName, pending.version, alreadyApplied, attempt);
        long t0 = System.nanoTime();
        try {
            synchronized (cacheReloadLock) {
                long latestApplied = appliedVersion(cacheName);
                if (pending.version > latestApplied) {
                    cacheRuntime.reload(pending);
                    markApplied(cacheName, pending.version);
                }
            }
            long ms = (System.nanoTime() - t0) / 1_000_000L;
            pendingRef.compareAndSet(pending, null);
            CacheUpdateEvent newer = pendingRef.get();
            if (newer == null) {
                log.info("[CACHE][SPRING] connection restored cacheName={} loadedVersion={} attempts={} durationMs={}", cacheName, pending.version, attempt, ms);
            } else {
                log.info("[CACHE][SPRING] version {} loaded for cacheName={}, but newer pendingVersion={} exists", pending.version, cacheName, newer.version);
            }
            continueOrFinishRetry(cacheName, cacheRetryInitialDelayMs, 1);
        } catch (Exception ex) {
            long nextDelayMs = nextRetryDelay(previousDelayMs);
            CacheUpdateEvent latest = pendingRef.get();
            long pendingVersion = latest == null ? pending.version : latest.version;
            log.warn("[CACHE][SPRING] Spring cache service unavailable cacheName={} attemptedVersion={} pendingVersion={} currentVersion={} attempt={} retryInMs={} cause={}", cacheName, pending.version, pendingVersion, appliedVersion(cacheName), attempt, nextDelayMs, rootMessage(ex));
            scheduleRetry(cacheName, nextDelayMs, attempt + 1);
        }
    }

    private void continueOrFinishRetry(String cacheName, long delayMs, int attempt) {
        AtomicReference<CacheUpdateEvent> pendingRef = pendingReference(cacheName);
        AtomicBoolean activeRef = retryActiveReference(cacheName);
        if (pendingRef != null && pendingRef.get() != null) {
            scheduleRetry(cacheName, delayMs, attempt);
            return;
        }
        if (activeRef != null) {
            activeRef.set(false);
            if (pendingRef != null && pendingRef.get() != null && activeRef.compareAndSet(false, true)) {
                scheduleRetry(cacheName, delayMs, attempt);
            }
        }
    }

    private void finishRetryLoop(String cacheName) {
        AtomicBoolean activeRef = retryActiveReference(cacheName);
        if (activeRef != null) {
            activeRef.set(false);
        }
    }

    private long nextRetryDelay(long previousDelayMs) {
        long doubled = previousDelayMs >= cacheRetryMaxDelayMs / 2 ? cacheRetryMaxDelayMs : previousDelayMs * 2;
        return Math.min(cacheRetryMaxDelayMs, Math.max(cacheRetryInitialDelayMs, doubled));
    }

    private AtomicReference<CacheUpdateEvent> pendingReference(String cacheName) {
        if (RulesCacheRuntime.CACHE_COMPILED_RULES.equals(cacheName)) {
            return pendingCompiledRules;
        }
        if (RulesCacheRuntime.CACHE_POLITICS.equals(cacheName)) {
            return pendingPolitics;
        }
        return null;
    }

    private AtomicBoolean retryActiveReference(String cacheName) {
        if (RulesCacheRuntime.CACHE_COMPILED_RULES.equals(cacheName)) {
            return compiledRulesRetryActive;
        }
        if (RulesCacheRuntime.CACHE_POLITICS.equals(cacheName)) {
            return politicsRetryActive;
        }
        return null;
    }

    private long appliedVersion(String cacheName) {
        if (RulesCacheRuntime.CACHE_COMPILED_RULES.equals(cacheName)) {
            return appliedCompiledRulesVersion.get();
        }
        if (RulesCacheRuntime.CACHE_POLITICS.equals(cacheName)) {
            return appliedPoliticsVersion.get();
        }
        return 0L;
    }

    private void markApplied(String cacheName, long version) {
        if (RulesCacheRuntime.CACHE_COMPILED_RULES.equals(cacheName)) {
            appliedCompiledRulesVersion.accumulateAndGet(version, Math::max);
        } else if (RulesCacheRuntime.CACHE_POLITICS.equals(cacheName)) {
            appliedPoliticsVersion.accumulateAndGet(version, Math::max);
        }
    }

    private void clearPendingUpTo(String cacheName, long version) {
        AtomicReference<CacheUpdateEvent> pendingRef = pendingReference(cacheName);
        if (pendingRef != null) {
            pendingRef.updateAndGet(current -> current != null && current.version <= version ? null : current);
        }
    }

    private static String rootMessage(Throwable throwable) {
        if (throwable == null) {
            return "unknown";
        }
        Throwable current = throwable;
        while (current.getCause() != null && current.getCause() != current) {
            current = current.getCause();
        }
        String message = current.getMessage();
        return current.getClass().getSimpleName() + (message == null || message.isBlank() ? "" : ": " + message);
    }

    @Override
    public void close() {
        if (cacheRetryExecutor != null) {
            cacheRetryExecutor.shutdownNow();
        }
        if (cacheRuntime != null) {
            cacheRuntime.close();
        }
    }
}