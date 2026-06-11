package com.gpb.datafirewall.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.gpb.datafirewall.dto.ProcessingResult;
import com.gpb.datafirewall.kafka.CacheUpdateEvent;

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

    public static final OutputTag<String> DOTNET_SHADOW_REQUEST_TAG =
            new OutputTag<String>("dotnet-shadow-request") {
            };

    private final MapStateDescriptor<String, CacheUpdateEvent> rulesBroadcastDesc;
    private final String jwt;
    private final String dotnetJwt;

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
            String dotnetJwt
    ) {
        this.rulesBroadcastDesc = rulesBroadcastDesc;
        this.jwt = jwt;
        this.dotnetJwt = dotnetJwt;
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

        this.dotnetHandlerClient = new DotnetHandlerClient(
                pt.get("handler.dotnet.url", pt.get("dotnet.handler.url", null)),
                firstNotBlank(pt.get("handler.dotnet.jwt", null), dotnetJwt),
                pt.getLong("handler.dotnet.timeout.ms", 20_000L),
                mapper
        );

        log.info(
                "[INIT] subtask={} handler.default={} rulesLoaded={} dataset2controlAreaLoaded={} controlAreaRulesLoaded={} errorMessagesLoaded={} datasetExclusionLoaded={} filterFlagLoaded={}",
                rc.getIndexOfThisSubtask(),
                defaultHandler.value(),
                cacheRuntime.rulesSize(),
                cacheRuntime.dataset2ControlAreaSize(),
                cacheRuntime.controlAreaRulesSize(),
                cacheRuntime.errorMessagesSize(),
                cacheRuntime.datasetExclusionSize(),
                cacheRuntime.filterFlagSize()
        );
    }

    @Override
    public void processElement(MessageRecord in, ReadOnlyContext ctx, Collector<ProcessingResult> out) {
        DynamicHandler handler = handlerRoutingState.currentHandler();

        try {
            if (handler == DynamicHandler.DOTNET) {
                ProcessingResult result = dotnetHandlerClient.process(in);
                if (result != null) {
                    out.collect(result);
                }
                emitDotnetShadowRequest(in, ctx);
                return;
            }

            ProcessingResult result = messageProcessor.process(in);
            if (result != null) {
                out.collect(result);
            }
        } catch (Exception e) {
            String eventId = in == null ? "unknown" : in.eventId();
            log.error("[PIPE][eventId={}] failed to process message. handler={}", eventId, handler.value(), e);
        }
    }

    private void emitDotnetShadowRequest(MessageRecord in, ReadOnlyContext ctx) {
        if (in == null || in.payload == null || in.payload.isBlank()) {
            return;
        }

        ctx.output(DOTNET_SHADOW_REQUEST_TAG, in.payload);
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
            if (current != null && current.handler != null && current.handler.equalsIgnoreCase(ev.handler)) {
                log.info("[HANDLER][KAFKA] ignore handler event hadnler={} current={}",
                        ev.handler, current.handler);
                return;
            }

            DynamicHandler newHandler = DynamicHandler.from(ev.handler, null);
            if (newHandler == null) {
                log.warn("[HANDLER][KAFKA] ignore handler event with invalid handler='{}'",
                        ev.handler);
                return;
            }

            DynamicHandler currentHandler = handlerRoutingState.currentHandler();

            handlerRoutingState.update(newHandler);
            st.put(ev.cacheName, ev);

            log.info("[HANDLER][KAFKA] handler switched from {} to {} by event",
                    currentHandler.value(), newHandler.value());
            return;
        }

        if (current != null && ev.version <= current.version) {
            log.info("[CACHE][KAFKA] ignore cacheName={} version={} (current={})",
                    ev.cacheName, ev.version, current.version);
            return;
        }

        log.info("[CACHE][KAFKA] new event cacheName={} version={} (prev={}) -> reloading...",
                ev.cacheName,
                ev.version,
                current != null ? current.version : null);

        long t0 = System.nanoTime();
        try {
            cacheRuntime.reload(ev);

            long ms = (System.nanoTime() - t0) / 1_000_000;
            st.put(ev.cacheName, ev);

            log.info("[CACHE][KAFKA] reload OK cacheName={} version={} in {}ms",
                    ev.cacheName, ev.version, ms);

        } catch (Exception ex) {
            long ms = (System.nanoTime() - t0) / 1_000_000;
            log.error("[CACHE][KAFKA] reload FAILED cacheName={} version={} after {}ms (keep old snapshot)",
                    ev.cacheName, ev.version, ms, ex);
        }
    }

    @Override
    public void close() {
        if (cacheRuntime != null) {
            cacheRuntime.close();
        }
    }
}
