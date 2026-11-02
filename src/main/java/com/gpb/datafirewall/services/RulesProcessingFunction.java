package com.gpb.datafirewall.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.gpb.datafirewall.bundle.CacheBundleState;
import com.gpb.datafirewall.dto.ProcessingResult;
import com.gpb.datafirewall.vault.dto.VaultSecretsDto;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

/** Background preparation is pure; publication and processing run on the operator mailbox. */
public final class RulesProcessingFunction extends AbstractRichFunction {
    private static final Logger log = LoggerFactory.getLogger(RulesProcessingFunction.class);
    public static final OutputTag<ProcessingResult> FLINK_AUDIT_RESULT_TAG =
            new OutputTag<ProcessingResult>("flink-audit-result") {};
    private final String jwt;
    private final VaultSecretsDto vaultSecrets;
    private transient ObjectMapper mapper;
    private transient RulesCacheRuntime cacheRuntime;
    private transient MessageProcessingService messageProcessor;
    private transient DotnetHandlerClient dotnetHandlerClient;

    public RulesProcessingFunction(String jwt, VaultSecretsDto secrets) {
        this.jwt = jwt;
        this.vaultSecrets = secrets;
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

        this.mapper = new ObjectMapper();
        this.cacheRuntime = new RulesCacheRuntime(mapper,jwt);

        this.cacheRuntime.open(pt);

        this.messageProcessor = new MessageProcessingService(
                mapper,
                cacheRuntime,
                logPayloads
        );

        String dotnetUrl = pt.get("handler.dotnet.url", pt.get("dotnet.handler.url", null));

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


        log.info("[CACHE] processor subtask={} ready for local background activation, no API call during open",
                getRuntimeContext().getIndexOfThisSubtask());
    }

    /** Runs on the cache preparation executor. Never modifies the active epoch. */
    public RuntimeEpoch prepareEpoch(CacheBundleState bundle) {
        RulesCacheSnapshot next = cacheRuntime.prepare(bundle);
        DynamicHandler handler = DynamicHandler.from(bundle.handler(), DynamicHandler.FLINK);
        if (handler == DynamicHandler.DOTNET && !dotnetHandlerClient.isConfigured()) {
            throw new IllegalStateException("Cannot activate dotnet handler without URL and JWT");
        }
        return new RuntimeEpoch(next, handler);
    }

    /** Runs on the mailbox, between messages. One atomic publication for the complete runtime. */
    public void activate(RuntimeEpoch epoch) {
        cacheRuntime.activate(epoch);
        log.info("[CACHE] subtask={} atomically activated epoch={} handler={}",
                getRuntimeContext().getIndexOfThisSubtask(), epoch.cacheEpoch(), epoch.handler());
    }

    public long activeEpoch() {
        return cacheRuntime.snapshot().cacheEpoch();
    }

    public void process(MessageRecord in, Consumer<ProcessingResult> reply,
                        Consumer<ProcessingResult> audit) {
        Objects.requireNonNull(in, "message");
        RuntimeEpoch epoch = cacheRuntime.epoch();
        if (epoch.cacheEpoch() < 0) throw new IllegalStateException("Cache is not active");
        ProcessingResult internal;
        ProcessingResult response;
        if (epoch.handler() == DynamicHandler.DOTNET) {
            response = requireResult(dotnetHandlerClient.process(in), in);
            internal = requireResult(messageProcessor.process(in, epoch.snapshot()), in);
        } else {
            internal = requireResult(messageProcessor.process(in, epoch.snapshot()), in);
            response = internal;
        }
        reply.accept(response);
        audit.accept(internal);
    }

    private static ProcessingResult requireResult(ProcessingResult result, MessageRecord in) {
        if (result == null || result.getShortJson() == null || result.getShortJson().isBlank()) {
            throw new IllegalStateException("No processing result for eventId=" + in.eventId());
        }
        return result;
    }

    private static String firstNotBlank(String... values) {
        for (String value : values) if (value != null && !value.isBlank()) return value.trim();
        return null;
    }

    @Override public void close() throws Exception {
        if (cacheRuntime != null) cacheRuntime.close();
        super.close();
    }
}
