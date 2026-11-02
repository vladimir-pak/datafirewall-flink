package com.gpb.datafirewall.transaction;

import com.gpb.datafirewall.audit.AuditConfig;
import com.gpb.datafirewall.audit.AuditEventType;
import com.gpb.datafirewall.audit.CefAuditEvent;
import com.gpb.datafirewall.audit.CefAuditPublisher;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Connection lifecycle CEF events for both logical sides of a transactional broker worker.
 *
 * <p>GET and PUT share one physical connection/session, but the legacy SOURCE and SINK
 * event types remain stable for existing monitoring rules. All CEF failures are isolated
 * from the broker's commit/rollback decision.</p>
 */
public final class BrokerConnectionAudit implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(BrokerConnectionAudit.class);

    private final AuditConfig config;
    private final CefAuditPublisher publisher;
    private final int subtaskIndex;
    private final int parallelism;
    private final boolean mq;
    private final Map<String, Object> sourceDetails;
    private final Map<String, Object> sinkDetails;
    private boolean connected;

    private BrokerConnectionAudit(
            AuditConfig config,
            int subtaskIndex,
            int parallelism,
            boolean mq,
            Map<String, Object> sourceDetails,
            Map<String, Object> sinkDetails) {
        this.config = config;
        this.publisher = config == null || !config.enabled() ? null : new CefAuditPublisher(config);
        this.subtaskIndex = subtaskIndex;
        this.parallelism = parallelism;
        this.mq = mq;
        this.sourceDetails = Collections.unmodifiableMap(new LinkedHashMap<>(sourceDetails));
        this.sinkDetails = Collections.unmodifiableMap(new LinkedHashMap<>(sinkDetails));
    }

    public static BrokerConnectionAudit mq(
            AuditConfig config, int subtaskIndex, int parallelism,
            String host, int port, String channel, String qmgr,
            String inputQueue, String outputQueue, String errorQueue,
            String user, boolean tls, String cipherSuite, String trustStore,
            String xFrom, String xServiceId) {
        Map<String, Object> common = new LinkedHashMap<>();
        common.put("host", host);
        common.put("port", port);
        common.put("channel", channel);
        common.put("qmgr", qmgr);
        common.put("user", displayUser(user));
        common.put("tlsEnabled", tls);
        common.put("protocol", tls ? "TLS" : "TCP");
        common.put("cipherSuite", displayValue(cipherSuite));
        common.put("trustStore", displayValue(trustStore));
        common.put("transactional", true);
        common.put("errorQueue", errorQueue);

        Map<String, Object> source = new LinkedHashMap<>(common);
        source.put("component", "MqSource");
        source.put("queue", inputQueue);
        Map<String, Object> sink = new LinkedHashMap<>(common);
        sink.put("component", "MqSink");
        sink.put("queue", outputQueue);
        sink.put("xFrom", displayValue(xFrom));
        sink.put("xServiceId", displayValue(xServiceId));
        return new BrokerConnectionAudit(config, subtaskIndex, parallelism, true, source, sink);
    }

    public static BrokerConnectionAudit artemis(
            AuditConfig config, int subtaskIndex, int parallelism,
            String url, String inputQueue, String outputQueue, String errorQueue,
            String user, long timeoutMs) {
        Map<String, Object> common = new LinkedHashMap<>();
        common.put("brokerUrl", maskBrokerUrl(url));
        common.put("user", displayUser(user));
        common.put("transactional", true);
        common.put("errorQueue", errorQueue);
        Map<String, Object> source = new LinkedHashMap<>(common);
        source.put("component", "ArtemisSource");
        source.put("queue", inputQueue);
        source.put("receiveTimeoutMs", timeoutMs);
        Map<String, Object> sink = new LinkedHashMap<>(common);
        sink.put("component", "ArtemisSink");
        sink.put("queue", outputQueue);
        return new BrokerConnectionAudit(config, subtaskIndex, parallelism, false, source, sink);
    }

    public void connecting() {
        emit(sourceType("CONNECTING"), "SUCCESS", sourceDetails, null);
        emit(sinkType("CONNECTING"), "SUCCESS", sinkDetails, null);
    }

    public void connected() {
        connected = true;
        emit(sourceType("CONNECTED"), "SUCCESS", sourceDetails, null);
        emit(sinkType("CONNECTED"), "SUCCESS", sinkDetails, null);
    }

    public void sourceFailed(Exception error) {
        emit(sourceType("CONNECTION_FAILED"), "FAILED", sourceDetails, error);
    }

    public void sinkFailed(Exception error) {
        emit(sinkType("CONNECTION_FAILED"), "FAILED", sinkDetails, error);
    }

    public void connectionFailed(Exception error) {
        sourceFailed(error);
        sinkFailed(error);
    }

    private AuditEventType sourceType(String suffix) {
        return AuditEventType.valueOf((mq ? "IBM_MQ" : "ARTEMIS") + "_SOURCE_" + suffix);
    }

    private AuditEventType sinkType(String suffix) {
        return AuditEventType.valueOf((mq ? "IBM_MQ" : "ARTEMIS") + "_SINK_" + suffix);
    }

    private void emit(AuditEventType type, String status, Map<String, Object> details, Exception error) {
        if (publisher == null) {
            return;
        }
        try {
            CefAuditEvent.Builder builder = config.enrich(CefAuditEvent.builder(type))
                    .status(status)
                    .subtaskIndex(subtaskIndex >= 0 ? subtaskIndex : null)
                    .parallelism(parallelism > 0 ? parallelism : null)
                    .putAll(details);
            if (error != null) {
                builder.put("errorClass", error.getClass().getName())
                        .put("errorMessage", error.getMessage());
            }
            publisher.publish(builder.build());
        } catch (RuntimeException e) {
            // Even with cef.audit.fail-on-error=true, diagnostic CEF must not change MQ UOW.
            log.warn("Failed to publish broker connection audit event {}", type, e);
        }
    }

    @Override
    public void close() {
        if (connected) {
            connected = false;
            emit(sourceType("DISCONNECTED"), "SUCCESS", sourceDetails, null);
            emit(sinkType("DISCONNECTED"), "SUCCESS", sinkDetails, null);
        }
        if (publisher != null) {
            try {
                publisher.close();
            } catch (RuntimeException e) {
                log.warn("Failed to close broker connection CEF publisher", e);
            }
        }
    }

    private static String displayUser(String user) {
        return user == null || user.isBlank() ? "<empty>" : user;
    }

    private static String displayValue(String value) {
        return value == null || value.isBlank() ? "<empty>" : value;
    }

    private static String maskBrokerUrl(String url) {
        if (url == null) {
            return null;
        }
        return url.replaceAll("(?i)(trustStorePassword=)[^&]*", "$1***")
                .replaceAll("(?i)(keyStorePassword=)[^&]*", "$1***");
    }
}
