package com.gpb.datafirewall.mq;

import com.gpb.datafirewall.audit.AuditConfig;
import com.gpb.datafirewall.audit.AuditEventType;
import com.gpb.datafirewall.audit.CefAuditEvent;
import com.gpb.datafirewall.audit.CefAuditPublisher;
import com.gpb.datafirewall.services.MessageReply;
import com.ibm.mq.MQException;
import com.ibm.mq.MQMessage;
import com.ibm.mq.MQPutMessageOptions;
import com.ibm.mq.MQQueue;
import com.ibm.mq.MQQueueManager;
import com.ibm.mq.constants.MQConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;

public class MqSink extends RichSinkFunction<MessageReply> {

    private static final Logger log = LoggerFactory.getLogger(MqSink.class);

    private static final long RECONNECT_INITIAL_DELAY_MS = 5_000;
    private static final long RECONNECT_MAX_DELAY_MS = 60_000;

    private static final String HEADER_X_FROM = "X_From";
    private static final String HEADER_X_SERVICE_ID = "X_ServiceID";
    private static final String HEADER_X_CREATE_DATE_TIME = "X_CreateDateTime";

    private static final DateTimeFormatter MQ_HEADER_TIME_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").withZone(ZoneOffset.UTC);

    private final String host;
    private final int port;
    private final String channel;
    private final String qmgr;
    private final String outQueue;
    private final String user;
    private final String password;
    private final boolean tlsEnabled;
    private final String tlsCipherSuite;
    private final String trustStore;
    private final String trustStorePassword;
    private final String xFrom;
    private final String xServiceId;
    private final AuditConfig auditConfig;

    private transient volatile boolean running;
    private transient volatile Thread invokeThread;
    private transient MQQueueManager qm;
    private transient MQQueue queue;
    private transient CefAuditPublisher auditPublisher;

    public MqSink(String host, int port, String channel, String qmgr, String outQueue, String user, String password) {
        this(host, port, channel, qmgr, outQueue, user, password, false, null, null, null, null, null, null);
    }

    public MqSink(String host, int port, String channel, String qmgr, String outQueue, String user, String password,
                  boolean tlsEnabled, String tlsCipherSuite, String trustStore, String trustStorePassword) {
        this(host, port, channel, qmgr, outQueue, user, password, tlsEnabled, tlsCipherSuite,
                trustStore, trustStorePassword, null, null, null);
    }

    public MqSink(String host, int port, String channel, String qmgr, String outQueue, String user, String password,
                  boolean tlsEnabled, String tlsCipherSuite, String trustStore, String trustStorePassword,
                  AuditConfig auditConfig) {
        this(host, port, channel, qmgr, outQueue, user, password, tlsEnabled, tlsCipherSuite,
                trustStore, trustStorePassword, null, null, auditConfig);
    }

    public MqSink(String host, int port, String channel, String qmgr, String outQueue, String user, String password,
                  boolean tlsEnabled, String tlsCipherSuite, String trustStore, String trustStorePassword,
                  String xFrom, String xServiceId, AuditConfig auditConfig) {
        this.host = host;
        this.port = port;
        this.channel = channel;
        this.qmgr = qmgr;
        this.outQueue = outQueue;
        this.user = user;
        this.password = password;
        this.tlsEnabled = tlsEnabled;
        this.tlsCipherSuite = tlsCipherSuite;
        this.trustStore = trustStore;
        this.trustStorePassword = trustStorePassword;
        this.xFrom = normalizeHeaderValue(xFrom, "MKD");
        this.xServiceId = normalizeHeaderValue(xServiceId, "");
        this.auditConfig = auditConfig;
    }

    public MqSink(String host, int port, String channel, String qmgr, String outQueue) {
        this(host, port, channel, qmgr, outQueue, null, null);
    }

    @Override
    public void open(Configuration parameters) {
        running = true;
        auditPublisher = new CefAuditPublisher(auditConfig);

        log.info("MqSink initialized: subtask={} host={} port={} qmgr={} channel={} queue={} user={} tls={}",
                getRuntimeContext().getIndexOfThisSubtask(), host, port, qmgr, channel, outQueue, user, tlsEnabled);
    }

    @Override
    public void invoke(MessageReply value, Context context) throws Exception {
        if (value == null) {
            return;
        }

        if (value.mqCorrelationId == null || value.mqCorrelationId.length != MessageReply.MQ_ID_LEN) {
            throw new IllegalArgumentException("IBM MQ reply requires mqCorrelationId with length "
                    + MessageReply.MQ_ID_LEN + ", actual="
                    + (value.mqCorrelationId == null ? "null" : value.mqCorrelationId.length));
        }

        invokeThread = Thread.currentThread();
        long reconnectDelay = RECONNECT_INITIAL_DELAY_MS;

        try {
            while (running) {
                try {
                    ensureConnected();

                    MQMessage msg = buildMessage(value);

                    MQPutMessageOptions pmo = new MQPutMessageOptions();
                    pmo.options = MQConstants.MQPMO_NO_SYNCPOINT | MQConstants.MQPMO_FAIL_IF_QUIESCING;

                    queue.put(msg, pmo);

                    return;

                } catch (MQException e) {
                    if (!running) {
                        return;
                    }

                    if (!isReconnectable(e)) {
                        log.error("MQ PUT/connect fatal error completionCode={} reasonCode={}",
                                e.completionCode, e.reasonCode, e);
                        publishAudit(AuditEventType.IBM_MQ_SINK_CONNECTION_FAILED, "FAILED", e);
                        throw e;
                    }

                    log.warn("MqSink MQ unavailable completionCode={} reasonCode={}. Reconnect in {} ms",
                            e.completionCode, e.reasonCode, reconnectDelay);

                    publishAudit(AuditEventType.IBM_MQ_SINK_CONNECTION_FAILED, "FAILED", e);
                    disconnectMqQuietly();

                    if (!sleepReconnect(reconnectDelay)) {
                        return;
                    }

                    reconnectDelay = Math.min(reconnectDelay * 2, RECONNECT_MAX_DELAY_MS);
                }
            }
        } finally {
            invokeThread = null;
        }
    }

    private void ensureConnected() throws MQException {
        if (qm != null && queue != null) {
            return;
        }

        log.info("MqSink connecting: subtask={} host={}:{} qmgr={} queue={}",
                getRuntimeContext().getIndexOfThisSubtask(), host, port, qmgr, outQueue);

        publishAudit(AuditEventType.IBM_MQ_SINK_CONNECTING, "SUCCESS", null);

        qm = MqConnect.connect(qmgr, host, port, channel, user, password,
                tlsEnabled, tlsCipherSuite, trustStore, trustStorePassword);

        queue = qm.accessQueue(outQueue, MQConstants.MQOO_OUTPUT | MQConstants.MQOO_FAIL_IF_QUIESCING);

        log.info("MqSink connected: subtask={} host={}:{} qmgr={} queue={}",
                getRuntimeContext().getIndexOfThisSubtask(), host, port, qmgr, outQueue);

        publishAudit(AuditEventType.IBM_MQ_SINK_CONNECTED, "SUCCESS", null);
    }

    private MQMessage buildMessage(MessageReply value) throws Exception {
        String payload = value.payload;
        byte[] body = payload == null ? new byte[0] : payload.getBytes(StandardCharsets.UTF_8);

        MQMessage msg = new MQMessage();
        msg.format = MQConstants.MQFMT_STRING;
        msg.characterSet = 1208;
        msg.correlationId = value.mqCorrelationId;

        applyRequiredHeaders(msg);
        msg.write(body);

        return msg;
    }

    private boolean isReconnectable(MQException e) {
        return e.reasonCode == MQConstants.MQRC_CONNECTION_BROKEN
                || e.reasonCode == MQConstants.MQRC_Q_MGR_NOT_AVAILABLE
                || e.reasonCode == MQConstants.MQRC_HOST_NOT_AVAILABLE;
    }

    private boolean sleepReconnect(long delay) {
        try {
            Thread.sleep(delay);
            return running;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    private void disconnectMqQuietly() {
        try {
            if (queue != null) {
                queue.close();
            }
        } catch (Exception e) {
            log.debug("Failed to close MQQueue during reconnect: {}", e.getMessage());
        } finally {
            queue = null;
        }

        try {
            if (qm != null) {
                qm.disconnect();
            }
        } catch (Exception e) {
            log.debug("Failed to disconnect MQQueueManager during reconnect: {}", e.getMessage());
        } finally {
            qm = null;
        }
    }

    private void applyRequiredHeaders(MQMessage msg) throws Exception {
        msg.setStringProperty(HEADER_X_FROM, xFrom);
        msg.setStringProperty(HEADER_X_SERVICE_ID, xServiceId);
        msg.setStringProperty(HEADER_X_CREATE_DATE_TIME, currentMqHeaderTimestamp());
    }

    private static String currentMqHeaderTimestamp() {
        return MQ_HEADER_TIME_FORMATTER.format(Instant.now().truncatedTo(ChronoUnit.MILLIS));
    }

    private static String normalizeHeaderValue(String value, String defaultValue) {
        return value == null || value.isBlank() ? defaultValue : value.trim();
    }

    @Override
    public void close() {
        running = false;

        Thread thread = invokeThread;
        if (thread != null) {
            thread.interrupt();
        }

        publishAudit(AuditEventType.IBM_MQ_SINK_DISCONNECTED, "SUCCESS", null);
        disconnectMqQuietly();

        try {
            if (auditPublisher != null) {
                auditPublisher.close();
            }
        } catch (Exception e) {
            log.warn("Failed to close CefAuditPublisher", e);
        } finally {
            auditPublisher = null;
        }
    }

    private void publishAudit(AuditEventType type, String status, Exception error) {
        if (auditPublisher == null || auditConfig == null || !auditConfig.enabled()) {
            return;
        }

        CefAuditEvent.Builder builder = auditConfig.enrich(CefAuditEvent.builder(type))
                .status(status)
                .subtaskIndex(getRuntimeContext().getIndexOfThisSubtask())
                .parallelism(getRuntimeContext().getNumberOfParallelSubtasks())
                .put("component", "MqSink")
                .put("host", host)
                .put("port", port)
                .put("qmgr", qmgr)
                .put("channel", channel)
                .put("queue", outQueue)
                .put("user", user == null || user.isBlank() ? "<empty>" : user)
                .put("tlsEnabled", tlsEnabled)
                .put("protocol", tlsEnabled ? "TLS" : "TCP")
                .put("cipherSuite", tlsCipherSuite == null || tlsCipherSuite.isBlank() ? "<empty>" : tlsCipherSuite)
                .put("trustStore", trustStore == null || trustStore.isBlank() ? "<empty>" : trustStore)
                .put("xFrom", xFrom == null || xFrom.isBlank() ? "<empty>" : xFrom)
                .put("xServiceId", xServiceId == null || xServiceId.isBlank() ? "<empty>" : xServiceId);

        if (error != null) {
            builder.put("errorClass", error.getClass().getName()).put("errorMessage", error.getMessage());
        }

        auditPublisher.publish(builder.build());
    }
}