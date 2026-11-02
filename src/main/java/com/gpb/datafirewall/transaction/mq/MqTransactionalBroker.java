package com.gpb.datafirewall.transaction.mq;

import com.gpb.datafirewall.audit.AuditConfig;

import com.gpb.datafirewall.services.MessageRecord;
import com.gpb.datafirewall.transaction.TransactionBroker;
import com.gpb.datafirewall.transaction.TransactionalReply;
import com.gpb.datafirewall.transaction.BrokerConnectionAudit;
import com.ibm.mq.MQException;
import com.ibm.mq.MQGetMessageOptions;
import com.ibm.mq.MQMessage;
import com.ibm.mq.MQPutMessageOptions;
import com.ibm.mq.MQQueue;
import com.ibm.mq.MQQueueManager;
import com.ibm.mq.constants.MQConstants;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.concurrent.CancellationException;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Base64;
import java.util.Enumeration;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Single local MQ unit of work: MQGET + MQPUT + commit. Never shares an MQQueueManager. */
public final class MqTransactionalBroker implements TransactionBroker {
    private static final Logger log = LoggerFactory.getLogger(MqTransactionalBroker.class);
    private static final DateTimeFormatter TIME =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").withZone(ZoneOffset.UTC);

    private final String host, channel, qmgr, inQueue, outQueue, user, password;
    private final int port, waitMs;
    private final boolean tls;
    private final String cipherSuite, trustStore, trustStorePassword, xFrom, xServiceId;
    private final BrokerConnectionAudit audit;
    private MQQueueManager manager;
    private MQQueue input, output;

    public MqTransactionalBroker(
            String host, int port, String channel, String qmgr,
            String inQueue, String outQueue,
            String user, String password, int waitMs,
            boolean tls, String cipherSuite, String trustStore, String trustStorePassword,
            String xFrom, String xServiceId) throws MQException {
        this(host, port, channel, qmgr, inQueue, outQueue, user, password,
                waitMs, tls, cipherSuite, trustStore, trustStorePassword, xFrom, xServiceId,
                null, -1, -1);
    }

    public MqTransactionalBroker(
            String host, int port, String channel, String qmgr,
            String inQueue, String outQueue,
            String user, String password, int waitMs,
            boolean tls, String cipherSuite, String trustStore, String trustStorePassword,
            String xFrom, String xServiceId,
            AuditConfig auditConfig, int subtaskIndex, int parallelism) throws MQException {
        this.host = host;
        this.port = port;
        this.channel = channel;
        this.qmgr = qmgr;
        this.inQueue = inQueue;
        this.outQueue = outQueue;
        this.user = user;
        this.password = password;
        this.waitMs = waitMs;
        this.tls = tls;
        this.cipherSuite = cipherSuite;
        this.trustStore = trustStore;
        this.trustStorePassword = trustStorePassword;
        this.xFrom = xFrom;
        this.xServiceId = xServiceId;
        this.audit = BrokerConnectionAudit.mq(
                auditConfig, subtaskIndex, parallelism,
                host, port, channel, qmgr, inQueue, outQueue,
                user, tls, cipherSuite, trustStore, xFrom, xServiceId);
        try {
            connect();
        } catch (MQException error) {
            audit.close();
            throw error;
        }
    }

    private void connect() throws MQException {
        audit.connecting();
        MQQueueManager newManager = null;
        MQQueue newInput = null;
        MQQueue newOutput = null;
        try {
            newManager = MqConnect.connect(qmgr, host, port, channel, user, password,
                    tls, cipherSuite, trustStore, trustStorePassword);
            newInput = newManager.accessQueue(inQueue,
                    MQConstants.MQOO_INPUT_SHARED | MQConstants.MQOO_FAIL_IF_QUIESCING);
            newOutput = newManager.accessQueue(outQueue,
                    MQConstants.MQOO_OUTPUT | MQConstants.MQOO_FAIL_IF_QUIESCING);
            this.input = newInput;
            this.output = newOutput;
            this.manager = newManager;
            audit.connected();
        } catch (MQException error) {
            audit.connectionFailed(error);
            closeQueueQuietly(newOutput);
            closeQueueQuietly(newInput);
            if (newManager != null) {
                try { newManager.disconnect(); } catch (Exception ignored) { }
            }
            throw error;
        }
    }

    private static void closeQueueQuietly(MQQueue queue) {
        if (queue != null) {
            try { queue.close(); } catch (Exception ignored) { }
        }
    }

    private void commit() throws MQException {
        try {
            manager.commit();
        } catch (MQException error) {
            audit.connectionFailed(error);
            throw error;
        }
    }

    @Override
    public TransactionalReply poll(Processor processor) throws Exception {
        MQGetMessageOptions gmo = new MQGetMessageOptions();
        gmo.options = MQConstants.MQGMO_WAIT | MQConstants.MQGMO_FAIL_IF_QUIESCING
                | MQConstants.MQGMO_PROPERTIES_IN_HANDLE | MQConstants.MQGMO_SYNCPOINT;
        gmo.waitInterval = waitMs;
        MQMessage received = new MQMessage();
        try {
            input.get(received, gmo);
        } catch (MQException e) {
            if (e.reasonCode == MQConstants.MQRC_NO_MSG_AVAILABLE) return null;
            audit.sourceFailed(e);
            throw e;
        }

        try {
            TransactionalReply result;
            try {
                MessageRecord record = decode(received);
                result = processor.process(record);
                // A malformed reply is a processing failure, not an infrastructure outage.
                if (result.reply().getMqCorrelationId() == null
                        || !Arrays.equals(result.reply().getMqCorrelationId(), received.messageId)) {
                    throw new IllegalStateException("Reply correlation ID does not match MQ request");
                }
            } catch (CancellationException | InterruptedException cancellation) {
                // A stopping worker must not acknowledge an unfinished request.
                throw cancellation;
            } catch (MQException brokerFailure) {
                // A broker I/O error must never be acknowledged as a business failure.
                throw brokerFailure;
            } catch (Exception businessFailure) {
                // Business/validation failure: acknowledge the original request without
                // publishing a reply or forwarding the payload to an error queue.
                commit();
                log.warn("MQ request committed without reply after processing error, msgId={}: {}",
                        MessageRecord.mqIdToHex(received.messageId), businessFailure.toString(),
                        businessFailure);
                return null;
            }
            putReply(result.reply().getMqCorrelationId(), result.reply().getShortJson());
            commit();
            return result;
        } catch (Exception e) {
            try { manager.backout(); }
            catch (MQException rollback) {
                audit.connectionFailed(rollback);
                e.addSuppressed(rollback);
            }
            throw e;
        }
    }

    private MessageRecord decode(MQMessage message) throws Exception {
        // IBM MQ decodes according to MQMD.CodedCharSetId.
        // Invalid text is considered a processing failure and is logged before commit.
        String payload = message.readStringOfByteLength(message.getDataLength());
        Map<String, Object> headers = new LinkedHashMap<>();
        try {
            Enumeration<?> names = message.getPropertyNames("%");
            while (names.hasMoreElements()) {
                String name = String.valueOf(names.nextElement());
                Object value = message.getObjectProperty(name);
                if (value == null) continue;
                String lowercase = name.toLowerCase(Locale.ROOT);
                if (lowercase.contains("password") || lowercase.contains("authorization")
                        || lowercase.contains("token") || lowercase.contains("secret")
                        || lowercase.contains("credential")) {
                    headers.put(name, "***");
                } else if (value instanceof byte[] bytes) {
                    headers.put(name, Base64.getEncoder().encodeToString(bytes));
                } else if (value instanceof String || value instanceof Number || value instanceof Boolean) {
                    headers.put(name, value);
                } else {
                    headers.put(name, value.toString());
                }
            }
        } catch (Exception e) {
            log.warn("Unable to read MQ properties", e);
        }
        Long created = message.putDateTime == null ? null : message.putDateTime.getTimeInMillis();
        return MessageRecord.fromMq(Arrays.copyOf(message.messageId, message.messageId.length),
                payload, created, Instant.now().toEpochMilli(), headers);
    }

    private void putReply(byte[] correlation, String json) throws Exception {
        if (correlation == null || correlation.length != MessageRecord.MQ_ID_LEN) {
            throw new IllegalArgumentException("Missing 24-byte MQ reply correlationId");
        }
        MQMessage message = new MQMessage();
        message.format = MQConstants.MQFMT_STRING;
        message.characterSet = 1208;
        message.persistence = MQConstants.MQPER_PERSISTENT;
        message.correlationId = correlation;
        message.setStringProperty("X_From", xFrom);
        message.setStringProperty("X_ServiceID", xServiceId);
        message.setStringProperty("X_CreateDateTime", TIME.format(Instant.now()));
        message.write(json.getBytes(StandardCharsets.UTF_8));
        try {
            output.put(message, putOptions());
        } catch (MQException error) {
            audit.sinkFailed(error);
            throw error;
        }
    }

    private static MQPutMessageOptions putOptions() {
        MQPutMessageOptions options = new MQPutMessageOptions();
        options.options = MQConstants.MQPMO_SYNCPOINT | MQConstants.MQPMO_FAIL_IF_QUIESCING;
        return options;
    }

    @Override
    public void close() {
        for (MQQueue q : new MQQueue[]{input, output}) {
            if (q != null) try { q.close(); } catch (Exception ignored) { }
        }
        if (manager != null) try { manager.disconnect(); } catch (Exception ignored) { }
        input = output = null;
        manager = null;
        audit.close();
    }
}
