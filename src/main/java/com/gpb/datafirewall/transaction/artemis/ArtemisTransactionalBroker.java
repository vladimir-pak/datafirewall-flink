
package com.gpb.datafirewall.transaction.artemis;

import com.gpb.datafirewall.audit.AuditConfig;
import com.gpb.datafirewall.dto.ProcessingResult;
import com.gpb.datafirewall.services.MessageRecord;
import com.gpb.datafirewall.transaction.BrokerConnectionAudit;
import com.gpb.datafirewall.transaction.ProcessingErrorAudit;
import com.gpb.datafirewall.transaction.TransactionBroker;
import com.gpb.datafirewall.transaction.TransactionalReply;
import com.gpb.datafirewall.util.ReplyMetadataEnricher;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.concurrent.CancellationException;
import java.util.function.Consumer;
import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** RECEIVE and SEND are committed in one JMS transaction on one broker. */
public final class ArtemisTransactionalBroker implements TransactionBroker {
    private static final Logger log = LoggerFactory.getLogger(ArtemisTransactionalBroker.class);
    private final ActiveMQConnectionFactory factory;
    private final BrokerConnectionAudit audit;
    private final String inQueue, outQueue;
    private final long timeoutMs;
    private Connection connection;
    private Session session;
    private MessageConsumer consumer;
    private MessageProducer replyProducer;

    public ArtemisTransactionalBroker(String url, String user, String password,
                                      String inQueue, String outQueue, long timeoutMs) throws JMSException {
        this(url, user, password, inQueue, outQueue, timeoutMs, null, -1, -1);
    }

    public ArtemisTransactionalBroker(String url, String user, String password,
                                      String inQueue, String outQueue, long timeoutMs,
                                      AuditConfig auditConfig, int subtaskIndex, int parallelism) throws JMSException {
        factory = new ActiveMQConnectionFactory(url, user, password);
        this.inQueue = inQueue;
        this.outQueue = outQueue;
        this.timeoutMs = timeoutMs;
        audit = BrokerConnectionAudit.artemis(auditConfig, subtaskIndex, parallelism,
                url, inQueue, outQueue, user, timeoutMs);
        try {
            connect();
        } catch (JMSException error) {
            audit.close();
            try { factory.close(); } catch (Exception ignored) {}
            throw error;
        }
    }

    private void connect() throws JMSException {
        audit.connecting();
        Connection newConnection = null;
        try {
            newConnection = factory.createConnection();
            Session newSession = newConnection.createSession(true, Session.SESSION_TRANSACTED);
            Queue input = newSession.createQueue(inQueue);
            Queue output = newSession.createQueue(outQueue);
            MessageConsumer newConsumer = newSession.createConsumer(input);
            MessageProducer newReply = newSession.createProducer(output);
            newReply.setDeliveryMode(DeliveryMode.PERSISTENT);
            newConnection.start();
            connection = newConnection;
            session = newSession;
            consumer = newConsumer;
            replyProducer = newReply;
            audit.connected();
        } catch (JMSException error) {
            audit.connectionFailed(error);
            if (newConnection != null) try { newConnection.close(); } catch (Exception ignored) {}
            throw error;
        }
    }

    @Override
    public TransactionalReply poll(Processor processor, Consumer<ProcessingResult> errorAudit) throws Exception {
        Message incoming;
        try {
            incoming = consumer.receive(timeoutMs);
        } catch (JMSException error) {
            audit.sourceFailed(error);
            throw error;
        }
        if (incoming == null) return null;

        boolean committed = false;
        try {
            String id = incoming.getJMSMessageID();
            MessageRecord record;
            try {
                String payload = extractPayload(incoming);
                long timestamp = incoming.getJMSTimestamp();
                record = MessageRecord.fromJms(id, payload,
                        timestamp > 0 ? timestamp : null, Instant.now().toEpochMilli());
            } catch (JMSException brokerFailure) {
                throw brokerFailure;
            } catch (Exception decodeFailure) {
                // Do not acknowledge a message when its original QUERY cannot be recovered.
                log.error("Unable to decode Artemis request, messageId={}; transaction will rollback",
                        id, decodeFailure);
                throw decodeFailure;
            }

            TransactionalReply response;
            try {
                response = processor.process(record);
                if (response == null || response.reply() == null) {
                    throw new IllegalStateException("Processor returned no Artemis reply");
                }
                if (!id.equals(response.reply().getJmsCorrelationId())) {
                    throw new IllegalStateException("Reply correlation ID does not match JMS request " + id);
                }
                ReplyMetadataEnricher.enrich(response, record, Instant.now());
            } catch (CancellationException | InterruptedException cancellation) {
                throw cancellation;
            } catch (JMSException brokerFailure) {
                throw brokerFailure;
            } catch (Exception businessFailure) {
                ProcessingResult errorResult = null;
                try {
                    errorResult = ProcessingErrorAudit.create(record, businessFailure);
                } catch (Exception auditFailure) {
                    log.error("Failed to prepare Artemis processing error audit, request={}", id, auditFailure);
                }

                commit();
                committed = true;
                log.warn("Artemis request {} committed without reply after processing error", id, businessFailure);

                if (errorResult != null && errorAudit != null) {
                    try {
                        errorAudit.accept(errorResult);
                    } catch (Exception auditFailure) {
                        log.warn("Failed to enqueue Artemis processing error audit, request={}", id, auditFailure);
                    }
                }
                return null;
            }

            try {
                TextMessage reply = session.createTextMessage(response.reply().getShortJson());
                reply.setJMSCorrelationID(response.reply().getJmsCorrelationId());
                replyProducer.send(reply);
            } catch (JMSException brokerFailure) {
                audit.sinkFailed(brokerFailure);
                throw brokerFailure;
            }

            commit();
            committed = true;
            return response;
        } catch (Exception error) {
            if (!committed) rollback(error);
            throw error;
        }
    }

    private String extractPayload(Message incoming) throws JMSException {
        if (incoming instanceof TextMessage text) return text.getText();
        if (incoming instanceof BytesMessage bytes) {
            long length = bytes.getBodyLength();
            if (length > Integer.MAX_VALUE) {
                throw new IllegalArgumentException("JMS message exceeds Java String limit");
            }
            byte[] body = new byte[(int) length];
            bytes.reset();
            int offset = 0;
            while (offset < body.length) {
                byte[] next = new byte[Math.min(8192, body.length - offset)];
                int n = bytes.readBytes(next);
                if (n <= 0) throw new JMSException("Truncated JMS bytes message");
                System.arraycopy(next, 0, body, offset, n);
                offset += n;
            }
            return new String(body, StandardCharsets.UTF_8);
        }
        throw new IllegalArgumentException("Unsupported JMS message type: " + incoming.getClass());
    }

    private void commit() throws JMSException {
        try {
            session.commit();
        } catch (JMSException error) {
            audit.connectionFailed(error);
            throw error;
        }
    }

    private void rollback(Exception original) {
        try {
            session.rollback();
        } catch (JMSException error) {
            audit.connectionFailed(error);
            original.addSuppressed(error);
        }
    }

    @Override
    public void close() {
        if (connection != null) try { connection.close(); } catch (JMSException ignored) {}
        try { factory.close(); } catch (Exception ignored) {}
        connection = null;
        session = null;
        consumer = null;
        replyProducer = null;
        audit.close();
    }
}
