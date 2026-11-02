package com.gpb.datafirewall.transaction.artemis;

import com.gpb.datafirewall.audit.AuditConfig;

import com.gpb.datafirewall.services.MessageRecord;
import com.gpb.datafirewall.transaction.BrokerConnectionAudit;
import com.gpb.datafirewall.transaction.TransactionBroker;
import com.gpb.datafirewall.transaction.TransactionalReply;
import com.gpb.datafirewall.transaction.TransactionBroker.Processor;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
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
    private final String inQueue;
    private final String outQueue;
    private final String failureQueue;
    private final long timeoutMs;
    private Connection connection;
    private Session session;
    private MessageConsumer consumer;
    private MessageProducer replyProducer;
    private MessageProducer failureProducer;

    public ArtemisTransactionalBroker(
            String url, String user, String password, String inQueue, String outQueue,
            String failureQueue, long timeoutMs) throws JMSException {
        this(url, user, password, inQueue, outQueue, failureQueue, timeoutMs,
                null, -1, -1);
    }

    public ArtemisTransactionalBroker(
            String url, String user, String password, String inQueue, String outQueue,
            String failureQueue, long timeoutMs,
            AuditConfig auditConfig, int subtaskIndex, int parallelism) throws JMSException {
        if (failureQueue == null || failureQueue.isBlank()) {
            throw new IllegalArgumentException("artemis.error.queue is required for processing exceptions");
        }
        this.factory = new ActiveMQConnectionFactory(url, user, password);
        this.inQueue = inQueue;
        this.outQueue = outQueue;
        this.failureQueue = failureQueue;
        this.timeoutMs = timeoutMs;
        this.audit = BrokerConnectionAudit.artemis(
                auditConfig, subtaskIndex, parallelism,
                url, inQueue, outQueue, failureQueue, user, timeoutMs);
        try {
            connect();
        } catch (JMSException error) {
            audit.close();
            try { factory.close(); } catch (Exception ignored) { }
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
            Queue errors = newSession.createQueue(failureQueue);
            MessageConsumer newConsumer = newSession.createConsumer(input);
            MessageProducer newReply = newSession.createProducer(output);
            MessageProducer newFailure = newSession.createProducer(errors);
            newReply.setDeliveryMode(DeliveryMode.PERSISTENT);
            newFailure.setDeliveryMode(DeliveryMode.PERSISTENT);
            newConnection.start();
            this.connection = newConnection;
            this.session = newSession;
            this.consumer = newConsumer;
            this.replyProducer = newReply;
            this.failureProducer = newFailure;
            audit.connected();
        } catch (JMSException e) {
            audit.connectionFailed(e);
            if (newConnection != null) try { newConnection.close(); } catch (Exception ignored) { }
            throw e;
        }
    }

    @Override
    public TransactionalReply poll(Processor processor) throws Exception {
        Message incoming;
        try {
            incoming = consumer.receive(timeoutMs);
        } catch (JMSException error) {
            audit.sourceFailed(error);
            throw error;
        }
        if (incoming == null) return null;

        String id;
        try {
            id = incoming.getJMSMessageID();
        } catch (JMSException error) {
            audit.sourceFailed(error);
            rollback(error);
            throw error;
        }
        try {
            TransactionalReply response;
            try {
                String payload = extractPayload(incoming);
                long timestamp = incoming.getJMSTimestamp();
                MessageRecord record = MessageRecord.fromJms(id, payload,
                        timestamp > 0 ? timestamp : null, Instant.now().toEpochMilli());
                response = processor.process(record);
                if (!id.equals(response.reply().getJmsCorrelationId())) {
                    throw new IllegalStateException("Reply correlation ID does not match JMS request " + id);
                }
            } catch (Exception businessFailure) {
                sendFailure(incoming, businessFailure);
                commit();
                log.warn("Artemis message {} moved to {} after processing error: {}", id,
                        failureQueue, businessFailure.toString());
                return null;
            }
            try {
                TextMessage reply = session.createTextMessage(response.reply().getShortJson());
                reply.setJMSCorrelationID(response.reply().getJmsCorrelationId());
                replyProducer.send(reply);
            } catch (JMSException error) {
                audit.sinkFailed(error);
                throw error;
            }
            commit();
            return response;
        } catch (Exception error) {
            rollback(error);
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

    private void sendFailure(Message original, Exception cause) throws JMSException {
        // Copy the entire original payload to the durable error queue in the SAME transaction.
        Message error;
        if (original instanceof TextMessage text) {
            error = session.createTextMessage(text.getText());
        } else if (original instanceof BytesMessage bytes) {
            BytesMessage copy = session.createBytesMessage();
            bytes.reset();
            byte[] block = new byte[8192];
            int count;
            while ((count = bytes.readBytes(block)) > 0) {
                copy.writeBytes(block, 0, count);
            }
            error = copy;
        } else {
            // Never commit a placeholder body for an unsupported JMS message type.
            // The broker must redeliver it until an explicit error policy is configured.
            throw new JMSException("Cannot preserve unsupported JMS message type in error queue: "
                    + original.getClass().getName());
        }
        error.setStringProperty("dfw_original_message_id", original.getJMSMessageID());
        error.setStringProperty("dfw_error_type", cause.getClass().getSimpleName());
        try {
            failureProducer.send(error);
        } catch (JMSException failure) {
            audit.sinkFailed(failure);
            throw failure;
        }
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
        try { session.rollback(); }
        catch (JMSException e) {
            audit.connectionFailed(e);
            original.addSuppressed(e);
        }
    }

    @Override
    public void close() {
        if (connection != null) try { connection.close(); } catch (JMSException ignored) { }
        if (factory != null) try { factory.close(); } catch (Exception ignored) { }
        connection = null;
        session = null;
        consumer = null;
        replyProducer = null;
        failureProducer = null;
        audit.close();
    }
}
