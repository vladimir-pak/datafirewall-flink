package com.gpb.datafirewall.artemis;

import com.gpb.datafirewall.audit.*;
import com.gpb.datafirewall.services.MessageReply;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;

public class ArtemisSink extends RichSinkFunction<MessageReply> {

    private static final Logger log = LoggerFactory.getLogger(ArtemisSink.class);
    private static final long RECONNECT_INITIAL_MS = 5_000L;
    private static final long RECONNECT_MAX_MS = 60_000L;

    private final String brokerUrl;
    private final String username;
    private final String password;
    private final String queueName;
    private final AuditConfig auditConfig;

    private transient volatile boolean running;
    private transient Object connectionLock;
    private transient ActiveMQConnectionFactory connectionFactory;
    private transient CefAuditPublisher auditPublisher;
    private transient volatile Connection connection;
    private transient volatile Session session;
    private transient volatile MessageProducer producer;

    public ArtemisSink(String brokerUrl, String username, String password, String queueName) {
        this(brokerUrl, username, password, queueName, null);
    }

    public ArtemisSink(String brokerUrl, String username, String password, String queueName, AuditConfig auditConfig) {
        this.brokerUrl = brokerUrl;
        this.username = username;
        this.password = password;
        this.queueName = queueName;
        this.auditConfig = auditConfig;
    }

    @Override
    public void open(Configuration parameters) {
        running = true;
        connectionLock = new Object();
        auditPublisher = new CefAuditPublisher(auditConfig);
        connectionFactory = new ActiveMQConnectionFactory(brokerUrl, username, password);

        try {
            connect();
        } catch (Exception e) {
            publishAudit(AuditEventType.ARTEMIS_SINK_CONNECTION_FAILED, "FAILED", e);
            log.warn("ArtemisSink initial connection failed. Will reconnect on send: broker={}, queue={}, error={}",
                    maskBrokerUrl(brokerUrl), queueName, e.getMessage());
            closeJmsResources();
        }
    }

    @Override
    public void invoke(MessageReply value, Context context) {
        if (value == null) return;

        if (value.jmsCorrelationId == null || value.jmsCorrelationId.isBlank()) {
            throw new IllegalArgumentException("Artemis reply requires jmsCorrelationId");
        }

        long delay = RECONNECT_INITIAL_MS;

        while (running) {
            if (producer == null) {
                try {
                    connect();
                    delay = RECONNECT_INITIAL_MS;
                } catch (Exception e) {
                    if (!running) return;

                    publishAudit(AuditEventType.ARTEMIS_SINK_CONNECTION_FAILED, "FAILED", e);
                    log.warn("ArtemisSink reconnect failed, retry in {} ms: queue={}, correlationId={}, error={}",
                            delay, queueName, value.jmsCorrelationId, e.getMessage());

                    closeJmsResources();
                    if (!sleep(delay)) return;
                    delay = nextDelay(delay);
                    continue;
                }
            }

            try {
                TextMessage message = session.createTextMessage(value.payload == null ? "" : value.payload);
                message.setJMSCorrelationID(value.jmsCorrelationId);
                producer.send(message);

                log.info("ArtemisSink message sent: subtask={}, queue={}, correlationId={}",
                        getRuntimeContext().getIndexOfThisSubtask(), queueName, value.jmsCorrelationId);

                return;
            } catch (JMSException e) {
                if (!running) return;

                publishAudit(AuditEventType.ARTEMIS_SINK_CONNECTION_FAILED, "FAILED", e);
                log.warn("ArtemisSink send failed, current message will be retried in {} ms: queue={}, correlationId={}, error={}",
                        delay, queueName, value.jmsCorrelationId, e.getMessage());

                closeJmsResources();
                if (!sleep(delay)) return;
                delay = nextDelay(delay);
            }
        }
    }

    private void connect() throws JMSException {
        publishAudit(AuditEventType.ARTEMIS_SINK_CONNECTING, "SUCCESS", null);

        Connection newConnection = null;
        Session newSession = null;
        MessageProducer newProducer = null;

        try {
            log.info("ArtemisSink connecting: subtask={}, brokerUrl={}, queue={}",
                    getRuntimeContext().getIndexOfThisSubtask(), maskBrokerUrl(brokerUrl), queueName);

            newConnection = connectionFactory.createConnection();
            newSession = newConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = newSession.createQueue(queueName);
            newProducer = newSession.createProducer(queue);
            newConnection.start();

            if (!running) {
                closeQuietly(newProducer, "Artemis MessageProducer");
                closeQuietly(newSession, "Artemis Session");
                closeQuietly(newConnection, "Artemis Connection");
                return;
            }

            synchronized (connectionLock) {
                connection = newConnection;
                session = newSession;
                producer = newProducer;
            }

            log.info("ArtemisSink connected: subtask={}, brokerUrl={}, queue={}",
                    getRuntimeContext().getIndexOfThisSubtask(), maskBrokerUrl(brokerUrl), queueName);

            publishAudit(AuditEventType.ARTEMIS_SINK_CONNECTED, "SUCCESS", null);
        } catch (JMSException e) {
            closeQuietly(newProducer, "Artemis MessageProducer");
            closeQuietly(newSession, "Artemis Session");
            closeQuietly(newConnection, "Artemis Connection");
            throw e;
        }
    }

    @Override
    public void close() {
        running = false;
        closeJmsResources();

        publishAudit(AuditEventType.ARTEMIS_SINK_DISCONNECTED, "SUCCESS", null);

        closeQuietly(connectionFactory, "Artemis ConnectionFactory");
        connectionFactory = null;

        closeQuietly(auditPublisher, "BusinessAuditPublisher");
        auditPublisher = null;
    }

    private void closeJmsResources() {
        if (connectionLock == null) return;

        synchronized (connectionLock) {
            MessageProducer oldProducer = producer;
            Session oldSession = session;
            Connection oldConnection = connection;

            producer = null;
            session = null;
            connection = null;

            closeQuietly(oldProducer, "Artemis MessageProducer");
            closeQuietly(oldSession, "Artemis Session");
            closeQuietly(oldConnection, "Artemis Connection");
        }
    }

    private boolean sleep(long delay) {
        if (!running) return false;
        try {
            Thread.sleep(delay);
            return running;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    private static long nextDelay(long delay) {
        return Math.min(delay * 2, RECONNECT_MAX_MS);
    }

    private void closeQuietly(AutoCloseable resource, String name) {
        if (resource == null) return;
        try {
            resource.close();
        } catch (Exception e) {
            log.warn("Failed to close {}", name, e);
        }
    }

    private void publishAudit(AuditEventType type, String status, Exception error) {
        if (auditPublisher == null || auditConfig == null || !auditConfig.enabled()) return;

        try {
            CefAuditEvent.Builder builder = auditConfig.enrich(CefAuditEvent.builder(type))
                    .status(status)
                    .subtaskIndex(getRuntimeContext().getIndexOfThisSubtask())
                    .parallelism(getRuntimeContext().getNumberOfParallelSubtasks())
                    .put("component", "ArtemisSink")
                    .put("brokerUrl", maskBrokerUrl(brokerUrl))
                    .put("queue", queueName)
                    .put("user", username == null || username.isBlank() ? "<empty>" : username);

            if (error != null) builder.put("errorClass", error.getClass().getName()).put("errorMessage", error.getMessage());
            auditPublisher.publish(builder.build());
        } catch (Exception e) {
            log.warn("Failed to publish ArtemisSink audit event type={}", type, e);
        }
    }

    private static String maskBrokerUrl(String url) {
        if (url == null) return null;
        return url.replaceAll("(?i)(trustStorePassword=)[^&]*", "$1***")
                .replaceAll("(?i)(keyStorePassword=)[^&]*", "$1***");
    }
}