package com.gpb.datafirewall.artemis;

import com.gpb.datafirewall.audit.*;
import com.gpb.datafirewall.services.MessageRecord;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;
import java.lang.IllegalStateException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;

public class ArtemisSource extends RichSourceFunction<MessageRecord> {

    private static final Logger log = LoggerFactory.getLogger(ArtemisSource.class);
    private static final long RECONNECT_INITIAL_MS = 5_000L;
    private static final long RECONNECT_MAX_MS = 60_000L;

    private final String brokerUrl;
    private final String username;
    private final String password;
    private final String queueName;
    private final long receiveTimeoutMs;
    private final AuditConfig auditConfig;

    private transient volatile boolean running;
    private transient Object connectionLock;
    private transient CefAuditPublisher auditPublisher;
    private transient ActiveMQConnectionFactory connectionFactory;
    private transient volatile Connection connection;
    private transient volatile Session session;
    private transient volatile MessageConsumer consumer;

    public ArtemisSource(String brokerUrl, String username, String password, String queueName, long receiveTimeoutMs) {
        this(brokerUrl, username, password, queueName, receiveTimeoutMs, null);
    }

    public ArtemisSource(String brokerUrl, String username, String password, String queueName, long receiveTimeoutMs, AuditConfig auditConfig) {
        this.brokerUrl = brokerUrl;
        this.username = username;
        this.password = password;
        this.queueName = queueName;
        this.receiveTimeoutMs = receiveTimeoutMs;
        this.auditConfig = auditConfig;
    }

    @Override
    public void open(Configuration parameters) {
        running = true;
        connectionLock = new Object();
        auditPublisher = new CefAuditPublisher(auditConfig);
        connectionFactory = new ActiveMQConnectionFactory(brokerUrl, username, password);

        log.info("ArtemisSource initialized: subtask={}, brokerUrl={}, queue={}, receiveTimeoutMs={}",
                getRuntimeContext().getIndexOfThisSubtask(), maskBrokerUrl(brokerUrl), queueName, receiveTimeoutMs);
    }

    @Override
    public void run(SourceContext<MessageRecord> ctx) {
        long delay = RECONNECT_INITIAL_MS;

        while (running) {
            if (consumer == null) {
                try {
                    connect();
                    delay = RECONNECT_INITIAL_MS;
                } catch (Exception e) {
                    if (!running) return;

                    publishAudit(AuditEventType.ARTEMIS_SOURCE_CONNECTION_FAILED, "FAILED", e);
                    log.warn("ArtemisSource connect failed, retry in {} ms: broker={}, queue={}, error={}",
                            delay, maskBrokerUrl(brokerUrl), queueName, e.getMessage());

                    closeJmsResources();
                    if (!sleep(delay)) return;
                    delay = nextDelay(delay);
                    continue;
                }
            }

            try {
                Message message = consumer.receive(receiveTimeoutMs);
                if (message == null) continue;

                long readedDttm = Instant.now().toEpochMilli();
                String payload = extractPayload(message);
                String msgId = extractMessageId(message);
                Long jmsTimestamp = extractJmsTimestamp(message);

                synchronized (ctx.getCheckpointLock()) {
                    ctx.collect(MessageRecord.fromJms(msgId, payload, jmsTimestamp, readedDttm));
                }

                delay = RECONNECT_INITIAL_MS;
            } catch (JMSException e) {
                if (!running) return;

                publishAudit(AuditEventType.ARTEMIS_SOURCE_CONNECTION_FAILED, "FAILED", e);
                log.warn("ArtemisSource connection lost, reconnect in {} ms: queue={}, error={}",
                        delay, queueName, e.getMessage());

                closeJmsResources();
                if (!sleep(delay)) return;
                delay = nextDelay(delay);
            } catch (IllegalStateException e) {
                log.error("ArtemisSource invalid message skipped: queue={}, error={}", queueName, e.getMessage(), e);
            }
        }
    }

    private void connect() throws JMSException {
        publishAudit(AuditEventType.ARTEMIS_SOURCE_CONNECTING, "SUCCESS", null);

        Connection newConnection = null;
        Session newSession = null;
        MessageConsumer newConsumer = null;

        try {
            log.info("ArtemisSource connecting: subtask={}, brokerUrl={}, queue={}",
                    getRuntimeContext().getIndexOfThisSubtask(), maskBrokerUrl(brokerUrl), queueName);

            newConnection = connectionFactory.createConnection();
            newSession = newConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = newSession.createQueue(queueName);
            newConsumer = newSession.createConsumer(queue);
            newConnection.start();

            if (!running) {
                closeQuietly(newConsumer, "Artemis MessageConsumer");
                closeQuietly(newSession, "Artemis Session");
                closeQuietly(newConnection, "Artemis Connection");
                return;
            }

            synchronized (connectionLock) {
                connection = newConnection;
                session = newSession;
                consumer = newConsumer;
            }

            log.info("ArtemisSource connected: subtask={}, brokerUrl={}, queue={}",
                    getRuntimeContext().getIndexOfThisSubtask(), maskBrokerUrl(brokerUrl), queueName);

            publishAudit(AuditEventType.ARTEMIS_SOURCE_CONNECTED, "SUCCESS", null);
        } catch (JMSException e) {
            closeQuietly(newConsumer, "Artemis MessageConsumer");
            closeQuietly(newSession, "Artemis Session");
            closeQuietly(newConnection, "Artemis Connection");
            throw e;
        }
    }

    @Override
    public void cancel() {
        running = false;
        closeJmsResources();
    }

    @Override
    public void close() {
        running = false;
        closeJmsResources();

        publishAudit(AuditEventType.ARTEMIS_SOURCE_DISCONNECTED, "SUCCESS", null);

        closeQuietly(connectionFactory, "Artemis ConnectionFactory");
        connectionFactory = null;

        closeQuietly(auditPublisher, "BusinessAuditPublisher");
        auditPublisher = null;
    }

    private void closeJmsResources() {
        if (connectionLock == null) return;

        synchronized (connectionLock) {
            MessageConsumer oldConsumer = consumer;
            Session oldSession = session;
            Connection oldConnection = connection;

            consumer = null;
            session = null;
            connection = null;

            closeQuietly(oldConsumer, "Artemis MessageConsumer");
            closeQuietly(oldSession, "Artemis Session");
            closeQuietly(oldConnection, "Artemis Connection");
        }
    }

    private String extractPayload(Message message) throws JMSException {
        if (message instanceof TextMessage textMessage) return textMessage.getText();

        if (message instanceof BytesMessage bytesMessage) {
            long len = bytesMessage.getBodyLength();
            if (len > Integer.MAX_VALUE) throw new IllegalStateException("Artemis message too large: " + len);

            byte[] body = new byte[(int) len];
            bytesMessage.readBytes(body);
            return new String(body, StandardCharsets.UTF_8);
        }

        throw new IllegalStateException("Unsupported Artemis message type: " + message.getClass().getName());
    }

    private String extractMessageId(Message message) throws JMSException {
        String id = message.getJMSMessageID();
        if (id == null || id.isBlank()) throw new IllegalStateException("Artemis message has empty JMSMessageID");
        return id;
    }

    private Long extractJmsTimestamp(Message message) throws JMSException {
        long ts = message.getJMSTimestamp();
        return ts > 0 ? ts : null;
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
                    .put("component", "ArtemisSource")
                    .put("brokerUrl", maskBrokerUrl(brokerUrl))
                    .put("queue", queueName)
                    .put("receiveTimeoutMs", receiveTimeoutMs)
                    .put("user", username == null || username.isBlank() ? "<empty>" : username);

            if (error != null) builder.put("errorClass", error.getClass().getName()).put("errorMessage", error.getMessage());
            auditPublisher.publish(builder.build());
        } catch (Exception e) {
            log.warn("Failed to publish ArtemisSource audit event type={}", type, e);
        }
    }

    private static String maskBrokerUrl(String url) {
        if (url == null) return null;
        return url.replaceAll("(?i)(trustStorePassword=)[^&]*", "$1***")
                .replaceAll("(?i)(keyStorePassword=)[^&]*", "$1***");
    }
}