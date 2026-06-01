package com.gpb.datafirewall.artemis;

import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gpb.datafirewall.services.MessageRecord;
import com.gpb.datafirewall.audit.AuditConfig;
import com.gpb.datafirewall.audit.AuditEventType;
import com.gpb.datafirewall.audit.CefAuditEvent;
import com.gpb.datafirewall.audit.CefAuditPublisher;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.nio.charset.StandardCharsets;
import java.time.Instant;

public class ArtemisSource extends RichSourceFunction<MessageRecord> {

    private static final Logger log = LoggerFactory.getLogger(ArtemisSource.class);

    private final String brokerUrl;
    private final String username;
    private final String password;
    private final String queueName;
    private final long receiveTimeoutMs;
    private final AuditConfig auditConfig;

    private transient volatile boolean running;
    private transient CefAuditPublisher auditPublisher;

    private transient ActiveMQConnectionFactory connectionFactory;
    private transient Connection connection;
    private transient Session session;
    private transient MessageConsumer consumer;

    public ArtemisSource(
            String brokerUrl,
            String username,
            String password,
            String queueName,
            long receiveTimeoutMs
    ) {
        this(brokerUrl, username, password, queueName, receiveTimeoutMs, null);
    }

    public ArtemisSource(
            String brokerUrl,
            String username,
            String password,
            String queueName,
            long receiveTimeoutMs,
            AuditConfig auditConfig
    ) {
        this.brokerUrl = brokerUrl;
        this.username = username;
        this.password = password;
        this.queueName = queueName;
        this.receiveTimeoutMs = receiveTimeoutMs;
        this.auditConfig = auditConfig;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        running = true;

        try {
            auditPublisher = new CefAuditPublisher(auditConfig);
            publishAudit(AuditEventType.ARTEMIS_SOURCE_CONNECTING, "SUCCESS", null);

            log.info(
                    "ArtemisSource connecting: subtask={}, brokerUrl={}, queue={}, user={}",
                    getRuntimeContext().getIndexOfThisSubtask(),
                    maskBrokerUrl(brokerUrl),
                    queueName,
                    username == null || username.isBlank() ? "<empty>" : "<set>"
            );

            connectionFactory = new ActiveMQConnectionFactory(brokerUrl, username, password);
            connection = connectionFactory.createConnection();
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            Queue queue = session.createQueue(queueName);
            consumer = session.createConsumer(queue);

            connection.start();

            log.info(
                    "ArtemisSource opened: subtask={}, brokerUrl={}, queue={}, receiveTimeoutMs={}",
                    getRuntimeContext().getIndexOfThisSubtask(),
                    maskBrokerUrl(brokerUrl),
                    queueName,
                    receiveTimeoutMs
            );
            publishAudit(AuditEventType.ARTEMIS_SOURCE_CONNECTED, "SUCCESS", null);
        } catch (Exception e) {
            publishAudit(AuditEventType.ARTEMIS_SOURCE_CONNECTION_FAILED, "FAILED", e);
            close();
            throw new RuntimeException(
                    "Failed to open ArtemisSource. brokerUrl=" + maskBrokerUrl(brokerUrl) +
                            ", queue=" + queueName,
                    e
            );
        }
    }

    @Override
    public void run(SourceContext<MessageRecord> ctx) throws Exception {
        while (running) {
            Message message;
            try {
                message = consumer.receive(receiveTimeoutMs);
            } catch (Exception e) {
                if (!running) {
                    log.info("ArtemisSource receive loop stopped during shutdown.");
                    return;
                }
                throw e;
            }

            Long readedDttm = Instant.now().toEpochMilli();

            if (message == null) {
                continue;
            }

            String payload = extractPayload(message);
            String msgId = extractMessageId(message);
            Long jmsTimestamp = extractJmsTimestamp(message);

            synchronized (ctx.getCheckpointLock()) {
                ctx.collect(MessageRecord.fromJms(msgId, payload, jmsTimestamp, readedDttm));
            }
        }
    }

    @Override
    public void cancel() {
        running = false;
        close();
    }

    @Override
    public void close() {
        publishAudit(AuditEventType.ARTEMIS_SOURCE_DISCONNECTED, "SUCCESS", null);
        closeQuietly(consumer, "Artemis MessageConsumer");
        consumer = null;

        closeQuietly(session, "Artemis Session");
        session = null;

        closeQuietly(connection, "Artemis Connection");
        connection = null;

        closeQuietly(connectionFactory, "Artemis ConnectionFactory");
        connectionFactory = null;

        closeQuietly(auditPublisher, "BusinessAuditPublisher");
        auditPublisher = null;
    }

    private String extractPayload(Message message) throws JMSException {
        if (message instanceof TextMessage textMessage) {
            return textMessage.getText();
        }

        if (message instanceof BytesMessage bytesMessage) {
            long len = bytesMessage.getBodyLength();
            if (len > Integer.MAX_VALUE) {
                throw new IllegalStateException("Artemis message too large: " + len);
            }

            byte[] body = new byte[(int) len];
            bytesMessage.readBytes(body);
            return new String(body, StandardCharsets.UTF_8);
        }

        throw new IllegalStateException(
                "Unsupported Artemis message type: " + message.getClass().getName()
        );
    }

    private String extractMessageId(Message message) throws JMSException {
        String jmsMessageId = message.getJMSMessageID();
        if (jmsMessageId == null || jmsMessageId.isBlank()) {
            throw new IllegalStateException("Artemis message has empty JMSMessageID, cannot build reply correlation id");
        }
        return jmsMessageId;
    }

    private void closeQuietly(AutoCloseable c, String resourceName) {
        if (c == null) {
            return;
        }
        try {
            c.close();
        } catch (Exception e) {
            log.warn("Failed to close {}", resourceName, e);
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
                .put("component", "ArtemisSource")
                .put("brokerUrl", maskBrokerUrl(brokerUrl))
                .put("queue", queueName)
                .put("receiveTimeoutMs", receiveTimeoutMs)
                .put("user", username == null || username.isBlank() ? "<empty>" : username);
        if (error != null) {
            builder.put("errorClass", error.getClass().getName())
                    .put("errorMessage", error.getMessage());
        }
        auditPublisher.publish(builder.build());
    }

    // Метод для маскирования паролей в trustStorePassword и keyStorePassword
    private static String maskBrokerUrl(String url) {
        if (url == null) {
            return null;
        }

        return url
                .replaceAll("(?i)(trustStorePassword=)[^&]*", "$1***")
                .replaceAll("(?i)(keyStorePassword=)[^&]*", "$1***");
    }

    private Long extractJmsTimestamp(Message message) throws JMSException {
        long ts = message.getJMSTimestamp();
        return ts > 0 ? ts : null;
    }
}