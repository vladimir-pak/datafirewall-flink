package ru.gpbapp.datafirewallflink.artemis;

import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.gpbapp.datafirewallflink.services.MessageRecord;

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
import java.util.UUID;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Locale;

public class ArtemisSource extends RichSourceFunction<MessageRecord> {

    private static final Logger log = LoggerFactory.getLogger(ArtemisSource.class);

    private final String brokerUrl;
    private final String username;
    private final String password;
    private final String queueName;
    private final long receiveTimeoutMs;

    private final boolean tlsEnabled;
    private final String trustStore;
    private final String trustStorePassword;

    private transient volatile boolean running;

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
        this(
                brokerUrl,
                username,
                password,
                queueName,
                receiveTimeoutMs,
                false,
                null,
                null
        );
    }

    public ArtemisSource(
            String brokerUrl,
            String username,
            String password,
            String queueName,
            long receiveTimeoutMs,
            boolean tlsEnabled,
            String trustStore,
            String trustStorePassword
    ) {
        this.brokerUrl = brokerUrl;
        this.username = username;
        this.password = password;
        this.queueName = queueName;
        this.receiveTimeoutMs = receiveTimeoutMs;
        this.tlsEnabled = tlsEnabled;
        this.trustStore = trustStore;
        this.trustStorePassword = trustStorePassword;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        running = true;

        try {
            if (tlsEnabled) {
                setSystemPropertyIfPresent("javax.net.ssl.trustStore", trustStore);
                setSystemPropertyIfPresent("javax.net.ssl.trustStorePassword", trustStorePassword);
            }

            log.info(
                    "ArtemisSource connecting: subtask={}, brokerUrl={}, queue={}, user={}, tls={}, trustStore={}",
                    getRuntimeContext().getIndexOfThisSubtask(),
                    brokerUrl,
                    queueName,
                    username == null || username.isBlank() ? "<empty>" : "<set>",
                    tlsEnabled,
                    trustStore == null || trustStore.isBlank() ? "<empty>" : trustStore
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
                    brokerUrl,
                    queueName,
                    receiveTimeoutMs
            );
        } catch (Exception e) {
            close();
            throw new RuntimeException(
                    "Failed to open ArtemisSource. brokerUrl=" + brokerUrl +
                            ", queue=" + queueName +
                            ", tlsEnabled=" + tlsEnabled,
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

            if (message == null) {
                continue;
            }
//            String readedFromMQDttm = Instant.now()
//                    .atOffset(ZoneOffset.UTC)
//                    .format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
            String payload = extractPayload(message);
            byte[] msgId = extractMessageId(message);
//            String createdDttm = extractIngressTimestamp(message);

            synchronized (ctx.getCheckpointLock()) {
//                ctx.collect(new MessageRecord(msgId, payload, createdDttm, readedFromMQDttm));
                ctx.collect(new MessageRecord(msgId, payload));
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
        closeQuietly(consumer, "Artemis MessageConsumer");
        consumer = null;

        closeQuietly(session, "Artemis Session");
        session = null;

        closeQuietly(connection, "Artemis Connection");
        connection = null;

        closeQuietly(connectionFactory, "Artemis ConnectionFactory");
        connectionFactory = null;
    }

    private String extractIngressTimestamp(Message message) throws JMSException {
        try {
            Object timestampObj = message.getObjectProperty("_AMQ_INGRESS_TIMESTAMP");
            if (timestampObj != null) {
                String ingressTimestampStr = timestampObj.toString();

                // Проверяем, что это число (миллисекунды)
                if (isNumeric(ingressTimestampStr)) {
                    long timestampMs = Long.parseLong(ingressTimestampStr);

                    // Unix timestamp (ms) → Instant → ZonedDateTime → ISO 8601 с 7 цифрами ns
                    Instant instant = Instant.ofEpochMilli(timestampMs);
                    ZonedDateTime zdt = instant.atZone(ZoneOffset.UTC);

                    // Формат: "2026-04-21T06:51:54.6896660Z" (7 цифр после точки)
                    DateTimeFormatter formatter = DateTimeFormatter.ofPattern(
                            "yyyy-MM-dd'T'HH:mm:ss.SAAAAAA'Z'",
                            Locale.ROOT
                    ).withZone(ZoneOffset.UTC);

                    return zdt.format(formatter);
                }
            }
        } catch (Exception e) {
            log.warn("Failed to parse _AMQ_INGRESS_TIMESTAMP: {}", e.getMessage());
        }

        // Fallback: текущее время UTC
        return Instant.now()
                .atZone(ZoneOffset.UTC)
                .format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SAAAAAA'Z'", Locale.ROOT));
    }

    // Вспомогательный метод для проверки числа
    private boolean isNumeric(String str) {
        return str != null && str.matches("-?\\d+");
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

    private byte[] extractMessageId(Message message) throws JMSException {
        String jmsMessageId = message.getJMSMessageID();
        if (jmsMessageId == null || jmsMessageId.isBlank()) {
            jmsMessageId = UUID.randomUUID().toString();
        }
        return jmsMessageId.getBytes(StandardCharsets.UTF_8);
    }

    private void setSystemPropertyIfPresent(String key, String value) {
        if (value != null && !value.isBlank()) {
            System.setProperty(key, value);
        }
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
}