package ru.gpb.datafirewall.artemis;

import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ru.gpb.datafirewall.services.MessageReply;

import javax.jms.Connection;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

public class ArtemisSink extends RichSinkFunction<MessageReply> {

    private static final Logger log = LoggerFactory.getLogger(ArtemisSink.class);

    private final String brokerUrl;
    private final String username;
    private final String password;
    private final String queueName;

    private final boolean tlsEnabled;
    private final String trustStore;
    private final String trustStorePassword;

    private transient ActiveMQConnectionFactory connectionFactory;
    private transient Connection connection;
    private transient Session session;
    private transient MessageProducer producer;

    public ArtemisSink(
            String brokerUrl,
            String username,
            String password,
            String queueName
    ) {
        this(
                brokerUrl,
                username,
                password,
                queueName,
                false,
                null,
                null
        );
    }

    public ArtemisSink(
            String brokerUrl,
            String username,
            String password,
            String queueName,
            boolean tlsEnabled,
            String trustStore,
            String trustStorePassword
    ) {
        this.brokerUrl = brokerUrl;
        this.username = username;
        this.password = password;
        this.queueName = queueName;
        this.tlsEnabled = tlsEnabled;
        this.trustStore = trustStore;
        this.trustStorePassword = trustStorePassword;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        try {
            if (tlsEnabled) {
                setSystemPropertyIfPresent("javax.net.ssl.trustStore", trustStore);
                setSystemPropertyIfPresent("javax.net.ssl.trustStorePassword", trustStorePassword);
            }

            log.info(
                    "ArtemisSink connecting: subtask={}, brokerUrl={}, queue={}, user={}, tls={}, trustStore={}",
                    getRuntimeContext().getIndexOfThisSubtask(),
                    brokerUrl,
                    queueName,
                    username,
                    tlsEnabled,
                    trustStore == null || trustStore.isBlank() ? "<empty>" : trustStore
            );

            connectionFactory = new ActiveMQConnectionFactory(brokerUrl, username, password);
            connection = connectionFactory.createConnection();
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            Queue queue = session.createQueue(queueName);
            producer = session.createProducer(queue);

            connection.start();

            log.info(
                    "ArtemisSink opened: subtask={}, brokerUrl={}, queue={}",
                    getRuntimeContext().getIndexOfThisSubtask(),
                    brokerUrl,
                    queueName
            );
        } catch (Exception e) {
            close();
            throw new RuntimeException(
                    "Failed to open ArtemisSink. brokerUrl=" + brokerUrl +
                            ", queue=" + queueName +
                            ", tlsEnabled=" + tlsEnabled,
                    e
            );
        }
    }

    @Override
    public void invoke(MessageReply value, Context context) throws Exception {
        if (value == null) {
            return;
        }

        if (value.jmsCorrelationId == null || value.jmsCorrelationId.isBlank()) {
            throw new IllegalArgumentException("Artemis reply requires jmsCorrelationId");
        }

        TextMessage msg = session.createTextMessage(value.payload == null ? "" : value.payload);
        msg.setJMSCorrelationID(value.jmsCorrelationId);

        producer.send(msg);
    }

    @Override
    public void close() {
        closeQuietly(producer, "Artemis MessageProducer");
        producer = null;

        closeQuietly(session, "Artemis Session");
        session = null;

        closeQuietly(connection, "Artemis Connection");
        connection = null;

        closeQuietly(connectionFactory, "Artemis ConnectionFactory");
        connectionFactory = null;
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