package com.gpb.datafirewall.kafka;

import java.util.Properties;

/**
 * Builder для конфигурации подключения к Kafka.
 * Аналог ArtemisConnectionUrlBuilder для единообразия архитектуры.
 */
public final class KafkaConnectionUrlBuilder {

    private KafkaConnectionUrlBuilder() {
    }

    /**
     * Строит Properties для Kafka consumer/producer на основе конфигурации.
     */
    public static Properties buildFromConfig(
            String bootstrapServers,
            String groupId,
            boolean tlsEnabled,
            String securityProtocol,
            String saslMechanism,
            String trustStoreLocation,
            String trustStorePassword,
            String keyStoreLocation,
            String keyStorePassword,
            String kafkaUser,
            String kafkaPassword
    ) {
        if (bootstrapServers == null || bootstrapServers.isBlank()) {
            throw new IllegalArgumentException("kafka.bootstrap.servers must be provided");
        }

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", bootstrapServers.trim());

        if (groupId != null && !groupId.isBlank()) {
            props.setProperty("group.id", groupId.trim());
        }

        // TLS/SASL конфигурация
        if (tlsEnabled || isSslProtocol(securityProtocol) || isSaslProtocol(securityProtocol)) {
            configureSecurity(props, securityProtocol, saslMechanism,
                    trustStoreLocation, trustStorePassword,
                    keyStoreLocation, keyStorePassword,
                    kafkaUser, kafkaPassword);
        }

        return props;
    }

    /**
     * Строит Properties для Kafka consumer.
     */
    public static Properties buildConsumerConfig(
            String bootstrapServers,
            String groupId,
            boolean tlsEnabled,
            String securityProtocol,
            String saslMechanism,
            String trustStoreLocation,
            String trustStorePassword,
            String keyStoreLocation,
            String keyStorePassword,
            String kafkaUser,
            String kafkaPassword
    ) {
        Properties props = buildFromConfig(
                bootstrapServers, groupId, tlsEnabled, securityProtocol, saslMechanism,
                trustStoreLocation, trustStorePassword,
                keyStoreLocation, keyStorePassword,
                kafkaUser, kafkaPassword
        );

        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("auto.offset.reset", "latest");
        props.setProperty("enable.auto.commit", "false");

        return props;
    }

    /**
     * Строит Properties для Kafka producer.
     */
    public static Properties buildProducerConfig(
            String bootstrapServers,
            boolean tlsEnabled,
            String securityProtocol,
            String saslMechanism,
            String trustStoreLocation,
            String trustStorePassword,
            String keyStoreLocation,
            String keyStorePassword,
            String kafkaUser,
            String kafkaPassword
    ) {
        Properties props = buildFromConfig(
                bootstrapServers, null, tlsEnabled, securityProtocol, saslMechanism,
                trustStoreLocation, trustStorePassword,
                keyStoreLocation, keyStorePassword,
                kafkaUser, kafkaPassword
        );

        props.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.setProperty("acks", "all");
        props.setProperty("retries", "3");
        props.setProperty("delivery.timeout.ms", "120000");
        props.setProperty("request.timeout.ms", "30000");

        return props;
    }

    private static void configureSecurity(
            Properties props,
            String securityProtocol,
            String saslMechanism,
            String trustStoreLocation,
            String trustStorePassword,
            String keyStoreLocation,
            String keyStorePassword,
            String kafkaUser,
            String kafkaPassword
    ) {
        // Security protocol
        if (securityProtocol != null && !securityProtocol.isBlank()) {
            props.setProperty("security.protocol", securityProtocol);
        } else {
            // Определяем автоматически
            boolean hasSasl = kafkaUser != null && !kafkaUser.isBlank();
            boolean hasTls = trustStoreLocation != null && !trustStoreLocation.isBlank();

            if (hasSasl && hasTls) {
                props.setProperty("security.protocol", "SASL_SSL");
            } else if (hasSasl) {
                props.setProperty("security.protocol", "SASL_PLAINTEXT");
            } else if (hasTls) {
                props.setProperty("security.protocol", "SSL");
            }
        }

        String protocol = props.getProperty("security.protocol", "");

        // SASL конфигурация
        if (protocol.startsWith("SASL_")) {
            String mechanism = saslMechanism != null && !saslMechanism.isBlank()
                    ? saslMechanism : "SCRAM-SHA-512";
            props.setProperty("sasl.mechanism", mechanism);

            if (kafkaUser != null && !kafkaUser.isBlank() &&
                    kafkaPassword != null && !kafkaPassword.isBlank()) {
                String loginModule = "PLAIN".equalsIgnoreCase(mechanism)
                        ? "org.apache.kafka.common.security.plain.PlainLoginModule"
                        : "org.apache.kafka.common.security.scram.ScramLoginModule";

                String jaasConfig = loginModule +
                        " required username=\"" + escapeJaas(kafkaUser) +
                        "\" password=\"" + escapeJaas(kafkaPassword) + "\";";
                props.setProperty("sasl.jaas.config", jaasConfig);
            }
        }

        // TLS конфигурация
        if (protocol.contains("SSL")) {
            if (trustStoreLocation != null && !trustStoreLocation.isBlank()) {
                props.setProperty("ssl.truststore.location", trustStoreLocation);
            }
            if (trustStorePassword != null && !trustStorePassword.isBlank()) {
                props.setProperty("ssl.truststore.password", trustStorePassword);
                props.setProperty("ssl.truststore.type", "PKCS12");
            }
            if (keyStoreLocation != null && !keyStoreLocation.isBlank()) {
                props.setProperty("ssl.keystore.location", keyStoreLocation);
            }
            if (keyStorePassword != null && !keyStorePassword.isBlank()) {
                props.setProperty("ssl.keystore.password", keyStorePassword);
                props.setProperty("ssl.key.password", keyStorePassword);
                props.setProperty("ssl.keystore.type", "PKCS12");
            }
            props.setProperty("ssl.endpoint.identification.algorithm", "https");
        }
    }

    private static boolean isSslProtocol(String securityProtocol) {
        return securityProtocol != null && securityProtocol.toUpperCase().contains("SSL");
    }

    private static boolean isSaslProtocol(String securityProtocol) {
        return securityProtocol != null && securityProtocol.toUpperCase().startsWith("SASL_");
    }

    private static String escapeJaas(String value) {
        return value == null ? "" : value.replace("\\", "\\\\").replace("\"", "\\\"");
    }
}