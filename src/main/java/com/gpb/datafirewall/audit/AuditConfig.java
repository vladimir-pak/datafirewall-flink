package com.gpb.datafirewall.audit;

import com.gpb.datafirewall.vault.dto.VaultSecretsDto;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.File;
import java.io.Serial;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.security.CodeSource;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class AuditConfig implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    public static final String CEF_VERSION = "0";
    public static final String CEF_PROJECT = "MKD";
    public static final String CEF_DOMAIN = "GPB";
    public static final String CEF_SEVERITY = "1";
    public static final String DEFAULT_PROTOCOL = "TCP";

    private static final Pattern VERSION_IN_JAR = Pattern.compile("^(.+?)-([0-9][A-Za-z0-9._-]*(?:SNAPSHOT|RELEASE|FINAL|RC[0-9]*)?)$");

    private final boolean enabled;
    private final boolean fileEnabled;
    private final boolean kafkaEnabled;
    private final boolean failOnError;
    private final String cefFilePath;
    private final String kafkaBootstrap;
    private final String kafkaTopic;
    private final boolean kafkaTlsEnabled;
    private final boolean kafkaSaslEnabled;
    private final String kafkaSecurityProtocol;
    private final String kafkaSaslMechanism;
    private final String kafkaJaasConfig;
    private final String kafkaUser;
    private final String kafkaTruststoreLocation;
    private final String kafkaTruststoreType;
    private final String kafkaTruststorePassword;
    private final String kafkaKeystoreLocation;
    private final String kafkaKeystoreType;
    private final String kafkaKeystorePassword;
    private final String kafkaKeyPassword;
    private final String kafkaEndpointIdentificationAlgorithm;
    private final String jobName;
    private final String jobVersion;
    private final String clusterId;
    private final String hostname;
    private final String appName;
    private final String appVersion;
    private final String sourceUser;
    private final String sourceIp;
    private final String sourceMac;
    private final String sourceHost;
    private final int sourcePort;

    private AuditConfig(
            boolean enabled,
            boolean fileEnabled,
            boolean kafkaEnabled,
            boolean failOnError,
            String cefFilePath,
            String kafkaBootstrap,
            String kafkaTopic,
            boolean kafkaTlsEnabled,
            boolean kafkaSaslEnabled,
            String kafkaSecurityProtocol,
            String kafkaSaslMechanism,
            String kafkaJaasConfig,
            String kafkaUser,
            String kafkaTruststoreLocation,
            String kafkaTruststoreType,
            String kafkaTruststorePassword,
            String kafkaKeystoreLocation,
            String kafkaKeystoreType,
            String kafkaKeystorePassword,
            String kafkaKeyPassword,
            String kafkaEndpointIdentificationAlgorithm,
            String jobName,
            String jobVersion,
            String clusterId,
            String hostname,
            String appName,
            String appVersion,
            String sourceUser,
            String sourceIp,
            String sourceMac,
            String sourceHost,
            int sourcePort
    ) {
        this.enabled = enabled;
        this.fileEnabled = fileEnabled;
        this.kafkaEnabled = kafkaEnabled;
        this.failOnError = failOnError;
        this.cefFilePath = cefFilePath;
        this.kafkaBootstrap = kafkaBootstrap;
        this.kafkaTopic = kafkaTopic;
        this.kafkaTlsEnabled = kafkaTlsEnabled;
        this.kafkaSaslEnabled = kafkaSaslEnabled;
        this.kafkaSecurityProtocol = kafkaSecurityProtocol;
        this.kafkaSaslMechanism = kafkaSaslMechanism;
        this.kafkaJaasConfig = kafkaJaasConfig;
        this.kafkaUser = kafkaUser;
        this.kafkaTruststoreLocation = kafkaTruststoreLocation;
        this.kafkaTruststoreType = kafkaTruststoreType;
        this.kafkaTruststorePassword = kafkaTruststorePassword;
        this.kafkaKeystoreLocation = kafkaKeystoreLocation;
        this.kafkaKeystoreType = kafkaKeystoreType;
        this.kafkaKeystorePassword = kafkaKeystorePassword;
        this.kafkaKeyPassword = kafkaKeyPassword;
        this.kafkaEndpointIdentificationAlgorithm = kafkaEndpointIdentificationAlgorithm;
        this.jobName = jobName;
        this.jobVersion = jobVersion;
        this.clusterId = clusterId;
        this.hostname = hostname;
        this.appName = appName;
        this.appVersion = appVersion;
        this.sourceUser = sourceUser;
        this.sourceIp = sourceIp;
        this.sourceMac = sourceMac;
        this.sourceHost = sourceHost;
        this.sourcePort = sourcePort;
    }

    public static AuditConfig from(ParameterTool pt, VaultSecretsDto vaultSecrets, String defaultKafkaBootstrap) {
        ParameterTool params = pt == null ? ParameterTool.fromMap(Map.of()) : pt;

        String kafkaBootstrap = kafkaParam(params, "bootstrap", defaultKafkaBootstrap);

        String kafkaTopic = auditKafkaTopicParam(params, "datafirewall.audit");

        String cefPath = params.get("cef.audit.cef.path",
                params.get("audit.cef.path", "cef.log"));

        String securityProtocol = kafkaParam(params, "security.protocol", null);
        boolean tlsEnabled = kafkaBooleanParam(params, "tls.enabled", false) || isSslProtocol(securityProtocol);
        boolean saslEnabled = kafkaBooleanParam(params, "sasl.enabled", false) || isSaslProtocol(securityProtocol);

        if (securityProtocol == null || securityProtocol.isBlank()) {
            if (saslEnabled) {
                securityProtocol = tlsEnabled ? "SASL_SSL" : "SASL_PLAINTEXT";
            } else if (tlsEnabled) {
                securityProtocol = "SSL";
            }
        }

        String saslMechanism = kafkaParam(params, "sasl.mechanism", "SCRAM-SHA-512");
        String jaasConfig = saslEnabled ? buildKafkaJaasConfig(saslMechanism, vaultSecrets) : null;
        String truststoreLocation = kafkaParam(params, "ssl.truststore.location", null);
        String truststoreType = kafkaParam(params, "ssl.truststore.type", null);
        String keystoreLocation = kafkaParam(params, "ssl.keystore.location", null);
        String keystoreType = kafkaParam(params, "ssl.keystore.type", null);
        String keyPassword = kafkaParam(params, "ssl.key.password", vaultSecrets == null ? null : vaultSecrets.keystorePassword());
        String endpointIdentification = kafkaParam(params, "ssl.endpoint.identification.algorithm", "https");

        JarApplicationInfo jarInfo = resolveJarApplicationInfo();
        String hostName = firstNotBlank(params.get("cef.audit.host.name", null), resolveHostname());
        String srcIp = firstNotBlank(params.get("cef.audit.src.ip", null), resolveLocalIp());
        String srcMac = firstNotBlank(params.get("cef.audit.src.mac", null), resolveLocalMac(srcIp));

        return new AuditConfig(
                params.getBoolean("cef.audit.enabled", true),
                params.getBoolean("cef.audit.file.enabled", true),
                params.getBoolean("cef.audit.kafka.enabled", true),
                params.getBoolean("cef.audit.fail-on-error", false),
                cefPath,
                kafkaBootstrap,
                kafkaTopic,
                tlsEnabled,
                saslEnabled,
                securityProtocol,
                saslMechanism,
                jaasConfig,
                vaultSecrets == null ? null : vaultSecrets.kafkaUser(),
                truststoreLocation,
                truststoreType,
                vaultSecrets == null ? null : vaultSecrets.truststorePassword(),
                keystoreLocation,
                keystoreType,
                vaultSecrets == null ? null : vaultSecrets.keystorePassword(),
                keyPassword,
                endpointIdentification,
                params.get("job.name", jarInfo.appName()),
                params.get("job.version", jarInfo.appVersion()),
                params.get("flink.cluster.id", params.get("cluster.id", "default")),
                hostName,
                params.get("cef.audit.app.name", jarInfo.appName()),
                params.get("cef.audit.app.version", jarInfo.appVersion()),
                params.get("cef.audit.source.user", System.getProperty("user.name", "unknown")),
                srcIp,
                srcMac,
                hostName,
                resolveSourcePort(params)
        );
    }

    public CefAuditEvent.Builder enrich(CefAuditEvent.Builder builder) {
        return builder
                .jobName(jobName)
                .jobVersion(jobVersion)
                .clusterId(clusterId)
                .hostname(hostname);
    }

    public Properties toKafkaProducerProperties() {
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrap);
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        props.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.setProperty(ProducerConfig.RETRIES_CONFIG, "10");
        props.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        props.setProperty(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "120000");
        props.setProperty(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "30000");

        putIfNotBlank(props, "security.protocol", kafkaSecurityProtocol);

        if (kafkaSaslEnabled) {
            putIfNotBlank(props, "sasl.mechanism", kafkaSaslMechanism);
            putIfNotBlank(props, "sasl.jaas.config", kafkaJaasConfig);
        }

        if (kafkaTlsEnabled) {
            putIfNotBlank(props, "ssl.truststore.location", kafkaTruststoreLocation);
            putIfNotBlank(props, "ssl.truststore.type", kafkaTruststoreType);
            putIfNotBlank(props, "ssl.truststore.password", kafkaTruststorePassword);
            putIfNotBlank(props, "ssl.keystore.location", kafkaKeystoreLocation);
            putIfNotBlank(props, "ssl.keystore.type", kafkaKeystoreType);
            putIfNotBlank(props, "ssl.keystore.password", kafkaKeystorePassword);
            putIfNotBlank(props, "ssl.key.password", kafkaKeyPassword);
            putIfNotBlank(props, "ssl.endpoint.identification.algorithm", kafkaEndpointIdentificationAlgorithm);
        }
        return props;
    }

    public boolean enabled() { return enabled; }
    public boolean fileEnabled() { return fileEnabled; }
    public boolean kafkaEnabled() { return kafkaEnabled; }
    public boolean failOnError() { return failOnError; }
    public String cefFilePath() { return cefFilePath; }
    public String kafkaBootstrap() { return kafkaBootstrap; }
    public String kafkaTopic() { return kafkaTopic; }
    public String jobName() { return jobName; }
    public String jobVersion() { return jobVersion; }
    public String clusterId() { return clusterId; }
    public String hostname() { return hostname; }
    public String appName() { return appName; }
    public String appVersion() { return appVersion; }
    public String sourceUser() { return sourceUser; }
    public String sourceIp() { return sourceIp; }
    public String sourceMac() { return sourceMac; }
    public String sourceHost() { return sourceHost; }
    public int sourcePort() { return sourcePort; }
    public String kafkaSecurityProtocol() { return kafkaSecurityProtocol == null || kafkaSecurityProtocol.isBlank() ? DEFAULT_PROTOCOL : kafkaSecurityProtocol; }
    public String kafkaUser() { return kafkaUser; }

    private static String kafkaParam(ParameterTool params, String suffix, String defaultValue) {
        String value = params.get("cef.audit.kafka." + suffix, null);
        if (value != null && !value.isBlank()) {
            return value;
        }

        value = params.get("audit.kafka." + suffix, null);
        if (value != null && !value.isBlank()) {
            return value;
        }

        value = params.get("kafka." + suffix, null);
        if (value != null && !value.isBlank()) {
            return value;
        }

        return defaultValue;
    }

    private static String auditKafkaTopicParam(ParameterTool params, String defaultValue) {
        String value = params.get("cef.audit.kafka.topic", null);
        if (value != null && !value.isBlank()) {
            return value;
        }

        value = params.get("audit.kafka.topic", null);
        if (value != null && !value.isBlank()) {
            return value;
        }

        return defaultValue;
    }

    private static boolean kafkaBooleanParam(ParameterTool params, String suffix, boolean defaultValue) {
        String value = kafkaParam(params, suffix, null);
        return value == null ? defaultValue : Boolean.parseBoolean(value);
    }

    private static String buildKafkaJaasConfig(String mechanism, VaultSecretsDto vaultSecrets) {
        requireNotBlank(vaultSecrets == null ? null : vaultSecrets.kafkaUser(), "kafkaUser in vault");
        requireNotBlank(vaultSecrets == null ? null : vaultSecrets.kafkaPassword(), "kafkaPassword in vault");

        String loginModule = "PLAIN".equalsIgnoreCase(mechanism)
                ? "org.apache.kafka.common.security.plain.PlainLoginModule"
                : "org.apache.kafka.common.security.scram.ScramLoginModule";

        return loginModule
                + " required username=\"" + escapeJaas(vaultSecrets.kafkaUser())
                + "\" password=\"" + escapeJaas(vaultSecrets.kafkaPassword()) + "\";";
    }

    private static String escapeJaas(String value) {
        return value == null ? "" : value.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    private static boolean isSslProtocol(String securityProtocol) {
        return securityProtocol != null && securityProtocol.toUpperCase().contains("SSL");
    }

    private static boolean isSaslProtocol(String securityProtocol) {
        return securityProtocol != null && securityProtocol.toUpperCase().startsWith("SASL_");
    }

    private static void requireNotBlank(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must be provided");
        }
    }

    private static void putIfNotBlank(Properties props, String key, String value) {
        if (value != null && !value.isBlank()) {
            props.setProperty(key, value);
        }
    }

    private static String resolveHostname() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            return "unknown";
        }
    }

    private static String resolveLocalIp() {
        try {
            return InetAddress.getLocalHost().getHostAddress();
        } catch (Exception e) {
            return "127.0.0.1";
        }
    }

    private static String resolveLocalMac(String ip) {
        try {
            InetAddress address = ip == null || ip.isBlank() ? InetAddress.getLocalHost() : InetAddress.getByName(ip);
            NetworkInterface ni = NetworkInterface.getByInetAddress(address);
            if (ni == null || ni.getHardwareAddress() == null) {
                return "";
            }
            byte[] mac = ni.getHardwareAddress();
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < mac.length; i++) {
                if (i > 0) { sb.append('-'); }
                sb.append(String.format("%02X", mac[i]));
            }
            return sb.toString();
        } catch (Exception e) {
            return "";
        }
    }

    private static int resolveSourcePort(ParameterTool params) {
        int port = params.getInt("cef.audit.source.port", -1);
        if (port >= 0) { return port; }
        port = params.getInt("rest.port", -1);
        if (port >= 0) { return port; }
        port = params.getInt("jobmanager.rpc.port", -1);
        return Math.max(port, 0);
    }

    private static JarApplicationInfo resolveJarApplicationInfo() {
        String fallbackName = "datafirewall-flink";
        String fallbackVersion = readImplementationVersion();
        try {
            CodeSource codeSource = AuditConfig.class.getProtectionDomain().getCodeSource();
            if (codeSource == null || codeSource.getLocation() == null) {
                return new JarApplicationInfo(fallbackName, fallbackVersion);
            }
            File file = Path.of(codeSource.getLocation().toURI()).toFile();
            String fileName = file.getName();
            if (file.isDirectory() || fileName.isBlank()) {
                return new JarApplicationInfo(fallbackName, fallbackVersion);
            }
            String baseName = stripJarExtension(fileName);
            Matcher matcher = VERSION_IN_JAR.matcher(baseName);
            if (matcher.matches()) {
                return new JarApplicationInfo(matcher.group(1), matcher.group(2));
            }
            return new JarApplicationInfo(baseName, fallbackVersion);
        } catch (IllegalArgumentException | URISyntaxException e) {
            return new JarApplicationInfo(fallbackName, fallbackVersion);
        }
    }

    private static String stripJarExtension(String fileName) {
        return fileName.toLowerCase().endsWith(".jar") ? fileName.substring(0, fileName.length() - 4) : fileName;
    }

    private static String readImplementationVersion() {
        Package p = AuditConfig.class.getPackage();
        String v = p == null ? null : p.getImplementationVersion();
        return v == null || v.isBlank() ? "unknown" : v;
    }

    private static String firstNotBlank(String first, String second) {
        return first == null || first.isBlank() ? second : first;
    }

    private record JarApplicationInfo(String appName, String appVersion) { }
}
