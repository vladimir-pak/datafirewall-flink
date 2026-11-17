package com.gpb.datafirewall.transaction.artemis;

import com.gpb.datafirewall.util.SslStoreTypes;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import org.apache.activemq.artemis.utils.uri.URISupport;

public final class ArtemisConnectionUrlBuilder {
    private ArtemisConnectionUrlBuilder() { }

    public static String buildFromBrokerUrl(
            String brokerUrl, 
            boolean tlsEnabled,
            String trustStorePath, 
            String trustStorePassword, 
            String keyStorePath,
            String keyStorePassword, 
            String cipherSuites) {

        return buildFromBrokerUrl(
                brokerUrl, 
                tlsEnabled, 
                trustStorePath, 
                trustStorePassword,
                keyStorePath, 
                keyStorePassword, 
                cipherSuites, 
                null, 
                null
        );
    }

    public static String buildFromBrokerUrl(
            String brokerUrl, 
            boolean tlsEnabled,
            String trustStorePath, 
            String trustStorePassword, 
            String keyStorePath,
            String keyStorePassword, 
            String cipherSuites, 
            String trustStoreType, 
            String keyStoreType) {

        if (!notBlank(brokerUrl)) {
            throw new IllegalArgumentException("artemis.broker.url must be provided");
        }

        String cleanUrl = brokerUrl.trim();

        if (!tlsEnabled) return cleanUrl;

        if (cleanUrl.startsWith("(")) {
            // Apply SSL to every connector in a failover URL, preserving factory parameters.
            try {
                URISupport.CompositeData composite = 
                        URISupport.parseComposite(new URI("static:" + cleanUrl));

                Map<String, String> common = 
                        new LinkedHashMap<>(composite.getParameters());

                common.putIfAbsent("ha", "true");

                StringJoiner connectors = 
                        new StringJoiner(",", "(", ")");

                int index = 0;

                for (URI component : composite.getComponents()) {
                    connectors.add(withSsl(component.toString(), common, trustStorePath,
                            trustStorePassword, keyStorePath, keyStorePassword,
                            cipherSuites, trustStoreType, keyStoreType, index++ == 0 ? 2 : 3));
                }

                // Outer query is appended to the first connector by Artemis and must not
                // overwrite the SSL values just resolved from application properties.
                for (String name : List.of(
                        "sslEnabled", 
                        "enabledProtocols", 
                        "enabledCipherSuites",
                        "trustStorePath", 
                        "trustStorePassword", 
                        "trustStoreType",
                        "keyStorePath", 
                        "keyStorePassword", 
                        "keyStoreType", 
                        "forceSSLParameters",
                        "useDefaultSslContext"
                )) common.remove(name);

                return connectors + "?" + query(common, 2);

            } catch (URISyntaxException failure) {
                // A URL may contain passwords; do not include it in the error message/cause.
                throw new IllegalArgumentException("Invalid composite artemis.broker.url");
            }
        }
        return withSsl(
                cleanUrl, 
                Map.of(), 
                trustStorePath, 
                trustStorePassword,
                keyStorePath, 
                keyStorePassword, 
                cipherSuites, 
                trustStoreType, 
                keyStoreType, 
                2
        );
    }

    private static String withSsl(
            String url, 
            Map<String, String> common,
            String trustPath, 
            String trustPassword, 
            String keyPath, 
            String keyPassword,
            String cipherSuites, 
            String trustType, 
            String keyType, 
            int encodingLevels) {
        int question = url.indexOf('?');
        String address = question < 0 ? url : url.substring(0, question);
        Map<String, String> params = new LinkedHashMap<>(common);
        if (question >= 0) {
            for (String entry : url.substring(question + 1).split("[&;]")) {
                if (entry.isEmpty()) continue;
                int equals = entry.indexOf('=');
                params.put(decode(equals < 0 ? entry : entry.substring(0, equals)),
                        equals < 0 ? "" : decode(entry.substring(equals + 1)));
            }
        }
        params.put("sslEnabled", "true");
        params.putIfAbsent("enabledProtocols", "TLSv1.2,TLSv1.3");
        params.putIfAbsent("ha", "true");
        putIfNotBlank(params, "enabledCipherSuites", cipherSuites);
        putIfNotBlank(params, "trustStorePath", trustPath);
        putIfNotBlank(params, "trustStorePassword", trustPassword);
        putIfNotBlank(params, "keyStorePath", keyPath);
        putIfNotBlank(params, "keyStorePassword", keyPassword);
        params.put("trustStoreType", SslStoreTypes.normalize(notBlank(trustType)
                ? trustType : params.get("trustStoreType")));
        params.put("keyStoreType", SslStoreTypes.normalize(notBlank(keyType)
                ? keyType : params.get("keyStoreType")));
        // Otherwise JVM SSL properties can silently override the selected types and paths.
        params.put("forceSSLParameters", "true");
        if ("true".equalsIgnoreCase(params.get("useDefaultSslContext"))) {
            throw new IllegalArgumentException(
                    "useDefaultSslContext=true ignores configured Artemis SSL stores; "
                    + "remove it when using artemis.tls.enabled=true"
            );
        }
        return address + "?" + query(params, encodingLevels);
    }

    public static String build(
            String host, 
            int port, 
            boolean tlsEnabled,
            String trustStorePath, 
            String trustStorePassword,
            String keyStorePath,
            String keyStorePassword, 
            String cipherSuites) {
        return build(
                host, 
                port, 
                tlsEnabled, 
                trustStorePath, 
                trustStorePassword,
                keyStorePath, 
                keyStorePassword, 
                cipherSuites, 
                null, 
                null
        );
    }

    public static String build(
            String host, 
            int port, 
            boolean tlsEnabled,
            String trustStorePath, 
            String trustStorePassword, 
            String keyStorePath,
            String keyStorePassword, 
            String cipherSuites, 
            String trustStoreType, 
            String keyStoreType) {
        return buildFromBrokerUrl(
                "tcp://" + host + ":" + port,
                tlsEnabled, 
                trustStorePath,
                trustStorePassword, 
                keyStorePath, 
                keyStorePassword, 
                cipherSuites,
                trustStoreType, 
                keyStoreType
        );
    }

    private static void putIfNotBlank(Map<String, String> params, String name, String value) {
        if (notBlank(value)) params.put(name, value);
    }

    private static String query(Map<String, String> params, int encodingLevels) {
        StringJoiner query = new StringJoiner("&");
        params.forEach((name, value) -> query.add(encode(name, encodingLevels) + "=" + encode(value, encodingLevels)));
        return query.toString();
    }

    private static boolean notBlank(String value) { return value != null && !value.isBlank(); }
    private static String encode(String value, int levels) {
        // Artemis decodes URI.getQuery() and then URLDecoder; backup connectors
        // undergo one additional URI.getFragment() decode. Protect separators at each level.
        String encoded = value == null ? "" : value;
        for (int i = 0; i < levels; i++) encoded = URLEncoder.encode(encoded, StandardCharsets.UTF_8);
        return encoded;
    }
    private static String decode(String value) { return URLDecoder.decode(value, StandardCharsets.UTF_8); }
}
